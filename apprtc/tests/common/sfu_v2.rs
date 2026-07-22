//! Shared harness for the SFU V2 end-to-end tests.
//!
//! Emulates one V2 browser through the P2P -> SFU upgrade: it joins over HTTP, registers
//! its WebSocket, waits for the room to reach SFU mode, then publishes to the assigned SFU
//! worker and answers the worker's subscribe offers — the same wire protocol the browser
//! `peerconnectionclient.js` / `signalingchannel.js` speak:
//!
//!   * outbound `{cmd:"send", epoch, msg:"<inner-json>"}` where the inner JSON is an SDP
//!     (`{type,sdp}`, plus `requestid` when answering a subscribe offer) or a trickle
//!     candidate (`{type:"candidate", candidate, id, label}`),
//!   * inbound `{msg:"<inner-json>"}` carrying the worker's publish answer, its subscribe
//!     offers (`{type:"offer", sdp, requestid}`), and its trickle candidates.
#![allow(dead_code)]

use super::{WsStream, join_v2, ws_receive_json, ws_register_v2};
use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rtc::rtp_transceiver::rtp_sender::{RTCRtpHeaderExtensionCapability, RtpCodecKind};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_remote::TrackRemote;
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCConfigurationBuilder, RTCIceCandidateInit, RTCPeerConnectionIceEvent,
    RTCPeerConnectionState, RTCSessionDescription, Registry, register_default_interceptors,
};
use webrtc::rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use webrtc::runtime::default_runtime;

pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Peer-connection event handler: forwards connection-state changes, the tracks the SFU
/// forwards to this client, and locally gathered ICE candidates (as JSON
/// `RTCIceCandidateInit`).
#[derive(Clone)]
pub struct Events {
    pub states: mpsc::UnboundedSender<RTCPeerConnectionState>,
    pub outgoing: mpsc::UnboundedSender<String>,
    pub tracks: mpsc::UnboundedSender<Arc<dyn TrackRemote>>,
}

#[async_trait]
impl PeerConnectionEventHandler for Events {
    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        let _ = self.states.send(state);
    }

    async fn on_ice_candidate(&self, event: RTCPeerConnectionIceEvent) {
        if let Ok(init) = event.candidate.to_json()
            && !init.candidate.is_empty()
            && let Ok(json) = serde_json::to_string(&init)
        {
            let _ = self.outgoing.send(json);
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        let _ = self.tracks.send(track);
    }
}

pub struct Peer {
    pub pc: Arc<dyn PeerConnection>,
    pub states: mpsc::UnboundedReceiver<RTCPeerConnectionState>,
    pub outgoing: mpsc::UnboundedReceiver<String>,
    pub tracks: mpsc::UnboundedReceiver<Arc<dyn TrackRemote>>,
}

/// Build a peer connection wired to `Events`. The caller adds its data channel or local
/// tracks before calling [`drive`].
pub async fn peer() -> Result<Peer> {
    build_peer(|_media| Ok(())).await
}

/// A peer that also registers `ssrc-audio-level` on audio — the way a real browser (Chrome)
/// does. Registering it before the default extensions gives this peer's audio m-line a low
/// header-extension id, which collides with the SFU's forwarded video `sdes:mid` (also a low
/// id) once bundled — reproducing Chrome's "BUNDLE group contains a codec collision for header
/// extension id" `setLocalDescription` failure.
pub async fn browser_peer() -> Result<Peer> {
    build_peer(|media| {
        media.register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: "urn:ietf:params:rtp-hdrext:ssrc-audio-level".to_owned(),
            },
            RtpCodecKind::Audio,
            None,
        )?;
        Ok(())
    })
    .await
}

async fn build_peer(configure_media: impl FnOnce(&mut MediaEngine) -> Result<()>) -> Result<Peer> {
    let mut media = MediaEngine::default();
    media.register_default_codecs()?;
    configure_media(&mut media)?;
    let registry = register_default_interceptors(Registry::new(), &mut media)?;
    let runtime = default_runtime().ok_or_else(|| anyhow!("no async runtime"))?;
    let (states_tx, states_rx) = mpsc::unbounded_channel();
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let (tracks_tx, tracks_rx) = mpsc::unbounded_channel();
    let pc = PeerConnectionBuilder::new()
        .with_configuration(RTCConfigurationBuilder::new().build())
        .with_media_engine(media)
        .with_interceptor_registry(registry)
        .with_handler(Arc::new(Events {
            states: states_tx,
            outgoing: out_tx,
            tracks: tracks_tx,
        }))
        .with_runtime(runtime)
        .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
        .build()
        .await?;
    Ok(Peer {
        pc: Arc::new(pc),
        states: states_rx,
        outgoing: out_rx,
        tracks: tracks_rx,
    })
}

/// Wait for the peer connection to reach `Connected`.
pub async fn connected(rx: &mut mpsc::UnboundedReceiver<RTCPeerConnectionState>) -> Result<()> {
    timeout(CONNECT_TIMEOUT, async {
        while let Some(state) = rx.recv().await {
            if state == RTCPeerConnectionState::Connected {
                return Ok(());
            }
            if matches!(
                state,
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed
            ) {
                return Err(anyhow!("peer connection entered {state}"));
            }
        }
        Err(anyhow!("connection state channel closed"))
    })
    .await
    .context("timed out waiting for SFU peer connection")?
}

/// One admitted, registered member ready to publish to the SFU.
pub struct Member {
    pub client_id: u64,
    pub token: String,
    pub ws: WsStream,
    pub registered: Value,
}

fn parse_member(joined: &Value) -> Result<(u64, String)> {
    let client_id = joined["params"]["client_id"]
        .as_str()
        .context("join has no client_id")?
        .parse()
        .context("client_id is not u64")?;
    let token = joined["params"]["admission_token"]
        .as_str()
        .context("join has no admission_token")?
        .to_owned();
    Ok((client_id, token))
}

/// Drive a room through the P2P -> SFU upgrade and return three registered members, all in
/// SFU mode at epoch 1. The first two join as P2P and are pushed `sfu-upgrade`; the third
/// join blocks until the worker has joined all three and the room commits to SFU.
pub async fn upgrade_three(room_id: u64) -> Result<[Member; 3]> {
    let first = join_v2(room_id).await?;
    let second = join_v2(room_id).await?;
    assert_eq!(first["params"]["mode"], "p2p", "first join should be P2P");
    assert_eq!(second["params"]["mode"], "p2p", "second join should be P2P");
    let (first_id, first_token) = parse_member(&first)?;
    let (second_id, second_token) = parse_member(&second)?;
    let (mut first_ws, first_registered) = ws_register_v2(room_id, first_id, &first_token).await?;
    let (mut second_ws, second_registered) =
        ws_register_v2(room_id, second_id, &second_token).await?;
    assert_eq!(first_registered["mode"], "p2p");
    assert_eq!(second_registered["mode"], "p2p");

    // Blocks until signaling has received MemberJoined for all three from the SFU and
    // committed epoch 1.
    let third = join_v2(room_id).await?;
    assert_eq!(
        third["params"]["mode"], "sfu",
        "third join must upgrade the room to SFU: {third}"
    );
    assert_eq!(third["params"]["epoch"], "1");
    let (third_id, third_token) = parse_member(&third)?;
    let (third_ws, third_registered) = ws_register_v2(room_id, third_id, &third_token).await?;
    assert_eq!(third_registered["mode"], "sfu");
    assert_eq!(third_registered["epoch"], "1");

    // The two existing members are pushed the sfu-upgrade control at commit.
    for ws in [&mut first_ws, &mut second_ws] {
        let control = ws_receive_json(ws).await?;
        assert_eq!(control["control"], "sfu-upgrade", "existing member upgrade");
        assert_eq!(control["epoch"], "1");
    }

    Ok([
        Member {
            client_id: first_id,
            token: first_token,
            ws: first_ws,
            registered: first_registered,
        },
        Member {
            client_id: second_id,
            token: second_token,
            ws: second_ws,
            registered: second_registered,
        },
        Member {
            client_id: third_id,
            token: third_token,
            ws: third_ws,
            registered: third_registered,
        },
    ])
}

type Writer = SplitSink<WsStream, Message>;

/// Send `{cmd:"send", epoch, msg}` with `msg` an inner JSON string.
async fn send_inner(writer: &mut Writer, epoch: &str, inner: Value) -> Result<()> {
    let frame = json!({ "cmd": "send", "epoch": epoch, "msg": inner.to_string() });
    writer.send(Message::text(frame.to_string())).await?;
    Ok(())
}

/// Send an SDP as the browser does: `{type,sdp}` plus `requestid` when it is an answer to a
/// subscribe offer.
async fn send_sdp(
    writer: &mut Writer,
    epoch: &str,
    sdp: &RTCSessionDescription,
    request_id: Option<&str>,
) -> Result<()> {
    let mut value = serde_json::to_value(sdp)?;
    if let (Some(object), Some(request_id)) = (value.as_object_mut(), request_id) {
        object.insert("requestid".into(), request_id.into());
    }
    send_inner(writer, epoch, value).await
}

/// Convert a locally gathered `RTCIceCandidateInit` JSON into the SFU's candidate envelope
/// and send it.
async fn send_candidate(writer: &mut Writer, epoch: &str, candidate_json: &str) -> Result<()> {
    let init: Value = serde_json::from_str(candidate_json)?;
    let inner = json!({
        "type": "candidate",
        "candidate": init["candidate"],
        "id": init.get("sdpMid").cloned().unwrap_or(Value::Null),
        "label": init.get("sdpMLineIndex").cloned().unwrap_or(Value::Null),
    });
    send_inner(writer, epoch, inner).await
}

/// Print an SDP's media layout when `SFU_V2_DUMP` is set (m-lines, mids, directions, ssrcs).
fn dump_sdp(label: &str, sdp: &str) {
    if std::env::var("SFU_V2_DUMP").is_err() {
        return;
    }
    let summary: Vec<&str> = sdp
        .lines()
        .filter(|l| {
            l.starts_with("m=")
                || l.starts_with("a=mid:")
                || l.starts_with("a=sendonly")
                || l.starts_with("a=recvonly")
                || l.starts_with("a=sendrecv")
                || l.starts_with("a=inactive")
                || l.starts_with("a=ssrc:")
        })
        .collect();
    eprintln!(
        "=== {label} ===\n{}\n=====================",
        summary.join("\n")
    );
}

/// Create an offer covering the peer connection's current transceivers and data channels, set
/// it as the local description, and send it. Used for the initial publish and for the
/// re-publish that follows a polite-peer glare rollback. Each local description is also reported
/// to `local_descriptions` (when set) so a test can assert it is valid for a real browser.
async fn send_offer(
    pc: &Arc<dyn PeerConnection>,
    writer: &mut Writer,
    epoch: &str,
    label: &str,
    local_descriptions: Option<&mpsc::UnboundedSender<RTCSessionDescription>>,
) -> Result<()> {
    let offer = pc.create_offer(None).await?;
    dump_sdp(label, &offer.sdp);
    pc.set_local_description(offer.clone()).await?;
    if let Some(sink) = local_descriptions {
        let _ = sink.send(offer.clone());
    }
    send_sdp(writer, epoch, &offer, None).await
}

/// Apply one inbound `{msg}` frame from the SFU using perfect negotiation with the client as the
/// **polite** peer and the SFU as the **impolite** peer: apply publish answers; on a subscribe
/// re-offer that collides with our own outstanding offer (glare), roll our offer back, apply the
/// SFU's, answer it, then re-issue our publish so it settles onto its own m-line; and buffer
/// trickle candidates until a remote description exists.
#[allow(clippy::too_many_arguments)]
async fn apply_inbound(
    pc: &Arc<dyn PeerConnection>,
    writer: &mut Writer,
    epoch: &str,
    text: &str,
    remote_set: &mut bool,
    pending: &mut Vec<RTCIceCandidateInit>,
    seen_offers: &mpsc::UnboundedSender<RTCSessionDescription>,
    local_descriptions: Option<&mpsc::UnboundedSender<RTCSessionDescription>>,
) -> Result<()> {
    let Some(inner) = serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|frame| frame.get("msg")?.as_str().map(str::to_owned))
    else {
        return Ok(());
    };
    let Ok(value) = serde_json::from_str::<Value>(&inner) else {
        return Ok(());
    };
    match value.get("type").and_then(Value::as_str) {
        Some("answer") => {
            // Answer to one of our offers. Ignore a stray answer when we have no local offer.
            if pc.pending_local_description().await.is_none() {
                return Ok(());
            }
            let sdp: RTCSessionDescription = serde_json::from_value(value)?;
            pc.set_remote_description(sdp).await?;
            *remote_set = true;
            for candidate in pending.drain(..) {
                let _ = pc.add_ice_candidate(candidate).await;
            }
        }
        Some("offer") => {
            let request_id = value
                .get("requestid")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let sdp: RTCSessionDescription = serde_json::from_value(value)?;
            dump_sdp("SUBSCRIBE OFFER (SFU->client)", &sdp.sdp);
            let _ = seen_offers.send(sdp.clone());
            // Polite peer: if our own offer is still outstanding, this remote offer is a glare
            // collision. Roll our offer back before applying theirs (JSEP RFC 8829 Section 5.7),
            // so the subscribe and our publish don't fight over the same mid.
            let glare = pc.pending_local_description().await.is_some();
            if glare {
                pc.set_local_description(RTCSessionDescription::rollback(None)?)
                    .await?;
            }
            pc.set_remote_description(sdp).await?;
            *remote_set = true;
            for candidate in pending.drain(..) {
                let _ = pc.add_ice_candidate(candidate).await;
            }
            let answer = pc.create_answer(None).await?;
            pc.set_local_description(answer.clone()).await?;
            if let Some(sink) = local_descriptions {
                let _ = sink.send(answer.clone());
            }
            send_sdp(writer, epoch, &answer, request_id.as_deref()).await?;
            // Re-issue the local offer we rolled back so our own publish finishes negotiating on
            // its own m-line, additively alongside the track we just subscribed to.
            if glare {
                send_offer(pc, writer, epoch, "REPUBLISH OFFER", local_descriptions).await?;
            }
        }
        Some("candidate") => {
            let candidate = RTCIceCandidateInit {
                candidate: value["candidate"].as_str().unwrap_or_default().to_owned(),
                sdp_mid: value["id"].as_str().map(str::to_owned),
                sdp_mline_index: value["label"].as_u64().map(|v| v as u16),
                ..Default::default()
            };
            if *remote_set {
                let _ = pc.add_ice_candidate(candidate).await;
            } else {
                pending.push(candidate);
            }
        }
        _ => {}
    }
    Ok(())
}

/// Drive one client's SFU signaling for its peer connection's lifetime, mirroring the SFU
/// `chat` client. It attaches `publish_track` (if any) as a sendonly transceiver and sends the
/// first offer — publishing the media on `mid:0`, the same mid the SFU uses for its subscribe
/// offers — then auto-answers every subscribe re-offer the SFU pushes, resolving glare as the
/// polite peer (see `apply_inbound`). `seen_offers` receives each subscribe offer for
/// validation; `connected_tx` fires when the peer connection reaches Connected.
pub struct DriveConfig {
    pub ws: WsStream,
    pub pc: Arc<dyn PeerConnection>,
    pub epoch: String,
    pub outgoing: mpsc::UnboundedReceiver<String>,
    pub seen_offers: mpsc::UnboundedSender<RTCSessionDescription>,
    pub states: mpsc::UnboundedReceiver<RTCPeerConnectionState>,
    pub connected_tx: oneshot::Sender<Result<()>>,
    pub publish_track: Option<Arc<dyn TrackLocal>>,
    /// When set, receives every local description (publish offer, re-publish offer, and answer)
    /// this client sets — so a test can assert each one is valid for a real browser.
    pub local_descriptions: Option<mpsc::UnboundedSender<RTCSessionDescription>>,
}

pub fn drive(config: DriveConfig) {
    tokio::spawn(async move {
        let DriveConfig {
            ws,
            pc,
            epoch,
            mut outgoing,
            seen_offers,
            mut states,
            connected_tx,
            publish_track,
            local_descriptions,
        } = config;
        let (mut writer, mut reader): (Writer, SplitStream<WsStream>) = ws.split();
        let mut remote_set = false;
        let mut pending: Vec<RTCIceCandidateInit> = Vec::new();
        let mut connected_tx = Some(connected_tx);

        // Publish up front: attach the media track (if any) so the first offer carries it on
        // `mid:0` — the same mid the SFU uses for its subscribe offers, so a polite-peer glare
        // rollback lets the subscribe and our publish settle onto separate m-lines. Any data
        // channel the caller already added is included in the same offer.
        let first_offer = async {
            if let Some(track) = &publish_track {
                pc.add_transceiver_from_track(
                    track.clone(),
                    Some(RTCRtpTransceiverInit {
                        direction: RTCRtpTransceiverDirection::Sendonly,
                        streams: vec![],
                        send_encodings: vec![],
                    }),
                )
                .await?;
            }
            send_offer(
                &pc,
                &mut writer,
                &epoch,
                "PUBLISH OFFER",
                local_descriptions.as_ref(),
            )
            .await
        };
        if let Err(error) = first_offer.await {
            if let Some(tx) = connected_tx.take() {
                let _ = tx.send(Err(anyhow!("initial publish offer failed: {error}")));
            }
            return;
        }

        loop {
            tokio::select! {
                state = states.recv() => match state {
                    Some(RTCPeerConnectionState::Connected) => {
                        if let Some(tx) = connected_tx.take() {
                            let _ = tx.send(Ok(()));
                        }
                    }
                    Some(RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) => {
                        if let Some(tx) = connected_tx.take() {
                            let _ = tx.send(Err(anyhow!("peer connection failed")));
                        }
                        break;
                    }
                    Some(_) => {}
                    None => break,
                },
                outbound = outgoing.recv() => {
                    let Some(candidate_json) = outbound else { break };
                    if send_candidate(&mut writer, &epoch, &candidate_json).await.is_err() {
                        break;
                    }
                }
                inbound = reader.next() => match inbound {
                    Some(Ok(Message::Text(text))) => {
                        if let Err(error) = apply_inbound(
                            &pc, &mut writer, &epoch, &text,
                            &mut remote_set, &mut pending, &seen_offers,
                            local_descriptions.as_ref(),
                        ).await {
                            eprintln!("apply inbound failed: {error}");
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        let _ = writer.send(Message::Pong(payload)).await;
                    }
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                },
            }
        }
    });
}
