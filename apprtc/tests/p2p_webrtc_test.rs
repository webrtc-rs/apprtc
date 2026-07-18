//! End-to-end P2P V1 test using the async `webrtc` implementation.
mod common;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use common::{WsStream, http, unique_room, wait_for_server, ws_register};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use webrtc::data_channel::DataChannelEvent;
use webrtc::peer_connection::{
    MediaEngine, PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler,
    RTCConfigurationBuilder, RTCIceCandidateInit, RTCPeerConnectionIceEvent,
    RTCPeerConnectionState, RTCSessionDescription, Registry, register_default_interceptors,
};
use webrtc::runtime::default_runtime;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Peer-connection event handler: forwards connection-state changes to the test and
/// trickles locally-gathered ICE candidates out over the signaling channel (as JSON
/// `RTCIceCandidateInit`). Dropping these — as the original handler did — is what left
/// ICE unable to complete.
#[derive(Clone)]
struct Events {
    states: mpsc::UnboundedSender<RTCPeerConnectionState>,
    outgoing: mpsc::UnboundedSender<String>,
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
}

type PeerParts = (
    Arc<dyn PeerConnection>,
    mpsc::UnboundedReceiver<RTCPeerConnectionState>,
    mpsc::UnboundedSender<String>,
    mpsc::UnboundedReceiver<String>,
);

async fn peer() -> Result<PeerParts> {
    let mut media = MediaEngine::default();
    media.register_default_codecs()?;
    let registry = register_default_interceptors(Registry::new(), &mut media)?;
    let runtime = default_runtime().ok_or_else(|| anyhow!("no async runtime"))?;
    let (states_tx, states_rx) = mpsc::unbounded_channel();
    let (out_tx, out_rx) = mpsc::unbounded_channel();
    let pc = PeerConnectionBuilder::new()
        .with_configuration(RTCConfigurationBuilder::new().build())
        .with_media_engine(media)
        .with_interceptor_registry(registry)
        .with_handler(Arc::new(Events {
            states: states_tx,
            outgoing: out_tx.clone(),
        }))
        .with_runtime(runtime)
        .with_udp_addrs(vec!["127.0.0.1:0".to_owned()])
        .build()
        .await?;
    Ok((Arc::new(pc), states_rx, out_tx, out_rx))
}

async fn connected(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<RTCPeerConnectionState>,
) -> Result<()> {
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
    .context("timed out waiting for P2P connection")?
}

/// Relay signaling over `ws`: `outgoing` payloads (an SDP or an `RTCIceCandidateInit`,
/// as JSON) are wrapped in a `{cmd:"send"}` frame and written; inbound `{msg}` frames
/// are applied to `pc` as a remote description or an ICE candidate.
///
/// `remote_already_set` is true when `pc`'s remote description was applied before the
/// relay starts (the callee applies the queued offer up front), so candidates apply
/// immediately. When it is false (the initiator, still awaiting the answer) candidates
/// are buffered until the answer arrives — `add_ice_candidate` needs a remote description.
fn spawn_signaling(
    ws: WsStream,
    pc: Arc<dyn PeerConnection>,
    mut outgoing: mpsc::UnboundedReceiver<String>,
    remote_already_set: bool,
) {
    tokio::spawn(async move {
        let (mut writer, mut reader) = ws.split();
        let mut remote_set = remote_already_set;
        let mut pending: Vec<RTCIceCandidateInit> = Vec::new();
        loop {
            tokio::select! {
                outbound = outgoing.recv() => {
                    let Some(payload) = outbound else { break };
                    let frame = json!({ "cmd": "send", "msg": payload }).to_string();
                    if writer.send(Message::text(frame)).await.is_err() {
                        break;
                    }
                }
                inbound = reader.next() => match inbound {
                    Some(Ok(Message::Text(text))) => {
                        apply_incoming(&pc, &text, &mut remote_set, &mut pending).await;
                    }
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {}
                },
            }
        }
    });
}

/// Apply one inbound `{msg}` frame: a remote description (`{type,sdp}`) or an ICE
/// candidate (`{candidate,...}`). Best-effort — malformed frames are ignored.
async fn apply_incoming(
    pc: &Arc<dyn PeerConnection>,
    text: &str,
    remote_set: &mut bool,
    pending: &mut Vec<RTCIceCandidateInit>,
) {
    let Some(inner) = serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|frame| frame.get("msg")?.as_str().map(str::to_owned))
    else {
        return;
    };
    let Ok(value) = serde_json::from_str::<Value>(&inner) else {
        return;
    };

    if value.get("candidate").is_some() {
        if let Ok(candidate) = serde_json::from_value::<RTCIceCandidateInit>(value) {
            if *remote_set {
                let _ = pc.add_ice_candidate(candidate).await;
            } else {
                pending.push(candidate);
            }
        }
    } else if value.get("type").is_some()
        && let Ok(sdp) = serde_json::from_value::<RTCSessionDescription>(value)
    {
        let _ = pc.set_remote_description(sdp).await;
        *remote_set = true;
        for candidate in pending.drain(..) {
            let _ = pc.add_ice_candidate(candidate).await;
        }
    }
}

#[tokio::test]
async fn completes_v1_signaling_and_webrtc_datachannel_connection() -> Result<()> {
    wait_for_server().await?;
    let index = http("GET", "/", &[]).await?;
    assert_eq!(index.status, 200);
    assert!(index.text()?.contains("AppRTC"));

    let room = unique_room("webrtc-p2p");

    // Initiator joins, builds its peer and offer, and posts the offer via `/message`.
    let first = common::join(&room).await?;
    let first_id = first["params"]["client_id"]
        .as_str()
        .context("initiator id")?
        .to_owned();
    let (initiator_pc, mut initiator_states, _initiator_out, initiator_out_rx) = peer().await?;
    let initiator_dc = initiator_pc.create_data_channel("p2p-test", None).await?;
    let initiator_ws = ws_register(&room, &first_id).await?;
    let offer = initiator_pc.create_offer(None).await?;
    initiator_pc.set_local_description(offer.clone()).await?;
    let offer_body = serde_json::to_string(&offer)?;
    let posted = http(
        "POST",
        &format!("/message/{room}/{first_id}"),
        offer_body.as_bytes(),
    )
    .await?;
    assert_eq!(posted.status, 200);
    // The initiator's relay starts only after the callee registers (below). Until then
    // its trickled candidates buffer in `initiator_out_rx`, so they reach the callee over
    // WebSocket rather than draining into the callee's queued `messages` at join time.

    // Callee joins, applies the queued offer, answers, and starts its relay.
    let second = common::join(&room).await?;
    let second_id = second["params"]["client_id"]
        .as_str()
        .context("callee id")?
        .to_owned();
    let messages = second["params"]["messages"]
        .as_array()
        .context("queued offer")?;
    let queued: RTCSessionDescription =
        serde_json::from_str(messages[0].as_str().context("offer text")?)?;
    let (callee_pc, mut callee_states, callee_out, callee_out_rx) = peer().await?;
    let callee_ws = ws_register(&room, &second_id).await?;
    callee_pc.set_remote_description(queued).await?;
    let answer = callee_pc.create_answer(None).await?;
    callee_pc.set_local_description(answer.clone()).await?;
    callee_out
        .send(serde_json::to_string(&answer)?)
        .map_err(|_| anyhow!("callee relay closed"))?;
    spawn_signaling(callee_ws, callee_pc.clone(), callee_out_rx, true);

    // Now start the initiator's relay: flush its buffered candidates and receive the
    // answer plus the callee's candidates.
    spawn_signaling(initiator_ws, initiator_pc.clone(), initiator_out_rx, false);

    connected(&mut initiator_states).await?;
    connected(&mut callee_states).await?;

    timeout(CONNECT_TIMEOUT, async {
        loop {
            match initiator_dc.poll().await {
                Some(DataChannelEvent::OnOpen) => break,
                Some(DataChannelEvent::OnClose) | None => {
                    return Err(anyhow!("data channel closed"));
                }
                _ => {}
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("timed out waiting for SCTP data channel")??;
    initiator_dc.send_text("hello from initiator").await?;

    let _ = http("POST", &format!("/leave/{room}/{first_id}"), &[]).await?;
    let _ = http("POST", &format!("/leave/{room}/{second_id}"), &[]).await?;
    initiator_pc.close().await?;
    callee_pc.close().await?;
    Ok(())
}
