//! End-to-end SFU V2 media test: three `webrtc` clients join one room, the third join
//! upgrades it to SFU, all three publish a video track, and the SFU forwards each
//! publisher's track to the other two. Each client must receive two remote tracks, and the
//! SFU's subscribe-offer SDP must be valid for a real browser (a single BUNDLE group with a
//! consistent RTP-header-extension id map across its m-lines — the exact rule Chrome
//! enforces with "a BUNDLE group contains a codec collision for header extension id").
mod common;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use common::sfu_v2::{DriveConfig, Peer, drive, peer, upgrade_three};
use common::wait_for_server;
use rtc::media_stream::MediaStreamTrack;
use rtc::rtp::Packet;
use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_rtp::TrackLocalStaticRTP;
use webrtc::peer_connection::{PeerConnection, RTCSessionDescription};

/// Reject an SDP whose BUNDLE m-lines assign one RTP-header-extension id to two different
/// URIs — the SDP-level form of Chrome's "BUNDLE group contains a codec collision for
/// header extension id" `setLocalDescription` failure.
fn assert_bundle_extmap_consistent(sdp: &str) -> Result<()> {
    let mut id_uri: HashMap<u16, String> = HashMap::new();
    for line in sdp.lines() {
        let Some(rest) = line.trim().strip_prefix("a=extmap:") else {
            continue;
        };
        let mut parts = rest.split_whitespace();
        let id = parts
            .next()
            .and_then(|field| field.split('/').next())
            .and_then(|id| id.parse::<u16>().ok())
            .ok_or_else(|| anyhow!("malformed extmap line: {line}"))?;
        let uri = parts.next().unwrap_or_default().to_owned();
        if let Some(previous) = id_uri.get(&id) {
            if previous != &uri {
                bail!(
                    "BUNDLE header-extension id collision: id={id} maps to both '{previous}' and '{uri}'\nSDP:\n{sdp}"
                );
            }
        } else {
            id_uri.insert(id, uri);
        }
    }
    Ok(())
}

fn video_track(ssrc: u32, id: &str) -> Arc<TrackLocalStaticRTP> {
    Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
        format!("stream-{id}"),
        format!("video-{id}"),
        "video".into(),
        RtpCodecKind::Video,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(ssrc),
                ..Default::default()
            },
            active: true,
            codec: RTCRtpCodec {
                mime_type: "video/VP8".into(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            ..Default::default()
        }],
    )))
}

/// Keep sending VP8 RTP so the SFU binds the SSRC and instantiates the remote track on each
/// subscriber, until the test drops the returned guard.
fn keep_publishing(track: Arc<TrackLocalStaticRTP>, ssrc: u32) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut sequence_number: u16 = 0;
        loop {
            let _ = track
                .write_rtp(Packet {
                    header: rtc::rtp::Header {
                        version: 2,
                        payload_type: 96,
                        sequence_number,
                        timestamp: u32::from(sequence_number).wrapping_mul(3000),
                        ssrc,
                        marker: sequence_number == 0,
                        ..Default::default()
                    },
                    payload: Bytes::from_static(&[0x90, 0x00, 0x00, 0x00, 0x00]),
                })
                .await;
            sequence_number = sequence_number.wrapping_add(1);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
}

struct Active {
    pc: Arc<dyn PeerConnection>,
    tracks: mpsc::UnboundedReceiver<Arc<dyn webrtc::media_stream::track_remote::TrackRemote>>,
    offers: mpsc::UnboundedReceiver<RTCSessionDescription>,
    _publisher: tokio::task::JoinHandle<()>,
    _track: Arc<TrackLocalStaticRTP>,
}

// End-to-end SFU multi-party media forwarding. Requires a live signaling + sfu + appweb stack.
// All three clients upgrade to SFU, connect, and publish through WebRTC perfect negotiation (the
// polite-peer rollback + re-publish that recovers from the worker's glare rejection), and the
// SFU forwards every publisher's track to the other two members — each member receives two
// remote tracks. This exercises the two forwarding fixes: the worker draining `poll_read` so the
// SFU dispatches inbound RTP to subscriber senders, and the peer connection re-running the
// negotiation-needed check on return to `stable` so a last joiner's second inbound forward
// (added while it was still answering its own publish) is actually offered.
#[tokio::test]
async fn forwards_each_publisher_to_every_other_member_over_the_sfu() -> Result<()> {
    wait_for_server().await?;
    let room_id = rand::random::<u64>().max(1);

    let members = upgrade_three(room_id).await?;

    // Bring the publishers up one at a time: each member's SFU peer connection must reach
    // Connected before the next one publishes. This builds the forwarding graph incrementally
    // and keeps a subscribe offer (which the SFU emits as soon as a new publisher appears)
    // from racing into a peer whose own publish offer is still unanswered — the glare that a
    // real browser resolves with perfect-negotiation rollback.
    let mut actives: Vec<Active> = Vec::new();
    for (index, member) in members.into_iter().enumerate() {
        let Peer {
            pc,
            states,
            outgoing,
            tracks,
        } = peer().await?;
        let ssrc = 0x1000_0000 + index as u32 + 1;
        let track = video_track(ssrc, &member.client_id.to_string());
        // drive() publishes the video track on mid:0 as the first offer; subscribe re-offers
        // (also mid:0) are resolved as the polite peer via glare rollback + re-publish.
        let (offers_tx, offers_rx) = mpsc::unbounded_channel();
        let (connected_tx, connected_rx) = oneshot::channel();
        drive(DriveConfig {
            ws: member.ws,
            pc: pc.clone(),
            epoch: "1".to_owned(),
            outgoing,
            seen_offers: offers_tx,
            states,
            connected_tx,
            publish_track: Some(track.clone() as Arc<dyn TrackLocal>),
        });
        let publisher = keep_publishing(track.clone(), ssrc);
        timeout(Duration::from_secs(30), connected_rx)
            .await
            .with_context(|| format!("member {index} connect timed out"))?
            .with_context(|| format!("member {index} publish task ended"))?
            .with_context(|| format!("member {index} did not connect to the SFU"))?;
        actives.push(Active {
            pc,
            tracks,
            offers: offers_rx,
            _publisher: publisher,
            _track: track,
        });
    }

    // Every member receives the two other publishers' forwarded tracks.
    for (index, active) in actives.iter_mut().enumerate() {
        for received in 0..2 {
            timeout(Duration::from_secs(30), active.tracks.recv())
                .await
                .with_context(|| {
                    format!("member {index} timed out waiting for forwarded track {received}")
                })?
                .ok_or_else(|| anyhow!("member {index} track channel closed"))?;
        }
    }

    // Every subscribe offer the SFU sent must be valid for a real browser: one BUNDLE group
    // with a consistent header-extension id map across its m-lines.
    for (index, active) in actives.iter_mut().enumerate() {
        let mut offers = 0;
        while let Ok(offer) = active.offers.try_recv() {
            offers += 1;
            assert_bundle_extmap_consistent(&offer.sdp)
                .with_context(|| format!("member {index} received an invalid subscribe offer"))?;
        }
        assert!(offers > 0, "member {index} received no subscribe offer");
    }

    for active in &actives {
        active.pc.close().await?;
    }
    Ok(())
}
