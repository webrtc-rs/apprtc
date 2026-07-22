//! End-to-end SFU V2 header-extension test: three browser-like clients join one room, the third
//! join upgrades it to SFU, and all three publish **audio + video** (as a real browser does).
//!
//! This reproduces the RTP-header-extension-id collision a real Chrome client hits but the Rust
//! harness previously did not: every local description a client sets — its publish offer, the
//! answer to the SFU's subscribe offer, and the re-publish offer — must be valid for a browser,
//! i.e. within the single BUNDLE group no header-extension id may map to two different URIs.
//!
//! The clients register `ssrc-audio-level` on audio (like Chrome), so their audio m-line takes a
//! low extension id. The SFU's forwarded video m-line uses that same low id for `sdes:mid`; once
//! a client bundles its own audio publish with a forwarded video track, the ids collide and
//! Chrome rejects the offer with "A BUNDLE group contains a codec collision for header extension
//! id". The Rust peer connection tolerates the invalid SDP, so the assertion below catches it.
mod common;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use common::sfu_v2::{DriveConfig, browser_peer, drive, upgrade_three};
use common::wait_for_server;
use rtc::media_stream::MediaStreamTrack;
use rtc::rtp::Packet;
use rtc::rtp_transceiver::rtp_sender::{
    RTCRtpCodec, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use webrtc::media_stream::track_local::TrackLocal;
use webrtc::media_stream::track_local::static_rtp::TrackLocalStaticRTP;
use webrtc::peer_connection::{PeerConnection, RTCSdpType, RTCSessionDescription};
use webrtc::rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit};

/// Reject a set of SDPs that, taken together, assign one RTP-header-extension id to two different
/// URIs — the SDP-level form of Chrome's "BUNDLE group contains a codec collision for header
/// extension id" `setLocalDescription` failure.
///
/// The collision a real browser hits is *between* SDPs, not within one: the client publishes with
/// its own extension ids (e.g. `id=1=ssrc-audio-level` on audio), and the SFU's subscribe offer
/// dictates ids for the forwarded m-lines (e.g. `id=1=sdes:mid`). A browser keeps both — its
/// publish ids and the ids the SFU chose — in one BUNDLE group, so if any id maps to two URIs
/// across that union it cannot set its re-publish offer. (The Rust peer connection silently
/// renumbers on re-offer, hiding the clash within any single SDP, which is why the check must
/// span the publish offer *and* the SFU's offers.)
fn assert_bundle_extmap_consistent(label: &str, sdps: &[(&str, String)]) -> Result<()> {
    let mut id_uri: HashMap<u16, (String, String)> = HashMap::new();
    for (source, sdp) in sdps {
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
            if let Some((previous_uri, previous_source)) = id_uri.get(&id) {
                if previous_uri != &uri {
                    bail!(
                        "{label}: BUNDLE header-extension id collision: id={id} maps to \
                         '{previous_uri}' (from {previous_source}) and '{uri}' (from {source}) — \
                         a browser cannot bundle these into one re-publish offer"
                    );
                }
            } else {
                id_uri.insert(id, (uri, (*source).to_owned()));
            }
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

fn audio_track(ssrc: u32, id: &str) -> Arc<TrackLocalStaticRTP> {
    Arc::new(TrackLocalStaticRTP::new(MediaStreamTrack::new(
        format!("stream-{id}"),
        format!("audio-{id}"),
        "audio".into(),
        RtpCodecKind::Audio,
        vec![RTCRtpEncodingParameters {
            rtp_coding_parameters: RTCRtpCodingParameters {
                ssrc: Some(ssrc),
                ..Default::default()
            },
            active: true,
            codec: RTCRtpCodec {
                mime_type: "audio/opus".into(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
                rtcp_feedback: vec![],
            },
            ..Default::default()
        }],
    )))
}

/// Keep sending VP8 RTP so the SFU binds the SSRC and forwards the video track to the other
/// members (driving the re-publish that bundles audio with a forwarded video m-line).
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
    local_descriptions: mpsc::UnboundedReceiver<RTCSessionDescription>,
    sfu_offers: mpsc::UnboundedReceiver<RTCSessionDescription>,
    _publisher: tokio::task::JoinHandle<()>,
    _video: Arc<TrackLocalStaticRTP>,
    _audio: Arc<TrackLocalStaticRTP>,
}

// Run explicitly against a live signaling + sfu + appweb stack:
//   cargo test -p apprtc --test sfu_v2_webrtc_header_extension_test -- --ignored --nocapture
//
// Ignored by default because it currently reproduces (fails on) an unfixed SFU bug: the worker's
// subscribe offers assign RTP-header-extension ids independently of the ids a client already
// published with, so a real browser hits "A BUNDLE group contains a codec collision for header
// extension id" when it re-publishes. Un-ignore once the SFU derives the forwarded m-lines'
// extension ids from the subscriber's negotiated ids.
#[ignore = "reproduces the unfixed SFU header-extension-id collision (browser re-publish fails)"]
#[tokio::test]
async fn every_local_description_has_a_browser_valid_bundle_extmap() -> Result<()> {
    wait_for_server().await?;
    let room_id = rand::random::<u64>().max(1);

    let members = upgrade_three(room_id).await?;

    // Bring members up one at a time (each must reach Connected before the next publishes), each
    // publishing audio + video like a browser.
    let mut actives: Vec<Active> = Vec::new();
    for (index, member) in members.into_iter().enumerate() {
        let peer = browser_peer().await?;
        let ssrc = 0x1000_0000 + (index as u32 + 1) * 2;
        let video = video_track(ssrc, &member.client_id.to_string());
        let audio = audio_track(ssrc + 1, &member.client_id.to_string());

        // Attach audio first (mid:0) so drive() adds the video track (mid:1) into the same first
        // offer — the audio-then-video ordering a browser publishes with.
        peer.pc
            .add_transceiver_from_track(
                audio.clone() as Arc<dyn TrackLocal>,
                Some(RTCRtpTransceiverInit {
                    direction: RTCRtpTransceiverDirection::Sendonly,
                    streams: vec![],
                    send_encodings: vec![],
                }),
            )
            .await?;

        let (offers_tx, offers_rx) = mpsc::unbounded_channel();
        let (locals_tx, locals_rx) = mpsc::unbounded_channel();
        let (connected_tx, connected_rx) = oneshot::channel();
        drive(DriveConfig {
            ws: member.ws,
            pc: peer.pc.clone(),
            epoch: "1".to_owned(),
            outgoing: peer.outgoing,
            seen_offers: offers_tx,
            states: peer.states,
            connected_tx,
            publish_track: Some(video.clone() as Arc<dyn TrackLocal>),
            local_descriptions: Some(locals_tx),
        });
        let publisher = keep_publishing(video.clone(), ssrc);
        timeout(Duration::from_secs(30), connected_rx)
            .await
            .with_context(|| format!("member {index} connect timed out"))?
            .with_context(|| format!("member {index} publish task ended"))?
            .with_context(|| format!("member {index} did not connect to the SFU"))?;
        actives.push(Active {
            pc: peer.pc,
            local_descriptions: locals_rx,
            sfu_offers: offers_rx,
            _publisher: publisher,
            _video: video,
            _audio: audio,
        });
    }

    // Let the forwarding graph settle so every member has re-published bundling its own audio
    // with the forwarded video m-lines.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // For each member, model the BUNDLE group a real browser would hold: its own publish offer's
    // extension ids together with the ids the SFU dictated in every subscribe offer (which a
    // browser adopts for the forwarded m-lines). No id may map to two URIs across that union, or
    // the browser could not set its re-publish offer.
    for (index, active) in actives.iter_mut().enumerate() {
        let mut sources: Vec<(&str, String)> = Vec::new();

        let mut publish_offer = None;
        while let Ok(sdp) = active.local_descriptions.try_recv() {
            if sdp.sdp_type == RTCSdpType::Offer && publish_offer.is_none() {
                publish_offer = Some(sdp.sdp);
            }
        }
        sources.push((
            "client publish offer",
            publish_offer.ok_or_else(|| anyhow!("member {index} never sent a publish offer"))?,
        ));

        let mut subscribe_offers = 0;
        while let Ok(sdp) = active.sfu_offers.try_recv() {
            subscribe_offers += 1;
            sources.push(("SFU subscribe offer", sdp.sdp));
        }
        assert!(
            subscribe_offers > 0,
            "member {index} received no SFU subscribe offer"
        );

        assert_bundle_extmap_consistent(&format!("member {index}"), &sources)?;
    }

    for active in &actives {
        active.pc.close().await?;
    }
    Ok(())
}
