//! End-to-end SFU V2 test: three `webrtc` clients join one room, the third join upgrades
//! it from P2P to SFU, and all three publish to the assigned SFU worker over a data channel.
//!
//! This exercises the full upgrade + SFU publish/answer/ICE path headlessly: the room page
//! gate is bypassed (the client POSTs `/v2/join` directly), the third join blocks until the
//! worker has joined all three, and each client's peer connection must connect to the SFU
//! and open its data channel.
mod common;

use anyhow::{Context, Result, anyhow};
use common::sfu_v2::{DriveConfig, Peer, drive, peer, upgrade_three};
use common::wait_for_server;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::PeerConnection;

struct Active {
    pc: Arc<dyn PeerConnection>,
    data_channel: Arc<dyn DataChannel>,
}

#[tokio::test]
async fn upgrades_three_v2_clients_to_sfu_and_opens_data_channels() -> Result<()> {
    wait_for_server().await?;
    let room_id = rand::random::<u64>().max(1);

    let members = upgrade_three(room_id).await?;

    // Each member publishes to the SFU with its own data channel.
    let mut actives: Vec<Active> = Vec::new();
    let mut connections = Vec::new();
    for member in members {
        let Peer {
            pc,
            states,
            outgoing,
            tracks: _,
        } = peer().await?;
        // The data channel is the bootstrap m-line drive() offers to bring the PC up.
        let data_channel = pc.create_data_channel("sfu-v2-test", None).await?;
        // Data-channel-only publish never draws a subscribe offer; sink is unused.
        let (offers_tx, _offers_rx) = mpsc::unbounded_channel();
        let (connected_tx, connected_rx) = oneshot::channel();
        drive(DriveConfig {
            ws: member.ws,
            pc: pc.clone(),
            epoch: "1".to_owned(),
            outgoing,
            seen_offers: offers_tx,
            states,
            connected_tx,
            publish_track: None,
        });
        actives.push(Active { pc, data_channel });
        connections.push(connected_rx);
    }

    // All three peer connections connect to the SFU.
    for (index, connected_rx) in connections.into_iter().enumerate() {
        timeout(Duration::from_secs(30), connected_rx)
            .await
            .with_context(|| format!("member {index} connect timed out"))?
            .with_context(|| format!("member {index} publish task ended"))?
            .with_context(|| format!("member {index} did not connect to the SFU"))?;
    }

    // Each client's data channel to the SFU opens.
    for (index, active) in actives.iter().enumerate() {
        timeout(Duration::from_secs(30), async {
            loop {
                match active.data_channel.poll().await {
                    Some(DataChannelEvent::OnOpen) => return Ok(()),
                    Some(DataChannelEvent::OnClose) | None => {
                        return Err(anyhow!("data channel closed"));
                    }
                    _ => {}
                }
            }
        })
        .await
        .with_context(|| format!("member {index} data channel did not open"))??;
    }

    for active in &actives {
        active.pc.close().await?;
    }
    Ok(())
}
