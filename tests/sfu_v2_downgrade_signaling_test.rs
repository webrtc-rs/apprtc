//! Black-box V2 mode-transition flow through the real AppWeb → signaling → SFU-worker stack:
//! two members run **P2P**, a third join **upgrades** the room to **SFU**, and after that third
//! member leaves and the room dwells at two members it **downgrades** back to direct **P2P**.
//!
//! This exercises the signaling and SDP-exchange envelope of the whole flow — HTTP join,
//! WebSocket registration, opaque offer/answer/candidate relay in the P2P phases, the
//! `sfu-upgrade` control at the upgrade commit, and the `sfu-downgrade` control (with the elected
//! initiator) at the downgrade commit — without real media. Real SFU media forwarding is covered
//! by `sfu_v2_webrtc_media_test`; here the focus is the mode state machine end to end.

mod common;

use anyhow::{Context, Result};
use common::{
    http, http_with_headers, join_v2, wait_for_server, ws_expect_close, ws_receive_json,
    ws_register_v2, ws_send,
};
use serde_json::{Value, json};

fn admission(response: &Value) -> Result<(u64, String)> {
    assert_eq!(response["result"], "SUCCESS", "join failed: {response}");
    let params = &response["params"];
    let client_id = params["client_id"]
        .as_str()
        .context("missing V2 client_id")?
        .parse()?;
    let token = params["admission_token"]
        .as_str()
        .context("missing V2 admission_token")?
        .to_owned();
    Ok((client_id, token))
}

/// Relay one opaque `{cmd:"send"}` payload from `from` and assert `to` receives it verbatim.
async fn relay(
    from: &mut common::WsStream,
    to: &mut common::WsStream,
    epoch: &str,
    payload: &str,
) -> Result<()> {
    ws_send(from, json!({"cmd": "send", "epoch": epoch, "msg": payload})).await?;
    assert_eq!(
        ws_receive_json(to).await?,
        json!({"msg": payload, "error": ""}),
        "relayed payload mismatch"
    );
    Ok(())
}

async fn leave(room_id: u64, client_id: u64, token: &str) -> Result<()> {
    let authorization = format!("Bearer {token}");
    let left = http_with_headers(
        "POST",
        &format!("/v2/leave/{room_id}/{client_id}"),
        &[],
        &[("Authorization", &authorization)],
    )
    .await?;
    assert_eq!(left.json()?["result"], "SUCCESS", "leave failed");
    Ok(())
}

#[tokio::test]
async fn p2p_upgrades_to_sfu_then_downgrades_to_p2p() -> Result<()> {
    wait_for_server().await?;
    let room_id = rand::random::<u64>();

    let page = http("GET", &format!("/v2/r/{room_id}"), &[]).await?;
    assert_eq!(page.status, 200);
    assert!(page.text()?.contains("signalingVersion: 2"));

    // ---- Phase 1: two members negotiate a direct P2P call at epoch 0 ----
    let first = join_v2(room_id).await?;
    assert_eq!(first["params"]["mode"], "p2p");
    assert_eq!(first["params"]["epoch"], "0");
    assert_eq!(first["params"]["is_initiator"], true);
    let (first_id, first_token) = admission(&first)?;
    let (mut first_ws, first_registered) = ws_register_v2(room_id, first_id, &first_token).await?;
    assert_eq!(first_registered["mode"], "p2p");
    assert_eq!(first_registered["is_initiator"], true);

    let second = join_v2(room_id).await?;
    assert_eq!(second["params"]["mode"], "p2p");
    assert_eq!(second["params"]["is_initiator"], false);
    let (second_id, second_token) = admission(&second)?;
    let (mut second_ws, second_registered) =
        ws_register_v2(room_id, second_id, &second_token).await?;
    assert_eq!(second_registered["is_initiator"], false);

    // Initiator offers + trickles a candidate; callee answers. Signaling relays opaquely.
    relay(
        &mut first_ws,
        &mut second_ws,
        "0",
        r#"{"type":"offer","sdp":"v=0\\r\\n"}"#,
    )
    .await?;
    relay(
        &mut first_ws,
        &mut second_ws,
        "0",
        r#"{"type":"candidate","label":0,"id":"0","candidate":"candidate:1"}"#,
    )
    .await?;
    relay(
        &mut second_ws,
        &mut first_ws,
        "0",
        r#"{"type":"answer","sdp":"v=0\\r\\n"}"#,
    )
    .await?;

    // ---- Phase 2: a third join upgrades the room to SFU at epoch 1 ----
    // join_v2 blocks until signaling has driven the worker's JoinMember barrier for all three
    // members and committed SFU mode.
    let third = join_v2(room_id).await?;
    assert_eq!(
        third["params"]["mode"], "sfu",
        "third join must upgrade to SFU: {third}"
    );
    assert_eq!(third["params"]["epoch"], "1");
    let (third_id, third_token) = admission(&third)?;
    let (mut third_ws, third_registered) = ws_register_v2(room_id, third_id, &third_token).await?;
    assert_eq!(third_registered["mode"], "sfu");
    assert_eq!(third_registered["epoch"], "1");

    // The two existing members are pushed the sfu-upgrade control at commit.
    for ws in [&mut first_ws, &mut second_ws] {
        assert_eq!(
            ws_receive_json(ws).await?,
            json!({
                "control": "sfu-upgrade",
                "roomid": room_id.to_string(),
                "epoch": "1",
            })
        );
    }

    // ---- Phase 3: the third member leaves; the room dwells at two and downgrades to P2P ----
    leave(room_id, third_id, &third_token).await?;
    ws_expect_close(&mut third_ws).await?;

    // signaling elects the lower client id as the direct offerer. The dwell (default 2s) is well
    // within the common receive helper's 5s per-frame timeout.
    let initiator_id = first_id.min(second_id);
    for (id, ws) in [(first_id, &mut first_ws), (second_id, &mut second_ws)] {
        let control = ws_receive_json(ws).await?;
        assert_eq!(
            control,
            json!({
                "control": "sfu-downgrade",
                "roomid": room_id.to_string(),
                "epoch": "2",
                "is_initiator": id == initiator_id,
            }),
            "unexpected downgrade control for client {id}"
        );
    }

    // ---- Phase 4: the two members renegotiate a direct P2P call at the new epoch 2 ----
    let (mut offerer_ws, mut answerer_ws) = if initiator_id == first_id {
        (first_ws, second_ws)
    } else {
        (second_ws, first_ws)
    };
    relay(
        &mut offerer_ws,
        &mut answerer_ws,
        "2",
        r#"{"type":"offer","sdp":"v=0\\r\\n"}"#,
    )
    .await?;
    relay(
        &mut answerer_ws,
        &mut offerer_ws,
        "2",
        r#"{"type":"answer","sdp":"v=0\\r\\n"}"#,
    )
    .await?;

    // Both members leave, closing their sockets.
    let (offerer_id, offerer_token, answerer_id, answerer_token) = if initiator_id == first_id {
        (first_id, first_token, second_id, second_token)
    } else {
        (second_id, second_token, first_id, first_token)
    };
    leave(room_id, offerer_id, &offerer_token).await?;
    ws_expect_close(&mut offerer_ws).await?;
    leave(room_id, answerer_id, &answerer_token).await?;
    ws_expect_close(&mut answerer_ws).await?;
    Ok(())
}
