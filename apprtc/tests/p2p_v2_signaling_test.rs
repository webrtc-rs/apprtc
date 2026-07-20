//! Black-box P2P V2 AppWeb/gRPC/WebSocket signaling flow.

mod common;

use anyhow::{Context, Result};
use common::{
    http, http_with_headers, join_v2, wait_for_server, ws_expect_close, ws_receive_json,
    ws_register_v2, ws_send,
};
use serde_json::{Value, json};

fn admission(response: &Value) -> Result<(u64, &str)> {
    assert_eq!(response["result"], "SUCCESS");
    let params = &response["params"];
    let client_id = params["client_id"]
        .as_str()
        .context("missing V2 client_id")?
        .parse()?;
    let token = params["admission_token"]
        .as_str()
        .context("missing V2 admission_token")?;
    Ok((client_id, token))
}

#[tokio::test]
async fn completes_p2p_v2_http_websocket_relay_and_leave_flow() -> Result<()> {
    wait_for_server().await?;
    let room_id = rand::random::<u64>();

    let page = http("GET", &format!("/v2/r/{room_id}"), &[]).await?;
    assert_eq!(page.status, 200);
    assert!(page.text()?.contains("signalingVersion: 2"));

    let first = join_v2(room_id).await?;
    let (first_id, first_token) = admission(&first)?;
    assert_eq!(first["params"]["mode"], "p2p");
    assert_eq!(first["params"]["epoch"], "0");
    assert_eq!(first["params"]["is_initiator"], true);
    let (mut first_ws, first_registered) = ws_register_v2(room_id, first_id, first_token).await?;
    assert_eq!(
        first_registered,
        json!({
            "control": "registered",
            "roomid": room_id.to_string(),
            "epoch": "0",
            "mode": "p2p",
            "is_initiator": true,
        })
    );

    let second = join_v2(room_id).await?;
    let (second_id, second_token) = admission(&second)?;
    assert_eq!(second["params"]["is_initiator"], false);
    let (mut second_ws, second_registered) =
        ws_register_v2(room_id, second_id, second_token).await?;
    assert_eq!(second_registered["control"], "registered");
    assert_eq!(second_registered["is_initiator"], false);

    for payload in [
        r#"{"type":"offer","sdp":"v=0\\r\\n"}"#,
        r#"{"type":"candidate","label":0,"id":"0","candidate":"candidate:1"}"#,
    ] {
        ws_send(
            &mut first_ws,
            json!({"cmd": "send", "epoch": "0", "msg": payload}),
        )
        .await?;
        assert_eq!(
            ws_receive_json(&mut second_ws).await?,
            json!({"msg": payload, "error": ""})
        );
    }
    let answer = r#"{"type":"answer","sdp":"v=0\\r\\n"}"#;
    ws_send(
        &mut second_ws,
        json!({"cmd": "send", "epoch": "0", "msg": answer}),
    )
    .await?;
    assert_eq!(
        ws_receive_json(&mut first_ws).await?,
        json!({"msg": answer, "error": ""})
    );

    let authorization = format!("Bearer {second_token}");
    let left = http_with_headers(
        "POST",
        &format!("/v2/leave/{room_id}/{second_id}"),
        &[],
        &[("Authorization", &authorization)],
    )
    .await?;
    assert_eq!(left.json()?["result"], "SUCCESS");
    ws_expect_close(&mut second_ws).await?;
    assert_eq!(
        ws_receive_json(&mut first_ws).await?,
        json!({
            "control": "p2p-promote",
            "roomid": room_id.to_string(),
            "epoch": "0",
            "is_initiator": true,
        })
    );

    let authorization = format!("Bearer {first_token}");
    let left = http_with_headers(
        "POST",
        &format!("/v2/leave/{room_id}/{first_id}"),
        &[],
        &[("Authorization", &authorization)],
    )
    .await?;
    assert_eq!(left.json()?["result"], "SUCCESS");
    ws_expect_close(&mut first_ws).await?;
    Ok(())
}
