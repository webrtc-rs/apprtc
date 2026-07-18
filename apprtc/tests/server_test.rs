//! Black-box compatibility tests against a separately running AppRTC server.
//!
//! Start the server before running this test target:
//!
//! ```text
//! cargo run -p apprtc -- --host 127.0.0.1 --port 8080 --web-root appweb --tls &
//! cargo test -p apprtc --test '*'
//! ```

mod common;

use anyhow::{Context, Result};
use common::{
    http, join, unique_room, wait_for_server, ws_connect, ws_expect_close, ws_receive_json,
    ws_register, ws_send,
};
use serde_json::{Value, json};

#[tokio::test]
async fn serves_pages_configuration_static_assets_and_status() -> Result<()> {
    wait_for_server().await?;

    let root = http("GET", "/", &[]).await?;
    assert_eq!(root.status, 200);
    assert!(root.text()?.contains("AppRTC"));
    assert!(
        root.headers
            .get("content-type")
            .is_some_and(|value| value.starts_with("text/html"))
    );

    let params = http("GET", "/params", &[]).await?;
    assert_eq!(params.status, 200);
    let params = params.json()?;
    assert_eq!(params["wss_url"], "wss://127.0.0.1:8080/ws");
    assert_eq!(params["wss_post_url"], "https://127.0.0.1:8080");

    let ice = http("POST", "/v1alpha/iceconfig", &[]).await?;
    assert_eq!(ice.status, 200);
    assert!(ice.json()?["iceServers"].is_array());

    let script = http("GET", "/js/call.js", &[]).await?;
    assert_eq!(script.status, 200);
    assert!(script.text()?.contains("Call.prototype"));

    let status = http("GET", "/status", &[]).await?;
    assert_eq!(status.status, 200);
    let status = status.json()?;
    for field in ["upsec", "openws", "totalws", "wserrors", "httperrors"] {
        assert!(status.get(field).is_some(), "missing status field {field}");
    }
    Ok(())
}

#[tokio::test]
async fn completes_the_stock_v1_join_queue_and_websocket_relay_flow() -> Result<()> {
    wait_for_server().await?;
    let room = unique_room("stock-flow");

    let first = join(&room).await?;
    assert_eq!(first["result"], "SUCCESS");
    assert_eq!(first["params"]["room_id"], room);
    assert_eq!(first["params"]["is_initiator"], "true");
    let first_id = client_id(&first)?;
    assert_eq!(first_id.len(), 8);
    assert!(first_id.bytes().all(|byte| byte.is_ascii_digit()));
    let mut first_ws = ws_register(&room, first_id).await?;

    let offer = r#"{"type":"offer","sdp":"v=0\\r\\n"}"#;
    let candidate = r#"{"type":"candidate","label":0,"id":"0","candidate":"candidate:1"}"#;
    for payload in [offer, candidate] {
        let response = http(
            "POST",
            &format!("/message/{room}/{first_id}"),
            payload.as_bytes(),
        )
        .await?;
        assert_eq!(response.status, 200);
        assert_eq!(response.json()?["result"], "SUCCESS");
    }

    let second = join(&room).await?;
    assert_eq!(second["result"], "SUCCESS");
    assert_eq!(second["params"]["is_initiator"], "false");
    assert_eq!(second["params"]["messages"], json!([offer, candidate]));
    let second_id = client_id(&second)?;
    let mut second_ws = ws_register(&room, second_id).await?;

    let answer = r#"{"type":"answer","sdp":"v=0\\r\\n"}"#;
    ws_send(&mut second_ws, json!({ "cmd": "send", "msg": answer })).await?;
    assert_eq!(
        ws_receive_json(&mut first_ws).await?,
        json!({ "msg": answer, "error": "" })
    );

    let return_candidate = r#"{"type":"candidate","label":0,"id":"0","candidate":"candidate:2"}"#;
    ws_send(
        &mut first_ws,
        json!({ "cmd": "send", "msg": return_candidate }),
    )
    .await?;
    assert_eq!(
        ws_receive_json(&mut second_ws).await?,
        json!({ "msg": return_candidate, "error": "" })
    );

    let third = join(&room).await?;
    assert_eq!(third["result"], "FULL");

    let leave = http("POST", &format!("/leave/{room}/{first_id}"), &[]).await?;
    assert_eq!(leave.status, 200);
    assert!(leave.body.is_empty());
    ws_expect_close(&mut first_ws).await?;

    let replacement = join(&room).await?;
    assert_eq!(replacement["result"], "SUCCESS");
    assert_eq!(replacement["params"]["is_initiator"], "false");
    let replacement_id = client_id(&replacement)?;

    cleanup(&room, second_id).await?;
    cleanup(&room, replacement_id).await?;
    second_ws.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn supports_root_and_internal_fallback_routes() -> Result<()> {
    wait_for_server().await?;
    let room = unique_room("fallback");
    let first = join(&room).await?;
    let first_id = client_id(&first)?;
    let second = join(&room).await?;
    let second_id = client_id(&second)?;
    let mut first_ws = ws_register(&room, first_id).await?;
    let mut second_ws = ws_register(&room, second_id).await?;

    let fallback = r#"{"type":"candidate","candidate":"fallback"}"#;
    let response = http("POST", &format!("/{room}/{first_id}"), fallback.as_bytes()).await?;
    assert_eq!(response.status, 200);
    assert_eq!(response.text()?, "OK\n");
    assert_eq!(
        ws_receive_json(&mut second_ws).await?,
        json!({ "msg": fallback, "error": "" })
    );

    let deleted = http("DELETE", &format!("/{room}/{first_id}"), &[]).await?;
    assert_eq!(deleted.status, 200);
    assert_eq!(deleted.text()?, "OK\n");
    ws_expect_close(&mut first_ws).await?;

    let queued = r#"{"type":"offer","sdp":"internal-alias"}"#;
    let response = http(
        "POST",
        &format!("/_internal/{room}/{second_id}"),
        queued.as_bytes(),
    )
    .await?;
    assert_eq!(response.status, 200);
    let replacement = join(&room).await?;
    assert_eq!(replacement["params"]["messages"], json!([queued]));
    let replacement_id = client_id(&replacement)?;

    let empty = http("POST", &format!("/{room}/{second_id}"), &[]).await?;
    assert_eq!(empty.status, 500);

    cleanup(&room, second_id).await?;
    cleanup(&room, replacement_id).await?;
    second_ws.close(None).await?;
    Ok(())
}

#[tokio::test]
async fn preserves_v1_websocket_errors_and_duplicate_registration_rules() -> Result<()> {
    wait_for_server().await?;

    let mut unregistered = ws_connect().await?;
    ws_send(&mut unregistered, json!({ "cmd": "send", "msg": "offer" })).await?;
    assert_eq!(
        ws_receive_json(&mut unregistered).await?,
        json!({ "msg": "", "error": "Client not registered" })
    );
    ws_expect_close(&mut unregistered).await?;

    let room = unique_room("duplicate");
    let first = join(&room).await?;
    let first_id = client_id(&first)?;
    let mut original = ws_register(&room, first_id).await?;
    let mut duplicate = ws_connect().await?;
    ws_send(
        &mut duplicate,
        json!({ "cmd": "register", "roomid": room, "clientid": first_id }),
    )
    .await?;
    assert_eq!(
        ws_receive_json(&mut duplicate).await?,
        json!({ "msg": "", "error": "Duplicated registration" })
    );
    ws_expect_close(&mut duplicate).await?;

    let second = join(&room).await?;
    let second_id = client_id(&second)?;
    let mut second_ws = ws_register(&room, second_id).await?;
    ws_send(
        &mut second_ws,
        json!({ "cmd": "send", "msg": "still-bound" }),
    )
    .await?;
    assert_eq!(
        ws_receive_json(&mut original).await?,
        json!({ "msg": "still-bound", "error": "" })
    );

    cleanup(&room, first_id).await?;
    cleanup(&room, second_id).await?;
    original.close(None).await?;
    second_ws.close(None).await?;
    Ok(())
}

fn client_id(response: &Value) -> Result<&str> {
    response["params"]["client_id"]
        .as_str()
        .context("join response has no client_id")
}

async fn cleanup(room_id: &str, client_id: &str) -> Result<()> {
    let response = http("POST", &format!("/leave/{room_id}/{client_id}"), &[]).await?;
    if response.status != 200 {
        anyhow::bail!("cleanup leave returned HTTP {}", response.status);
    }
    Ok(())
}
