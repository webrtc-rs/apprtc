//! AppWeb's client for the signaling room authority.
//!
//! The initial supported deployment is all-in-one, so this adapter calls the
//! Collider owner task directly. Its operation/result boundary is transport
//! independent and can later be carried by the control WebSocket unchanged.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use signaling::collider::{AuthorityOperation, AuthorityResult, StatusSnapshot};
use signaling::ws_server::ColliderHandle;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub is_initiator: bool,
    pub messages: Vec<String>,
}

#[derive(Clone)]
pub struct WsClient {
    collider: ColliderHandle,
}

impl WsClient {
    pub fn new(collider: ColliderHandle) -> Self {
        Self { collider }
    }

    pub async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String> {
        match self
            .collider
            .request(AuthorityOperation::Admit {
                roomid,
                clientid,
                is_loopback,
                now: Instant::now(),
            })
            .await?
        {
            AuthorityResult::Admitted {
                is_initiator,
                messages,
            } => Ok(Admission {
                is_initiator,
                messages,
            }),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to admit".to_string()),
        }
    }

    pub async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        match self
            .collider
            .request(AuthorityOperation::Remove { roomid, clientid })
            .await?
        {
            AuthorityResult::Removed => Ok(()),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to remove".to_string()),
        }
    }

    pub async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        match self
            .collider
            .request(AuthorityOperation::Occupancy { roomid })
            .await?
        {
            AuthorityResult::Occupancy { count } => Ok(count),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to occupancy".to_string()),
        }
    }

    pub async fn inject(
        &self,
        roomid: String,
        clientid: String,
        msg: String,
    ) -> Result<(), String> {
        match self
            .collider
            .request(AuthorityOperation::Inject {
                roomid,
                clientid,
                msg,
                now: Instant::now(),
            })
            .await?
        {
            AuthorityResult::Injected => Ok(()),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to inject".to_string()),
        }
    }

    pub async fn status(&self) -> Result<StatusSnapshot, String> {
        match self.collider.request(AuthorityOperation::Status).await? {
            AuthorityResult::Status(status) => Ok(status),
            AuthorityResult::Error { result } => Err(result),
            _ => Err("unexpected signaling authority response to status".to_string()),
        }
    }
}

#[async_trait]
pub trait RoomAuthority: Send + Sync {
    async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String>;
    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String>;
    async fn occupancy(&self, roomid: String) -> Result<usize, String>;
    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String>;
    async fn status(&self) -> Result<StatusSnapshot, String>;
}

#[async_trait]
impl RoomAuthority for WsClient {
    async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String> {
        self.admit(roomid, clientid, is_loopback).await
    }
    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        self.remove(roomid, clientid).await
    }
    async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        self.occupancy(roomid).await
    }
    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String> {
        self.inject(roomid, clientid, msg).await
    }
    async fn status(&self) -> Result<StatusSnapshot, String> {
        self.status().await
    }
}

#[derive(Clone)]
pub struct WebSocketAuthority {
    socket: std::sync::Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    next_req: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Deserialize)]
struct ControlReply {
    reply: String,
    req: u64,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    is_initiator: Option<bool>,
    #[serde(default)]
    messages: Option<Vec<String>>,
    #[serde(default)]
    count: Option<usize>,
    #[serde(default)]
    rooms: Option<usize>,
    #[serde(default)]
    clients: Option<usize>,
    #[serde(default)]
    websocket_connections: Option<usize>,
    #[serde(default)]
    total_websocket_connections: Option<u64>,
    #[serde(default)]
    websocket_errors: Option<u64>,
}

impl WebSocketAuthority {
    pub async fn connect(url: &str, appid: &str, token: &str) -> Result<Self, String> {
        let (mut socket, _) = connect_async(url).await.map_err(|e| e.to_string())?;
        socket
            .send(Message::Text(
                serde_json::json!({"cmd":"app","appid":appid,"token":token})
                    .to_string()
                    .into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
        let Some(Ok(Message::Text(frame))) = socket.next().await else {
            return Err("signaling control socket closed during registration".into());
        };
        if serde_json::from_str::<serde_json::Value>(&frame)
            .ok()
            .and_then(|v| v.get("control").and_then(|v| v.as_str()).map(str::to_owned))
            .as_deref()
            != Some("registered")
        {
            return Err(format!("signaling control registration failed: {frame}"));
        }
        Ok(Self {
            socket: std::sync::Arc::new(Mutex::new(socket)),
            next_req: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
        })
    }

    async fn request(&self, value: serde_json::Value) -> Result<ControlReply, String> {
        let req = value["req"].as_u64().unwrap_or_default();
        let mut socket = self.socket.lock().await;
        socket
            .send(Message::Text(value.to_string().into()))
            .await
            .map_err(|e| e.to_string())?;
        loop {
            let Some(frame) = socket.next().await else {
                return Err("signaling control socket closed".into());
            };
            let Message::Text(text) = frame.map_err(|e| e.to_string())? else {
                continue;
            };
            let reply: ControlReply = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if reply.req == req {
                return Ok(reply);
            }
        }
    }
    fn req(&self) -> u64 {
        self.next_req
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl RoomAuthority for WebSocketAuthority {
    async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String> {
        let r = self.request(serde_json::json!({"cmd":"admit","req":self.req(),"roomid":roomid,"clientid":clientid,"is_loopback":is_loopback})).await?;
        if r.reply == "error" {
            return Err(r.result.unwrap_or_else(|| "admission failed".into()));
        }
        Ok(Admission {
            is_initiator: r.is_initiator.unwrap_or(false),
            messages: r.messages.unwrap_or_default(),
        })
    }
    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        let r = self.request(serde_json::json!({"cmd":"remove","req":self.req(),"roomid":roomid,"clientid":clientid})).await?;
        if r.reply == "error" {
            Err(r.result.unwrap_or_else(|| "remove failed".into()))
        } else {
            Ok(())
        }
    }
    async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        let r = self
            .request(serde_json::json!({"cmd":"occupancy","req":self.req(),"roomid":roomid}))
            .await?;
        r.count
            .ok_or_else(|| r.result.unwrap_or_else(|| "occupancy failed".into()))
    }
    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String> {
        let r = self.request(serde_json::json!({"cmd":"inject","req":self.req(),"roomid":roomid,"clientid":clientid,"msg":msg})).await?;
        if r.reply == "error" {
            Err(r.result.unwrap_or_else(|| "inject failed".into()))
        } else {
            Ok(())
        }
    }
    async fn status(&self) -> Result<StatusSnapshot, String> {
        let r = self
            .request(serde_json::json!({"cmd":"status","req":self.req()}))
            .await?;
        Ok(StatusSnapshot {
            rooms: r.rooms.unwrap_or(0),
            clients: r.clients.unwrap_or(0),
            websocket_connections: r.websocket_connections.unwrap_or(0),
            total_websocket_connections: r.total_websocket_connections.unwrap_or(0),
            websocket_errors: r.websocket_errors.unwrap_or(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn client() -> WsClient {
        WsClient::new(ColliderHandle::spawn(Duration::from_secs(10)))
    }

    #[tokio::test]
    async fn admit_inject_and_occupancy_round_trip() {
        let authority = client();
        let first = authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap();
        assert!(first.is_initiator);
        assert!(first.messages.is_empty());
        authority
            .inject("room".into(), "client-a".into(), "offer".into())
            .await
            .unwrap();
        assert_eq!(authority.occupancy("room".into()).await.unwrap(), 1);
        let second = authority
            .admit("room".into(), "client-b".into(), false)
            .await
            .unwrap();
        assert!(!second.is_initiator);
        assert_eq!(second.messages, vec!["offer"]);
    }

    #[tokio::test]
    async fn remove_releases_capacity_and_status_is_available() {
        let authority = client();
        authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap();
        authority
            .remove("room".into(), "client-a".into())
            .await
            .unwrap();
        assert_eq!(authority.occupancy("room".into()).await.unwrap(), 0);
        let status = authority.status().await.unwrap();
        assert!(status.rooms <= 1);
        assert!(status.total_websocket_connections <= 1);
    }

    #[tokio::test]
    async fn duplicate_admission_returns_authority_error() {
        let authority = client();
        authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap();
        let error = authority
            .admit("room".into(), "client-a".into(), false)
            .await
            .unwrap_err();
        assert_eq!(error, "DUPLICATE_CLIENT");
    }

    #[tokio::test]
    async fn all_operations_report_transport_error_after_collider_shutdown() {
        let authority = client();
        authority.collider.shutdown().await.unwrap();
        assert!(
            authority
                .admit("room".into(), "client".into(), false)
                .await
                .is_err()
        );
        assert!(
            authority
                .remove("room".into(), "client".into())
                .await
                .is_err()
        );
        assert!(authority.occupancy("room".into()).await.is_err());
        assert!(
            authority
                .inject("room".into(), "client".into(), "msg".into())
                .await
                .is_err()
        );
        assert!(authority.status().await.is_err());
    }
}
