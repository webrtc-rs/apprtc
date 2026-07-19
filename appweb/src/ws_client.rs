//! AppWeb's client for the signaling room authority.
//!
//! The initial supported deployment is all-in-one, so this adapter calls the
//! Collider owner task directly. Its operation/result boundary is transport
//! independent and can later be carried by the control WebSocket unchanged.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async, connect_async_tls_with_config,
    tungstenite::Message,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub is_initiator: bool,
    pub messages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StatusSnapshot {
    pub rooms: usize,
    pub clients: usize,
    pub websocket_connections: usize,
    pub total_websocket_connections: u64,
    pub websocket_errors: u64,
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
        Self::connect_with_options(url, appid, token, false).await
    }

    pub async fn connect_with_options(
        url: &str,
        appid: &str,
        token: &str,
        insecure_tls: bool,
    ) -> Result<Self, String> {
        let (mut socket, _) = if insecure_tls && url.starts_with("wss://") {
            let config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(NoCertificateVerification))
                .with_no_client_auth();
            connect_async_tls_with_config(
                url,
                None,
                false,
                Some(Connector::Rustls(std::sync::Arc::new(config))),
            )
            .await
        } else {
            connect_async(url).await
        }
        .map_err(|e| e.to_string())?;
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

#[derive(Debug)]
struct NoCertificateVerification;
impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
            .to_vec()
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
