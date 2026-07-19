//! Reconnecting AppWeb control client for the signaling room authority.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
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
    requests: mpsc::Sender<AuthorityRequest>,
    next_req: Arc<AtomicU64>,
}

type ControlSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const REQUEST_QUEUE_CAPACITY: usize = 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);
const RECONNECT_MIN_DELAY: Duration = Duration::from_secs(1);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

struct AuthorityRequest {
    value: serde_json::Value,
    reply: oneshot::Sender<Result<ControlReply, String>>,
}

#[derive(Clone)]
struct ConnectionOptions {
    url: String,
    appid: String,
    token: String,
    insecure_tls: bool,
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
        let options = ConnectionOptions {
            url: url.to_owned(),
            appid: appid.to_owned(),
            token: token.to_owned(),
            insecure_tls,
        };
        let socket = tokio::time::timeout(CONNECT_TIMEOUT, connect_control(&options, 0))
            .await
            .map_err(|_| "initial signaling control connection timed out".to_string())??;
        let (requests, receiver) = mpsc::channel(REQUEST_QUEUE_CAPACITY);
        tokio::spawn(run_connection(options, socket, receiver));
        Ok(Self {
            requests,
            next_req: Arc::new(AtomicU64::new(1)),
        })
    }

    async fn request(&self, value: serde_json::Value) -> Result<ControlReply, String> {
        let req = value["req"].as_u64().unwrap_or_default();
        let operation = value["cmd"].as_str().unwrap_or("unknown").to_owned();
        let (reply, response) = oneshot::channel();
        self.requests
            .send(AuthorityRequest { value, reply })
            .await
            .map_err(|_| "signaling control worker stopped".to_string())?;
        let started = Instant::now();
        let result = tokio::time::timeout(REQUEST_TIMEOUT, response)
            .await
            .map_err(|_| format!("signaling {operation} request {req} timed out"))?
            .map_err(|_| "signaling control worker stopped".to_string())?;
        match &result {
            Ok(_) => log::debug!(
                "Signaling control request completed: operation={operation} request_id={req} elapsed_ms={}",
                started.elapsed().as_millis()
            ),
            Err(error) => log::warn!(
                "Signaling control request failed: operation={operation} request_id={req} elapsed_ms={} error={error}",
                started.elapsed().as_millis()
            ),
        }
        result
    }
    fn req(&self) -> u64 {
        self.next_req.fetch_add(1, Ordering::Relaxed)
    }
}

async fn open_socket(options: &ConnectionOptions) -> Result<ControlSocket, String> {
    let (socket, _) = if options.insecure_tls && options.url.starts_with("wss://") {
        let config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(NoCertificateVerification))
            .with_no_client_auth();
        connect_async_tls_with_config(
            &options.url,
            None,
            false,
            Some(Connector::Rustls(std::sync::Arc::new(config))),
        )
        .await
    } else {
        connect_async(&options.url).await
    }
    .map_err(|error| error.to_string())?;
    Ok(socket)
}

async fn connect_control(
    options: &ConnectionOptions,
    attempt: u32,
) -> Result<ControlSocket, String> {
    log::info!(
        "Connecting signaling control WebSocket: url={} appid={} attempt={} insecure_tls={}",
        options.url,
        options.appid,
        attempt,
        options.insecure_tls
    );
    let started = Instant::now();
    let mut socket = open_socket(options).await?;
    socket
        .send(Message::Text(
            serde_json::json!({"cmd":"app","appid":options.appid,"token":options.token})
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
    log::info!(
        "Signaling control WebSocket registered: url={} appid={} attempt={} elapsed_ms={}",
        options.url,
        options.appid,
        attempt,
        started.elapsed().as_millis()
    );
    Ok(socket)
}

async fn run_connection(
    options: ConnectionOptions,
    mut socket: ControlSocket,
    mut requests: mpsc::Receiver<AuthorityRequest>,
) {
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut reconnect_attempt = 0_u32;
    let mut heartbeat_sequence = 0_u64;

    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else {
                    log::info!("Signaling control worker stopped: appid={}", options.appid);
                    let _ = socket.close(None).await;
                    return;
                };
                if request.reply.is_closed() {
                    continue;
                }
                let request_id = request.value["req"].as_u64().unwrap_or_default();
                let outcome = match tokio::time::timeout(HEARTBEAT_TIMEOUT, exchange(&mut socket, request.value, request_id)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(format!("signaling control request {request_id} timed out waiting for reply")),
                };
                let failed = outcome.is_err();
                let _ = request.reply.send(outcome);
                if failed {
                    log::warn!("Signaling control connection lost during request: appid={} request_id={request_id}", options.appid);
                    socket = reconnect(&options, &mut reconnect_attempt).await;
                    heartbeat.reset();
                }
            }
            _ = heartbeat.tick() => {
                heartbeat_sequence = heartbeat_sequence.wrapping_add(1);
                let sequence = heartbeat_sequence;
                let started = Instant::now();
                let heartbeat_result = async {
                    socket.send(Message::Ping(sequence.to_be_bytes().to_vec().into())).await.map_err(|error| error.to_string())?;
                    loop {
                        let frame = socket.next().await.ok_or_else(|| "signaling control socket closed".to_string())?.map_err(|error| error.to_string())?;
                        match frame {
                            Message::Pong(_) => return Ok::<(), String>(()),
                            Message::Ping(payload) => socket.send(Message::Pong(payload)).await.map_err(|error| error.to_string())?,
                            Message::Close(_) => return Err("signaling control socket closed".into()),
                            _ => {}
                        }
                    }
                };
                match tokio::time::timeout(HEARTBEAT_TIMEOUT, heartbeat_result).await {
                    Ok(Ok(())) => log::debug!("Signaling control heartbeat succeeded: appid={} latency_ms={}", options.appid, started.elapsed().as_millis()),
                    Ok(Err(error)) => {
                        log::warn!("Signaling control heartbeat failed: appid={} error={error}", options.appid);
                        socket = reconnect(&options, &mut reconnect_attempt).await;
                        heartbeat.reset();
                    }
                    Err(_) => {
                        log::warn!("Signaling control heartbeat timed out: appid={} timeout_secs={}", options.appid, HEARTBEAT_TIMEOUT.as_secs());
                        socket = reconnect(&options, &mut reconnect_attempt).await;
                        heartbeat.reset();
                    }
                }
            }
        }
    }
}

async fn exchange(
    socket: &mut ControlSocket,
    value: serde_json::Value,
    request_id: u64,
) -> Result<ControlReply, String> {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let frame = socket
            .next()
            .await
            .ok_or_else(|| "signaling control socket closed".to_string())?
            .map_err(|error| error.to_string())?;
        match frame {
            Message::Text(text) => {
                let reply: ControlReply =
                    serde_json::from_str(&text).map_err(|error| error.to_string())?;
                if reply.req == request_id {
                    return Ok(reply);
                }
            }
            Message::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|error| error.to_string())?,
            Message::Close(_) => return Err("signaling control socket closed".into()),
            _ => {}
        }
    }
}

async fn reconnect(options: &ConnectionOptions, attempt: &mut u32) -> ControlSocket {
    loop {
        *attempt = attempt.saturating_add(1);
        let exponent = (*attempt).saturating_sub(1).min(5);
        let base_delay = (RECONNECT_MIN_DELAY * 2_u32.pow(exponent)).min(RECONNECT_MAX_DELAY);
        let jitter = Duration::from_millis(rand::random::<u64>() % 250);
        let delay = (base_delay + jitter).min(RECONNECT_MAX_DELAY);
        log::info!(
            "Signaling control reconnect scheduled: appid={} attempt={} delay_ms={}",
            options.appid,
            *attempt,
            delay.as_millis()
        );
        tokio::time::sleep(delay).await;
        match tokio::time::timeout(CONNECT_TIMEOUT, connect_control(options, *attempt)).await {
            Err(_) => log::warn!(
                "Signaling control reconnect timed out: appid={} attempt={} timeout_secs={}",
                options.appid,
                *attempt,
                CONNECT_TIMEOUT.as_secs()
            ),
            Ok(Ok(socket)) => {
                log::info!(
                    "Signaling control reconnected: appid={} attempt={}",
                    options.appid,
                    *attempt
                );
                *attempt = 0;
                return socket;
            }
            Ok(Err(error)) => log::warn!(
                "Signaling control reconnect failed: appid={} attempt={} error={error}",
                options.appid,
                *attempt
            ),
        }
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
