//! Reconnecting AppWeb control client for the signaling room authority.
//!
//! The wire format is *shared* with the signaling authority:
//! [`AppControlRequest`]/[`AppControlResponse`] live in the Sans-I/O `signaling` crate
//! and are used by both ends of the control WebSocket, so the protocol is defined once.
//! The decision logic of this client — reply correlation, registration acknowledgement,
//! frame classification, backoff schedule, and reply→domain conversions — is pure
//! functions below, unit-tested without sockets or a clock; the async machinery only
//! moves frames.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use signaling::messages::{AppControlRequest, AppControlResponse};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream, connect_async, connect_async_tls_with_config,
    tungstenite::{Bytes, Message},
};

pub use signaling::collider::StatusSnapshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub is_initiator: bool,
    pub messages: Vec<String>,
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
const RECONNECT_JITTER_MS: u64 = 250;

struct AuthorityRequest {
    request: AppControlRequest,
    reply: oneshot::Sender<Result<AppControlResponse, String>>,
}

#[derive(Clone)]
struct ConnectionOptions {
    url: String,
    appid: String,
    token: String,
    insecure_tls: bool,
}

// ─────────────────────────────── pure decision logic ───────────────────────────────

/// What one control-channel frame means to whoever is waiting on the socket.
#[derive(Debug, PartialEq)]
enum FrameAction {
    /// A JSON text payload — a control reply, for [`correlate_reply`].
    Text(String),
    /// The peer acknowledged our ping (any pong counts as the heartbeat ack).
    Pong,
    /// The peer pinged us; answer with this payload.
    ReplyPing(Bytes),
    /// The peer closed the socket.
    Closed,
    /// Frame types with no meaning on the control channel.
    Ignore,
}

fn classify_frame(frame: Message) -> FrameAction {
    match frame {
        Message::Text(text) => FrameAction::Text(text.to_string()),
        Message::Pong(_) => FrameAction::Pong,
        Message::Ping(payload) => FrameAction::ReplyPing(payload),
        Message::Close(_) => FrameAction::Closed,
        Message::Binary(_) | Message::Frame(_) => FrameAction::Ignore,
    }
}

/// Decode a control reply and match it against the outstanding request.
/// `Ok(Some)` is our reply, `Ok(None)` is a stale reply for an earlier request
/// (keep waiting), `Err` is an undecodable frame, which fails the request.
fn correlate_reply(text: &str, request_id: u64) -> Result<Option<AppControlResponse>, String> {
    let reply = AppControlResponse::from_wire(text)?;
    Ok((reply.req == request_id).then_some(reply))
}

/// The reply to the `app` registration frame must be `{"control":"registered"}`.
fn registration_ack(frame: &str) -> Result<(), String> {
    let registered = serde_json::from_str::<serde_json::Value>(frame)
        .ok()
        .and_then(|value| value.get("control").and_then(|v| v.as_str()).map(str::to_owned))
        .as_deref()
        == Some("registered");
    if registered {
        Ok(())
    } else {
        Err(format!("signaling control registration failed: {frame}"))
    }
}

/// Exponential backoff for control reconnects: 1 s doubling per attempt to a 30 s
/// cap (the exponent saturates at attempt 6), plus caller-supplied jitter, with the
/// jittered total never exceeding the cap.
fn reconnect_delay(attempt: u32, jitter: Duration) -> Duration {
    let exponent = attempt.saturating_sub(1).min(5);
    let base_delay = (RECONNECT_MIN_DELAY * 2_u32.pow(exponent)).min(RECONNECT_MAX_DELAY);
    (base_delay + jitter).min(RECONNECT_MAX_DELAY)
}

/// An `error` reply carries its message in `result`; `fallback` covers a malformed
/// error reply that omitted it.
fn expect_ack(reply: AppControlResponse, fallback: &str) -> Result<AppControlResponse, String> {
    if reply.reply == "error" {
        Err(reply.result.unwrap_or_else(|| fallback.to_string()))
    } else {
        Ok(reply)
    }
}

fn admission_from(reply: AppControlResponse) -> Result<Admission, String> {
    let reply = expect_ack(reply, "admission failed")?;
    Ok(Admission {
        is_initiator: reply.is_initiator.unwrap_or(false),
        messages: reply.messages.unwrap_or_default(),
    })
}

fn occupancy_from(mut reply: AppControlResponse) -> Result<usize, String> {
    match reply.count {
        Some(count) => Ok(count),
        None => Err(reply
            .result
            .take()
            .unwrap_or_else(|| "occupancy failed".into())),
    }
}

fn snapshot_from(reply: AppControlResponse) -> StatusSnapshot {
    StatusSnapshot {
        rooms: reply.rooms.unwrap_or(0),
        clients: reply.clients.unwrap_or(0),
        websocket_connections: reply.websocket_connections.unwrap_or(0),
        total_websocket_connections: reply.total_websocket_connections.unwrap_or(0),
        websocket_errors: reply.websocket_errors.unwrap_or(0),
    }
}

// ───────────────────────────────── async machinery ─────────────────────────────────

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

    async fn request(&self, request: AppControlRequest) -> Result<AppControlResponse, String> {
        let req = request.req;
        let operation = request.cmd.clone();
        let (reply, response) = oneshot::channel();
        self.requests
            .send(AuthorityRequest { request, reply })
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
    let register = AppControlRequest::register(options.appid.clone(), options.token.clone());
    socket
        .send(Message::text(register.to_wire()))
        .await
        .map_err(|e| e.to_string())?;
    let Some(Ok(Message::Text(frame))) = socket.next().await else {
        return Err("signaling control socket closed during registration".into());
    };
    registration_ack(&frame)?;
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
                let request_id = request.request.req;
                let outcome = match tokio::time::timeout(HEARTBEAT_TIMEOUT, exchange(&mut socket, request.request, request_id)).await {
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
                        match classify_frame(frame) {
                            FrameAction::Pong => return Ok::<(), String>(()),
                            FrameAction::ReplyPing(payload) => socket.send(Message::Pong(payload)).await.map_err(|error| error.to_string())?,
                            FrameAction::Closed => return Err("signaling control socket closed".into()),
                            FrameAction::Text(_) | FrameAction::Ignore => {}
                        }
                    }
                };
                match tokio::time::timeout(HEARTBEAT_TIMEOUT, heartbeat_result).await {
                    Ok(Ok(())) => log::info!("Signaling control heartbeat succeeded: appid={} sequence={} latency_ms={}", options.appid, sequence, started.elapsed().as_millis()),
                    Ok(Err(error)) => {
                        log::warn!("Signaling control heartbeat failed: appid={} sequence={} error={error}", options.appid, sequence);
                        socket = reconnect(&options, &mut reconnect_attempt).await;
                        heartbeat.reset();
                    }
                    Err(_) => {
                        log::warn!("Signaling control heartbeat timed out: appid={} sequence={} timeout_secs={}", options.appid, sequence, HEARTBEAT_TIMEOUT.as_secs());
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
    request: AppControlRequest,
    request_id: u64,
) -> Result<AppControlResponse, String> {
    socket
        .send(Message::text(request.to_wire()))
        .await
        .map_err(|error| error.to_string())?;
    loop {
        let frame = socket
            .next()
            .await
            .ok_or_else(|| "signaling control socket closed".to_string())?
            .map_err(|error| error.to_string())?;
        match classify_frame(frame) {
            FrameAction::Text(text) => {
                if let Some(reply) = correlate_reply(&text, request_id)? {
                    return Ok(reply);
                }
            }
            FrameAction::ReplyPing(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|error| error.to_string())?,
            FrameAction::Closed => return Err("signaling control socket closed".into()),
            FrameAction::Pong | FrameAction::Ignore => {}
        }
    }
}

async fn reconnect(options: &ConnectionOptions, attempt: &mut u32) -> ControlSocket {
    loop {
        *attempt = attempt.saturating_add(1);
        let jitter = Duration::from_millis(rand::random::<u64>() % RECONNECT_JITTER_MS);
        let delay = reconnect_delay(*attempt, jitter);
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
        admission_from(
            self.request(AppControlRequest::admit(
                self.req(),
                roomid,
                clientid,
                is_loopback,
            ))
            .await?,
        )
    }
    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        expect_ack(
            self.request(AppControlRequest::remove(self.req(), roomid, clientid))
                .await?,
            "remove failed",
        )
        .map(|_| ())
    }
    async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        occupancy_from(
            self.request(AppControlRequest::occupancy(self.req(), roomid))
                .await?,
        )
    }
    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String> {
        expect_ack(
            self.request(AppControlRequest::inject(self.req(), roomid, clientid, msg))
                .await?,
            "inject failed",
        )
        .map(|_| ())
    }
    async fn status(&self) -> Result<StatusSnapshot, String> {
        Ok(snapshot_from(
            self.request(AppControlRequest::status(self.req())).await?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_from_one_second_and_caps_at_thirty() {
        assert_eq!(reconnect_delay(1, Duration::ZERO), Duration::from_secs(1));
        assert_eq!(reconnect_delay(2, Duration::ZERO), Duration::from_secs(2));
        assert_eq!(reconnect_delay(5, Duration::ZERO), Duration::from_secs(16));
        // Attempt 6 would be 32 s; the cap holds it at 30 s, and stays there.
        assert_eq!(reconnect_delay(6, Duration::ZERO), Duration::from_secs(30));
        assert_eq!(reconnect_delay(100, Duration::ZERO), Duration::from_secs(30));
        // Jitter is added below the cap but can never push the delay past it.
        assert_eq!(
            reconnect_delay(1, Duration::from_millis(249)),
            Duration::from_millis(1249)
        );
        assert_eq!(
            reconnect_delay(100, Duration::from_millis(249)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn replies_are_correlated_by_request_id() {
        let matched = correlate_reply(r#"{"reply":"status","req":5}"#, 5).unwrap();
        assert_eq!(matched.unwrap().reply, "status");
        // A stale reply for an earlier request is skipped, not an error.
        assert!(correlate_reply(r#"{"reply":"status","req":4}"#, 5).unwrap().is_none());
        // An undecodable frame fails the request.
        assert!(correlate_reply("not json", 5).is_err());
    }

    #[test]
    fn registration_requires_the_registered_control_frame() {
        assert!(registration_ack(r#"{"control":"registered"}"#).is_ok());
        let error = registration_ack(r#"{"control":"nope"}"#).unwrap_err();
        assert!(error.contains(r#"{"control":"nope"}"#));
        assert!(registration_ack("garbage").is_err());
    }

    #[test]
    fn control_frames_classify_by_what_the_waiter_should_do() {
        assert_eq!(
            classify_frame(Message::text(r#"{"reply":"ok"}"#)),
            FrameAction::Text(r#"{"reply":"ok"}"#.into())
        );
        assert_eq!(
            classify_frame(Message::Pong(Bytes::from_static(b"1"))),
            FrameAction::Pong
        );
        assert_eq!(
            classify_frame(Message::Ping(Bytes::from_static(b"2"))),
            FrameAction::ReplyPing(Bytes::from_static(b"2"))
        );
        assert_eq!(classify_frame(Message::Close(None)), FrameAction::Closed);
        assert_eq!(
            classify_frame(Message::Binary(Bytes::from_static(b"x"))),
            FrameAction::Ignore
        );
    }

    #[test]
    fn error_replies_map_to_their_result_or_the_fallback() {
        let error = AppControlResponse {
            reply: "error".into(),
            result: Some("FULL".into()),
            ..Default::default()
        };
        assert_eq!(admission_from(error).unwrap_err(), "FULL");

        let bare_error = AppControlResponse {
            reply: "error".into(),
            ..Default::default()
        };
        assert_eq!(admission_from(bare_error).unwrap_err(), "admission failed");

        let admitted = AppControlResponse {
            reply: "admitted".into(),
            is_initiator: Some(true),
            messages: Some(vec!["offer".into()]),
            ..Default::default()
        };
        assert_eq!(
            admission_from(admitted).unwrap(),
            Admission {
                is_initiator: true,
                messages: vec!["offer".into()],
            }
        );
    }

    #[test]
    fn occupancy_and_status_replies_convert_with_defaults() {
        let counted = AppControlResponse {
            reply: "occupancy".into(),
            count: Some(2),
            ..Default::default()
        };
        assert_eq!(occupancy_from(counted).unwrap(), 2);
        let missing = AppControlResponse {
            reply: "occupancy".into(),
            ..Default::default()
        };
        assert_eq!(occupancy_from(missing).unwrap_err(), "occupancy failed");

        let snapshot = snapshot_from(AppControlResponse {
            reply: "status".into(),
            rooms: Some(1),
            clients: Some(2),
            ..Default::default()
        });
        assert_eq!(snapshot.rooms, 1);
        assert_eq!(snapshot.clients, 2);
        assert_eq!(snapshot.websocket_connections, 0);
        assert_eq!(snapshot.total_websocket_connections, 0);
    }
}
