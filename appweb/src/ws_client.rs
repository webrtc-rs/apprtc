//! Reconnecting AppWeb control client for the signaling room authority.
//!
//! This module is the *I/O driver* only: sockets, TLS, channels, timers, and
//! randomness (jitter sampling). The wire format is shared with the signaling
//! authority — [`AppControlRequest`]/[`AppControlResponse`] live in the Sans-I/O
//! `signaling` crate — and every protocol decision lives in [`crate::controller`],
//! unit-tested without sockets or a clock: the per-connection [`Controller`]
//! object owns heartbeat/backoff sequencing and the frame decisions, and
//! [`ControlResponseExt`] converts responses into domain values.

use crate::controller::{ControlResponseExt, Controller, FrameAction};
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
    tungstenite::Message,
};

pub use crate::controller::Admission;
pub use signaling::collider::StatusSnapshot;

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
const RECONNECT_JITTER_MS: u64 = 250;

struct AuthorityRequest {
    request: AppControlRequest,
    response: oneshot::Sender<Result<AppControlResponse, String>>,
}

#[derive(Clone)]
struct ConnectionOptions {
    url: String,
    appid: String,
    token: String,
    insecure_tls: bool,
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

    async fn request(&self, request: AppControlRequest) -> Result<AppControlResponse, String> {
        let req = request.req;
        let operation = request.cmd.clone();
        let (response_tx, response_rx) = oneshot::channel();
        self.requests
            .send(AuthorityRequest {
                request,
                response: response_tx,
            })
            .await
            .map_err(|_| "signaling control worker stopped".to_string())?;
        let started = Instant::now();
        let result = tokio::time::timeout(REQUEST_TIMEOUT, response_rx)
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
    Controller::registration_ack(&frame)?;
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
    let mut controller = Controller::new();

    loop {
        tokio::select! {
            request = requests.recv() => {
                let Some(request) = request else {
                    log::info!("Signaling control worker stopped: appid={}", options.appid);
                    let _ = socket.close(None).await;
                    return;
                };
                if request.response.is_closed() {
                    continue;
                }
                let request_id = request.request.req;
                let outcome = match tokio::time::timeout(HEARTBEAT_TIMEOUT, exchange(&mut socket, request.request, request_id)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(format!("signaling control request {request_id} timed out waiting for response")),
                };
                let failed = outcome.is_err();
                let _ = request.response.send(outcome);
                if failed {
                    log::warn!("Signaling control connection lost during request: appid={} request_id={request_id}", options.appid);
                    socket = reconnect(&options, &mut controller).await;
                    heartbeat.reset();
                }
            }
            _ = heartbeat.tick() => {
                let sequence = controller.next_heartbeat();
                let started = Instant::now();
                let heartbeat_result = async {
                    socket.send(Message::Ping(sequence.to_be_bytes().to_vec().into())).await.map_err(|error| error.to_string())?;
                    loop {
                        let frame = socket.next().await.ok_or_else(|| "signaling control socket closed".to_string())?.map_err(|error| error.to_string())?;
                        match Controller::classify_frame(frame) {
                            FrameAction::Pong => return Ok::<(), String>(()),
                            FrameAction::Ping(payload) => socket.send(Message::Pong(payload)).await.map_err(|error| error.to_string())?,
                            FrameAction::Closed => return Err("signaling control socket closed".into()),
                            FrameAction::Text(_) | FrameAction::Ignore => {}
                        }
                    }
                };
                match tokio::time::timeout(HEARTBEAT_TIMEOUT, heartbeat_result).await {
                    Ok(Ok(())) => log::info!("Signaling control heartbeat succeeded: appid={} sequence={} latency_ms={}", options.appid, sequence, started.elapsed().as_millis()),
                    Ok(Err(error)) => {
                        log::warn!("Signaling control heartbeat failed: appid={} sequence={} error={error}", options.appid, sequence);
                        socket = reconnect(&options, &mut controller).await;
                        heartbeat.reset();
                    }
                    Err(_) => {
                        log::warn!("Signaling control heartbeat timed out: appid={} sequence={} timeout_secs={}", options.appid, sequence, HEARTBEAT_TIMEOUT.as_secs());
                        socket = reconnect(&options, &mut controller).await;
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
        match Controller::classify_frame(frame) {
            FrameAction::Text(text) => {
                if let Some(response) = Controller::correlate_response(&text, request_id)? {
                    return Ok(response);
                }
            }
            FrameAction::Ping(payload) => socket
                .send(Message::Pong(payload))
                .await
                .map_err(|error| error.to_string())?,
            FrameAction::Closed => return Err("signaling control socket closed".into()),
            FrameAction::Pong | FrameAction::Ignore => {}
        }
    }
}

async fn reconnect(options: &ConnectionOptions, controller: &mut Controller) -> ControlSocket {
    loop {
        let jitter = Duration::from_millis(rand::random::<u64>() % RECONNECT_JITTER_MS);
        let (attempt, delay) = controller.schedule_reconnect(jitter);
        log::info!(
            "Signaling control reconnect scheduled: appid={} attempt={} delay_ms={}",
            options.appid,
            attempt,
            delay.as_millis()
        );
        tokio::time::sleep(delay).await;
        match tokio::time::timeout(CONNECT_TIMEOUT, connect_control(options, attempt)).await {
            Err(_) => log::warn!(
                "Signaling control reconnect timed out: appid={} attempt={} timeout_secs={}",
                options.appid,
                attempt,
                CONNECT_TIMEOUT.as_secs()
            ),
            Ok(Ok(socket)) => {
                log::info!(
                    "Signaling control reconnected: appid={} attempt={}",
                    options.appid,
                    attempt
                );
                controller.reconnected();
                return socket;
            }
            Ok(Err(error)) => log::warn!(
                "Signaling control reconnect failed: appid={} attempt={} error={error}",
                options.appid,
                attempt
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
        self.request(AppControlRequest::admit(
            self.req(),
            roomid,
            clientid,
            is_loopback,
        ))
        .await?
        .admission()
    }
    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        self.request(AppControlRequest::remove(self.req(), roomid, clientid))
            .await?
            .ack("remove failed")
            .map(|_| ())
    }
    async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        self.request(AppControlRequest::occupancy(self.req(), roomid))
            .await?
            .occupancy_count()
    }
    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String> {
        self.request(AppControlRequest::inject(self.req(), roomid, clientid, msg))
            .await?
            .ack("inject failed")
            .map(|_| ())
    }
    async fn status(&self) -> Result<StatusSnapshot, String> {
        Ok(self
            .request(AppControlRequest::status(self.req()))
            .await?
            .status_snapshot())
    }
}
