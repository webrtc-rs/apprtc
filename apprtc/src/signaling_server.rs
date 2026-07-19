//! Async (Tokio) I/O driver for the Sans-I/O [`signaling::collider::Collider`].
//!
//! Keeps the architecture of the SFU `chat` example — the binary owns the TCP + TLS
//! listener, per-connection WebSocket sessions, and the event loop that owns the state
//! machine — but every blocking call is replaced with an async task, so sessions and the
//! event loop sleep on `tokio::select!` instead of polling with read timeouts: inputs,
//! outputs, deadlines, and the stop signal all wake their task immediately.
//!
//! Browsers use JSON text frames on `/ws`; AppWeb uses Protobuf binary frames on
//! `/app`. A hand-rolled HTTP layer completes the RFC 6455 upgrade, then a
//! `tokio-tungstenite` session task shuttles typed inputs to the event loop over channels.
//! The event loop serializes every [`BrowserInput`] through the single `Collider` and
//! routes every [`BrowserOutput`] back to the owning session's channel.

use futures_util::{SinkExt, StreamExt};
use rustls::ServerConfig;
use sansio::Protocol;
use signaling::collider::{BrowserInput, BrowserOutput, Collider, ConnectionId};
use signaling_proto::Request as AppControlRequest;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};

pub const COMMAND_CAPACITY: usize = 1024;
const SOCKET_OUTPUT_CAPACITY: usize = 1024;
/// Maximum size of one signaling WebSocket message/frame.
///
/// SDP and trickle-ICE messages are normally only a few KiB, but a 1 MiB
/// limit leaves room for larger browser-generated descriptions without
/// allowing an unbounded allocation on the dedicated signaling host.
const MAX_WS_MESSAGE_SIZE: usize = 1024 * 1024;
/// An idle socket (no text, no ping) is closed after this long.
const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24);
/// The TLS handshake and HTTP request head must complete within this long.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub enum SocketOutput {
    Text(String),
    Binary(Vec<u8>),
    Close,
}

#[derive(Debug, Clone, Copy)]
enum Endpoint {
    Browser,
    AppControl,
}

/// Commands a session task hands to the event loop that owns the `Collider`.
pub enum DriverCommand {
    Connected {
        connection_id: ConnectionId,
        output: mpsc::Sender<SocketOutput>,
    },
    Text {
        connection_id: ConnectionId,
        text: String,
        now: Instant,
    },
    AppControl {
        connection_id: ConnectionId,
        request: AppControlRequest,
        now: Instant,
    },
    Disconnected {
        connection_id: ConnectionId,
        now: Instant,
    },
}

/// A plain or TLS stream over one accepted `TcpStream`.
enum Stream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Stream::Plain(tcp) => Pin::new(tcp).poll_read(cx, buf),
            Stream::Tls(tls) => Pin::new(tls).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match &mut *self {
            Stream::Plain(tcp) => Pin::new(tcp).poll_write(cx, buf),
            Stream::Tls(tls) => Pin::new(tls).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Stream::Plain(tcp) => Pin::new(tcp).poll_flush(cx),
            Stream::Tls(tls) => Pin::new(tls).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match &mut *self {
            Stream::Plain(tcp) => Pin::new(tcp).poll_shutdown(cx),
            Stream::Tls(tls) => Pin::new(tls).poll_shutdown(cx),
        }
    }
}

// ───────────────────────────── TLS + HTTP + WebSocket ──────────────────────────────

/// Accept connections until `stop_rx` fires; upgrade a supported WebSocket endpoint.
pub async fn accept_loop(
    mut stop_rx: watch::Receiver<()>,
    commands: mpsc::Sender<DriverCommand>,
    listener: TcpListener,
    tls_config: Option<Arc<ServerConfig>>,
) {
    let tls_acceptor = tls_config.map(TlsAcceptor::from);
    let mut next_connection_id: ConnectionId = 1;
    loop {
        // Stop on an explicit signal *or* when the sender drops.
        tokio::select! {
            _ = stop_rx.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, _peer)) => {
                    let connection_id = next_connection_id;
                    next_connection_id += 1;
                    let tls_acceptor = tls_acceptor.clone();
                    let commands = commands.clone();
                    tokio::spawn(async move {
                        if let Err(err) =
                            handle_connection(tcp, tls_acceptor, connection_id, commands).await
                        {
                            log::trace!("connection ended: {err}");
                        }
                    });
                }
                Err(err) => log::warn!("accept error: {err}"),
            },
        }
    }
    log::info!("signaling accept loop stopped");
}

/// One accepted connection: optional TLS handshake, read the HTTP request head, then
/// either upgrade `/ws` or `/app` to a WebSocket or answer with HTTP status.
async fn handle_connection(
    tcp: TcpStream,
    tls_acceptor: Option<TlsAcceptor>,
    connection_id: ConnectionId,
    commands: mpsc::Sender<DriverCommand>,
) -> anyhow::Result<()> {
    let (mut stream, head) = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        let mut stream = match tls_acceptor {
            Some(acceptor) => Stream::Tls(Box::new(acceptor.accept(tcp).await?)),
            None => Stream::Plain(tcp),
        };
        let head = read_http_head(&mut stream).await?;
        anyhow::Ok((stream, head))
    })
    .await
    .map_err(|_| anyhow::anyhow!("TLS/HTTP handshake timed out"))??;
    let request = HttpRequest::parse(&head);
    log::trace!(
        "HTTP request: connection_id={connection_id} method={} path={}",
        request.method,
        request.path
    );

    let is_upgrade = request
        .header("upgrade")
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        && request.header("sec-websocket-key").is_some();

    let endpoint = match request.path.as_str() {
        "/ws" | "/ws/" => Endpoint::Browser,
        "/app" | "/app/" => Endpoint::AppControl,
        _ => return respond(&mut stream, "404 Not Found", "not found").await,
    };
    if !is_upgrade {
        return respond(&mut stream, "426 Upgrade Required", "upgrade required").await;
    }

    // Complete the RFC 6455 handshake ourselves, then hand the raw stream to
    // tokio-tungstenite.
    let key = request.header("sec-websocket-key").unwrap();
    let accept = derive_accept_key(key.as_bytes());
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;

    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_SIZE))
        .max_frame_size(Some(MAX_WS_MESSAGE_SIZE));
    let ws = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
    ws_session(ws, endpoint, connection_id, commands).await;
    Ok(())
}

/// Read bytes until the end of the HTTP request head (`\r\n\r\n`). WebSocket clients send
/// nothing after the head until they receive the `101`, so this consumes exactly the head.
async fn read_http_head(stream: &mut Stream) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            anyhow::bail!("connection closed before request head");
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        anyhow::ensure!(buf.len() <= 64 * 1024, "request head too large");
    }
    Ok(buf)
}

struct HttpRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

impl HttpRequest {
    fn parse(head: &[u8]) -> Self {
        let text = String::from_utf8_lossy(head);
        let mut lines = text.split("\r\n");
        let mut request_line = lines.next().unwrap_or("").split_whitespace();
        let method = request_line.next().unwrap_or("").to_owned();
        let path = request_line.next().unwrap_or("/").to_owned();
        let headers = lines
            .take_while(|line| !line.is_empty())
            .filter_map(|line| {
                let (key, value) = line.split_once(':')?;
                Some((key.trim().to_owned(), value.trim().to_owned()))
            })
            .collect();
        HttpRequest {
            method,
            path,
            headers,
        }
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

async fn respond(stream: &mut Stream, status: &str, body: &str) -> anyhow::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Drive one browser's WebSocket for its lifetime: forward client frames to the event
/// loop, and push event-loop→browser outputs to the client — each side wakes the task
/// the moment it is ready, with no polling interval in between.
async fn ws_session(
    ws: WebSocketStream<Stream>,
    endpoint: Endpoint,
    connection_id: ConnectionId,
    commands: mpsc::Sender<DriverCommand>,
) {
    log::info!("WebSocket connected: connection_id={connection_id} endpoint={endpoint:?}");
    // Event-loop→browser channel; its sender is owned by the event loop for routing.
    let (output, mut outputs) = mpsc::channel::<SocketOutput>(SOCKET_OUTPUT_CAPACITY);
    if commands
        .send(DriverCommand::Connected {
            connection_id,
            output,
        })
        .await
        .is_err()
    {
        return;
    }

    let (mut writer, mut reader) = ws.split();
    let mut idle_deadline = tokio::time::Instant::now() + WS_IDLE_TIMEOUT;
    loop {
        tokio::select! {
            output = outputs.recv() => match output {
                Some(SocketOutput::Text(text)) => {
                    if writer.send(Message::text(text)).await.is_err() {
                        break;
                    }
                }
                Some(SocketOutput::Binary(bytes)) => {
                    if writer.send(Message::binary(bytes)).await.is_err() {
                        break;
                    }
                }
                Some(SocketOutput::Close) | None => {
                    let _ = writer.send(Message::Close(None)).await;
                    break;
                }
            },
            incoming = reader.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    if !matches!(endpoint, Endpoint::Browser) {
                        log::warn!("AppWeb control text frame rejected: connection_id={connection_id}");
                        break;
                    }
                    log::info!(
                        "WebSocket message: connection_id={connection_id} bytes={}",
                        text.len()
                    );
                    if commands.send(DriverCommand::Text {
                        connection_id,
                        text: text.to_string(),
                        now: Instant::now(),
                    }).await.is_err() {
                        break;
                    }
                    idle_deadline = tokio::time::Instant::now() + WS_IDLE_TIMEOUT;
                }
                Some(Ok(Message::Binary(bytes))) => {
                    if !matches!(endpoint, Endpoint::AppControl) {
                        log::warn!("Browser binary frame rejected: connection_id={connection_id}");
                        break;
                    }
                    let request = match AppControlRequest::decode_wire(&bytes) {
                        Ok(request) if request.request_id != 0 && request.command.is_some() => request,
                        Ok(_) => {
                            log::warn!("AppWeb control Protobuf request rejected: connection_id={connection_id} reason=missing_request_id_or_command");
                            break;
                        }
                        Err(error) => {
                            log::warn!("AppWeb control Protobuf decode failed: connection_id={connection_id} bytes={} error={error}", bytes.len());
                            break;
                        }
                    };
                    log::info!(
                        "AppWeb control WebSocket message: connection_id={connection_id} operation={} request_id={} bytes={}",
                        request.operation_name(),
                        request.request_id,
                        bytes.len()
                    );
                    if commands.send(DriverCommand::AppControl {
                        connection_id,
                        request,
                        now: Instant::now(),
                    }).await.is_err() {
                        break;
                    }
                    idle_deadline = tokio::time::Instant::now() + WS_IDLE_TIMEOUT;
                }
                Some(Ok(Message::Ping(payload))) => {
                    // tokio-tungstenite queues the pong response automatically.
                    log::info!(
                        "WebSocket keep-alive ping received: connection_id={connection_id} bytes={}",
                        payload.len()
                    );
                    idle_deadline = tokio::time::Instant::now() + WS_IDLE_TIMEOUT;
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Frame(_))) => {}
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
            },
            _ = tokio::time::sleep_until(idle_deadline) => break,
        }
    }

    let _ = commands
        .send(DriverCommand::Disconnected {
            connection_id,
            now: Instant::now(),
        })
        .await;
    log::info!("WebSocket disconnected: connection_id={connection_id}");
}

// ──────────────────────────────────── event loop ────────────────────────────────────

/// The event loop that owns the Sans-I/O `Collider`: serializes every browser input,
/// fires the state machine's timeouts, and routes outputs to each session's channel.
pub async fn event_loop(
    mut stop_rx: watch::Receiver<()>,
    mut commands: mpsc::Receiver<DriverCommand>,
    register_timeout: Duration,
) {
    let mut collider = Collider::new(register_timeout);
    let mut sockets: HashMap<ConnectionId, mpsc::Sender<SocketOutput>> = HashMap::new();
    loop {
        // Sleep until the next command, the state machine's own deadline, or the stop
        // signal — whichever wakes first.
        let command = if let Some(deadline) = collider.poll_timeout() {
            tokio::select! {
                _ = stop_rx.changed() => break,
                command = commands.recv() => command,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    if collider.handle_timeout(Instant::now()).is_err() {
                        break;
                    }
                    drain_outputs(&mut collider, &mut sockets);
                    continue;
                }
            }
        } else {
            tokio::select! {
                _ = stop_rx.changed() => break,
                command = commands.recv() => command,
            }
        };

        let Some(command) = command else { break };
        handle_command(&mut collider, command, &mut sockets);
        drain_outputs(&mut collider, &mut sockets);
    }

    // Graceful stop: apply commands already queued before the signal, then close every
    // browser socket and release all signaling state.
    while let Ok(command) = commands.try_recv() {
        handle_command(&mut collider, command, &mut sockets);
    }
    let _ = collider.close();
    drain_outputs(&mut collider, &mut sockets);
    log::info!("signaling event loop stopped");
}

fn handle_command(
    collider: &mut Collider,
    command: DriverCommand,
    sockets: &mut HashMap<ConnectionId, mpsc::Sender<SocketOutput>>,
) {
    match command {
        DriverCommand::Connected {
            connection_id,
            output,
        } => {
            sockets.insert(connection_id, output);
            if collider
                .handle_read(BrowserInput::Connected { connection_id })
                .is_err()
            {
                sockets.remove(&connection_id);
            }
        }
        DriverCommand::Text {
            connection_id,
            text,
            now,
        } => {
            if collider
                .handle_read(BrowserInput::Text {
                    connection_id,
                    text,
                    now,
                })
                .is_err()
                && let Some(socket) = sockets.remove(&connection_id)
            {
                let _ = socket.try_send(SocketOutput::Close);
            }
        }
        DriverCommand::AppControl {
            connection_id,
            request,
            now,
        } => {
            if collider
                .handle_read(BrowserInput::AppControl {
                    connection_id,
                    request,
                    now,
                })
                .is_err()
                && let Some(socket) = sockets.remove(&connection_id)
            {
                let _ = socket.try_send(SocketOutput::Close);
            }
        }
        DriverCommand::Disconnected { connection_id, now } => {
            sockets.remove(&connection_id);
            let _ = collider.handle_read(BrowserInput::Disconnected { connection_id, now });
        }
    }
}

fn drain_outputs(
    collider: &mut Collider,
    sockets: &mut HashMap<ConnectionId, mpsc::Sender<SocketOutput>>,
) {
    let mut disconnected = Vec::new();
    while let Some(output) = collider.poll_write() {
        match output {
            BrowserOutput::Text {
                connection_id,
                text,
            } => {
                if sockets
                    .get(&connection_id)
                    .is_some_and(|socket| socket.try_send(SocketOutput::Text(text)).is_err())
                {
                    disconnected.push(connection_id);
                }
            }
            BrowserOutput::AppControl {
                connection_id,
                response,
            } => {
                if sockets.get(&connection_id).is_some_and(|socket| {
                    socket
                        .try_send(SocketOutput::Binary(response.encode_wire()))
                        .is_err()
                }) {
                    disconnected.push(connection_id);
                }
            }
            BrowserOutput::Close { connection_id } => {
                if let Some(socket) = sockets.remove(&connection_id) {
                    let _ = socket.try_send(SocketOutput::Close);
                }
                disconnected.push(connection_id);
            }
        }
    }
    disconnected.sort_unstable();
    disconnected.dedup();
    for connection_id in disconnected {
        sockets.remove(&connection_id);
        let _ = collider.handle_read(BrowserInput::Disconnected {
            connection_id,
            now: Instant::now(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signaling_proto::Response as AppControlResponse;

    struct Harness {
        stop_tx: watch::Sender<()>,
        commands: mpsc::Sender<DriverCommand>,
        run: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn spawn() -> Self {
            let (stop_tx, stop_rx) = watch::channel(());
            let (commands, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
            let run = tokio::spawn(event_loop(stop_rx, commands_rx, Duration::from_secs(10)));
            Self {
                stop_tx,
                commands,
                run,
            }
        }

        async fn connect(&self, connection_id: ConnectionId) -> mpsc::Receiver<SocketOutput> {
            let (output, outputs) = mpsc::channel(SOCKET_OUTPUT_CAPACITY);
            self.commands
                .send(DriverCommand::Connected {
                    connection_id,
                    output,
                })
                .await
                .unwrap();
            outputs
        }

        async fn text(&self, connection_id: ConnectionId, text: &str) {
            self.commands
                .send(DriverCommand::Text {
                    connection_id,
                    text: text.to_string(),
                    now: Instant::now(),
                })
                .await
                .unwrap();
        }

        async fn app_control(&self, connection_id: ConnectionId, request: AppControlRequest) {
            self.commands
                .send(DriverCommand::AppControl {
                    connection_id,
                    request,
                    now: Instant::now(),
                })
                .await
                .unwrap();
        }

        async fn shutdown(self) {
            self.stop_tx.send(()).unwrap();
            self.run.await.unwrap();
        }
    }

    async fn recv_text(outputs: &mut mpsc::Receiver<SocketOutput>) -> String {
        match tokio::time::timeout(Duration::from_secs(5), outputs.recv())
            .await
            .unwrap()
            .unwrap()
        {
            SocketOutput::Text(text) => text,
            SocketOutput::Binary(_) => panic!("expected a text frame, got binary"),
            SocketOutput::Close => panic!("expected a text frame, got a close"),
        }
    }

    async fn recv_close(outputs: &mut mpsc::Receiver<SocketOutput>) {
        match tokio::time::timeout(Duration::from_secs(5), outputs.recv())
            .await
            .unwrap()
            .unwrap()
        {
            SocketOutput::Close => {}
            SocketOutput::Text(text) => panic!("expected a close, got text: {text}"),
            SocketOutput::Binary(_) => panic!("expected a close, got binary"),
        }
    }

    async fn recv_control(outputs: &mut mpsc::Receiver<SocketOutput>) -> AppControlResponse {
        match tokio::time::timeout(Duration::from_secs(5), outputs.recv())
            .await
            .unwrap()
            .unwrap()
        {
            SocketOutput::Binary(bytes) => AppControlResponse::decode_wire(&bytes).unwrap(),
            SocketOutput::Text(text) => panic!("expected binary, got text: {text}"),
            SocketOutput::Close => panic!("expected binary, got close"),
        }
    }

    #[tokio::test]
    async fn v1_send_relays_between_registered_sessions() {
        let harness = Harness::spawn();
        let mut outputs_1 = harness.connect(1).await;
        let mut outputs_2 = harness.connect(2).await;
        harness
            .text(1, r#"{"cmd":"register","roomid":"room","clientid":"1"}"#)
            .await;
        harness
            .text(2, r#"{"cmd":"register","roomid":"room","clientid":"2"}"#)
            .await;
        harness.text(1, r#"{"cmd":"send","msg":"candidate"}"#).await;
        assert_eq!(
            recv_text(&mut outputs_2).await,
            r#"{"msg":"candidate","error":""}"#.to_string()
        );
        assert!(outputs_1.try_recv().is_err(), "registration is silent");
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn protobuf_app_control_registers_and_reports_status() {
        let harness = Harness::spawn();
        let mut outputs = harness.connect(9).await;
        harness
            .app_control(
                9,
                AppControlRequest::register(1, "appweb-test".into(), String::new()),
            )
            .await;
        let registered = recv_control(&mut outputs).await;
        assert_eq!(registered.request_id, 1);
        assert!(registered.is_ok());

        harness.app_control(9, AppControlRequest::status(2)).await;
        let status = recv_control(&mut outputs).await;
        assert_eq!(status.request_id, 2);
        assert!(status.is_ok());
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn protocol_errors_are_framed_then_close_the_socket() {
        let harness = Harness::spawn();
        let mut outputs = harness.connect(1).await;
        harness.text(1, r#"{"cmd":"send","msg":"offer"}"#).await;
        assert!(
            recv_text(&mut outputs)
                .await
                .contains("Client not registered")
        );
        recv_close(&mut outputs).await;
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_closes_every_connected_socket() {
        let harness = Harness::spawn();
        let mut outputs_1 = harness.connect(1).await;
        let mut outputs_2 = harness.connect(2).await;
        harness
            .text(1, r#"{"cmd":"register","roomid":"room","clientid":"1"}"#)
            .await;
        harness.shutdown().await;
        recv_close(&mut outputs_1).await;
        recv_close(&mut outputs_2).await;
    }
}
