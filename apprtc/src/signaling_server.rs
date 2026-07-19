//! Blocking-thread I/O driver for the Sans-I/O [`signaling::collider::Collider`].
//!
//! Mirrors the architecture of the SFU `chat` example: the binary owns the TCP + TLS
//! listener, per-connection WebSocket threads, and the run loop that owns the state
//! machine — the `signaling` crate itself contains no sockets, no threads, and no clock.
//!
//! Each browser opens **one** WebSocket on `/ws`. A hand-rolled HTTP layer completes the
//! RFC 6455 upgrade (so the raw stream keeps a read timeout), then a `tungstenite`
//! session thread shuttles frames to the run loop over channels. The run loop serializes
//! every [`BrowserInput`] through the single `Collider` and routes every
//! [`BrowserOutput`] back to the owning session's channel.

use rustls::{ServerConfig, ServerConnection, StreamOwned};
use sansio::Protocol;
use signaling::collider::{BrowserInput, BrowserOutput, Collider, ConnectionId};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TryRecvError};
use std::time::{Duration, Instant};
use tungstenite::handshake::derive_accept_key;
use tungstenite::protocol::{Role, WebSocketConfig};
use tungstenite::{Message, WebSocket};

pub const COMMAND_CAPACITY: usize = 1024;
const SOCKET_OUTPUT_CAPACITY: usize = 1024;
/// Maximum size of one signaling WebSocket message/frame.
///
/// SDP and trickle-ICE messages are normally only a few KiB, but a 1 MiB
/// limit leaves room for larger browser-generated descriptions without
/// allowing an unbounded allocation on the dedicated signaling host.
const MAX_WS_MESSAGE_SIZE: usize = 1024 * 1024;
/// A WebSocket `read()` blocks at most this long, so the session thread can interleave
/// run-loop→browser writes (a relayed message or control reply reaches the peer within
/// one interval). Kept short because AppWeb serializes its control requests over one
/// WebSocket: every interval of reply latency directly caps control throughput.
const WS_POLL_TIMEOUT: Duration = Duration::from_millis(5);
/// An idle socket (no text, no ping) is closed after this long.
const WS_IDLE_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24);
/// The run loop notices a stop signal within this interval even with no traffic.
const STOP_POLL_TIMEOUT: Duration = Duration::from_millis(100);

pub enum SocketOutput {
    Text(String),
    Close,
}

/// Commands a session thread hands to the run loop that owns the `Collider`.
pub enum DriverCommand {
    Connected {
        connection_id: ConnectionId,
        output: SyncSender<SocketOutput>,
    },
    Text {
        connection_id: ConnectionId,
        text: String,
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
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl Stream {
    fn socket(&self) -> &TcpStream {
        match self {
            Stream::Plain(tcp) => tcp,
            Stream::Tls(tls) => &tls.sock,
        }
    }
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(tcp) => tcp.read(buf),
            Stream::Tls(tls) => tls.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Stream::Plain(tcp) => tcp.write(buf),
            Stream::Tls(tls) => tls.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(tcp) => tcp.flush(),
            Stream::Tls(tls) => tls.flush(),
        }
    }
}

// ───────────────────────────── TLS + HTTP + WebSocket ──────────────────────────────

/// Accept connections until `stop_rx` fires; upgrade `/ws` to a WebSocket session.
pub fn serve_io(
    stop_rx: crossbeam_channel::Receiver<()>,
    listener: TcpListener,
    tls_config: Option<Arc<ServerConfig>>,
    commands: SyncSender<DriverCommand>,
) {
    listener
        .set_nonblocking(true)
        .expect("set signaling listener non-blocking");

    let mut next_connection_id: ConnectionId = 1;
    loop {
        // Stop on an explicit signal *or* when the sender drops (the single `()` is
        // consumed by one receiver, so everyone else sees disconnect on shutdown).
        match stop_rx.try_recv() {
            Ok(_) => break,
            Err(err) if err.is_disconnected() => break,
            Err(_) => {}
        }
        match listener.accept() {
            Ok((tcp, _peer)) => {
                let connection_id = next_connection_id;
                next_connection_id += 1;
                let tls_config = tls_config.clone();
                let commands = commands.clone();
                std::thread::spawn(move || {
                    if let Err(err) = handle_connection(tcp, tls_config, connection_id, commands) {
                        log::trace!("connection ended: {err}");
                    }
                });
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(err) => log::warn!("accept error: {err}"),
        }
    }
    log::info!("signaling accept loop stopped");
}

/// One accepted connection: optional TLS handshake, read the HTTP request head, then
/// either upgrade `/ws` to a WebSocket or answer with a plain HTTP status.
fn handle_connection(
    tcp: TcpStream,
    tls_config: Option<Arc<ServerConfig>>,
    connection_id: ConnectionId,
    commands: SyncSender<DriverCommand>,
) -> anyhow::Result<()> {
    // The accepted stream can inherit the listener's non-blocking flag; the TLS handshake
    // and request-head read need blocking I/O (the per-frame read timeout is set later,
    // only for the WebSocket poll loop).
    tcp.set_nonblocking(false)?;
    let mut stream = match tls_config {
        Some(config) => {
            // rustls performs the handshake lazily on the first read/write.
            let conn = ServerConnection::new(config)?;
            Stream::Tls(Box::new(StreamOwned::new(conn, tcp)))
        }
        None => Stream::Plain(tcp),
    };
    let head = read_http_head(&mut stream)?;
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

    if request.path != "/ws" && request.path != "/ws/" {
        return respond(&mut stream, "404 Not Found", "not found");
    }
    if !is_upgrade {
        return respond(&mut stream, "426 Upgrade Required", "upgrade required");
    }

    // Complete the RFC 6455 handshake ourselves, then hand the raw stream to
    // tungstenite with the read timeout that drives the session poll loop.
    let key = request.header("sec-websocket-key").unwrap();
    let accept = derive_accept_key(key.as_bytes());
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;

    stream.socket().set_read_timeout(Some(WS_POLL_TIMEOUT))?;
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE_SIZE))
        .max_frame_size(Some(MAX_WS_MESSAGE_SIZE));
    let ws = WebSocket::from_raw_socket(stream, Role::Server, Some(config));
    ws_session(ws, connection_id, commands);
    Ok(())
}

/// Read bytes until the end of the HTTP request head (`\r\n\r\n`). WebSocket clients send
/// nothing after the head until they receive the `101`, so this consumes exactly the head.
fn read_http_head(stream: &mut Stream) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 1024];
    loop {
        let n = stream.read(&mut chunk)?;
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

fn respond(stream: &mut Stream, status: &str, body: &str) -> anyhow::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: text/plain; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

/// Drive one browser's WebSocket for its lifetime: read client frames → run loop, and
/// drain run-loop→browser outputs → client.
fn ws_session(
    mut ws: WebSocket<Stream>,
    connection_id: ConnectionId,
    commands: SyncSender<DriverCommand>,
) {
    log::info!("WebSocket connected: connection_id={connection_id}");
    // Run-loop→browser channel; its sender is owned by the run loop for routing.
    let (output, outputs) = std::sync::mpsc::sync_channel::<SocketOutput>(SOCKET_OUTPUT_CAPACITY);
    if commands
        .send(DriverCommand::Connected {
            connection_id,
            output,
        })
        .is_err()
    {
        return;
    }

    let mut last_activity = Instant::now();
    'session: loop {
        // 1. Read one client frame (blocks up to WS_POLL_TIMEOUT).
        match ws.read() {
            Ok(Message::Text(text)) => {
                log::info!(
                    "WebSocket message: connection_id={connection_id} bytes={}",
                    text.len()
                );
                if commands
                    .send(DriverCommand::Text {
                        connection_id,
                        text: text.to_string(),
                        now: Instant::now(),
                    })
                    .is_err()
                {
                    break;
                }
                last_activity = Instant::now();
            }
            Ok(Message::Ping(payload)) => {
                log::info!(
                    "WebSocket keep-alive ping received: connection_id={connection_id} bytes={}",
                    payload.len()
                );
                let _ = ws.send(Message::Pong(payload));
                last_activity = Instant::now();
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Binary(_)) => break,
            Ok(Message::Close(_)) => break,
            Ok(Message::Frame(_)) => {}
            Err(tungstenite::Error::Io(err))
                if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut => {}
            Err(err) => {
                log::trace!("ws read ended: {err}");
                break;
            }
        }
        if last_activity.elapsed() >= WS_IDLE_TIMEOUT {
            break;
        }

        // 2. Flush run-loop→browser outputs (relayed messages, errors, closes).
        loop {
            match outputs.try_recv() {
                Ok(SocketOutput::Text(text)) => {
                    if ws.send(Message::text(text)).is_err() {
                        break 'session;
                    }
                }
                Ok(SocketOutput::Close) => {
                    let _ = ws.close(None);
                    let _ = ws.flush();
                    break 'session;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break 'session,
            }
        }
    }

    let _ = commands.send(DriverCommand::Disconnected {
        connection_id,
        now: Instant::now(),
    });
    log::info!("WebSocket disconnected: connection_id={connection_id}");
}

// ──────────────────────────────────── run loop ─────────────────────────────────────

/// The run loop that owns the Sans-I/O `Collider`: serializes every browser input,
/// fires the state machine's timeouts, and routes outputs to each session's channel.
pub fn event_loop(
    stop_rx: crossbeam_channel::Receiver<()>,
    commands: Receiver<DriverCommand>,
    register_timeout: Duration,
) {
    let mut collider = Collider::new(register_timeout);
    let mut sockets: HashMap<ConnectionId, SyncSender<SocketOutput>> = HashMap::new();
    loop {
        match stop_rx.try_recv() {
            Ok(_) => break,
            Err(err) if err.is_disconnected() => break,
            Err(_) => {}
        }

        // Wait for the next command, capped by the state machine's own deadline (and by
        // the stop-poll interval so a shutdown signal is noticed without traffic).
        let wait = match collider.poll_timeout() {
            Some(deadline) => {
                let until_deadline = deadline.saturating_duration_since(Instant::now());
                if until_deadline.is_zero() {
                    if collider.handle_timeout(Instant::now()).is_err() {
                        break;
                    }
                    drain_outputs(&mut collider, &mut sockets);
                    continue;
                }
                until_deadline.min(STOP_POLL_TIMEOUT)
            }
            None => STOP_POLL_TIMEOUT,
        };

        match commands.recv_timeout(wait) {
            Ok(command) => {
                handle_command(&mut collider, command, &mut sockets);
                drain_outputs(&mut collider, &mut sockets);
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }

    // Graceful stop: apply commands already queued before the signal, then close every
    // browser socket and release all signaling state.
    while let Ok(command) = commands.try_recv() {
        handle_command(&mut collider, command, &mut sockets);
    }
    let _ = collider.close();
    drain_outputs(&mut collider, &mut sockets);
    log::info!("signaling run loop stopped");
}

fn handle_command(
    collider: &mut Collider,
    command: DriverCommand,
    sockets: &mut HashMap<ConnectionId, SyncSender<SocketOutput>>,
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
        DriverCommand::Disconnected { connection_id, now } => {
            sockets.remove(&connection_id);
            let _ = collider.handle_read(BrowserInput::Disconnected { connection_id, now });
        }
    }
}

fn drain_outputs(
    collider: &mut Collider,
    sockets: &mut HashMap<ConnectionId, SyncSender<SocketOutput>>,
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
    use std::sync::mpsc::sync_channel;

    struct Harness {
        stop_tx: crossbeam_channel::Sender<()>,
        commands: SyncSender<DriverCommand>,
        run: std::thread::JoinHandle<()>,
    }

    impl Harness {
        fn spawn() -> Self {
            let (stop_tx, stop_rx) = crossbeam_channel::bounded(1);
            let (commands, commands_rx) = sync_channel(COMMAND_CAPACITY);
            let run = std::thread::spawn(move || {
                event_loop(stop_rx, commands_rx, Duration::from_secs(10))
            });
            Self {
                stop_tx,
                commands,
                run,
            }
        }

        fn connect(&self, connection_id: ConnectionId) -> Receiver<SocketOutput> {
            let (output, outputs) = sync_channel(SOCKET_OUTPUT_CAPACITY);
            self.commands
                .send(DriverCommand::Connected {
                    connection_id,
                    output,
                })
                .unwrap();
            outputs
        }

        fn text(&self, connection_id: ConnectionId, text: &str) {
            self.commands
                .send(DriverCommand::Text {
                    connection_id,
                    text: text.to_string(),
                    now: Instant::now(),
                })
                .unwrap();
        }

        fn shutdown(self) {
            self.stop_tx.send(()).unwrap();
            self.run.join().unwrap();
        }
    }

    fn recv_text(outputs: &Receiver<SocketOutput>) -> String {
        match outputs.recv_timeout(Duration::from_secs(5)).unwrap() {
            SocketOutput::Text(text) => text,
            SocketOutput::Close => panic!("expected a text frame, got a close"),
        }
    }

    fn recv_close(outputs: &Receiver<SocketOutput>) {
        match outputs.recv_timeout(Duration::from_secs(5)).unwrap() {
            SocketOutput::Close => {}
            SocketOutput::Text(text) => panic!("expected a close, got text: {text}"),
        }
    }

    #[test]
    fn v1_send_relays_between_registered_sessions() {
        let harness = Harness::spawn();
        let outputs_1 = harness.connect(1);
        let outputs_2 = harness.connect(2);
        harness.text(1, r#"{"cmd":"register","roomid":"room","clientid":"1"}"#);
        harness.text(2, r#"{"cmd":"register","roomid":"room","clientid":"2"}"#);
        harness.text(1, r#"{"cmd":"send","msg":"candidate"}"#);
        assert_eq!(
            recv_text(&outputs_2),
            r#"{"msg":"candidate","error":""}"#.to_string()
        );
        assert!(outputs_1.try_recv().is_err(), "registration is silent");
        harness.shutdown();
    }

    #[test]
    fn protocol_errors_are_framed_then_close_the_socket() {
        let harness = Harness::spawn();
        let outputs = harness.connect(1);
        harness.text(1, r#"{"cmd":"send","msg":"offer"}"#);
        assert!(recv_text(&outputs).contains("Client not registered"));
        recv_close(&outputs);
        harness.shutdown();
    }

    #[test]
    fn shutdown_closes_every_connected_socket() {
        let harness = Harness::spawn();
        let outputs_1 = harness.connect(1);
        let outputs_2 = harness.connect(2);
        harness.text(1, r#"{"cmd":"register","roomid":"room","clientid":"1"}"#);
        harness.shutdown();
        recv_close(&outputs_1);
        recv_close(&outputs_2);
    }
}
