//! Browser-facing WebSocket transport for the signaling service.
//!
//! This module owns TCP/TLS acceptance, the HTTP upgrade handshake, WebSocket framing,
//! and each browser connection's asynchronous lifetime. Protocol state remains in the
//! Sans-I/O Collider driven by [`crate::signaling_server`].

use crate::signaling_server::DriverCommand;
use futures_util::{SinkExt, StreamExt};
use rustls::ServerConfig;
use signaling::collider::ConnectionId;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};

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
/// Bound the graceful WebSocket close flush so one stalled peer cannot block shutdown.
const WS_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

pub enum SocketOutput {
    Text(String),
    Close,
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

/// Accept connections until `stop_rx` fires; upgrade the supported WebSocket endpoint.
pub async fn accept_loop(
    mut stop_rx: watch::Receiver<()>,
    commands: mpsc::Sender<DriverCommand>,
    listener: TcpListener,
    tls_config: Option<Arc<ServerConfig>>,
) {
    let tls_acceptor = tls_config.map(TlsAcceptor::from);
    let mut next_connection_id: ConnectionId = 1;
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = stop_rx.changed() => break,
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    log::warn!("signaling connection task failed: {error}");
                }
            }
            accepted = listener.accept() => match accepted {
                Ok((tcp, _peer)) => {
                    let connection_id = next_connection_id;
                    next_connection_id += 1;
                    let tls_acceptor = tls_acceptor.clone();
                    let commands = commands.clone();
                    let connection_stop_rx = stop_rx.clone();
                    connections.spawn(async move {
                        if let Err(err) = handle_connection(
                            connection_stop_rx,
                            tcp,
                            tls_acceptor,
                            connection_id,
                            commands,
                        )
                        .await
                        {
                            log::trace!("connection ended: {err}");
                        }
                    });
                }
                Err(err) => log::warn!("accept error: {err}"),
            },
        }
    }
    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            log::warn!("signaling connection task failed during shutdown: {error}");
        }
    }
    log::info!("signaling accept loop stopped");
}

/// One accepted connection: optional TLS handshake, read the HTTP request head, then
/// either upgrade `/ws` to a WebSocket or answer with HTTP status.
async fn handle_connection(
    mut stop_rx: watch::Receiver<()>,
    tcp: TcpStream,
    tls_acceptor: Option<TlsAcceptor>,
    connection_id: ConnectionId,
    commands: mpsc::Sender<DriverCommand>,
) -> anyhow::Result<()> {
    let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, async {
        let mut stream = match tls_acceptor {
            Some(acceptor) => Stream::Tls(Box::new(acceptor.accept(tcp).await?)),
            None => Stream::Plain(tcp),
        };
        let head = read_http_head(&mut stream).await?;
        anyhow::Ok((stream, head))
    });
    let (mut stream, head) = tokio::select! {
        _ = stop_rx.changed() => {
            log::debug!("connection stopped during TLS/HTTP handshake: connection_id={connection_id}");
            return Ok(());
        }
        result = handshake => result
            .map_err(|_| anyhow::anyhow!("TLS/HTTP handshake timed out"))??,
    };
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

    match request.path.as_str() {
        "/ws" | "/ws/" => {}
        _ => return respond(&mut stream, "404 Not Found", "not found").await,
    }
    if !is_upgrade {
        return respond(&mut stream, "426 Upgrade Required", "upgrade required").await;
    }

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
    ws_session(stop_rx, ws, connection_id, commands).await;
    Ok(())
}

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
        Self {
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

/// Drive one browser's WebSocket for its lifetime.
async fn ws_session<S>(
    mut stop_rx: watch::Receiver<()>,
    ws: WebSocketStream<S>,
    connection_id: ConnectionId,
    commands: mpsc::Sender<DriverCommand>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    log::info!("Browser WebSocket connected: connection_id={connection_id}");
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
            _ = stop_rx.changed() => {
                log::info!("Browser WebSocket shutdown requested: connection_id={connection_id}");
                let _ = tokio::time::timeout(WS_CLOSE_TIMEOUT, async {
                    writer.send(Message::Close(None)).await?;
                    writer.close().await
                }).await;
                break;
            }
            output = outputs.recv() => match output {
                Some(SocketOutput::Text(text)) => {
                    if writer.send(Message::text(text)).await.is_err() {
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
                    log::warn!("Browser binary frame rejected: connection_id={connection_id} bytes={}", bytes.len());
                    break;
                }
                Some(Ok(Message::Ping(payload))) => {
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
    log::info!("Browser WebSocket disconnected: connection_id={connection_id}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signaling_server::COMMAND_CAPACITY;

    #[tokio::test]
    async fn websocket_session_sends_close_when_stop_signal_fires() {
        let (stop_tx, stop_rx) = watch::channel(());
        let (commands, mut command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (server_io, client_io) = tokio::io::duplex(4096);
        let (server_ws, mut client_ws) = tokio::join!(
            WebSocketStream::from_raw_socket(server_io, Role::Server, None),
            WebSocketStream::from_raw_socket(client_io, Role::Client, None),
        );

        let session = tokio::spawn(ws_session(stop_rx, server_ws, 42, commands));
        let output = match command_rx.recv().await.unwrap() {
            DriverCommand::Connected {
                connection_id: 42,
                output,
            } => output,
            _ => panic!("expected connected command"),
        };

        stop_tx.send(()).unwrap();
        let close = tokio::time::timeout(Duration::from_secs(5), client_ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(close, Message::Close(_)));
        tokio::time::timeout(Duration::from_secs(5), session)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            command_rx.recv().await,
            Some(DriverCommand::Disconnected {
                connection_id: 42,
                ..
            })
        ));
        drop(output);
    }
}
