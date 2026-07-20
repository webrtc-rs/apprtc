#![allow(dead_code)]

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use rustls::ClientConfig;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

pub const HOST: &str = "127.0.0.1";
pub const PORT: u16 = 8080;
pub const SIGNALING_PORT: u16 = 8081;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn text(&self) -> Result<&str> {
        std::str::from_utf8(&self.body).context("HTTP response body is not UTF-8")
    }

    pub fn json(&self) -> Result<Value> {
        serde_json::from_slice(&self.body).context("HTTP response body is not valid JSON")
    }
}

pub fn unique_room(prefix: &str) -> String {
    format!("{prefix}-{}", rand::random::<u64>())
}

pub async fn wait_for_server() -> Result<()> {
    let mut last_error = None;
    for _ in 0..50 {
        match http("GET", "/status", &[]).await {
            Ok(response) if response.status == 200 => return Ok(()),
            Ok(response) => {
                last_error = Some(anyhow!("status endpoint returned {}", response.status))
            }
            Err(error) => last_error = Some(error),
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(last_error.unwrap_or_else(|| anyhow!("AppRTC server did not become ready")))
}

pub async fn http(method: &str, path: &str, body: &[u8]) -> Result<HttpResponse> {
    http_with_headers(method, path, body, &[]).await
}

pub async fn http_with_headers(
    method: &str,
    path: &str,
    body: &[u8],
    headers: &[(&str, &str)],
) -> Result<HttpResponse> {
    ensure_crypto_provider();
    let stream = timeout(IO_TIMEOUT, TcpStream::connect((HOST, PORT)))
        .await
        .context("timed out connecting to AppRTC")??;
    let server_name = HOST
        .to_string()
        .try_into()
        .map_err(|_| anyhow!("invalid TLS server name {HOST}"))?;
    let mut stream = timeout(
        IO_TIMEOUT,
        TlsConnector::from(tls_config()?).connect(server_name, stream),
    )
    .await
    .context("timed out during AppRTC TLS handshake")??;
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {HOST}:{PORT}\r\n");
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    timeout(IO_TIMEOUT, stream.write_all(request.as_bytes()))
        .await
        .context("timed out writing HTTP headers")??;
    timeout(IO_TIMEOUT, stream.write_all(body))
        .await
        .context("timed out writing HTTP body")??;
    let mut bytes = Vec::new();
    timeout(IO_TIMEOUT, stream.read_to_end(&mut bytes))
        .await
        .context("timed out reading HTTP response")??;
    parse_http_response(&bytes)
}

pub async fn join_v2(room_id: u64) -> Result<Value> {
    let response = http("POST", &format!("/v2/join/{room_id}"), &[]).await?;
    if response.status != 200 {
        bail!(
            "V2 join returned HTTP {}: {}",
            response.status,
            response.text()?
        );
    }
    response.json()
}

pub async fn join(room_id: &str) -> Result<Value> {
    let response = http("POST", &format!("/join/{room_id}"), &[]).await?;
    if response.status != 200 {
        bail!(
            "join returned HTTP {}: {}",
            response.status,
            response.text()?
        );
    }
    response.json()
}

pub async fn ws_register(room_id: &str, client_id: &str) -> Result<WsStream> {
    let mut socket = ws_connect().await?;
    socket
        .send(Message::text(
            serde_json::json!({
                "cmd": "register",
                "roomid": room_id,
                "clientid": client_id,
            })
            .to_string(),
        ))
        .await?;
    // V1 registration is deliberately silent. Give the owner task time to apply it before
    // another independent HTTP or WebSocket client acts on the same room.
    sleep(Duration::from_millis(50)).await;
    Ok(socket)
}

pub async fn ws_register_v2(
    room_id: u64,
    client_id: u64,
    admission_token: &str,
) -> Result<(WsStream, Value)> {
    let mut socket = ws_connect().await?;
    ws_send(
        &mut socket,
        serde_json::json!({
            "cmd": "register",
            "roomid": room_id.to_string(),
            "clientid": client_id.to_string(),
            "ver": 2,
            "token": admission_token,
        }),
    )
    .await?;
    let registered = ws_receive_json(&mut socket).await?;
    Ok((socket, registered))
}

pub async fn ws_connect() -> Result<WsStream> {
    ws_connect_path("/ws").await
}

pub async fn ws_connect_path(path: &str) -> Result<WsStream> {
    ensure_crypto_provider();
    let request = format!("wss://{HOST}:{SIGNALING_PORT}{path}").into_client_request()?;
    let connector = Connector::Rustls(tls_config()?);
    let (socket, response) = timeout(
        IO_TIMEOUT,
        tokio_tungstenite::connect_async_tls_with_config(request, None, false, Some(connector)),
    )
    .await
    .context("timed out connecting to AppRTC WebSocket")??;
    if response.status() != 101 {
        bail!("WebSocket upgrade returned {}", response.status());
    }
    Ok(socket)
}

pub async fn ws_send_binary(socket: &mut WsStream, bytes: Vec<u8>) -> Result<()> {
    timeout(IO_TIMEOUT, socket.send(Message::binary(bytes)))
        .await
        .context("timed out sending binary WebSocket frame")??;
    Ok(())
}

pub async fn ws_receive_binary(socket: &mut WsStream) -> Result<Vec<u8>> {
    loop {
        let message = timeout(IO_TIMEOUT, socket.next())
            .await
            .context("timed out waiting for binary WebSocket frame")?
            .ok_or_else(|| anyhow!("WebSocket closed before a binary frame arrived"))??;
        match message {
            Message::Binary(bytes) => return Ok(bytes.to_vec()),
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Pong(_) => {}
            Message::Close(frame) => {
                bail!("WebSocket closed before a binary frame arrived: {frame:?}")
            }
            Message::Text(_) | Message::Frame(_) => {}
        }
    }
}

pub async fn ws_send(socket: &mut WsStream, value: Value) -> Result<()> {
    timeout(IO_TIMEOUT, socket.send(Message::text(value.to_string())))
        .await
        .context("timed out sending WebSocket frame")??;
    Ok(())
}

pub async fn ws_receive_json(socket: &mut WsStream) -> Result<Value> {
    loop {
        let message = timeout(IO_TIMEOUT, socket.next())
            .await
            .context("timed out waiting for WebSocket frame")?
            .ok_or_else(|| anyhow!("WebSocket closed before a JSON frame arrived"))??;
        match message {
            Message::Text(text) => {
                return serde_json::from_str(&text).context("invalid JSON frame");
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Pong(_) => {}
            Message::Close(frame) => {
                bail!("WebSocket closed before a JSON frame arrived: {frame:?}")
            }
            Message::Binary(_) | Message::Frame(_) => {}
        }
    }
}

pub async fn ws_expect_close(socket: &mut WsStream) -> Result<()> {
    loop {
        let message = timeout(IO_TIMEOUT, socket.next())
            .await
            .context("timed out waiting for WebSocket close")?;
        match message {
            Some(Ok(Message::Close(_))) | None => return Ok(()),
            Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
            Some(Ok(_)) => {}
            Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => return Ok(()),
            Some(Err(WsError::Io(error)))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                return Ok(());
            }
            Some(Err(error)) => return Err(error.into()),
        }
    }
}

fn tls_config() -> Result<Arc<ClientConfig>> {
    Ok(Arc::new(
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(danger::NoCertVerification))
            .with_no_client_auth(),
    ))
}

mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error, SignatureScheme};

    #[derive(Debug)]
    pub struct NoCertVerification;

    impl ServerCertVerifier for NoCertVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _certificate: &CertificateDer<'_>,
            _signature: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _certificate: &CertificateDer<'_>,
            _signature: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            rustls::crypto::ring::default_provider()
                .signature_verification_algorithms
                .supported_schemes()
        }
    }
}

fn ensure_crypto_provider() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn parse_http_response(bytes: &[u8]) -> Result<HttpResponse> {
    let header_end = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("HTTP response has no header terminator"))?;
    let header = std::str::from_utf8(&bytes[..header_end]).context("HTTP headers are not UTF-8")?;
    let mut lines = header.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| anyhow!("HTTP response has no status"))?
        .parse::<u16>()?;
    let mut headers = HashMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| anyhow!("malformed HTTP header: {line}"))?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_string());
    }
    let raw_body = &bytes[header_end + 4..];
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        decode_chunked(raw_body)?
    } else {
        raw_body.to_vec()
    };
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn decode_chunked(mut bytes: &[u8]) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        let line_end = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .ok_or_else(|| anyhow!("malformed chunk length"))?;
        let line = std::str::from_utf8(&bytes[..line_end])?;
        let size = usize::from_str_radix(line.split(';').next().unwrap_or_default(), 16)?;
        bytes = &bytes[line_end + 2..];
        if size == 0 {
            return Ok(body);
        }
        if bytes.len() < size + 2 || &bytes[size..size + 2] != b"\r\n" {
            bail!("malformed HTTP chunk");
        }
        body.extend_from_slice(&bytes[..size]);
        bytes = &bytes[size + 2..];
    }
}
