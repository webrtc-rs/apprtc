use anyhow::{Context, Result, bail};
use axum::serve::Listener;
use rustls::ServerConfig;
use rustls::pki_types::CertificateDer;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

pub fn config(certificate: &str, private_key: &str) -> Result<Arc<ServerConfig>> {
    let (certificate, private_key) = if certificate.is_empty() && private_key.is_empty() {
        (
            include_bytes!("../cert/cert.pem").to_vec(),
            include_bytes!("../cert/key.pem").to_vec(),
        )
    } else if !certificate.is_empty() && !private_key.is_empty() {
        (
            std::fs::read(certificate)
                .with_context(|| format!("failed to read certificate {certificate}"))?,
            std::fs::read(private_key)
                .with_context(|| format!("failed to read private key {private_key}"))?,
        )
    } else {
        bail!("--certificate and --private-key must be supplied together");
    };
    let certificates: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(&certificate[..])).collect::<Result<_, _>>()?;
    let private_key = rustls_pemfile::private_key(&mut BufReader::new(&private_key[..]))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in PEM input"))?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    Ok(Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)?,
    ))
}

pub struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    pub fn new(listener: TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            listener,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

impl Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.listener.accept().await {
                Ok((stream, address)) => match tokio::time::timeout(
                    Duration::from_secs(10),
                    self.acceptor.accept(stream),
                )
                .await
                {
                    Ok(Ok(stream)) => return (stream, address),
                    Ok(Err(error)) => log::warn!("TLS handshake from {address} failed: {error}"),
                    Err(_) => log::warn!("TLS handshake from {address} timed out"),
                },
                Err(error) => {
                    log::error!("TCP accept failed: {error}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
}
