#![warn(rust_2018_idioms)]
#![allow(dead_code)]

use anyhow::{Context, Result, bail};
use axum::serve::Listener;
use rustls::ServerConfig;
use rustls::pki_types::CertificateDer;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

pub mod grpc_server;
pub mod sfu_server;
pub mod signaling_server;
pub mod ws_server;

pub fn tls_pem(certificate: &str, private_key: &str) -> Result<(Vec<u8>, Vec<u8>)> {
    if certificate.is_empty() && private_key.is_empty() {
        Ok((
            include_bytes!("../scripts/cert.pem").to_vec(),
            include_bytes!("../scripts/key.pem").to_vec(),
        ))
    } else if !certificate.is_empty() && !private_key.is_empty() {
        Ok((
            std::fs::read(certificate)
                .with_context(|| format!("failed to read certificate {certificate}"))?,
            std::fs::read(private_key)
                .with_context(|| format!("failed to read private key {private_key}"))?,
        ))
    } else {
        bail!("--certificate and --private-key must be supplied together")
    }
}

pub fn tls_config(certificate: &str, private_key: &str) -> Result<Arc<ServerConfig>> {
    let (certificate, private_key) = tls_pem(certificate, private_key)?;
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
    address: SocketAddr,
    streams: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
}

impl TlsListener {
    pub fn new(listener: TcpListener, config: Arc<ServerConfig>) -> Self {
        let address = listener.local_addr().expect("TLS listener local address");
        let acceptor = TlsAcceptor::from(config);
        let (sender, streams) = mpsc::channel(1024);
        tokio::spawn(async move {
            loop {
                let (stream, peer) = match listener.accept().await {
                    Ok(value) => value,
                    Err(error) => {
                        log::error!("TCP accept failed: {error}");
                        continue;
                    }
                };
                let acceptor = acceptor.clone();
                let sender = sender.clone();
                tokio::spawn(async move {
                    match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(stream))
                        .await
                    {
                        Ok(Ok(stream)) => {
                            let _ = sender.send((stream, peer)).await;
                        }
                        Ok(Err(error)) => log::warn!("TLS handshake from {peer} failed: {error}"),
                        Err(_) => log::warn!("TLS handshake from {peer} timed out"),
                    }
                });
            }
        });
        Self { address, streams }
    }
}

impl Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        self.streams.recv().await.expect("TLS accept loop stopped")
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.address)
    }
}
