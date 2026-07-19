//! Standalone browser/AppWeb signaling endpoint.
#[path = "../tls.rs"]
mod tls;
use clap::Parser;
use signaling::ws_server::{ColliderHandle, router};
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host_ip: String,
    #[arg(short, long, default_value_t = 8081)]
    port: u16,
    #[arg(long)]
    tls: bool,
    #[arg(long, default_value_t = String::new())]
    certificate: String,
    #[arg(long, default_value_t = String::new())]
    private_key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let handle = ColliderHandle::spawn(Duration::from_secs(10));
    let listener = TcpListener::bind((cli.host_ip.as_str(), cli.port)).await?;
    let app = router(handle.clone());
    if cli.tls {
        println!("Signaling WebSocket listening on wss://{}:{}/ws", cli.host_ip, cli.port);
        axum::serve(listener.with_tls(tls::config(&cli.certificate, &cli.private_key)?), app)
            .with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; })
            .await?;
    } else {
        println!("Signaling WebSocket listening on ws://{}:{}/ws", cli.host_ip, cli.port);
        axum::serve(listener, app)
            .with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; })
            .await?;
    }
    let _ = handle.shutdown().await;
    Ok(())
}

trait TcpListenerTls {
    fn with_tls(self, config: std::sync::Arc<rustls::ServerConfig>) -> tls::TlsListener;
}
impl TcpListenerTls for TcpListener {
    fn with_tls(self, config: std::sync::Arc<rustls::ServerConfig>) -> tls::TlsListener {
        tls::TlsListener::new(self, config)
    }
}
