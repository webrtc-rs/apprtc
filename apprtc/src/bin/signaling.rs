//! Standalone browser/AppWeb signaling endpoint.
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let handle = ColliderHandle::spawn(Duration::from_secs(10));
    let listener = TcpListener::bind((cli.host_ip.as_str(), cli.port)).await?;
    println!(
        "Signaling WebSocket listening on ws://{}:{}/ws",
        cli.host_ip, cli.port
    );
    axum::serve(listener, router(handle.clone()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    let _ = handle.shutdown().await;
    Ok(())
}
