//! Standalone AppWeb HTTP/API server using a remote signaling authority.
use anyhow::{Result, bail};
use appweb::config::Config;
use appweb::room_server::RoomServer;
use appweb::ws_client::WebSocketAuthority;
use clap::Parser;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use url::Url;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host_ip: String,
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
    #[arg(long, default_value = "appweb")]
    web_root: String,
    #[arg(long)]
    public_url: String,
    #[arg(long)]
    signaling_url: String,
    #[arg(long, default_value = "appweb-1")]
    appid: String,
    #[arg(long, default_value = "")]
    signaling_token: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let public = Url::parse(&cli.public_url)?;
    let scheme = public.scheme();
    if scheme != "http" && scheme != "https" {
        bail!("--public-url must use http or https")
    }
    let host = public
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("--public-url has no host"))?;
    let public_host = match public.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    let authority =
        WebSocketAuthority::connect(&cli.signaling_url, &cli.appid, &cli.signaling_token)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    let server = RoomServer::new(
        Config {
            web_root: cli.web_root,
            host: public_host,
            force_tls: scheme == "https",
            ..Default::default()
        },
        authority,
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let address: SocketAddr = format!("{}:{}", cli.host_ip, cli.port).parse()?;
    let listener = TcpListener::bind(address).await?;
    println!("AppWeb listening on {public}");
    axum::serve(listener, server.router())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
