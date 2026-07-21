//! Standalone AppWeb HTTP/API server using a remote signaling authority.
use anyhow::{Result, bail};
use apprtc::{TlsListener, tls_config};
use appweb::config::Config;
use appweb::grpc_client::GrpcAuthority;
use appweb::room_server::RoomServer;
use clap::Parser;
use env_logger::Target;
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;
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
    /// Public browser signaling WebSocket URL ending in /ws.
    #[arg(long)]
    ws_url: String,
    /// Private signaling gRPC origin used by AppWeb room operations.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    grpc_url: String,
    /// Disable signaling gRPC certificate verification for local development.
    #[arg(long)]
    insecure_tls: bool,
    #[arg(long)]
    tls: bool,
    #[arg(long, default_value_t = String::new())]
    certificate: String,
    #[arg(long, default_value_t = String::new())]
    private_key: String,
    #[arg(long = "ice-server-url", value_delimiter = ',')]
    ice_server_urls: Vec<String>,
    #[arg(long, default_value_t = String::new())]
    ice_server_base_url: String,
    #[arg(long, default_value_t = String::new())]
    ice_server_api_key: String,
    #[arg(long, default_value_t = String::new())]
    header_message: String,
    #[arg(long)]
    bypass_join_confirmation: bool,
    #[arg(short, long)]
    debug: bool,
    #[arg(short, long, default_value = "info")]
    level: Level,
    #[arg(short = 'o', long, default_value_t = String::new())]
    output_log_file: String,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Level {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl From<Level> for LevelFilter {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => Self::Error,
            Level::Warn => Self::Warn,
            Level::Info => Self::Info,
            Level::Debug => Self::Debug,
            Level::Trace => Self::Trace,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.debug {
        env_logger::Builder::new()
            .target(if cli.output_log_file.is_empty() {
                Target::Stdout
            } else {
                Target::Pipe(Box::new(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&cli.output_log_file)?,
                ))
            })
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{} [{}] {}:{} - {}",
                    chrono::Local::now().format("%Y/%m/%d %H:%M:%S%.6f"),
                    record.level(),
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.args()
                )
            })
            .filter_level(cli.level.into())
            .init();
    }
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
    let ws_url = Url::parse(cli.ws_url.trim_end_matches('/'))?;
    if !matches!(ws_url.scheme(), "ws" | "wss")
        || ws_url.host_str().is_none()
        || ws_url.path() != "/ws"
        || ws_url.query().is_some()
        || ws_url.fragment().is_some()
    {
        bail!("--ws-url must be a ws:// or wss:// URL ending in /ws");
    }
    let authority =
        GrpcAuthority::connect(&cli.grpc_url, cli.insecure_tls).map_err(|e| anyhow::anyhow!(e))?;
    let server = RoomServer::new(
        Config {
            web_root: cli.web_root,
            host: public_host,
            force_tls: scheme == "https",
            signaling_ws_url: ws_url.to_string(),
            ice_server_urls: cli.ice_server_urls,
            ice_server_base_url: cli.ice_server_base_url,
            ice_server_api_key: cli.ice_server_api_key,
            header_message: cli.header_message,
            bypass_join_confirmation: cli.bypass_join_confirmation,
            ..Default::default()
        },
        authority,
    )
    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let address: SocketAddr = format!("{}:{}", cli.host_ip, cli.port).parse()?;
    let listener = TcpListener::bind(address).await?;
    println!("AppWeb listening on {public}");
    let app = server.router();
    if cli.tls {
        axum::serve(
            TlsListener::new(listener, tls_config(&cli.certificate, &cli.private_key)?),
            app,
        )
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
    }
    Ok(())
}
