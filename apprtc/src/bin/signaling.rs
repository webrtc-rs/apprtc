//! Standalone browser/AppWeb signaling endpoint.
#[path = "../tls.rs"]
mod tls;
use clap::Parser;
use env_logger::Target;
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;
use signaling::ws_server::{ColliderHandle, router};
use std::time::Duration;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host_ip: String,
    #[arg(short, long, default_value_t = 8081)]
    port: u16,
    /// Public WSS/WS origin advertised to AppWeb deployments.
    #[arg(long, default_value_t = String::new())]
    public_url: String,
    #[arg(long)]
    tls: bool,
    #[arg(long, default_value_t = String::new())]
    certificate: String,
    #[arg(long, default_value_t = String::new())]
    private_key: String,
    #[arg(short, long)]
    debug: bool,
    #[arg(short, long, default_value = "info")]
    level: Level,
    #[arg(short = 'o', long, default_value_t = String::new())]
    output_log_file: String,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Level { Error, Warn, Info, Debug, Trace }
impl From<Level> for LevelFilter { fn from(level: Level) -> Self { match level { Level::Error => Self::Error, Level::Warn => Self::Warn, Level::Info => Self::Info, Level::Debug => Self::Debug, Level::Trace => Self::Trace } } }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if cli.debug {
        env_logger::Builder::new()
            .target(if cli.output_log_file.is_empty() { Target::Stdout } else { Target::Pipe(Box::new(OpenOptions::new().create(true).append(true).open(&cli.output_log_file)?)) })
            .format(|buf, record| writeln!(buf, "{} [{}] {}:{} - {}", chrono::Local::now().format("%Y/%m/%d %H:%M:%S%.6f"), record.level(), record.file().unwrap_or("unknown"), record.line().unwrap_or(0), record.args()))
            .filter_level(cli.level.into())
            .init();
    }
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
