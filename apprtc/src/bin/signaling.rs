//! Standalone browser/AppWeb signaling endpoint.
//!
//! All I/O lives here (async Tokio tasks + `tokio-tungstenite`, keeping the SFU `chat`
//! example's architecture); the `signaling` crate is a pure Sans-I/O state machine.
#[path = "../signaling_server.rs"]
mod signaling_server;
#[allow(dead_code)]
#[path = "../tls.rs"]
mod tls;

use clap::Parser;
use env_logger::Target;
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

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
async fn main() -> anyhow::Result<()> {
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

    let tls_config = if cli.tls {
        Some(tls::config(&cli.certificate, &cli.private_key)?)
    } else {
        None
    };

    let listener = TcpListener::bind((cli.host_ip.as_str(), cli.port)).await?;
    println!(
        "Signaling browser WebSocket listening on {}://{}:{}/ws",
        if cli.tls { "wss" } else { "ws" },
        cli.host_ip,
        cli.port
    );
    println!(
        "Signaling control WebSocket listening on {}://{}:{}/app",
        if cli.tls { "wss" } else { "ws" },
        cli.host_ip,
        cli.port
    );

    let (stop_tx, stop_rx) = watch::channel(());
    let (commands_tx, commands_rx) = mpsc::channel(signaling_server::COMMAND_CAPACITY);

    // The event loop that owns the Sans-I/O Collider is a separate task to the accept loop
    let event_loop_handle = tokio::spawn(signaling_server::event_loop(
        stop_rx.clone(),
        commands_rx,
        REGISTER_TIMEOUT,
    ));
    let accept_loop_handle = tokio::spawn(signaling_server::accept_loop(
        stop_rx,
        commands_tx,
        listener,
        tls_config,
    ));

    println!("Press Ctrl-C to stop");
    let _ = tokio::signal::ctrl_c().await;
    println!("Wait for Signaling Server Gracefully Shutdown...");
    let _ = stop_tx.send(());
    let _ = accept_loop_handle.await;
    let _ = event_loop_handle.await;
    println!("signaling server is gracefully down");
    Ok(())
}
