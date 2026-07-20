//! Standalone browser/AppWeb signaling endpoint.
//!
//! All I/O lives here (async Tokio tasks + `tokio-tungstenite`, keeping the SFU `chat`
//! example's architecture); the `signaling` crate is a pure Sans-I/O state machine.

use apprtc::{grpc_server, signaling_server, tls, ws_server};
use clap::Parser;
use env_logger::Target;
use log::LevelFilter;
use signaling_proto::v2::signaling_service_server::SignalingServiceServer;
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};

const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Parser)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host_ip: String,
    #[arg(short, long, default_value_t = 8081)]
    port: u16,
    /// Private gRPC listening port used by AppWeb and future SFU workers.
    #[arg(long, default_value_t = 50051)]
    grpc_port: u16,
    /// Optional bearer token required by private gRPC calls.
    #[arg(long, default_value_t = String::new())]
    grpc_token: String,
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
    let grpc_listener = TcpListener::bind((cli.host_ip.as_str(), cli.grpc_port)).await?;
    println!(
        "Signaling browser WebSocket listening on {}://{}:{}/ws",
        if cli.tls { "wss" } else { "ws" },
        cli.host_ip,
        cli.port
    );
    println!(
        "Signaling gRPC listening on {}://{}:{}",
        if cli.tls { "https" } else { "http" },
        cli.host_ip,
        cli.grpc_port
    );

    let (io_stop_tx, io_stop_rx) = watch::channel(());
    let (event_stop_tx, event_stop_rx) = watch::channel(());
    let (commands_tx, commands_rx) = mpsc::channel(signaling_server::COMMAND_CAPACITY);

    // The event loop that owns the Sans-I/O Collider is a separate task to the accept loop
    let event_loop_handle = tokio::spawn(signaling_server::event_loop(
        event_stop_rx,
        commands_rx,
        REGISTER_TIMEOUT,
    ));
    let accept_loop_handle = tokio::spawn(ws_server::accept_loop(
        io_stop_rx.clone(),
        commands_tx.clone(),
        listener,
        tls_config,
    ));
    let grpc_service = grpc_server::GrpcSignalingService::new(commands_tx, cli.grpc_token);
    let mut grpc_server = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)))
        .tcp_keepalive(Some(Duration::from_secs(30)));
    if cli.tls {
        let (certificate, private_key) = tls::pem(&cli.certificate, &cli.private_key)?;
        grpc_server = grpc_server.tls_config(
            ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key)),
        )?;
    }
    let mut grpc_stop_rx = io_stop_rx;
    let grpc_handle = tokio::spawn(async move {
        grpc_server
            .add_service(SignalingServiceServer::new(grpc_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), async move {
                let _ = grpc_stop_rx.changed().await;
            })
            .await
    });

    println!("Press Ctrl-C to stop");
    let _ = tokio::signal::ctrl_c().await;
    println!("Wait for Signaling Server Gracefully Shutdown...");
    let _ = io_stop_tx.send(());
    let _ = accept_loop_handle.await;
    match grpc_handle.await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => log::error!("signaling gRPC server failed: {error}"),
        Err(error) => log::error!("signaling gRPC task failed: {error}"),
    }
    let _ = event_stop_tx.send(());
    let _ = event_loop_handle.await;
    println!("signaling server is gracefully down");
    Ok(())
}
