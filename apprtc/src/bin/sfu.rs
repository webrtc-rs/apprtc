//! Standalone SFU media worker using the V2 signaling gRPC session.

use apprtc::sfu_server::{self, Config};
use apprtc::{TlsListener, tls_config};
use axum::Router;
use axum::response::Redirect;
use clap::Parser;
use env_logger::Target;
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::{IpAddr, SocketAddr};
use tokio::net::TcpListener;
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(name = "AppRTC SFU Server")]
#[command(author, version)]
#[command(about = "AppRTC Sans-I/O SFU media worker", long_about = None)]
struct Cli {
    /// IP address used to bind the UDP media sockets.
    #[arg(long, default_value = "127.0.0.1")]
    host_ip: IpAddr,

    /// Port for the optional HTTP/HTTPS redirect server (see `--redirect-url`).
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Serve the redirect endpoint over HTTPS instead of HTTP.
    #[arg(long)]
    tls: bool,

    /// When set, run a server on `--host-ip:--port` (HTTPS if `--tls`, else HTTP) that redirects
    /// every request to this URL — e.g. the AppWeb landing page. Disabled when empty.
    #[arg(long, default_value_t = String::new())]
    redirect_url: String,

    /// TLS certificate chain (PEM path) for `--tls`; a bundled self-signed cert is used when empty.
    #[arg(long, default_value_t = String::new())]
    certificate: String,

    /// TLS private key (PEM path) for `--tls`; a bundled self-signed key is used when empty.
    #[arg(long, default_value_t = String::new())]
    private_key: String,

    /// IP address advertised to browsers in ICE candidates. Defaults to `--host-ip` when omitted
    /// (set it only when the advertised address differs from the bind address, e.g. behind NAT).
    #[arg(long)]
    media_public_ip: Option<IpAddr>,

    #[arg(long, default_value_t = 3478)]
    media_port_min: u16,

    #[arg(long, default_value_t = 3497)]
    media_port_max: u16,

    /// Private signaling gRPC origin.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    grpc_url: String,

    /// Disable signaling gRPC certificate verification for local development.
    #[arg(long)]
    insecure_tls: bool,

    #[arg(long, default_value_t = 1_000)]
    max_rooms: u64,

    #[arg(long, default_value_t = 10_000)]
    max_clients: u64,

    /// Stable identity for this process incarnation; generated when omitted.
    #[arg(long, default_value_t = String::new())]
    instance_id: String,

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
                        .write(true)
                        .truncate(true)
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

    let instance_id = if cli.instance_id.is_empty() {
        format!("sfu-{:032x}", rand::random::<u128>())
    } else {
        cli.instance_id
    };
    // The advertised (ICE) address defaults to the bind address unless overridden for NAT.
    let media_ip = cli.media_public_ip.unwrap_or(cli.host_ip);
    println!(
        "SFU media listening on {}:{}-{} (advertising {})",
        cli.host_ip, cli.media_port_min, cli.media_port_max, media_ip
    );
    println!("SFU signaling session connecting to {}", cli.grpc_url);

    let (stop_tx, stop_rx) = watch::channel(());

    // Optional HTTP/HTTPS redirect server on --host-ip:--port.
    let redirect = if cli.redirect_url.is_empty() {
        None
    } else {
        let address = SocketAddr::new(cli.host_ip, cli.port);
        println!(
            "SFU redirect listening on {}://{address} -> {}",
            if cli.tls { "https" } else { "http" },
            cli.redirect_url
        );
        Some(tokio::spawn(run_redirect(
            address,
            cli.redirect_url.clone(),
            cli.tls,
            cli.certificate.clone(),
            cli.private_key.clone(),
            stop_rx.clone(),
        )))
    };

    let mut service = tokio::spawn(sfu_server::run(
        stop_rx,
        Config {
            host_ip: cli.host_ip,
            media_ip,
            media_port_min: cli.media_port_min,
            media_port_max: cli.media_port_max,
            grpc_url: cli.grpc_url,
            insecure_tls: cli.insecure_tls,
            max_rooms: cli.max_rooms,
            max_clients: cli.max_clients,
            instance_id,
        },
    ));

    println!("Press Ctrl-C to stop");
    tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            signal?;
            println!("Wait for SFU Server Gracefully Shutdown...");
            let _ = stop_tx.send(());
            service.await??;
            if let Some(redirect) = redirect {
                redirect.await??;
            }
            println!("SFU Server is gracefully down");
            Ok(())
        }
        result = &mut service => {
            result??;
            anyhow::bail!("SFU service stopped unexpectedly")
        }
    }
}

/// Run an HTTP/HTTPS server on `address` that redirects every request to `redirect_url`, shutting
/// down gracefully when `stop` fires. Uses a bundled self-signed cert when `certificate` /
/// `private_key` are empty.
async fn run_redirect(
    address: SocketAddr,
    redirect_url: String,
    tls: bool,
    certificate: String,
    private_key: String,
    mut stop: watch::Receiver<()>,
) -> anyhow::Result<()> {
    let app = Router::new().fallback(move || {
        let redirect_url = redirect_url.clone();
        async move { Redirect::temporary(&redirect_url) }
    });
    let listener = TcpListener::bind(address).await?;
    let shutdown = async move {
        let _ = stop.changed().await;
    };
    if tls {
        axum::serve(
            TlsListener::new(listener, tls_config(&certificate, &private_key)?),
            app,
        )
        .with_graceful_shutdown(shutdown)
        .await?;
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await?;
    }
    Ok(())
}
