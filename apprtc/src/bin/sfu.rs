//! Standalone SFU media worker using the V2 signaling gRPC session.

use apprtc::sfu_server::{self, Config};
use clap::Parser;
use env_logger::Target;
use log::LevelFilter;
use std::fs::OpenOptions;
use std::io::Write;
use std::net::IpAddr;
use tokio::sync::watch;

#[derive(Debug, Parser)]
#[command(name = "AppRTC SFU Server")]
#[command(author, version)]
#[command(about = "AppRTC Sans-I/O SFU media worker", long_about = None)]
struct Cli {
    /// IP address used to bind the UDP media sockets.
    #[arg(long, default_value = "127.0.0.1")]
    host_ip: IpAddr,

    /// IP address advertised to browsers in ICE candidates.
    #[arg(long, default_value = "127.0.0.1")]
    public_ip: IpAddr,

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

    let instance_id = if cli.instance_id.is_empty() {
        format!("sfu-{:032x}", rand::random::<u128>())
    } else {
        cli.instance_id
    };
    println!(
        "SFU media listening on {}:{}-{} (advertising {})",
        cli.host_ip, cli.media_port_min, cli.media_port_max, cli.public_ip
    );
    println!("SFU signaling session connecting to {}", cli.grpc_url);

    let (stop_tx, stop_rx) = watch::channel(());
    let mut service = tokio::spawn(sfu_server::run(
        stop_rx,
        Config {
            host_ip: cli.host_ip,
            public_ip: cli.public_ip,
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
            println!("SFU Server is gracefully down");
            Ok(())
        }
        result = &mut service => {
            result??;
            anyhow::bail!("SFU service stopped unexpectedly")
        }
    }
}
