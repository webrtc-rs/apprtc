//! Standalone browser/AppWeb signaling endpoint.
//!
//! All I/O lives here (blocking threads + `tungstenite`, following the SFU `chat`
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
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

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

fn main() -> anyhow::Result<()> {
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

    let listener = TcpListener::bind((cli.host_ip.as_str(), cli.port))?;
    println!(
        "Signaling WebSocket listening on {}://{}:{}/ws",
        if cli.tls { "wss" } else { "ws" },
        cli.host_ip,
        cli.port
    );

    let (stop_tx, stop_rx) = crossbeam_channel::bounded::<()>(1);
    let (commands_tx, commands_rx) = mpsc::sync_channel(signaling_server::COMMAND_CAPACITY);

    // The run loop that owns the Sans-I/O Collider is on a separate thread to the
    // accept loop, exactly like the chat example's media run loops.
    let event_loop_handle = {
        let stop_rx = stop_rx.clone();
        std::thread::spawn(move || {
            signaling_server::event_loop(stop_rx, commands_rx, REGISTER_TIMEOUT)
        })
    };
    let serve_io_handle = {
        let stop_rx = stop_rx.clone();
        std::thread::spawn(move || {
            signaling_server::serve_io(stop_rx, listener, tls_config, commands_tx)
        })
    };

    println!("Press Ctrl-C to stop");
    let mut stop_tx = Some(stop_tx);
    ctrlc::set_handler(move || {
        if let Some(stop_tx) = stop_tx.take() {
            let _ = stop_tx.send(());
        }
    })?;
    let _ = stop_rx.recv();
    println!("Wait for Signaling Server Gracefully Shutdown...");
    let _ = serve_io_handle.join();
    let _ = event_loop_handle.join();
    println!("signaling server is gracefully down");
    Ok(())
}
