use appweb::config::Config;
use appweb::webserver::RoomServer;
use appweb::wsclient::WsClient;
use clap::Parser;
use env_logger::Target;
use signaling::wsserver::{ColliderHandle, router as signaling_router};
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

const REGISTER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Default, Debug, Copy, Clone, clap::ValueEnum)]
enum Level {
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl From<Level> for log::LevelFilter {
    fn from(level: Level) -> Self {
        match level {
            Level::Error => log::LevelFilter::Error,
            Level::Warn => log::LevelFilter::Warn,
            Level::Info => log::LevelFilter::Info,
            Level::Debug => log::LevelFilter::Debug,
            Level::Trace => log::LevelFilter::Trace,
        }
    }
}

#[derive(Debug, Parser)]
#[command(name = "AppRTC Server")]
#[command(author, version)]
#[command(about = "AppRTC P2P/SFU signaling server", long_about = None)]
struct Cli {
    /// Local interface on which the HTTP/WebSocket server listens.
    #[arg(long, default_value_t = format!("0.0.0.0"))]
    host: String,

    /// Local HTTP/WebSocket listening port.
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Path to the AppRTC web application assets.
    #[arg(long, default_value_t = format!("appweb"))]
    web_root: String,

    /// Public host[:port] used in generated room and WebSocket URLs.
    #[arg(long, default_value_t = String::new())]
    public_host: String,

    /// Generate https/wss public URLs when TLS terminates at a proxy.
    #[arg(long)]
    force_tls: bool,

    /// ICE server URL; repeat the option or use comma-separated values.
    #[arg(long = "ice-server-url", value_delimiter = ',')]
    ice_server_urls: Vec<String>,

    /// Optional external ICE credential service origin.
    #[arg(long, default_value_t = String::new())]
    ice_server_base_url: String,

    /// API key appended to the ICE credential service URL.
    #[arg(long, default_value_t = String::new())]
    ice_server_api_key: String,

    /// Banner displayed by the AppRTC page.
    #[arg(long, default_value_t = String::new())]
    header_message: String,

    /// Skip the browser's ready-to-join confirmation.
    #[arg(long)]
    bypass_join_confirmation: bool,

    /// Enable application logging.
    #[arg(short, long)]
    debug: bool,

    /// Maximum log level used when --debug is enabled.
    #[arg(short, long, default_value_t = Level::Info, value_enum)]
    level: Level,

    /// Write logs to this file instead of stdout.
    #[arg(short, long, default_value_t = String::new())]
    output_log_file: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_log(&cli)?;

    let collider = ColliderHandle::spawn(REGISTER_TIMEOUT);
    let config = Config {
        web_root: cli.web_root,
        host: cli.public_host,
        force_tls: cli.force_tls,
        ice_server_urls: cli.ice_server_urls,
        ice_server_base_url: cli.ice_server_base_url,
        ice_server_api_key: cli.ice_server_api_key,
        header_message: cli.header_message,
        bypass_join_confirmation: cli.bypass_join_confirmation,
        ..Default::default()
    };
    let room_server = RoomServer::new(config, WsClient::new(collider.clone()))
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let app = room_server.router().merge(signaling_router(collider));
    let address = format!("{}:{}", cli.host, cli.port);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    log::info!("AppRTC listening on {address}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn init_log(cli: &Cli) -> anyhow::Result<()> {
    if cli.debug {
        env_logger::Builder::new()
            .target(if !cli.output_log_file.is_empty() {
                Target::Pipe(Box::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(&cli.output_log_file)?,
                ))
            } else {
                Target::Stdout
            })
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} [{}] {} - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.level(),
                    chrono::Local::now().format("%H:%M:%S.%6f"),
                    record.args()
                )
            })
            .filter(None, cli.level.into())
            .init();
    }
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
