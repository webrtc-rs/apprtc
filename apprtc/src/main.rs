use appweb::config::Config;
use appweb::webserver::RoomServer;
use appweb::wsclient::WsClient;
use clap::Parser;
use env_logger::Target;
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use signaling::wsserver::{ColliderHandle, router as signaling_router};
use std::fs::OpenOptions;
use std::io::BufReader;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

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
    /// Host used both for the listener and generated HTTP/WebSocket URLs.
    #[arg(long, default_value_t = format!("127.0.0.1"))]
    host: String,

    /// Local HTTP/WebSocket listening port.
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Path to the AppRTC web application assets.
    #[arg(long, default_value_t = format!("appweb"))]
    web_root: String,

    /// Serve HTTPS/WSS instead of HTTP/WS.
    #[arg(long)]
    tls: bool,

    /// PEM certificate chain used by --tls; defaults to the bundled development certificate.
    #[arg(long, default_value_t = String::new())]
    certificate: String,

    /// PEM private key used by --tls; defaults to the bundled development key.
    #[arg(long, default_value_t = String::new())]
    private_key: String,

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
    let tls = if cli.tls {
        Some(tls_config(&cli)?)
    } else {
        None
    };
    let address = format!("{}:{}", cli.host, cli.port);

    let collider = ColliderHandle::spawn(REGISTER_TIMEOUT);
    let config = Config {
        web_root: cli.web_root,
        host: address.clone(),
        force_tls: cli.tls,
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
    let listener = TcpListener::bind(&address).await?;
    if let Some(tls) = tls {
        if cli.certificate.is_empty() {
            println!(
                "Using bundled self-signed development certificate; trust {}/cert/cert.pem before opening the site",
                env!("CARGO_MANIFEST_DIR")
            );
        }
        println!("AppRTC listening on https://{address}");
        axum::serve(TlsListener::new(listener, tls), app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    } else {
        println!("AppRTC listening on http://{address}");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }
    Ok(())
}

fn tls_config(cli: &Cli) -> anyhow::Result<Arc<ServerConfig>> {
    let (certificate, private_key) = match (cli.certificate.is_empty(), cli.private_key.is_empty())
    {
        (true, true) => (
            include_bytes!("../cert/cert.pem").to_vec(),
            include_bytes!("../cert/key.pem").to_vec(),
        ),
        (false, false) => (
            std::fs::read(&cli.certificate).map_err(|error| {
                anyhow::anyhow!("failed to read certificate {}: {error}", cli.certificate)
            })?,
            std::fs::read(&cli.private_key).map_err(|error| {
                anyhow::anyhow!("failed to read private key {}: {error}", cli.private_key)
            })?,
        ),
        _ => anyhow::bail!("--certificate and --private-key must be supplied together"),
    };

    let certificates: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(&certificate[..])).collect::<Result<_, _>>()?;
    let private_key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut BufReader::new(&private_key[..]))?
            .ok_or_else(|| anyhow::anyhow!("no private key found in PEM input"))?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    Ok(Arc::new(
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)?,
    ))
}

struct TlsListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

impl TlsListener {
    fn new(listener: TcpListener, config: Arc<ServerConfig>) -> Self {
        Self {
            listener,
            acceptor: TlsAcceptor::from(config),
        }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.listener.accept().await {
                Ok((stream, address)) => match self.acceptor.accept(stream).await {
                    Ok(stream) => return (stream, address),
                    Err(error) => log::debug!("TLS handshake from {address} failed: {error}"),
                },
                Err(error) => {
                    log::error!("TCP accept failed: {error}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.listener.local_addr()
    }
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
