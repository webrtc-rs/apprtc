use serde::Serialize;

/// The synthetic peer added in loopback debug mode (was
/// `constants.LOOPBACK_CLIENT_ID`).
pub const LOOPBACK_CLIENT_ID: &str = "LOOPBACK_CLIENT_ID";

/// Mirrors a single entry of the `RTCConfiguration.iceServers` array.
#[derive(Debug, Clone, Default, Serialize)]
pub struct IceServer {
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub username: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub credential: String,
}

/// Holds the server configuration that used to live in the App Engine room
/// server (`constants.py` + `app.yaml` env_variables). It is populated from
/// flags/env by the binary entry point and consumed by the room server handlers.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Path to the `web_app/` directory served as static assets.
    pub web_root: String,

    /// If set, overrides the public host:port used to build self URLs
    /// (`wss_url`/`wss_post_url`/`room_link`). When empty the request Host is used.
    pub host: String,

    /// If true, builds https/wss self URLs even when the incoming request looks
    /// like plain HTTP (e.g. behind a TLS-terminating proxy).
    pub force_tls: bool,

    /// If non-empty, returned verbatim from `/v1alpha/iceconfig` and used as the
    /// `iceServers` of the peer connection config (was `constants.ICE_SERVER_OVERRIDE`).
    pub ice_server_override: Vec<IceServer>,

    /// The list of ICE urls returned from `/v1alpha/iceconfig` when no override is
    /// set (was `constants.ICE_SERVER_URLS`).
    pub ice_server_urls: Vec<String>,

    /// The origin of the ICE server provider used to build `ice_server_url`. When
    /// empty, the server's own origin is used so the page fetches
    /// `/v1alpha/iceconfig` from this binary.
    pub ice_server_base_url: String,

    /// The api key appended to `ice_server_url`.
    pub ice_server_api_key: String,

    /// An optional banner shown on every page (was `HEADER_MESSAGE`).
    pub header_message: String,

    /// Skips the "Ready to join?" prompt (was `BYPASS_JOIN_CONFIRMATION`).
    pub bypass_join_confirmation: bool,

    /// How long an idle room is kept before the sweeper reaps it (was
    /// `ROOM_MEMCACHE_EXPIRATION_SEC`). 0 disables the sweeper.
    pub room_max_age_sec: i64,
}
