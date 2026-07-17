use rand::RngExt;
use serde::Serialize;
use serde_json::{Value, json};
use url::Url;

use crate::config::Config;

/// Port of `apprtc.py::get_room_parameters`. The field names match the keys the
/// web app expects (see `web_app/js/*.js` and the templates). The `*_json`-style
/// fields are pre-marshaled JSON strings that the templates inject verbatim (the
/// Jinja `| safe` filter), so the JS literals parse correctly.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RoomParameters {
    // Plain string params (default-escaped in templates).
    #[serde(skip_serializing_if = "String::is_empty")]
    pub client_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub room_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub room_link: String,
    pub wss_url: String,
    pub wss_post_url: String,
    pub ice_server_url: String,
    pub ice_server_transports: String,
    pub header_message: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub is_initiator: String,

    // JSON-valued params injected raw into the page (Jinja `| safe`).
    pub is_loopback: String,
    pub pc_config: String,
    pub pc_constraints: String,
    pub offer_options: String,
    pub media_constraints: String,
    pub bypass_join_confirmation: String,
    pub version_info: String,
    pub include_loopback_js: String,

    // Messages queued for the joining client + advisory messages.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub error_messages: Vec<String>,
    pub warning_messages: Vec<String>,
}

/// Marshal `v` to a JSON string, falling back to `"null"` on error (mirrors the
/// Go `mustJSON`).
fn must_json<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| "null".to_string())
}

/// A numeric string of the given length, mirroring `apprtc.py::generate_random`.
fn generate_random(length: usize) -> String {
    const DIGITS: &[u8] = b"0123456789";
    let mut rng = rand::rng();
    (0..length)
        .map(|_| DIGITS[rng.random_range(0..DIGITS.len())] as char)
        .collect()
}

/// The first value of query parameter `key`, or `""` if absent (mirrors Go's
/// `url.Query().Get`).
fn query_get(url: &Url, key: &str) -> String {
    url.query_pairs()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default()
}

fn bool_str(b: bool) -> &'static str {
    if b { "true" } else { "false" }
}

impl Config {
    /// The (http scheme, ws scheme, host) this server is reachable at, honoring
    /// the `host`/`force_tls` config overrides. Mirrors `maybe_use_https_host_url`.
    /// `host` is the request's host, used only when `config.host` is unset. Unlike
    /// the Go version this cannot inspect the incoming connection, so TLS is driven
    /// solely by `force_tls` (set it when terminating TLS at a proxy).
    pub fn self_origin(&self, host: String) -> (String, String, String) {
        let host = if self.host.is_empty() {
            host
        } else {
            self.host.clone()
        };
        if self.force_tls {
            ("https".to_string(), "wss".to_string(), host)
        } else {
            ("http".to_string(), "ws".to_string(), host)
        }
    }

    /// Port of `get_room_parameters`. `room_id`/`client_id` may be empty (e.g. for
    /// the landing page or `/params`). `is_initiator` is `None` unless known. `host`
    /// is the request host and `url` supplies the query string.
    pub fn build_room_parameters(
        &self,
        host: String,
        url: &Url,
        room_id: &str,
        client_id: &str,
        is_initiator: Option<bool>,
    ) -> RoomParameters {
        let (http_scheme, ws_scheme, host) = self.self_origin(host);

        // Single, self-hosted server: the WSS server is this binary.
        let wss_url = format!("{ws_scheme}://{host}/ws");
        let wss_post_url = format!("{http_scheme}://{host}");

        // pc_config: iceServers filled in by the client via the TURN request, plus
        // the override when configured.
        let mut pc_config = json!({
            "iceServers": [],
            "bundlePolicy": "max-bundle",
            "rtcpMuxPolicy": "require",
        });
        if !self.ice_server_override.is_empty() {
            pc_config["iceServers"] =
                serde_json::to_value(&self.ice_server_override).unwrap_or_else(|_| json!([]));
        }
        let it = query_get(url, "it");
        if !it.is_empty() {
            pc_config["iceTransports"] = Value::String(it);
        }

        // ice_server_url: where the client fetches TURN credentials. Defaults to
        // this server's own /v1alpha/iceconfig endpoint.
        let mut base = query_get(url, "ts");
        if base.is_empty() {
            base = self.ice_server_base_url.clone();
        }
        if base.is_empty() {
            base = format!("{http_scheme}://{host}");
        }
        let ice_server_url = if base.is_empty() {
            String::new()
        } else {
            format!("{base}/v1alpha/iceconfig?key={}", self.ice_server_api_key)
        };

        let is_loopback = query_get(url, "debug") == "loopback";
        let include_loopback_js = if is_loopback {
            r#"<script src="/js/loopback.js"></script>"#.to_string()
        } else {
            String::new()
        };

        let mut params = RoomParameters {
            wss_url,
            wss_post_url,
            ice_server_url,
            ice_server_transports: query_get(url, "tt"),
            header_message: self.header_message.clone(),

            is_loopback: must_json(&is_loopback),
            pc_config: must_json(&pc_config),
            pc_constraints: must_json(&json!({ "optional": [] })),
            offer_options: "{}".to_string(),
            media_constraints: must_json(&json!({ "audio": true, "video": true })),
            bypass_join_confirmation: must_json(&self.bypass_join_confirmation),
            version_info: "null".to_string(),
            include_loopback_js,

            error_messages: Vec::new(),
            warning_messages: Vec::new(),
            ..Default::default()
        };

        if !room_id.is_empty() {
            params.room_id = room_id.to_string();
            let mut room_link = format!("{http_scheme}://{host}/r/{room_id}");
            if let Some(q) = url.query()
                && !q.is_empty()
            {
                room_link.push('?');
                room_link.push_str(q);
            }
            params.room_link = room_link;
        }
        if !client_id.is_empty() {
            params.client_id = client_id.to_string();
        }
        if let Some(is_initiator) = is_initiator {
            params.is_initiator = bool_str(is_initiator).to_string();
        }
        params
    }

    /// The body of the `/v1alpha/iceconfig` response, mirroring
    /// `IceConfigurationPage`.
    pub fn ice_config(&self) -> Value {
        if !self.ice_server_override.is_empty() {
            return json!({ "iceServers": self.ice_server_override });
        }
        // With no configured urls, return an empty list rather than a single entry
        // whose "urls" is null: the browser rejects {urls: null} with "ICE server
        // protocol not supported" when building the RTCPeerConnection.
        let mut ice_servers: Vec<Value> = Vec::new();
        if !self.ice_server_urls.is_empty() {
            ice_servers.push(json!({ "urls": self.ice_server_urls }));
        }
        json!({ "iceServers": ice_servers })
    }
}

/// Split a `"/a/b"` style path into its non-empty segments.
pub(crate) fn trim_collider_path(p: &str) -> Vec<&str> {
    p.split('/').filter(|s| !s.is_empty()).collect()
}
