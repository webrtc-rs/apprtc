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
pub(crate) fn generate_random(length: usize) -> String {
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
        let wss_url = if self.signaling_url.is_empty() {
            format!("{ws_scheme}://{host}/ws")
        } else {
            format!("{}/ws", self.signaling_url.trim_end_matches('/'))
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IceServer;

    fn url(value: &str) -> Url {
        Url::parse(value).unwrap()
    }

    #[test]
    fn self_origin_honors_configured_host_and_tls() {
        let config = Config {
            host: "public.example:8443".into(),
            force_tls: true,
            ..Default::default()
        };
        assert_eq!(
            config.self_origin("request.example".into()),
            ("https".into(), "wss".into(), "public.example:8443".into())
        );
    }

    #[test]
    fn builds_room_urls_and_preserves_query_string() {
        let config = Config::default();
        let params = config.build_room_parameters(
            "localhost:8080".into(),
            &url("http://localhost/r/x?debug=loopback&tt=relay"),
            "room/a",
            "client-α",
            Some(true),
        );
        assert_eq!(
            params.room_link,
            "http://localhost:8080/r/room/a?debug=loopback&tt=relay"
        );
        assert_eq!(params.room_id, "room/a");
        assert_eq!(params.client_id, "client-α");
        assert_eq!(params.is_initiator, "true");
    }

    #[test]
    fn query_options_enable_loopback_and_ice_transport_settings() {
        let config = Config::default();
        let params = config.build_room_parameters(
            "host".into(),
            &url("http://host/?debug=loopback&it=all&tt=relay"),
            "",
            "",
            Some(false),
        );
        assert_eq!(params.is_loopback, "true");
        assert_eq!(params.ice_server_transports, "relay");
        assert!(params.pc_config.contains("iceTransports"));
        assert!(params.include_loopback_js.contains("loopback.js"));
        assert_eq!(params.is_initiator, "false");
    }

    #[test]
    fn ice_server_url_uses_override_base_and_api_key_precedence() {
        let config = Config {
            ice_server_base_url: "https://turn.example/api".into(),
            ice_server_api_key: "secret".into(),
            ..Default::default()
        };
        let params = config.build_room_parameters(
            "host".into(),
            &url("http://host/?ts=https%3A%2F%2Fcustom.example"),
            "",
            "",
            None,
        );
        assert_eq!(
            params.ice_server_url,
            "https://custom.example/v1alpha/iceconfig?key=secret"
        );
    }

    #[test]
    fn ice_config_returns_override_or_configured_urls() {
        let configured = Config {
            ice_server_urls: vec!["stun:stun.example".into()],
            ..Default::default()
        };
        assert_eq!(
            configured.ice_config()["iceServers"][0]["urls"],
            json!(["stun:stun.example"])
        );
        let override_config = Config {
            ice_server_override: vec![IceServer {
                urls: vec!["turn:turn.example".into()],
                username: "u".into(),
                credential: "p".into(),
            }],
            ..Default::default()
        };
        assert_eq!(
            override_config.ice_config()["iceServers"][0]["username"],
            "u"
        );
        assert_eq!(Config::default().ice_config(), json!({"iceServers": []}));
    }

    #[test]
    fn random_ids_and_path_trimming_have_expected_shapes() {
        let id = generate_random(32);
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|b| b.is_ascii_digit()));
        assert_eq!(
            trim_collider_path("//room///client/"),
            vec!["room", "client"]
        );
        assert!(trim_collider_path("///").is_empty());
    }
}
