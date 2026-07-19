use crate::client::ClientId;
use crate::room::RoomId;
use serde::{Deserialize, Serialize};

/// First-frame role for the shared signaling WebSocket. V1 browser frames remain
/// unchanged; AppWeb uses the private `app` role on the same endpoint.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AppControlRequest {
    #[serde(default)]
    pub cmd: String,
    #[serde(default)]
    pub appid: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub req: u64,
    #[serde(default)]
    pub roomid: String,
    #[serde(default)]
    pub clientid: String,
    #[serde(default)]
    pub msg: String,
    #[serde(default)]
    pub is_loopback: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AppControlResponse {
    pub reply: String,
    pub req: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_initiator: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rooms: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clients: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub websocket_connections: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_websocket_connections: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub websocket_errors: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub roomid: RoomId,
    pub clientid: ClientId,
    pub msg: String,
}

/// A WebSocket message from the client.
///
/// Fields absent from the frame default to `""` (via `#[serde(default)]`),
/// matching Go's `json.Unmarshal` zero-value behavior — a `send` frame carries
/// only `cmd` and `msg`, a `register` frame only `cmd`/`roomid`/`clientid`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsClientMsg {
    #[serde(default)]
    pub cmd: String,
    #[serde(default)]
    pub roomid: String,
    #[serde(default)]
    pub clientid: String,
    #[serde(default)]
    pub msg: String,
}

/// A message sent to a client on behalf of another client. Both fields are
/// always serialized (no `omitempty`, as in Go): a relay carries `error: ""`,
/// an error carries `msg: ""`.
#[derive(Debug, Clone, Default, Serialize)]
pub struct WsServerMsg {
    pub msg: String,
    pub error: String,
}

impl WsServerMsg {
    /// A server frame carrying the relay payload `msg` (was `sendServerMsg`).
    pub fn msg(msg: impl Into<String>) -> Self {
        Self {
            msg: msg.into(),
            error: String::new(),
        }
    }

    /// A server frame carrying `error` (was `sendServerErr`).
    pub fn err(error: impl Into<String>) -> Self {
        Self {
            msg: String::new(),
            error: error.into(),
        }
    }
}

/// Frame a relay payload as the `{ "msg": … }` wire string the driver writes to
/// a client socket. Port of `sendServerMsg`, minus the write: sans-IO returns the
/// bytes instead of pushing them into an `io.Writer`. (Go's `json.Encoder.Encode`
/// appended a trailing newline; a discrete WebSocket text frame does not need it.)
pub fn server_msg(msg: &str) -> String {
    to_wire(&WsServerMsg::msg(msg))
}

/// Frame an error as the `{ "error": … }` wire string. Port of `sendServerErr`.
pub fn server_err(error: &str) -> String {
    to_wire(&WsServerMsg::err(error))
}

/// Serialize any value to its JSON wire string (was the generic `send`).
/// Serialization of these fixed string structs is infallible, so an encoding
/// error yields an empty string rather than propagating.
fn to_wire<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_control_frames_preserve_role_and_correlation_fields() {
        let request: AppControlRequest = serde_json::from_str(
            r#"{"cmd":"admit","appid":"frontend-1","req":7,"roomid":"room","clientid":"client","is_loopback":false}"#,
        )
        .unwrap();
        assert_eq!(request.cmd, "admit");
        assert_eq!(request.appid, "frontend-1");
        assert_eq!(request.req, 7);
        assert_eq!(request.roomid, "room");

        let reply = serde_json::to_value(AppControlResponse {
            reply: "admitted".into(),
            req: 7,
            is_initiator: Some(true),
            messages: Some(vec!["offer".into()]),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(reply["reply"], "admitted");
        assert_eq!(reply["req"], 7);
        assert_eq!(reply["is_initiator"], true);
        assert_eq!(reply["messages"], serde_json::json!(["offer"]));
    }
}
