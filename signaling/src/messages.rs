use crate::client::ClientId;
use crate::room::RoomId;
use serde::{Deserialize, Serialize};

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// First-frame role for the shared signaling WebSocket. V1 browser frames remain
/// unchanged; AppWeb uses the private `app` role on the same endpoint.
///
/// These are the *shared* wire types for the app-control protocol: the signaling
/// authority deserializes them and the AppWeb control client serializes them, so
/// both sides of the wire are defined once, here. The `skip_serializing_if`
/// attributes keep encoded frames minimal (a `status` request is just
/// `{"cmd":"status","req":N}`); deserialization treats every absent field as its
/// default either way.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppControlRequest {
    #[serde(default)]
    pub cmd: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub appid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub req: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub roomid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub clientid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub msg: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_loopback: bool,
}

impl AppControlRequest {
    /// The first frame on a control socket: claim the `app` role.
    pub fn register(appid: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            cmd: "app".into(),
            appid: appid.into(),
            token: token.into(),
            ..Default::default()
        }
    }

    pub fn admit(req: u64, roomid: RoomId, clientid: ClientId, is_loopback: bool) -> Self {
        Self {
            cmd: "admit".into(),
            req,
            roomid,
            clientid,
            is_loopback,
            ..Default::default()
        }
    }

    pub fn remove(req: u64, roomid: RoomId, clientid: ClientId) -> Self {
        Self {
            cmd: "remove".into(),
            req,
            roomid,
            clientid,
            ..Default::default()
        }
    }

    pub fn occupancy(req: u64, roomid: RoomId) -> Self {
        Self {
            cmd: "occupancy".into(),
            req,
            roomid,
            ..Default::default()
        }
    }

    pub fn inject(req: u64, roomid: RoomId, clientid: ClientId, msg: String) -> Self {
        Self {
            cmd: "inject".into(),
            req,
            roomid,
            clientid,
            msg,
            ..Default::default()
        }
    }

    pub fn status(req: u64) -> Self {
        Self {
            cmd: "status".into(),
            req,
            ..Default::default()
        }
    }

    /// Encode this request as its JSON wire string.
    pub fn to_wire(&self) -> String {
        to_wire(self)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppControlResponse {
    #[serde(default)]
    pub response: String,
    #[serde(default)]
    pub req: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_initiator: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub messages: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rooms: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_connections: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_websocket_connections: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_errors: Option<u64>,
}

impl AppControlResponse {
    /// Decode a control response from its JSON wire string.
    pub fn from_wire(text: &str) -> Result<Self, String> {
        serde_json::from_str(text).map_err(|error| error.to_string())
    }
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

        let response = serde_json::to_value(AppControlResponse {
            response: "admitted".into(),
            req: 7,
            is_initiator: Some(true),
            messages: Some(vec!["offer".into()]),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(response["response"], "admitted");
        assert_eq!(response["req"], 7);
        assert_eq!(response["is_initiator"], true);
        assert_eq!(response["messages"], serde_json::json!(["offer"]));
    }

    #[test]
    fn request_constructors_encode_minimal_frames() {
        assert_eq!(
            AppControlRequest::register("frontend-1", "").to_wire(),
            r#"{"cmd":"app","appid":"frontend-1"}"#
        );
        assert_eq!(
            AppControlRequest::status(3).to_wire(),
            r#"{"cmd":"status","req":3}"#
        );
        assert_eq!(
            AppControlRequest::admit(7, "room".into(), "client".into(), false).to_wire(),
            r#"{"cmd":"admit","req":7,"roomid":"room","clientid":"client"}"#
        );
        assert_eq!(
            AppControlRequest::admit(7, "room".into(), "client".into(), true).to_wire(),
            r#"{"cmd":"admit","req":7,"roomid":"room","clientid":"client","is_loopback":true}"#
        );
        assert_eq!(
            AppControlRequest::inject(9, "room".into(), "client".into(), "offer".into()).to_wire(),
            r#"{"cmd":"inject","req":9,"roomid":"room","clientid":"client","msg":"offer"}"#
        );
    }

    #[test]
    fn requests_and_responses_round_trip_through_the_wire() {
        let request = AppControlRequest::admit(7, "room".into(), "client".into(), true);
        let decoded: AppControlRequest = serde_json::from_str(&request.to_wire()).unwrap();
        assert_eq!(decoded.cmd, "admit");
        assert_eq!(decoded.req, 7);
        assert_eq!(decoded.roomid, "room");
        assert_eq!(decoded.clientid, "client");
        assert!(decoded.is_loopback);

        let response = AppControlResponse {
            response: "occupancy".into(),
            req: 7,
            count: Some(2),
            ..Default::default()
        };
        let decoded =
            AppControlResponse::from_wire(&serde_json::to_string(&response).unwrap()).unwrap();
        assert_eq!(decoded.response, "occupancy");
        assert_eq!(decoded.req, 7);
        assert_eq!(decoded.count, Some(2));
        assert_eq!(decoded.result, None);

        assert!(AppControlResponse::from_wire("not json").is_err());
    }
}
