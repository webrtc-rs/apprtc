use crate::client::ClientId;
use crate::room::RoomId;
use serde::{Deserialize, Serialize};

fn is_false(value: &bool) -> bool {
    !*value
}

mod u64_decimal_string {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        let value = value.parse().map_err(serde::de::Error::custom)?;
        if value == 0 {
            return Err(serde::de::Error::custom(
                "requestid must be greater than zero",
            ));
        }
        Ok(value)
    }
}

/// First-frame role for the shared signaling WebSocket. V1 browser frames remain
/// unchanged; AppWeb uses the private `app` role on the same endpoint.
///
/// These are the *shared* wire types for the app-control protocol: the signaling
/// authority deserializes them and the AppWeb control client serializes them, so
/// both sides of the wire are defined once, here. The `skip_serializing_if`
/// attributes keep encoded frames minimal (a `status` request is just
/// `{"cmd":"status","requestid":"N"}`). `requestid` is intentionally not
/// defaulted: every control request must carry it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppControlRequest {
    #[serde(default)]
    pub cmd: String,
    #[serde(with = "u64_decimal_string")]
    pub requestid: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub appid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub token: String,
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
    pub fn register(requestid: u64, appid: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            cmd: "app".into(),
            requestid,
            appid: appid.into(),
            token: token.into(),
            ..Default::default()
        }
    }

    pub fn admit(requestid: u64, roomid: RoomId, clientid: ClientId, is_loopback: bool) -> Self {
        Self {
            cmd: "admit".into(),
            requestid,
            roomid,
            clientid,
            is_loopback,
            ..Default::default()
        }
    }

    pub fn remove(requestid: u64, roomid: RoomId, clientid: ClientId) -> Self {
        Self {
            cmd: "remove".into(),
            requestid,
            roomid,
            clientid,
            ..Default::default()
        }
    }

    pub fn occupancy(requestid: u64, roomid: RoomId) -> Self {
        Self {
            cmd: "occupancy".into(),
            requestid,
            roomid,
            ..Default::default()
        }
    }

    pub fn inject(requestid: u64, roomid: RoomId, clientid: ClientId, msg: String) -> Self {
        Self {
            cmd: "inject".into(),
            requestid,
            roomid,
            clientid,
            msg,
            ..Default::default()
        }
    }

    pub fn status(requestid: u64) -> Self {
        Self {
            cmd: "status".into(),
            requestid,
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
    /// Required correlation identifier copied from the corresponding request.
    #[serde(with = "u64_decimal_string")]
    pub requestid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
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
            r#"{"cmd":"admit","appid":"frontend-1","requestid":"7","roomid":"room","clientid":"client","is_loopback":false}"#,
        )
        .unwrap();
        assert_eq!(request.cmd, "admit");
        assert_eq!(request.appid, "frontend-1");
        assert_eq!(request.requestid, 7);
        assert_eq!(request.roomid, "room");

        let response = serde_json::to_value(AppControlResponse {
            requestid: 7,
            result: Some("OK".into()),
            is_initiator: Some(true),
            messages: Some(vec!["offer".into()]),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(response["requestid"], "7");
        assert_eq!(response["result"], "OK");
        assert_eq!(response["is_initiator"], true);
        assert_eq!(response["messages"], serde_json::json!(["offer"]));
    }

    #[test]
    fn request_constructors_encode_minimal_frames() {
        assert_eq!(
            AppControlRequest::register(1, "frontend-1", "").to_wire(),
            r#"{"cmd":"app","requestid":"1","appid":"frontend-1"}"#
        );
        assert_eq!(
            AppControlRequest::status(3).to_wire(),
            r#"{"cmd":"status","requestid":"3"}"#
        );
        assert_eq!(
            AppControlRequest::admit(7, "room".into(), "client".into(), false).to_wire(),
            r#"{"cmd":"admit","requestid":"7","roomid":"room","clientid":"client"}"#
        );
        assert_eq!(
            AppControlRequest::admit(7, "room".into(), "client".into(), true).to_wire(),
            r#"{"cmd":"admit","requestid":"7","roomid":"room","clientid":"client","is_loopback":true}"#
        );
        assert_eq!(
            AppControlRequest::inject(9, "room".into(), "client".into(), "offer".into()).to_wire(),
            r#"{"cmd":"inject","requestid":"9","roomid":"room","clientid":"client","msg":"offer"}"#
        );
    }

    #[test]
    fn requests_and_responses_round_trip_through_the_wire() {
        let request = AppControlRequest::admit(7, "room".into(), "client".into(), true);
        let decoded: AppControlRequest = serde_json::from_str(&request.to_wire()).unwrap();
        assert_eq!(decoded.cmd, "admit");
        assert_eq!(decoded.requestid, 7);
        assert_eq!(decoded.roomid, "room");
        assert_eq!(decoded.clientid, "client");
        assert!(decoded.is_loopback);

        let response = AppControlResponse {
            requestid: 7,
            result: Some("OK".into()),
            count: Some(2),
            ..Default::default()
        };
        let decoded =
            AppControlResponse::from_wire(&serde_json::to_string(&response).unwrap()).unwrap();
        assert_eq!(decoded.requestid, 7);
        assert_eq!(decoded.count, Some(2));
        assert_eq!(decoded.result.as_deref(), Some("OK"));
        assert_eq!(decoded.reason, None);

        assert!(AppControlResponse::from_wire("not json").is_err());
        assert!(AppControlResponse::from_wire(r#"{"result":"OK"}"#).is_err());
        assert!(AppControlResponse::from_wire(r#"{"requestid":"0","result":"OK"}"#).is_err());
        assert!(serde_json::from_str::<AppControlRequest>(r#"{"cmd":"status"}"#).is_err());
        assert!(
            serde_json::from_str::<AppControlRequest>(r#"{"cmd":"status","requestid":"0"}"#)
                .is_err()
        );
    }
}
