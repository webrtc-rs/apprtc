//! Sans-I/O signaling authority and V1 browser WebSocket protocol.

use crate::client::ClientId;
use crate::messages::{
    AppControlMsg, AppControlReply, Message, WsClientMsg, server_err, server_msg,
};
use crate::room::RoomId;
use crate::room_table::RoomTable;
use sansio::Protocol;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::time::{Duration, Instant};

pub type ConnectionId = u64;
pub type RequestId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserInput {
    Connected {
        connection_id: ConnectionId,
    },
    Text {
        connection_id: ConnectionId,
        text: String,
        now: Instant,
    },
    Disconnected {
        connection_id: ConnectionId,
        now: Instant,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrowserOutput {
    Text {
        connection_id: ConnectionId,
        text: String,
    },
    Close {
        connection_id: ConnectionId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityOperation {
    Admit {
        roomid: RoomId,
        clientid: ClientId,
        is_loopback: bool,
        now: Instant,
    },
    Remove {
        roomid: RoomId,
        clientid: ClientId,
    },
    Occupancy {
        roomid: RoomId,
    },
    Inject {
        roomid: RoomId,
        clientid: ClientId,
        msg: String,
        now: Instant,
    },
    Status,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityCommand {
    pub request_id: RequestId,
    pub operation: AuthorityOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSnapshot {
    pub rooms: usize,
    pub clients: usize,
    pub websocket_connections: usize,
    pub total_websocket_connections: u64,
    pub websocket_errors: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorityResult {
    Admitted {
        is_initiator: bool,
        messages: Vec<String>,
    },
    Removed,
    Occupancy {
        count: usize,
    },
    Injected,
    Status(StatusSnapshot),
    Error {
        result: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityReply {
    pub request_id: RequestId,
    pub result: AuthorityResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Session {
    Connected,
    Registered { roomid: RoomId, clientid: ClientId },
    App { appid: String },
}

/// Owns all mutable room state. Drivers must serialize inputs through this value.
pub struct Collider {
    rooms: RoomTable,
    sessions: HashMap<ConnectionId, Session>,
    connections: HashMap<(RoomId, ClientId), ConnectionId>,
    browser_outputs: VecDeque<BrowserOutput>,
    authority_replies: VecDeque<AuthorityReply>,
    total_websocket_connections: u64,
    websocket_errors: u64,
}

impl Collider {
    pub fn new(register_timeout: Duration) -> Self {
        Self {
            rooms: RoomTable::new(register_timeout),
            sessions: HashMap::new(),
            connections: HashMap::new(),
            browser_outputs: VecDeque::new(),
            authority_replies: VecDeque::new(),
            total_websocket_connections: 0,
            websocket_errors: 0,
        }
    }

    fn handle_browser_text(
        &mut self,
        connection_id: ConnectionId,
        text: String,
        now: Instant,
    ) -> Result<(), String> {
        let value: serde_json::Value = match serde_json::from_str(&text) {
            Ok(value) => value,
            Err(error) => {
                self.fail_connection(
                    connection_id,
                    format!("websocket.JSON.Receive error: {error}"),
                );
                return Ok(());
            }
        };
        if matches!(self.sessions.get(&connection_id), Some(Session::App { .. })) {
            let msg: AppControlMsg = serde_json::from_value(value).map_err(|e| e.to_string())?;
            return self.handle_app_message(connection_id, msg, now);
        }
        if value.get("cmd").and_then(|v| v.as_str()) == Some("app") {
            let msg: AppControlMsg = serde_json::from_value(value).map_err(|e| e.to_string())?;
            return self.handle_app_message(connection_id, msg, now);
        }
        let msg: WsClientMsg = match serde_json::from_value(value) {
            Ok(msg) => msg,
            Err(error) => {
                self.fail_connection(
                    connection_id,
                    format!("websocket.JSON.Receive error: {error}"),
                );
                return Ok(());
            }
        };

        match msg.cmd.as_str() {
            "register" => self.register(connection_id, msg, now),
            "send" => self.send(connection_id, msg, now),
            _ => {
                self.fail_connection(connection_id, "Invalid message: unexpected 'cmd'");
                Ok(())
            }
        }
    }

    fn handle_app_message(
        &mut self,
        connection_id: ConnectionId,
        msg: AppControlMsg,
        now: Instant,
    ) -> Result<(), String> {
        if !matches!(self.sessions.get(&connection_id), Some(Session::App { .. })) {
            if msg.cmd != "app" || msg.appid.is_empty() {
                self.fail_connection(connection_id, "Invalid app control registration");
                return Ok(());
            }
            self.sessions
                .insert(connection_id, Session::App { appid: msg.appid });
            self.browser_outputs.push_back(BrowserOutput::Text {
                connection_id,
                text: json!({"control":"registered"}).to_string(),
            });
            return Ok(());
        }
        let reply = match msg.cmd.as_str() {
            "admit" => match self
                .rooms
                .join(now, &msg.roomid, &msg.clientid, msg.is_loopback)
            {
                Ok((is_initiator, messages)) => AppControlReply {
                    reply: "admitted".into(),
                    req: msg.req,
                    result: Some("SUCCESS".into()),
                    is_initiator: Some(is_initiator),
                    messages: Some(messages),
                    ..Default::default()
                },
                Err(result) => AppControlReply {
                    reply: "error".into(),
                    req: msg.req,
                    result: Some(result),
                    ..Default::default()
                },
            },
            "remove" => {
                // A control-plane leave must have the same wire behavior as the
                // legacy HTTP leave: remove the room member and close its live
                // browser WebSocket after the control reply is delivered.
                self.rooms.leave(&msg.roomid, &msg.clientid);
                AppControlReply {
                    reply: "removed".into(),
                    req: msg.req,
                    ..Default::default()
                }
            }
            "occupancy" => AppControlReply {
                reply: "occupancy".into(),
                req: msg.req,
                count: Some(self.rooms.occupancy(&msg.roomid)),
                ..Default::default()
            },
            "inject" => match self
                .rooms
                .save_or_send(now, &msg.roomid, &msg.clientid, msg.msg)
            {
                Ok(()) => {
                    self.drain_room_writes();
                    AppControlReply {
                        reply: "injected".into(),
                        req: msg.req,
                        ..Default::default()
                    }
                },
                Err(result) => AppControlReply {
                    reply: "error".into(),
                    req: msg.req,
                    result: Some(result),
                    ..Default::default()
                },
            },
            "status" => AppControlReply {
                reply: "status".into(),
                req: msg.req,
                rooms: Some(self.rooms.room_count()),
                clients: Some(self.rooms.client_count()),
                websocket_connections: Some(self.rooms.ws_count()),
                total_websocket_connections: Some(self.total_websocket_connections),
                websocket_errors: Some(self.websocket_errors),
                ..Default::default()
            },
            _ => AppControlReply {
                reply: "error".into(),
                req: msg.req,
                result: Some("Invalid app command".into()),
                ..Default::default()
            },
        };
        self.browser_outputs.push_back(BrowserOutput::Text {
            connection_id,
            text: serde_json::to_string(&reply).unwrap(),
        });
        if msg.cmd == "remove" {
            self.close_client_connection(&msg.roomid, &msg.clientid);
        }
        Ok(())
    }

    fn register(
        &mut self,
        connection_id: ConnectionId,
        msg: WsClientMsg,
        now: Instant,
    ) -> Result<(), String> {
        if matches!(
            self.sessions.get(&connection_id),
            Some(Session::Registered { .. })
        ) {
            self.fail_connection(connection_id, "Duplicated register request");
            return Ok(());
        }
        if msg.roomid.is_empty() || msg.clientid.is_empty() {
            self.fail_connection(
                connection_id,
                "Invalid register request: missing 'clientid' or 'roomid'",
            );
            return Ok(());
        }

        match self.rooms.register(now, &msg.roomid, &msg.clientid) {
            Ok(()) => {
                log::info!(
                    "V1 register: connection_id={connection_id} room_id={} client_id={}",
                    msg.roomid,
                    msg.clientid
                );
                self.sessions.insert(
                    connection_id,
                    Session::Registered {
                        roomid: msg.roomid.clone(),
                        clientid: msg.clientid.clone(),
                    },
                );
                self.connections
                    .insert((msg.roomid, msg.clientid), connection_id);
                self.total_websocket_connections =
                    self.total_websocket_connections.saturating_add(1);
                self.drain_room_writes();
            }
            Err(error) => self.fail_connection(connection_id, error),
        }
        Ok(())
    }

    fn send(
        &mut self,
        connection_id: ConnectionId,
        msg: WsClientMsg,
        now: Instant,
    ) -> Result<(), String> {
        let Some(Session::Registered { roomid, clientid }) =
            self.sessions.get(&connection_id).cloned()
        else {
            self.fail_connection(connection_id, "Client not registered");
            return Ok(());
        };
        if msg.msg.is_empty() {
            self.fail_connection(connection_id, "Invalid send request: missing 'msg'");
            return Ok(());
        }

        log::info!(
            "V1 send: connection_id={connection_id} room_id={roomid} client_id={clientid} bytes={}",
            msg.msg.len()
        );
        self.rooms.handle_read(Message {
            roomid,
            clientid,
            msg: msg.msg,
        })?;
        while let Some(message) = self.rooms.poll_read() {
            self.rooms
                .send(now, &message.roomid, &message.clientid, message.msg)?;
        }
        self.drain_room_writes();
        Ok(())
    }

    fn disconnect(&mut self, connection_id: ConnectionId, now: Instant) {
        let Some(session) = self.sessions.remove(&connection_id) else {
            return;
        };
        if let Session::Registered { roomid, clientid } = session {
            let key = (roomid.clone(), clientid.clone());
            if self.connections.get(&key) == Some(&connection_id) {
                self.connections.remove(&key);
                self.rooms.deregister(now, &roomid, &clientid);
            }
        }
    }

    fn fail_connection(&mut self, connection_id: ConnectionId, error: impl Into<String>) {
        self.websocket_errors = self.websocket_errors.saturating_add(1);
        self.browser_outputs.push_back(BrowserOutput::Text {
            connection_id,
            text: server_err(&error.into()),
        });
        self.browser_outputs
            .push_back(BrowserOutput::Close { connection_id });
    }

    fn close_client_connection(&mut self, roomid: &RoomId, clientid: &ClientId) {
        let key = (roomid.clone(), clientid.clone());
        if let Some(connection_id) = self.connections.remove(&key) {
            self.sessions.remove(&connection_id);
            self.browser_outputs
                .push_back(BrowserOutput::Close { connection_id });
        }
    }

    fn drain_room_writes(&mut self) {
        while let Some(message) = self.rooms.poll_write() {
            if let Some(&connection_id) = self
                .connections
                .get(&(message.roomid.clone(), message.clientid.clone()))
            {
                self.browser_outputs.push_back(BrowserOutput::Text {
                    connection_id,
                    text: server_msg(&message.msg),
                });
            }
        }
    }

    fn handle_authority(&mut self, command: AuthorityCommand) {
        let result = match command.operation {
            AuthorityOperation::Admit {
                roomid,
                clientid,
                is_loopback,
                now,
            } => match self.rooms.join(now, &roomid, &clientid, is_loopback) {
                Ok((is_initiator, messages)) => AuthorityResult::Admitted {
                    is_initiator,
                    messages,
                },
                Err(result) => AuthorityResult::Error { result },
            },
            AuthorityOperation::Remove { roomid, clientid } => {
                self.close_client_connection(&roomid, &clientid);
                self.rooms.leave(&roomid, &clientid);
                AuthorityResult::Removed
            }
            AuthorityOperation::Occupancy { roomid } => AuthorityResult::Occupancy {
                count: self.rooms.occupancy(&roomid),
            },
            AuthorityOperation::Inject {
                roomid,
                clientid,
                msg,
                now,
            } => match self.rooms.save_or_send(now, &roomid, &clientid, msg) {
                Ok(()) => {
                    self.drain_room_writes();
                    AuthorityResult::Injected
                }
                Err(result) => AuthorityResult::Error { result },
            },
            AuthorityOperation::Status => AuthorityResult::Status(StatusSnapshot {
                rooms: self.rooms.room_count(),
                clients: self.rooms.client_count(),
                websocket_connections: self.rooms.ws_count(),
                total_websocket_connections: self.total_websocket_connections,
                websocket_errors: self.websocket_errors,
            }),
        };
        self.authority_replies.push_back(AuthorityReply {
            request_id: command.request_id,
            result,
        });
    }
}

impl Protocol<BrowserInput, AuthorityCommand, Infallible> for Collider {
    type Rout = Infallible;
    type Wout = BrowserOutput;
    type Eout = AuthorityReply;
    type Error = String;
    type Time = Instant;

    fn handle_read(&mut self, input: BrowserInput) -> Result<(), Self::Error> {
        match input {
            BrowserInput::Connected { connection_id } => {
                self.sessions.insert(connection_id, Session::Connected);
            }
            BrowserInput::Text {
                connection_id,
                text,
                now,
            } => self.handle_browser_text(connection_id, text, now)?,
            BrowserInput::Disconnected { connection_id, now } => {
                self.disconnect(connection_id, now)
            }
        }
        Ok(())
    }

    fn poll_read(&mut self) -> Option<Self::Rout> {
        None
    }

    fn handle_write(&mut self, command: AuthorityCommand) -> Result<(), Self::Error> {
        self.handle_authority(command);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<Self::Wout> {
        self.browser_outputs.pop_front()
    }

    fn poll_event(&mut self) -> Option<Self::Eout> {
        self.authority_replies.pop_front()
    }

    fn handle_event(&mut self, event: Infallible) -> Result<(), Self::Error> {
        match event {}
    }

    fn handle_timeout(&mut self, now: Self::Time) -> Result<(), Self::Error> {
        self.rooms.handle_timeout(now)
    }

    fn poll_timeout(&mut self) -> Option<Self::Time> {
        self.rooms.poll_timeout()
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        for connection_id in self.sessions.keys().copied().collect::<Vec<_>>() {
            self.browser_outputs
                .push_back(BrowserOutput::Close { connection_id });
        }
        self.sessions.clear();
        self.connections.clear();
        self.rooms.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(collider: &mut Collider, connection_id: u64, value: &str, now: Instant) {
        collider
            .handle_read(BrowserInput::Text {
                connection_id,
                text: value.to_string(),
                now,
            })
            .unwrap();
    }

    fn connect(collider: &mut Collider, connection_id: u64) {
        collider
            .handle_read(BrowserInput::Connected { connection_id })
            .unwrap();
    }

    #[test]
    fn app_role_registers_and_handles_authority_commands() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 99);
        text(
            &mut collider,
            99,
            r#"{"cmd":"app","appid":"frontend-1"}"#,
            now,
        );
        assert!(
            matches!(collider.poll_write(), Some(BrowserOutput::Text { connection_id: 99, text }) if text.contains("registered"))
        );
        text(
            &mut collider,
            99,
            r#"{"cmd":"admit","req":1,"roomid":"room","clientid":"client"}"#,
            now,
        );
        assert!(
            matches!(collider.poll_write(), Some(BrowserOutput::Text { connection_id: 99, text }) if text.contains("admitted"))
        );
        text(
            &mut collider,
            99,
            r#"{"cmd":"occupancy","req":2,"roomid":"room"}"#,
            now,
        );
        assert!(
            matches!(collider.poll_write(), Some(BrowserOutput::Text { connection_id: 99, text }) if text.contains("occupancy") && text.contains("\"count\":1"))
        );
        text(&mut collider, 99, r#"{"cmd":"status","req":3}"#, now);
        assert!(
            matches!(collider.poll_write(), Some(BrowserOutput::Text { connection_id: 99, text }) if text.contains("\"reply\":\"status\"") && text.contains("\"rooms\":1"))
        );
    }

    fn authority(
        collider: &mut Collider,
        request_id: u64,
        operation: AuthorityOperation,
    ) -> AuthorityResult {
        collider
            .handle_write(AuthorityCommand {
                request_id,
                operation,
            })
            .unwrap();
        let reply = collider.poll_event().unwrap();
        assert_eq!(reply.request_id, request_id);
        reply.result
    }

    fn assert_error_and_close(collider: &mut Collider, connection_id: ConnectionId, error: &str) {
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id,
                text: server_err(error),
            })
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id })
        );
    }

    #[test]
    fn successful_v1_registration_is_silent_and_send_relays() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        for (request_id, clientid) in [(1, "1"), (2, "2")] {
            assert!(matches!(
                authority(
                    &mut collider,
                    request_id,
                    AuthorityOperation::Admit {
                        roomid: "room".into(),
                        clientid: clientid.into(),
                        is_loopback: false,
                        now,
                    }
                ),
                AuthorityResult::Admitted { .. }
            ));
        }
        connect(&mut collider, 10);
        connect(&mut collider, 20);
        text(
            &mut collider,
            10,
            r#"{"cmd":"register","roomid":"room","clientid":"1"}"#,
            now,
        );
        text(
            &mut collider,
            20,
            r#"{"cmd":"register","roomid":"room","clientid":"2"}"#,
            now,
        );
        assert_eq!(collider.poll_write(), None);

        text(
            &mut collider,
            10,
            r#"{"cmd":"send","msg":"candidate"}"#,
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Text {
                connection_id: 20,
                text: r#"{"msg":"candidate","error":""}"#.into(),
            })
        );
    }

    #[test]
    fn v1_protocol_errors_are_framed_then_close_the_socket() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(&mut collider, 1, r#"{"cmd":"send","msg":"offer"}"#, now);
        assert_error_and_close(&mut collider, 1, "Client not registered");
    }

    #[test]
    fn all_v1_validation_failures_preserve_legacy_errors() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));

        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"","clientid":"client"}"#,
            now,
        );
        assert_error_and_close(
            &mut collider,
            1,
            "Invalid register request: missing 'clientid' or 'roomid'",
        );

        connect(&mut collider, 2);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"opaque-room","clientid":"opaque-client"}"#,
            now,
        );
        assert_eq!(collider.poll_write(), None);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"opaque-room","clientid":"opaque-client"}"#,
            now,
        );
        assert_error_and_close(&mut collider, 2, "Duplicated register request");

        connect(&mut collider, 3);
        text(
            &mut collider,
            3,
            r#"{"cmd":"register","roomid":"other","clientid":"other"}"#,
            now,
        );
        text(&mut collider, 3, r#"{"cmd":"send","msg":""}"#, now);
        assert_error_and_close(&mut collider, 3, "Invalid send request: missing 'msg'");

        connect(&mut collider, 4);
        text(&mut collider, 4, r#"{"cmd":"wat"}"#, now);
        assert_error_and_close(&mut collider, 4, "Invalid message: unexpected 'cmd'");
    }

    #[test]
    fn duplicate_live_client_registration_closes_only_the_new_socket() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        connect(&mut collider, 2);
        for connection_id in [1, 2] {
            text(
                &mut collider,
                connection_id,
                r#"{"cmd":"register","roomid":"room","clientid":"client"}"#,
                now,
            );
        }
        assert_error_and_close(&mut collider, 2, "Duplicated registration");
        assert_eq!(
            collider.connections.get(&("room".into(), "client".into())),
            Some(&1)
        );
    }

    #[test]
    fn authority_remove_closes_a_live_browser_connection() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"room","clientid":"client"}"#,
            now,
        );
        assert_eq!(
            authority(
                &mut collider,
                1,
                AuthorityOperation::Remove {
                    roomid: "room".into(),
                    clientid: "client".into(),
                }
            ),
            AuthorityResult::Removed
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id: 1 })
        );
    }

    #[test]
    fn authority_admit_includes_queued_messages_for_v1_join() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        assert!(matches!(
            authority(
                &mut collider,
                1,
                AuthorityOperation::Inject {
                    roomid: "room".into(),
                    clientid: "1".into(),
                    msg: "offer".into(),
                    now,
                }
            ),
            AuthorityResult::Injected
        ));
        assert_eq!(
            authority(
                &mut collider,
                2,
                AuthorityOperation::Admit {
                    roomid: "room".into(),
                    clientid: "2".into(),
                    is_loopback: false,
                    now,
                }
            ),
            AuthorityResult::Admitted {
                is_initiator: false,
                messages: vec!["offer".into()],
            }
        );
    }

    #[test]
    fn stale_disconnect_does_not_deregister_replacement_connection() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 1);
        text(
            &mut collider,
            1,
            r#"{"cmd":"register","roomid":"room","clientid":"1"}"#,
            now,
        );
        collider
            .handle_read(BrowserInput::Disconnected {
                connection_id: 1,
                now,
            })
            .unwrap();
        connect(&mut collider, 2);
        text(
            &mut collider,
            2,
            r#"{"cmd":"register","roomid":"room","clientid":"1"}"#,
            now,
        );
        collider
            .handle_read(BrowserInput::Disconnected {
                connection_id: 1,
                now,
            })
            .unwrap();
        assert_eq!(
            collider.connections.get(&("room".into(), "1".into())),
            Some(&2)
        );
    }
}
