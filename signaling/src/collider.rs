//! Sans-I/O signaling authority and V1 browser WebSocket protocol.

use crate::client::ClientId;
use crate::messages::{Message, WsClientMsg, server_err, server_msg};
use crate::room::RoomId;
use crate::room_table::RoomTable;
use sansio::Protocol;
use signaling_proto::{
    Request as AppControlRequest, Response as AppControlResponse,
    v1::{
        Admitted, OccupancyResult, StatusResult, request::Command as AppCommand,
        response::Payload as AppPayload,
    },
};
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
    AppControl {
        connection_id: ConnectionId,
        request: AppControlRequest,
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
    AppControl {
        connection_id: ConnectionId,
        response: AppControlResponse,
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
pub struct AuthorityResponse {
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
    authority_responses: VecDeque<AuthorityResponse>,
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
            authority_responses: VecDeque::new(),
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
        msg: AppControlRequest,
        now: Instant,
    ) -> Result<(), String> {
        let requestid = msg.requestid;
        let operation = msg.operation_name();
        if requestid == 0 || msg.command.is_none() {
            log::warn!(
                "AppWeb control request rejected: connection_id={connection_id} requestid={requestid} operation={operation} reason=invalid_protobuf_request"
            );
            self.websocket_errors = self.websocket_errors.saturating_add(1);
            self.browser_outputs
                .push_back(BrowserOutput::Close { connection_id });
            return Ok(());
        }

        if !matches!(self.sessions.get(&connection_id), Some(Session::App { .. })) {
            let AppCommand::App(register) = msg.command.unwrap() else {
                let response =
                    AppControlResponse::err(requestid, "First AppWeb control command must be app");
                self.browser_outputs.push_back(BrowserOutput::AppControl {
                    connection_id,
                    response,
                });
                self.browser_outputs
                    .push_back(BrowserOutput::Close { connection_id });
                return Ok(());
            };
            log::info!(
                "AppWeb control registration request: connection_id={connection_id} requestid={} appid={} token_present={}",
                requestid,
                register.appid,
                !register.token.is_empty()
            );
            if register.appid.is_empty() {
                log::warn!(
                    "AppWeb control registration rejected: connection_id={connection_id} requestid={} appid={} reason=invalid_registration",
                    requestid,
                    register.appid
                );
                self.websocket_errors = self.websocket_errors.saturating_add(1);
                self.browser_outputs.push_back(BrowserOutput::AppControl {
                    connection_id,
                    response: AppControlResponse::err(
                        requestid,
                        "Invalid app control registration",
                    ),
                });
                self.browser_outputs
                    .push_back(BrowserOutput::Close { connection_id });
                log::info!(
                    "AppWeb control registration response: connection_id={connection_id} requestid={} appid={} result=ERR reason=Invalid app control registration registered=false",
                    requestid,
                    register.appid
                );
                return Ok(());
            }
            let appid = register.appid;
            self.sessions.insert(
                connection_id,
                Session::App {
                    appid: appid.clone(),
                },
            );
            self.browser_outputs.push_back(BrowserOutput::AppControl {
                connection_id,
                response: AppControlResponse::ok(requestid),
            });
            log::info!(
                "AppWeb control registration response: connection_id={connection_id} requestid={} appid={appid} result=OK registered=true",
                requestid
            );
            return Ok(());
        }
        let appid = match self.sessions.get(&connection_id) {
            Some(Session::App { appid }) => appid.clone(),
            _ => unreachable!("AppWeb session was checked above"),
        };
        let command = msg.command.unwrap();
        let (response, close_client) = match command {
            AppCommand::Admit(admit) => {
                log::info!(
                    "AppWeb control command: connection_id={connection_id} appid={appid} operation=admit requestid={requestid} room_id={} client_id={}",
                    admit.roomid,
                    admit.clientid
                );
                let response =
                    match self
                        .rooms
                        .join(now, &admit.roomid, &admit.clientid, admit.is_loopback)
                    {
                        Ok((is_initiator, messages)) => AppControlResponse {
                            payload: Some(AppPayload::Admitted(Admitted {
                                is_initiator,
                                messages,
                            })),
                            ..AppControlResponse::ok(requestid)
                        },
                        Err(reason) => AppControlResponse::err(requestid, reason),
                    };
                (response, None)
            }
            AppCommand::Remove(remove) => {
                log::info!(
                    "AppWeb control command: connection_id={connection_id} appid={appid} operation=remove requestid={requestid} room_id={} client_id={}",
                    remove.roomid,
                    remove.clientid
                );
                // A control-plane leave must have the same wire behavior as the
                // legacy HTTP leave: remove the room member and close its live
                // browser WebSocket after the control response is delivered.
                self.rooms.leave(&remove.roomid, &remove.clientid);
                (
                    AppControlResponse::ok(requestid),
                    Some((remove.roomid, remove.clientid)),
                )
            }
            AppCommand::Occupancy(occupancy) => {
                log::info!(
                    "AppWeb control command: connection_id={connection_id} appid={appid} operation=occupancy requestid={requestid} room_id={}",
                    occupancy.roomid
                );
                let count =
                    u64::try_from(self.rooms.occupancy(&occupancy.roomid)).unwrap_or(u64::MAX);
                (
                    AppControlResponse {
                        payload: Some(AppPayload::Occupancy(OccupancyResult { count })),
                        ..AppControlResponse::ok(requestid)
                    },
                    None,
                )
            }
            AppCommand::Inject(inject) => {
                log::info!(
                    "AppWeb control command: connection_id={connection_id} appid={appid} operation=inject requestid={requestid} room_id={} client_id={}",
                    inject.roomid,
                    inject.clientid
                );
                let response =
                    match self
                        .rooms
                        .save_or_send(now, &inject.roomid, &inject.clientid, inject.msg)
                    {
                        Ok(()) => {
                            self.drain_room_writes();
                            AppControlResponse::ok(requestid)
                        }
                        Err(reason) => AppControlResponse::err(requestid, reason),
                    };
                (response, None)
            }
            AppCommand::Status(_) => {
                log::info!(
                    "AppWeb control command: connection_id={connection_id} appid={appid} operation=status requestid={requestid}"
                );
                (
                    AppControlResponse {
                        payload: Some(AppPayload::Status(StatusResult {
                            rooms: u64::try_from(self.rooms.room_count()).unwrap_or(u64::MAX),
                            clients: u64::try_from(self.rooms.client_count()).unwrap_or(u64::MAX),
                            websocket_connections: u64::try_from(self.rooms.ws_count())
                                .unwrap_or(u64::MAX),
                            total_websocket_connections: self.total_websocket_connections,
                            websocket_errors: self.websocket_errors,
                        })),
                        ..AppControlResponse::ok(requestid)
                    },
                    None,
                )
            }
            AppCommand::App(_) => (
                AppControlResponse::err(requestid, "AppWeb control already registered"),
                None,
            ),
        };
        self.browser_outputs.push_back(BrowserOutput::AppControl {
            connection_id,
            response: response.clone(),
        });
        log::info!(
            "AppWeb control response: connection_id={connection_id} appid={appid} operation={} requestid={} result={} reason={}",
            operation,
            requestid,
            response.result_name(),
            response.reason
        );
        if let Some((roomid, clientid)) = close_client {
            self.close_client_connection(&roomid, &clientid);
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
        match session {
            Session::Registered { roomid, clientid } => {
                let key = (roomid.clone(), clientid.clone());
                if self.connections.get(&key) == Some(&connection_id) {
                    self.connections.remove(&key);
                    self.rooms.deregister(now, &roomid, &clientid);
                }
            }
            Session::App { appid } => log::info!(
                "AppWeb control disconnected: connection_id={connection_id} appid={appid}"
            ),
            Session::Connected => {}
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
        self.authority_responses.push_back(AuthorityResponse {
            request_id: command.request_id,
            result,
        });
    }
}

impl Protocol<BrowserInput, AuthorityCommand, Infallible> for Collider {
    type Rout = Infallible;
    type Wout = BrowserOutput;
    type Eout = AuthorityResponse;
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
            BrowserInput::AppControl {
                connection_id,
                request,
                now,
            } => self.handle_app_message(connection_id, request, now)?,
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
        self.authority_responses.pop_front()
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
    use signaling_proto::ResultCode;

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

    fn control(
        collider: &mut Collider,
        connection_id: u64,
        request: AppControlRequest,
        now: Instant,
    ) {
        collider
            .handle_read(BrowserInput::AppControl {
                connection_id,
                request,
                now,
            })
            .unwrap();
    }

    #[test]
    fn app_role_registers_and_handles_authority_commands() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));
        connect(&mut collider, 99);
        control(
            &mut collider,
            99,
            AppControlRequest::register(500, "frontend-1".into(), String::new()),
            now,
        );
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::AppControl { connection_id: 99, response })
                if response.requestid == 500 && response.result == i32::from(ResultCode::Ok)
        ));
        control(
            &mut collider,
            99,
            AppControlRequest::admit(1, "room".into(), "client".into(), false),
            now,
        );
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::AppControl { connection_id: 99, response })
                if response.requestid == 1
                    && matches!(response.payload, Some(AppPayload::Admitted(Admitted { is_initiator: true, .. })))
        ));
        control(
            &mut collider,
            99,
            AppControlRequest::occupancy(2, "room".into()),
            now,
        );
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::AppControl { connection_id: 99, response })
                if matches!(response.payload, Some(AppPayload::Occupancy(OccupancyResult { count: 1 })))
        ));
        control(&mut collider, 99, AppControlRequest::status(3), now);
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::AppControl { connection_id: 99, response })
                if matches!(response.payload, Some(AppPayload::Status(StatusResult { rooms: 1, .. })))
        ));
    }

    #[test]
    fn app_role_requires_requestid_for_registration_and_every_command() {
        let now = Instant::now();
        let mut collider = Collider::new(Duration::from_secs(10));

        connect(&mut collider, 98);
        control(
            &mut collider,
            98,
            AppControlRequest::register(0, "frontend-1".into(), String::new()),
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id: 98 })
        );

        connect(&mut collider, 97);
        control(
            &mut collider,
            97,
            AppControlRequest::register(499, String::new(), String::new()),
            now,
        );
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::AppControl { connection_id: 97, response })
                if response.requestid == 499
                    && response.result == i32::from(ResultCode::Err)
                    && response.reason == "Invalid app control registration"
        ));
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id: 97 })
        );

        connect(&mut collider, 99);
        control(
            &mut collider,
            99,
            AppControlRequest::register(500, "frontend-1".into(), String::new()),
            now,
        );
        assert!(matches!(
            collider.poll_write(),
            Some(BrowserOutput::AppControl { connection_id: 99, response })
                if response.requestid == 500 && response.result == i32::from(ResultCode::Ok)
        ));
        control(
            &mut collider,
            99,
            AppControlRequest {
                requestid: 501,
                command: None,
            },
            now,
        );
        assert_eq!(
            collider.poll_write(),
            Some(BrowserOutput::Close { connection_id: 99 })
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
        let response = collider.poll_event().unwrap();
        assert_eq!(response.request_id, request_id);
        response.result
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
