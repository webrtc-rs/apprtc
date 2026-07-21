//! Async event-loop driver for the Sans-I/O [`signaling::collider::Collider`].
//!
//! The event loop serializes browser and authority commands through one Collider, handles
//! protocol deadlines, and routes outputs back to browser sessions owned by
//! [`crate::ws_server`].

use crate::ws_server::SocketOutput;
use sansio::Protocol;
use signaling::collider::{
    AuthorityCommand, AuthorityResponse, BrowserInput, BrowserOutput, Collider, ConnectionId,
};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

pub const COMMAND_CAPACITY: usize = 1024;

/// Commands a session task hands to the event loop that owns the `Collider`.
pub enum DriverCommand {
    Connected {
        connection_id: ConnectionId,
        output: mpsc::Sender<SocketOutput>,
    },
    Text {
        connection_id: ConnectionId,
        text: String,
        now: Instant,
    },
    Authority {
        command: AuthorityCommand,
        response: oneshot::Sender<AuthorityResponse>,
    },
    SfuConnected {
        input: signaling::sfu::Input,
        output: mpsc::Sender<signaling::sfu::Output>,
    },
    SfuInput {
        input: signaling::sfu::Input,
    },
    Disconnected {
        connection_id: ConnectionId,
        now: Instant,
    },
}

/// The event loop that owns the Sans-I/O `Collider`: serializes every browser input,
/// fires the state machine's timeouts, and routes outputs to each session's channel.
pub async fn run(
    mut stop_rx: watch::Receiver<()>,
    mut commands: mpsc::Receiver<DriverCommand>,
    register_timeout: Duration,
) {
    let mut collider = Collider::new(register_timeout);
    let mut sockets: HashMap<ConnectionId, mpsc::Sender<SocketOutput>> = HashMap::new();
    let mut sfu_sessions = HashMap::new();
    let mut pending_authority = HashMap::new();
    loop {
        // Sleep until the next command, the state machine's own deadline, or the stop
        // signal — whichever wakes first.
        let command = if let Some(deadline) = collider.poll_timeout() {
            tokio::select! {
                _ = stop_rx.changed() => break,
                command = commands.recv() => command,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    if collider.handle_timeout(Instant::now()).is_err() {
                        break;
                    }
                    drain_outputs(&mut collider, &mut sockets, &mut sfu_sessions);
                    drain_authority_responses(&mut collider, &mut pending_authority);
                    continue;
                }
            }
        } else {
            tokio::select! {
                _ = stop_rx.changed() => break,
                command = commands.recv() => command,
            }
        };

        let Some(command) = command else { break };
        handle_command(
            &mut collider,
            command,
            &mut sockets,
            &mut sfu_sessions,
            &mut pending_authority,
        );
        drain_outputs(&mut collider, &mut sockets, &mut sfu_sessions);
        drain_authority_responses(&mut collider, &mut pending_authority);
    }

    // Graceful stop: apply commands already queued before the signal, then close every
    // browser socket and release all signaling state.
    while let Ok(command) = commands.try_recv() {
        handle_command(
            &mut collider,
            command,
            &mut sockets,
            &mut sfu_sessions,
            &mut pending_authority,
        );
    }
    let _ = collider.close();
    drain_outputs(&mut collider, &mut sockets, &mut sfu_sessions);
    drain_authority_responses(&mut collider, &mut pending_authority);
    log::info!("signaling event loop stopped");
}

fn handle_command(
    collider: &mut Collider,
    command: DriverCommand,
    sockets: &mut HashMap<ConnectionId, mpsc::Sender<SocketOutput>>,
    sfu_sessions: &mut HashMap<signaling::sfu::ConnectionId, mpsc::Sender<signaling::sfu::Output>>,
    pending_authority: &mut HashMap<u64, oneshot::Sender<AuthorityResponse>>,
) {
    match command {
        DriverCommand::Connected {
            connection_id,
            output,
        } => {
            sockets.insert(connection_id, output);
            if collider
                .handle_read(BrowserInput::Connected { connection_id })
                .is_err()
            {
                sockets.remove(&connection_id);
            }
        }
        DriverCommand::Text {
            connection_id,
            text,
            now,
        } => {
            if collider
                .handle_read(BrowserInput::Text {
                    connection_id,
                    text,
                    now,
                })
                .is_err()
                && let Some(socket) = sockets.remove(&connection_id)
            {
                let _ = socket.try_send(SocketOutput::Close);
            }
        }
        DriverCommand::Authority { command, response } => {
            let request_id = command.request_id;
            pending_authority.insert(request_id, response);
            if collider.handle_write(command).is_err()
                && let Some(response) = pending_authority.remove(&request_id)
            {
                let _ = response.send(AuthorityResponse {
                    request_id,
                    result: signaling::collider::AuthorityResult::Error {
                        result: "INTERNAL".into(),
                    },
                });
            }
        }
        DriverCommand::SfuConnected { input, output } => {
            let connection_id = match &input {
                signaling::sfu::Input::Register { connection_id, .. } => *connection_id,
                _ => return,
            };
            sfu_sessions.insert(connection_id, output);
            if collider.handle_sfu_input(input).is_err() {
                sfu_sessions.remove(&connection_id);
            }
        }
        DriverCommand::SfuInput { input } => {
            let _ = collider.handle_sfu_input(input);
        }
        DriverCommand::Disconnected { connection_id, now } => {
            sockets.remove(&connection_id);
            let _ = collider.handle_read(BrowserInput::Disconnected { connection_id, now });
        }
    }
}

fn drain_outputs(
    collider: &mut Collider,
    sockets: &mut HashMap<ConnectionId, mpsc::Sender<SocketOutput>>,
    sfu_sessions: &mut HashMap<signaling::sfu::ConnectionId, mpsc::Sender<signaling::sfu::Output>>,
) {
    let mut disconnected = Vec::new();
    while let Some(output) = collider.poll_write() {
        match output {
            BrowserOutput::Text {
                connection_id,
                text,
            } => {
                if sockets
                    .get(&connection_id)
                    .is_some_and(|socket| socket.try_send(SocketOutput::Text(text)).is_err())
                {
                    disconnected.push(connection_id);
                }
            }
            BrowserOutput::Close { connection_id } => {
                if let Some(socket) = sockets.remove(&connection_id) {
                    let _ = socket.try_send(SocketOutput::Close);
                }
                disconnected.push(connection_id);
            }
            BrowserOutput::Sfu(output) => {
                let connection_id = sfu_output_connection_id(&output);
                if sfu_sessions
                    .get(&connection_id)
                    .is_some_and(|session| session.try_send(output).is_err())
                {
                    sfu_sessions.remove(&connection_id);
                }
            }
        }
    }
    disconnected.sort_unstable();
    disconnected.dedup();
    for connection_id in disconnected {
        sockets.remove(&connection_id);
        let _ = collider.handle_read(BrowserInput::Disconnected {
            connection_id,
            now: Instant::now(),
        });
    }
}

fn sfu_output_connection_id(output: &signaling::sfu::Output) -> signaling::sfu::ConnectionId {
    match output {
        signaling::sfu::Output::Registered { connection_id, .. }
        | signaling::sfu::Output::RegistrationError { connection_id, .. }
        | signaling::sfu::Output::Command { connection_id, .. }
        | signaling::sfu::Output::EventAck { connection_id, .. }
        | signaling::sfu::Output::Close { connection_id } => *connection_id,
    }
}

fn drain_authority_responses(
    collider: &mut Collider,
    pending: &mut HashMap<u64, oneshot::Sender<AuthorityResponse>>,
) {
    while let Some(response) = collider.poll_event() {
        if let Some(sender) = pending.remove(&response.request_id) {
            let _ = sender.send(response);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Harness {
        stop_tx: watch::Sender<()>,
        commands: mpsc::Sender<DriverCommand>,
        run: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn spawn() -> Self {
            let (stop_tx, stop_rx) = watch::channel(());
            let (commands, commands_rx) = mpsc::channel(COMMAND_CAPACITY);
            let run = tokio::spawn(run(stop_rx, commands_rx, Duration::from_secs(10)));
            Self {
                stop_tx,
                commands,
                run,
            }
        }

        async fn connect(&self, connection_id: ConnectionId) -> mpsc::Receiver<SocketOutput> {
            let (output, outputs) = mpsc::channel(COMMAND_CAPACITY);
            self.commands
                .send(DriverCommand::Connected {
                    connection_id,
                    output,
                })
                .await
                .unwrap();
            outputs
        }

        async fn text(&self, connection_id: ConnectionId, text: &str) {
            self.commands
                .send(DriverCommand::Text {
                    connection_id,
                    text: text.to_string(),
                    now: Instant::now(),
                })
                .await
                .unwrap();
        }

        async fn authority(
            &self,
            request_id: u64,
            operation: signaling::collider::AuthorityOperation,
        ) -> signaling::collider::AuthorityResult {
            let (response, response_rx) = oneshot::channel();
            self.commands
                .send(DriverCommand::Authority {
                    command: AuthorityCommand {
                        request_id,
                        operation,
                    },
                    response,
                })
                .await
                .unwrap();
            response_rx.await.unwrap().result
        }

        async fn shutdown(self) {
            self.stop_tx.send(()).unwrap();
            self.run.await.unwrap();
        }
    }

    async fn recv_text(outputs: &mut mpsc::Receiver<SocketOutput>) -> String {
        match tokio::time::timeout(Duration::from_secs(5), outputs.recv())
            .await
            .unwrap()
            .unwrap()
        {
            SocketOutput::Text(text) => text,
            SocketOutput::Close => panic!("expected a text frame, got a close"),
        }
    }

    async fn recv_close(outputs: &mut mpsc::Receiver<SocketOutput>) {
        match tokio::time::timeout(Duration::from_secs(5), outputs.recv())
            .await
            .unwrap()
            .unwrap()
        {
            SocketOutput::Close => {}
            SocketOutput::Text(text) => panic!("expected a close, got text: {text}"),
        }
    }

    #[tokio::test]
    async fn v1_send_relays_between_registered_sessions() {
        let harness = Harness::spawn();
        let mut outputs_1 = harness.connect(1).await;
        let mut outputs_2 = harness.connect(2).await;
        harness
            .text(1, r#"{"cmd":"register","roomid":"room","clientid":"1"}"#)
            .await;
        harness
            .text(2, r#"{"cmd":"register","roomid":"room","clientid":"2"}"#)
            .await;
        harness.text(1, r#"{"cmd":"send","msg":"candidate"}"#).await;
        assert_eq!(
            recv_text(&mut outputs_2).await,
            r#"{"msg":"candidate","error":""}"#.to_string()
        );
        assert!(outputs_1.try_recv().is_err(), "registration is silent");
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn protocol_errors_are_framed_then_close_the_socket() {
        let harness = Harness::spawn();
        let mut outputs = harness.connect(1).await;
        harness.text(1, r#"{"cmd":"send","msg":"offer"}"#).await;
        assert!(
            recv_text(&mut outputs)
                .await
                .contains("Client not registered")
        );
        recv_close(&mut outputs).await;
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn v2_admission_registration_and_relay_cross_the_async_driver() {
        use signaling::collider::{AuthorityOperation, AuthorityResult};

        let harness = Harness::spawn();
        let first_token = match harness
            .authority(
                1,
                AuthorityOperation::AdmitV2 {
                    room_id: 42,
                    client_id: 101,
                    admission_token: "token-101".into(),
                    now: Instant::now(),
                },
            )
            .await
        {
            AuthorityResult::AdmittedV2 {
                admission_token, ..
            } => admission_token,
            result => panic!("unexpected admission: {result:?}"),
        };
        let second_token = match harness
            .authority(
                2,
                AuthorityOperation::AdmitV2 {
                    room_id: 42,
                    client_id: 102,
                    admission_token: "token-102".into(),
                    now: Instant::now(),
                },
            )
            .await
        {
            AuthorityResult::AdmittedV2 {
                admission_token, ..
            } => admission_token,
            result => panic!("unexpected admission: {result:?}"),
        };
        let mut first_outputs = harness.connect(1).await;
        let mut second_outputs = harness.connect(2).await;
        harness
            .text(
                1,
                &format!(
                    r#"{{"cmd":"register","roomid":"42","clientid":"101","ver":2,"token":"{first_token}"}}"#
                ),
            )
            .await;
        assert!(recv_text(&mut first_outputs).await.contains("registered"));
        harness
            .text(
                2,
                &format!(
                    r#"{{"cmd":"register","roomid":"42","clientid":"102","ver":2,"token":"{second_token}"}}"#
                ),
            )
            .await;
        assert!(recv_text(&mut second_outputs).await.contains("registered"));
        harness
            .text(
                1,
                r#"{"cmd":"send","epoch":"0","msg":"{\"type\":\"candidate\"}"}"#,
            )
            .await;
        assert_eq!(
            recv_text(&mut second_outputs).await,
            r#"{"msg":"{\"type\":\"candidate\"}","error":""}"#
        );
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_closes_every_connected_socket() {
        let harness = Harness::spawn();
        let mut outputs_1 = harness.connect(1).await;
        let mut outputs_2 = harness.connect(2).await;
        harness
            .text(1, r#"{"cmd":"register","roomid":"room","clientid":"1"}"#)
            .await;
        harness.shutdown().await;
        recv_close(&mut outputs_1).await;
        recv_close(&mut outputs_2).await;
    }
}
