// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

//! Tokio/Axum driver for the Sans-I/O [`crate::collider::Collider`].

use crate::collider::{
    AuthorityCommand, AuthorityOperation, AuthorityReply, AuthorityResult, BrowserInput,
    BrowserOutput, Collider, ConnectionId,
};
use axum::Router;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::get;
use futures_util::{SinkExt, StreamExt};
use sansio::Protocol;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

const COMMAND_CAPACITY: usize = 1024;
const SOCKET_OUTPUT_CAPACITY: usize = 1024;
const WS_READ_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24);

enum SocketOutput {
    Text(String),
    Close,
}

enum DriverCommand {
    Connected {
        connection_id: ConnectionId,
        output: mpsc::Sender<SocketOutput>,
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
    Authority {
        command: AuthorityCommand,
        reply: oneshot::Sender<AuthorityReply>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

struct Shared {
    commands: mpsc::Sender<DriverCommand>,
    next_connection_id: AtomicU64,
    next_request_id: AtomicU64,
}

/// Cloneable entry point to the single task that owns the signaling state.
#[derive(Clone)]
pub struct ColliderHandle {
    shared: Arc<Shared>,
}

impl ColliderHandle {
    pub fn spawn(register_timeout: Duration) -> Self {
        let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
        let handle = Self {
            shared: Arc::new(Shared {
                commands,
                next_connection_id: AtomicU64::new(1),
                next_request_id: AtomicU64::new(1),
            }),
        };
        tokio::spawn(run(Collider::new(register_timeout), receiver));
        handle
    }

    pub async fn request(&self, operation: AuthorityOperation) -> Result<AuthorityResult, String> {
        let request_id = self.shared.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (reply, response) = oneshot::channel();
        self.shared
            .commands
            .send(DriverCommand::Authority {
                command: AuthorityCommand {
                    request_id,
                    operation,
                },
                reply,
            })
            .await
            .map_err(|_| "signaling authority stopped".to_string())?;
        let response = response
            .await
            .map_err(|_| "signaling authority stopped".to_string())?;
        if response.request_id != request_id {
            return Err("signaling authority returned a mismatched request ID".to_string());
        }
        Ok(response.result)
    }

    /// Ask the owner task to close every browser socket and release all signaling state.
    pub async fn shutdown(&self) -> Result<(), String> {
        let (reply, response) = oneshot::channel();
        self.shared
            .commands
            .send(DriverCommand::Shutdown { reply })
            .await
            .map_err(|_| "signaling authority already stopped".to_string())?;
        response
            .await
            .map_err(|_| "signaling authority stopped during shutdown".to_string())
    }
}

pub fn router(handle: ColliderHandle) -> Router {
    Router::new()
        .route("/ws", get(websocket_upgrade))
        .with_state(handle)
}

async fn websocket_upgrade(
    State(handle): State<ColliderHandle>,
    upgrade: WebSocketUpgrade,
) -> Response {
    upgrade.on_upgrade(move |socket| serve_socket(handle, socket))
}

async fn serve_socket(handle: ColliderHandle, socket: WebSocket) {
    let connection_id = handle
        .shared
        .next_connection_id
        .fetch_add(1, Ordering::Relaxed);
    log::info!("WebSocket connected: connection_id={connection_id}");
    let (output, mut outputs) = mpsc::channel(SOCKET_OUTPUT_CAPACITY);
    if handle
        .shared
        .commands
        .send(DriverCommand::Connected {
            connection_id,
            output,
        })
        .await
        .is_err()
    {
        return;
    }

    let (mut writer, mut reader) = socket.split();
    let mut read_deadline = tokio::time::Instant::now() + WS_READ_TIMEOUT;
    loop {
        tokio::select! {
            output = outputs.recv() => match output {
                Some(SocketOutput::Text(text)) => {
                    if writer.send(Message::Text(text.into())).await.is_err() {
                        break;
                    }
                }
                Some(SocketOutput::Close) | None => {
                    let _ = writer.send(Message::Close(None)).await;
                    break;
                }
            },
            incoming = reader.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    log::info!("WebSocket message: connection_id={connection_id} bytes={}", text.len());
                    if handle.shared.commands.send(DriverCommand::Text {
                        connection_id,
                        text: text.to_string(),
                        now: Instant::now(),
                    }).await.is_err() {
                        break;
                    }
                    read_deadline = tokio::time::Instant::now() + WS_READ_TIMEOUT;
                }
                Some(Ok(Message::Ping(payload))) => {
                    if writer.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Binary(_))) => break,
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
            },
            _ = tokio::time::sleep_until(read_deadline) => break,
        }
    }

    let _ = handle
        .shared
        .commands
        .send(DriverCommand::Disconnected {
            connection_id,
            now: Instant::now(),
        })
        .await;
    log::info!("WebSocket disconnected: connection_id={connection_id}");
}

async fn run(mut collider: Collider, mut commands: mpsc::Receiver<DriverCommand>) {
    let mut sockets: HashMap<ConnectionId, mpsc::Sender<SocketOutput>> = HashMap::new();
    loop {
        let deadline = collider.poll_timeout();
        let command = if let Some(deadline) = deadline {
            tokio::select! {
                command = commands.recv() => command,
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                    if collider.handle_timeout(Instant::now()).is_err() {
                        break;
                    }
                    drain_outputs(&mut collider, &mut sockets);
                    continue;
                }
            }
        } else {
            commands.recv().await
        };

        let Some(command) = command else {
            let _ = collider.close();
            drain_outputs(&mut collider, &mut sockets);
            break;
        };

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
            DriverCommand::Disconnected { connection_id, now } => {
                sockets.remove(&connection_id);
                let _ = collider.handle_read(BrowserInput::Disconnected { connection_id, now });
            }
            DriverCommand::Authority { command, reply } => {
                if collider.handle_write(command).is_ok()
                    && let Some(response) = collider.poll_event()
                {
                    let _ = reply.send(response);
                }
            }
            DriverCommand::Shutdown { reply } => {
                let _ = collider.close();
                drain_outputs(&mut collider, &mut sockets);
                let _ = reply.send(());
                break;
            }
        }
        drain_outputs(&mut collider, &mut sockets);
    }
}

fn drain_outputs(
    collider: &mut Collider,
    sockets: &mut HashMap<ConnectionId, mpsc::Sender<SocketOutput>>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn authority_requests_are_correlated() {
        let handle = ColliderHandle::spawn(Duration::from_secs(10));
        let result = handle
            .request(AuthorityOperation::Admit {
                roomid: "room".into(),
                clientid: "1".into(),
                is_loopback: false,
                now: Instant::now(),
            })
            .await
            .unwrap();
        assert!(matches!(
            result,
            AuthorityResult::Admitted {
                is_initiator: true,
                messages
            } if messages.is_empty()
        ));
        handle.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_is_terminal_and_subsequent_requests_fail() {
        let handle = ColliderHandle::spawn(Duration::from_secs(10));
        handle.shutdown().await.unwrap();
        assert_eq!(
            handle.shutdown().await.unwrap_err(),
            "signaling authority already stopped"
        );
        let error = handle
            .request(AuthorityOperation::Status)
            .await
            .unwrap_err();
        assert_eq!(error, "signaling authority stopped");
    }

    #[tokio::test]
    async fn authority_operations_return_expected_results() {
        let handle = ColliderHandle::spawn(Duration::from_secs(10));
        let room = "room".to_string();
        let client = "client".to_string();
        assert!(matches!(
            handle
                .request(AuthorityOperation::Occupancy {
                    roomid: room.clone()
                })
                .await
                .unwrap(),
            AuthorityResult::Occupancy { count: 0 }
        ));
        assert!(matches!(
            handle
                .request(AuthorityOperation::Inject {
                    roomid: room.clone(),
                    clientid: client.clone(),
                    msg: "offer".into(),
                    now: Instant::now()
                })
                .await
                .unwrap(),
            AuthorityResult::Injected
        ));
        assert!(matches!(
            handle
                .request(AuthorityOperation::Remove {
                    roomid: room.clone(),
                    clientid: client
                })
                .await
                .unwrap(),
            AuthorityResult::Removed
        ));
        assert!(matches!(
            handle.request(AuthorityOperation::Status).await.unwrap(),
            AuthorityResult::Status(_)
        ));
        handle.shutdown().await.unwrap();
    }
}
