//! Runtime adapter between the signaling gRPC stream and the Sans-I/O SFU.

use bytes::BytesMut;
use rtc::peer_connection::sdp::{RTCSdpType, RTCSessionDescription};
use rtc::peer_connection::transport::RTCIceCandidateInit;
use rtc::shared::{TaggedBytesMut, TransportContext, TransportProtocol};
use sansio::Protocol;
use serde::Deserialize;
use signaling_proto::v2::signaling_service_client::SignalingServiceClient;
use signaling_proto::v2::{self, AppId, RequestContext, SfuToSignaling};
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{ClientTlsConfig, Endpoint};
use url::Url;

const CHANNEL_CAPACITY: usize = 1024;
const COMMAND_CACHE_CAPACITY: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
const INITIAL_RECONNECT_DELAY: Duration = Duration::from_millis(250);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Config {
    pub host_ip: IpAddr,
    pub public_ip: IpAddr,
    pub media_port_min: u16,
    pub media_port_max: u16,
    pub grpc_url: String,
    pub insecure_tls: bool,
    pub max_rooms: u64,
    pub max_clients: u64,
    pub instance_id: String,
}

#[derive(Default)]
struct Metrics {
    rooms: AtomicU64,
    clients: AtomicU64,
}

struct MediaCommand {
    command: v2::SfuCommand,
    response: oneshot::Sender<v2::SfuCommandResult>,
}

struct SessionContext<'a> {
    config: &'a Config,
    media: &'a [mpsc::Sender<MediaCommand>],
    metrics: &'a Metrics,
    next_event_id: &'a AtomicU64,
}

pub async fn run(mut stop_rx: watch::Receiver<()>, config: Config) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.media_port_min <= config.media_port_max,
        "media port range is empty"
    );
    anyhow::ensure!(
        config.max_rooms > 0 && config.max_clients > 0,
        "SFU capacity must be nonzero"
    );
    let endpoint = grpc_endpoint(&config.grpc_url, config.insecure_tls)?;

    // Bind the complete configured range before spawning any shard so a bad
    // URL or one unavailable port cannot leave detached partial service state.
    let mut sockets = Vec::new();
    for port in config.media_port_min..=config.media_port_max {
        sockets.push((port, UdpSocket::bind((config.host_ip, port)).await?));
    }

    let metrics = Arc::new(Metrics::default());
    let next_event_id = Arc::new(AtomicU64::new(1));
    let (event_tx, mut event_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let mut media = Vec::new();
    let mut media_handles = Vec::new();
    for (port, socket) in sockets {
        let advertised_addr = SocketAddr::new(config.public_ip, port);
        let (command_tx, command_rx) = mpsc::channel(CHANNEL_CAPACITY);
        media.push(command_tx);
        media_handles.push(tokio::spawn(media_loop(
            stop_rx.clone(),
            socket,
            advertised_addr,
            command_rx,
            event_tx.clone(),
            metrics.clone(),
            next_event_id.clone(),
        )));
        log::info!(
            "SFU media socket ready: bind={}:{} advertised={advertised_addr}",
            config.host_ip,
            port
        );
    }
    drop(event_tx);

    let mut pending_events = HashMap::new();
    let mut reconnect_delay = INITIAL_RECONNECT_DELAY;
    loop {
        log::info!(
            "Connecting SFU gRPC session: url={} instance_id={}",
            config.grpc_url,
            config.instance_id
        );
        let session = run_session(
            &mut stop_rx,
            endpoint.clone(),
            &mut event_rx,
            &mut pending_events,
            SessionContext {
                config: &config,
                media: &media,
                metrics: &metrics,
                next_event_id: &next_event_id,
            },
        )
        .await;
        match session {
            Ok(true) => break,
            Ok(false) => log::warn!("SFU gRPC session ended; reconnecting"),
            Err(error) => log::warn!("SFU gRPC session failed: {error}; reconnecting"),
        }
        tokio::select! {
            _ = stop_rx.changed() => break,
            _ = tokio::time::sleep(reconnect_delay) => {}
        }
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }

    // Unblock any media shard applying event-queue backpressure before waiting
    // for the shards to observe shutdown.
    drop(event_rx);
    for handle in media_handles {
        let _ = handle.await;
    }
    log::info!("SFU service stopped: instance_id={}", config.instance_id);
    Ok(())
}

async fn run_session(
    stop_rx: &mut watch::Receiver<()>,
    endpoint: Endpoint,
    event_rx: &mut mpsc::Receiver<v2::SfuEvent>,
    pending_events: &mut HashMap<u64, v2::SfuEvent>,
    context: SessionContext<'_>,
) -> anyhow::Result<bool> {
    let SessionContext {
        config,
        media,
        metrics,
        next_event_id,
    } = context;
    let mut client = SignalingServiceClient::new(endpoint.connect().await?);
    let (outgoing_tx, outgoing_rx) = mpsc::channel(CHANNEL_CAPACITY);
    let registration_request_id = next_nonzero(next_event_id);
    outgoing_tx
        .send(SfuToSignaling {
            message: Some(v2::sfu_to_signaling::Message::Register(v2::RegisterSfu {
                context: Some(RequestContext {
                    app_id: AppId::Sfu as i32,
                    instance_id: config.instance_id.clone(),
                    request_id: registration_request_id,
                }),
                capacity: Some(capacity(config)),
            })),
        })
        .await?;
    let mut incoming = client
        .open_sfu_session(ReceiverStream::new(outgoing_rx))
        .await?
        .into_inner();
    let registered = incoming
        .message()
        .await?
        .ok_or_else(|| anyhow::anyhow!("SFU session closed before registration response"))?;
    let (health_interval, resumed) = decode_registration(registered, registration_request_id)?;
    log::info!(
        "SFU gRPC registered: instance_id={} request_id={} resumed={} health_interval_ms={}",
        config.instance_id,
        registration_request_id,
        resumed,
        health_interval.as_millis()
    );

    for event in pending_events.values() {
        outgoing_tx
            .send(SfuToSignaling {
                message: Some(v2::sfu_to_signaling::Message::Event(event.clone())),
            })
            .await?;
    }
    queue_health(&outgoing_tx, pending_events, config, metrics, next_event_id).await?;
    let mut health_tick = tokio::time::interval(health_interval);
    health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    health_tick.tick().await;

    loop {
        tokio::select! {
            _ = stop_rx.changed() => return Ok(true),
            _ = health_tick.tick() => {
                queue_health(&outgoing_tx, pending_events, config, metrics, next_event_id).await?;
            }
            event = event_rx.recv() => {
                let Some(event) = event else { return Ok(true) };
                pending_events.insert(event.request_id, event.clone());
                outgoing_tx.send(SfuToSignaling {
                    message: Some(v2::sfu_to_signaling::Message::Event(event)),
                }).await?;
            }
            message = incoming.message() => {
                let Some(message) = message? else { return Ok(false) };
                match message.message {
                    Some(v2::signaling_to_sfu::Message::Command(command)) => {
                        let result = dispatch_command(media, command).await;
                        outgoing_tx.send(SfuToSignaling {
                            message: Some(v2::sfu_to_signaling::Message::CommandResult(result)),
                        }).await?;
                    }
                    Some(v2::signaling_to_sfu::Message::EventAck(ack)) => {
                        pending_events.remove(&ack.request_id);
                        log::debug!("SFU event acknowledged: request_id={}", ack.request_id);
                    }
                    Some(v2::signaling_to_sfu::Message::Registered(_)) => {
                        anyhow::bail!("duplicate SFU registration response");
                    }
                    None => anyhow::bail!("empty signaling-to-SFU message"),
                }
            }
        }
    }
}

async fn dispatch_command(
    media: &[mpsc::Sender<MediaCommand>],
    command: v2::SfuCommand,
) -> v2::SfuCommandResult {
    let request_id = command.request_id;
    log::info!(
        "SFU command received: request_id={request_id} operation={}",
        proto_command_name(&command)
    );
    let Some(room_id) = command_room_id(&command) else {
        return command_error(
            request_id,
            v2::ErrorCode::InvalidRequest,
            "unsupported command",
        );
    };
    let Some(shard) = media.get((room_id as usize) % media.len()) else {
        return command_error(
            request_id,
            v2::ErrorCode::WorkerUnavailable,
            "media shard unavailable",
        );
    };
    let (response, response_rx) = oneshot::channel();
    if shard
        .send(MediaCommand { command, response })
        .await
        .is_err()
    {
        return command_error(
            request_id,
            v2::ErrorCode::WorkerUnavailable,
            "media shard stopped",
        );
    }
    let result = response_rx.await.unwrap_or_else(|_| {
        command_error(
            request_id,
            v2::ErrorCode::WorkerUnavailable,
            "media shard stopped",
        )
    });
    match &result.result {
        Some(v2::sfu_command_result::Result::Ok(_)) => {
            log::info!("SFU command completed: request_id={request_id} result=OK");
        }
        Some(v2::sfu_command_result::Result::Error(error)) => log::warn!(
            "SFU command completed: request_id={request_id} result=ERR code={:?} reason={}",
            v2::ErrorCode::try_from(error.code).unwrap_or_default(),
            error.reason
        ),
        None => log::warn!(
            "SFU command completed: request_id={request_id} result=ERR reason=missing_result"
        ),
    }
    result
}

fn proto_command_name(command: &v2::SfuCommand) -> &'static str {
    match &command.command {
        Some(v2::sfu_command::Command::SyncRoom(_)) => "sync_room",
        Some(v2::sfu_command::Command::Join(_)) => "join",
        Some(v2::sfu_command::Command::Leave(_)) => "leave",
        Some(v2::sfu_command::Command::Signal(_)) => "signal",
        Some(v2::sfu_command::Command::Drain(_)) => "drain",
        None => "missing",
    }
}

fn command_room_id(command: &v2::SfuCommand) -> Option<u64> {
    match command.command.as_ref()? {
        v2::sfu_command::Command::SyncRoom(value) => Some(value.room_id),
        v2::sfu_command::Command::Join(value) => Some(value.room_id),
        v2::sfu_command::Command::Leave(value) => Some(value.room_id),
        v2::sfu_command::Command::Signal(value) => Some(value.room_id),
        v2::sfu_command::Command::Drain(_) => None,
    }
}

async fn media_loop(
    mut stop_rx: watch::Receiver<()>,
    socket: UdpSocket,
    advertised_addr: SocketAddr,
    mut commands: mpsc::Receiver<MediaCommand>,
    events: mpsc::Sender<v2::SfuEvent>,
    metrics: Arc<Metrics>,
    next_event_id: Arc<AtomicU64>,
) {
    let mut engine = sfu::Sfu::new(rand::random(), advertised_addr);
    let mut projection = HashMap::new();
    let mut command_cache: HashMap<u64, v2::SfuCommandResult> = HashMap::new();
    let mut command_order = VecDeque::new();
    let mut packet = vec![0_u8; 2048];
    let mut metric_rooms = 0_u64;
    let mut metric_clients = 0_u64;

    loop {
        let deadline = engine
            .poll_timeout()
            .unwrap_or_else(|| Instant::now() + Duration::from_millis(100));
        tokio::select! {
            _ = stop_rx.changed() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                let result = if let Some(cached) = command_cache.get(&command.command.request_id) {
                    cached.clone()
                } else {
                    let result = apply_command(&mut engine, &mut projection, command.command);
                    cache_command(&mut command_cache, &mut command_order, result.clone());
                    update_metrics(
                        &projection,
                        &metrics,
                        &mut metric_rooms,
                        &mut metric_clients,
                    );
                    result
                };
                let _ = command.response.send(result);
            }
            received = socket.recv_from(&mut packet) => {
                match received {
                    Ok((size, peer_addr)) => {
                        let input = TaggedBytesMut {
                            now: Instant::now(),
                            transport: TransportContext {
                                local_addr: advertised_addr,
                                peer_addr,
                                transport_protocol: TransportProtocol::UDP,
                                ecn: None,
                            },
                            message: BytesMut::from(&packet[..size]),
                        };
                        if let Err(error) = engine.handle_read(input) {
                            log::debug!("SFU media packet dropped: local={advertised_addr} peer={peer_addr} error={error}");
                        }
                    }
                    Err(error) => log::warn!("SFU UDP receive failed: local={advertised_addr} error={error}"),
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                if let Err(error) = engine.handle_timeout(Instant::now()) {
                    log::warn!("SFU timeout failed: local={advertised_addr} error={error}");
                }
            }
        }
        drain_engine(&mut engine, &socket, &projection, &events, &next_event_id).await;
    }
    let _ = engine.close();
    metrics.rooms.fetch_sub(metric_rooms, Ordering::Relaxed);
    metrics.clients.fetch_sub(metric_clients, Ordering::Relaxed);
}

fn apply_command(
    engine: &mut sfu::Sfu,
    projection: &mut HashMap<(u64, u64), (u64, u64)>,
    command: v2::SfuCommand,
) -> v2::SfuCommandResult {
    let request_id = command.request_id;
    if request_id == 0 {
        return command_error(
            request_id,
            v2::ErrorCode::InvalidRequest,
            "request_id must be nonzero",
        );
    }
    let result = match command.command {
        Some(v2::sfu_command::Command::Join(join)) => {
            let key = (join.room_id, join.client_id);
            match projection.get(&key) {
                Some(current) if *current == (join.lifecycle_id, join.assignment_epoch) => Ok(
                    v2::sfu_command_ok::Payload::MemberJoined(v2::MemberJoined {
                        room_id: join.room_id,
                        client_id: join.client_id,
                        lifecycle_id: join.lifecycle_id,
                        assignment_epoch: join.assignment_epoch,
                    }),
                ),
                Some(_) => Err((
                    v2::ErrorCode::StaleLifecycle,
                    "conflicting member lifecycle",
                )),
                None => engine
                    .handle_event(sfu::SFUEvent::Join {
                        request_id,
                        room_id: join.room_id,
                        client_id: join.client_id,
                    })
                    .map(|()| {
                        projection.insert(key, (join.lifecycle_id, join.assignment_epoch));
                        v2::sfu_command_ok::Payload::MemberJoined(v2::MemberJoined {
                            room_id: join.room_id,
                            client_id: join.client_id,
                            lifecycle_id: join.lifecycle_id,
                            assignment_epoch: join.assignment_epoch,
                        })
                    })
                    .map_err(|_| (v2::ErrorCode::Internal, "SFU join failed")),
            }
        }
        Some(v2::sfu_command::Command::Leave(leave)) => {
            let key = (leave.room_id, leave.client_id);
            match projection.get(&key) {
                None => Ok(v2::sfu_command_ok::Payload::MemberLeft(v2::MemberLeft {
                    room_id: leave.room_id,
                    client_id: leave.client_id,
                    lifecycle_id: leave.lifecycle_id,
                    assignment_epoch: leave.assignment_epoch,
                })),
                Some((_, assignment_epoch)) if *assignment_epoch != leave.assignment_epoch => {
                    Err((
                        v2::ErrorCode::StaleAssignmentEpoch,
                        "stale assignment epoch",
                    ))
                }
                Some((lifecycle_id, _)) if *lifecycle_id != leave.lifecycle_id => {
                    Err((v2::ErrorCode::StaleLifecycle, "stale member lifecycle"))
                }
                Some(_) => engine
                    .handle_event(sfu::SFUEvent::Leave {
                        request_id,
                        room_id: leave.room_id,
                        client_id: leave.client_id,
                        reason: format!(
                            "{:?}",
                            v2::LeaveReason::try_from(leave.reason).unwrap_or_default()
                        ),
                    })
                    .map(|()| {
                        projection.remove(&key);
                        v2::sfu_command_ok::Payload::MemberLeft(v2::MemberLeft {
                            room_id: leave.room_id,
                            client_id: leave.client_id,
                            lifecycle_id: leave.lifecycle_id,
                            assignment_epoch: leave.assignment_epoch,
                        })
                    })
                    .map_err(|_| (v2::ErrorCode::Internal, "SFU leave failed")),
            }
        }
        Some(v2::sfu_command::Command::Signal(signal)) => {
            let key = (signal.room_id, signal.client_id);
            if projection.get(&key) != Some(&(signal.lifecycle_id, signal.assignment_epoch)) {
                Err((v2::ErrorCode::StaleLifecycle, "stale signal lifecycle"))
            } else {
                apply_signal(engine, request_id, signal)
                    .map(|()| v2::sfu_command_ok::Payload::Acknowledged(v2::Empty {}))
            }
        }
        Some(v2::sfu_command::Command::SyncRoom(sync)) => {
            sync_room(engine, projection, request_id, sync)
        }
        Some(v2::sfu_command::Command::Drain(_)) => {
            Ok(v2::sfu_command_ok::Payload::Acknowledged(v2::Empty {}))
        }
        None => Err((v2::ErrorCode::InvalidRequest, "missing SFU command")),
    };
    match result {
        Ok(payload) => v2::SfuCommandResult {
            request_id,
            result: Some(v2::sfu_command_result::Result::Ok(v2::SfuCommandOk {
                payload: Some(payload),
            })),
        },
        Err((code, reason)) => command_error(request_id, code, reason),
    }
}

fn sync_room(
    engine: &mut sfu::Sfu,
    projection: &mut HashMap<(u64, u64), (u64, u64)>,
    request_id: u64,
    sync: v2::SyncRoom,
) -> Result<v2::sfu_command_ok::Payload, (v2::ErrorCode, &'static str)> {
    let wanted = sync
        .members
        .iter()
        .map(|member| (member.client_id, member.lifecycle_id))
        .collect::<HashMap<_, _>>();
    let stale = projection
        .iter()
        .filter(|((room_id, client_id), _)| {
            *room_id == sync.room_id && !wanted.contains_key(client_id)
        })
        .map(|((_, client_id), _)| *client_id)
        .collect::<Vec<_>>();
    for client_id in stale {
        engine
            .handle_event(sfu::SFUEvent::Leave {
                request_id,
                room_id: sync.room_id,
                client_id,
                reason: "room synchronization".into(),
            })
            .map_err(|_| (v2::ErrorCode::Internal, "SFU room sync leave failed"))?;
        projection.remove(&(sync.room_id, client_id));
    }
    for member in sync.members {
        let key = (sync.room_id, member.client_id);
        if !projection.contains_key(&key) {
            engine
                .handle_event(sfu::SFUEvent::Join {
                    request_id,
                    room_id: sync.room_id,
                    client_id: member.client_id,
                })
                .map_err(|_| (v2::ErrorCode::Internal, "SFU room sync join failed"))?;
        }
        projection.insert(key, (member.lifecycle_id, sync.assignment_epoch));
    }
    Ok(v2::sfu_command_ok::Payload::RoomSynced(v2::RoomSynced {
        room_id: sync.room_id,
        assignment_epoch: sync.assignment_epoch,
    }))
}

#[derive(Deserialize)]
struct AppSignal {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    candidate: String,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    label: Option<u16>,
    #[serde(default)]
    requestid: Option<String>,
}

fn apply_signal(
    engine: &mut sfu::Sfu,
    command_request_id: u64,
    signal: v2::SfuSignal,
) -> Result<(), (v2::ErrorCode, &'static str)> {
    let value: serde_json::Value = serde_json::from_str(&signal.message_json)
        .map_err(|_| (v2::ErrorCode::InvalidSignal, "invalid signaling JSON"))?;
    let envelope: AppSignal = serde_json::from_value(value.clone())
        .map_err(|_| (v2::ErrorCode::InvalidSignal, "invalid signaling message"))?;
    match envelope.kind.as_str() {
        "offer" | "answer" => {
            let sdp: RTCSessionDescription = serde_json::from_value(value)
                .map_err(|_| (v2::ErrorCode::InvalidSignal, "invalid session description"))?;
            let request_id = if sdp.sdp_type == RTCSdpType::Answer {
                envelope
                    .requestid
                    .as_deref()
                    .and_then(|value| value.parse().ok())
                    .or(signal.sdp_request_id)
                    .ok_or((v2::ErrorCode::InvalidSignal, "answer missing requestid"))?
            } else {
                signal.sdp_request_id.unwrap_or(command_request_id)
            };
            engine
                .handle_event(sfu::SFUEvent::SessionDescription {
                    request_id,
                    room_id: signal.room_id,
                    client_id: signal.client_id,
                    sdp,
                })
                .map_err(|_| (v2::ErrorCode::InvalidSignal, "session description rejected"))
        }
        "candidate" => engine
            .handle_event(sfu::SFUEvent::IceCandidate {
                request_id: command_request_id,
                room_id: signal.room_id,
                client_id: signal.client_id,
                candidate: RTCIceCandidateInit {
                    candidate: envelope.candidate,
                    sdp_mid: envelope.id,
                    sdp_mline_index: envelope.label,
                    username_fragment: None,
                    url: None,
                },
            })
            .map_err(|_| (v2::ErrorCode::InvalidSignal, "ICE candidate rejected")),
        "end-of-candidates" => engine
            .handle_event(sfu::SFUEvent::IceCandidate {
                request_id: command_request_id,
                room_id: signal.room_id,
                client_id: signal.client_id,
                candidate: RTCIceCandidateInit::default(),
            })
            .map_err(|_| (v2::ErrorCode::InvalidSignal, "end of candidates rejected")),
        "bye" => Ok(()),
        _ => Err((
            v2::ErrorCode::InvalidSignal,
            "unknown signaling message type",
        )),
    }
}

async fn drain_engine(
    engine: &mut sfu::Sfu,
    socket: &UdpSocket,
    projection: &HashMap<(u64, u64), (u64, u64)>,
    events: &mpsc::Sender<v2::SfuEvent>,
    next_event_id: &AtomicU64,
) {
    // Pump the read side so the SFU dispatches inbound RTP/RTCP to each subscriber's
    // forwarding sender (`Room::poll_read` does this as a side effect, then the forwarded
    // packets surface below via `poll_write`). Without this the worker receives publisher
    // media but never forwards it. `Rout` is `Infallible`, so this only runs the side
    // effect and never yields a value.
    while engine.poll_read().is_some() {}

    while let Some(transmit) = engine.poll_write() {
        if let Err(error) = socket
            .send_to(&transmit.message, transmit.transport.peer_addr)
            .await
        {
            log::warn!(
                "SFU UDP send failed: peer={} error={error}",
                transmit.transport.peer_addr
            );
        }
    }
    while let Some(event) = engine.poll_event() {
        let Some((room_id, client_id)) = event.room_id().zip(event.client_id()) else {
            continue;
        };
        let Some(&(lifecycle_id, assignment_epoch)) = projection.get(&(room_id, client_id)) else {
            continue;
        };
        let event_kind = match event {
            sfu::SFUEvent::SessionDescription { request_id, sdp, .. } => {
                let mut value = serde_json::to_value(&sdp).unwrap_or_default();
                if sdp.sdp_type == RTCSdpType::Offer
                    && let Some(object) = value.as_object_mut()
                {
                    object.insert("requestid".into(), request_id.to_string().into());
                }
                v2::sfu_event::Event::Signal(v2::SfuSignal {
                    room_id,
                    client_id,
                    lifecycle_id,
                    assignment_epoch,
                    message_json: value.to_string(),
                    sdp_request_id: Some(request_id),
                })
            }
            sfu::SFUEvent::IceCandidate { candidate, .. } => {
                v2::sfu_event::Event::Signal(v2::SfuSignal {
                    room_id,
                    client_id,
                    lifecycle_id,
                    assignment_epoch,
                    message_json: serde_json::json!({
                        "type": if candidate.candidate.is_empty() { "end-of-candidates" } else { "candidate" },
                        "label": candidate.sdp_mline_index,
                        "id": candidate.sdp_mid,
                        "candidate": candidate.candidate,
                    }).to_string(),
                    sdp_request_id: None,
                })
            }
            sfu::SFUEvent::Err { request_id, reason, .. } => {
                v2::sfu_event::Event::Failure(v2::SfuFailure {
                    error: Some(v2::Error {
                        code: v2::ErrorCode::Internal as i32,
                        reason,
                        retryable: false,
                        retry_after_ms: None,
                    }),
                    room_id: Some(room_id),
                    client_id: Some(client_id),
                    lifecycle_id: Some(lifecycle_id),
                    sdp_request_id: Some(request_id),
                })
            }
            sfu::SFUEvent::Ok { .. } | sfu::SFUEvent::Join { .. } | sfu::SFUEvent::Leave { .. } => continue,
        };
        let event = v2::SfuEvent {
            request_id: next_nonzero(next_event_id),
            event: Some(event_kind),
        };
        if events.send(event).await.is_err() {
            log::warn!("SFU event receiver closed: room_id={room_id} client_id={client_id}");
            return;
        }
    }
}

fn cache_command(
    cache: &mut HashMap<u64, v2::SfuCommandResult>,
    order: &mut VecDeque<u64>,
    result: v2::SfuCommandResult,
) {
    if cache.insert(result.request_id, result.clone()).is_none() {
        order.push_back(result.request_id);
    }
    while order.len() > COMMAND_CACHE_CAPACITY {
        if let Some(oldest) = order.pop_front() {
            cache.remove(&oldest);
        }
    }
}

fn update_metrics(
    projection: &HashMap<(u64, u64), (u64, u64)>,
    metrics: &Metrics,
    previous_rooms: &mut u64,
    previous_clients: &mut u64,
) {
    let rooms = projection
        .keys()
        .map(|(room_id, _)| *room_id)
        .collect::<std::collections::HashSet<_>>();
    replace_contribution(&metrics.rooms, previous_rooms, rooms.len() as u64);
    replace_contribution(&metrics.clients, previous_clients, projection.len() as u64);
}

fn replace_contribution(total: &AtomicU64, previous: &mut u64, current: u64) {
    if current >= *previous {
        total.fetch_add(current - *previous, Ordering::Relaxed);
    } else {
        total.fetch_sub(*previous - current, Ordering::Relaxed);
    }
    *previous = current;
}

async fn queue_health(
    outgoing: &mpsc::Sender<SfuToSignaling>,
    pending: &mut HashMap<u64, v2::SfuEvent>,
    config: &Config,
    metrics: &Metrics,
    next_event_id: &AtomicU64,
) -> anyhow::Result<()> {
    let request_id = next_nonzero(next_event_id);
    let event = v2::SfuEvent {
        request_id,
        event: Some(v2::sfu_event::Event::Health(v2::SfuHealth {
            state: v2::SfuState::Ready as i32,
            capacity: Some(capacity(config)),
            current_rooms: metrics.rooms.load(Ordering::Relaxed),
            current_clients: metrics.clients.load(Ordering::Relaxed),
        })),
    };
    pending.insert(request_id, event.clone());
    outgoing
        .send(SfuToSignaling {
            message: Some(v2::sfu_to_signaling::Message::Event(event)),
        })
        .await?;
    log::info!(
        "SFU health sent: request_id={request_id} state=READY rooms={} clients={}",
        metrics.rooms.load(Ordering::Relaxed),
        metrics.clients.load(Ordering::Relaxed)
    );
    Ok(())
}

fn decode_registration(
    response: v2::SignalingToSfu,
    expected_request_id: u64,
) -> anyhow::Result<(Duration, bool)> {
    let registered = match response.message {
        Some(v2::signaling_to_sfu::Message::Registered(response)) => response,
        _ => anyhow::bail!("expected RegisterSfuResponse"),
    };
    anyhow::ensure!(
        registered
            .context
            .as_ref()
            .map(|context| context.request_id)
            == Some(expected_request_id),
        "registration response request_id mismatch"
    );
    match registered.result {
        Some(v2::register_sfu_response::Result::Registered(result)) => Ok((
            Duration::from_millis(result.health_interval_ms.max(1)),
            result.resumed,
        )),
        Some(v2::register_sfu_response::Result::Error(error)) => {
            anyhow::bail!("SFU registration rejected: {}", error.reason)
        }
        None => anyhow::bail!("empty SFU registration response"),
    }
}

fn command_error(
    request_id: u64,
    code: v2::ErrorCode,
    reason: impl Into<String>,
) -> v2::SfuCommandResult {
    v2::SfuCommandResult {
        request_id,
        result: Some(v2::sfu_command_result::Result::Error(v2::Error {
            code: code as i32,
            reason: reason.into(),
            retryable: false,
            retry_after_ms: None,
        })),
    }
}

fn capacity(config: &Config) -> v2::SfuCapacity {
    v2::SfuCapacity {
        max_rooms: config.max_rooms,
        max_clients: config.max_clients,
    }
}

fn next_nonzero(counter: &AtomicU64) -> u64 {
    loop {
        let value = counter.fetch_add(1, Ordering::Relaxed);
        if value != 0 {
            return value;
        }
    }
}

fn grpc_endpoint(url: &str, insecure_tls: bool) -> anyhow::Result<Endpoint> {
    let parsed = Url::parse(url)?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "--grpc-url must use http or https"
    );
    let mut endpoint = Endpoint::from_shared(url.to_owned())?
        .connect_timeout(CONNECT_TIMEOUT)
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(KEEPALIVE_INTERVAL));
    if parsed.scheme() == "https" {
        let domain = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("gRPC URL has no host"))?;
        let tls = ClientTlsConfig::new().domain_name(domain.to_owned());
        endpoint = if insecure_tls {
            endpoint.tls_config_with_verifier(tls, Arc::new(NoCertificateVerification))?
        } else {
            endpoint.tls_config(tls.with_webpki_roots())?
        };
    }
    Ok(endpoint)
}

#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
            .to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn engine() -> sfu::Sfu {
        sfu::Sfu::new(1, "127.0.0.1:3478".parse().unwrap())
    }

    #[test]
    fn join_signal_leave_commands_update_the_worker_projection() {
        let mut engine = engine();
        let mut projection = HashMap::new();
        let join = v2::JoinMember {
            room_id: 42,
            client_id: 101,
            lifecycle_id: 1,
            assignment_epoch: 1,
        };
        let joined = apply_command(
            &mut engine,
            &mut projection,
            v2::SfuCommand {
                request_id: 10,
                command: Some(v2::sfu_command::Command::Join(join)),
            },
        );
        assert!(matches!(
            joined.result,
            Some(v2::sfu_command_result::Result::Ok(v2::SfuCommandOk {
                payload: Some(v2::sfu_command_ok::Payload::MemberJoined(_)),
            }))
        ));
        assert_eq!(projection.get(&(42, 101)), Some(&(1, 1)));

        let bye = apply_command(
            &mut engine,
            &mut projection,
            v2::SfuCommand {
                request_id: 11,
                command: Some(v2::sfu_command::Command::Signal(v2::SfuSignal {
                    room_id: 42,
                    client_id: 101,
                    lifecycle_id: 1,
                    assignment_epoch: 1,
                    message_json: r#"{"type":"bye"}"#.into(),
                    sdp_request_id: None,
                })),
            },
        );
        assert!(matches!(
            bye.result,
            Some(v2::sfu_command_result::Result::Ok(_))
        ));

        let left = apply_command(
            &mut engine,
            &mut projection,
            v2::SfuCommand {
                request_id: 12,
                command: Some(v2::sfu_command::Command::Leave(v2::LeaveMember {
                    room_id: 42,
                    client_id: 101,
                    lifecycle_id: 1,
                    assignment_epoch: 1,
                    reason: v2::LeaveReason::User as i32,
                })),
            },
        );
        assert!(matches!(
            left.result,
            Some(v2::sfu_command_result::Result::Ok(v2::SfuCommandOk {
                payload: Some(v2::sfu_command_ok::Payload::MemberLeft(_)),
            }))
        ));
        assert!(projection.is_empty());
    }

    #[test]
    fn stale_signal_is_rejected_before_it_reaches_the_sfu() {
        let result = apply_command(
            &mut engine(),
            &mut HashMap::from([((42, 101), (5, 7))]),
            v2::SfuCommand {
                request_id: 20,
                command: Some(v2::sfu_command::Command::Signal(v2::SfuSignal {
                    room_id: 42,
                    client_id: 101,
                    lifecycle_id: 4,
                    assignment_epoch: 7,
                    message_json: r#"{"type":"bye"}"#.into(),
                    sdp_request_id: None,
                })),
            },
        );
        assert!(matches!(
            result.result,
            Some(v2::sfu_command_result::Result::Error(v2::Error { code, .. }))
                if code == v2::ErrorCode::StaleLifecycle as i32
        ));
    }
}
