//! Private gRPC adapter for AppWeb and SFU workers.

use crate::{
    signaling_server::{COMMAND_CAPACITY, DriverCommand},
    tls,
};
use rand::RngExt;
use signaling::collider::{
    AuthorityCommand, AuthorityOperation, AuthorityResponse, AuthorityResult,
};
use signaling_proto::v2::signaling_service_server::{SignalingService, SignalingServiceServer};
use signaling_proto::v2::{
    self, AdmitV1Request, AdmitV1Response, AdmitV2Request, AdmitV2Response, AppId, Empty, Error,
    ErrorCode, InjectV1Request, Occupancy, OccupancyResponse, OccupancyV1Request,
    OccupancyV2Request, OperationResponse, RemoveV1Request, RemoveV2Request, RequestContext,
    ResponseContext, RoomMode, SfuToSignaling, SignalingToSfu, Status, StatusRequest,
    StatusResponse, V1Admission, V2Admission,
};
use std::collections::{HashMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status as GrpcStatus};

const AUTHORITY_TIMEOUT: Duration = Duration::from_secs(15);
const REQUEST_CACHE_CAPACITY: usize = 4096;
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn run(
    mut stop_rx: watch::Receiver<()>,
    commands: mpsc::Sender<DriverCommand>,
    listener: TcpListener,
    tls_files: Option<(String, String)>,
) -> anyhow::Result<()> {
    let service = GrpcSignalingService::new(commands);
    let mut server = Server::builder()
        .http2_keepalive_interval(Some(KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(KEEPALIVE_TIMEOUT))
        .tcp_keepalive(Some(KEEPALIVE_INTERVAL));
    if let Some((certificate, private_key)) = tls_files {
        let (certificate, private_key) = tls::pem(&certificate, &private_key)?;
        server = server.tls_config(
            ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key)),
        )?;
    }
    server
        .add_service(SignalingServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let _ = stop_rx.changed().await;
        })
        .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RequestKey {
    instance_id: String,
    request_id: u64,
}

#[derive(Debug, Clone)]
struct CachedResult {
    operation_fingerprint: u64,
    result: AuthorityResult,
}

#[derive(Debug)]
struct InFlightRequest {
    operation_fingerprint: u64,
    completed: watch::Sender<bool>,
}

#[derive(Debug, Default)]
struct RequestCache {
    entries: HashMap<RequestKey, CachedResult>,
    in_flight: HashMap<RequestKey, InFlightRequest>,
    order: VecDeque<RequestKey>,
}

impl RequestCache {
    fn insert(&mut self, key: RequestKey, value: CachedResult) {
        if self.entries.insert(key.clone(), value).is_none() {
            self.order.push_back(key);
        }
        while self.entries.len() > REQUEST_CACHE_CAPACITY {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

#[derive(Clone)]
pub struct GrpcSignalingService {
    commands: mpsc::Sender<DriverCommand>,
    request_cache: Arc<Mutex<RequestCache>>,
    next_authority_request_id: Arc<AtomicU64>,
    next_sfu_connection_id: Arc<AtomicU64>,
}

impl GrpcSignalingService {
    pub fn new(commands: mpsc::Sender<DriverCommand>) -> Self {
        Self {
            commands,
            request_cache: Arc::new(Mutex::new(RequestCache::default())),
            next_authority_request_id: Arc::new(AtomicU64::new(1)),
            next_sfu_connection_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn app_context(context: Option<RequestContext>) -> Result<RequestContext, GrpcStatus> {
        let context = context.ok_or_else(|| GrpcStatus::invalid_argument("missing context"))?;
        if context.app_id != AppId::Appweb as i32 {
            return Err(GrpcStatus::permission_denied(
                "AppWeb RPC requires APP_ID_APPWEB",
            ));
        }
        if context.instance_id.is_empty() {
            return Err(GrpcStatus::invalid_argument(
                "instance_id must not be empty",
            ));
        }
        if context.request_id == 0 {
            return Err(GrpcStatus::invalid_argument("request_id must be nonzero"));
        }
        Ok(context)
    }

    async fn execute(
        &self,
        context: &RequestContext,
        operation: AuthorityOperation,
        operation_name: &'static str,
    ) -> Result<AuthorityResult, GrpcStatus> {
        let request_id = context.request_id;
        let request_key = RequestKey {
            instance_id: context.instance_id.clone(),
            request_id,
        };
        let operation_fingerprint = operation_fingerprint(&operation);
        loop {
            let wait_for = {
                let mut request_cache = self.request_cache.lock().await;
                if let Some(cached) = request_cache.entries.get(&request_key) {
                    if cached.operation_fingerprint != operation_fingerprint {
                        return Err(GrpcStatus::already_exists(
                            "request_id was already used for a different operation",
                        ));
                    }
                    log::info!(
                        "gRPC response replayed: app_id=APPWEB instance_id={} operation={} request_id={request_id}",
                        context.instance_id,
                        operation_name
                    );
                    return Ok(cached.result.clone());
                }
                if let Some(in_flight) = request_cache.in_flight.get(&request_key) {
                    if in_flight.operation_fingerprint != operation_fingerprint {
                        return Err(GrpcStatus::already_exists(
                            "request_id is in use for a different operation",
                        ));
                    }
                    Some(in_flight.completed.subscribe())
                } else {
                    let (completed, _) = watch::channel(false);
                    request_cache.in_flight.insert(
                        request_key.clone(),
                        InFlightRequest {
                            operation_fingerprint,
                            completed,
                        },
                    );
                    None
                }
            };
            let Some(mut completed) = wait_for else {
                break;
            };
            if !*completed.borrow() {
                let _ = completed.changed().await;
            }
        }

        let authority_request_id = self
            .next_authority_request_id
            .fetch_add(1, Ordering::Relaxed)
            .max(1);
        log::info!(
            "gRPC request: app_id=APPWEB instance_id={} operation={} request_id={request_id}",
            context.instance_id,
            operation_name
        );
        let result = async {
            let (response_tx, response_rx) = oneshot::channel();
            self.commands
                .send(DriverCommand::Authority {
                    command: AuthorityCommand {
                        request_id: authority_request_id,
                        operation,
                    },
                    response: response_tx,
                })
                .await
                .map_err(|_| GrpcStatus::unavailable("signaling authority stopped"))?;
            let response = tokio::time::timeout(AUTHORITY_TIMEOUT, response_rx)
                .await
                .map_err(|_| GrpcStatus::deadline_exceeded("signaling authority timed out"))?
                .map_err(|_| GrpcStatus::unavailable("signaling authority stopped"))?;
            if response.request_id != authority_request_id {
                return Err(GrpcStatus::internal(
                    "authority response correlation failed",
                ));
            }
            log_result(context, operation_name, &response);
            Ok(response.result)
        }
        .await;

        let mut request_cache = self.request_cache.lock().await;
        let in_flight = request_cache.in_flight.remove(&request_key);
        if let Ok(result) = &result {
            request_cache.insert(
                request_key,
                CachedResult {
                    operation_fingerprint,
                    result: result.clone(),
                },
            );
        }
        if let Some(in_flight) = in_flight {
            let _ = in_flight.completed.send(true);
        }
        result
    }
}

fn operation_fingerprint(operation: &AuthorityOperation) -> u64 {
    let mut hasher = DefaultHasher::new();
    match operation {
        AuthorityOperation::Admit {
            roomid,
            clientid,
            is_loopback,
            ..
        } => {
            "admit".hash(&mut hasher);
            roomid.hash(&mut hasher);
            clientid.hash(&mut hasher);
            is_loopback.hash(&mut hasher);
        }
        AuthorityOperation::Remove { roomid, clientid } => {
            "remove".hash(&mut hasher);
            roomid.hash(&mut hasher);
            clientid.hash(&mut hasher);
        }
        AuthorityOperation::Occupancy { roomid } => {
            "occupancy".hash(&mut hasher);
            roomid.hash(&mut hasher);
        }
        AuthorityOperation::Inject {
            roomid,
            clientid,
            msg,
            ..
        } => {
            "inject".hash(&mut hasher);
            roomid.hash(&mut hasher);
            clientid.hash(&mut hasher);
            msg.hash(&mut hasher);
        }
        AuthorityOperation::AdmitV2 {
            room_id, client_id, ..
        } => {
            "admit_v2".hash(&mut hasher);
            room_id.hash(&mut hasher);
            client_id.hash(&mut hasher);
        }
        AuthorityOperation::RemoveV2 {
            room_id,
            client_id,
            admission_token,
        } => {
            "remove_v2".hash(&mut hasher);
            room_id.hash(&mut hasher);
            client_id.hash(&mut hasher);
            admission_token.hash(&mut hasher);
        }
        AuthorityOperation::OccupancyV2 { room_id } => {
            "occupancy_v2".hash(&mut hasher);
            room_id.hash(&mut hasher);
        }
        AuthorityOperation::Status => "status".hash(&mut hasher),
    }
    hasher.finish()
}

fn response_context(request_id: u64) -> Option<ResponseContext> {
    Some(ResponseContext { request_id })
}

fn domain_error(reason: String) -> Error {
    let code = match reason.as_str() {
        "FULL" => ErrorCode::Full,
        "DUPLICATE_CLIENT" => ErrorCode::DuplicateClient,
        "ROOM_NOT_FOUND" => ErrorCode::RoomNotFound,
        "CLIENT_NOT_FOUND" => ErrorCode::ClientNotFound,
        "UNAUTHORIZED" => ErrorCode::Unauthorized,
        "NO_SFU_AVAILABLE" => ErrorCode::NoSfuAvailable,
        "ROOM_TRANSITION" => ErrorCode::RoomTransition,
        "WORKER_UNAVAILABLE" => ErrorCode::WorkerUnavailable,
        "STALE_ASSIGNMENT" => ErrorCode::StaleAssignmentEpoch,
        "STALE_LIFECYCLE" => ErrorCode::StaleLifecycle,
        "RESOURCE_EXHAUSTED" => ErrorCode::ResourceExhausted,
        _ => ErrorCode::InvalidRequest,
    };
    Error {
        code: code as i32,
        reason,
        retryable: false,
        retry_after_ms: None,
    }
}

fn log_result(context: &RequestContext, operation: &str, response: &AuthorityResponse) {
    match &response.result {
        AuthorityResult::Error { result } => log::info!(
            "gRPC response: app_id=APPWEB instance_id={} operation={} request_id={} result=ERR reason={result}",
            context.instance_id,
            operation,
            context.request_id
        ),
        _ => log::info!(
            "gRPC response: app_id=APPWEB instance_id={} operation={} request_id={} result=OK",
            context.instance_id,
            operation,
            context.request_id
        ),
    }
}

#[tonic::async_trait]
impl SignalingService for GrpcSignalingService {
    async fn admit_v1(
        &self,
        request: Request<AdmitV1Request>,
    ) -> Result<Response<AdmitV1Response>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        if request.room_id.is_empty() || request.client_id.is_empty() {
            return Err(GrpcStatus::invalid_argument(
                "room_id and client_id must not be empty",
            ));
        }
        let result = self
            .execute(
                &context,
                AuthorityOperation::Admit {
                    roomid: request.room_id,
                    clientid: request.client_id,
                    is_loopback: request.is_loopback,
                    now: Instant::now(),
                },
                "admit_v1",
            )
            .await?;
        let result = match result {
            AuthorityResult::Admitted {
                is_initiator,
                messages,
            } => v2::admit_v1_response::Result::Admitted(V1Admission {
                is_initiator,
                messages,
            }),
            AuthorityResult::Error { result } => {
                v2::admit_v1_response::Result::Error(domain_error(result))
            }
            _ => return Err(GrpcStatus::internal("unexpected authority response")),
        };
        Ok(Response::new(AdmitV1Response {
            context: response_context(context.request_id),
            result: Some(result),
        }))
    }

    async fn remove_v1(
        &self,
        request: Request<RemoveV1Request>,
    ) -> Result<Response<OperationResponse>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        if request.room_id.is_empty() || request.client_id.is_empty() {
            return Err(GrpcStatus::invalid_argument(
                "room_id and client_id must not be empty",
            ));
        }
        let result = self
            .execute(
                &context,
                AuthorityOperation::Remove {
                    roomid: request.room_id,
                    clientid: request.client_id,
                },
                "remove_v1",
            )
            .await?;
        Ok(Response::new(operation_response(
            context.request_id,
            result,
        )?))
    }

    async fn occupancy_v1(
        &self,
        request: Request<OccupancyV1Request>,
    ) -> Result<Response<OccupancyResponse>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        if request.room_id.is_empty() {
            return Err(GrpcStatus::invalid_argument("room_id must not be empty"));
        }
        let result = self
            .execute(
                &context,
                AuthorityOperation::Occupancy {
                    roomid: request.room_id,
                },
                "occupancy_v1",
            )
            .await?;
        let result = match result {
            AuthorityResult::Occupancy { count } => {
                v2::occupancy_response::Result::Occupancy(Occupancy {
                    member_count: u64::try_from(count).unwrap_or(u64::MAX),
                    mode: RoomMode::P2p as i32,
                })
            }
            AuthorityResult::Error { result } => {
                v2::occupancy_response::Result::Error(domain_error(result))
            }
            _ => return Err(GrpcStatus::internal("unexpected authority response")),
        };
        Ok(Response::new(OccupancyResponse {
            context: response_context(context.request_id),
            result: Some(result),
        }))
    }

    async fn inject_v1(
        &self,
        request: Request<InjectV1Request>,
    ) -> Result<Response<OperationResponse>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        if request.room_id.is_empty()
            || request.client_id.is_empty()
            || request.message_json.is_empty()
        {
            return Err(GrpcStatus::invalid_argument(
                "room_id, client_id, and message_json must not be empty",
            ));
        }
        let result = self
            .execute(
                &context,
                AuthorityOperation::Inject {
                    roomid: request.room_id,
                    clientid: request.client_id,
                    msg: request.message_json,
                    now: Instant::now(),
                },
                "inject_v1",
            )
            .await?;
        Ok(Response::new(operation_response(
            context.request_id,
            result,
        )?))
    }

    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        let result = self
            .execute(&context, AuthorityOperation::Status, "get_status")
            .await?;
        let result = match result {
            AuthorityResult::Status(status) => v2::status_response::Result::Status(Status {
                v1_rooms: u64::try_from(status.rooms).unwrap_or(u64::MAX),
                v2_rooms: u64::try_from(status.v2_rooms).unwrap_or(u64::MAX),
                clients: u64::try_from(status.clients).unwrap_or(u64::MAX),
                browser_websocket_connections: u64::try_from(status.websocket_connections)
                    .unwrap_or(u64::MAX),
                total_browser_websocket_connections: status.total_websocket_connections,
                browser_websocket_errors: status.websocket_errors,
                connected_sfu_instances: u64::try_from(status.connected_sfu_instances)
                    .unwrap_or(u64::MAX),
                ready_sfu_instances: u64::try_from(status.ready_sfu_instances).unwrap_or(u64::MAX),
            }),
            AuthorityResult::Error { result } => {
                v2::status_response::Result::Error(domain_error(result))
            }
            _ => return Err(GrpcStatus::internal("unexpected authority response")),
        };
        Ok(Response::new(StatusResponse {
            context: response_context(context.request_id),
            result: Some(result),
        }))
    }

    async fn admit_v2(
        &self,
        request: Request<AdmitV2Request>,
    ) -> Result<Response<AdmitV2Response>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        let result = self
            .execute(
                &context,
                AuthorityOperation::AdmitV2 {
                    room_id: request.room_id,
                    client_id: request.client_id,
                    admission_token: new_admission_token(),
                    now: Instant::now(),
                },
                "admit_v2",
            )
            .await?;
        let result = match result {
            AuthorityResult::AdmittedV2 {
                mode,
                signal_epoch,
                admission_token,
                is_initiator,
            } => v2::admit_v2_response::Result::Admitted(V2Admission {
                mode: proto_room_mode(mode) as i32,
                signal_epoch,
                admission_token,
                is_initiator,
            }),
            AuthorityResult::Error { result } => {
                v2::admit_v2_response::Result::Error(domain_error(result))
            }
            _ => return Err(GrpcStatus::internal("unexpected authority response")),
        };
        Ok(Response::new(AdmitV2Response {
            context: response_context(context.request_id),
            result: Some(result),
        }))
    }

    async fn remove_v2(
        &self,
        request: Request<RemoveV2Request>,
    ) -> Result<Response<OperationResponse>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        if request.admission_token.is_empty() {
            return Err(GrpcStatus::invalid_argument(
                "admission_token must not be empty",
            ));
        }
        let result = self
            .execute(
                &context,
                AuthorityOperation::RemoveV2 {
                    room_id: request.room_id,
                    client_id: request.client_id,
                    admission_token: request.admission_token,
                },
                "remove_v2",
            )
            .await?;
        Ok(Response::new(operation_response(
            context.request_id,
            result,
        )?))
    }

    async fn occupancy_v2(
        &self,
        request: Request<OccupancyV2Request>,
    ) -> Result<Response<OccupancyResponse>, GrpcStatus> {
        let request = request.into_inner();
        let context = Self::app_context(request.context)?;
        let result = self
            .execute(
                &context,
                AuthorityOperation::OccupancyV2 {
                    room_id: request.room_id,
                },
                "occupancy_v2",
            )
            .await?;
        let result = match result {
            AuthorityResult::OccupancyV2 { count, mode } => {
                v2::occupancy_response::Result::Occupancy(Occupancy {
                    member_count: u64::try_from(count).unwrap_or(u64::MAX),
                    mode: proto_room_mode(mode) as i32,
                })
            }
            AuthorityResult::Error { result } => {
                v2::occupancy_response::Result::Error(domain_error(result))
            }
            _ => return Err(GrpcStatus::internal("unexpected authority response")),
        };
        Ok(Response::new(OccupancyResponse {
            context: response_context(context.request_id),
            result: Some(result),
        }))
    }

    type OpenSfuSessionStream = Pin<
        Box<dyn futures_util::Stream<Item = Result<SignalingToSfu, GrpcStatus>> + Send + 'static>,
    >;

    async fn open_sfu_session(
        &self,
        request: Request<tonic::Streaming<SfuToSignaling>>,
    ) -> Result<Response<Self::OpenSfuSessionStream>, GrpcStatus> {
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(AUTHORITY_TIMEOUT, inbound.message())
            .await
            .map_err(|_| GrpcStatus::deadline_exceeded("timed out waiting for RegisterSfu"))??
            .ok_or_else(|| GrpcStatus::invalid_argument("missing RegisterSfu"))?;
        let register = match first.message {
            Some(v2::sfu_to_signaling::Message::Register(register)) => register,
            _ => {
                return Err(GrpcStatus::invalid_argument(
                    "RegisterSfu must be the first message",
                ));
            }
        };
        let context = sfu_context(register.context)?;
        let capacity = register
            .capacity
            .ok_or_else(|| GrpcStatus::invalid_argument("missing SFU capacity"))?;
        if capacity.max_rooms == 0 || capacity.max_clients == 0 {
            return Err(GrpcStatus::invalid_argument(
                "SFU capacity limits must be nonzero",
            ));
        }
        let connection_id = self
            .next_sfu_connection_id
            .fetch_add(1, Ordering::Relaxed)
            .max(1);
        let instance_id = context.instance_id.clone();
        let (domain_tx, mut domain_rx) = mpsc::channel(COMMAND_CAPACITY);
        self.commands
            .send(DriverCommand::SfuConnected {
                input: signaling::sfu::Input::Register {
                    connection_id,
                    instance_id: instance_id.clone(),
                    request_id: context.request_id,
                    capacity: signaling::sfu::Capacity {
                        max_rooms: capacity.max_rooms,
                        max_clients: capacity.max_clients,
                    },
                },
                output: domain_tx,
            })
            .await
            .map_err(|_| GrpcStatus::unavailable("signaling authority stopped"))?;

        let commands = self.commands.clone();
        let (wire_tx, wire_rx) = mpsc::channel(COMMAND_CAPACITY);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    incoming = inbound.message() => {
                        match incoming {
                            Ok(Some(message)) => match decode_sfu_input(connection_id, &instance_id, message) {
                                Ok(input) => {
                                    if commands.send(DriverCommand::SfuInput { input }).await.is_err() {
                                        break;
                                    }
                                }
                                Err(status) => {
                                    let _ = wire_tx.send(Err(status)).await;
                                    break;
                                }
                            },
                            Ok(None) => break,
                            Err(error) => {
                                log::warn!("SFU gRPC receive failed: instance_id={instance_id} connection_id={connection_id} error={error}");
                                break;
                            }
                        }
                    }
                    output = domain_rx.recv() => {
                        let Some(output) = output else { break };
                        if matches!(output, signaling::sfu::Output::Close { .. }) {
                            break;
                        }
                        if wire_tx.send(Ok(encode_sfu_output(output))).await.is_err() {
                            break;
                        }
                    }
                }
            }
            let _ = commands
                .send(DriverCommand::SfuInput {
                    input: signaling::sfu::Input::Disconnected {
                        connection_id,
                        instance_id: instance_id.clone(),
                        now: Instant::now(),
                    },
                })
                .await;
            log::info!(
                "SFU gRPC session closed: instance_id={instance_id} connection_id={connection_id}"
            );
        });

        log::info!(
            "SFU gRPC session opened: instance_id={} connection_id={} request_id={} max_rooms={} max_clients={}",
            context.instance_id,
            connection_id,
            context.request_id,
            capacity.max_rooms,
            capacity.max_clients
        );
        Ok(Response::new(Box::pin(ReceiverStream::new(wire_rx))))
    }
}

fn sfu_context(context: Option<RequestContext>) -> Result<RequestContext, GrpcStatus> {
    let context = context.ok_or_else(|| GrpcStatus::invalid_argument("missing context"))?;
    if context.app_id != AppId::Sfu as i32 {
        return Err(GrpcStatus::permission_denied(
            "SFU session requires APP_ID_SFU",
        ));
    }
    if context.instance_id.is_empty() {
        return Err(GrpcStatus::invalid_argument(
            "instance_id must not be empty",
        ));
    }
    if context.request_id == 0 {
        return Err(GrpcStatus::invalid_argument("request_id must be nonzero"));
    }
    Ok(context)
}

fn decode_sfu_input(
    connection_id: u64,
    instance_id: &str,
    message: SfuToSignaling,
) -> Result<signaling::sfu::Input, GrpcStatus> {
    match message.message {
        Some(v2::sfu_to_signaling::Message::Register(_)) => Err(GrpcStatus::invalid_argument(
            "RegisterSfu must appear exactly once",
        )),
        Some(v2::sfu_to_signaling::Message::CommandResult(result)) => {
            if result.request_id == 0 {
                return Err(GrpcStatus::invalid_argument(
                    "command result request_id must be nonzero",
                ));
            }
            let result_value = match result.result {
                Some(v2::sfu_command_result::Result::Ok(ok)) => {
                    let payload = match ok.payload {
                        Some(v2::sfu_command_ok::Payload::Acknowledged(_)) => {
                            signaling::sfu::CommandOk::Acknowledged
                        }
                        Some(v2::sfu_command_ok::Payload::MemberJoined(joined)) => {
                            signaling::sfu::CommandOk::MemberJoined(signaling::sfu::JoinMember {
                                room_id: joined.room_id,
                                client_id: joined.client_id,
                                lifecycle_id: joined.lifecycle_id,
                                assignment_epoch: joined.assignment_epoch,
                            })
                        }
                        Some(v2::sfu_command_ok::Payload::MemberLeft(left)) => {
                            signaling::sfu::CommandOk::MemberLeft(signaling::sfu::LeaveMember {
                                room_id: left.room_id,
                                client_id: left.client_id,
                                lifecycle_id: left.lifecycle_id,
                                assignment_epoch: left.assignment_epoch,
                                reason: signaling::sfu::LeaveReason::User,
                            })
                        }
                        Some(v2::sfu_command_ok::Payload::RoomSynced(synced)) => {
                            signaling::sfu::CommandOk::RoomSynced(signaling::sfu::RoomSynced {
                                room_id: synced.room_id,
                                assignment_epoch: synced.assignment_epoch,
                            })
                        }
                        None => signaling::sfu::CommandOk::Acknowledged,
                    };
                    Ok(payload)
                }
                Some(v2::sfu_command_result::Result::Error(error)) => Err(signaling::sfu::Error {
                    reason: error.reason,
                    retryable: error.retryable,
                }),
                None => return Err(GrpcStatus::invalid_argument("missing command result")),
            };
            Ok(signaling::sfu::Input::CommandResult {
                connection_id,
                instance_id: instance_id.into(),
                result: signaling::sfu::CommandResult {
                    request_id: result.request_id,
                    result: result_value,
                },
            })
        }
        Some(v2::sfu_to_signaling::Message::Event(event)) => {
            if event.request_id == 0 {
                return Err(GrpcStatus::invalid_argument(
                    "event request_id must be nonzero",
                ));
            }
            let event_kind = match event.event {
                Some(v2::sfu_event::Event::Health(health)) => {
                    let capacity = health
                        .capacity
                        .ok_or_else(|| GrpcStatus::invalid_argument("missing health capacity"))?;
                    let state = match v2::SfuState::try_from(health.state) {
                        Ok(v2::SfuState::Ready) => signaling::sfu::State::Ready,
                        Ok(v2::SfuState::Draining) => signaling::sfu::State::Draining,
                        _ => return Err(GrpcStatus::invalid_argument("invalid SFU health state")),
                    };
                    signaling::sfu::EventKind::Health(signaling::sfu::Health {
                        state,
                        capacity: signaling::sfu::Capacity {
                            max_rooms: capacity.max_rooms,
                            max_clients: capacity.max_clients,
                        },
                        current_rooms: health.current_rooms,
                        current_clients: health.current_clients,
                    })
                }
                Some(v2::sfu_event::Event::Signal(signal)) => {
                    signaling::sfu::EventKind::Signal(decode_signal(signal)?)
                }
                Some(v2::sfu_event::Event::Failure(failure)) => {
                    let error = failure
                        .error
                        .ok_or_else(|| GrpcStatus::invalid_argument("missing SFU failure error"))?;
                    signaling::sfu::EventKind::Failure {
                        error: signaling::sfu::Error {
                            reason: error.reason,
                            retryable: error.retryable,
                        },
                        room_id: failure.room_id,
                        client_id: failure.client_id,
                        lifecycle_id: failure.lifecycle_id,
                        sdp_request_id: failure.sdp_request_id,
                    }
                }
                None => return Err(GrpcStatus::invalid_argument("missing SFU event")),
            };
            Ok(signaling::sfu::Input::Event {
                connection_id,
                instance_id: instance_id.into(),
                event: signaling::sfu::Event {
                    request_id: event.request_id,
                    event: event_kind,
                },
            })
        }
        None => Err(GrpcStatus::invalid_argument("empty SFU message")),
    }
}

fn decode_signal(signal: v2::SfuSignal) -> Result<signaling::sfu::Signal, GrpcStatus> {
    if signal.message_json.is_empty() {
        return Err(GrpcStatus::invalid_argument(
            "SFU signal message_json must not be empty",
        ));
    }
    Ok(signaling::sfu::Signal {
        room_id: signal.room_id,
        client_id: signal.client_id,
        lifecycle_id: signal.lifecycle_id,
        assignment_epoch: signal.assignment_epoch,
        message_json: signal.message_json,
        sdp_request_id: signal.sdp_request_id,
    })
}

fn encode_sfu_output(output: signaling::sfu::Output) -> SignalingToSfu {
    let message = match output {
        signaling::sfu::Output::Registered {
            request_id,
            health_interval_ms,
            resumed,
            ..
        } => v2::signaling_to_sfu::Message::Registered(v2::RegisterSfuResponse {
            context: response_context(request_id),
            result: Some(v2::register_sfu_response::Result::Registered(
                v2::SfuRegistered {
                    health_interval_ms,
                    resumed,
                },
            )),
        }),
        signaling::sfu::Output::RegistrationError {
            request_id, error, ..
        } => v2::signaling_to_sfu::Message::Registered(v2::RegisterSfuResponse {
            context: response_context(request_id),
            result: Some(v2::register_sfu_response::Result::Error(domain_error(
                error.reason,
            ))),
        }),
        signaling::sfu::Output::Command { command, .. } => {
            let command_kind = match command.command {
                signaling::sfu::CommandKind::SyncRoom(sync) => {
                    v2::sfu_command::Command::SyncRoom(v2::SyncRoom {
                        room_id: sync.room_id,
                        assignment_epoch: sync.assignment_epoch,
                        members: sync
                            .members
                            .into_iter()
                            .map(|member| v2::MemberProjection {
                                client_id: member.client_id,
                                lifecycle_id: member.lifecycle_id,
                            })
                            .collect(),
                    })
                }
                signaling::sfu::CommandKind::Join(join) => {
                    v2::sfu_command::Command::Join(v2::JoinMember {
                        room_id: join.room_id,
                        client_id: join.client_id,
                        lifecycle_id: join.lifecycle_id,
                        assignment_epoch: join.assignment_epoch,
                    })
                }
                signaling::sfu::CommandKind::Leave(leave) => {
                    v2::sfu_command::Command::Leave(v2::LeaveMember {
                        room_id: leave.room_id,
                        client_id: leave.client_id,
                        lifecycle_id: leave.lifecycle_id,
                        assignment_epoch: leave.assignment_epoch,
                        reason: v2::LeaveReason::User as i32,
                    })
                }
                signaling::sfu::CommandKind::Signal(signal) => {
                    v2::sfu_command::Command::Signal(encode_signal(signal))
                }
            };
            v2::signaling_to_sfu::Message::Command(v2::SfuCommand {
                request_id: command.request_id,
                command: Some(command_kind),
            })
        }
        signaling::sfu::Output::EventAck { request_id, .. } => {
            v2::signaling_to_sfu::Message::EventAck(v2::SfuEventAck { request_id })
        }
        signaling::sfu::Output::Close { .. } => {
            unreachable!("close is consumed by the stream adapter")
        }
    };
    SignalingToSfu {
        message: Some(message),
    }
}

fn encode_signal(signal: signaling::sfu::Signal) -> v2::SfuSignal {
    v2::SfuSignal {
        room_id: signal.room_id,
        client_id: signal.client_id,
        lifecycle_id: signal.lifecycle_id,
        assignment_epoch: signal.assignment_epoch,
        message_json: signal.message_json,
        sdp_request_id: signal.sdp_request_id,
    }
}

fn operation_response(
    request_id: u64,
    result: AuthorityResult,
) -> Result<OperationResponse, GrpcStatus> {
    let result = match result {
        AuthorityResult::Removed | AuthorityResult::RemovedV2 | AuthorityResult::Injected => {
            v2::operation_response::Result::Ok(Empty {})
        }
        AuthorityResult::Error { result } => {
            v2::operation_response::Result::Error(domain_error(result))
        }
        _ => return Err(GrpcStatus::internal("unexpected authority response")),
    };
    Ok(OperationResponse {
        context: response_context(request_id),
        result: Some(result),
    })
}

fn new_admission_token() -> String {
    let bytes: [u8; 32] = rand::rng().random();
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(token, "{byte:02x}");
    }
    token
}

fn proto_room_mode(mode: signaling::v2::RoomMode) -> RoomMode {
    match mode {
        signaling::v2::RoomMode::P2p => RoomMode::P2p,
        signaling::v2::RoomMode::Upgrading => RoomMode::Upgrading,
        signaling::v2::RoomMode::Sfu => RoomMode::Sfu,
        signaling::v2::RoomMode::Failed => RoomMode::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signaling_server::{self, COMMAND_CAPACITY};
    use signaling_proto::v2::signaling_service_client::SignalingServiceClient;
    use signaling_proto::v2::{
        admit_v1_response, admit_v2_response, occupancy_response, operation_response,
        status_response,
    };
    use tokio::sync::watch;

    struct Harness {
        stop: watch::Sender<()>,
        run: tokio::task::JoinHandle<()>,
        service: GrpcSignalingService,
    }

    impl Harness {
        fn spawn() -> Self {
            let (stop, stop_rx) = watch::channel(());
            let (commands, receiver) = mpsc::channel(COMMAND_CAPACITY);
            let run = tokio::spawn(signaling_server::run(
                stop_rx,
                receiver,
                Duration::from_secs(10),
            ));
            Self {
                stop,
                run,
                service: GrpcSignalingService::new(commands),
            }
        }

        async fn shutdown(self) {
            self.stop.send(()).unwrap();
            self.run.await.unwrap();
        }
    }

    fn context(request_id: u64) -> RequestContext {
        RequestContext {
            app_id: AppId::Appweb as i32,
            instance_id: "appweb-test-instance".into(),
            request_id,
        }
    }

    #[tokio::test]
    async fn v1_grpc_methods_preserve_admission_queue_occupancy_and_status() {
        let harness = Harness::spawn();
        let first = harness
            .service
            .admit_v1(Request::new(AdmitV1Request {
                context: Some(context(1)),
                room_id: "opaque-room".into(),
                client_id: "opaque-client-a".into(),
                is_loopback: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            &first.result,
            Some(admit_v1_response::Result::Admitted(V1Admission {
                is_initiator: true,
                ..
            }))
        ));
        let replayed = harness
            .service
            .admit_v1(Request::new(AdmitV1Request {
                context: Some(context(1)),
                room_id: "opaque-room".into(),
                client_id: "opaque-client-a".into(),
                is_loopback: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(replayed, first);

        let reused_id = harness
            .service
            .occupancy_v1(Request::new(OccupancyV1Request {
                context: Some(context(1)),
                room_id: "opaque-room".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(reused_id.code(), tonic::Code::AlreadyExists);

        harness
            .service
            .inject_v1(Request::new(InjectV1Request {
                context: Some(context(2)),
                room_id: "opaque-room".into(),
                client_id: "opaque-client-a".into(),
                message_json: "offer".into(),
            }))
            .await
            .unwrap();
        let second = harness
            .service
            .admit_v1(Request::new(AdmitV1Request {
                context: Some(context(3)),
                room_id: "opaque-room".into(),
                client_id: "opaque-client-b".into(),
                is_loopback: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            second.result,
            Some(admit_v1_response::Result::Admitted(V1Admission {
                is_initiator: false,
                messages,
            })) if messages == ["offer"]
        ));

        let occupancy = harness
            .service
            .occupancy_v1(Request::new(OccupancyV1Request {
                context: Some(context(4)),
                room_id: "opaque-room".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            occupancy.result,
            Some(occupancy_response::Result::Occupancy(Occupancy {
                member_count: 2,
                mode,
            })) if mode == RoomMode::P2p as i32
        ));

        let status = harness
            .service
            .get_status(Request::new(StatusRequest {
                context: Some(context(5)),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            status.result,
            Some(status_response::Result::Status(Status {
                v1_rooms: 1,
                v2_rooms: 0,
                clients: 2,
                ..
            }))
        ));

        harness
            .service
            .remove_v1(Request::new(RemoveV1Request {
                context: Some(context(6)),
                room_id: "opaque-room".into(),
                client_id: "opaque-client-a".into(),
            }))
            .await
            .unwrap();
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn v2_grpc_methods_admit_remove_report_occupancy_and_status() {
        let harness = Harness::spawn();
        let first = harness
            .service
            .admit_v2(Request::new(AdmitV2Request {
                context: Some(context(10)),
                room_id: 42,
                client_id: 101,
            }))
            .await
            .unwrap()
            .into_inner();
        let token = match first.result {
            Some(admit_v2_response::Result::Admitted(V2Admission {
                mode,
                signal_epoch: 0,
                admission_token,
                is_initiator: Some(true),
            })) if mode == RoomMode::P2p as i32 => admission_token,
            result => panic!("unexpected V2 admission: {result:?}"),
        };
        let occupancy = harness
            .service
            .occupancy_v2(Request::new(OccupancyV2Request {
                context: Some(context(11)),
                room_id: 42,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            occupancy.result,
            Some(occupancy_response::Result::Occupancy(Occupancy {
                member_count: 1,
                mode,
            })) if mode == RoomMode::P2p as i32
        ));
        harness
            .service
            .admit_v2(Request::new(AdmitV2Request {
                context: Some(context(14)),
                room_id: 42,
                client_id: 102,
            }))
            .await
            .unwrap();
        let third = harness
            .service
            .admit_v2(Request::new(AdmitV2Request {
                context: Some(context(15)),
                room_id: 42,
                client_id: 103,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            third.result,
            Some(admit_v2_response::Result::Error(Error { code, .. }))
                if code == ErrorCode::NoSfuAvailable as i32
        ));
        let status = harness
            .service
            .get_status(Request::new(StatusRequest {
                context: Some(context(12)),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            status.result,
            Some(status_response::Result::Status(Status {
                v1_rooms: 0,
                v2_rooms: 1,
                clients: 2,
                ..
            }))
        ));
        let removed = harness
            .service
            .remove_v2(Request::new(RemoveV2Request {
                context: Some(context(13)),
                room_id: 42,
                client_id: 101,
                admission_token: token,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            removed.result,
            Some(operation_response::Result::Ok(Empty {}))
        ));
        harness.shutdown().await;
    }

    #[tokio::test]
    async fn sfu_stream_registration_and_join_barrier_commit_a_v2_upgrade() {
        let (driver_stop, driver_stop_rx) = watch::channel(());
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let driver = tokio::spawn(signaling_server::run(
            driver_stop_rx,
            command_rx,
            Duration::from_secs(10),
        ));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (grpc_stop, grpc_stop_rx) = watch::channel(());
        let server = tokio::spawn(run(grpc_stop_rx, commands, listener, None));
        let mut client = SignalingServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();

        let (sfu_tx, sfu_rx) = mpsc::channel(16);
        sfu_tx
            .send(SfuToSignaling {
                message: Some(v2::sfu_to_signaling::Message::Register(v2::RegisterSfu {
                    context: Some(RequestContext {
                        app_id: AppId::Sfu as i32,
                        instance_id: "sfu-test-instance".into(),
                        request_id: 1,
                    }),
                    capacity: Some(v2::SfuCapacity {
                        max_rooms: 10,
                        max_clients: 100,
                    }),
                })),
            })
            .await
            .unwrap();
        let mut sfu_stream = client
            .open_sfu_session(ReceiverStream::new(sfu_rx))
            .await
            .unwrap()
            .into_inner();
        assert!(matches!(
            sfu_stream.message().await.unwrap().unwrap().message,
            Some(v2::signaling_to_sfu::Message::Registered(
                v2::RegisterSfuResponse {
                    result: Some(v2::register_sfu_response::Result::Registered(_)),
                    ..
                }
            ))
        ));
        sfu_tx
            .send(SfuToSignaling {
                message: Some(v2::sfu_to_signaling::Message::Event(v2::SfuEvent {
                    request_id: 2,
                    event: Some(v2::sfu_event::Event::Health(v2::SfuHealth {
                        state: v2::SfuState::Ready as i32,
                        capacity: Some(v2::SfuCapacity {
                            max_rooms: 10,
                            max_clients: 100,
                        }),
                        current_rooms: 0,
                        current_clients: 0,
                    })),
                })),
            })
            .await
            .unwrap();
        assert!(matches!(
            sfu_stream.message().await.unwrap().unwrap().message,
            Some(v2::signaling_to_sfu::Message::EventAck(v2::SfuEventAck {
                request_id: 2
            }))
        ));

        for (request_id, client_id) in [(10, 101), (11, 102)] {
            let response = client
                .admit_v2(Request::new(AdmitV2Request {
                    context: Some(context(request_id)),
                    room_id: 42,
                    client_id,
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(matches!(
                response.result,
                Some(admit_v2_response::Result::Admitted(V2Admission { mode, .. }))
                    if mode == RoomMode::P2p as i32
            ));
        }

        let mut admission_client = client.clone();
        let third_admission = tokio::spawn(async move {
            admission_client
                .admit_v2(Request::new(AdmitV2Request {
                    context: Some(context(12)),
                    room_id: 42,
                    client_id: 103,
                }))
                .await
                .unwrap()
                .into_inner()
        });
        let status = tokio::time::timeout(
            Duration::from_secs(1),
            client.get_status(Request::new(StatusRequest {
                context: Some(context(13)),
            })),
        )
        .await
        .expect("an in-flight SFU admission must not block unrelated unary RPCs")
        .unwrap()
        .into_inner();
        assert!(matches!(
            status.result,
            Some(status_response::Result::Status(Status {
                v2_rooms: 1,
                connected_sfu_instances: 1,
                ready_sfu_instances: 1,
                ..
            }))
        ));
        for expected_client in [101, 102, 103] {
            let command = match sfu_stream.message().await.unwrap().unwrap().message {
                Some(v2::signaling_to_sfu::Message::Command(command)) => command,
                message => panic!("expected SFU command, got {message:?}"),
            };
            let join = match command.command {
                Some(v2::sfu_command::Command::Join(join)) => join,
                command => panic!("expected JoinMember, got {command:?}"),
            };
            assert_eq!(join.client_id, expected_client);
            sfu_tx
                .send(SfuToSignaling {
                    message: Some(v2::sfu_to_signaling::Message::CommandResult(
                        v2::SfuCommandResult {
                            request_id: command.request_id,
                            result: Some(v2::sfu_command_result::Result::Ok(v2::SfuCommandOk {
                                payload: Some(v2::sfu_command_ok::Payload::MemberJoined(
                                    v2::MemberJoined {
                                        room_id: join.room_id,
                                        client_id: join.client_id,
                                        lifecycle_id: join.lifecycle_id,
                                        assignment_epoch: join.assignment_epoch,
                                    },
                                )),
                            })),
                        },
                    )),
                })
                .await
                .unwrap();
        }
        let response = third_admission.await.unwrap();
        assert!(matches!(
            response.result,
            Some(admit_v2_response::Result::Admitted(V2Admission {
                mode,
                signal_epoch: 1,
                is_initiator: None,
                ..
            })) if mode == RoomMode::Sfu as i32
        ));

        drop(sfu_tx);
        grpc_stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        let _ = driver_stop.send(());
        driver.await.unwrap();
    }

    #[tokio::test]
    async fn grpc_rejects_invalid_context_and_v2_token() {
        let harness = Harness::spawn();
        let error = harness
            .service
            .get_status(Request::new(StatusRequest { context: None }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        let error = harness
            .service
            .remove_v2(Request::new(RemoveV2Request {
                context: Some(context(1)),
                room_id: 1,
                client_id: 2,
                admission_token: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        harness.shutdown().await;
    }
}
