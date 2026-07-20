//! Private gRPC adapter for AppWeb and future SFU workers.

use crate::{signaling_server::DriverCommand, tls};
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
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_stream::wrappers::TcpListenerStream;
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

#[derive(Debug, Default)]
struct RequestCache {
    entries: HashMap<RequestKey, CachedResult>,
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
}

impl GrpcSignalingService {
    pub fn new(commands: mpsc::Sender<DriverCommand>) -> Self {
        Self {
            commands,
            request_cache: Arc::new(Mutex::new(RequestCache::default())),
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
        // Collider mutations are serialized by its event loop. Holding this lock across
        // the authority round trip additionally coalesces concurrent retries of one
        // logical request before either can mutate that state.
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
        log::info!(
            "gRPC request: app_id=APPWEB instance_id={} operation={} request_id={request_id}",
            context.instance_id,
            operation_name
        );
        let (response_tx, response_rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::Authority {
                command: AuthorityCommand {
                    request_id,
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
        if response.request_id != request_id {
            return Err(GrpcStatus::internal(
                "authority response correlation failed",
            ));
        }
        log_result(context, operation_name, &response);
        request_cache.insert(
            request_key,
            CachedResult {
                operation_fingerprint,
                result: response.result.clone(),
            },
        );
        Ok(response.result)
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
            response.request_id
        ),
        _ => log::info!(
            "gRPC response: app_id=APPWEB instance_id={} operation={} request_id={} result=OK",
            context.instance_id,
            operation,
            response.request_id
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
                connected_sfu_instances: 0,
                ready_sfu_instances: 0,
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
                signal_epoch,
                admission_token,
                is_initiator,
            } => v2::admit_v2_response::Result::Admitted(V2Admission {
                mode: RoomMode::P2p as i32,
                signal_epoch,
                admission_token,
                is_initiator: Some(is_initiator),
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
            AuthorityResult::OccupancyV2 { count } => {
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

    type OpenSfuSessionStream = Pin<
        Box<dyn futures_util::Stream<Item = Result<SignalingToSfu, GrpcStatus>> + Send + 'static>,
    >;

    async fn open_sfu_session(
        &self,
        _request: Request<tonic::Streaming<SfuToSignaling>>,
    ) -> Result<Response<Self::OpenSfuSessionStream>, GrpcStatus> {
        Err(GrpcStatus::unimplemented("SFU V2 is not implemented"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signaling_server::{self, COMMAND_CAPACITY};
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
