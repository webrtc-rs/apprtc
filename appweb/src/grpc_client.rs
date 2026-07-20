//! AppWeb client for the private signaling gRPC service.

use async_trait::async_trait;
use signaling_proto::v2::signaling_service_client::SignalingServiceClient;
use signaling_proto::v2::{
    self, AdmitV1Request, AdmitV2Request, AppId, InjectV1Request, OccupancyV1Request,
    OccupancyV2Request, RemoveV1Request, RemoveV2Request, RequestContext, RoomMode, StatusRequest,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tonic::Status;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
pub trait RoomAuthority: Send + Sync {
    async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String>;
    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String>;
    async fn occupancy(&self, roomid: String) -> Result<usize, String>;
    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String>;
    async fn admit_v2(&self, room_id: u64, client_id: u64) -> Result<V2Admission, String>;
    async fn remove_v2(
        &self,
        room_id: u64,
        client_id: u64,
        admission_token: String,
    ) -> Result<(), String>;
    async fn occupancy_v2(&self, room_id: u64) -> Result<usize, String>;
    async fn status(&self) -> Result<StatusSnapshot, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    pub is_initiator: bool,
    pub messages: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2Admission {
    pub signal_epoch: u64,
    pub admission_token: String,
    pub is_initiator: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSnapshot {
    pub rooms: usize,
    pub clients: usize,
    pub websocket_connections: usize,
    pub total_websocket_connections: u64,
    pub websocket_errors: u64,
}

#[derive(Clone)]
pub struct GrpcAuthority {
    client: SignalingServiceClient<Channel>,
    instance_id: Arc<str>,
    next_request_id: Arc<AtomicU64>,
}

impl GrpcAuthority {
    pub fn connect(url: &str, insecure_tls: bool) -> Result<Self, String> {
        let parsed = Url::parse(url).map_err(|error| error.to_string())?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err("signaling gRPC URL must use http or https".into());
        }
        let mut endpoint = Endpoint::from_shared(url.to_owned())
            .map_err(|error| error.to_string())?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
            .keep_alive_timeout(KEEPALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .tcp_keepalive(Some(KEEPALIVE_INTERVAL));
        if parsed.scheme() == "https" {
            let domain = parsed
                .host_str()
                .ok_or_else(|| "signaling gRPC URL has no host".to_string())?;
            let tls = ClientTlsConfig::new().domain_name(domain.to_owned());
            endpoint = if insecure_tls {
                endpoint
                    .tls_config_with_verifier(tls, Arc::new(NoCertificateVerification))
                    .map_err(|error| error.to_string())?
            } else {
                endpoint
                    .tls_config(tls.with_webpki_roots())
                    .map_err(|error| error.to_string())?
            };
        }
        let instance_id = format!("appweb-{:032x}", rand::random::<u128>());
        log::info!(
            "Signaling gRPC channel configured: url={url} instance_id={instance_id} insecure_tls={insecure_tls}"
        );
        Ok(Self {
            client: SignalingServiceClient::new(endpoint.connect_lazy()),
            instance_id: instance_id.into(),
            next_request_id: Arc::new(AtomicU64::new(1)),
        })
    }

    fn context(&self) -> RequestContext {
        let request_id = loop {
            let candidate = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            if candidate != 0 {
                break candidate;
            }
        };
        RequestContext {
            app_id: AppId::Appweb as i32,
            instance_id: self.instance_id.to_string(),
            request_id,
        }
    }

    fn grpc_error(operation: &str, request_id: u64, error: Status, started: Instant) -> String {
        log::warn!(
            "Signaling gRPC request failed: operation={operation} request_id={request_id} code={} elapsed_ms={} error={}",
            error.code(),
            started.elapsed().as_millis(),
            error.message()
        );
        format!("signaling {operation} failed: {error}")
    }
}

fn response_request_id(context: Option<v2::ResponseContext>, expected: u64) -> Result<(), String> {
    let actual = context
        .ok_or_else(|| "signaling response missing context".to_string())?
        .request_id;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "signaling response request_id mismatch: expected {expected}, received {actual}"
        ))
    }
}

fn domain_error(error: v2::Error) -> String {
    if error.reason.is_empty() {
        v2::ErrorCode::try_from(error.code)
            .map(|code| code.as_str_name().to_owned())
            .unwrap_or_else(|_| "UNKNOWN_SIGNALING_ERROR".into())
    } else {
        error.reason
    }
}

fn log_response(operation: &str, request_id: u64, result: &str, reason: &str, started: Instant) {
    log::info!(
        "Signaling gRPC response: operation={operation} request_id={request_id} result={result} reason={reason} elapsed_ms={}",
        started.elapsed().as_millis()
    );
}

#[async_trait]
impl RoomAuthority for GrpcAuthority {
    async fn admit(
        &self,
        roomid: String,
        clientid: String,
        is_loopback: bool,
    ) -> Result<Admission, String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!("Signaling gRPC request: operation=admit_v1 request_id={request_id}");
        let mut client = self.client.clone();
        let response = client
            .admit_v1(AdmitV1Request {
                context: Some(context),
                room_id: roomid,
                client_id: clientid,
                is_loopback,
            })
            .await
            .map_err(|error| Self::grpc_error("admit_v1", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        match response.result {
            Some(v2::admit_v1_response::Result::Admitted(admitted)) => {
                log_response("admit_v1", request_id, "OK", "", started);
                Ok(Admission {
                    is_initiator: admitted.is_initiator,
                    messages: admitted.messages,
                })
            }
            Some(v2::admit_v1_response::Result::Error(error)) => {
                let reason = domain_error(error);
                log_response("admit_v1", request_id, "ERR", &reason, started);
                Err(reason)
            }
            None => Err("signaling admission response missing result".into()),
        }
    }

    async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!("Signaling gRPC request: operation=remove_v1 request_id={request_id}");
        let mut client = self.client.clone();
        let response = client
            .remove_v1(RemoveV1Request {
                context: Some(context),
                room_id: roomid,
                client_id: clientid,
            })
            .await
            .map_err(|error| Self::grpc_error("remove_v1", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        operation_result(response.result, "remove_v1", request_id, started)
    }

    async fn occupancy(&self, roomid: String) -> Result<usize, String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!("Signaling gRPC request: operation=occupancy_v1 request_id={request_id}");
        let mut client = self.client.clone();
        let response = client
            .occupancy_v1(OccupancyV1Request {
                context: Some(context),
                room_id: roomid,
            })
            .await
            .map_err(|error| Self::grpc_error("occupancy_v1", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        match response.result {
            Some(v2::occupancy_response::Result::Occupancy(occupancy)) => {
                log_response("occupancy_v1", request_id, "OK", "", started);
                usize::try_from(occupancy.member_count)
                    .map_err(|_| "occupancy count exceeds usize".into())
            }
            Some(v2::occupancy_response::Result::Error(error)) => {
                let reason = domain_error(error);
                log_response("occupancy_v1", request_id, "ERR", &reason, started);
                Err(reason)
            }
            None => Err("signaling occupancy response missing result".into()),
        }
    }

    async fn inject(&self, roomid: String, clientid: String, msg: String) -> Result<(), String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!("Signaling gRPC request: operation=inject_v1 request_id={request_id}");
        let mut client = self.client.clone();
        let response = client
            .inject_v1(InjectV1Request {
                context: Some(context),
                room_id: roomid,
                client_id: clientid,
                message_json: msg,
            })
            .await
            .map_err(|error| Self::grpc_error("inject_v1", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        operation_result(response.result, "inject_v1", request_id, started)
    }

    async fn admit_v2(&self, room_id: u64, client_id: u64) -> Result<V2Admission, String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!(
            "Signaling gRPC request: operation=admit_v2 request_id={request_id} room_id={room_id} client_id={client_id}"
        );
        let mut client = self.client.clone();
        let response = client
            .admit_v2(AdmitV2Request {
                context: Some(context),
                room_id,
                client_id,
            })
            .await
            .map_err(|error| Self::grpc_error("admit_v2", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        match response.result {
            Some(v2::admit_v2_response::Result::Admitted(admitted)) => {
                if admitted.mode != RoomMode::P2p as i32 {
                    return Err("UNSUPPORTED_ROOM_MODE".into());
                }
                let is_initiator = admitted
                    .is_initiator
                    .ok_or_else(|| "P2P admission missing is_initiator".to_string())?;
                if admitted.admission_token.is_empty() {
                    return Err("P2P admission missing admission_token".into());
                }
                log_response("admit_v2", request_id, "OK", "", started);
                Ok(V2Admission {
                    signal_epoch: admitted.signal_epoch,
                    admission_token: admitted.admission_token,
                    is_initiator,
                })
            }
            Some(v2::admit_v2_response::Result::Error(error)) => {
                let reason = domain_error(error);
                log_response("admit_v2", request_id, "ERR", &reason, started);
                Err(reason)
            }
            None => Err("signaling V2 admission response missing result".into()),
        }
    }

    async fn remove_v2(
        &self,
        room_id: u64,
        client_id: u64,
        admission_token: String,
    ) -> Result<(), String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!(
            "Signaling gRPC request: operation=remove_v2 request_id={request_id} room_id={room_id} client_id={client_id}"
        );
        let mut client = self.client.clone();
        let response = client
            .remove_v2(RemoveV2Request {
                context: Some(context),
                room_id,
                client_id,
                admission_token,
            })
            .await
            .map_err(|error| Self::grpc_error("remove_v2", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        operation_result(response.result, "remove_v2", request_id, started)
    }

    async fn occupancy_v2(&self, room_id: u64) -> Result<usize, String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!(
            "Signaling gRPC request: operation=occupancy_v2 request_id={request_id} room_id={room_id}"
        );
        let mut client = self.client.clone();
        let response = client
            .occupancy_v2(OccupancyV2Request {
                context: Some(context),
                room_id,
            })
            .await
            .map_err(|error| Self::grpc_error("occupancy_v2", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        match response.result {
            Some(v2::occupancy_response::Result::Occupancy(occupancy)) => {
                if occupancy.mode != RoomMode::P2p as i32 {
                    return Err("UNSUPPORTED_ROOM_MODE".into());
                }
                log_response("occupancy_v2", request_id, "OK", "", started);
                usize::try_from(occupancy.member_count)
                    .map_err(|_| "occupancy count exceeds usize".into())
            }
            Some(v2::occupancy_response::Result::Error(error)) => {
                let reason = domain_error(error);
                log_response("occupancy_v2", request_id, "ERR", &reason, started);
                Err(reason)
            }
            None => Err("signaling V2 occupancy response missing result".into()),
        }
    }

    async fn status(&self) -> Result<StatusSnapshot, String> {
        let context = self.context();
        let request_id = context.request_id;
        let started = Instant::now();
        log::info!("Signaling gRPC request: operation=get_status request_id={request_id}");
        let mut client = self.client.clone();
        let response = client
            .get_status(StatusRequest {
                context: Some(context),
            })
            .await
            .map_err(|error| Self::grpc_error("get_status", request_id, error, started))?
            .into_inner();
        response_request_id(response.context, request_id)?;
        match response.result {
            Some(v2::status_response::Result::Status(status)) => {
                log_response("get_status", request_id, "OK", "", started);
                Ok(StatusSnapshot {
                    rooms: usize::try_from(status.v1_rooms)
                        .map_err(|_| "room count exceeds usize")?,
                    clients: usize::try_from(status.clients)
                        .map_err(|_| "client count exceeds usize")?,
                    websocket_connections: usize::try_from(status.browser_websocket_connections)
                        .map_err(|_| "WebSocket count exceeds usize")?,
                    total_websocket_connections: status.total_browser_websocket_connections,
                    websocket_errors: status.browser_websocket_errors,
                })
            }
            Some(v2::status_response::Result::Error(error)) => {
                let reason = domain_error(error);
                log_response("get_status", request_id, "ERR", &reason, started);
                Err(reason)
            }
            None => Err("signaling status response missing result".into()),
        }
    }
}

fn operation_result(
    result: Option<v2::operation_response::Result>,
    operation: &str,
    request_id: u64,
    started: Instant,
) -> Result<(), String> {
    match result {
        Some(v2::operation_response::Result::Ok(_)) => {
            log_response(operation, request_id, "OK", "", started);
            Ok(())
        }
        Some(v2::operation_response::Result::Error(error)) => {
            let reason = domain_error(error);
            log_response(operation, request_id, "ERR", &reason, started);
            Err(reason)
        }
        None => Err(format!("signaling {operation} response missing result")),
    }
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
