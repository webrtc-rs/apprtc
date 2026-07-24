//! AppRTC V1 room API, HTML pages, and static assets.

use crate::config::Config;
use crate::dashboard::StatusReport;
use crate::grpc_client::RoomAuthority;
use crate::params::{RoomParameters, generate_random};
use crate::templates::Templates;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, AUTHORIZATION, CONTENT_TYPE, HOST,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use serde::Serialize;
use serde_json::json;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tower_http::services::ServeDir;
use url::Url;

const MAX_ROOM_CAPACITY: usize = 2;

struct Inner {
    config: Config,
    templates: Templates,
    authority: Arc<dyn RoomAuthority>,
    started: Instant,
    http_errors: AtomicU64,
}

#[derive(Clone)]
pub struct RoomServer {
    inner: Arc<Inner>,
}

#[derive(Debug, Default, Serialize)]
struct JoinResponse {
    result: String,
    params: RoomParameters,
}

#[derive(Debug, Serialize)]
struct V2JoinResponse {
    result: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<V2JoinParameters>,
}

#[derive(Debug, Serialize)]
struct V2JoinParameters {
    client_id: String,
    room_id: String,
    room_link: String,
    mode: &'static str,
    epoch: String,
    wss_url: String,
    admission_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_initiator: Option<bool>,
}

impl RoomServer {
    pub fn new<A: RoomAuthority + 'static>(
        config: Config,
        authority: A,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let templates = Templates::load(&config.web_root)?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                templates,
                authority: Arc::new(authority),
                started: Instant::now(),
                http_errors: AtomicU64::new(0),
            }),
        })
    }

    pub fn router(self) -> Router {
        let web_root = self.inner.config.web_root.clone();
        Router::new()
            .route("/", get(main_page))
            .route("/r/{roomid}", get(room_page))
            .route("/v2/r/{roomid}", get(v2_room_page))
            .route("/join/{roomid}", post(join))
            .route("/v2/join/{roomid}", post(v2_join))
            .route("/leave/{roomid}/{clientid}", post(leave))
            .route("/v2/leave/{roomid}/{clientid}", post(v2_leave))
            .route("/message/{roomid}/{clientid}", post(message))
            .route("/params", get(params))
            .route("/v2/params", get(v2_params))
            .route("/v1alpha/iceconfig", get(ice_config).post(ice_config))
            .route("/status", get(status))
            // V1 call.js posts to wss_post_url + /{roomid}/{clientid}.
            .route(
                "/{roomid}/{clientid}",
                post(bridge_post).delete(bridge_delete),
            )
            // Keep the unified Go Collider's newer internal alias as well.
            .route(
                "/_internal/{roomid}/{clientid}",
                post(bridge_post).delete(bridge_delete),
            )
            .nest_service("/js", ServeDir::new(format!("{web_root}/js")))
            .nest_service("/css", ServeDir::new(format!("{web_root}/css")))
            .nest_service("/images", ServeDir::new(format!("{web_root}/images")))
            .nest_service("/html", ServeDir::new(format!("{web_root}/html")))
            .fallback_service(ServeDir::new(web_root))
            .with_state(self)
    }

    fn request_context(&self, headers: &HeaderMap, uri: &Uri) -> Result<(String, Url), String> {
        let host = headers
            .get(HOST)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("localhost")
            .to_string();
        let url = Url::parse(&format!("http://{host}{uri}"))
            .map_err(|error| format!("Invalid request URL: {error}"))?;
        Ok((host, url))
    }

    fn http_error(&self, message: impl Into<String>) -> Response {
        self.inner.http_errors.fetch_add(1, Ordering::Relaxed);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{}\n", message.into()),
        )
            .into_response()
    }
}

async fn main_page(State(server): State<RoomServer>, headers: HeaderMap, uri: Uri) -> Response {
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    let params = server
        .inner
        .config
        .build_room_parameters(host, &url, "", "", None);
    match server.inner.templates.render_index(&params) {
        Ok(body) => Html(body).into_response(),
        Err(error) => server.http_error(format!("Failed to render index: {error}")),
    }
}

async fn room_page(
    State(server): State<RoomServer>,
    Path(roomid): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    let occupancy = match server.inner.authority.occupancy(roomid.clone()).await {
        Ok(occupancy) => occupancy,
        Err(error) => return server.http_error(error),
    };
    let params = server
        .inner
        .config
        .build_room_parameters(host, &url, &roomid, "", None);
    let rendered = if occupancy >= MAX_ROOM_CAPACITY {
        server.inner.templates.render_full(&params)
    } else {
        server.inner.templates.render_index(&params)
    };
    match rendered {
        Ok(body) => Html(body).into_response(),
        Err(error) => server.http_error(format!("Failed to render page: {error}")),
    }
}

async fn v2_room_page(
    State(server): State<RoomServer>,
    Path(roomid): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if canonical_u64(&roomid).is_none() {
        return axum::Json(json!({ "result": "INVALID_ROOM_ID" })).into_response();
    }
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    // V2 rooms are NOT capped at two. The third join upgrades P2P -> SFU (design
    // sec 4.2), so the room page always serves the app rather than the "full"
    // template. The V1 two-person cap must not leak into V2, or the third browser
    // is shown "this room is full" and never POSTs /v2/join, so the upgrade never
    // starts. Real capacity limits (no SFU worker / SFU exhausted) are decided at
    // /v2/join and surfaced to the client as a join error, never as a full page.
    let params = server
        .inner
        .config
        .build_v2_room_parameters(host, &url, &roomid);
    match server.inner.templates.render_index(&params) {
        Ok(body) => Html(body).into_response(),
        Err(error) => server.http_error(format!("Failed to render V2 page: {error}")),
    }
}

async fn params(State(server): State<RoomServer>, headers: HeaderMap, uri: Uri) -> Response {
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    axum::Json(
        server
            .inner
            .config
            .build_room_parameters(host, &url, "", "", None),
    )
    .into_response()
}

async fn v2_params(State(server): State<RoomServer>, headers: HeaderMap, uri: Uri) -> Response {
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    axum::Json(server.inner.config.build_v2_room_parameters(host, &url, "")).into_response()
}

async fn ice_config(State(server): State<RoomServer>) -> impl IntoResponse {
    axum::Json(server.inner.config.ice_config())
}

async fn join(
    State(server): State<RoomServer>,
    Path(roomid): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    let clientid = generate_random(8);
    let is_loopback = url
        .query_pairs()
        .any(|(key, value)| key == "debug" && value == "loopback");
    log::info!("HTTP join: room_id={roomid} client_id={clientid} loopback={is_loopback}");
    match server
        .inner
        .authority
        .admit(roomid.clone(), clientid.clone(), is_loopback)
        .await
    {
        Ok(admission) => {
            let mut params = server.inner.config.build_room_parameters(
                host,
                &url,
                &roomid,
                &clientid,
                Some(admission.is_initiator),
            );
            params.messages = admission.messages;
            axum::Json(JoinResponse {
                result: "SUCCESS".to_string(),
                params,
            })
            .into_response()
        }
        Err(result) => axum::Json(JoinResponse {
            result,
            ..Default::default()
        })
        .into_response(),
    }
}

async fn v2_join(
    State(server): State<RoomServer>,
    Path(roomid): Path<String>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    const CLIENT_ID_ATTEMPTS: usize = 8;

    let room_id = match canonical_u64(&roomid) {
        Some(room_id) => room_id,
        None => {
            return axum::Json(V2JoinResponse {
                result: "INVALID_ROOM_ID".into(),
                params: None,
            })
            .into_response();
        }
    };
    let (host, url) = match server.request_context(&headers, &uri) {
        Ok(context) => context,
        Err(error) => return server.http_error(error),
    };
    for attempt in 1..=CLIENT_ID_ATTEMPTS {
        let client_id = rand::random::<u64>();
        log::info!("HTTP V2 join: room_id={room_id} client_id={client_id} attempt={attempt}");
        match server.inner.authority.admit_v2(room_id, client_id).await {
            Ok(admission) => {
                let mode = match admission.mode {
                    signaling_proto::v2::RoomMode::P2p => "p2p",
                    signaling_proto::v2::RoomMode::Sfu => "sfu",
                    _ => unreachable!("gRPC authority filters transition modes"),
                };
                log::info!(
                    "HTTP V2 join response: room_id={room_id} client_id={client_id} result=SUCCESS mode={mode} epoch={}",
                    admission.signal_epoch
                );
                let params = server
                    .inner
                    .config
                    .build_v2_room_parameters(host, &url, &roomid);
                return axum::Json(V2JoinResponse {
                    result: "SUCCESS".into(),
                    params: Some(V2JoinParameters {
                        client_id: client_id.to_string(),
                        room_id: roomid,
                        room_link: params.room_link,
                        mode,
                        epoch: admission.signal_epoch.to_string(),
                        wss_url: params.wss_url,
                        admission_token: admission.admission_token,
                        is_initiator: admission.is_initiator,
                    }),
                })
                .into_response();
            }
            Err(error) if error == "DUPLICATE_CLIENT" && attempt < CLIENT_ID_ATTEMPTS => continue,
            Err(result) => {
                return axum::Json(V2JoinResponse {
                    result,
                    params: None,
                })
                .into_response();
            }
        }
    }
    axum::Json(V2JoinResponse {
        result: "RESOURCE_EXHAUSTED".into(),
        params: None,
    })
    .into_response()
}

async fn leave(
    State(server): State<RoomServer>,
    Path((roomid, clientid)): Path<(String, String)>,
) -> Response {
    log::info!("HTTP leave: room_id={roomid} client_id={clientid}");
    match server.inner.authority.remove(roomid, clientid).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => server.http_error(error),
    }
}

async fn v2_leave(
    State(server): State<RoomServer>,
    Path((roomid, clientid)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let Some(room_id) = canonical_u64(&roomid) else {
        return axum::Json(json!({ "result": "INVALID_ROOM_ID" })).into_response();
    };
    let Some(client_id) = canonical_u64(&clientid) else {
        return axum::Json(json!({ "result": "INVALID_CLIENT_ID" })).into_response();
    };
    let Some(admission_token) = bearer_token(&headers) else {
        return axum::Json(json!({ "result": "UNAUTHORIZED" })).into_response();
    };
    log::info!("HTTP V2 leave: room_id={room_id} client_id={client_id}");
    match server
        .inner
        .authority
        .remove_v2(room_id, client_id, admission_token)
        .await
    {
        Ok(()) => axum::Json(json!({ "result": "SUCCESS" })).into_response(),
        Err(result) => axum::Json(json!({ "result": result })).into_response(),
    }
}

fn canonical_u64(value: &str) -> Option<u64> {
    if value == "0" {
        return Some(0);
    }
    if value.is_empty()
        || value.starts_with('0')
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

async fn message(
    State(server): State<RoomServer>,
    Path((roomid, clientid)): Path<(String, String)>,
    body: String,
) -> Response {
    log::info!(
        "HTTP message: room_id={roomid} client_id={clientid} bytes={}",
        body.len()
    );
    match server.inner.authority.inject(roomid, clientid, body).await {
        Ok(()) => axum::Json(json!({ "result": "SUCCESS" })).into_response(),
        Err(result) => axum::Json(json!({ "result": result })).into_response(),
    }
}

async fn bridge_post(
    State(server): State<RoomServer>,
    Path((roomid, clientid)): Path<(String, String)>,
    body: String,
) -> Response {
    if body.is_empty() {
        return cors(server.http_error("Empty request body"), "POST, DELETE");
    }
    match server.inner.authority.inject(roomid, clientid, body).await {
        Ok(()) => cors_text("OK\n"),
        Err(error) => cors(
            server.http_error(format!("Failed to send the message: {error}")),
            "POST, DELETE",
        ),
    }
}

async fn bridge_delete(
    State(server): State<RoomServer>,
    Path((roomid, clientid)): Path<(String, String)>,
) -> Response {
    match server.inner.authority.remove(roomid, clientid).await {
        Ok(()) => cors_text("OK\n"),
        Err(error) => cors(server.http_error(error), "POST, DELETE"),
    }
}

async fn status(State(server): State<RoomServer>) -> Response {
    match server.inner.authority.status().await {
        Ok(snapshot) => {
            let report = StatusReport {
                up_time_sec: server.inner.started.elapsed().as_secs_f64(),
                open_ws: snapshot.websocket_connections as u64,
                total_ws: snapshot.total_websocket_connections,
                ws_errs: snapshot.websocket_errors,
                http_errs: server.inner.http_errors.load(Ordering::Relaxed),
            };
            let mut response = axum::Json(report).into_response();
            response
                .headers_mut()
                .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            response.headers_mut().insert(
                ACCESS_CONTROL_ALLOW_METHODS,
                HeaderValue::from_static("GET"),
            );
            response
        }
        Err(error) => server.http_error(error),
    }
}

fn cors_text(body: &'static str) -> Response {
    let mut response = Response::new(Body::from(body));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    cors(response, "POST, DELETE")
}

fn cors(mut response: Response, methods: &'static str) -> Response {
    response
        .headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    response.headers_mut().insert(
        ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static(methods),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc_client::StatusSnapshot;
    use crate::grpc_client::{Admission, RoomAuthority, V2Admission};
    use async_trait::async_trait;
    use axum::body::to_bytes;
    use axum::http::{Method, Request};
    use serde_json::Value;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    type MockClient = (String, Vec<String>);
    type MockRooms = HashMap<String, Vec<MockClient>>;

    #[derive(Clone, Default)]
    struct MockAuthority(Arc<Mutex<MockRooms>>);

    #[async_trait]
    impl RoomAuthority for MockAuthority {
        async fn admit(
            &self,
            roomid: String,
            clientid: String,
            _: bool,
        ) -> Result<Admission, String> {
            let mut rooms = self.0.lock().unwrap();
            let clients = rooms.entry(roomid).or_default();
            if clients.iter().any(|(id, _)| id == &clientid) {
                return Err("DUPLICATE_CLIENT".into());
            }
            if clients.len() >= 2 {
                return Err("FULL".into());
            }
            let initiator = clients.is_empty();
            let messages = clients.first().map(|(_, q)| q.clone()).unwrap_or_default();
            clients.push((clientid, Vec::new()));
            Ok(Admission {
                is_initiator: initiator,
                messages,
            })
        }
        async fn remove(&self, roomid: String, clientid: String) -> Result<(), String> {
            if let Some(clients) = self.0.lock().unwrap().get_mut(&roomid) {
                clients.retain(|(id, _)| id != &clientid);
            }
            Ok(())
        }
        async fn occupancy(&self, roomid: String) -> Result<usize, String> {
            Ok(self.0.lock().unwrap().get(&roomid).map_or(0, Vec::len))
        }
        async fn inject(
            &self,
            roomid: String,
            clientid: String,
            msg: String,
        ) -> Result<(), String> {
            let mut rooms = self.0.lock().unwrap();
            let clients = rooms.entry(roomid).or_default();
            if let Some((_, queue)) = clients.iter_mut().find(|(id, _)| id == &clientid) {
                queue.push(msg);
            } else {
                clients.push((clientid, vec![msg]));
            }
            Ok(())
        }
        async fn admit_v2(&self, room_id: u64, client_id: u64) -> Result<V2Admission, String> {
            let mut rooms = self.0.lock().unwrap();
            let clients = rooms.entry(format!("v2:{room_id}")).or_default();
            let client_id = client_id.to_string();
            if clients.iter().any(|(id, _)| id == &client_id) {
                return Err("DUPLICATE_CLIENT".into());
            }
            if clients.len() >= 2 {
                return Err("NO_SFU_AVAILABLE".into());
            }
            let is_initiator = clients.is_empty();
            clients.push((client_id.clone(), Vec::new()));
            Ok(V2Admission {
                mode: signaling_proto::v2::RoomMode::P2p,
                signal_epoch: 0,
                admission_token: format!("token-{room_id}-{client_id}"),
                is_initiator: Some(is_initiator),
            })
        }
        async fn remove_v2(&self, room_id: u64, client_id: u64, _: String) -> Result<(), String> {
            if let Some(clients) = self.0.lock().unwrap().get_mut(&format!("v2:{room_id}")) {
                clients.retain(|(id, _)| id != &client_id.to_string());
            }
            Ok(())
        }
        async fn occupancy_v2(&self, room_id: u64) -> Result<usize, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(&format!("v2:{room_id}"))
                .map_or(0, Vec::len))
        }
        async fn status(&self) -> Result<StatusSnapshot, String> {
            let rooms = self.0.lock().unwrap();
            Ok(StatusSnapshot {
                rooms: rooms.len(),
                clients: rooms.values().map(Vec::len).sum(),
                websocket_connections: 0,
                total_websocket_connections: 0,
                websocket_errors: 0,
            })
        }
    }
    use tower::ServiceExt;

    fn app() -> Router {
        let config = Config {
            web_root: env!("CARGO_MANIFEST_DIR").to_string(),
            ..Default::default()
        };
        RoomServer::new(config, MockAuthority::default())
            .unwrap()
            .router()
    }

    async fn request(app: &Router, method: Method, uri: &str, body: &str) -> Response {
        app.clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header(HOST, "example.test")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    async fn json_body(response: Response) -> Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn v1_join_message_replay_capacity_and_leave_flow() {
        let app = app();
        let first = json_body(request(&app, Method::POST, "/join/room", "").await).await;
        assert_eq!(first["result"], "SUCCESS");
        assert_eq!(first["params"]["is_initiator"], "true");
        let first_id = first["params"]["client_id"].as_str().unwrap();
        assert_eq!(first_id.len(), 8);
        assert!(first_id.bytes().all(|byte| byte.is_ascii_digit()));

        let message_uri = format!("/message/room/{first_id}");
        let sent = json_body(request(&app, Method::POST, &message_uri, "offer").await).await;
        assert_eq!(sent["result"], "SUCCESS");

        let second = json_body(request(&app, Method::POST, "/join/room", "").await).await;
        assert_eq!(second["result"], "SUCCESS");
        assert_eq!(second["params"]["is_initiator"], "false");
        assert_eq!(second["params"]["messages"], json!(["offer"]));

        let full = json_body(request(&app, Method::POST, "/join/room", "").await).await;
        assert_eq!(full["result"], "FULL");

        let leave_uri = format!("/leave/room/{first_id}");
        let left = request(&app, Method::POST, &leave_uri, "").await;
        assert_eq!(left.status(), StatusCode::OK);
        assert!(
            to_bytes(left.into_body(), usize::MAX)
                .await
                .unwrap()
                .is_empty()
        );

        let replacement = json_body(request(&app, Method::POST, "/join/room", "").await).await;
        assert_eq!(replacement["result"], "SUCCESS");
    }

    #[tokio::test]
    async fn v2_join_uses_numeric_ids_token_epoch_and_authenticated_leave() {
        let app = app();
        let first = json_body(request(&app, Method::POST, "/v2/join/42", "").await).await;
        assert_eq!(first["result"], "SUCCESS");
        assert_eq!(first["params"]["room_id"], "42");
        assert_eq!(first["params"]["room_link"], "http://example.test/v2/r/42");
        assert_eq!(first["params"]["mode"], "p2p");
        assert_eq!(first["params"]["epoch"], "0");
        assert_eq!(first["params"]["is_initiator"], true);
        assert!(
            first["params"]["admission_token"]
                .as_str()
                .is_some_and(|token| !token.is_empty())
        );
        let client_id = first["params"]["client_id"].as_str().unwrap();
        assert!(canonical_u64(client_id).is_some());

        let unauthorized =
            request(&app, Method::POST, &format!("/v2/leave/42/{client_id}"), "").await;
        assert_eq!(json_body(unauthorized).await["result"], "UNAUTHORIZED");

        let leave = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(format!("/v2/leave/42/{client_id}"))
                    .header(HOST, "example.test")
                    .header(
                        AUTHORIZATION,
                        format!(
                            "Bearer {}",
                            first["params"]["admission_token"].as_str().unwrap()
                        ),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_body(leave).await["result"], "SUCCESS");
    }

    #[tokio::test]
    async fn v2_routes_reject_noncanonical_ids_and_expose_v2_page_configuration() {
        let app = app();
        for room_id in ["", "01", "+1", "18446744073709551616"] {
            if room_id.is_empty() {
                continue;
            }
            let response =
                json_body(request(&app, Method::POST, &format!("/v2/join/{room_id}"), "").await)
                    .await;
            assert_eq!(response["result"], "INVALID_ROOM_ID", "room={room_id}");
        }

        let page = request(&app, Method::GET, "/v2/r/42", "").await;
        assert_eq!(page.status(), StatusCode::OK);
        let body = to_bytes(page.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(body.contains("signalingVersion: 2"));
        assert!(body.contains("id=\"signaling-v2-checkbox\""));
        assert!(body.contains("id=\"signaling-v2-checkbox\" checked"));

        let params = json_body(request(&app, Method::GET, "/v2/params", "").await).await;
        assert!(params.get("wss_post_url").is_none());
    }

    #[tokio::test]
    async fn v2_third_join_reports_no_sfu_available_without_affecting_v1() {
        let app = app();
        for expected_initiator in [true, false] {
            let joined = json_body(request(&app, Method::POST, "/v2/join/99", "").await).await;
            assert_eq!(joined["result"], "SUCCESS");
            assert_eq!(joined["params"]["is_initiator"], expected_initiator);
        }
        let third = json_body(request(&app, Method::POST, "/v2/join/99", "").await).await;
        assert_eq!(third["result"], "NO_SFU_AVAILABLE");
        assert!(third.get("params").is_none());

        let v1 = json_body(request(&app, Method::POST, "/join/99", "").await).await;
        assert_eq!(v1["result"], "SUCCESS");
        assert_eq!(v1["params"]["is_initiator"], "true");
    }

    #[tokio::test]
    async fn v1_pages_static_assets_bridge_and_status_are_served() {
        let app = app();
        let root = request(&app, Method::GET, "/", "").await;
        assert_eq!(root.status(), StatusCode::OK);
        assert!(
            root.headers()[CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        let root_body = to_bytes(root.into_body(), usize::MAX).await.unwrap();
        let root_body = String::from_utf8_lossy(&root_body);
        assert!(root_body.contains("signalingVersion: 1"));
        assert!(root_body.contains("id=\"signaling-v2-checkbox\""));
        assert!(root_body.contains("id=\"signaling-v2-checkbox\" checked"));

        let script = request(&app, Method::GET, "/js/call.js", "").await;
        assert_eq!(script.status(), StatusCode::OK);

        let bridge = request(&app, Method::POST, "/room/client", "candidate").await;
        assert_eq!(bridge.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(bridge.into_body(), usize::MAX).await.unwrap(),
            "OK\n"
        );

        let internal = request(&app, Method::DELETE, "/_internal/room/client", "").await;
        assert_eq!(internal.status(), StatusCode::OK);

        let status = request(&app, Method::GET, "/status", "").await;
        assert_eq!(status.status(), StatusCode::OK);
        assert_eq!(status.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        let status = json_body(status).await;
        assert!(status.get("upsec").is_some());
        assert!(status.get("openws").is_some());
        assert!(status.get("totalws").is_some());
        assert!(status.get("wserrors").is_some());
        assert!(status.get("httperrors").is_some());
    }

    #[tokio::test]
    async fn bridge_validates_empty_body_and_sets_cors_headers() {
        let app = app();
        let empty = request(&app, Method::POST, "/room/client", "").await;
        assert_eq!(empty.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(empty.headers()[ACCESS_CONTROL_ALLOW_ORIGIN], "*");
        assert_eq!(
            empty.headers()[ACCESS_CONTROL_ALLOW_METHODS],
            "POST, DELETE"
        );

        let invalid = request(&app, Method::GET, "/room/client", "").await;
        assert_eq!(invalid.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn params_honor_query_options_and_request_host() {
        let config = Config {
            web_root: env!("CARGO_MANIFEST_DIR").to_string(),
            force_tls: true,
            ..Default::default()
        };
        let app = RoomServer::new(config, MockAuthority::default())
            .unwrap()
            .router();
        let response = request(
            &app,
            Method::GET,
            "/params?debug=loopback&it=relay&tt=relay",
            "",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let params = json_body(response).await;
        assert_eq!(params["wss_url"], "wss://example.test/ws");
        assert_eq!(params["is_loopback"], "true");
        assert_eq!(params["ice_server_transports"], "relay");
    }

    #[tokio::test]
    async fn room_page_switches_to_full_template_at_capacity() {
        let app = app();
        let first = json_body(request(&app, Method::POST, "/join/full-room", "").await).await;
        let second = json_body(request(&app, Method::POST, "/join/full-room", "").await).await;
        let page = request(&app, Method::GET, "/r/full-room", "").await;
        assert_eq!(page.status(), StatusCode::OK);
        let body = to_bytes(page.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("this room is full"));
        let _ = request(
            &app,
            Method::POST,
            &format!(
                "/leave/full-room/{}",
                first["params"]["client_id"].as_str().unwrap()
            ),
            "",
        )
        .await;
        let _ = request(
            &app,
            Method::POST,
            &format!(
                "/leave/full-room/{}",
                second["params"]["client_id"].as_str().unwrap()
            ),
            "",
        )
        .await;
    }

    #[tokio::test]
    async fn v2_room_page_never_shows_full_so_the_third_join_can_upgrade() {
        // The V1 two-person cap must NOT gate a V2 room. At two occupants the page
        // still serves the app so the third browser can POST /v2/join and trigger the
        // P2P -> SFU upgrade (design sec 4.2), instead of being shown "this room is
        // full" and never joining.
        let app = app();
        for _ in 0..2 {
            let joined = json_body(request(&app, Method::POST, "/v2/join/424242", "").await).await;
            assert_eq!(joined["result"], "SUCCESS");
        }
        let page = request(&app, Method::GET, "/v2/r/424242", "").await;
        assert_eq!(page.status(), StatusCode::OK);
        let body = to_bytes(page.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8_lossy(&body);
        assert!(
            !body.contains("this room is full"),
            "V2 room page must not gate at the V1 two-person capacity"
        );
        assert!(body.contains("signalingVersion: 2"));
    }
}
