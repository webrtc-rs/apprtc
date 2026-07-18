// Copyright (c) 2014 The WebRTC project authors. All Rights Reserved.
// Use of this source code is governed by a BSD-style license
// that can be found in the LICENSE file in the root of the source
// tree.

//! AppRTC V1 room API, HTML pages, and static assets.

use crate::config::Config;
use crate::dashboard::StatusReport;
use crate::params::{RoomParameters, generate_random};
use crate::templates::Templates;
use crate::wsclient::WsClient;
use axum::Router;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE, HOST,
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
    authority: WsClient,
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

impl RoomServer {
    pub fn new(config: Config, authority: WsClient) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let templates = Templates::load(&config.web_root)?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                templates,
                authority,
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
            .route("/join/{roomid}", post(join))
            .route("/leave/{roomid}/{clientid}", post(leave))
            .route("/message/{roomid}/{clientid}", post(message))
            .route("/params", get(params))
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
    use axum::body::to_bytes;
    use axum::http::{Method, Request};
    use serde_json::Value;
    use signaling::wsserver::ColliderHandle;
    use std::time::Duration;
    use tower::ServiceExt;

    fn app() -> Router {
        let collider = ColliderHandle::spawn(Duration::from_secs(10));
        let config = Config {
            web_root: env!("CARGO_MANIFEST_DIR").to_string(),
            ..Default::default()
        };
        RoomServer::new(config, WsClient::new(collider))
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
        let collider = ColliderHandle::spawn(Duration::from_secs(10));
        let config = Config {
            web_root: env!("CARGO_MANIFEST_DIR").to_string(),
            force_tls: true,
            ..Default::default()
        };
        let app = RoomServer::new(config, WsClient::new(collider))
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
}
