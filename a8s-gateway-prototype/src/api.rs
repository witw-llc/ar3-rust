use crate::{
    Error, Result,
    model::*,
    provider::{LocalObjects, LocalStorage, S3Remote},
    store::Store,
};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

pub struct Service {
    pub store: Store,
    pub remote: S3Remote<LocalObjects>,
    pub storage: LocalStorage,
}
impl Service {
    pub fn open(state: &Path, objects: &Path) -> Result<Self> {
        Ok(Self {
            store: Store::open(state)?,
            remote: S3Remote(LocalObjects::new(objects)?),
            storage: LocalStorage::new(objects)?,
        })
    }
}
#[derive(Clone)]
pub struct App {
    service: Arc<Mutex<Service>>,
    requests: Arc<Semaphore>,
}
impl App {
    pub fn new(service: Service) -> Self {
        Self {
            service: Arc::new(Mutex::new(service)),
            requests: Arc::new(Semaphore::new(32)),
        }
    }
}
pub fn router(app: App) -> Router {
    Router::new().fallback(any(dispatch)).with_state(app)
}
pub fn validate_bind(addr: std::net::SocketAddr) -> Result<()> {
    if addr.ip().is_loopback() {
        Ok(())
    } else {
        Err(Error("forbidden"))
    }
}
fn authenticate(value: Option<&str>) -> Result<Principal> {
    match value {
        Some("Bearer analyst-test") => Ok(Principal::synthetic("analyst")),
        Some("Bearer builder-test") => Ok(Principal::synthetic("builder")),
        Some("Bearer builder-reader-test") => {
            let mut p = Principal::synthetic("builder");
            p.id = "builder-reader".into();
            p.destinations.clear();
            p.can_claim = false;
            p.can_upload = false;
            Ok(p)
        }
        _ => Err(Error("unauthenticated")),
    }
}
async fn dispatch(State(app): State<App>, request: Request) -> Response {
    let response = execute(app, request)
        .await
        .unwrap_or_else(IntoResponse::into_response);
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    parts.headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        "nosniff".parse().expect("static header"),
    );
    Response::from_parts(parts, body)
}
async fn execute(app: App, request: Request) -> Result<Response> {
    let permit = app
        .requests
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error("quota"))?;
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    if path == "/health/live" && method == Method::GET {
        return Ok(Json(json!({"live":true})).into_response());
    }
    let p = authenticate(
        request
            .headers()
            .get(header::AUTHORIZATION)
            .and_then(|x| x.to_str().ok()),
    )?;
    if request.uri().query().is_some() {
        return Err(Error("invalid_input"));
    }
    let key = request
        .headers()
        .get("idempotency-key")
        .and_then(|x| x.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if method == Method::POST && (key.is_empty() || key.len() > 128) {
        return Err(Error("invalid_input"));
    }
    let name = request
        .headers()
        .get("x-attachment-filename")
        .and_then(|x| x.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let max = if path == "/v1/attachments" {
        MAX_ATTACHMENT
    } else {
        MAX_JSON
    };
    let body = tokio::time::timeout(Duration::from_secs(10), to_bytes(request.into_body(), max))
        .await
        .map_err(|_| Error("invalid_input"))?
        .map_err(|_| Error("body_too_large"))?;
    tokio::task::spawn_blocking(move || {

        let mut service=app.service.lock().map_err(|_|Error("durable_storage_unavailable"))?;
        let Service{store,remote,storage}= &mut *service;
        let segments:Vec<_>=path.trim_matches('/').split('/').collect();
        let response=match (method.as_str(),segments.as_slice()) {
            ("GET",["health","ready"]) => Json(store.status()?).into_response(),
            ("GET",["v1","identity"]) => Json(json!({"tenant":p.tenant,"principal":p.id,"seat":p.seat,"consumer":p.consumer,"auth":"synthetic_loopback_only"})).into_response(),
            ("GET",["v1","capabilities"]) => Json(json!({"prototype":true,"auth":"synthetic_loopback_only","https":false,"remote":"local_s3_double","live_s3":false,"mqtt":"contract_double_only","mcp":false,"idle_agent_wake":false,"json_bytes":MAX_JSON,"attachment_bytes":MAX_ATTACHMENT,"http_requests":32,"outbound_auto_retry":false})).into_response(),
            ("POST",["v1","messages"]) => (StatusCode::ACCEPTED,Json(store.accept(&p,&key,serde_json::from_slice(&body)?)?)).into_response(),
            ("GET",["v1","operations",op]) => Json(store.operation_status(&p,op)?).into_response(),
            ("POST",["v1","operations",op,"publish"]) => { empty(&body)?; Json(store.publish_operation(&p,op,remote,storage)?).into_response() },
            ("GET",["v1","inboxes",mailbox,"messages"]) => Json(json!({"messages":store.inbox(&p,mailbox)?})).into_response(),
            ("POST",["v1","inboxes",mailbox,"claims"]) => Json(store.claim(&p,mailbox,&key,serde_json::from_slice(&body)?,now())?).into_response(),
            ("POST",["v1","inboxes",mailbox,"poll"]) => {
                p.authorize_inbox(mailbox,Some(&p.consumer))?; empty(&body)?;
                Json(store.poll_with_key(&p,&key,remote,storage,&[Principal::synthetic("analyst"),Principal::synthetic("builder")])?).into_response()
            },
            ("POST",["v1","deliveries",delivery,"disposition"]) => Json(store.disposition(&p,delivery,&key,serde_json::from_slice(&body)?,now())?).into_response(),
            ("POST",["v1","attachments"]) => (StatusCode::CREATED,Json(store.upload(&p,&key,&name,&body)?)).into_response(),
            ("GET",["v1","attachments",aid,"content"]) => {
                let (_,bytes)=store.attachment(&p,aid)?;
                ([(header::CONTENT_TYPE,"application/octet-stream")],Body::from(bytes)).into_response()
            },
            ("GET",["v1","messages",mid,"files",name]) => {
                let name=percent_encoding::percent_decode_str(name).decode_utf8().map_err(|_|Error("invalid_input"))?;
                filename(&name)?;
                ([(header::CONTENT_TYPE,"application/octet-stream")],Body::from(store.inbound_attachment(&p,mid,&name)?)).into_response()
            },
            _ => return Err(Error("not_found")),
        };
        let (parts,body)=response.into_parts();
        Ok(Response::from_parts(parts,Body::new(ResponsePermit{inner:body,permit:Some(permit)})))
    }).await.map_err(|_|Error("durable_storage_unavailable"))?
}
fn empty(body: &[u8]) -> Result<()> {
    if body.is_empty() || serde_json::from_slice::<Value>(body)? == json!({}) {
        Ok(())
    } else {
        Err(Error("invalid_input"))
    }
}

struct ResponsePermit {
    inner: Body,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}
impl http_body::Body for ResponsePermit {
    type Data = axum::body::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<std::result::Result<http_body::Frame<Self::Data>, Self::Error>>>
    {
        let result = std::pin::Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(result, std::task::Poll::Ready(None)) {
            self.permit.take();
        }
        result
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}
