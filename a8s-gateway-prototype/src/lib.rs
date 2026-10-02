pub mod api;
pub mod model;
pub mod provider;
pub mod store;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub &'static str);
pub type Result<T> = std::result::Result<T, Error>;

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for Error {}
impl From<rusqlite::Error> for Error {
    fn from(_: rusqlite::Error) -> Self {
        Self("durable_storage_unavailable")
    }
}
impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Self("provider_unavailable")
    }
}
impl From<serde_json::Error> for Error {
    fn from(_: serde_json::Error) -> Self {
        Self("invalid_input")
    }
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self.0 {
            "unauthenticated" => StatusCode::UNAUTHORIZED,
            "forbidden" => StatusCode::FORBIDDEN,
            "not_found" => StatusCode::NOT_FOUND,
            "idempotency_conflict" | "stale_lease" | "message_conflict" => StatusCode::CONFLICT,
            "quota" => StatusCode::TOO_MANY_REQUESTS,
            "recovery_hold" | "durable_storage_unavailable" | "provider_unavailable" => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            "body_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::BAD_REQUEST,
        };
        (status, Json(json!({"error":{"code":self.0,"message":self.0,"request_id":ulid::Ulid::new().to_string(),"retryable":self.0=="provider_unavailable"}}))).into_response()
    }
}
