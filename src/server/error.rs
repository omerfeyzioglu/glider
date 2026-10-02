use crate::{admission, Error};
use axum::{
    body::{to_bytes, Body},
    extract::Request,
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// JSON error with a status that tells clients whether to retry.
pub(super) struct ApiError(pub(super) StatusCode, pub(super) String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<admission::Error> for ApiError {
    fn from(error: admission::Error) -> Self {
        let status = match &error {
            admission::Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,
            admission::Error::Database(Error::Invalid(_)) => StatusCode::BAD_REQUEST,
            admission::Error::Database(Error::RequestConflict | Error::RequestExpired) => {
                StatusCode::CONFLICT
            }
            admission::Error::Database(Error::Corrupt(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        ApiError(status, error.to_string())
    }
}

pub(super) fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

/// Bodies of framework rejections are short diagnostics; keep at most this much.
const MAX_REJECTION_BYTES: usize = 8 * 1024;

/// Make every error response `{"error": "..."}`. Handlers already answer in
/// JSON; this covers axum's own rejections (malformed JSON, wrong content
/// type, oversized body, bad path parameter, unknown route, wrong method),
/// which are plain text or empty. Status codes and other headers are kept.
pub(super) async fn json_errors(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    let status = response.status();
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    if !(status.is_client_error() || status.is_server_error()) || is_json {
        return response;
    }
    let (mut parts, body) = response.into_parts();
    let text = match to_bytes(body, MAX_REJECTION_BYTES).await {
        Ok(bytes) => String::from_utf8_lossy(&bytes).trim().to_owned(),
        Err(_) => String::new(),
    };
    let message = if text.is_empty() {
        status
            .canonical_reason()
            .unwrap_or("error")
            .to_ascii_lowercase()
    } else {
        text
    };
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, Body::from(json!({ "error": message }).to_string()))
}
