use crate::{admission, Error};
use axum::{
    http::StatusCode,
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
