use axum::{Json, http::StatusCode, response::{IntoResponse, Response}};
use summdb_core::error::SummError;

pub enum AppError {
    NotFound,
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
            AppError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };
        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

impl From<SummError> for AppError {
    fn from(e: SummError) -> Self {
        match e {
            SummError::NotFound => AppError::NotFound,
            other => AppError::Internal(other.to_string()),
        }
    }
}
