use axum::{
	Json,
	http::{HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::{EngineError, ErrorCode};

#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
	pub code:    String,
	pub message: String,
}

#[derive(Clone, Debug)]
pub struct ApiError {
	status:       StatusCode,
	body:         ErrorBody,
	authenticate: bool,
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

impl ApiError {
	pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			status,
			body: ErrorBody { code: code.into(), message: message.into() },
			authenticate: false,
		}
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_REQUEST, "invalid", message)
	}

	pub fn code(&self) -> &str {
		&self.body.code
	}

	pub fn message(&self) -> &str {
		&self.body.message
	}

	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self { authenticate: true, ..Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message) }
	}

	pub fn forbidden(message: impl Into<String>) -> Self {
		Self::new(StatusCode::FORBIDDEN, "unauthorized", message)
	}

	pub fn bad_gateway(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_GATEWAY, code, message)
	}
}

impl From<EngineError> for ApiError {
	fn from(value: EngineError) -> Self {
		let status = match value.code {
			ErrorCode::NotFound => StatusCode::NOT_FOUND,
			ErrorCode::NotRunning | ErrorCode::Busy => StatusCode::CONFLICT,
			ErrorCode::Invalid => StatusCode::BAD_REQUEST,
			ErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
			ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
			ErrorCode::Engine => StatusCode::SERVICE_UNAVAILABLE,
		};
		let mut err = Self::new(status, value.code.as_str(), value.message);
		if matches!(value.code, ErrorCode::Unauthorized) {
			err.authenticate = true;
		}
		err
	}
}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		let mut response = (self.status, Json(self.body)).into_response();
		if self.authenticate {
			response
				.headers_mut()
				.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
		}
		response
	}
}

pub fn join_error(err: tokio::task::JoinError) -> ApiError {
	ApiError::new(
		StatusCode::SERVICE_UNAVAILABLE,
		"engine_error",
		format!("engine task failed: {err}"),
	)
}
