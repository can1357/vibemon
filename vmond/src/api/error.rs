use axum::{
	Json,
	http::{HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::{EngineError, ErrorCode};

#[derive(Clone, Debug, Serialize)]
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

	/// Construct an API error from a stable function-runtime error code.
	///
	/// The HTTP status mirrors the gRPC mapping in `api::grpc::status_from`;
	/// callers retain the stable code in either response representation.
	pub fn function(code: impl Into<String>, message: impl Into<String>) -> Self {
		let code = code.into();
		let status = match code.as_str() {
			"not_found" => StatusCode::NOT_FOUND,
			"invalid" | "checksum" => StatusCode::BAD_REQUEST,
			"unauthorized" => StatusCode::UNAUTHORIZED,
			"actor_lost" | "unavailable_secret" => StatusCode::PRECONDITION_FAILED,
			"busy" | "conflict" => StatusCode::CONFLICT,
			"deadline" => StatusCode::GATEWAY_TIMEOUT,
			"unsupported" => StatusCode::NOT_IMPLEMENTED,
			_ => StatusCode::SERVICE_UNAVAILABLE,
		};
		Self::new(status, code, message)
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
		if value.message.starts_with("actor_lost") || value.message.contains("actor is lost") {
			return Self::function("actor_lost", value.message);
		}
		if value.message.starts_with("secret_unavailable")
			|| value.message.starts_with("unavailable_secret")
		{
			return Self::function("unavailable_secret", value.message);
		}
		if value.message.contains("checksum mismatch") || value.message.contains("digest mismatch") {
			return Self::function("checksum", value.message);
		}
		if value.message.contains("deadline exceeded") {
			return Self::function("deadline", value.message);
		}
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
