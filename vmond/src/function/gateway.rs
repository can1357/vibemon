//! Authenticated, portable JSON-only HTTP invocation gateway.

use std::{
	collections::HashSet,
	fmt,
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
};

use axum::{
	Json, Router,
	body::Bytes,
	extract::{DefaultBodyLimit, Path, State},
	http::{HeaderMap, StatusCode, header},
	routing::post,
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use sha2::{Digest as _, Sha256};
use vmon_proto::v1 as pb;

use crate::api::ApiError;

use super::FunctionDomain;

const MAX_JSON_BYTES: usize = 64 * 1024 * 1024;

/// Mount the portable unary invocation endpoint.
///
/// middleware. The handler verifies the media type and parses through an
/// I-JSON visitor before it resolves or creates durable work.
pub fn router(domain: Arc<FunctionDomain>) -> Router {
	Router::new()
		.route("/v1/functions/{namespace}/{name}/invoke", post(invoke))
		.layer(DefaultBodyLimit::max(MAX_JSON_BYTES))
		.with_state(domain)
}

async fn invoke(
	State(domain): State<Arc<FunctionDomain>>,
	Path((namespace, name)): Path<(String, String)>,
	headers: HeaderMap,
	body: Bytes,
) -> Result<Json<Value>, ApiError> {
	let content_type = headers
		.get(header::CONTENT_TYPE)
		.and_then(|value| value.to_str().ok())
		.unwrap_or_default();
	if !content_type
		.split(';')
		.next()
		.is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
	{
		return Err(ApiError::new(
			StatusCode::UNSUPPORTED_MEDIA_TYPE,
			"unsupported",
			"public HTTP invocation requires application/json",
		));
	}
	let value = parse_ijson(&body)?;
	let function = pb::FunctionRef { namespace, name };
	let lookup = domain.clone();
	let revision = tokio::task::spawn_blocking(move || lookup.store().get_active_revision(&function))
		.await
		.map_err(join_error)?
		.map_err(ApiError::from)?;
	let serializer = revision
		.spec
		.as_ref()
		.and_then(|spec| spec.serializer.as_ref())
		.ok_or_else(|| ApiError::function("invalid", "function serializer contract is missing"))?;
	if serializer.input_serializer != pb::ValueSerializer::Json as i32
		|| serializer.result_serializer != pb::ValueSerializer::Json as i32
	{
		return Err(ApiError::new(
			StatusCode::UNSUPPORTED_MEDIA_TYPE,
			"unsupported",
			"public HTTP invocation requires JSON input and result serializers",
		));
	}
	let revision_ref = revision
		.r#ref
		.ok_or_else(|| ApiError::function("unavailable", "active revision has no identity"))?;
	let input = json_envelope(&value)?;
	let request = pb::CreateCallRequest {
		r#type: pb::CallType::Unary as i32,
		target: Some(pb::CallTarget { function: Some(revision_ref), receiver: None }),
		inputs: vec![pb::CallInput {
			index: 0,
			payload: Some(pb::call_input::Payload::Value(input)),
			input_id: uuid::Uuid::new_v4().to_string(),
		}],
		inputs_closed: true,
		graph: Some(pb::CallGraph::default()),
		request_id: String::new(),
		labels: Default::default(),
		result_ttl_millis_presence: None,
	};
	let create_domain = domain.clone();
	let call = tokio::task::spawn_blocking(move || {
		create_domain.store().create_call(&request, unix_millis())
	})
	.await
	.map_err(join_error)?
	.map_err(ApiError::from)?;
	let call_id = call
		.r#ref
		.as_ref()
		.map(|call| call.call_id.clone())
		.ok_or_else(|| ApiError::function("unavailable", "created call has no identity"))?;
	// The call is committed, including its input, before workers are notified.
	domain.notify_work();
	let mut watch = domain.watch_call(&call_id, 0).map_err(ApiError::from)?;
	loop {
		let read_domain = domain.clone();
		let id = call_id.clone();
		let current = tokio::task::spawn_blocking(move || read_domain.store().get_call(&id))
			.await
			.map_err(join_error)?
			.map_err(ApiError::from)?;
		match pb::CallStatus::try_from(current.status).unwrap_or(pb::CallStatus::Unspecified) {
			pb::CallStatus::Succeeded => {
				let result_domain = domain.clone();
				let id = call_id.clone();
				let result = tokio::task::spawn_blocking(move || {
					result_domain.store().results_after(&id, 0, 1)
				})
				.await
				.map_err(join_error)?
				.map_err(ApiError::from)?
				.into_iter()
				.next()
				.ok_or_else(|| ApiError::function("unavailable", "call succeeded without a result"))?;
				return result_json(&domain, result).await.map(Json);
			},
			pb::CallStatus::Failed | pb::CallStatus::Cancelled => {
				let error = current.error_presence.and_then(|presence| match presence {
					pb::call_record::ErrorPresence::Error(error) => Some(error),
				});
				return Err(error.map_or_else(
					|| ApiError::function("cancelled", "call was cancelled"),
					|error| ApiError::function(error.code, error.message),
				));
			},
			_ => {},
		}
		watch
			.recv()
			.await
			.map_err(|_| ApiError::function("unavailable", "call event stream closed"))?;
	}
}

fn json_envelope(value: &Value) -> Result<pb::ValueEnvelope, ApiError> {
	let bytes = serde_json::to_vec(value).map_err(|error| ApiError::invalid(error.to_string()))?;
	let checksum = Sha256::digest(&bytes).to_vec();
	Ok(pb::ValueEnvelope {
		schema_version: 1,
		serializer: pb::ValueSerializer::Json as i32,
		compression: pb::ValueCompression::None as i32,
		checksum: Some(pb::Digest { algorithm: pb::DigestAlgorithm::Sha256 as i32, value: checksum }),
		uncompressed_size_bytes: bytes.len() as u64,
		storage: Some(pb::value_envelope::Storage::InlineData(bytes)),
		python_presence: None,
		type_name_presence: None,
	})
}

async fn result_json(domain: &Arc<FunctionDomain>, result: pb::CallResult) -> Result<Value, ApiError> {
	let envelope = match result.outcome {
		Some(pb::call_result::Outcome::Value(value)) => value,
		Some(pb::call_result::Outcome::Error(error)) => {
			return Err(ApiError::function(error.code, error.message));
		},
		None => return Err(ApiError::function("unavailable", "result has no outcome")),
	};
	if envelope.serializer != pb::ValueSerializer::Json as i32 {
		return Err(ApiError::new(
			StatusCode::UNSUPPORTED_MEDIA_TYPE,
			"unsupported",
			"public HTTP responses must use JSON",
		));
	}
	if envelope.compression != pb::ValueCompression::None as i32 {
		return Err(ApiError::function("unsupported", "compressed HTTP results are unsupported"));
	}
	let bytes = match envelope.storage {
		Some(pb::value_envelope::Storage::InlineData(bytes)) => bytes,
		Some(pb::value_envelope::Storage::Artifact(artifact)) => {
			let digest = artifact
				.digest
				.filter(|digest| digest.algorithm == pb::DigestAlgorithm::Sha256 as i32)
				.ok_or_else(|| ApiError::function("checksum", "invalid result artifact digest"))?;
			let digest = hex::encode(digest.value);
			let artifacts = domain.artifacts().clone();
			tokio::task::spawn_blocking(move || {
				artifacts.read(&digest, Some(envelope.uncompressed_size_bytes))
			})
			.await
			.map_err(join_error)?
			.map_err(ApiError::from)?
		},
		None => return Err(ApiError::function("invalid", "result has no storage")),
	};
	if bytes.len() as u64 != envelope.uncompressed_size_bytes {
		return Err(ApiError::function("checksum", "result size does not match envelope"));
	}
	let checksum = envelope
		.checksum
		.ok_or_else(|| ApiError::function("checksum", "result checksum is required"))?;
	if checksum.algorithm != pb::DigestAlgorithm::Sha256 as i32
		|| checksum.value.as_slice() != Sha256::digest(&bytes).as_slice()
	{
		return Err(ApiError::function("checksum", "result checksum mismatch"));
	}
	serde_json::from_slice(&bytes).map_err(|error| ApiError::function("invalid", error.to_string()))
}

fn join_error(error: tokio::task::JoinError) -> ApiError {
	ApiError::function("unavailable", format!("function task failed: {error}"))
}

fn unix_millis() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}
