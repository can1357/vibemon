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

use super::FunctionDomain;
use crate::api::ApiError;

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
	let revision =
		tokio::task::spawn_blocking(move || lookup.store().get_active_revision(&function))
			.await
			.map_err(join_error)?
			.map_err(ApiError::from)?;
	require_json_serializers(&revision)?;
	let revision_ref = revision
		.r#ref
		.ok_or_else(|| ApiError::function("unavailable", "active revision has no identity"))?;
	let input = json_envelope(&value)?;
	let request = pb::CreateCallRequest {
		r#type: pb::CallType::Unary as i32,
		target: Some(pb::CallTarget { function: Some(revision_ref), receiver: None }),
		inputs: vec![pb::CallInput {
			index:    0,
			payload:  Some(pb::call_input::Payload::Value(input)),
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
				let result =
					tokio::task::spawn_blocking(move || result_domain.store().results_after(&id, 0, 1))
						.await
						.map_err(join_error)?
						.map_err(ApiError::from)?
						.into_iter()
						.next()
						.ok_or_else(|| {
							ApiError::function("unavailable", "call succeeded without a result")
						})?;
				return result_json(&domain, result).await.map(Json);
			},
			pb::CallStatus::Failed | pb::CallStatus::Cancelled => {
				let error = current.error_presence.map(|presence| match presence {
					pb::call_record::ErrorPresence::Error(error) => error,
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

fn require_json_serializers(revision: &pb::FunctionRevision) -> Result<(), ApiError> {
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
	Ok(())
}

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

fn parse_ijson(bytes: &[u8]) -> Result<Value, ApiError> {
	let mut deserializer = serde_json::Deserializer::from_slice(bytes);
	let value = IJsonSeed
		.deserialize(&mut deserializer)
		.map_err(|error| ApiError::invalid(format!("invalid I-JSON: {error}")))?;
	deserializer
		.end()
		.map_err(|error| ApiError::invalid(format!("invalid I-JSON: {error}")))?;
	Ok(value)
}

#[derive(Clone, Copy)]
struct IJsonSeed;

impl<'de> DeserializeSeed<'de> for IJsonSeed {
	type Value = Value;

	fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		deserializer.deserialize_any(IJsonVisitor)
	}
}

struct IJsonVisitor;

impl<'de> Visitor<'de> for IJsonVisitor {
	type Value = Value;

	fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("an I-JSON value")
	}

	fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
		Ok(Value::Bool(value))
	}

	fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
	where
		E: de::Error,
	{
		if !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&value) {
			return Err(E::custom("integer is outside the I-JSON safe range"));
		}
		Ok(Value::Number(Number::from(value)))
	}

	fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
	where
		E: de::Error,
	{
		if value > MAX_SAFE_INTEGER as u64 {
			return Err(E::custom("integer is outside the I-JSON safe range"));
		}
		Ok(Value::Number(Number::from(value)))
	}

	fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
	where
		E: de::Error,
	{
		if !value.is_finite() || (value.fract() == 0.0 && value.abs() > MAX_SAFE_INTEGER as f64) {
			return Err(E::custom("number is not finite or is outside the I-JSON safe range"));
		}
		Number::from_f64(value)
			.map(Value::Number)
			.ok_or_else(|| E::custom("number is not representable as JSON"))
	}

	fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
		Ok(Value::String(value.to_owned()))
	}

	fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
		Ok(Value::String(value))
	}

	fn visit_none<E>(self) -> Result<Self::Value, E> {
		Ok(Value::Null)
	}

	fn visit_unit<E>(self) -> Result<Self::Value, E> {
		Ok(Value::Null)
	}

	fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
	where
		A: SeqAccess<'de>,
	{
		let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(1024));
		while let Some(value) = sequence.next_element_seed(IJsonSeed)? {
			values.push(value);
		}
		Ok(Value::Array(values))
	}

	fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
	where
		A: MapAccess<'de>,
	{
		let mut keys = HashSet::new();
		let mut values = Map::new();
		while let Some(key) = object.next_key::<String>()? {
			if !keys.insert(key.clone()) {
				return Err(de::Error::custom(format!("duplicate object key {key:?}")));
			}
			values.insert(key, object.next_value_seed(IJsonSeed)?);
		}
		Ok(Value::Object(values))
	}
}

fn json_envelope(value: &Value) -> Result<pb::ValueEnvelope, ApiError> {
	let bytes = serde_json::to_vec(value).map_err(|error| ApiError::invalid(error.to_string()))?;
	let checksum = Sha256::digest(&bytes).to_vec();
	Ok(pb::ValueEnvelope {
		schema_version:          1,
		serializer:              pb::ValueSerializer::Json as i32,
		compression:             pb::ValueCompression::None as i32,
		checksum:                Some(pb::Digest {
			algorithm: pb::DigestAlgorithm::Sha256 as i32,
			value:     checksum,
		}),
		uncompressed_size_bytes: bytes.len() as u64,
		storage:                 Some(pb::value_envelope::Storage::InlineData(bytes)),
		python_presence:         None,
		type_name_presence:      None,
	})
}

async fn result_json(
	domain: &Arc<FunctionDomain>,
	result: pb::CallResult,
) -> Result<Value, ApiError> {
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn ijson_accepts_safe_integer_boundaries() {
		assert_eq!(
			parse_ijson(b"9007199254740991").expect("positive boundary"),
			Value::Number(Number::from(MAX_SAFE_INTEGER))
		);
		assert_eq!(
			parse_ijson(b"-9007199254740991").expect("negative boundary"),
			Value::Number(Number::from(-MAX_SAFE_INTEGER))
		);
	}

	#[test]
	fn ijson_rejects_unsafe_integers_and_duplicate_keys() {
		for invalid in [
			br"9007199254740992".as_slice(),
			br"-9007199254740992".as_slice(),
			br"1e100".as_slice(),
			br#"{"same":1,"same":2}"#.as_slice(),
		] {
			assert!(parse_ijson(invalid).is_err(), "{}", String::from_utf8_lossy(invalid));
		}
	}

	#[test]
	fn gateway_requires_json_input_and_result_contracts() {
		let mut revision = pb::FunctionRevision {
			spec: Some(pb::FunctionSpec {
				serializer: Some(pb::SerializerSpec {
					input_serializer: pb::ValueSerializer::Json as i32,
					result_serializer: pb::ValueSerializer::Json as i32,
					..Default::default()
				}),
				..Default::default()
			}),
			..Default::default()
		};
		assert!(require_json_serializers(&revision).is_ok());
		revision
			.spec
			.as_mut()
			.unwrap()
			.serializer
			.as_mut()
			.unwrap()
			.input_serializer = pb::ValueSerializer::Cbor as i32;
		assert!(require_json_serializers(&revision).is_err());
		revision
			.spec
			.as_mut()
			.unwrap()
			.serializer
			.as_mut()
			.unwrap()
			.input_serializer = pb::ValueSerializer::Json as i32;
		revision
			.spec
			.as_mut()
			.unwrap()
			.serializer
			.as_mut()
			.unwrap()
			.result_serializer = pb::ValueSerializer::Cloudpickle as i32;
		assert!(require_json_serializers(&revision).is_err());
	}

	#[test]
	fn ijson_accepts_nested_unique_string_keys_and_finite_fractionals() {
		let parsed = parse_ijson(br#"{"nested":{"value":1.5},"array":[true,null,"ok"]}"#)
			.expect("valid I-JSON");
		assert_eq!(parsed["nested"]["value"], Value::from(1.5));
	}
}
