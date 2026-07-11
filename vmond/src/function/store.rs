//! Transactional SQLite store for the durable function runtime.

use std::{collections::HashSet, fs, str::FromStr, sync::Mutex};
use chrono::{TimeZone as _, Utc};
use chrono_tz::Tz;
use cron::Schedule;

use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;
use vmon_proto::{prost::Message, v1::*};

use crate::{home::Home, EngineError, Result};

const SCHEMA_VERSION: u32 = 2;
const INPUT_QUEUED: i32 = 1;
const INPUT_LEASED: i32 = 2;
const INPUT_RUNNING: i32 = 3;
const INPUT_SUCCEEDED: i32 = 4;
const INPUT_FAILED: i32 = 5;
const INPUT_CANCELLED: i32 = 6;

/// Exclusive ownership proof for one input attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseToken {
	pub call_id: String,
	pub input_index: u64,
	pub worker_id: String,
	pub lease_generation: u64,
}

/// Work item atomically leased to a worker.
#[derive(Clone, Debug)]
pub struct LeasedInput {
	pub lease: LeaseToken,
	pub revision: FunctionRevision,
	pub input: CallInput,
	pub call: CallRecord,
	pub user_attempts: u32,
	pub infra_attempts: u32,
	pub execution_deadline_ms: Option<u64>,
}

/// A revision with work currently eligible for scheduling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueuedRevision {
	pub revision_id: String,
	pub oldest_available_ms: u64,
	pub queued_inputs: u64,
}
/// Startup recovery counts.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoverySummary {
	pub requeued_inputs: u64,
	pub lost_actors: u64,
}

/// One page of stable, creation-ordered records.
#[derive(Clone, Debug)]
pub struct Page<T> {
	pub items: Vec<T>,
	pub next_page_token: String,
}

/// Durable SQLite metadata store. All methods serialize through one connection;
/// `BEGIN IMMEDIATE` protects competing lease and transition writers.
pub struct Store {
	connection: Mutex<Connection>,
}

impl Store {
	/// Open, migrate, and configure the function metadata database.
	pub fn open(home: &Home) -> Result<Self> {
		fs::create_dir_all(home.functions_dir())?;
		let connection = Connection::open(home.functions_db()).map_err(sql_error)?;
		connection.pragma_update(None, "journal_mode", "WAL").map_err(sql_error)?;
		connection.pragma_update(None, "synchronous", "FULL").map_err(sql_error)?;
		connection.pragma_update(None, "foreign_keys", "ON").map_err(sql_error)?;
		connection.busy_timeout(std::time::Duration::from_secs(5)).map_err(sql_error)?;
		migrate(&connection)?;
		Ok(Self { connection: Mutex::new(connection) })
	}

	/// Register an immutable revision, deduplicating by the full canonical spec digest.
	pub fn register_function(&self, spec: &FunctionSpec, request_id: &str, now_ms: u64) -> Result<FunctionRevision> {
		validate_function_spec(spec)?;
		let spec_bytes = spec.encode_to_vec();
		let digest = canonical_spec_digest(spec);
		let digest_hex = hex::encode(digest);
		let function = spec.function.as_ref().ok_or_else(|| EngineError::invalid("function is required"))?;
		let request_fingerprint = digest_hex.as_bytes();
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		if let Some(resource) = idempotent_resource(&tx, "register", request_id, request_fingerprint)? {
			let revision = revision_by_id(&tx, &resource)?;
			tx.commit().map_err(sql_error)?;
			return Ok(revision);
		}
		if let Some(revision) = revision_by_digest(&tx, &digest_hex)? {
			put_idempotency(&tx, "register", request_id, request_fingerprint, revision_id(&revision)?, now_ms)?;
			tx.commit().map_err(sql_error)?;
			return Ok(revision);
		}
		let id = Uuid::new_v4().to_string();
		let revision = FunctionRevision {
			r#ref: Some(RevisionRef { function: Some(function.clone()), revision_id: id.clone() }),
			spec: Some(spec.clone()),
			spec_digest: Some(Digest { algorithm: DigestAlgorithm::Sha256 as i32, value: digest.to_vec() }),
			created_at_unix_millis: now_ms,
			status: FunctionRevisionStatus::Ready as i32,
			unavailable_secrets: Vec::new(),
			snapshot_provenance_presence: None,
		};
		tx.execute(
			"INSERT INTO revisions(id,digest,namespace,name,spec,record,created_ms) VALUES(?,?,?,?,?,?,?)",
			params![id, digest_hex, function.namespace, function.name, spec_bytes, revision.encode_to_vec(), u64_i64(now_ms)?],
		).map_err(sql_error)?;
		put_idempotency(&tx, "register", request_id, request_fingerprint, &id, now_ms)?;
		for digest in function_spec_artifacts(spec) { add_artifact_ref(&tx, &digest, "revision", &id)?; }
		tx.commit().map_err(sql_error)?;
		Ok(revision)
	}

	/// Resolve a pinned revision identifier.
	pub fn get_revision(&self, revision_id: &str) -> Result<FunctionRevision> {
		let connection = self.connection.lock().map_err(lock_error)?;
		revision_by_id(&connection, revision_id)
	}

	/// Resolve the active revision for a logical function.
	pub fn get_active_revision(&self, function: &FunctionRef) -> Result<FunctionRevision> {
		let connection = self.connection.lock().map_err(lock_error)?;
		let id: String = connection.query_row(
			"SELECT revision_id FROM aliases WHERE namespace=? AND name=?",
			params![function.namespace, function.name], |row| row.get(0),
		).optional().map_err(sql_error)?.ok_or_else(|| EngineError::not_found("function has no active revision"))?;
		revision_by_id(&connection, &id)
	}

	/// Atomically change a logical function alias with optional compare-and-swap.
	pub fn activate_function(&self, revision: &RevisionRef, expected_current: Option<&RevisionRef>, now_ms: u64) -> Result<FunctionRecord> {
		let function = revision.function.as_ref().ok_or_else(|| EngineError::invalid("revision function is required"))?;
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		let stored = revision_by_id(&tx, &revision.revision_id)?;
		if stored.r#ref.as_ref().and_then(|value| value.function.as_ref()) != Some(function) {
			return Err(EngineError::invalid("revision does not belong to function"));
		}
		let current: Option<String> = tx.query_row("SELECT revision_id FROM aliases WHERE namespace=? AND name=?", params![function.namespace, function.name], |row| row.get(0)).optional().map_err(sql_error)?;
		check_expected_revision(current.as_deref(), expected_current)?;
		tx.execute("INSERT INTO aliases(namespace,name,revision_id,updated_ms) VALUES(?,?,?,?) ON CONFLICT(namespace,name) DO UPDATE SET revision_id=excluded.revision_id,updated_ms=excluded.updated_ms", params![function.namespace,function.name,revision.revision_id,u64_i64(now_ms)?]).map_err(sql_error)?;
		tx.commit().map_err(sql_error)?;
		Ok(FunctionRecord { function: Some(function.clone()), current: Some(revision.clone()), updated_at_unix_millis: now_ms })
	}

	/// List revisions using an opaque creation/id cursor.
	pub fn list_revisions(&self, namespace: Option<&str>, page_size: u32, page_token: &str) -> Result<Page<FunctionRevision>> {
		let limit = normalized_page_size(page_size);
		let (after_ms, after_id) = decode_page_token(page_token)?;
		let connection = self.connection.lock().map_err(lock_error)?;
		let mut statement = connection.prepare("SELECT record,created_ms,id FROM revisions WHERE (?1 IS NULL OR namespace=?1) AND (created_ms>?2 OR (created_ms=?2 AND id>?3)) ORDER BY created_ms,id LIMIT ?4").map_err(sql_error)?;
		let rows = statement.query_map(params![namespace, u64_i64(after_ms)?, after_id, i64::from(limit + 1)], |row| Ok((row.get::<_,Vec<u8>>(0)?,row.get::<_,i64>(1)?,row.get::<_,String>(2)?))).map_err(sql_error)?;
		page_from_rows(rows, limit, decode_message)
	}


	/// Delete an inactive, unreferenced immutable function revision.
	pub fn delete_revision(&self, revision:&RevisionRef)->Result<()> {
		let mut connection=self.connection.lock().map_err(lock_error)?; let tx=immediate(&mut connection)?;
		let stored=revision_by_id(&tx,&revision.revision_id)?;
		if stored.r#ref.as_ref().and_then(|r|r.function.as_ref()) != revision.function.as_ref(){return Err(EngineError::invalid("revision belongs to another function"));}
		let referenced:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM aliases WHERE revision_id=? UNION ALL SELECT 1 FROM app_members WHERE revision_id=? UNION ALL SELECT 1 FROM calls WHERE revision_id=? UNION ALL SELECT 1 FROM actors WHERE revision_id=?)",params![revision.revision_id,revision.revision_id,revision.revision_id,revision.revision_id],|r|r.get(0)).map_err(sql_error)?;
		if referenced{return Err(EngineError::busy("revision is active or referenced"));}
		tx.execute("DELETE FROM artifact_refs WHERE owner_kind='revision' AND owner_id=?",[&revision.revision_id]).map_err(sql_error)?;
		tx.execute("DELETE FROM revisions WHERE id=?",[&revision.revision_id]).map_err(sql_error)?;
		tx.commit().map_err(sql_error)?; Ok(())
	}

	/// Atomically publish an immutable application revision.
	pub fn activate_app(&self, request: &ActivateAppRequest, now_ms: u64) -> Result<AppRevision> {
		let app = request.app.as_ref().ok_or_else(|| EngineError::invalid("app is required"))?;
		validate_name(&app.namespace, "app namespace")?; validate_name(&app.name, "app name")?;
		let mut bindings = request.functions.clone();
		bindings.sort_by(|a,b| a.name.cmp(&b.name));
		if bindings.windows(2).any(|pair| pair[0].name == pair[1].name) { return Err(EngineError::invalid("app binding names must be unique")); }
		let digest = digest_bindings(&bindings);
		let fingerprint = digest.as_slice();
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		if let Some(resource) = idempotent_resource(&tx,"activate_app",&request.request_id,fingerprint)? { let value=app_revision_by_id(&tx,&resource)?; tx.commit().map_err(sql_error)?; return Ok(value); }
		for binding in &bindings { let revision=binding.revision.as_ref().ok_or_else(|| EngineError::invalid("app binding revision is required"))?; revision_by_id(&tx,&revision.revision_id)?; }
		let current: Option<String> = tx.query_row("SELECT revision_id FROM app_aliases WHERE namespace=? AND name=?",params![app.namespace,app.name],|row|row.get(0)).optional().map_err(sql_error)?;
		check_expected_app(current.as_deref(), request.expected_current_presence.as_ref().map(|v| match v { activate_app_request::ExpectedCurrentPresence::ExpectedCurrent(r)=>r }))?;
		let id=Uuid::new_v4().to_string();
		let value=AppRevision { r#ref:Some(AppRevisionRef{app:Some(app.clone()),revision_id:id.clone()}),functions:bindings.clone(),content_digest:Some(Digest{algorithm:DigestAlgorithm::Sha256 as i32,value:digest.to_vec()}),created_at_unix_millis:now_ms,previous_presence:current.clone().map(|revision_id|app_revision::PreviousPresence::Previous(AppRevisionRef{app:Some(app.clone()),revision_id})) };
		tx.execute("INSERT INTO app_revisions(id,namespace,name,digest,record,created_ms) VALUES(?,?,?,?,?,?)",params![id,app.namespace,app.name,hex::encode(digest),value.encode_to_vec(),u64_i64(now_ms)?]).map_err(sql_error)?;
		for binding in &bindings { tx.execute("INSERT INTO app_members(app_revision_id,name,revision_id) VALUES(?,?,?)",params![id,binding.name,binding.revision.as_ref().unwrap().revision_id]).map_err(sql_error)?; }
		tx.execute("INSERT INTO app_aliases(namespace,name,revision_id,updated_ms) VALUES(?,?,?,?) ON CONFLICT(namespace,name) DO UPDATE SET revision_id=excluded.revision_id,updated_ms=excluded.updated_ms",params![app.namespace,app.name,id,u64_i64(now_ms)?]).map_err(sql_error)?;
		put_idempotency(&tx,"activate_app",&request.request_id,fingerprint,&id,now_ms)?;
		tx.commit().map_err(sql_error)?; Ok(value)
	}

	/// Atomically move an app alias back to an existing immutable revision.
	pub fn rollback_app(&self, request: &RollbackAppRequest, now_ms: u64) -> Result<AppRevision> {
		let target=request.target.as_ref().ok_or_else(||EngineError::invalid("rollback target is required"))?;
		let app=target.app.as_ref().ok_or_else(||EngineError::invalid("target app is required"))?;
		let fingerprint=target.encode_to_vec();
		let mut connection=self.connection.lock().map_err(lock_error)?; let tx=immediate(&mut connection)?;
		if let Some(resource)=idempotent_resource(&tx,"rollback_app",&request.request_id,&fingerprint)? { let value=app_revision_by_id(&tx,&resource)?; tx.commit().map_err(sql_error)?; return Ok(value); }
		let value=app_revision_by_id(&tx,&target.revision_id)?;
		if value.r#ref.as_ref().and_then(|r|r.app.as_ref()) != Some(app) { return Err(EngineError::invalid("app revision belongs to another app")); }
		let current:Option<String>=tx.query_row("SELECT revision_id FROM app_aliases WHERE namespace=? AND name=?",params![app.namespace,app.name],|r|r.get(0)).optional().map_err(sql_error)?;
		check_expected_app(current.as_deref(),request.expected_current_presence.as_ref().map(|v|match v{rollback_app_request::ExpectedCurrentPresence::ExpectedCurrent(r)=>r}))?;
		tx.execute("UPDATE app_aliases SET revision_id=?,updated_ms=? WHERE namespace=? AND name=?",params![target.revision_id,u64_i64(now_ms)?,app.namespace,app.name]).map_err(sql_error)?;
		put_idempotency(&tx,"rollback_app",&request.request_id,&fingerprint,&target.revision_id,now_ms)?; tx.commit().map_err(sql_error)?; Ok(value)
	}

	/// Resolve a pinned app revision.
	pub fn get_app_revision(&self, id:&str)->Result<AppRevision>{let c=self.connection.lock().map_err(lock_error)?;app_revision_by_id(&c,id)}

	/// Resolve the current app revision.
	pub fn get_active_app(&self,app:&AppRef)->Result<AppRevision>{let c=self.connection.lock().map_err(lock_error)?;let id:String=c.query_row("SELECT revision_id FROM app_aliases WHERE namespace=? AND name=?",params![app.namespace,app.name],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("app has no active revision"))?;app_revision_by_id(&c,&id)}

	/// Create a call and all initial inputs in one transaction.
	pub fn create_call(&self, request:&CreateCallRequest, now_ms:u64)->Result<CallRecord>{
		validate_call_request(request)?; let target=request.target.as_ref().unwrap(); let revision_ref=target.function.as_ref().unwrap();
		let fingerprint=Sha256::digest(request.encode_to_vec()); let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;
		if let Some(resource)=idempotent_resource(&tx,"create_call",&request.request_id,&fingerprint)?{let value=call_record(&tx,&resource)?;tx.commit().map_err(sql_error)?;return Ok(value)}
		let revision=revision_by_id(&tx,&revision_ref.revision_id)?; let spec=revision.spec.as_ref().ok_or_else(||EngineError::engine("revision missing spec"))?;
		let timeouts=spec.timeouts.as_ref(); let queue_deadline=timeouts.and_then(|t|(t.queue_millis>0).then(||now_ms.saturating_add(t.queue_millis))); let execution_ms=timeouts.map_or(0,|t|t.execution_millis); let ttl=request.result_ttl_millis_presence.as_ref().map(|v|match v{create_call_request::ResultTtlMillisPresence::ResultTtlMillis(v)=>*v}).unwrap_or_else(||timeouts.map_or(0,|t|t.result_ttl_millis));
		let id=Uuid::new_v4().to_string();let mut persisted=request.clone();for input in &mut persisted.inputs{if input.input_id.is_empty(){input.input_id=assigned_input_id(&id,input.index);}}let mut persisted_ids=HashSet::new();if persisted.inputs.iter().any(|input|!persisted_ids.insert(&input.input_id)){return Err(EngineError::invalid("input ids must be unique"));}let status=if persisted.inputs.is_empty() && !persisted.inputs_closed {CallStatus::Pending}else{CallStatus::Queued};
		tx.execute("INSERT INTO calls(id,revision_id,actor_id,kind,status,input_closed,request,created_ms,updated_ms,queued_ms,queue_deadline_ms,execution_timeout_ms,result_ttl_ms,event_seq,result_seq) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0,0)",params![id,revision_ref.revision_id,target.actor_presence.as_ref().map(|v|match v{call_target::ActorPresence::Actor(a)=>a.actor_id.as_str()}),request.r#type,status as i32,persisted.inputs_closed,persisted.encode_to_vec(),u64_i64(now_ms)?,u64_i64(now_ms)?,u64_i64(now_ms)?,opt_u64_i64(queue_deadline)?,u64_i64(execution_ms)?,u64_i64(ttl)?]).map_err(sql_error)?;
		for input in &persisted.inputs { insert_input(&tx,&id,input,now_ms)?; for digest in envelope_artifacts(input.value.as_ref()) {add_artifact_ref(&tx,&digest,"input",&format!("{id}:{}",input.index))?;} }
		append_status_event(&tx,&id,status,now_ms)?; put_idempotency(&tx,"create_call",&request.request_id,&fingerprint,&id,now_ms)?;
		let value=call_record(&tx,&id)?; tx.commit().map_err(sql_error)?; Ok(value)
	}

	/// Append one exactly-next input transactionally. Replays of identical committed indexes are idempotent.
	pub fn append_input(&self,call_id:&str,input:&CallInput,now_ms:u64)->Result<u64>{let mut normalized=input.clone();if normalized.input_id.is_empty(){normalized.input_id=assigned_input_id(call_id,normalized.index);}validate_envelope(normalized.value.as_ref().ok_or_else(||EngineError::invalid("input value is required"))?)?;let input=&normalized;let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;let (closed,count,status):(bool,u64,i32)=tx.query_row("SELECT input_closed,(SELECT COUNT(*) FROM inputs WHERE call_id=calls.id),status FROM calls WHERE id=?",[call_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("call not found"))?;if closed{return Err(EngineError::invalid("call inputs are closed"))}if input.index<count{let existing:Vec<u8>=tx.query_row("SELECT payload FROM inputs WHERE call_id=? AND input_index=?",params![call_id,u64_i64(input.index)?],|r|r.get(0)).map_err(sql_error)?;if existing!=input.encode_to_vec(){return Err(EngineError::invalid("input index already contains different payload"))}tx.commit().map_err(sql_error)?;return Ok(count)}if input.index!=count{return Err(EngineError::invalid(format!("input index must be contiguous; expected {count}")))}let mut statement=tx.prepare("SELECT payload FROM inputs WHERE call_id=?").map_err(sql_error)?;let blobs=statement.query_map([call_id],|row|row.get::<_,Vec<u8>>(0)).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;drop(statement);for blob in blobs{let existing:CallInput=decode_message(&blob)?;if existing.input_id==input.input_id{return Err(EngineError::invalid("input id already exists in call"));}}insert_input(&tx,call_id,input,now_ms)?;if status==CallStatus::Pending as i32{tx.execute("UPDATE calls SET status=?,queued_ms=?,updated_ms=? WHERE id=?",params![CallStatus::Queued as i32,u64_i64(now_ms)?,u64_i64(now_ms)?,call_id]).map_err(sql_error)?;append_status_event(&tx,call_id,CallStatus::Queued,now_ms)?;}for digest in envelope_artifacts(input.value.as_ref()){add_artifact_ref(&tx,&digest,"input",&format!("{call_id}:{}",input.index))?;}tx.commit().map_err(sql_error)?;Ok(count+1)}

	/// Close an input stream only when its committed count matches the caller's frontier.
	pub fn close_inputs(&self,call_id:&str,expected_count:u64,now_ms:u64)->Result<CallRecord>{let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;let (closed,count):(bool,u64)=tx.query_row("SELECT input_closed,(SELECT COUNT(*) FROM inputs WHERE call_id=calls.id) FROM calls WHERE id=?",[call_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("call not found"))?;if count!=expected_count{return Err(EngineError::invalid(format!("expected {expected_count} inputs, found {count}")))}if !closed{tx.execute("UPDATE calls SET input_closed=1,updated_ms=? WHERE id=?",params![u64_i64(now_ms)?,call_id]).map_err(sql_error)?;let response=StreamCallInputsResponse{call:Some(CallRef{call_id:call_id.into()}),committed_input_count:count};append_event(&tx,call_id,CallEventType::InputClosed,call_event::Payload::InputClosed(response),now_ms)?;}let value=call_record(&tx,call_id)?;tx.commit().map_err(sql_error)?;Ok(value)}

	/// Return the latest durable call record.
	pub fn get_call(&self,id:&str)->Result<CallRecord>{let c=self.connection.lock().map_err(lock_error)?;call_record(&c,id)}

	/// Return the disconnect policy and private creator-session capability.
	pub fn client_cancellation(&self, call_id: &str) -> Result<(ClientCancellationPolicy, Option<String>)> {
		let connection = self.connection.lock().map_err(lock_error)?;
		let bytes: Vec<u8> = connection
			.query_row("SELECT request FROM calls WHERE id=?", [call_id], |row| row.get(0))
			.optional()
			.map_err(sql_error)?
			.ok_or_else(|| EngineError::not_found("call not found"))?;
		let request: CreateCallRequest = decode_message(&bytes)?;
		let policy = ClientCancellationPolicy::try_from(request.client_cancellation)
			.map_err(|_| EngineError::engine("corrupt client cancellation policy"))?;
		let session = request.client_session_id_presence.map(|value| match value {
			create_call_request::ClientSessionIdPresence::ClientSessionId(id) => id,
		});
		Ok((policy, session))
	}

	/// Return one persisted input, including a server-assigned stable input ID.
	pub fn get_input(&self,call_id:&str,index:u64)->Result<CallInput>{let connection=self.connection.lock().map_err(lock_error)?;let bytes:Vec<u8>=connection.query_row("SELECT payload FROM inputs WHERE call_id=? AND input_index=?",params![call_id,u64_i64(index)?],|row|row.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("call input not found"))?;decode_message(&bytes)}


	/// Return one indexed durable result.
	pub fn get_result(&self,call_id:&str,index:u64)->Result<CallResult>{let connection=self.connection.lock().map_err(lock_error)?;let bytes:Vec<u8>=connection.query_row("SELECT payload FROM results WHERE call_id=? AND result_index=?",params![call_id,u64_i64(index)?],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("call result not found"))?;decode_message(&bytes)}

	/// List calls with stable pagination and typed filters.
	pub fn list_calls(&self,request:&ListCallsRequest)->Result<Page<CallRecord>>{
		let function=request.function_presence.as_ref().map(|v|match v{list_calls_request::FunctionPresence::Function(f)=>f});
		let actor=request.actor_presence.as_ref().map(|v|match v{list_calls_request::ActorPresence::Actor(a)=>a});
		let status=request.status_presence.as_ref().map(|v|match v{list_calls_request::StatusPresence::Status(s)=>*s});
		let created_after=request.created_after_presence.as_ref().map(|v|match v{list_calls_request::CreatedAfterPresence::CreatedAfterUnixMillis(ms)=>*ms}).unwrap_or(0);
		let (token_ms,token_id)=decode_page_token(&request.page_token)?;let limit=normalized_page_size(request.page_size);
		let connection=self.connection.lock().map_err(lock_error)?;let mut statement=connection.prepare("SELECT c.id,c.created_ms FROM calls c JOIN revisions r ON r.id=c.revision_id WHERE (?1 IS NULL OR r.namespace=?1) AND (?2 IS NULL OR r.name=?2) AND (?3 IS NULL OR c.status=?3) AND (?4 IS NULL OR c.actor_id=?4) AND c.created_ms>=?5 AND (c.created_ms>?6 OR (c.created_ms=?6 AND c.id>?7)) ORDER BY c.created_ms,c.id LIMIT ?8").map_err(sql_error)?;
		let rows=statement.query_map(params![function.map(|f|f.namespace.as_str()),function.map(|f|f.name.as_str()),status,actor.map(|a|a.actor_id.as_str()),u64_i64(created_after)?,u64_i64(token_ms)?,token_id,i64::from(limit+1)],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;
		let more=rows.len()>limit as usize;let rows=if more{&rows[..limit as usize]}else{&rows[..]};let next_page_token=if more{rows.last().map(|(id,ms)|format!("{ms}:{id}")).unwrap_or_default()}else{String::new()};let mut items=Vec::with_capacity(rows.len());for(id,_)in rows{items.push(call_record(&connection,id)?)}Ok(Page{items,next_page_token})
	}

	/// Lease the oldest available input across all revisions.
	pub fn lease_next(&self, worker_id: &str, now_ms: u64, lease_ms: u64) -> Result<Option<LeasedInput>> {
		self.lease_next_matching(None, worker_id, now_ms, lease_ms)
	}

	/// Summarize revisions that currently have eligible queued work.
	pub fn queued_revisions(&self, now_ms: u64) -> Result<Vec<QueuedRevision>> {
		let connection = self.connection.lock().map_err(lock_error)?;
		let mut statement = connection.prepare(
			"SELECT c.revision_id,MIN(i.available_ms),COUNT(*) FROM inputs i JOIN calls c ON c.id=i.call_id WHERE i.status=? AND i.available_ms<=? AND c.status IN (?,?) AND (c.queue_deadline_ms IS NULL OR c.queue_deadline_ms>?) GROUP BY c.revision_id ORDER BY MIN(i.available_ms),c.revision_id"
		).map_err(sql_error)?;
		let rows = statement.query_map(params![INPUT_QUEUED,u64_i64(now_ms)?,CallStatus::Queued as i32,CallStatus::Running as i32,u64_i64(now_ms)?], |row| {
			Ok(QueuedRevision { revision_id: row.get(0)?, oldest_available_ms: row.get(1)?, queued_inputs: row.get(2)? })
		}).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;
		Ok(rows)
	}

	/// Lease the oldest available input for one immutable revision pool.
	pub fn lease_next_for_revision(&self, revision_id: &str, worker_id: &str, now_ms: u64, lease_ms: u64) -> Result<Option<LeasedInput>> {
		self.lease_next_matching(Some(revision_id), worker_id, now_ms, lease_ms)
	}

	fn lease_next_matching(&self, revision_id: Option<&str>, worker_id: &str, now_ms: u64, lease_ms: u64) -> Result<Option<LeasedInput>> {
		if worker_id.is_empty() || lease_ms == 0 { return Err(EngineError::invalid("worker id and positive lease duration are required")); }
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		expire_deadlines_tx(&tx, now_ms)?;
		let candidate: Option<(String, i64)> = tx.query_row(
			"SELECT i.call_id,i.input_index FROM inputs i JOIN calls c ON c.id=i.call_id WHERE i.status=? AND i.available_ms<=? AND c.status IN (?,?) AND (c.queue_deadline_ms IS NULL OR c.queue_deadline_ms>?) AND (? IS NULL OR c.revision_id=?) ORDER BY i.available_ms,c.created_ms,i.call_id,i.input_index LIMIT 1",
			params![INPUT_QUEUED,u64_i64(now_ms)?,CallStatus::Queued as i32,CallStatus::Running as i32,u64_i64(now_ms)?,revision_id,revision_id],
			|row| Ok((row.get(0)?,row.get(1)?))
		).optional().map_err(sql_error)?;
		let Some((call_id,index)) = candidate else { tx.commit().map_err(sql_error)?; return Ok(None); };
		let generation: i64 = tx.query_row("SELECT lease_generation+1 FROM inputs WHERE call_id=? AND input_index=?",params![call_id,index],|row|row.get(0)).map_err(sql_error)?;
		let changed = tx.execute("UPDATE inputs SET status=?,lease_owner=?,lease_expiry_ms=?,lease_generation=? WHERE call_id=? AND input_index=? AND status=?",params![INPUT_LEASED,worker_id,u64_i64(now_ms.saturating_add(lease_ms))?,generation,call_id,index,INPUT_QUEUED]).map_err(sql_error)?;
		if changed != 1 { return Err(EngineError::busy("input lease was claimed concurrently")); }
		let selected_revision: String = tx.query_row("SELECT revision_id FROM calls WHERE id=?",[&call_id],|row|row.get(0)).map_err(sql_error)?;
		let revision = revision_by_id(&tx,&selected_revision)?;
		let input_blob: Vec<u8> = tx.query_row("SELECT payload FROM inputs WHERE call_id=? AND input_index=?",params![call_id,index],|row|row.get(0)).map_err(sql_error)?;
		let input = decode_message(&input_blob)?;
		let (user_attempts,infra_attempts): (u32,u32) = tx.query_row("SELECT user_attempts,infra_attempts FROM inputs WHERE call_id=? AND input_index=?",params![call_id,index],|row|Ok((row.get(0)?,row.get(1)?))).map_err(sql_error)?;
		let execution_timeout: u64 = tx.query_row("SELECT execution_timeout_ms FROM calls WHERE id=?",[&call_id],|row|row.get(0)).map_err(sql_error)?;
		let call = call_record(&tx,&call_id)?;
		let item = LeasedInput { lease: LeaseToken { call_id, input_index:index as u64, worker_id:worker_id.into(), lease_generation:generation as u64 }, revision, input, call, user_attempts, infra_attempts, execution_deadline_ms:(execution_timeout>0).then(||now_ms.saturating_add(execution_timeout)) };
		tx.commit().map_err(sql_error)?;
		Ok(Some(item))
	}

	/// Extend a live lease, rejecting stale owners and generations.
	pub fn heartbeat(&self,lease:&LeaseToken,now_ms:u64,lease_ms:u64)->Result<()>{let c=self.connection.lock().map_err(lock_error)?;let changed=c.execute("UPDATE inputs SET lease_expiry_ms=? WHERE call_id=? AND input_index=? AND lease_owner=? AND lease_generation=? AND status IN (?,?) AND lease_expiry_ms>?",params![u64_i64(now_ms.saturating_add(lease_ms))?,lease.call_id,u64_i64(lease.input_index)?,lease.worker_id,u64_i64(lease.lease_generation)?,INPUT_LEASED,INPUT_RUNNING,u64_i64(now_ms)?]).map_err(sql_error)?;lease_changed(changed)}

	/// Mark a leased input running and append its attempt event atomically.
	pub fn mark_running(&self,lease:&LeaseToken,now_ms:u64,startup:StartupKind)->Result<()>{let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;require_lease(&tx,lease,&[INPUT_LEASED],now_ms)?;require_executable_call(&tx,&lease.call_id)?;tx.execute("UPDATE inputs SET status=?,started_ms=COALESCE(started_ms,?),user_attempts=user_attempts+1 WHERE call_id=? AND input_index=?",params![INPUT_RUNNING,u64_i64(now_ms)?,lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;let status:CallStatus=call_status(&tx,&lease.call_id)?;if status==CallStatus::Queued{tx.execute("UPDATE calls SET status=?,started_ms=COALESCE(started_ms,?),updated_ms=? WHERE id=?",params![CallStatus::Running as i32,u64_i64(now_ms)?,u64_i64(now_ms)?,lease.call_id]).map_err(sql_error)?;append_status_event(&tx,&lease.call_id,CallStatus::Running,now_ms)?;}let attempt:u32=tx.query_row("SELECT user_attempts FROM inputs WHERE call_id=? AND input_index=?",params![lease.call_id,u64_i64(lease.input_index)?],|r|r.get(0)).map_err(sql_error)?;append_event(&tx,&lease.call_id,CallEventType::Attempt,call_event::Payload::AttemptEvent(AttemptEvent{attempt,status:AttemptStatus::Started as i32,startup:startup as i32,worker_id:lease.worker_id.clone(),error_presence:None}),now_ms)?;tx.commit().map_err(sql_error)?;Ok(())}

	/// Commit a result exactly once. A terminal input cannot be rewritten.
	pub fn succeed(&self,lease:&LeaseToken,result:&CallResult,stats:Option<&AttemptStats>,now_ms:u64)->Result<()>{let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;require_lease(&tx,lease,&[INPUT_RUNNING,INPUT_LEASED],now_ms)?;require_executable_call(&tx,&lease.call_id)?;if result.index!=lease.input_index{return Err(EngineError::invalid("result index does not match lease"))}let sequence=next_result_sequence(&tx,&lease.call_id)?;let mut stored=result.clone();stored.call=Some(CallRef{call_id:lease.call_id.clone()});stored.sequence=sequence;stored.created_at_unix_millis=now_ms;stored.input_index=lease.input_index;if stored.input_id.is_empty(){let input:CallInput=decode_message(&tx.query_row::<Vec<u8>,_,_>("SELECT payload FROM inputs WHERE call_id=? AND input_index=?",params![lease.call_id,u64_i64(lease.input_index)?],|row|row.get(0)).map_err(sql_error)?)?;stored.input_id=input.input_id;}stored.yield_index_presence=None;tx.execute("INSERT INTO results(call_id,result_index,sequence,payload,created_ms) VALUES(?,?,?,?,?)",params![lease.call_id,u64_i64(stored.index)?,u64_i64(sequence)?,stored.encode_to_vec(),u64_i64(now_ms)?]).map_err(sql_error)?;tx.execute("UPDATE inputs SET status=?,finished_ms=?,result=?,lease_owner=NULL,lease_expiry_ms=NULL WHERE call_id=? AND input_index=?",params![INPUT_SUCCEEDED,u64_i64(now_ms)?,stored.encode_to_vec(),lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;if let Some(stats)=stats{tx.execute("UPDATE inputs SET stats=? WHERE call_id=? AND input_index=?",params![stats.encode_to_vec(),lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;}for digest in result_artifacts(&stored){add_artifact_ref(&tx,&digest,"result",&format!("{}:{}",lease.call_id,stored.index))?;}let event_type=CallEventType::Result;let payload=call_event::Payload::Result(stored);append_event(&tx,&lease.call_id,event_type,payload,now_ms)?;complete_call_tx(&tx,&lease.call_id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(())}

	/// Commit one generator yield without terminalizing the leased input.
	pub fn commit_yield(&self, lease: &LeaseToken, index: u64, value: ValueEnvelope, now_ms: u64) -> Result<CallEvent> {
		validate_envelope(&value)?;
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		require_lease(&tx, lease, &[INPUT_RUNNING], now_ms)?;
		require_executable_call(&tx,&lease.call_id)?;
		if call_kind(&tx, &lease.call_id)? != CallType::Generator {
			return Err(EngineError::invalid("only generator calls may commit yields"));
		}
		let sequence = next_result_sequence(&tx, &lease.call_id)?;
		let input: CallInput = decode_message(&tx.query_row::<Vec<u8>,_,_>("SELECT payload FROM inputs WHERE call_id=? AND input_index=?",params![lease.call_id,u64_i64(lease.input_index)?],|row|row.get(0)).map_err(sql_error)?)?;
		let result = CallResult {
			call: Some(CallRef { call_id: lease.call_id.clone() }),
			index,
			outcome: Some(call_result::Outcome::Value(value)),
			created_at_unix_millis: now_ms,
			sequence,
			input_id: input.input_id,
			input_index: lease.input_index,
			yield_index_presence: Some(call_result::YieldIndexPresence::YieldIndex(index)),
		};
		tx.execute("INSERT INTO results(call_id,result_index,sequence,payload,created_ms) VALUES(?,?,?,?,?)", params![lease.call_id,u64_i64(index)?,u64_i64(sequence)?,result.encode_to_vec(),u64_i64(now_ms)?]).map_err(sql_error)?;
		for digest in result_artifacts(&result) { add_artifact_ref(&tx,&digest,"result",&format!("{}:{index}",lease.call_id))?; }
		let event_sequence = append_event(&tx,&lease.call_id,CallEventType::Yield,call_event::Payload::Yield(result),now_ms)?;
		let event = event_by_sequence(&tx,&lease.call_id,event_sequence)?;
		tx.commit().map_err(sql_error)?;
		Ok(event)
	}

	/// Append worker log bytes while the owning lease is live.
	pub fn append_log(&self, lease: &LeaseToken, stream: LogStream, data: Vec<u8>, now_ms: u64) -> Result<CallEvent> {
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		require_lease(&tx,lease,&[INPUT_RUNNING],now_ms)?;
		let sequence = append_event(&tx,&lease.call_id,CallEventType::Log,call_event::Payload::Log(LogEvent { stream:stream as i32,data }),now_ms)?;
		let event = event_by_sequence(&tx,&lease.call_id,sequence)?;
		tx.commit().map_err(sql_error)?;
		Ok(event)
	}

	/// Return one exact event sequence for post-commit watcher publication.
	pub fn get_event(&self, call_id: &str, sequence: u64) -> Result<CallEvent> {
		let connection = self.connection.lock().map_err(lock_error)?;
		event_by_sequence(&connection,call_id,sequence)
	}
	/// Commit a non-retryable user failure.
	pub fn fail_user(&self,lease:&LeaseToken,error:&CallError,now_ms:u64)->Result<()>{finish_error(self,lease,error,None,now_ms)}

	/// Requeue an infrastructure failure at a caller-selected retry instant.
	pub fn fail_infra(&self,lease:&LeaseToken,error:&CallError,retry_at_ms:u64,now_ms:u64)->Result<()>{retry_error(self,lease,error,retry_at_ms,true,now_ms)}

	/// Requeue a retryable user failure without conflating it with infrastructure loss.
	pub fn retry_user(&self,lease:&LeaseToken,error:&CallError,retry_at_ms:u64,now_ms:u64)->Result<()>{retry_error(self,lease,error,retry_at_ms,false,now_ms)}

	/// Persist cancellation before workers are signalled.
	pub fn cancel_call(&self, call_id: &str, reason: &str, request_id: &str, now_ms: u64) -> Result<CallRecord> {
		let fingerprint = Sha256::digest([call_id.as_bytes(),reason.as_bytes()].concat());
		let mut connection = self.connection.lock().map_err(lock_error)?;
		let tx = immediate(&mut connection)?;
		if let Some(resource) = idempotent_resource(&tx,"cancel_call",request_id,&fingerprint)? { let value=call_record(&tx,&resource)?; tx.commit().map_err(sql_error)?; return Ok(value); }
		let status=call_status(&tx,call_id)?;
		if terminal_call(status) { let value=call_record(&tx,call_id)?; put_idempotency(&tx,"cancel_call",request_id,&fingerprint,call_id,now_ms)?; tx.commit().map_err(sql_error)?; return Ok(value); }
		if status != CallStatus::Cancelling {
			tx.execute("UPDATE calls SET status=?,cancel_reason=?,updated_ms=? WHERE id=?",params![CallStatus::Cancelling as i32,reason,u64_i64(now_ms)?,call_id]).map_err(sql_error)?;
			append_event(&tx,call_id,CallEventType::CancelRequested,call_event::Payload::CancelRequested(CancelCallRequest{call:Some(CallRef{call_id:call_id.into()}),reason:reason.into(),request_id:request_id.into()}),now_ms)?;
			append_status_event(&tx,call_id,CallStatus::Cancelling,now_ms)?;
			tx.execute("UPDATE inputs SET status=?,finished_ms=? WHERE call_id=? AND status=?",params![INPUT_CANCELLED,u64_i64(now_ms)?,call_id,INPUT_QUEUED]).map_err(sql_error)?;
		}
		let active:u64=tx.query_row("SELECT COUNT(*) FROM inputs WHERE call_id=? AND status IN (?,?)",params![call_id,INPUT_LEASED,INPUT_RUNNING],|row|row.get(0)).map_err(sql_error)?;
		if active==0 { finalize_cancelled_tx(&tx,call_id,now_ms)?; }
		put_idempotency(&tx,"cancel_call",request_id,&fingerprint,call_id,now_ms)?;
		let value=call_record(&tx,call_id)?; tx.commit().map_err(sql_error)?; Ok(value)
	}

	/// Terminalize a worker-owned input after a persisted cancellation request.
	pub fn finish_cancelled(&self, lease:&LeaseToken, reason:&str, now_ms:u64)->Result<CallRecord>{
		let mut connection=self.connection.lock().map_err(lock_error)?; let tx=immediate(&mut connection)?;
		require_lease(&tx,lease,&[INPUT_LEASED,INPUT_RUNNING],now_ms)?;
		if call_status(&tx,&lease.call_id)? != CallStatus::Cancelling { return Err(EngineError::invalid("call has no pending cancellation")); }
		tx.execute("UPDATE inputs SET status=?,finished_ms=?,lease_owner=NULL,lease_expiry_ms=NULL,error=NULL WHERE call_id=? AND input_index=?",params![INPUT_CANCELLED,u64_i64(now_ms)?,lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;
		if !reason.is_empty(){tx.execute("UPDATE calls SET cancel_reason=? WHERE id=?",params![reason,lease.call_id]).map_err(sql_error)?;}
		let active:u64=tx.query_row("SELECT COUNT(*) FROM inputs WHERE call_id=? AND status IN (?,?)",params![lease.call_id,INPUT_LEASED,INPUT_RUNNING],|row|row.get(0)).map_err(sql_error)?;
		if active==0{finalize_cancelled_tx(&tx,&lease.call_id,now_ms)?;}
		let value=call_record(&tx,&lease.call_id)?; tx.commit().map_err(sql_error)?; Ok(value)
	}

	/// Requeue expired leases and append durable attempt failures.
	pub fn expire_leases(&self,now_ms:u64)->Result<u64>{let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;let mut statement=tx.prepare("SELECT call_id,input_index FROM inputs WHERE status IN (?,?) AND lease_expiry_ms<=?").map_err(sql_error)?;let rows=statement.query_map(params![INPUT_LEASED,INPUT_RUNNING,u64_i64(now_ms)?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?))).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;drop(statement);for (call,index) in &rows{tx.execute("UPDATE inputs SET status=?,available_ms=?,lease_owner=NULL,lease_expiry_ms=NULL WHERE call_id=? AND input_index=?",params![INPUT_QUEUED,u64_i64(now_ms)?,call,index]).map_err(sql_error)?;}tx.commit().map_err(sql_error)?;Ok(rows.len() as u64)}

	/// Complete calls whose closed input set is entirely terminal.
	pub fn complete_ready_calls(&self,now_ms:u64)->Result<Vec<String>>{let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;let mut s=tx.prepare("SELECT id FROM calls WHERE input_closed=1 AND status IN (?,?)").map_err(sql_error)?;let ids=s.query_map(params![CallStatus::Queued as i32,CallStatus::Running as i32],|r|r.get::<_,String>(0)).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;drop(s);let mut completed=Vec::new();for id in ids{if complete_call_tx(&tx,&id,now_ms)?{completed.push(id)}}tx.commit().map_err(sql_error)?;Ok(completed)}


	/// Expire retained result payloads and release their artifact references.
	pub fn expire_results(&self,now_ms:u64)->Result<u64>{
		let mut connection=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut connection)?;
		let mut statement=tx.prepare("SELECT id FROM calls WHERE result_expiry_ms IS NOT NULL AND result_expiry_ms<=?").map_err(sql_error)?;
		let calls=statement.query_map([u64_i64(now_ms)?],|row|row.get::<_,String>(0)).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;drop(statement);
		for id in &calls{tx.execute("DELETE FROM artifact_refs WHERE owner_kind='result' AND (owner_id=? OR owner_id LIKE ?)",params![id,format!("{id}:%")]).map_err(sql_error)?;tx.execute("DELETE FROM results WHERE call_id=?",[id]).map_err(sql_error)?;tx.execute("DELETE FROM events WHERE call_id=? AND event_type IN (?,?)",params![id,CallEventType::Yield as i32,CallEventType::Result as i32]).map_err(sql_error)?;tx.execute("UPDATE inputs SET result=NULL WHERE call_id=?",[id]).map_err(sql_error)?;tx.execute("UPDATE calls SET result_expiry_ms=NULL WHERE id=?",[id]).map_err(sql_error)?;}
		tx.commit().map_err(sql_error)?;Ok(calls.len() as u64)
	}

	/// Replay immutable call events strictly after a durable cursor.
	pub fn events_after(&self,call_id:&str,after_sequence:u64,limit:u32)->Result<Vec<CallEvent>> { let c=self.connection.lock().map_err(lock_error)?;let mut s=c.prepare("SELECT payload FROM events WHERE call_id=? AND sequence>? ORDER BY sequence LIMIT ?").map_err(sql_error)?;let rows=s.query_map(params![call_id,u64_i64(after_sequence)?,i64::from(normalized_page_size(limit))],|r|r.get::<_,Vec<u8>>(0)).map_err(sql_error)?;decode_rows(rows) }

	/// Alias used by call watchers.
	pub fn list_events(&self, call_id: &str, after_sequence: u64, limit: u32) -> Result<Vec<CallEvent>> {
		self.events_after(call_id, after_sequence, limit)
	}

	/// Replay immutable results strictly after a durable cursor.
	pub fn results_after(&self,call_id:&str,after_sequence:u64,limit:u32)->Result<Vec<CallResult>> { let c=self.connection.lock().map_err(lock_error)?;let mut s=c.prepare("SELECT payload FROM results WHERE call_id=? AND sequence>? ORDER BY sequence LIMIT ?").map_err(sql_error)?;let rows=s.query_map(params![call_id,u64_i64(after_sequence)?,i64::from(normalized_page_size(limit))],|r|r.get::<_,Vec<u8>>(0)).map_err(sql_error)?;decode_rows(rows) }

	/// Alias used by reconnecting result consumers.
	pub fn list_results(&self, call_id: &str, after_sequence: u64, limit: u32) -> Result<Vec<CallResult>> {
		self.results_after(call_id, after_sequence, limit)
	}

	/// Replace or create a typed schedule with idempotency.
	pub fn put_schedule(&self,request:&CreateScheduleRequest,next_run_ms:Option<u64>,now_ms:u64)->Result<ScheduleRecord>{let spec=request.spec.as_ref().ok_or_else(||EngineError::invalid("schedule spec is required"))?;validate_schedule(spec)?;let id=request.schedule_id_presence.as_ref().map(|v|match v{create_schedule_request::ScheduleIdPresence::ScheduleId(id)=>id.clone()}).unwrap_or_else(||Uuid::new_v4().to_string());validate_id(&id,"schedule id")?;let fingerprint=Sha256::digest(request.encode_to_vec());let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;if let Some(resource)=idempotent_resource(&tx,"schedule",&request.request_id,&fingerprint)?{let v=schedule_by_id(&tx,&resource)?;tx.commit().map_err(sql_error)?;return Ok(v)}let created:u64=tx.query_row("SELECT created_ms FROM schedules WHERE id=?",[&id],|r|r.get(0)).optional().map_err(sql_error)?.unwrap_or(now_ms);let value=ScheduleRecord{r#ref:Some(ScheduleRef{schedule_id:id.clone()}),spec:Some(spec.clone()),created_at_unix_millis:created,updated_at_unix_millis:now_ms,next_run_presence:next_run_ms.map(schedule_record::NextRunPresence::NextRunUnixMillis)};let app=spec.app.as_ref().and_then(|r|r.app.as_ref()).ok_or_else(||EngineError::invalid("schedule app revision is required"))?;let function=spec.target.as_ref().and_then(|t|t.function.as_ref()).and_then(|r|r.function.as_ref()).ok_or_else(||EngineError::invalid("schedule target function is required"))?;tx.execute("DELETE FROM artifact_refs WHERE owner_kind='schedule' AND owner_id=?",[&id]).map_err(sql_error)?;tx.execute("INSERT INTO schedules(id,app_namespace,app_name,function_namespace,function_name,status,record,created_ms,updated_ms,next_run_ms) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET app_namespace=excluded.app_namespace,app_name=excluded.app_name,function_namespace=excluded.function_namespace,function_name=excluded.function_name,status=excluded.status,record=excluded.record,updated_ms=excluded.updated_ms,next_run_ms=excluded.next_run_ms",params![id,app.namespace,app.name,function.namespace,function.name,spec.status,value.encode_to_vec(),u64_i64(created)?,u64_i64(now_ms)?,opt_u64_i64(next_run_ms)?]).map_err(sql_error)?;for digest in envelope_artifacts(spec.target.as_ref().and_then(|t|t.input.as_ref())){add_artifact_ref(&tx,&digest,"schedule",&id)?;}put_idempotency(&tx,"schedule",&request.request_id,&fingerprint,&id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(value)}

	/// Return active schedules due at or before `now_ms`.
	pub fn due_schedules(&self,now_ms:u64,limit:u32)->Result<Vec<ScheduleRecord>>{let c=self.connection.lock().map_err(lock_error)?;let mut s=c.prepare("SELECT record FROM schedules WHERE status=? AND next_run_ms<=? ORDER BY next_run_ms,id LIMIT ?").map_err(sql_error)?;decode_rows(s.query_map(params![ScheduleStatus::Active as i32,u64_i64(now_ms)?,i64::from(normalized_page_size(limit))],|r|r.get::<_,Vec<u8>>(0)).map_err(sql_error)?)}

	/// Advance a schedule only if its expected prior next-run value still matches.
	pub fn advance_schedule(&self,id:&str,expected_ms:u64,next_ms:Option<u64>,now_ms:u64)->Result<bool>{let c=self.connection.lock().map_err(lock_error)?;let changed=c.execute("UPDATE schedules SET next_run_ms=?,updated_ms=? WHERE id=? AND next_run_ms=?",params![opt_u64_i64(next_ms)?,u64_i64(now_ms)?,id,u64_i64(expected_ms)?]).map_err(sql_error)?;Ok(changed==1)}


	/// Return one schedule by stable identifier.
	pub fn get_schedule(&self,id:&str)->Result<ScheduleRecord>{let connection=self.connection.lock().map_err(lock_error)?;schedule_by_id(&connection,id)}

	/// List schedules with optional logical app/function filters.
	pub fn list_schedules(&self,request:&ListSchedulesRequest)->Result<Page<ScheduleRecord>>{
		let app=request.app_presence.as_ref().map(|v|match v{list_schedules_request::AppPresence::App(a)=>a});
		let function=request.function_presence.as_ref().map(|v|match v{list_schedules_request::FunctionPresence::Function(f)=>f});
		let (after_ms,after_id)=decode_page_token(&request.page_token)?;let limit=normalized_page_size(request.page_size);
		let connection=self.connection.lock().map_err(lock_error)?;let mut statement=connection.prepare("SELECT record,created_ms,id FROM schedules WHERE (?1 IS NULL OR app_namespace=?1) AND (?2 IS NULL OR app_name=?2) AND (?3 IS NULL OR function_namespace=?3) AND (?4 IS NULL OR function_name=?4) AND (created_ms>?5 OR (created_ms=?5 AND id>?6)) ORDER BY created_ms,id LIMIT ?7").map_err(sql_error)?;
		let rows=statement.query_map(params![app.map(|a|a.namespace.as_str()),app.map(|a|a.name.as_str()),function.map(|f|f.namespace.as_str()),function.map(|f|f.name.as_str()),u64_i64(after_ms)?,after_id,i64::from(limit+1)],|r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,i64>(1)?,r.get::<_,String>(2)?))).map_err(sql_error)?;
		page_from_rows(rows,limit,decode_message)
	}

	/// Delete a schedule and release its artifact references.
	pub fn delete_schedule(&self,id:&str)->Result<()> {let mut connection=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut connection)?;schedule_by_id(&tx,id)?;tx.execute("DELETE FROM artifact_refs WHERE owner_kind='schedule' AND owner_id=?",[id]).map_err(sql_error)?;tx.execute("DELETE FROM schedules WHERE id=?",[id]).map_err(sql_error)?;tx.commit().map_err(sql_error)?;Ok(())}

	/// Create calls for due schedules before CAS-advancing their durable frontier.
	pub fn fire_due_schedules(&self, now_ms: u64, limit: u32) -> Result<Vec<String>> {
		let mut calls = Vec::new();
		for record in self.due_schedules(now_ms, limit)? {
			let schedule_id = record.r#ref.as_ref().ok_or_else(|| EngineError::engine("schedule missing ref"))?.schedule_id.clone();
			let expected = match record.next_run_presence {
				Some(schedule_record::NextRunPresence::NextRunUnixMillis(value)) => value,
				None => continue,
			};
			let spec = record.spec.as_ref().ok_or_else(|| EngineError::engine("schedule missing spec"))?;
			let target = spec.target.as_ref().ok_or_else(|| EngineError::engine("schedule missing target"))?;
			let request = CreateCallRequest {
				r#type: CallType::Unary as i32,
				target: Some(CallTarget { function: target.function.clone(), actor_presence: None, actor_method_presence: None }),
				inputs: vec![CallInput { index: 0, value: target.input.clone(), input_id: format!("{schedule_id}:{expected}") }],
				inputs_closed: true,
				graph: None,
				request_id: format!("schedule:{schedule_id}:{expected}"),
				labels: spec.labels.clone(),
				client_cancellation: ClientCancellationPolicy::Detach as i32,
				result_ttl_millis_presence: None,
				client_session_id_presence: None,
			};
			let call = self.create_call(&request, now_ms)?;
			let next = next_schedule_run(&record, expected)?;
			if self.advance_schedule(&schedule_id, expected, next, now_ms)? {
				calls.push(call.r#ref.ok_or_else(|| EngineError::engine("created call missing ref"))?.call_id);
			}
		}
		Ok(calls)
	}

	/// Create a durable actor identity idempotently.
	pub fn create_actor(&self,request:&CreateActorRequest,now_ms:u64)->Result<ActorRecord>{let function=request.function.as_ref().ok_or_else(||EngineError::invalid("actor function is required"))?;let fingerprint=Sha256::digest(request.encode_to_vec());let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;if let Some(resource)=idempotent_resource(&tx,"actor",&request.request_id,&fingerprint)?{let v=actor_by_id(&tx,&resource)?;tx.commit().map_err(sql_error)?;return Ok(v)}revision_by_id(&tx,&function.revision_id)?;let id=Uuid::new_v4().to_string();let value=ActorRecord{r#ref:Some(ActorRef{actor_id:id.clone()}),function:Some(function.clone()),status:ActorStatus::Creating as i32,created_at_unix_millis:now_ms,updated_at_unix_millis:now_ms,latest_checkpoint_presence:None,labels:request.labels.clone()};tx.execute("INSERT INTO actors(id,revision_id,status,request,record,created_ms,updated_ms) VALUES(?,?,?,?,?,?,?)",params![id,function.revision_id,value.status,request.encode_to_vec(),value.encode_to_vec(),u64_i64(now_ms)?,u64_i64(now_ms)?]).map_err(sql_error)?;for digest in envelope_artifacts(request.initial_state_presence.as_ref().map(|v|match v{create_actor_request::InitialStatePresence::InitialState(e)=>e})){add_artifact_ref(&tx,&digest,"actor",&id)?;}put_idempotency(&tx,"actor",&request.request_id,&fingerprint,&id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(value)}

	/// Get an actor record.
	pub fn get_actor(&self,id:&str)->Result<ActorRecord>{let c=self.connection.lock().map_err(lock_error)?;actor_by_id(&c,id)}


	/// Return an immutable actor checkpoint.
	pub fn get_checkpoint(&self,id:&str)->Result<ActorCheckpoint>{let connection=self.connection.lock().map_err(lock_error)?;checkpoint_by_id(&connection,id)}

	/// Mark an actor deleted without silently discarding its checkpoints.
	pub fn delete_actor(&self,id:&str,now_ms:u64)->Result<ActorRecord>{self.set_actor_status(id,ActorStatus::Deleted,None,now_ms)}

	/// Update actor lifecycle state after validating the transition.
	pub fn set_actor_status(&self,id:&str,status:ActorStatus,worker_id:Option<&str>,now_ms:u64)->Result<ActorRecord>{
		let mut connection=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut connection)?;let value=set_actor_status_tx(&tx,id,None,status,worker_id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(value)
	}

	/// Compare-and-swap an actor lifecycle state.
	pub fn set_actor_status_if(&self,id:&str,expected:ActorStatus,status:ActorStatus,worker_id:Option<&str>,now_ms:u64)->Result<ActorRecord>{
		let mut connection=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut connection)?;let value=set_actor_status_tx(&tx,id,Some(expected),status,worker_id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(value)
	}

	/// Persist an immutable actor checkpoint and atomically point the actor at it.
	pub fn put_checkpoint(&self,checkpoint:&ActorCheckpoint,request_id:&str,now_ms:u64)->Result<ActorCheckpoint>{let actor=checkpoint.actor.as_ref().ok_or_else(||EngineError::invalid("checkpoint actor is required"))?;let state=checkpoint.state.as_ref().ok_or_else(||EngineError::invalid("checkpoint state is required"))?;let fingerprint=Sha256::digest(checkpoint.encode_to_vec());let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;if let Some(resource)=idempotent_resource(&tx,"checkpoint",request_id,&fingerprint)?{let v=checkpoint_by_id(&tx,&resource)?;tx.commit().map_err(sql_error)?;return Ok(v)}let mut actor_record=actor_by_id(&tx,&actor.actor_id)?;let function=checkpoint.function.as_ref().ok_or_else(||EngineError::invalid("checkpoint function is required"))?;if actor_record.function.as_ref()!=Some(function){return Err(EngineError::invalid("checkpoint function does not match actor"))}let id=checkpoint.r#ref.as_ref().map(|r|r.checkpoint_id.clone()).filter(|v|!v.is_empty()).unwrap_or_else(||Uuid::new_v4().to_string());let mut value=checkpoint.clone();value.r#ref=Some(ActorCheckpointRef{checkpoint_id:id.clone()});value.created_at_unix_millis=now_ms;tx.execute("INSERT INTO checkpoints(id,actor_id,revision_id,sequence,record,created_ms) VALUES(?,?,?,?,?,?)",params![id,actor.actor_id,function.revision_id,u64_i64(value.sequence)?,value.encode_to_vec(),u64_i64(now_ms)?]).map_err(sql_error)?;actor_record.latest_checkpoint_presence=Some(actor_record::LatestCheckpointPresence::LatestCheckpoint(ActorCheckpointRef{checkpoint_id:id.clone()}));actor_record.updated_at_unix_millis=now_ms;tx.execute("UPDATE actors SET checkpoint_id=?,record=?,updated_ms=? WHERE id=?",params![id,actor_record.encode_to_vec(),u64_i64(now_ms)?,actor.actor_id]).map_err(sql_error)?;for digest in envelope_artifacts(Some(state)){add_artifact_ref(&tx,&digest,"checkpoint",&id)?;}put_idempotency(&tx,"checkpoint",request_id,&fingerprint,&id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(value)}

	/// Restore an actor pointer to a compatible immutable checkpoint.
	pub fn restore_actor(&self,actor_id:&str,checkpoint_id:&str,request_id:&str,now_ms:u64)->Result<ActorRecord>{let fingerprint=Sha256::digest([actor_id.as_bytes(),checkpoint_id.as_bytes()].concat());let mut c=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;if idempotent_resource(&tx,"restore_actor",request_id,&fingerprint)?.is_some(){let v=actor_by_id(&tx,actor_id)?;tx.commit().map_err(sql_error)?;return Ok(v)}let checkpoint=checkpoint_by_id(&tx,checkpoint_id)?;let mut actor=actor_by_id(&tx,actor_id)?;if actor.function!=checkpoint.function{return Err(EngineError::invalid("checkpoint is incompatible with actor function"))}actor.latest_checkpoint_presence=Some(actor_record::LatestCheckpointPresence::LatestCheckpoint(ActorCheckpointRef{checkpoint_id:checkpoint_id.into()}));actor.status=ActorStatus::Stopped as i32;actor.updated_at_unix_millis=now_ms;tx.execute("UPDATE actors SET status=?,worker_id=NULL,checkpoint_id=?,record=?,updated_ms=? WHERE id=?",params![actor.status,checkpoint_id,actor.encode_to_vec(),u64_i64(now_ms)?,actor_id]).map_err(sql_error)?;put_idempotency(&tx,"restore_actor",request_id,&fingerprint,actor_id,now_ms)?;tx.commit().map_err(sql_error)?;Ok(actor)}

	/// Requeue all pre-restart work and explicitly fail actors without checkpoints.
	pub fn recover_startup(&self,now_ms:u64)->Result<RecoverySummary>{
		let mut connection=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut connection)?;
		let requeued=tx.execute("UPDATE inputs SET status=?,available_ms=?,lease_owner=NULL,lease_expiry_ms=NULL WHERE status IN (?,?)",params![INPUT_QUEUED,u64_i64(now_ms)?,INPUT_LEASED,INPUT_RUNNING]).map_err(sql_error)?;
		tx.execute("UPDATE calls SET status=?,updated_ms=? WHERE status=?",params![CallStatus::Queued as i32,u64_i64(now_ms)?,CallStatus::Running as i32]).map_err(sql_error)?;
		let mut statement=tx.prepare("SELECT id,checkpoint_id FROM actors WHERE worker_id IS NOT NULL AND status IN (?,?)").map_err(sql_error)?;
		let actors=statement.query_map(params![ActorStatus::Ready as i32,ActorStatus::Busy as i32],|row|Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?))).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;drop(statement);
		let mut lost=0;for(id,checkpoint)in actors{let status=if checkpoint.is_some(){ActorStatus::Stopped}else{lost+=1;ActorStatus::Failed};let mut record=actor_by_id(&tx,&id)?;record.status=status as i32;record.updated_at_unix_millis=now_ms;tx.execute("UPDATE actors SET status=?,worker_id=NULL,record=?,updated_ms=? WHERE id=?",params![status as i32,record.encode_to_vec(),u64_i64(now_ms)?,id]).map_err(sql_error)?;}
		tx.commit().map_err(sql_error)?;Ok(RecoverySummary{requeued_inputs:requeued as u64,lost_actors:lost})
	}

	/// Record metadata for a verified content-addressed artifact.
	pub fn record_artifact(&self,digest:&str,size:u64,media_type:Option<&str>,path:&str,created_ms:u64,expires_ms:Option<u64>)->Result<()>{
		validate_digest_hex(digest)?;if path.is_empty(){return Err(EngineError::invalid("artifact path is required"));}
		let mut connection=self.connection.lock().map_err(lock_error)?;let tx=immediate(&mut connection)?;
		let existing:Option<(u64,String)>=tx.query_row("SELECT size,path FROM artifacts WHERE digest=?",[digest],|row|Ok((row.get(0)?,row.get(1)?))).optional().map_err(sql_error)?;
		if let Some((stored_size,stored_path))=existing{if stored_size!=size||stored_path!=path{return Err(EngineError::invalid("artifact digest already has different size or path metadata"));}}
		tx.execute("INSERT INTO artifacts(digest,size,media_type,created_ms,expires_ms,path) VALUES(?,?,?,?,?,?) ON CONFLICT(digest) DO UPDATE SET media_type=COALESCE(artifacts.media_type,excluded.media_type),expires_ms=CASE WHEN artifacts.expires_ms IS NULL THEN excluded.expires_ms WHEN excluded.expires_ms IS NULL THEN artifacts.expires_ms ELSE MAX(artifacts.expires_ms,excluded.expires_ms) END",params![digest,u64_i64(size)?,media_type,u64_i64(created_ms)?,opt_u64_i64(expires_ms)?,path]).map_err(sql_error)?;
		tx.commit().map_err(sql_error)?;Ok(())
	}

	/// Return persisted artifact metadata.
	pub fn stat_artifact(&self,digest:&str)->Result<(u64,Option<String>,u64,Option<u64>,String)>{validate_digest_hex(digest)?;let connection=self.connection.lock().map_err(lock_error)?;connection.query_row("SELECT size,media_type,created_ms,expires_ms,path FROM artifacts WHERE digest=?",[digest],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("artifact not found"))}

	/// Return expired artifacts with no durable metadata references.
	pub fn unreferenced_expired_artifacts(&self,now_ms:u64,limit:u32)->Result<Vec<(String,String)>>{let c=self.connection.lock().map_err(lock_error)?;let mut s=c.prepare("SELECT a.digest,a.path FROM artifacts a WHERE a.expires_ms IS NOT NULL AND a.expires_ms<=? AND NOT EXISTS(SELECT 1 FROM artifact_refs r WHERE r.digest=a.digest) ORDER BY a.expires_ms,a.digest LIMIT ?").map_err(sql_error)?;let rows=s.query_map(params![u64_i64(now_ms)?,i64::from(normalized_page_size(limit))],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;Ok(rows)}

	/// Delete artifact metadata only while it remains expired and unreferenced.
	pub fn delete_unreferenced_artifact(&self,digest:&str,now_ms:u64)->Result<bool>{let c=self.connection.lock().map_err(lock_error)?;let changed=c.execute("DELETE FROM artifacts WHERE digest=? AND expires_ms IS NOT NULL AND expires_ms<=? AND NOT EXISTS(SELECT 1 FROM artifact_refs WHERE digest=artifacts.digest)",params![digest,u64_i64(now_ms)?]).map_err(sql_error)?;Ok(changed==1)}
}


/// Compute the first enabled schedule occurrence strictly after `after_ms`.
pub fn next_schedule_run(record: &ScheduleRecord, after_ms: u64) -> Result<Option<u64>> {
    let spec = record.spec.as_ref().ok_or_else(|| EngineError::invalid("schedule spec is required"))?;
    if ScheduleStatus::try_from(spec.status).unwrap_or(ScheduleStatus::Unspecified) != ScheduleStatus::Active {
        return Ok(None);
    }
    match spec.timing.as_ref().ok_or_else(|| EngineError::invalid("schedule timing is required"))? {
        schedule_spec::Timing::Period(period) => {
            if period.period_millis == 0 { return Err(EngineError::invalid("schedule period must be positive")); }
            if after_ms < period.anchor_unix_millis { return Ok(Some(period.anchor_unix_millis)); }
            let steps = after_ms.saturating_sub(period.anchor_unix_millis) / period.period_millis + 1;
            Ok(period.anchor_unix_millis.checked_add(steps.checked_mul(period.period_millis).ok_or_else(|| EngineError::invalid("schedule timestamp overflow"))?))
        }
        schedule_spec::Timing::Cron(cron) => {
            let timezone = Tz::from_str(&cron.time_zone).map_err(|_| EngineError::invalid("invalid schedule time zone"))?;
            let fields = cron.expression.split_whitespace().count();
            let expression = match fields { 5 => format!("0 {}", cron.expression), 6 | 7 => cron.expression.clone(), _ => return Err(EngineError::invalid("cron expression must have five fields")) };
            let schedule = Schedule::from_str(&expression).map_err(|error| EngineError::invalid(format!("invalid cron expression: {error}")))?;
            let millis = i64::try_from(after_ms).map_err(|_| EngineError::invalid("schedule timestamp exceeds supported range"))?;
            let after = Utc.timestamp_millis_opt(millis).single().ok_or_else(|| EngineError::invalid("invalid schedule timestamp"))?.with_timezone(&timezone);
            Ok(schedule.after(&after).next().map(|time| time.timestamp_millis() as u64))
        }
    }
}

fn set_actor_status_tx(c:&Connection,id:&str,expected:Option<ActorStatus>,status:ActorStatus,worker_id:Option<&str>,now:u64)->Result<ActorRecord>{
	if status==ActorStatus::Unspecified{return Err(EngineError::invalid("unspecified actor status"));}
	let mut value=actor_by_id(c,id)?;let current=ActorStatus::try_from(value.status).map_err(|_|EngineError::engine("corrupt actor status"))?;
	if expected.is_some_and(|expected|expected!=current){return Err(EngineError::busy("actor status changed"));}
	let valid=current==status||matches!((current,status),(ActorStatus::Creating,ActorStatus::Ready|ActorStatus::Failed|ActorStatus::Deleted)|(ActorStatus::Ready,ActorStatus::Busy|ActorStatus::Stopped|ActorStatus::Failed|ActorStatus::Deleted)|(ActorStatus::Busy,ActorStatus::Ready|ActorStatus::Stopped|ActorStatus::Failed|ActorStatus::Deleted)|(ActorStatus::Stopped,ActorStatus::Creating|ActorStatus::Ready|ActorStatus::Failed|ActorStatus::Deleted)|(ActorStatus::Failed,ActorStatus::Stopped|ActorStatus::Deleted));
	if !valid{return Err(EngineError::invalid(format!("invalid actor transition {current:?} -> {status:?}")));}
	value.status=status as i32;value.updated_at_unix_millis=now;c.execute("UPDATE actors SET status=?,worker_id=?,record=?,updated_ms=? WHERE id=?",params![status as i32,worker_id,value.encode_to_vec(),u64_i64(now)?,id]).map_err(sql_error)?;Ok(value)
}
fn migrate(connection:&Connection)->Result<()> {
	let version:u32=connection.pragma_query_value(None,"user_version",|row|row.get(0)).map_err(sql_error)?;
	if version>SCHEMA_VERSION{return Err(EngineError::engine(format!("function store schema {version} is newer than supported {SCHEMA_VERSION}")))}
	connection.execute_batch("BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS artifacts(digest TEXT PRIMARY KEY CHECK(length(digest)=64),size INTEGER NOT NULL CHECK(size>=0),media_type TEXT,created_ms INTEGER NOT NULL,expires_ms INTEGER,path TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS revisions(id TEXT PRIMARY KEY,digest TEXT NOT NULL UNIQUE,namespace TEXT NOT NULL,name TEXT NOT NULL,spec BLOB NOT NULL,record BLOB NOT NULL,created_ms INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS revisions_list ON revisions(created_ms,id); CREATE INDEX IF NOT EXISTS revisions_function ON revisions(namespace,name,created_ms,id);
CREATE TABLE IF NOT EXISTS aliases(namespace TEXT NOT NULL,name TEXT NOT NULL,revision_id TEXT NOT NULL REFERENCES revisions(id),updated_ms INTEGER NOT NULL,PRIMARY KEY(namespace,name));
CREATE TABLE IF NOT EXISTS app_revisions(id TEXT PRIMARY KEY,namespace TEXT NOT NULL,name TEXT NOT NULL,digest TEXT NOT NULL,record BLOB NOT NULL,created_ms INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS app_revisions_list ON app_revisions(created_ms,id);
CREATE TABLE IF NOT EXISTS app_members(app_revision_id TEXT NOT NULL REFERENCES app_revisions(id) ON DELETE CASCADE,name TEXT NOT NULL,revision_id TEXT NOT NULL REFERENCES revisions(id),PRIMARY KEY(app_revision_id,name));
CREATE TABLE IF NOT EXISTS app_aliases(namespace TEXT NOT NULL,name TEXT NOT NULL,revision_id TEXT NOT NULL REFERENCES app_revisions(id),updated_ms INTEGER NOT NULL,PRIMARY KEY(namespace,name));
CREATE TABLE IF NOT EXISTS calls(id TEXT PRIMARY KEY,revision_id TEXT NOT NULL REFERENCES revisions(id),actor_id TEXT,kind INTEGER NOT NULL,status INTEGER NOT NULL,input_closed INTEGER NOT NULL,request BLOB NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,queued_ms INTEGER,started_ms INTEGER,finished_ms INTEGER,queue_deadline_ms INTEGER,execution_timeout_ms INTEGER NOT NULL,result_ttl_ms INTEGER NOT NULL,result_expiry_ms INTEGER,cancel_reason TEXT,error BLOB,event_seq INTEGER NOT NULL,result_seq INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS calls_queue ON calls(status,queue_deadline_ms,created_ms,id); CREATE INDEX IF NOT EXISTS calls_list ON calls(created_ms,id);
CREATE TABLE IF NOT EXISTS inputs(call_id TEXT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,input_index INTEGER NOT NULL,payload BLOB NOT NULL,status INTEGER NOT NULL,user_attempts INTEGER NOT NULL DEFAULT 0,infra_attempts INTEGER NOT NULL DEFAULT 0,available_ms INTEGER NOT NULL,lease_owner TEXT,lease_expiry_ms INTEGER,lease_generation INTEGER NOT NULL DEFAULT 0,started_ms INTEGER,finished_ms INTEGER,result BLOB,error BLOB,stats BLOB,PRIMARY KEY(call_id,input_index)); CREATE INDEX IF NOT EXISTS inputs_available ON inputs(status,available_ms,call_id,input_index); CREATE INDEX IF NOT EXISTS inputs_lease ON inputs(status,lease_expiry_ms);
CREATE TABLE IF NOT EXISTS events(call_id TEXT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,sequence INTEGER NOT NULL,payload BLOB NOT NULL,event_type INTEGER NOT NULL,created_ms INTEGER NOT NULL,PRIMARY KEY(call_id,sequence));
CREATE TABLE IF NOT EXISTS results(call_id TEXT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,result_index INTEGER NOT NULL,sequence INTEGER NOT NULL,payload BLOB NOT NULL,created_ms INTEGER NOT NULL,PRIMARY KEY(call_id,result_index),UNIQUE(call_id,sequence));
CREATE TABLE IF NOT EXISTS schedules(id TEXT PRIMARY KEY,app_namespace TEXT NOT NULL,app_name TEXT NOT NULL,function_namespace TEXT NOT NULL,function_name TEXT NOT NULL,status INTEGER NOT NULL,record BLOB NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,next_run_ms INTEGER); CREATE INDEX IF NOT EXISTS schedules_due ON schedules(status,next_run_ms,id);
CREATE TABLE IF NOT EXISTS actors(id TEXT PRIMARY KEY,revision_id TEXT NOT NULL REFERENCES revisions(id),status INTEGER NOT NULL,request BLOB NOT NULL,record BLOB NOT NULL,worker_id TEXT,checkpoint_id TEXT,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY,actor_id TEXT NOT NULL REFERENCES actors(id),revision_id TEXT NOT NULL REFERENCES revisions(id),sequence INTEGER NOT NULL,record BLOB NOT NULL,created_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS idempotency(scope TEXT NOT NULL,request_id TEXT NOT NULL,fingerprint BLOB NOT NULL,resource_id TEXT NOT NULL,created_ms INTEGER NOT NULL,PRIMARY KEY(scope,request_id));
CREATE TABLE IF NOT EXISTS artifact_refs(digest TEXT NOT NULL REFERENCES artifacts(digest) ON DELETE CASCADE,owner_kind TEXT NOT NULL,owner_id TEXT NOT NULL,PRIMARY KEY(digest,owner_kind,owner_id));
COMMIT;").map_err(sql_error)?;
	let has_actor_id = {
		let mut statement=connection.prepare("PRAGMA table_info(calls)").map_err(sql_error)?;
		let columns=statement.query_map([],|row|row.get::<_,String>(1)).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;
		columns.iter().any(|column|column=="actor_id")
	};
	if !has_actor_id { connection.execute("ALTER TABLE calls ADD COLUMN actor_id TEXT",[]).map_err(sql_error)?; }
	connection.pragma_update(None,"user_version",SCHEMA_VERSION).map_err(sql_error)?;
	Ok(())
}

fn immediate(c:&mut Connection)->Result<Transaction<'_>>{c.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql_error)}
fn sql_error(e:rusqlite::Error)->EngineError{EngineError::engine(format!("function store: {e}"))}
fn lock_error<T>(_:std::sync::PoisonError<T>)->EngineError{EngineError::engine("function store lock poisoned")}
fn u64_i64(v:u64)->Result<i64>{i64::try_from(v).map_err(|_|EngineError::invalid("numeric value exceeds SQLite range"))}
fn opt_u64_i64(v:Option<u64>)->Result<Option<i64>>{v.map(u64_i64).transpose()}
fn decode_message<M:Message+Default>(bytes:&[u8])->Result<M>{M::decode(bytes).map_err(|e|EngineError::engine(format!("corrupt protobuf in function store: {e}")))}
fn decode_rows<M:Message+Default>(rows:rusqlite::MappedRows<'_,impl FnMut(&rusqlite::Row<'_>)->rusqlite::Result<Vec<u8>>>)->Result<Vec<M>>{let mut out=Vec::new();for row in rows{out.push(decode_message(&row.map_err(sql_error)?)?)}Ok(out)}
fn revision_id(r:&FunctionRevision)->Result<&str>{Ok(&r.r#ref.as_ref().ok_or_else(||EngineError::engine("revision missing ref"))?.revision_id)}
fn revision_by_id(c:&Connection,id:&str)->Result<FunctionRevision>{let blob:Vec<u8>=c.query_row("SELECT record FROM revisions WHERE id=?",[id],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found(format!("revision {id} not found")))?;decode_message(&blob)}
fn revision_by_digest(c:&Connection,d:&str)->Result<Option<FunctionRevision>>{c.query_row("SELECT record FROM revisions WHERE digest=?",[d],|r|r.get::<_,Vec<u8>>(0)).optional().map_err(sql_error)?.map(|b|decode_message(&b)).transpose()}
fn app_revision_by_id(c:&Connection,id:&str)->Result<AppRevision>{let b:Vec<u8>=c.query_row("SELECT record FROM app_revisions WHERE id=?",[id],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("app revision not found"))?;decode_message(&b)}
fn schedule_by_id(c:&Connection,id:&str)->Result<ScheduleRecord>{let b:Vec<u8>=c.query_row("SELECT record FROM schedules WHERE id=?",[id],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("schedule not found"))?;decode_message(&b)}
fn actor_by_id(c:&Connection,id:&str)->Result<ActorRecord>{let b:Vec<u8>=c.query_row("SELECT record FROM actors WHERE id=?",[id],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("actor not found"))?;decode_message(&b)}
fn checkpoint_by_id(c:&Connection,id:&str)->Result<ActorCheckpoint>{let b:Vec<u8>=c.query_row("SELECT record FROM checkpoints WHERE id=?",[id],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("checkpoint not found"))?;decode_message(&b)}
fn idempotent_resource(c:&Connection,scope:&str,request_id:&str,fingerprint:&[u8])->Result<Option<String>>{if request_id.is_empty(){return Ok(None)}let row:Option<(Vec<u8>,String)>=c.query_row("SELECT fingerprint,resource_id FROM idempotency WHERE scope=? AND request_id=?",params![scope,request_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sql_error)?;match row{Some((existing,id)) if existing==fingerprint=>Ok(Some(id)),Some(_)=>Err(EngineError::invalid("request_id was already used for different content")),None=>Ok(None)}}
fn put_idempotency(c:&Connection,scope:&str,request_id:&str,fingerprint:&[u8],resource:&str,now:u64)->Result<()>{if !request_id.is_empty(){c.execute("INSERT INTO idempotency(scope,request_id,fingerprint,resource_id,created_ms) VALUES(?,?,?,?,?)",params![scope,request_id,fingerprint,resource,u64_i64(now)?]).map_err(sql_error)?;}Ok(())}
fn validate_name(v:&str,label:&str)->Result<()>{if v.is_empty()||v.len()>255||v.contains('\0'){Err(EngineError::invalid(format!("invalid {label}")))}else{Ok(())}}
fn validate_id(v:&str,label:&str)->Result<()>{validate_name(v,label)?;if v.contains('/')||v.contains('\\')||v==".."{return Err(EngineError::invalid(format!("invalid {label}")))}Ok(())}
fn validate_function_spec(s:&FunctionSpec)->Result<()>{let f=s.function.as_ref().ok_or_else(||EngineError::invalid("function identity is required"))?;validate_name(&f.namespace,"function namespace")?;validate_name(&f.name,"function name")?;if s.package.is_none()||s.image.is_none()||s.resources.is_none()||s.retry.is_none()||s.timeouts.is_none()||s.workers.is_none()||s.concurrency.is_none()||s.batching.is_none()||s.serializer.is_none(){return Err(EngineError::invalid("function spec is incomplete"))}if s.retry.as_ref().unwrap().max_attempts==0{return Err(EngineError::invalid("max_attempts must be positive"))}if s.secrets.iter().any(|r|r.name.is_empty()){return Err(EngineError::invalid("secret references require names"))}Ok(())}
fn canonical_spec_digest(s:&FunctionSpec)->[u8;32]{let mut base=s.clone();let mut maps=Vec::new();maps.push(sorted_map(&base.labels));base.labels.clear();if let Some(i)=base.image.as_mut(){maps.push(sorted_map(&i.environment));i.environment.clear()}if let Some(r)=base.reproducibility.as_mut(){maps.push(sorted_map(&r.environment));r.environment.clear()}let mut h=Sha256::new();h.update(base.encode_to_vec());for map in maps{for(k,v)in map{h.update((k.len()as u64).to_be_bytes());h.update(k);h.update((v.len()as u64).to_be_bytes());h.update(v)}}h.finalize().into()}
fn sorted_map(map:&std::collections::HashMap<String,String>)->Vec<(String,String)>{let mut v:Vec<_>=map.iter().map(|(k,v)|(k.clone(),v.clone())).collect();v.sort();v}
fn digest_bindings(bindings:&[AppFunctionBinding])->[u8;32]{let mut h=Sha256::new();for b in bindings{h.update((b.name.len()as u64).to_be_bytes());h.update(b.name.as_bytes());if let Some(r)=&b.revision{h.update(r.encode_to_vec())}}h.finalize().into()}
fn check_expected_revision(current:Option<&str>,expected:Option<&RevisionRef>)->Result<()>{if let Some(e)=expected{if current!=Some(e.revision_id.as_str()){return Err(EngineError::busy("active function revision changed"))}}Ok(())}
fn check_expected_app(current:Option<&str>,expected:Option<&AppRevisionRef>)->Result<()>{if let Some(e)=expected{if current!=Some(e.revision_id.as_str()){return Err(EngineError::busy("active app revision changed"))}}Ok(())}
fn assigned_input_id(call_id:&str,index:u64)->String{format!("{call_id}:{index}")}
fn validate_call_request(r:&CreateCallRequest)->Result<()>{
	if CallType::try_from(r.r#type).unwrap_or(CallType::Unspecified)==CallType::Unspecified{return Err(EngineError::invalid("call type is required"))}
	let target=r.target.as_ref().ok_or_else(||EngineError::invalid("call target is required"))?;
	if target.function.as_ref().is_none_or(|f|f.revision_id.is_empty()){return Err(EngineError::invalid("pinned function revision is required"))}
	let policy=ClientCancellationPolicy::try_from(r.client_cancellation).map_err(|_|EngineError::invalid("invalid client cancellation policy"))?;
	let session=r.client_session_id_presence.as_ref().map(|value|match value{create_call_request::ClientSessionIdPresence::ClientSessionId(id)=>id.as_str()});
	if policy==ClientCancellationPolicy::Cancel&&session.is_none_or(str::is_empty){return Err(EngineError::invalid("cancel-on-disconnect calls require a nonempty client session id"));}
	if session.is_some_and(str::is_empty){return Err(EngineError::invalid("client session id must be nonempty"));}
	let mut expected=0;let mut ids=HashSet::new();
	for input in &r.inputs{if input.index!=expected||input.value.is_none(){return Err(EngineError::invalid("initial input indexes must be contiguous and values are required"))}if !input.input_id.is_empty()&&!ids.insert(&input.input_id){return Err(EngineError::invalid("supplied input ids must be unique"))}validate_envelope(input.value.as_ref().unwrap())?;expected+=1}
	Ok(())
}
fn validate_envelope(value:&ValueEnvelope)->Result<()> {
	if value.schema_version==0{return Err(EngineError::invalid("value envelope schema version is required"))}
	if ValueSerializer::try_from(value.serializer).unwrap_or(ValueSerializer::Unspecified)==ValueSerializer::Unspecified{return Err(EngineError::invalid("value serializer is required"))}
	let checksum=value.checksum.as_ref().ok_or_else(||EngineError::invalid("value checksum is required"))?;
	if checksum.algorithm!=DigestAlgorithm::Sha256 as i32||checksum.value.len()!=32{return Err(EngineError::invalid("value checksum must be SHA-256"))}
	match value.storage.as_ref().ok_or_else(||EngineError::invalid("value storage is required"))? {
		value_envelope::Storage::InlineData(bytes) if value.compression==ValueCompression::None as i32 => {if bytes.len() as u64!=value.uncompressed_size_bytes||Sha256::digest(bytes).as_slice()!=checksum.value{return Err(EngineError::invalid("value size or checksum mismatch"));}},
		value_envelope::Storage::InlineData(_) => {},
		value_envelope::Storage::Artifact(reference) => {if artifact_ref_digest(Some(reference)).is_none(){return Err(EngineError::invalid("value artifact requires a SHA-256 digest"));}},
	}
	Ok(())
}

fn insert_input(c:&Connection,call:&str,input:&CallInput,now:u64)->Result<()>{c.execute("INSERT INTO inputs(call_id,input_index,payload,status,available_ms) VALUES(?,?,?,?,?)",params![call,u64_i64(input.index)?,input.encode_to_vec(),INPUT_QUEUED,u64_i64(now)?]).map_err(sql_error)?;Ok(())}
fn call_record(c:&Connection,id:&str)->Result<CallRecord>{let row:(i32,bool,Vec<u8>,u64,u64,Option<Vec<u8>>,u64,u64)=c.query_row("SELECT status,input_closed,request,created_ms,updated_ms,error,(SELECT COUNT(*) FROM inputs WHERE call_id=calls.id),result_seq FROM calls WHERE id=?",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?))).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found(format!("call {id} not found")))?;let req:CreateCallRequest=decode_message(&row.2)?;let error=row.5.map(|b|decode_message(&b)).transpose()?.map(call_record::ErrorPresence::Error);Ok(CallRecord{r#ref:Some(CallRef{call_id:id.into()}),r#type:req.r#type,target:req.target,status:row.0,inputs_closed:row.1,input_count:row.6,result_count:row.7,graph:req.graph,created_at_unix_millis:row.3,updated_at_unix_millis:row.4,error_presence:error,stats:Some(call_stats(c,id)?),labels:req.labels,result_cursor:Some(ResultCursor{call:Some(CallRef{call_id:id.into()}),after_sequence:c.query_row("SELECT result_seq FROM calls WHERE id=?",[id],|r|r.get(0)).map_err(sql_error)?})})}
fn call_stats(c:&Connection,id:&str)->Result<CallStats>{let (created,updated,queued,started):(u64,u64,Option<u64>,Option<u64>)=c.query_row("SELECT created_ms,updated_ms,queued_ms,started_ms FROM calls WHERE id=?",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).map_err(sql_error)?;let mut attempts=Vec::new();let mut s=c.prepare("SELECT stats FROM inputs WHERE call_id=? AND stats IS NOT NULL ORDER BY input_index").map_err(sql_error)?;for row in s.query_map([id],|r|r.get::<_,Vec<u8>>(0)).map_err(sql_error)?{attempts.push(decode_message(&row.map_err(sql_error)?)?)}let queue=started.unwrap_or(updated).saturating_sub(queued.unwrap_or(created));let execution=started.map_or(0,|s|updated.saturating_sub(s));Ok(CallStats{queue_millis:queue,startup_millis:attempts.iter().map(|a:&AttemptStats|a.startup_millis).sum(),execution_millis:execution,wall_millis:updated.saturating_sub(created),cpu_millis:attempts.iter().map(|a|a.cpu_millis).sum(),peak_memory_bytes:attempts.iter().map(|a|a.peak_memory_bytes).max().unwrap_or(0),attempts})}
fn next_event_sequence(c:&Connection,id:&str)->Result<u64>{let seq:u64=c.query_row("UPDATE calls SET event_seq=event_seq+1 WHERE id=? RETURNING event_seq",[id],|r|r.get(0)).map_err(sql_error)?;Ok(seq)}
fn next_result_sequence(c:&Connection,id:&str)->Result<u64>{let seq:u64=c.query_row("UPDATE calls SET result_seq=result_seq+1 WHERE id=? RETURNING result_seq",[id],|r|r.get(0)).map_err(sql_error)?;Ok(seq)}
fn append_event(c:&Connection,id:&str,event_type:CallEventType,payload:call_event::Payload,now:u64)->Result<u64>{let seq=next_event_sequence(c,id)?;let event=CallEvent{call:Some(CallRef{call_id:id.into()}),sequence:seq,created_at_unix_millis:now,r#type:event_type as i32,payload:Some(payload),input_id_presence:None,input_index_presence:None,attempt_presence:None};c.execute("INSERT INTO events(call_id,sequence,payload,event_type,created_ms) VALUES(?,?,?,?,?)",params![id,u64_i64(seq)?,event.encode_to_vec(),event_type as i32,u64_i64(now)?]).map_err(sql_error)?;Ok(seq)}
fn event_by_sequence(c:&Connection,id:&str,sequence:u64)->Result<CallEvent>{let bytes:Vec<u8>=c.query_row("SELECT payload FROM events WHERE call_id=? AND sequence=?",params![id,u64_i64(sequence)?],|row|row.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("call event not found"))?;decode_message(&bytes)}
fn append_status_event(c:&Connection,id:&str,status:CallStatus,now:u64)->Result<u64>{append_event(c,id,CallEventType::Status,call_event::Payload::Status(StatusEvent{status:status as i32}),now)}
fn call_status(c:&Connection,id:&str)->Result<CallStatus>{let raw:i32=c.query_row("SELECT status FROM calls WHERE id=?",[id],|r|r.get(0)).optional().map_err(sql_error)?.ok_or_else(||EngineError::not_found("call not found"))?;CallStatus::try_from(raw).map_err(|_|EngineError::engine("corrupt call status"))}
fn call_kind(c:&Connection,id:&str)->Result<CallType>{let raw:i32=c.query_row("SELECT kind FROM calls WHERE id=?",[id],|r|r.get(0)).map_err(sql_error)?;CallType::try_from(raw).map_err(|_|EngineError::engine("corrupt call type"))}
fn terminal_call(s:CallStatus)->bool{matches!(s,CallStatus::Succeeded|CallStatus::Failed|CallStatus::Cancelled)}
fn require_lease(c:&Connection,l:&LeaseToken,statuses:&[i32],now:u64)->Result<()> {let row:Option<(i32,String,u64,u64)>=c.query_row("SELECT status,COALESCE(lease_owner,''),lease_generation,COALESCE(lease_expiry_ms,0) FROM inputs WHERE call_id=? AND input_index=?",params![l.call_id,u64_i64(l.input_index)?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(sql_error)?;match row{Some((s,owner,generation,expiry))if statuses.contains(&s)&&owner==l.worker_id&&generation==l.lease_generation&&expiry>now=>Ok(()),_=>Err(EngineError::busy("stale or expired input lease"))}}
fn require_executable_call(c:&Connection,id:&str)->Result<()> {match call_status(c,id)?{CallStatus::Queued|CallStatus::Running=>Ok(()),CallStatus::Cancelling|CallStatus::Cancelled=>Err(EngineError::busy("call cancellation is pending")),status if terminal_call(status)=>Err(EngineError::invalid("call is already terminal")),_=>Err(EngineError::invalid("call is not executable"))}}
fn lease_changed(changed:usize)->Result<()>{if changed==1{Ok(())}else{Err(EngineError::busy("stale or expired input lease"))}}
fn retry_error(store:&Store,lease:&LeaseToken,error:&CallError,retry_at:u64,infra:bool,now:u64)->Result<()> {let mut c=store.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;require_lease(&tx,lease,&[INPUT_RUNNING,INPUT_LEASED],now)?;require_executable_call(&tx,&lease.call_id)?;tx.execute("UPDATE inputs SET status=?,available_ms=?,error=?,lease_owner=NULL,lease_expiry_ms=NULL,infra_attempts=infra_attempts+? WHERE call_id=? AND input_index=?",params![INPUT_QUEUED,u64_i64(retry_at)?,error.encode_to_vec(),if infra{1}else{0},lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;let attempt:u32=tx.query_row("SELECT user_attempts FROM inputs WHERE call_id=? AND input_index=?",params![lease.call_id,u64_i64(lease.input_index)?],|r|r.get(0)).map_err(sql_error)?;append_event(&tx,&lease.call_id,CallEventType::Attempt,call_event::Payload::AttemptEvent(AttemptEvent{attempt,status:AttemptStatus::Failed as i32,startup:StartupKind::Unspecified as i32,worker_id:lease.worker_id.clone(),error_presence:Some(attempt_event::ErrorPresence::Error(error.clone()))}),now)?;tx.commit().map_err(sql_error)?;Ok(())}
fn finish_error(store:&Store,lease:&LeaseToken,error:&CallError,retry:Option<u64>,now:u64)->Result<()>{let mut c=store.connection.lock().map_err(lock_error)?;let tx=immediate(&mut c)?;require_lease(&tx,lease,&[INPUT_RUNNING,INPUT_LEASED],now)?;require_executable_call(&tx,&lease.call_id)?;if let Some(at)=retry{tx.execute("UPDATE inputs SET status=?,available_ms=?,error=?,lease_owner=NULL,lease_expiry_ms=NULL WHERE call_id=? AND input_index=?",params![INPUT_QUEUED,u64_i64(at)?,error.encode_to_vec(),lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;}else{tx.execute("UPDATE inputs SET status=?,finished_ms=?,error=?,lease_owner=NULL,lease_expiry_ms=NULL WHERE call_id=? AND input_index=?",params![INPUT_FAILED,u64_i64(now)?,error.encode_to_vec(),lease.call_id,u64_i64(lease.input_index)?]).map_err(sql_error)?;tx.execute("UPDATE calls SET status=?,error=?,finished_ms=?,updated_ms=?,result_expiry_ms=CASE WHEN result_ttl_ms=0 THEN NULL ELSE ?+result_ttl_ms END WHERE id=? AND status NOT IN (?,?,?)",params![CallStatus::Failed as i32,error.encode_to_vec(),u64_i64(now)?,u64_i64(now)?,u64_i64(now)?,lease.call_id,CallStatus::Succeeded as i32,CallStatus::Failed as i32,CallStatus::Cancelled as i32]).map_err(sql_error)?;append_event(&tx,&lease.call_id,CallEventType::Error,call_event::Payload::Error(error.clone()),now)?;append_status_event(&tx,&lease.call_id,CallStatus::Failed,now)?;}tx.commit().map_err(sql_error)?;Ok(())}
fn finalize_cancelled_tx(c:&Connection,id:&str,now:u64)->Result<()> {if call_status(c,id)?==CallStatus::Cancelled{return Ok(())}c.execute("UPDATE calls SET status=?,finished_ms=?,updated_ms=?,result_expiry_ms=CASE WHEN result_ttl_ms=0 THEN NULL ELSE ?+result_ttl_ms END WHERE id=?",params![CallStatus::Cancelled as i32,u64_i64(now)?,u64_i64(now)?,u64_i64(now)?,id]).map_err(sql_error)?;append_status_event(c,id,CallStatus::Cancelled,now)?;Ok(())}
fn complete_call_tx(c:&Connection,id:&str,now:u64)->Result<bool>{let status=call_status(c,id)?;if terminal_call(status)||status==CallStatus::Cancelling{return Ok(false)}let (closed,pending,failed):(bool,u64,u64)=c.query_row("SELECT input_closed,SUM(CASE WHEN i.status IN (1,2,3) THEN 1 ELSE 0 END),SUM(CASE WHEN i.status=5 THEN 1 ELSE 0 END) FROM calls LEFT JOIN inputs i ON i.call_id=calls.id WHERE calls.id=? GROUP BY calls.id",[id],|r|Ok((r.get(0)?,r.get::<_,Option<u64>>(1)?.unwrap_or(0),r.get::<_,Option<u64>>(2)?.unwrap_or(0)))).map_err(sql_error)?;if !closed||pending>0{return Ok(false)}let target=if failed>0{CallStatus::Failed}else{CallStatus::Succeeded};c.execute("UPDATE calls SET status=?,finished_ms=?,updated_ms=?,result_expiry_ms=CASE WHEN result_ttl_ms=0 THEN NULL ELSE ?+result_ttl_ms END WHERE id=?",params![target as i32,u64_i64(now)?,u64_i64(now)?,u64_i64(now)?,id]).map_err(sql_error)?;append_status_event(c,id,target,now)?;Ok(true)}
fn expire_deadlines_tx(c:&Connection,now:u64)->Result<()> {let mut s=c.prepare("SELECT id FROM calls WHERE status IN (?,?) AND queue_deadline_ms IS NOT NULL AND queue_deadline_ms<=?").map_err(sql_error)?;let ids=s.query_map(params![CallStatus::Queued as i32,CallStatus::Pending as i32,u64_i64(now)?],|r|r.get::<_,String>(0)).map_err(sql_error)?.collect::<std::result::Result<Vec<_>,_>>().map_err(sql_error)?;drop(s);for id in ids{let error=CallError{code:"queue_deadline_exceeded".into(),message:"call exceeded its queue deadline".into(),r#type:"QueueDeadlineExceeded".into(),retryable:false,frames:vec![],cause_presence:None,details:Default::default()};c.execute("UPDATE calls SET status=?,error=?,finished_ms=?,updated_ms=? WHERE id=?",params![CallStatus::Failed as i32,error.encode_to_vec(),u64_i64(now)?,u64_i64(now)?,id]).map_err(sql_error)?;c.execute("UPDATE inputs SET status=?,finished_ms=? WHERE call_id=? AND status=?",params![INPUT_FAILED,u64_i64(now)?,id,INPUT_QUEUED]).map_err(sql_error)?;append_event(c,&id,CallEventType::Error,call_event::Payload::Error(error),now)?;append_status_event(c,&id,CallStatus::Failed,now)?;}Ok(())}
fn validate_schedule(s:&ScheduleSpec)->Result<()>{validate_name(&s.name,"schedule name")?;if s.app.is_none()||s.target.as_ref().is_none_or(|t|t.function.is_none()||t.input.is_none())||s.timing.is_none(){return Err(EngineError::invalid("schedule is incomplete"))}if let Some(schedule_spec::Timing::Period(p))=&s.timing{if p.period_millis==0{return Err(EngineError::invalid("schedule period must be positive"))}}if ScheduleStatus::try_from(s.status).unwrap_or(ScheduleStatus::Unspecified)==ScheduleStatus::Unspecified{return Err(EngineError::invalid("schedule status is required"))}Ok(())}
fn normalized_page_size(v:u32)->u32{if v==0{100}else{v.min(1000)}}
fn decode_page_token(t:&str)->Result<(u64,String)>{if t.is_empty(){return Ok((0,String::new()))}let Some((a,b))=t.split_once(':')else{return Err(EngineError::invalid("invalid page token"))};Ok((a.parse().map_err(|_|EngineError::invalid("invalid page token"))?,b.into()))}
fn page_from_rows<T,I,F>(rows:I,limit:u32,decode:F)->Result<Page<T>>where I:Iterator<Item=rusqlite::Result<(Vec<u8>,i64,String)>>,F:Fn(&[u8])->Result<T>{let mut values=Vec::new();for row in rows{values.push(row.map_err(sql_error)?)}let more=values.len()>limit as usize;if more{values.pop();}let token=if more{values.last().map(|(_,ms,id)|format!("{ms}:{id}")).unwrap_or_default()}else{String::new()};let mut items=Vec::new();for(b,_,_)in values{items.push(decode(&b)?)}Ok(Page{items,next_page_token:token})}
fn validate_digest_hex(d:&str)->Result<()>{if d.len()!=64||!d.bytes().all(|b|b.is_ascii_hexdigit()&&(!b.is_ascii_alphabetic()||b.is_ascii_lowercase())){Err(EngineError::invalid("invalid SHA-256 digest"))}else{Ok(())}}
fn add_artifact_ref(c:&Connection,digest:&str,kind:&str,id:&str)->Result<()>{validate_digest_hex(digest)?;let exists:bool=c.query_row("SELECT EXISTS(SELECT 1 FROM artifacts WHERE digest=?)",[digest],|r|r.get(0)).map_err(sql_error)?;if !exists{return Err(EngineError::invalid(format!("referenced artifact {digest} is not registered")))}c.execute("INSERT OR IGNORE INTO artifact_refs(digest,owner_kind,owner_id) VALUES(?,?,?)",params![digest,kind,id]).map_err(sql_error)?;Ok(())}
fn digest_hex(d:Option<&Digest>)->Option<String>{let d=d?;(d.algorithm==DigestAlgorithm::Sha256 as i32&&d.value.len()==32).then(||hex::encode(&d.value))}
fn artifact_ref_digest(a:Option<&ArtifactRef>)->Option<String>{digest_hex(a.and_then(|a|a.digest.as_ref()))}
fn envelope_artifacts(e:Option<&ValueEnvelope>)->Vec<String>{let mut v=Vec::new();if let Some(ValueEnvelope{storage:Some(value_envelope::Storage::Artifact(a)),..})=e{if let Some(d)=artifact_ref_digest(Some(a)){v.push(d)}}v}
fn result_artifacts(r:&CallResult)->Vec<String>{match &r.outcome{Some(call_result::Outcome::Value(v))=>envelope_artifacts(Some(v)),_=>Vec::new()}}
fn function_spec_artifacts(s:&FunctionSpec)->HashSet<String>{let mut out=HashSet::new();if let Some(p)=&s.package{if let Some(d)=artifact_ref_digest(p.source.as_ref()){out.insert(d);}if let Some(package_spec::LockfilePresence::Lockfile(a))=&p.lockfile_presence{if let Some(d)=artifact_ref_digest(Some(a)){out.insert(d);}}}if let Some(i)=&s.image{if let Some(image_spec::Source::Dockerfile(d))=&i.source{if let Some(v)=artifact_ref_digest(d.context.as_ref()){out.insert(v);}}for mount in &i.local_artifact_mounts{if let Some(v)=artifact_ref_digest(mount.artifact.as_ref()){out.insert(v);}}}if let Some(d)=s.snapshot_provenance_placeholder(){out.insert(d);}out}
trait SnapshotPlaceholder{fn snapshot_provenance_placeholder(&self)->Option<String>;}impl SnapshotPlaceholder for FunctionSpec{fn snapshot_provenance_placeholder(&self)->Option<String>{None}}


#[cfg(test)]
mod tests {
	use super::*;
	use std::{fs, sync::{Arc, Barrier}, thread};

	fn spec() -> FunctionSpec {
		FunctionSpec {
			function: Some(FunctionRef { namespace:"test".into(),name:"echo".into() }),
			package:Some(PackageSpec::default()), image:Some(ImageSpec::default()), resources:Some(ResourceSpec::default()),
			retry:Some(RetryPolicy { max_attempts:3,..Default::default() }),
			timeouts:Some(TimeoutSpec { execution_millis:1_000,queue_millis:10_000,result_ttl_millis:100,..Default::default() }),
			workers:Some(WorkerSpec::default()),concurrency:Some(ConcurrencySpec::default()),batching:Some(BatchingSpec::default()),serializer:Some(SerializerSpec::default()),
			secrets:vec![SecretRef{name:"token".into(),version_presence:None}],..Default::default()
		}
	}
	fn revision_ref(revision:&FunctionRevision)->RevisionRef { revision.r#ref.clone().unwrap() }
	fn envelope(bytes:&[u8])->ValueEnvelope { ValueEnvelope { schema_version:1,serializer:ValueSerializer::Json as i32,compression:ValueCompression::None as i32,checksum:Some(Digest{algorithm:DigestAlgorithm::Sha256 as i32,value:Sha256::digest(bytes).to_vec()}),uncompressed_size_bytes:bytes.len() as u64,storage:Some(value_envelope::Storage::InlineData(bytes.to_vec())),python_presence:None,type_name_presence:None } }
	fn call_request(revision:&FunctionRevision,request_id:&str)->CreateCallRequest { CreateCallRequest { r#type:CallType::Unary as i32,target:Some(CallTarget{function:Some(revision_ref(revision)),actor_presence:None,actor_method_presence:None}),inputs:vec![CallInput{index:0,value:Some(envelope(b"1")),input_id:"input-0".into()}],inputs_closed:true,graph:None,request_id:request_id.into(),labels:Default::default(),client_cancellation:ClientCancellationPolicy::Detach as i32,result_ttl_millis_presence:None,client_session_id_presence:None } }
	fn setup() -> (tempfile::TempDir,Home,Store,FunctionRevision) { let temp=tempfile::tempdir().unwrap();let home=Home::new(temp.path());let store=Store::open(&home).unwrap();let revision=store.register_function(&spec(),"register-1",10).unwrap();(temp,home,store,revision) }

	#[test]
	fn reopen_deduplicates_and_recovers_running_work() {
		let (_temp,home,store,revision)=setup();
		let again=store.register_function(&spec(),"register-2",11).unwrap();
		assert_eq!(revision_ref(&revision).revision_id,revision_ref(&again).revision_id);
		let call=store.create_call(&call_request(&revision,"call-1"),20).unwrap();
		let duplicate=store.create_call(&call_request(&revision,"call-1"),21).unwrap();
		assert_eq!(call.r#ref,duplicate.r#ref);
		let lease=store.lease_next("worker-a",22,100).unwrap().unwrap();
		store.mark_running(&lease.lease,23,StartupKind::Cold).unwrap();
		drop(store);
		let reopened=Store::open(&home).unwrap();
		assert_eq!(reopened.recover_startup(30).unwrap().requeued_inputs,1);
		assert!(reopened.lease_next("worker-b",31,100).unwrap().is_some());
	}

	#[test]
	fn competing_leasers_have_one_winner_and_terminal_state_is_monotonic() {
		let (_temp,home,store,revision)=setup();store.create_call(&call_request(&revision,"race"),20).unwrap();drop(store);
		let barrier=Arc::new(Barrier::new(3));let mut threads=Vec::new();
		for worker in ["one","two"] {let home=home.clone();let barrier=barrier.clone();threads.push(thread::spawn(move||{let store=Store::open(&home).unwrap();barrier.wait();store.lease_next(worker,21,1_000).unwrap()}));}
		barrier.wait();let leases:Vec<_>=threads.into_iter().map(|t|t.join().unwrap()).flatten().collect();assert_eq!(leases.len(),1);
		let store=Store::open(&home).unwrap();let leased=&leases[0];store.mark_running(&leased.lease,22,StartupKind::Warm).unwrap();
		let result=CallResult{call:None,index:0,created_at_unix_millis:0,sequence:0,input_id:String::new(),input_index:0,outcome:Some(call_result::Outcome::Value(envelope(b"2"))),yield_index_presence:None};
		store.succeed(&leased.lease,&result,None,23).unwrap();
		let error=CallError{code:"late".into(),message:"late".into(),..Default::default()};assert!(store.fail_user(&leased.lease,&error,24).is_err());
		assert_eq!(store.get_call(&leased.lease.call_id).unwrap().status,CallStatus::Succeeded as i32);
		let events=store.list_events(&leased.lease.call_id,1,100).unwrap();assert!(events.windows(2).all(|w|w[0].sequence<w[1].sequence));
		let results=store.list_results(&leased.lease.call_id,0,100).unwrap();assert_eq!(results.len(),1);assert!(store.list_results(&leased.lease.call_id,results[0].sequence,100).unwrap().is_empty());
		store.create_call(&call_request(&revision,"cancel-race"),30).unwrap();let cancelling=store.lease_next("cancel-worker",31,1_000).unwrap().unwrap();store.mark_running(&cancelling.lease,32,StartupKind::Warm).unwrap();let record=store.cancel_call(&cancelling.lease.call_id,"stop","cancel-request",33).unwrap();assert_eq!(record.status,CallStatus::Cancelling as i32);assert!(store.succeed(&cancelling.lease,&result,None,34).is_err());assert_eq!(store.finish_cancelled(&cancelling.lease,"stop",35).unwrap().status,CallStatus::Cancelled as i32);
	}

	#[test]
	fn assigns_and_replays_empty_input_ids_stably() {
		let (_temp,home,store,revision)=setup();let mut request=call_request(&revision,"assigned-inputs");request.r#type=CallType::Batch as i32;request.inputs_closed=false;request.inputs[0].input_id.clear();
		let call=store.create_call(&request,20).unwrap();let call_id=call.r#ref.unwrap().call_id;assert_eq!(store.client_cancellation(&call_id).unwrap(),(ClientCancellationPolicy::Detach,None));let first=store.get_input(&call_id,0).unwrap();assert_eq!(first.input_id,format!("{call_id}:0"));
		let appended=CallInput{index:1,value:Some(envelope(b"2")),input_id:String::new()};assert_eq!(store.append_input(&call_id,&appended,21).unwrap(),2);assert_eq!(store.append_input(&call_id,&appended,22).unwrap(),2);let second=store.get_input(&call_id,1).unwrap();assert_eq!(second.input_id,format!("{call_id}:1"));drop(store);
		let reopened=Store::open(&home).unwrap();assert_eq!(reopened.get_input(&call_id,0).unwrap().input_id,first.input_id);assert_eq!(reopened.get_input(&call_id,1).unwrap().input_id,second.input_id);
	}

	#[test]
	fn cancellation_capability_is_required_persisted_and_corruption_checked() {
		let (_temp,home,store,revision)=setup();let mut request=call_request(&revision,"cancel-capability");request.client_cancellation=ClientCancellationPolicy::Cancel as i32;
		assert!(store.create_call(&request,20).is_err());request.client_session_id_presence=Some(create_call_request::ClientSessionIdPresence::ClientSessionId("creator-session".into()));
		let call_id=store.create_call(&request,21).unwrap().r#ref.unwrap().call_id;assert_eq!(store.client_cancellation(&call_id).unwrap(),(ClientCancellationPolicy::Cancel,Some("creator-session".into())));drop(store);
		let reopened=Store::open(&home).unwrap();assert_eq!(reopened.client_cancellation(&call_id).unwrap(),(ClientCancellationPolicy::Cancel,Some("creator-session".into())));
		let connection=reopened.connection.lock().unwrap();let bytes:Vec<u8>=connection.query_row("SELECT request FROM calls WHERE id=?",[&call_id],|row|row.get(0)).unwrap();let mut corrupt:CreateCallRequest=decode_message(&bytes).unwrap();corrupt.client_cancellation=999;connection.execute("UPDATE calls SET request=? WHERE id=?",params![corrupt.encode_to_vec(),call_id]).unwrap();drop(connection);assert!(reopened.client_cancellation(&call_id).is_err());
	}

	#[test]
	fn app_activation_and_rollback_are_atomic_across_reopen() {
		let (_temp,home,store,revision)=setup();store.activate_function(&revision_ref(&revision),None,20).unwrap();
		let app=AppRef{namespace:"test".into(),name:"app".into()};
		let first=store.activate_app(&ActivateAppRequest{app:Some(app.clone()),functions:vec![AppFunctionBinding{name:"echo".into(),revision:Some(revision_ref(&revision))}],expected_current_presence:None,request_id:"app-1".into()},30).unwrap();
		let second=store.activate_app(&ActivateAppRequest{app:Some(app.clone()),functions:vec![],expected_current_presence:Some(activate_app_request::ExpectedCurrentPresence::ExpectedCurrent(first.r#ref.clone().unwrap())),request_id:"app-2".into()},31).unwrap();
		store.rollback_app(&RollbackAppRequest{target:first.r#ref.clone(),expected_current_presence:Some(rollback_app_request::ExpectedCurrentPresence::ExpectedCurrent(second.r#ref.clone().unwrap())),request_id:"rollback".into()},32).unwrap();drop(store);
		assert_eq!(Store::open(&home).unwrap().get_active_app(&app).unwrap().r#ref,first.r#ref);
	}

	#[test]
	fn schedules_actors_checkpoints_and_reference_aware_gc_persist() {
		let (_temp,home,store,revision)=setup();
		let actor=store.create_actor(&CreateActorRequest{function:Some(revision_ref(&revision)),initial_state_presence:Some(create_actor_request::InitialStatePresence::InitialState(envelope(b"actor"))),request_id:"actor".into(),labels:Default::default()},20).unwrap();
		let actor_ref=actor.r#ref.clone().unwrap();let checkpoint=store.put_checkpoint(&ActorCheckpoint{r#ref:None,actor:Some(actor_ref.clone()),function:Some(revision_ref(&revision)),state:Some(envelope(b"state")),sequence:7,created_at_unix_millis:0},"checkpoint",21).unwrap();
		assert_eq!(store.get_checkpoint(&checkpoint.r#ref.as_ref().unwrap().checkpoint_id).unwrap().sequence,7);
		let app=AppRef{namespace:"test".into(),name:"app".into()};let app_revision=store.activate_app(&ActivateAppRequest{app:Some(app),functions:vec![],expected_current_presence:None,request_id:"app".into()},22).unwrap();
		let schedule=store.put_schedule(&CreateScheduleRequest{schedule_id_presence:Some(create_schedule_request::ScheduleIdPresence::ScheduleId("daily".into())),spec:Some(ScheduleSpec{name:"daily".into(),app:app_revision.r#ref.clone(),target:Some(ScheduleTarget{function:Some(revision_ref(&revision)),input:Some(envelope(b"scheduled"))}),timing:Some(schedule_spec::Timing::Period(PeriodSchedule{period_millis:100,anchor_unix_millis:50})),status:ScheduleStatus::Active as i32,labels:Default::default()}),request_id:"schedule".into()},Some(50),23).unwrap();
		assert_eq!(next_schedule_run(&schedule,50).unwrap(),Some(150));
		store.set_actor_status(&actor_ref.actor_id,ActorStatus::Ready,Some("worker-checkpointed"),24).unwrap();
		let lost_actor=store.create_actor(&CreateActorRequest{function:Some(revision_ref(&revision)),initial_state_presence:None,request_id:"lost-actor".into(),labels:Default::default()},25).unwrap();let lost_id=lost_actor.r#ref.unwrap().actor_id;store.set_actor_status(&lost_id,ActorStatus::Ready,Some("worker-lost"),26).unwrap();drop(store);
		let reopened=Store::open(&home).unwrap();let recovery=reopened.recover_startup(27).unwrap();assert_eq!(recovery.lost_actors,1);assert_eq!(reopened.get_actor(&actor_ref.actor_id).unwrap().status,ActorStatus::Stopped as i32);assert_eq!(reopened.get_actor(&lost_id).unwrap().status,ActorStatus::Failed as i32);assert_eq!(reopened.get_actor(&actor_ref.actor_id).unwrap().latest_checkpoint_presence.is_some(),true);assert_eq!(reopened.get_schedule("daily").unwrap().r#ref.unwrap().schedule_id,"daily");
		let artifacts=super::super::artifact::ArtifactStore::open(home.function_artifacts_dir()).unwrap();let kept=artifacts.put(b"kept").unwrap();let gone=artifacts.put(b"gone").unwrap();reopened.record_artifact(&kept.digest,kept.size,None,kept.path.to_str().unwrap(),1,Some(2)).unwrap();reopened.record_artifact(&gone.digest,gone.size,None,gone.path.to_str().unwrap(),1,Some(2)).unwrap();assert!(reopened.record_artifact(&gone.digest,gone.size+1,None,gone.path.to_str().unwrap(),1,Some(2)).is_err());
		let artifact_value=ValueEnvelope{storage:Some(value_envelope::Storage::Artifact(ArtifactRef{digest:Some(Digest{algorithm:DigestAlgorithm::Sha256 as i32,value:hex::decode(&kept.digest).unwrap()})})),uncompressed_size_bytes:kept.size,checksum:Some(Digest{algorithm:DigestAlgorithm::Sha256 as i32,value:hex::decode(&kept.digest).unwrap()}),schema_version:1,serializer:ValueSerializer::Json as i32,compression:ValueCompression::None as i32,python_presence:None,type_name_presence:None};
		let mut request=call_request(&revision,"artifact-call");request.inputs[0].value=Some(artifact_value);reopened.create_call(&request,30).unwrap();let expired=reopened.unreferenced_expired_artifacts(3,100).unwrap();assert_eq!(expired,vec![(gone.digest.clone(),gone.path.to_string_lossy().into_owned())]);assert_eq!(artifacts.gc_expired(&reopened,3,100).unwrap(),1);assert!(artifacts.read(&gone.digest,None).is_err());assert!(artifacts.read(&kept.digest,Some(kept.size)).is_ok());
	}

	#[test]
	fn rejects_corrupt_values_and_never_persists_transient_secret_material() {
		let temp=tempfile::tempdir().unwrap();let home=Home::new(temp.path());let store=Store::open(&home).unwrap();let marker=b"TRANSIENT-SUPER-SECRET-92017".to_vec();
		let request=RegisterFunctionRequest{spec:Some(spec()),request_id:"secret".into(),transient_secrets:vec![TransientSecretMaterial{secret:Some(SecretRef{name:"token".into(),version_presence:None}),value:marker.clone()}]};
		store.register_function(request.spec.as_ref().unwrap(),&request.request_id,1).unwrap();let revision=store.get_active_revision(&FunctionRef{namespace:"test".into(),name:"echo".into()}).err();assert!(revision.is_some());
		let registered=store.get_revision(&store.register_function(&spec(),"again",2).unwrap().r#ref.unwrap().revision_id).unwrap();let mut call=call_request(&registered,"bad");call.inputs[0].value.as_mut().unwrap().uncompressed_size_bytes=99;assert!(store.create_call(&call,3).is_err());drop(store);
		for path in [home.functions_db(),home.functions_db().with_extension("sqlite3-wal"),home.functions_db().with_extension("sqlite3-shm")] {if let Ok(bytes)=fs::read(path){assert!(!bytes.windows(marker.len()).any(|window|window==marker));}}
		if let Ok(entries)=fs::read_dir(home.function_artifacts_dir()){for entry in entries.flatten(){if entry.path().is_file(){let bytes=fs::read(entry.path()).unwrap();assert!(!bytes.windows(marker.len()).any(|window|window==marker));}}}
	}
}
