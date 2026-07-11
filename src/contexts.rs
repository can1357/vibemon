use std::{
	collections::BTreeMap,
	fs,
	os::unix::fs::PermissionsExt,
	path::PathBuf,
	time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
	error::{CliError, Result, err},
	transport::ApiClient,
};

pub const LOCAL: &str = "local";

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Context {
	pub name:      String,
	#[serde(default)]
	pub endpoints: Vec<String>,
	#[serde(default)]
	pub region:    String,
	#[serde(default)]
	pub updated:   f64,
}

#[derive(Default, Deserialize, Serialize)]
struct ContextFile {
	current:  Option<String>,
	#[serde(default)]
	contexts: BTreeMap<String, Context>,
}

#[derive(Debug)]
pub struct ContextStore {
	path:     PathBuf,
	current:  Option<String>,
	contexts: BTreeMap<String, Context>,
}

impl ContextStore {
	pub fn load_default() -> Self {
		let path = contexts_path();
		let mut store = Self { path, current: None, contexts: BTreeMap::new() };
		store.load();
		store
	}

	pub fn list(&self) -> impl Iterator<Item = &Context> {
		self.contexts.values()
	}

	pub fn get(&self, name: &str) -> Option<&Context> {
		self.contexts.get(name)
	}

	pub fn current_name(&self) -> Option<String> {
		std::env::var("VMON_CONTEXT")
			.ok()
			.filter(|value| !value.trim().is_empty())
			.or_else(|| self.current.clone())
	}

	pub fn current(&self) -> Option<&Context> {
		let name = self.current_name()?;
		if name == LOCAL {
			return None;
		}
		self.get(&name)
	}

	pub fn use_context(&mut self, name: &str) -> Result<()> {
		if name != LOCAL && !self.contexts.contains_key(name) {
			return err(format!("no such context {name:?}"));
		}
		self.current = Some(name.to_owned());
		self.save()
	}

	pub fn put(&mut self, context: Context) -> Result<()> {
		require_safe_name(&context.name)?;
		self.contexts.insert(context.name.clone(), context);
		self.save()
	}

	pub fn remove(&mut self, name: &str) -> Result<()> {
		self.contexts.remove(name);
		let _ = fs::remove_file(self.token_path(name)?);
		if self.current.as_deref() == Some(name) {
			self.current = None;
		}
		self.save()
	}

	pub fn has_token(&self, name: &str) -> bool {
		self.load_token(name).ok().flatten().is_some()
	}

	pub fn resolve_token(&self, name: &str, explicit: Option<&str>) -> Result<Option<String>> {
		Ok(explicit
			.map(ToOwned::to_owned)
			.or_else(|| std::env::var("VMON_API_TOKEN").ok())
			.or(self.load_token(name)?))
	}

	pub fn save_token(&self, name: &str, token: &str) -> Result<()> {
		let path = self.token_path(name)?;
		if let Some(parent) = path.parent() {
			fs::create_dir_all(parent)?;
			fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
		}
		let tmp = path.with_file_name(format!(".{name}.token.tmp"));
		fs::write(&tmp, format!("{token}\n"))?;
		fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
		fs::rename(tmp, &path)?;
		fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
		Ok(())
	}

	fn load(&mut self) {
		let Ok(text) = fs::read_to_string(&self.path) else {
			return;
		};
		let Ok(file) = serde_json::from_str::<ContextFile>(&text) else {
			return;
		};
		self.current = file.current.filter(|value| !value.is_empty());
		self.contexts = file
			.contexts
			.into_iter()
			.filter_map(|(key, mut context)| {
				if context.name.is_empty() {
					context.name = key;
				}
				(!context.name.is_empty()).then_some((context.name.clone(), context))
			})
			.collect();
	}

	fn save(&self) -> Result<()> {
		if let Some(parent) = self.path.parent() {
			fs::create_dir_all(parent)?;
		}
		let tmp = self.path.with_extension("json.tmp");
		let file = ContextFile { current: self.current.clone(), contexts: self.contexts.clone() };
		fs::write(&tmp, serde_json::to_vec_pretty(&file)?)?;
		fs::rename(tmp, &self.path)?;
		Ok(())
	}

	fn token_path(&self, name: &str) -> Result<PathBuf> {
		require_safe_name(name)?;
		Ok(self
			.path
			.parent()
			.unwrap_or_else(|| std::path::Path::new("."))
			.join("credentials")
			.join(format!("{name}.token")))
	}

	fn load_token(&self, name: &str) -> Result<Option<String>> {
		let path = self.token_path(name)?;
		let Ok(text) = fs::read_to_string(path) else {
			return Ok(None);
		};
		let token = text.trim().to_owned();
		Ok((!token.is_empty()).then_some(token))
	}
}

pub fn contexts_path() -> PathBuf {
	vmond::home::state_dir().join("contexts.json")
}

pub fn normalize_server(server: &str) -> String {
	let trimmed = server.trim().trim_end_matches('/');
	if trimmed.contains("://") {
		trimmed.to_owned()
	} else {
		format!("http://{trimmed}")
	}
}

pub fn now_secs() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_secs_f64()
}

pub fn roster_from_status(status: &Value, fallback: &str) -> Vec<String> {
	let mut roster = Vec::new();
	let mut push = |node: &Value| {
		if let Some(advertise) = node.get("advertise").and_then(Value::as_str)
			&& !advertise.is_empty()
			&& !roster.iter().any(|value| value == advertise)
		{
			roster.push(advertise.to_owned());
		}
	};
	if let Some(this_node) = status.get("self") {
		push(this_node);
	}
	if let Some(peers) = status.get("peers").and_then(Value::as_array) {
		for peer in peers {
			push(peer);
		}
	}
	if roster.is_empty() {
		roster.push(fallback.to_owned());
	}
	roster
}

pub fn remote_client_from_context(context: &Context, token: Option<String>) -> Result<ApiClient> {
	if token.as_deref().is_none_or(str::is_empty) {
		return err("VMON_API_TOKEN or a saved context token is required for remote contexts");
	}
	ApiClient::remote(context.endpoints.clone(), token)
}

fn require_safe_name(name: &str) -> Result<()> {
	let mut chars = name.chars();
	let Some(first) = chars.next() else {
		return err("context name cannot be empty");
	};
	if !first.is_ascii_alphanumeric() {
		return err(format!("unsafe context name {name:?}"));
	}
	if chars.all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')) {
		Ok(())
	} else {
		Err(CliError::new(format!("unsafe context name {name:?}")))
	}
}
