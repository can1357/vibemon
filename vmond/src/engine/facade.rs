//! The real [`Engine`]: method-for-method port of `python/vmon/core.py`.

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fmt::Write as _,
	fs,
	io::{self, Read, Seek, SeekFrom, Write as _},
	net::IpAddr,
	os::unix::fs::OpenOptionsExt,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use flume::{Receiver, Sender};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::runtime::Runtime;
use vmm::snapshot::is_safe_snapshot_name;

use crate::{
	config::{ServeConfig, WarmImage},
	engine::{
		EngineApi, ExecCapture, ExecExit, ExecRequest, ExecStream, ShellSession,
		agent::{AgentConn, ExecHandle},
		control::ControlClient,
		diskdelta,
		s3proxy::S3Proxy,
		spawn::{LaunchSpec, RemoteFsShare, SandboxRuntime, SandboxVm, VmonRuntime, VolumeMount},
	},
	error::{EngineError, Result},
	home::Home,
	image::{self, CachedTemplate, TemplateBooter, TemplateRequest, TemplateSpec},
	models::{
		ForkBody, MAX_FORK_CLONES, NetworkBody, PoolPutBody, RestoreBody, S3MountSpec, SandboxCreate,
	},
	net::{self, SandboxNetwork},
	pools::{PoolRegistry, WarmPool, template_key},
	registry::{Registry, VmRecord},
	s3::{S3Auth, S3Client, S3Credentials, S3MountConfig, parse_s3_uri},
	volumes::{self, Secret, Volume, VolumeLock},
};

const DEFAULT_SHELL_IMAGE: &str = "debian:stable-slim";
const DEFAULT_SHELL_ARGV: [&str; 3] =
	["/bin/sh", "-c", "command -v bash >/dev/null 2>&1 && exec bash -i || exec sh -i"];
const DEFAULT_CREATE_TIMEOUT_SECS: u64 = 300;
const AGENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const LOG_FOLLOW_POLL: Duration = Duration::from_millis(100);
const EXEC_CAPTURE_CAP: Duration = Duration::from_mins(1);
const WARM_VOLUME_SLOTS: u64 = 8;
const ALLOWED_HA: [&str; 4] = ["async", "async+rerun", "off", "rerun"];
const S3_MOUNTS_FILE: &str = "s3-mounts.json";
const MAX_S3_MOUNTS: usize = 8;
const MAX_SNAPSHOT_METADATA_BYTES: u64 = 64 * 1024;

/// Single owner of the microVM registry and all VM lifecycle logic.
#[derive(Clone)]
pub struct Engine {
	inner: Arc<EngineInner>,
}

struct EngineInner {
	config:          ServeConfig,
	home:            Home,
	registry:        Registry,
	runtimes:        Mutex<HashMap<String, RuntimeState>>,
	pools:           PoolRegistry,
	events:          Mutex<Vec<Sender<Value>>>,
	event_sequence:  AtomicU64,
	counters:        Counters,
	latency:         Mutex<CreateLatency>,
	net_runtime:     Runtime,
	sandbox_runtime: Arc<dyn SandboxRuntime>,
}

#[derive(Default)]
struct RuntimeState {
	agent:          Option<AgentConn>,
	network:        Option<SandboxNetwork>,
	volume_locks:   Vec<VolumeLock>,
	secret_env:     BTreeMap<String, String>,
	env:            BTreeMap<String, String>,
	workdir:        Option<String>,
	connect_token:  Option<String>,
	network_policy: NetworkPolicy,
	network_spec:   Option<Value>,
	timeout_stop:   Option<Sender<()>>,
	s3_proxy:       Option<S3Proxy>,
}

#[derive(Default, Clone)]
struct NetworkPolicy {
	block_network:          Option<bool>,
	egress_allow:           Option<Vec<String>>,
	egress_allow_domains:   Option<Vec<String>>,
	inbound_cidr_allowlist: Option<Vec<String>>,
}

#[derive(Default)]
struct Counters {
	created:     AtomicU64,
	terminated:  AtomicU64,
	idle_reaped: AtomicU64,
	exec:        AtomicU64,
	file_read:   AtomicU64,
	file_write:  AtomicU64,
	file_delete: AtomicU64,
	snapshot:    AtomicU64,
	auth_failed: AtomicU64,
}

#[derive(Default)]
struct CreateLatency {
	sum_ms: f64,
	count:  u64,
}

struct EngineExecControl {
	handle: ExecHandle,
}

struct CreatePlan {
	params:               SandboxCreate,
	sid:                  String,
	ha:                   String,
	restart_policy:       String,
	tags:                 HashMap<String, String>,
	secrets:              Vec<Secret>,
	secret_env:           BTreeMap<String, String>,
	timeout_secs:         Option<u64>,
	volume_specs:         Vec<ResolvedVolume>,
	s3_specs:             Vec<ResolvedS3Mount>,
	template_dir:         PathBuf,
	image_spec:           Option<image::ImageConfig>,
	image_ref:            Option<String>,
	pool_key:             String,
	warm_volumes:         bool,
	host_slot:            bool,
	networked_warm:       bool,
	networked_warm_linux: bool,
}

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotOptions {
	agent:           Option<bool>,
	block_network:   Option<bool>,
	env:             Option<HashMap<String, String>>,
	workdir:         Option<String>,
	tags:            Option<HashMap<String, String>>,
	timeout:         Option<f64>,
	timeout_secs:    Option<u64>,
	readiness_probe: Option<Value>,
	secrets:         Option<Vec<Value>>,
	s3_mounts:       Option<HashMap<String, S3MountSpec>>,
	command:         Option<Vec<String>>,
}

#[derive(Clone)]
struct ResolvedSnapshotOptions {
	agent:           bool,
	block_network:   Option<bool>,
	env:             BTreeMap<String, String>,
	secret_env:      BTreeMap<String, String>,
	secret_names:    Vec<String>,
	workdir:         Option<String>,
	tags:            HashMap<String, String>,
	timeout_secs:    Option<u64>,
	readiness_probe: Option<Value>,
	s3_mounts:       Option<HashMap<String, S3MountSpec>>,
	command:         Option<Vec<String>>,
}

#[derive(Clone, Copy)]
enum SnapshotLaunchMode {
	Restore,
	Fork,
}

struct ResolvedVolume {
	mountpoint: String,
	name:       String,
	tag:        String,
	host_dir:   PathBuf,
	read_only:  bool,
	lock:       Option<VolumeLock>,
}

struct ResolvedS3Mount {
	mountpoint: String,
	tag:        String,
	read_only:  bool,
	client:     Arc<S3Client>,
	meta:       Value,
}

impl Engine {
	/// Construct the real engine with the built-in `vmon vmm` runtime.
	pub fn new(config: ServeConfig) -> Result<Self> {
		Self::with_runtime(config, Arc::new(VmonRuntime))
	}

	/// Construct the engine with an explicitly selected sandbox runtime.
	pub fn with_runtime(
		config: ServeConfig,
		sandbox_runtime: Arc<dyn SandboxRuntime>,
	) -> Result<Self> {
		align_process_home(&config.home);
		let home = Home::new(config.home.clone());
		fs::create_dir_all(home.vms_dir())?;
		fs::create_dir_all(home.templates_dir())?;
		fs::create_dir_all(home.volumes_dir())?;
		let registry = Registry::new();
		let lock_requests = registry.rehydrate(&home)?;
		registry.rebuild_idempotency_index();
		let net_runtime = Runtime::new()
			.map_err(|err| EngineError::engine(format!("starting network runtime: {err}")))?;
		let engine = Self {
			inner: Arc::new(EngineInner {
				config,
				home,
				registry,
				runtimes: Mutex::new(HashMap::new()),
				pools: PoolRegistry::new(),
				events: Mutex::new(Vec::new()),
				event_sequence: AtomicU64::new(0),
				counters: Counters::default(),
				latency: Mutex::new(CreateLatency::default()),
				net_runtime,
				sandbox_runtime,
			}),
		};
		engine.reacquire_volume_locks(lock_requests);
		engine.start_configured_pools()?;
		Ok(engine)
	}

	/// Stop pool refillers and parked clones. User sandboxes are intentionally
	/// left running.
	pub fn shutdown(&self) {
		self.inner.pools.shutdown();
	}

	fn home(&self) -> &Home {
		&self.inner.home
	}

	fn sandbox(&self, name: &str) -> SandboxVm {
		self.inner.sandbox_runtime.sandbox(name)
	}

	fn launch_sandbox(&self, vm: &SandboxVm, spec: &LaunchSpec) -> Result<()> {
		self.inner.sandbox_runtime.launch(vm, spec)?;
		vm.save_meta(Map::from_iter([(
			"runtime".to_owned(),
			json!(self.inner.sandbox_runtime.name()),
		)]))
	}

	fn stop_sandbox(&self, vm: &SandboxVm, wait: bool) -> Result<()> {
		self.inner.sandbox_runtime.stop(vm, wait)
	}

	fn remove_sandbox(&self, vm: &SandboxVm) -> Result<()> {
		self.inner.sandbox_runtime.remove(vm)
	}

	fn sandbox_is_running(&self, vm: &SandboxVm) -> Result<bool> {
		self.inner.sandbox_runtime.is_running(vm)
	}

	fn reacquire_volume_locks(&self, requests: Vec<(String, Vec<String>)>) {
		let mut runtimes = self.inner.runtimes.lock();
		for (name, volumes) in requests {
			let state = runtimes.entry(name).or_default();
			for volume_name in volumes {
				let Ok(volume) = Volume::new_in_home(self.home().root(), &volume_name) else {
					continue;
				};
				if let Ok(lock) = volume.acquire_write_lock() {
					state.volume_locks.push(lock);
				}
			}
		}
	}

	fn start_configured_pools(&self) -> Result<()> {
		for warm in &self.inner.config.warm_images {
			self.start_warm_image(warm)?;
		}
		Ok(())
	}

	fn start_warm_image(&self, warm: &WarmImage) -> Result<()> {
		let request = TemplateRequest {
			image: Some(warm.reference.clone()),
			// Pools serve pool-eligible sandboxes (block_network, no volumes,
			// no fs_dir), whose templates carry no NIC slot.
			nic_slot: false,
			..TemplateRequest::default()
		};
		let cached = image::cached_template(self, &request)?;
		let key = template_key_for_cached(&cached);
		let pool = WarmPool::with_runtime(
			cached.snapshot_dir,
			warm.count,
			Arc::clone(&self.inner.sandbox_runtime),
		)?;
		let old = self.inner.pools.set(key, pool);
		if let Some(old) = old {
			old.shutdown();
		}
		Ok(())
	}

	fn validate_create(params: &SandboxCreate) -> Result<()> {
		require_positive("cpus", u64::from(params.cpus))?;
		require_positive("memory", u64::from(params.memory))?;
		require_positive("disk_mb", u64::from(params.disk_mb))?;
		if let Some(timeout) = params.timeout
			&& (!timeout.is_finite() || timeout < 0.0)
		{
			return Err(EngineError::invalid("timeout must be non-negative"));
		}
		if params.block_network && params.ports.as_ref().is_some_and(|ports| !ports.is_empty()) {
			return Err(EngineError::invalid("ports cannot be exposed when block_network=True"));
		}
		validate_ports(params.ports.as_deref())?;
		validate_cidrs("egress_allow", params.egress_allow.as_deref())?;
		validate_cidrs("inbound_cidr_allowlist", params.inbound_cidr_allowlist.as_deref())?;
		validate_domains("egress_allow_domains", params.egress_allow_domains.as_deref())?;
		validate_ha(params.ha.as_deref())?;
		if params.remote_page_url.is_some()
			|| params.remote_page_token.is_some()
			|| params.remote_page_digest.is_some()
		{
			return Err(EngineError::invalid("remote_page_* fields are server-internal"));
		}
		validate_arch(params.arch.as_deref())?;
		Ok(())
	}

	fn prepare_create(&self, mut params: SandboxCreate) -> Result<CreatePlan> {
		Self::validate_create(&params)?;
		let sid = params
			.name
			.clone()
			.filter(|name| !name.is_empty())
			.unwrap_or_else(|| format!("sb-{}", random_hex(12)));
		params.name = Some(sid.clone());
		validate_local_name("sandbox name", &sid)?;
		if self.inner.registry.get(&sid).is_some() || self.sandbox(&sid).dir().exists() {
			return Err(EngineError::busy(format!("sandbox '{sid}' already exists")));
		}
		let ha = match params.ha.as_deref().filter(|ha| !ha.is_empty()) {
			Some(ha) => ha.to_owned(),
			None => self.inner.config.default_ha(false).to_owned(),
		};
		let restart_policy = restart_policy_for_ha(&ha).to_owned();
		let tags = params.tags.clone().unwrap_or_default();
		let secrets = parse_secrets(params.secrets.take())?;
		let secret_env = merge_secret_env(&secrets);
		let timeout_secs = effective_timeout_secs(params.timeout_secs, params.timeout)?;
		let mut used_tags = HashSet::new();
		let mut volume_specs = self.resolve_volumes(params.volumes.take(), &mut used_tags)?;
		let mut s3_specs = self.resolve_s3_mounts(params.s3_mounts.take(), &mut used_tags)?;
		if let Some(tags) = params.s3_restore_tags.take() {
			apply_restored_s3_tags(&mut s3_specs, &volume_specs, &tags)?;
		}
		let n_vols = u64::try_from(volume_specs.len()).unwrap_or(WARM_VOLUME_SLOTS + 1);
		let warm_volumes = params.block_network
			&& params.fs_dir.is_none()
			&& params.template.is_none()
			&& s3_specs.is_empty()
			&& (1..=WARM_VOLUME_SLOTS).contains(&n_vols);
		let host_slot = params.block_network
			&& params.fs_dir.is_some()
			&& params.template.is_none()
			&& s3_specs.is_empty()
			&& volume_specs.is_empty();
		let networked_warm = !params.block_network
			&& cfg!(target_os = "macos")
			&& params.template.is_none()
			&& params.fs_dir.is_none()
			&& s3_specs.is_empty()
			&& volume_specs.is_empty()
			&& params.ports.as_ref().is_none_or(Vec::is_empty)
			&& params.egress_allow.as_ref().is_none_or(Vec::is_empty)
			&& params
				.egress_allow_domains
				.as_ref()
				.is_none_or(Vec::is_empty)
			&& params
				.inbound_cidr_allowlist
				.as_ref()
				.is_none_or(Vec::is_empty);
		let networked_warm_linux = !params.block_network
			&& !cfg!(target_os = "macos")
			&& params.template.is_none()
			&& params.fs_dir.is_none()
			&& s3_specs.is_empty()
			&& volume_specs.is_empty();
		let fs_slots = if warm_volumes { n_vols } else { 0 };
		let (template_dir, image_spec, image_ref, cached_key) = self.resolve_template(
			&params,
			fs_slots,
			host_slot,
			networked_warm,
			networked_warm_linux,
		)?;
		if warm_volumes {
			for (index, spec) in volume_specs.iter_mut().enumerate() {
				spec.tag = image::slot_tag(index as u64);
			}
		}
		Ok(CreatePlan {
			params,
			sid,
			ha,
			restart_policy,
			tags,
			secrets,
			secret_env,
			timeout_secs,
			volume_specs,
			template_dir,
			image_spec,
			image_ref,
			pool_key: cached_key,
			warm_volumes,
			host_slot,
			networked_warm,
			networked_warm_linux,
			s3_specs,
		})
	}

	fn resolve_template(
		&self,
		params: &SandboxCreate,
		fs_slots: u64,
		host_slot: bool,
		nic_slot: bool,
		tap_slot: bool,
	) -> Result<(PathBuf, Option<image::ImageConfig>, Option<String>, String)> {
		if let Some(template) = &params.template {
			let path = PathBuf::from(template);
			let template_dir = if path.exists() || path.is_absolute() {
				path
			} else {
				self.home().templates_dir().join(template)
			};
			return Ok((template_dir, None, Some(template.clone()), template.clone()));
		}
		let request = TemplateRequest {
			image: params.image.clone(),
			dockerfile: params.dockerfile.as_ref().map(PathBuf::from),
			context: PathBuf::from(&params.context),
			disk_mb: u64::from(params.disk_mb),
			timeout: params.timeout.unwrap_or(300.0).max(0.0) as u64,
			memory: u64::from(params.memory),
			cpus: u64::from(params.cpus),
			fs_slots,
			host_slot,
			nic_slot,
			tap_slot,
		};
		let cached = image::cached_template(self, &request)?;
		let key = template_key_for_cached(&cached);
		Ok((cached.snapshot_dir, Some(cached.spec), Some(cached.name), key))
	}

	fn resolve_volumes(
		&self,
		volumes: Option<HashMap<String, Value>>,
		used_tags: &mut HashSet<String>,
	) -> Result<Vec<ResolvedVolume>> {
		let mut out = Vec::new();
		for (mountpoint, value) in volumes.unwrap_or_default() {
			let (name, read_only) = parse_volume_spec(&value)?;
			let volume = Volume::new_in_home(self.home().root(), &name)?;
			let lock = if read_only {
				None
			} else {
				Some(volume.acquire_write_lock()?)
			};
			let tag = unique_volume_tag(&name, used_tags);
			out.push(ResolvedVolume {
				mountpoint,
				name,
				tag,
				host_dir: volume.path(),
				read_only,
				lock,
			});
		}
		Ok(out)
	}

	fn resolve_s3_mounts(
		&self,
		mounts: Option<HashMap<String, S3MountSpec>>,
		used_tags: &mut HashSet<String>,
	) -> Result<Vec<ResolvedS3Mount>> {
		let mounts = mounts.unwrap_or_default();
		if mounts.len() > MAX_S3_MOUNTS {
			return Err(EngineError::invalid(format!(
				"at most {MAX_S3_MOUNTS} S3 mounts are supported"
			)));
		}
		let mut mounts = mounts.into_iter().collect::<Vec<_>>();
		mounts.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
		let mut out = Vec::with_capacity(mounts.len());
		for (mountpoint, spec) in mounts {
			if !Path::new(&mountpoint).is_absolute() {
				return Err(EngineError::invalid(format!(
					"s3 mountpoint must be absolute: {mountpoint}"
				)));
			}
			let (bucket, prefix) = parse_s3_uri(&spec.uri)?;
			let region = spec
				.region
				.clone()
				.filter(|region| !region.is_empty())
				.unwrap_or_else(|| {
					std::env::var("AWS_REGION")
						.ok()
						.filter(|region| !region.is_empty())
						.unwrap_or_else(|| "us-east-1".to_owned())
				});
			let (creds, auth) = s3_credentials(&mountpoint, &spec)?;
			let client = Arc::new(
				S3Client::new(S3MountConfig {
					bucket: bucket.clone(),
					prefix: prefix.clone(),
					region: region.clone(),
					endpoint: spec.endpoint.clone(),
					read_only: spec.read_only,
					creds,
					auth,
				})
				.map_err(|error| EngineError::invalid(format!("s3 mount '{mountpoint}': {error}")))?,
			);
			self
				.inner
				.net_runtime
				.block_on(client.probe())
				.map_err(|error| EngineError::invalid(format!("s3 mount '{mountpoint}': {error}")))?;
			let tag = unique_volume_tag(&bucket, used_tags);
			out.push(ResolvedS3Mount {
				mountpoint,
				tag: tag.clone(),
				read_only: spec.read_only,
				client,
				meta: json!({
					"uri": spec.uri,
					"endpoint": spec.endpoint,
					"region": region,
					"read_only": spec.read_only,
					"tag": tag,
					"auth": auth.as_str(),
				}),
			});
		}
		Ok(out)
	}

	/// Rebuild remote filesystem clients from a snapshot's credential-free mount
	/// metadata.
	fn snapshot_s3_mounts(
		&self,
		snapshot_dir: &Path,
		requested: Option<HashMap<String, S3MountSpec>>,
	) -> Result<Vec<ResolvedS3Mount>> {
		let path = snapshot_dir.join(S3_MOUNTS_FILE);
		if !path.is_file() {
			if requested.as_ref().is_some_and(|mounts| !mounts.is_empty()) {
				return Err(EngineError::invalid(
					"cannot add S3 mounts to a snapshot without remote filesystem devices",
				));
			}
			return Ok(Vec::new());
		}
		let source = serde_json::from_slice::<Value>(&read_snapshot_metadata(&path)?)?;
		if source.as_object().is_some_and(Map::is_empty) {
			if requested.as_ref().is_some_and(|mounts| !mounts.is_empty()) {
				return Err(EngineError::invalid(
					"cannot add S3 mounts to a snapshot without remote filesystem devices",
				));
			}
			return Ok(Vec::new());
		}
		let mut params = Map::from_iter([("s3_mounts".to_owned(), source)]);
		let tags = restore_s3_mount_params(&mut params)?.ok_or_else(|| {
			EngineError::invalid(format!(
				"snapshot {} has invalid S3 mount metadata",
				snapshot_dir.display()
			))
		})?;
		let mounts = match requested {
			Some(mounts) => mounts,
			None => serde_json::from_value(
				params
					.remove("s3_mounts")
					.expect("restored S3 mount parameters remain present"),
			)?,
		};
		let mut used_tags = HashSet::new();
		let mut mounts = self.resolve_s3_mounts(Some(mounts), &mut used_tags)?;
		apply_restored_s3_tags(&mut mounts, &[], &tags)?;
		Ok(mounts)
	}

	/// Start a per-VM proxy before its remote virtio-fs devices connect.
	fn with_s3_proxy(
		&self,
		vm: &SandboxVm,
		mut spec: LaunchSpec,
		mounts: &[ResolvedS3Mount],
	) -> Result<(LaunchSpec, Option<S3Proxy>)> {
		if mounts.is_empty() {
			return Ok((spec, None));
		}
		let sock = vm.dir().join("s3.sock");
		let routes = mounts
			.iter()
			.map(|mount| (mount.tag.clone(), Arc::clone(&mount.client)))
			.collect();
		let proxy = S3Proxy::start(&self.inner.net_runtime, &sock, routes)?;
		for mount in mounts {
			spec = spec.with_remote_fs(RemoteFsShare { tag: mount.tag.clone(), sock: sock.clone() });
		}
		Ok((spec, Some(proxy)))
	}

	/// Mount each remote virtio-fs device into its guest target.
	fn mount_s3_in_guest(agent: &AgentConn, mounts: &[ResolvedS3Mount]) -> Result<()> {
		for mount in mounts {
			let mountpoint = Path::new(&mount.mountpoint);
			if mount.read_only {
				agent.mount(&mount.tag, mountpoint, true, "virtiofs", AGENT_REQUEST_TIMEOUT)?;
			} else {
				agent.mount_overlay(&mount.tag, mountpoint, AGENT_REQUEST_TIMEOUT)?;
			}
		}
		Ok(())
	}

	fn launch_create(&self, plan: &mut CreatePlan) -> Result<(SandboxVm, RuntimeState)> {
		let mut runtime = RuntimeState {
			secret_env: plan.secret_env.clone(),
			env: image_env(plan.image_spec.as_ref(), plan.params.env.as_ref()),
			workdir: Some(
				plan
					.params
					.workdir
					.clone()
					.or_else(|| plan.image_spec.as_ref().map(|spec| spec.workdir.clone()))
					.unwrap_or_else(|| "/".to_owned()),
			),
			network_policy: NetworkPolicy {
				block_network:          Some(plan.params.block_network),
				egress_allow:           plan.params.egress_allow.clone(),
				egress_allow_domains:   plan.params.egress_allow_domains.clone(),
				inbound_cidr_allowlist: plan.params.inbound_cidr_allowlist.clone(),
			},
			..RuntimeState::default()
		};
		let vm = self.claim_or_launch_vm(plan, &mut runtime)?;
		let agent = Self::agent_for_vm(&vm, AGENT_CONNECT_TIMEOUT)?;
		agent.ping(AGENT_CONNECT_TIMEOUT)?;
		if let Some(timeout_secs) = plan.timeout_secs
			&& runtime.timeout_stop.is_none()
		{
			runtime.timeout_stop = Some(start_timeout_watchdog(
				vm.name().to_owned(),
				timeout_secs,
				Arc::clone(&self.inner.sandbox_runtime),
			));
		}
		if runtime.timeout_stop.is_some() && plan.pool_key.is_empty() {
			// no-op branch kept explicit: fresh launches pass --timeout-secs to
			// the VMM.
		}
		for volume in &plan.volume_specs {
			agent.mount(
				&volume.tag,
				Path::new(&volume.mountpoint),
				volume.read_only,
				"virtiofs",
				AGENT_REQUEST_TIMEOUT,
			)?;
		}
		Self::mount_s3_in_guest(&agent, &plan.s3_specs)?;
		if let Some(network) = &runtime.network {
			let gc = &network.guest_config;
			agent.net_config(
				&gc.guest_ip,
				gc.prefix,
				&gc.host_ip,
				Some(&gc.dns),
				AGENT_REQUEST_TIMEOUT,
			)?;
			runtime.network_spec = Some(json!({
				"flavor": "tap",
				"guest_config": network_guest_json(&network.guest_config),
				"ports": sorted_ports(plan.params.ports.as_deref(), &network.tunnels()),
				"tunnels": tunnels_json(&network.tunnels()),
				"policy": policy_json(&runtime.network_policy),
			}));
		} else if !plan.params.block_network {
			let gc = user_net_guest_config();
			let dns = net::USER_NET_DNS
				.iter()
				.map(|dns| (*dns).to_owned())
				.collect::<Vec<_>>();
			agent.net_config(
				gc["guest_ip"].as_str().unwrap_or(net::USER_NET_GUEST_IP),
				gc["prefix"]
					.as_u64()
					.unwrap_or_else(|| u64::from(net::USER_NET_PREFIX)) as u8,
				gc["host_ip"].as_str().unwrap_or(net::USER_NET_GATEWAY),
				Some(&dns),
				AGENT_REQUEST_TIMEOUT,
			)?;
			runtime.network_spec = Some(json!({
				"flavor": "user",
				"guest_config": gc,
				"ports": [],
				"tunnels": {},
				"policy": {},
			}));
		}
		if let Some(probe) = &plan.params.readiness_probe {
			Self::wait_until_ready(&agent, &runtime, probe, plan.params.timeout.unwrap_or(300.0))?;
		}
		runtime.agent = Some(agent);
		runtime.volume_locks = plan
			.volume_specs
			.iter_mut()
			.filter_map(|volume| volume.lock.take())
			.collect();
		let mut meta = Map::new();
		meta.insert("sandbox".to_owned(), json!(true));
		meta.insert("image".to_owned(), json!(plan.image_ref));
		meta.insert("template".to_owned(), json!(plan.template_dir.to_string_lossy()));
		meta.insert("workdir".to_owned(), json!(runtime.workdir));
		meta.insert("env_names".to_owned(), json!(runtime.env.keys().collect::<Vec<_>>()));
		meta.insert(
			"secret_names".to_owned(),
			json!(
				plan
					.secrets
					.iter()
					.map(|secret| &secret.name)
					.collect::<Vec<_>>()
			),
		);
		meta.insert("tags".to_owned(), json!(plan.tags));
		meta.insert("volumes".to_owned(), volumes_meta(&plan.volume_specs));
		meta.insert("s3_mounts".to_owned(), s3_mounts_meta(&plan.s3_specs));
		meta.insert("block_network".to_owned(), json!(plan.params.block_network));
		meta.insert("network".to_owned(), runtime.network_spec.clone().unwrap_or(Value::Null));
		meta.insert("timeout_secs".to_owned(), json!(plan.timeout_secs));
		vm.save_meta(meta)?;
		Ok((vm, runtime))
	}

	fn claim_or_launch_vm(
		&self,
		plan: &CreatePlan,
		runtime: &mut RuntimeState,
	) -> Result<SandboxVm> {
		if (plan.params.block_network || plan.networked_warm)
			&& plan.volume_specs.is_empty()
			&& plan.s3_specs.is_empty()
			&& plan.params.fs_dir.is_none()
		{
			if plan.params.pool_size > 0 {
				let pool = WarmPool::with_runtime(
					&plan.template_dir,
					plan.params.pool_size as usize,
					Arc::clone(&self.inner.sandbox_runtime),
				)?;
				let old = self
					.inner
					.pools
					.set(plan.pool_key.clone(), Arc::clone(&pool));
				if let Some(old) = old {
					old.shutdown();
				}
			}
			if let Some(pool) = self.inner.pools.get(&plan.pool_key)
				&& let Some(vm) = pool.claim(Some(&plan.sid))?
			{
				if plan.networked_warm {
					runtime.network_spec = Some(json!({
						"flavor": "user",
						"guest_config": user_net_guest_config(),
						"ports": [],
						"tunnels": {},
						"policy": {},
					}));
				}
				if let Some(secs) = plan.timeout_secs {
					let _ = control_for_vm(&vm)?.extend(secs);
					runtime.timeout_stop = Some(start_timeout_watchdog(
						vm.name().to_owned(),
						secs,
						Arc::clone(&self.inner.sandbox_runtime),
					));
				}
				return Ok(vm);
			}
		}
		if plan.params.block_network
			&& plan.params.fs_dir.is_none()
			&& plan.volume_specs.is_empty()
			&& plan.s3_specs.is_empty()
		{
			return self.launch_restore_vm(plan, None, runtime);
		}
		if plan.warm_volumes || plan.host_slot || plan.networked_warm {
			return self.launch_restore_vm(plan, None, runtime);
		}
		if (plan.networked_warm_linux
			|| (!plan.params.block_network
				&& plan.s3_specs.is_empty()
				&& snapshot_state_present(&plan.template_dir)))
			&& !cfg!(target_os = "macos")
		{
			let network = self.setup_network(&plan.sid, &plan.params)?;
			let tap = network.guest_config.tap.clone();
			runtime.network = Some(network);
			return self.launch_restore_vm(plan, Some(tap), runtime);
		}
		if !plan.params.block_network
			&& plan.s3_specs.is_empty()
			&& cfg!(target_os = "macos")
			&& snapshot_state_present(&plan.template_dir)
		{
			reject_macos_host_network_features(&plan.params)?;
			return self.launch_restore_vm(plan, None, runtime);
		}
		self.launch_cold_vm(plan, runtime)
	}

	fn launch_restore_vm(
		&self,
		plan: &CreatePlan,
		tap: Option<String>,
		runtime: &mut RuntimeState,
	) -> Result<SandboxVm> {
		let vm = self.sandbox(&plan.sid);
		let mut spec = LaunchSpec::restore(vm.api_sock(), &plan.template_dir)
			.with_agent_sock(vm.dir().join("agent.sock"))
			.with_mem_mib(u64::from(plan.params.memory))
			.with_cpus(u64::from(plan.params.cpus));
		// The restored VM must own its disk: overlay the template's image so
		// guest writes land in a per-VM file (checkpointable, migratable)
		// instead of the shared absolute path recorded in the snapshot's
		// block-device hint — a path that does not even exist on a peer node
		// restoring a pulled checkpoint.
		let base_disk = plan.template_dir.join("rootfs.img");
		if base_disk.is_file() {
			spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
		}
		if let Some(secs) = plan.timeout_secs {
			spec = spec.with_timeout_secs(secs);
		}
		if let Some(tap) = tap {
			spec = spec.with_tap(tap);
		} else if !plan.params.block_network {
			spec = spec.with_user_net();
		}
		if let Some(fs_dir) = &plan.params.fs_dir {
			spec = spec.with_fs_share("host", fs_dir);
		}
		for volume in &plan.volume_specs {
			spec = spec.with_volume(VolumeMount::new(
				volume.tag.clone(),
				volume.host_dir.clone(),
				volume.read_only,
			)?);
		}
		if let Some(url) = &plan.params.remote_page_url {
			spec = spec.with_remote_page(
				url,
				plan.params.remote_page_token.clone(),
				plan.params.remote_page_digest.clone(),
			);
		}
		let (spec, s3_proxy) = self.with_s3_proxy(&vm, spec, &plan.s3_specs)?;
		self.launch_sandbox(&vm, &spec)?;
		runtime.s3_proxy = s3_proxy;
		if !plan.params.block_network && runtime.network.is_none() {
			runtime.network_spec = Some(json!({
				"flavor": "user",
				"guest_config": user_net_guest_config(),
				"ports": [],
				"tunnels": {},
				"policy": {},
			}));
		}
		Ok(vm)
	}

	fn launch_cold_vm(&self, plan: &CreatePlan, runtime: &mut RuntimeState) -> Result<SandboxVm> {
		let vm = self.sandbox(&plan.sid);
		let base_disk = plan.template_dir.join("rootfs.img");
		if !base_disk.is_file() {
			return Err(EngineError::engine(format!(
				"template {} has no rootfs.img; fresh-boot sandboxes require a disk-backed template",
				plan.template_dir.display()
			)));
		}
		let kernel = image::assets::default_kernel()?;
		let mut spec = LaunchSpec::boot_rootfs(vm.api_sock(), kernel, vm.dir().join("rootfs.img"))
			.with_agent_sock(vm.dir().join("agent.sock"))
			.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"))
			.with_mem_mib(u64::from(plan.params.memory))
			.with_cpus(u64::from(plan.params.cpus))
			.with_rng()
			.with_snapshot_root(self.snapshot_root());
		if let Some(secs) = plan.timeout_secs {
			spec = spec.with_timeout_secs(secs);
		}
		if !plan.params.block_network {
			if cfg!(target_os = "macos") {
				reject_macos_host_network_features(&plan.params)?;
				spec = spec.with_user_net();
			} else {
				let network = self.setup_network(&plan.sid, &plan.params)?;
				let tap = network.guest_config.tap.clone();
				runtime.network = Some(network);
				spec = spec.with_tap(tap);
			}
		}
		if let Some(fs_dir) = &plan.params.fs_dir {
			spec = spec.with_fs_share("host", fs_dir);
		}
		for volume in &plan.volume_specs {
			spec = spec.with_volume(VolumeMount::new(
				volume.tag.clone(),
				volume.host_dir.clone(),
				volume.read_only,
			)?);
		}
		let (spec, s3_proxy) = self.with_s3_proxy(&vm, spec, &plan.s3_specs)?;
		self.launch_sandbox(&vm, &spec)?;
		runtime.s3_proxy = s3_proxy;
		Ok(vm)
	}

	fn setup_network(&self, name: &str, params: &SandboxCreate) -> Result<SandboxNetwork> {
		self.inner.net_runtime.block_on(net::setup_sandbox_network(
			name,
			params.ports.as_deref().unwrap_or(&[]),
			params.egress_allow.as_deref(),
			params.egress_allow_domains.as_deref(),
			params.inbound_cidr_allowlist.as_deref(),
		))
	}

	fn wait_until_ready(
		agent: &AgentConn,
		runtime: &RuntimeState,
		probe: &Value,
		timeout: f64,
	) -> Result<()> {
		let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(0.0));
		let mut last = None;
		while Instant::now() < deadline {
			let remaining = deadline.saturating_duration_since(Instant::now());
			let attempt = remaining.min(Duration::from_secs(5));
			let result = if let Some(port) = probe.as_u64() {
				u16::try_from(port)
					.map_err(|_| EngineError::invalid("readiness port must be 1-65535"))
					.and_then(|port| agent.tcp_probe(port, "127.0.0.1", attempt))
					.and_then(|ready| {
						ready
							.then_some(())
							.ok_or_else(|| EngineError::engine("probe not ready"))
					})
			} else if let Some(port) = probe.get("port").and_then(Value::as_u64) {
				let host = probe
					.get("host")
					.and_then(Value::as_str)
					.unwrap_or("127.0.0.1");
				u16::try_from(port)
					.map_err(|_| EngineError::invalid("readiness port must be 1-65535"))
					.and_then(|port| agent.tcp_probe(port, host, attempt.min(Duration::from_secs(1))))
					.and_then(|ready| {
						ready
							.then_some(())
							.ok_or_else(|| EngineError::engine("probe not ready"))
					})
			} else {
				let argv = readiness_argv(probe);
				let env = merged_env(runtime, None);
				let cwd = runtime.workdir.as_deref().map(Path::new);
				agent
					.exec(&argv, cwd, Some(&env), false, Some(attempt))
					.and_then(|session| session.wait(Some(attempt)))
					.and_then(|code| {
						(code == 0)
							.then_some(())
							.ok_or_else(|| EngineError::engine("probe not ready"))
					})
			};
			match result {
				Ok(()) => return Ok(()),
				Err(err) => last = Some(err),
			}
			thread::sleep(
				Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
			);
		}
		Err(EngineError::engine(format!(
			"sandbox readiness probe timed out after {timeout}s{}",
			last.map_or_else(String::new, |err| format!(": {err}"))
		)))
	}

	fn agent_for(&self, name: &str) -> Result<AgentConn> {
		let cached_agent = self
			.inner
			.runtimes
			.lock()
			.get(name)
			.and_then(|state| state.agent.clone());
		if let Some(agent) = cached_agent.filter(|agent| !agent.is_closed()) {
			return Ok(agent);
		}
		let vm = self.sandbox(name);
		let agent = Self::agent_for_vm(&vm, AGENT_CONNECT_TIMEOUT)?;
		self
			.inner
			.runtimes
			.lock()
			.entry(name.to_owned())
			.or_default()
			.agent = Some(agent.clone());
		Ok(agent)
	}

	fn agent_for_vm(vm: &SandboxVm, timeout: Duration) -> Result<AgentConn> {
		AgentConn::connect(&vm.agent_sock()?, timeout)
	}

	fn start_entry_command(&self, name: String, cmd: Vec<String>) -> Result<()> {
		let agent = self.agent_for(&name)?;
		let (env, fallback_workdir) = {
			let runtimes = self.inner.runtimes.lock();
			runtimes.get(&name).map_or_else(
				|| (BTreeMap::new(), None),
				|state| (merged_env(state, None), state.workdir.clone()),
			)
		};
		let session =
			agent.exec(&cmd, fallback_workdir.as_deref().map(Path::new), Some(&env), false, None)?;
		let engine = self.clone();
		thread::Builder::new()
			.name(format!("vmon-entry-{name}"))
			.spawn(move || {
				if let Err(err) = engine.run_entry_command(name.clone(), session) {
					tracing::warn!(sandbox = %name, error = %err, "entry command failed");
				}
			})?;
		Ok(())
	}

	fn run_entry_command(
		&self,
		name: String,
		session: crate::engine::agent::ExecSession,
	) -> Result<()> {
		let log = Arc::new(Mutex::new(
			fs::OpenOptions::new()
				.create(true)
				.append(true)
				.open(self.sandbox(&name).log_path())?,
		));
		let parts = session.split();
		let _ = parts.control.close_stdin();
		let stdout = drain_entry_stream(parts.stdout, Arc::clone(&log));
		let stderr = drain_entry_stream(parts.stderr, Arc::clone(&log));
		match parts.exit.recv() {
			Ok(Ok(_status)) => {},
			Ok(Err(err)) => return Err(err),
			Err(_) => return Err(EngineError::engine("agent connection closed")),
		}
		let _ = stdout.join();
		let _ = stderr.join();
		Ok(())
	}

	fn get_record(&self, id: &str, require_running: bool) -> Result<VmRecord> {
		let mut record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		// Always refresh: a VMM that died on its own (timeout self-kill, guest
		// poweroff) must be observable through plain GET/list, like Python's
		// inspect()/ps() liveness refresh.
		record = self.refresh_record_status(record)?;
		if require_running {
			if record.status != "running" {
				return Err(EngineError::not_running(format!("sandbox '{id}' is not running")));
			}
			// Only actions on a running sandbox count as activity; plain reads
			// must not advance `last_active` (it feeds `expires_at`).
			self.inner.registry.update(id, VmRecord::touch);
		}
		Ok(record)
	}

	fn refresh_record_status(&self, record: VmRecord) -> Result<VmRecord> {
		if record.status != "running" {
			// A stop that raced the VMM's own exit can persist "stopped" before
			// status.json lands; backfill the exit code once it is readable.
			if record.status == "stopped"
				&& record.detail.get("returncode").is_none_or(Value::is_null)
				&& let Some(returncode) = Self::poll_returncode(&record.name)
			{
				let _ = self.persist_status(&record.id, "stopped", Some(returncode), None);
				if let Some(updated) = self.inner.registry.get(&record.id) {
					return Ok(updated);
				}
			}
			return Ok(record);
		}
		let vm = self.sandbox(&record.name);
		if record.pid.is_some() && !self.sandbox_is_running(&vm)? {
			// The VMM died on its own (e.g. --timeout-secs self-kill, guest
			// poweroff): surface its status.json exit code in the view like
			// Python's poll()/status.json path does.
			let returncode = Self::poll_returncode(&record.name);
			let _ = self.persist_status(&record.id, "stopped", returncode, None);
			let updated = self
				.inner
				.registry
				.get(&record.id)
				.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{}'", record.id)))?;
			self.publish_record_event("stopped", &updated);
			return Ok(updated);
		}
		Ok(record)
	}

	fn persist_status(
		&self,
		id: &str,
		status: &str,
		returncode: Option<i64>,
		terminated_at: Option<f64>,
	) -> Result<()> {
		self
			.inner
			.registry
			.persist_record_status(self.home(), id, status, returncode, terminated_at)
	}

	fn teardown(&self, record: &VmRecord) -> Option<i64> {
		let name = &record.name;
		let mut returncode = Self::poll_returncode(name);
		let removed = self.inner.runtimes.lock().remove(name);
		if let Some(mut state) = removed {
			if let Some(stop) = state.timeout_stop.take() {
				let _ = stop.send(());
			}
			if let Some(agent) = state.agent.take() {
				agent.close();
			}
			drop(state.s3_proxy.take());
			if let Some(network) = state.network.take() {
				let _ = network.teardown();
			} else {
				teardown_network(name);
			}
			drop(state.volume_locks);
		} else {
			teardown_network(name);
		}
		let vm = self.sandbox(name);
		let _ = self.stop_sandbox(&vm, true);
		if returncode.is_none() {
			returncode = Self::poll_returncode(name);
		}
		returncode
	}

	fn poll_returncode(name: &str) -> Option<i64> {
		let vm = SandboxVm::new(name);
		// The jail-aware control-socket parent is a best-effort candidate; the
		// plain VM dir must still be probed when metadata is unreadable.
		let mut candidates = Vec::with_capacity(2);
		if let Some(parent) = vm
			.control_sock()
			.ok()
			.and_then(|sock| sock.parent().map(Path::to_path_buf))
		{
			candidates.push(parent.join("status.json"));
		}
		candidates.push(vm.dir().join("status.json"));
		for path in candidates {
			let Ok(text) = fs::read_to_string(path) else {
				continue;
			};
			let Ok(data) = serde_json::from_str::<Value>(&text) else {
				continue;
			};
			if let Some(code) = data.get("vmm_returncode").and_then(Value::as_i64) {
				return Some(code);
			}
			if let Some(reason) = data.get("reason").and_then(Value::as_str) {
				return match reason {
					"timeout" => Some(124),
					"quit" | "killed" => Some(137),
					"shutdown" => Some(0),
					_ => None,
				};
			}
		}
		None
	}

	fn snapshot_root(&self) -> PathBuf {
		self.home().root().join("snapshots")
	}

	fn snapshot_dir(&self, name: &str) -> PathBuf {
		self.snapshot_root().join(name)
	}

	fn require_snapshot_dir(&self, name: &str) -> Result<PathBuf> {
		validate_local_name("snapshot name", name)?;
		let root = match fs::canonicalize(self.snapshot_root()) {
			Ok(root) => root,
			Err(error) if error.kind() == io::ErrorKind::NotFound => {
				return Err(EngineError::not_found(format!("snapshot not found: {name}")));
			},
			Err(error) => return Err(error.into()),
		};
		let dir = root.join(name);
		let metadata = match fs::symlink_metadata(&dir) {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == io::ErrorKind::NotFound => {
				return Err(EngineError::not_found(format!("snapshot not found: {name}")));
			},
			Err(error) => return Err(error.into()),
		};
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			return Err(EngineError::invalid(format!(
				"snapshot {name:?} is not a regular snapshot directory"
			)));
		}
		let canonical = fs::canonicalize(&dir)?;
		if canonical.parent() != Some(root.as_path()) {
			return Err(EngineError::invalid(format!(
				"snapshot {name:?} resolves outside the snapshot root"
			)));
		}
		Ok(canonical)
	}

	fn ensure_snapshot_target_available(&self, name: &str) -> Result<()> {
		validate_local_name("sandbox name", name)?;
		if self.inner.registry.get(name).is_some() || self.sandbox(name).dir().exists() {
			return Err(EngineError::busy(format!("sandbox already exists: {name}")));
		}
		Ok(())
	}

	fn rollback_snapshot_vm(&self, vm: &SandboxVm) {
		let name = vm.name();
		if let Some(mut runtime) = self.inner.runtimes.lock().remove(name)
			&& let Some(stop) = runtime.timeout_stop.take()
		{
			let _ = stop.send(());
		}
		self.inner.registry.remove(name);
		let _ = self.stop_sandbox(vm, false);
		let _ = self.remove_sandbox(vm);
	}

	fn launch_snapshot_vm(
		&self,
		snapshot: &str,
		snapshot_dir: &Path,
		name: String,
		mode: SnapshotLaunchMode,
		options: &ResolvedSnapshotOptions,
		s3_mounts: &[ResolvedS3Mount],
	) -> Result<(SandboxVm, VmRecord)> {
		self.ensure_snapshot_target_available(&name)?;
		let vm = self.sandbox(&name);
		let result = (|| {
			let mut spec = match mode {
				SnapshotLaunchMode::Restore => LaunchSpec::restore(vm.api_sock(), snapshot_dir),
				SnapshotLaunchMode::Fork => LaunchSpec::fork_from(vm.api_sock(), snapshot_dir),
			}
			.with_agent_sock(vm.dir().join("agent.sock"));
			let base_disk = snapshot_dir.join("rootfs.img");
			if base_disk.is_file() {
				spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
			}
			let needs_agent = options.agent
				|| !s3_mounts.is_empty()
				|| options.readiness_probe.is_some()
				|| options
					.command
					.as_ref()
					.is_some_and(|command| !command.is_empty());
			if matches!(mode, SnapshotLaunchMode::Restore) && needs_agent {
				spec = spec.with_console_agent();
			}
			if let Some(timeout_secs) = options.timeout_secs {
				spec = spec.with_timeout_secs(timeout_secs);
			}
			let (spec, s3_proxy) = self.with_s3_proxy(&vm, spec, s3_mounts)?;
			self.launch_sandbox(&vm, &spec)?;

			let mut runtime = RuntimeState {
				secret_env: options.secret_env.clone(),
				env: options.env.clone(),
				workdir: options.workdir.clone(),
				s3_proxy,
				..RuntimeState::default()
			};
			if let Some(timeout_secs) = options.timeout_secs {
				runtime.timeout_stop = Some(start_timeout_watchdog(
					name.clone(),
					timeout_secs,
					Arc::clone(&self.inner.sandbox_runtime),
				));
			}
			if needs_agent {
				let agent = Self::agent_for_vm(&vm, AGENT_CONNECT_TIMEOUT)?;
				agent.ping(AGENT_CONNECT_TIMEOUT)?;
				Self::mount_s3_in_guest(&agent, s3_mounts)?;
				if let Some(probe) = &options.readiness_probe {
					Self::wait_until_ready(
						&agent,
						&runtime,
						probe,
						options.timeout_secs.map_or(300.0, |secs| secs as f64),
					)?;
				}
				runtime.agent = Some(agent);
			}

			let mut detail = vm.meta()?;
			if let Some(block_network) = options.block_network {
				detail.insert("block_network".to_owned(), json!(block_network));
			}
			detail.insert("workdir".to_owned(), json!(runtime.workdir));
			detail.insert("env_names".to_owned(), json!(runtime.env.keys().collect::<Vec<_>>()));
			detail.insert("secret_names".to_owned(), json!(options.secret_names));
			detail.insert("tags".to_owned(), json!(options.tags));
			detail.insert("timeout_secs".to_owned(), json!(options.timeout_secs));
			detail.insert("s3_mounts".to_owned(), s3_mounts_meta(s3_mounts));
			if let Some(command) = &options.command {
				detail.insert("command".to_owned(), json!(command));
			}
			vm.save_meta(detail.clone())?;

			self.inner.runtimes.lock().insert(name.clone(), runtime);
			if let Some(command) = options
				.command
				.as_ref()
				.filter(|command| !command.is_empty())
			{
				self.start_entry_command(name.clone(), command.clone())?;
			}
			let now = unix_time();
			let record = VmRecord {
				id:            name.clone(),
				name:          name.clone(),
				status:        "running".to_owned(),
				pid:           detail
					.get("pid")
					.and_then(Value::as_i64)
					.and_then(|pid| i32::try_from(pid).ok()),
				source:        Some(format!("{}:{snapshot}", match mode {
					SnapshotLaunchMode::Restore => "restore",
					SnapshotLaunchMode::Fork => "fork",
				})),
				created_at:    now,
				timeout:       options.timeout_secs.map(|secs| secs as f64),
				detail:        Value::Object(detail),
				tags:          options.tags.clone(),
				last_active:   now,
				terminated_at: None,
				error:         None,
			};
			self
				.inner
				.registry
				.insert_persisted(self.home(), record.clone())?;
			Ok((vm.clone(), record))
		})();
		if result.is_err() {
			self.rollback_snapshot_vm(&vm);
		}
		result
	}

	fn snapshot_machine(
		&self,
		vm: &SandboxVm,
		name: &str,
		keep_running: bool,
		disk_src: Option<&Path>,
		snapshot_root: &Path,
		track: bool,
	) -> Result<PathBuf> {
		validate_local_name("snapshot name", name)?;
		fs::create_dir_all(snapshot_root)?;
		let root = fs::canonicalize(snapshot_root)?;
		let dir = root.join(name);
		match fs::symlink_metadata(&dir) {
			Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
				return Err(EngineError::invalid(format!(
					"snapshot {name:?} is not a regular snapshot directory"
				)));
			},
			Ok(_) => {},
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error.into()),
		}
		let mut control = control_for_vm(vm)?;
		control.pause()?;
		// Copy the disk while still paused so the image matches the captured
		// memory/state exactly; on APFS this is a cheap clone.
		let snapshot_result = control.snapshot(name, None, track).and_then(|_| {
			if let Some(disk_src) = disk_src.filter(|path| path.is_file()) {
				fs::create_dir_all(&dir)?;
				fs::copy(disk_src, dir.join("rootfs.img"))?;
			}
			Ok(Value::Null)
		});
		if let Err(error) = snapshot_result {
			let _ = control.resume();
			return Err(error);
		}
		if keep_running {
			control.resume()?;
		} else {
			let _ = self.stop_sandbox(vm, true);
		}
		Ok(dir)
	}

	fn publish_event(&self, event_type: &str, mut data: Map<String, Value>) {
		let sequence = self.inner.event_sequence.fetch_add(1, Ordering::Relaxed) + 1;
		data.insert("type".to_owned(), json!(event_type));
		data.insert("sequence".to_owned(), json!(sequence));
		data.insert("ts".to_owned(), json!(unix_time()));
		let payload = Value::Object(data);
		let mut subscribers = self.inner.events.lock();
		subscribers.retain(|sender| sender.send(payload.clone()).is_ok());
	}

	fn publish_record_event(&self, event_type: &str, record: &VmRecord) {
		let status = match event_type {
			"paused" => "paused",
			"removed" => "removed",
			"ready" | "resumed" | "restore" | "fork" => "running",
			_ => &record.status,
		};
		let mut data = Map::from_iter([
			("id".to_owned(), json!(record.id)),
			("name".to_owned(), json!(record.name)),
			("status".to_owned(), json!(status)),
			("source".to_owned(), json!(record.source)),
			("tags".to_owned(), json!(record.tags)),
		]);
		if let Some(returncode) = record.detail.get("returncode").and_then(Value::as_i64) {
			data.insert("returncode".to_owned(), json!(returncode));
		}
		if let Some(reason) = record
			.detail
			.get("terminated_reason")
			.and_then(Value::as_str)
		{
			data.insert("reason".to_owned(), json!(reason));
		}
		if let Some(error) = &record.error {
			data.insert("error".to_owned(), json!(error));
		}
		self.publish_event(event_type, data);
	}

	fn inc_counter(&self, name: &str) {
		match name {
			"created" => {
				self.inner.counters.created.fetch_add(1, Ordering::Relaxed);
			},
			"terminated" => {
				self
					.inner
					.counters
					.terminated
					.fetch_add(1, Ordering::Relaxed);
			},
			"idle_reaped" => {
				self
					.inner
					.counters
					.idle_reaped
					.fetch_add(1, Ordering::Relaxed);
			},
			"exec" => {
				self.inner.counters.exec.fetch_add(1, Ordering::Relaxed);
			},
			"file_read" => {
				self
					.inner
					.counters
					.file_read
					.fetch_add(1, Ordering::Relaxed);
			},
			"file_write" => {
				self
					.inner
					.counters
					.file_write
					.fetch_add(1, Ordering::Relaxed);
			},
			"file_delete" => {
				self
					.inner
					.counters
					.file_delete
					.fetch_add(1, Ordering::Relaxed);
			},
			"snapshot" => {
				self.inner.counters.snapshot.fetch_add(1, Ordering::Relaxed);
			},
			"auth_failed" => {
				self
					.inner
					.counters
					.auth_failed
					.fetch_add(1, Ordering::Relaxed);
			},
			_ => {},
		}
	}

	#[allow(
		clippy::unnecessary_wraps,
		reason = "mesh adapters expect Engine helpers to report errors uniformly"
	)]
	pub(crate) fn mesh_owned_ids(&self) -> Result<Vec<String>> {
		Ok(self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status != "terminated")
			.map(|record| record.id)
			.collect())
	}

	pub(crate) fn mesh_list_views(&self) -> Result<Vec<Value>> {
		<Self as EngineApi>::list(self, None)
	}

	pub(crate) fn mesh_has_sandbox(&self, sid: &str) -> bool {
		self
			.inner
			.registry
			.get(sid)
			.is_some_and(|record| record.status != "terminated")
	}

	pub(crate) fn mesh_get_view(&self, sid: &str) -> Result<Value> {
		Ok(self.get_record(sid, false)?.view())
	}

	pub(crate) fn mesh_checkpoint_age_sec(&self, sid: &str) -> Option<f64> {
		let record = self.inner.registry.get(sid)?;
		let checkpoint_ts = record.detail.get("checkpoint_ts")?.as_f64()?;
		Some((unix_time() - checkpoint_ts).max(0.0))
	}

	#[allow(
		clippy::unnecessary_wraps,
		reason = "mesh adapters expect Engine lookup helpers to report errors uniformly"
	)]
	pub(crate) fn mesh_find_by_idempotency_key(&self, key: &str) -> Result<Option<Value>> {
		let Some(name) = self.inner.registry.find_by_idempotency_key(key) else {
			return Ok(None);
		};
		Ok(self.inner.registry.get(&name).map(|record| record.view()))
	}

	pub(crate) fn mesh_record_idempotency(&self, sid: &str, key: &str) -> Result<()> {
		self.inner.registry.record_idempotency(sid, key);
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("idempotency_key".to_owned(), json!(key))]),
		)
	}

	/// Attach granted writable-volume lease metadata to a live record, keyed by
	/// volume name and merged over any existing leases (Python
	/// `Engine.record_volume_leases` parity).
	pub(crate) fn mesh_record_volume_leases(&self, sid: &str, leases: Vec<Value>) -> Result<()> {
		let mut lease_map = Map::new();
		for lease in leases {
			let Some(volume) = lease
				.get("volume")
				.and_then(Value::as_str)
				.filter(|volume| !volume.is_empty())
			else {
				continue;
			};
			lease_map.insert(volume.to_owned(), lease.clone());
		}
		if lease_map.is_empty() {
			return Ok(());
		}
		let mut merged = self
			.inner
			.registry
			.get(sid)
			.and_then(|record| {
				record
					.detail
					.get("volume_leases")
					.and_then(Value::as_object)
					.cloned()
			})
			.unwrap_or_default();
		merged.extend(lease_map);
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("volume_leases".to_owned(), Value::Object(merged))]),
		)
	}

	/// Forget lease metadata after all matching votes have been released.
	/// A no-op for already-removed records (fencing removes before releasing).
	#[allow(
		clippy::unnecessary_wraps,
		reason = "lease cleanup participates in fallible Engine cleanup flows"
	)]
	pub(crate) fn mesh_clear_volume_leases(&self, sid: &str) -> Result<()> {
		if self.inner.registry.get(sid).is_none() {
			return Ok(());
		}
		self.inner.registry.update(sid, |record| {
			if let Some(detail) = record.detail.as_object_mut() {
				detail.remove("volume_leases");
			}
		});
		let _ = self
			.sandbox(sid)
			.save_meta(Map::from_iter([("volume_leases".to_owned(), Value::Object(Map::new()))]));
		Ok(())
	}

	/// Running records that both hold writable volumes and have lease
	/// metadata: `(name, volume_leases, writable volume names)` per record.
	pub(crate) fn mesh_volume_lease_records(
		&self,
	) -> Vec<(String, Map<String, Value>, Vec<String>)> {
		self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status == "running")
			.filter_map(|record| {
				let leases = record
					.detail
					.get("volume_leases")
					.and_then(Value::as_object)
					.cloned()?;
				let writable = writable_volume_names_from_detail(&record.detail);
				if writable.is_empty() {
					return None;
				}
				Some((record.name, leases, writable))
			})
			.collect()
	}

	/// Stop a sandbox after a writable-volume lease renewal failure.
	pub(crate) fn mesh_stop_sandbox(&self, sid: &str) -> Result<()> {
		let _ = <Self as EngineApi>::stop(self, sid)?;
		Ok(())
	}

	pub(crate) fn mesh_set_ha_metadata(
		&self,
		sid: &str,
		ha: &str,
		restart_policy: &str,
	) -> Result<()> {
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([
				("ha".to_owned(), json!(ha)),
				("restart_policy".to_owned(), json!(restart_policy)),
			]),
		)
	}

	pub(crate) fn mesh_create_from_params(&self, mut params: Map<String, Value>) -> Result<Value> {
		let s3_restore_tags = restore_s3_mount_params(&mut params)?;
		let mut request = crate::mesh::runtime::sandbox_create_from_mesh_params(params)?;
		request.s3_restore_tags = s3_restore_tags;
		<Self as EngineApi>::create(self, request)
	}

	/// Eligibility checks + restore params shared by every peer checkpoint.
	///
	/// Merges the persisted record detail with the *live* runtime state (env,
	/// secrets, network spec) a peer needs to re-create the sandbox. Raises
	/// `Unsupported` for sandboxes that cannot move hosts (`fs_dir` shares,
	/// rehydrated records, missing network state).
	fn mesh_checkpoint_params(&self, sid: &str) -> Result<(VmRecord, Map<String, Value>)> {
		let record = self.get_record(sid, true)?;
		let detail = record.detail.as_object().cloned().unwrap_or_default();
		if detail
			.get("fs_dir")
			.and_then(Value::as_str)
			.is_some_and(|dir| !dir.is_empty())
		{
			return Err(EngineError::unsupported(
				"a sandbox with an fs_dir share cannot migrate; the share is host-local",
			));
		}
		// Live restore state exists only for in-process sandboxes; one
		// rehydrated after a daemon restart has lost the env/secret/network
		// identity needed to restore it elsewhere.
		let (env, secret_env, network_spec, workdir) = {
			let runtimes = self.inner.runtimes.lock();
			let Some(runtime) = runtimes.get(sid) else {
				return Err(EngineError::unsupported(
					"migration requires a live in-process sandbox; one rehydrated after a daemon \
					 restart has lost the identity needed to move it",
				));
			};
			(
				runtime.env.clone(),
				runtime.secret_env.clone(),
				runtime.network_spec.clone(),
				runtime.workdir.clone(),
			)
		};
		let mut params = detail;
		params.insert("name".to_owned(), json!(sid));
		params.insert("env".to_owned(), json!(env));
		if let Some(workdir) = workdir {
			params.insert("workdir".to_owned(), json!(workdir));
		}
		if !secret_env.is_empty() {
			// Carried over the bearer-authenticated cluster channel, like the
			// memory image; the peer keeps them in memory only.
			params.insert("secrets".to_owned(), json!([{ "name": "carried", "values": secret_env }]));
		}
		let block_network = params
			.get("block_network")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		if !block_network {
			let network = network_spec
				.filter(|spec| spec.is_object())
				.or_else(|| {
					params
						.get("network")
						.filter(|spec| spec.is_object())
						.cloned()
				})
				.ok_or_else(|| {
					EngineError::unsupported(format!(
						"networked sandbox '{sid}' is missing live host-network restore state"
					))
				})?;
			if let Some(ports) = network.get("ports").filter(|ports| ports.is_array()) {
				params.insert("ports".to_owned(), ports.clone());
			}
			params.insert("network".to_owned(), network);
		}
		for key in [
			"status",
			"pid",
			"api_sock",
			"agent_sock",
			"console_log",
			"idempotency_key",
			"env_names",
			"secret_names",
		] {
			params.remove(key);
		}
		Ok((record, params))
	}

	/// Build a peer-pullable checkpoint + restore params for replication and
	/// migration pre-copy. The source VM keeps running (it pauses only for
	/// the dump); volume data is captured into the checkpoint before the
	/// content digest so it travels with the bundle.
	fn mesh_checkpoint_for(
		&self,
		sid: &str,
		kind: &str,
		track: bool,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		let t0 = std::time::Instant::now();
		let (record, params) = self.mesh_checkpoint_params(sid)?;
		let snapshot = format!("{kind}-{sid}-{}", unix_millis());
		let vm = self.sandbox(sid);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let snapshot_dir = self.snapshot_machine(
			&vm,
			&snapshot,
			true,
			disk.as_deref(),
			&self.snapshot_root(),
			track,
		)?;
		let snapshot_ms = t0.elapsed().as_millis();
		stamp_checkpoint_rootfs(self.home(), &snapshot_dir, record.detail.as_object())?;
		stamp_checkpoint_marker(&snapshot_dir, record.detail.as_object())?;
		capture_checkpoint_volumes(self.home(), &snapshot_dir, record.detail.get("volumes"))?;
		ensure_checkpoint_template_present(&snapshot_dir)?;
		let stamp_ms = t0.elapsed().as_millis() - snapshot_ms;
		let digest = image::cas::index_template(&snapshot_dir, None)?;
		tracing::info!(
			sid,
			kind,
			snapshot_ms = snapshot_ms as u64,
			stamp_ms = stamp_ms as u64,
			index_ms = (t0.elapsed().as_millis() - snapshot_ms - stamp_ms) as u64,
			"checkpoint timings"
		);
		Ok(crate::mesh::reconciler::ReplicatePreparation {
			digest,
			snapshot_dir,
			params: Value::Object(params),
		})
	}

	/// Non-destructively checkpoint a live sandbox for HA replication; the
	/// source VM keeps running.
	pub(crate) fn mesh_replicate_prepare(
		&self,
		sid: &str,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		let prep = self.mesh_checkpoint_for(sid, "replica", false)?;
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("checkpoint_ts".to_owned(), json!(unix_time()))]),
		)?;
		Ok(prep)
	}

	/// Live-migration phase 1 (pre-copy): checkpoint the running sandbox for
	/// a peer pull; the source keeps running. Follow with
	/// [`Self::mesh_migrate_finalize`] once the target holds the bulk image.
	pub(crate) fn mesh_migrate_precopy(
		&self,
		sid: &str,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		self.mesh_checkpoint_for(sid, "migrate", true)
	}

	/// Live-migration phase 2: pause the source and capture a delta
	/// checkpoint against the pre-copy `base_dir` — changed RAM pages (the
	/// VMM's delta snapshot), changed disk blocks (`rootfs-delta.bin`), and
	/// fresh volume trees — then stop the source exactly at the captured
	/// state.
	///
	/// Any capture failure resumes the source and removes the partial delta;
	/// once every artifact is durable the source is stopped and the returned
	/// checkpoint is the sole authority. Follow with
	/// [`Self::mesh_migrate_commit`] once the target confirms, or
	/// [`Self::mesh_migrate_abort`] to restore the source locally.
	pub(crate) fn mesh_migrate_finalize(
		&self,
		sid: &str,
		base_dir: &Path,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		let (record, params) = self.mesh_checkpoint_params(sid)?;
		let base_name = base_dir
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| {
				EngineError::engine(format!(
					"pre-copy checkpoint {} has no usable directory name",
					base_dir.display()
				))
			})?;
		let snapshot = format!("migrate-{sid}-{}", unix_millis());
		let dir = self.snapshot_dir(&snapshot);
		let vm = self.sandbox(sid);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let mut control = control_for_vm(&vm)?;
		control.pause()?;
		let captured = (|| {
			control.snapshot(&snapshot, Some(base_name), false)?;
			if let Some(disk) = disk.as_deref() {
				let base_rootfs = base_dir.join("rootfs.img");
				if base_rootfs.is_file() {
					diskdelta::write_disk_delta(
						&base_rootfs,
						disk,
						&dir.join(diskdelta::DISK_DELTA_FILE),
					)?;
				}
			}
			capture_checkpoint_volumes(self.home(), &dir, record.detail.get("volumes"))?;
			stamp_checkpoint_marker(&dir, record.detail.as_object())?;
			image::cas::index_template(&dir, None)
		})();
		let digest = match captured {
			Ok(digest) => digest,
			Err(err) => {
				// The source must keep running when finalize fails; a partial
				// delta is useless without the stop that never came.
				let _ = control.resume();
				let _ = fs::remove_dir_all(&dir);
				return Err(err);
			},
		};
		drop(control);
		// Point of no return: the delta checkpoint is durable, so stop the
		// source exactly at it. Route the stop through engine teardown so
		// TAP/lease and volume locks are released, not just the VMM process.
		let rc = self.teardown(&record);
		self.persist_status(sid, "stopped", rc, None)?;
		Ok(crate::mesh::reconciler::ReplicatePreparation {
			digest,
			snapshot_dir: dir,
			params: Value::Object(params),
		})
	}

	/// Finalize a successful migration: drop the stopped source, its local
	/// record, and both transient checkpoints (pre-copy base + final delta).
	/// The local record MUST go — the mesh router treats any local record as
	/// owned-here and would refuse to proxy to the new owner — so the
	/// registry entry is force-dropped even if teardown was partial.
	pub(crate) fn mesh_migrate_commit(
		&self,
		sid: &str,
		base_dir: &Path,
		base_digest: &str,
		delta_dir: &Path,
		delta_digest: &str,
	) -> Result<()> {
		if <Self as EngineApi>::remove(self, sid).is_err() {
			self.inner.registry.remove(sid);
		}
		self.mesh_drop_checkpoint(delta_digest, delta_dir, true)?;
		self.mesh_drop_checkpoint(base_digest, base_dir, true)
	}

	/// Roll back a failed live migration by restoring the source locally.
	/// The source was stopped exactly at the delta checkpoint, so re-creating
	/// it from that checkpoint resumes the VM with no lost work. The VM's
	/// final disk image still exists locally and is adopted as the
	/// checkpoint's `rootfs.img` (cheaper than replaying the block delta).
	/// Both checkpoint directories are kept — the delta's memory chain
	/// resolves through the pre-copy base as a sibling — and only their
	/// peer-pullable CAS pointers are dropped.
	pub(crate) fn mesh_migrate_abort(
		&self,
		sid: &str,
		base_digest: &str,
		delta_dir: &Path,
		delta_digest: &str,
		mut params: Map<String, Value>,
	) -> Result<Value> {
		let rootfs = delta_dir.join("rootfs.img");
		if !rootfs.is_file() {
			let live = self
				.sandbox(sid)
				.rootfs_img()
				.ok()
				.filter(|path| path.is_file());
			let Some(live) = live else {
				return Err(EngineError::engine(format!(
					"cannot restore {sid}: its disk image is gone and the delta checkpoint {} carries \
					 no rootfs.img",
					delta_dir.display()
				)));
			};
			fs::copy(live, &rootfs)?;
		}
		if <Self as EngineApi>::remove(self, sid).is_err() {
			// The re-create below must not self-conflict with a half-removed
			// record of the same sid.
			self.inner.registry.remove(sid);
		}
		params.insert("template".to_owned(), json!(delta_dir.to_string_lossy()));
		let view = self.mesh_create_from_params(params)?;
		image::cas::drop_pointer(delta_digest)?;
		image::cas::drop_pointer(base_digest)?;
		Ok(view)
	}

	/// Drop a local replication checkpoint after peers pulled it: un-advertise
	/// the CAS pointer and delete the transient snapshot directory. Peer pulls
	/// happen synchronously inside the receive route, so once a replication
	/// cycle finishes the bundle is no longer needed locally.
	pub(crate) fn mesh_replicate_cleanup(&self, digest: &str, snapshot_dir: &Path) -> Result<()> {
		self.mesh_drop_checkpoint(digest, snapshot_dir, true)
	}

	/// Un-advertise a checkpoint's CAS pointer; optionally delete its directory.
	#[allow(
		clippy::unused_self,
		reason = "checkpoint cleanup is kept beside related Engine mesh helpers"
	)]
	fn mesh_drop_checkpoint(
		&self,
		digest: &str,
		snapshot_dir: &Path,
		delete_dir: bool,
	) -> Result<()> {
		image::cas::drop_pointer(digest)?;
		if delete_dir && snapshot_dir.is_dir() {
			fs::remove_dir_all(snapshot_dir)?;
		}
		Ok(())
	}

	/// Materialize carried volumes, then create the sandbox from the
	/// checkpoint. Port of Python `Engine.restore_from_template`: validates the
	/// checkpoint's network flavor against this host, refuses volume-name
	/// collisions, and rolls back freshly-materialized volume directories if
	/// the create fails so a transient restore error neither leaks host state
	/// nor poisons the collision guard on the next attempt.
	pub(crate) fn mesh_restore_from_template(
		&self,
		mut params: Map<String, Value>,
		template_dir: &Path,
		_quorum_ok: bool,
	) -> Result<Value> {
		validate_network_restore(&params)?;
		let created = materialize_checkpoint_volumes(
			self.home(),
			template_dir,
			params.get("volumes").and_then(Value::as_object),
		)?;
		params.insert("template".to_owned(), json!(template_dir.to_string_lossy()));
		match self.mesh_create_from_params(params) {
			Ok(view) => Ok(view),
			Err(err) => {
				for path in created {
					let _ = fs::remove_dir_all(path);
				}
				Err(err)
			},
		}
	}

	fn mesh_update_detail_fields(&self, sid: &str, fields: Map<String, Value>) -> Result<()> {
		self.get_record(sid, false)?;
		let vm = self.sandbox(sid);
		vm.save_meta(fields.clone())?;
		self.inner.registry.update(sid, |record| {
			if !record.detail.is_object() {
				record.detail = Value::Object(Map::new());
			}
			if let Some(detail) = record.detail.as_object_mut() {
				for (key, value) in fields {
					detail.insert(key, value);
				}
			}
		});
		Ok(())
	}

	/// Test constructor: aligns `VMON_HOME` with the configured home under the
	/// process-wide test lock and keeps it held for the returned guard's
	/// lifetime, so env-resolved helpers (CAS, `MicroVm` paths) stay isolated
	/// per test.
	#[cfg(test)]
	fn new_test(config: ServeConfig) -> (Self, crate::home::test_home::HomeGuard) {
		let guard = crate::home::test_home::set(&config.home);
		(Self::new(config).expect("test engine constructs"), guard)
	}

	#[cfg(test)]
	fn insert_test_record(&self, record: VmRecord) {
		self.inner.registry.insert(record);
	}
}

impl TemplateBooter for Engine {
	fn boot_verify_and_snapshot(&self, spec: &TemplateSpec) -> Result<()> {
		let old = self.sandbox(&spec.vm_name);
		if self.sandbox_is_running(&old).unwrap_or(false) {
			let _ = self.stop_sandbox(&old, true);
		}
		let _ = self.remove_sandbox(&old);
		let (tap_name, guest_config) = if spec.tap_slot {
			let config = net::allocate_guest_config(&spec.vm_name)?;
			net::setup_tap(&config.tap, &config.guest_ip, &config.host_ip, config.prefix, None, None)?;
			(Some(config.tap.clone()), Some(config))
		} else {
			(None, None)
		};
		let vm = self.sandbox(&spec.vm_name);
		let launch_result = (|| -> Result<()> {
			let mut launch = LaunchSpec::boot_rootfs(
				vm.api_sock(),
				image::assets::default_kernel()?,
				&spec.rootfs_ext4,
			)
			.with_agent_sock(vm.dir().join("agent.sock"))
			.with_mem_mib(spec.memory)
			.with_cpus(spec.cpus)
			.with_rng()
			.with_snapshot_root(&spec.snapshot_root);
			if spec.user_net {
				launch = launch.with_user_net();
			}
			if let Some(tap) = &tap_name {
				launch = launch.with_tap(tap.clone());
			}
			if let Some(fs_dir) = &spec.fs_dir {
				launch = launch.with_fs_share("host", fs_dir);
			}
			for volume in &spec.volumes {
				launch =
					launch.with_volume(VolumeMount::new(&volume.tag, &volume.dir, volume.readonly)?);
			}
			self.launch_sandbox(&vm, &launch)?;
			Self::agent_for_vm(&vm, Duration::from_secs(spec.timeout))?
				.ping(Duration::from_secs(spec.timeout))?;
			self.snapshot_machine(
				&vm,
				&spec.template_name,
				false,
				Some(&spec.rootfs_ext4),
				&spec.snapshot_root,
				false,
			)?;
			Ok(())
		})();
		let _ = self.remove_sandbox(&vm);
		if let Some(config) = guest_config {
			let _ = net::teardown_tap(
				&config.tap,
				Some(&config.guest_ip),
				Some(&config.host_ip),
				config.prefix,
				None,
				None,
			);
			let _ = net::release_guest_config(&spec.vm_name);
		}
		launch_result
	}
}

impl EngineApi for Engine {
	fn create(&self, params: SandboxCreate) -> Result<Value> {
		if let Some(key) = params
			.idempotency_key
			.as_deref()
			.filter(|key| !key.is_empty())
			&& let Some(name) = self.inner.registry.find_by_idempotency_key(key)
			&& let Some(record) = self.inner.registry.get(&name)
		{
			if record.status != "terminated" {
				return Ok(record.view());
			}
			self.inner.registry.remove_idempotency_for(key, &name);
		}
		let request_time = Instant::now();
		let mut plan = self.prepare_create(params)?;
		self.publish_event(
			"creating",
			Map::from_iter([
				("id".to_owned(), json!(plan.sid)),
				("name".to_owned(), json!(plan.sid)),
				("status".to_owned(), json!("creating")),
				("tags".to_owned(), json!(plan.tags)),
			]),
		);
		let (vm, runtime) = match self.launch_create(&mut plan) {
			Ok(result) => result,
			Err(err) => {
				self.publish_event(
					"failed",
					Map::from_iter([
						("id".to_owned(), json!(plan.sid)),
						("name".to_owned(), json!(plan.sid)),
						("status".to_owned(), json!("failed")),
						("code".to_owned(), json!(err.code.as_str())),
						("error".to_owned(), json!(&err.message)),
					]),
				);
				drop(plan);
				return Err(err);
			},
		};
		let now = unix_time();
		let meta = vm.meta()?;
		let mut detail = meta.clone();
		detail.insert("image".to_owned(), json!(plan.params.image));
		detail.insert(
			"template".to_owned(),
			json!(
				plan
					.params
					.template
					.clone()
					.unwrap_or_else(|| plan.template_dir.to_string_lossy().into_owned())
			),
		);
		detail.insert("tags".to_owned(), json!(plan.tags));
		detail.insert("cpus".to_owned(), json!(plan.params.cpus));
		detail.insert("memory".to_owned(), json!(plan.params.memory));
		detail.insert("disk_mb".to_owned(), json!(plan.params.disk_mb));
		detail.insert("block_network".to_owned(), json!(plan.params.block_network));
		detail.insert("egress_allow".to_owned(), json!(plan.params.egress_allow));
		detail.insert("egress_allow_domains".to_owned(), json!(plan.params.egress_allow_domains));
		detail.insert("inbound_cidr_allowlist".to_owned(), json!(plan.params.inbound_cidr_allowlist));
		detail.insert("pool_size".to_owned(), json!(plan.params.pool_size));
		if let Some(fs_dir) = &plan.params.fs_dir {
			// Host-local share: checkpointing/migration must refuse this sandbox.
			detail.insert("fs_dir".to_owned(), json!(fs_dir));
		}
		detail.insert("volumes".to_owned(), volumes_meta(&plan.volume_specs));
		detail.insert("s3_mounts".to_owned(), s3_mounts_meta(&plan.s3_specs));
		detail.insert(
			"create_latency_ms".to_owned(),
			json!(request_time.elapsed().as_secs_f64() * 1000.0),
		);
		detail.insert("ha".to_owned(), json!(plan.ha));
		detail.insert("restart_policy".to_owned(), json!(plan.restart_policy));
		if let Some(command) = &plan.params.command {
			detail.insert("command".to_owned(), json!(command));
		}
		if let Some(key) = plan
			.params
			.idempotency_key
			.as_deref()
			.filter(|key| !key.is_empty())
		{
			detail.insert("idempotency_key".to_owned(), json!(key));
			let mut update = Map::new();
			update.insert("idempotency_key".to_owned(), json!(key));
			let _ = vm.save_meta(update);
		}
		let record = VmRecord {
			id:            plan.sid.clone(),
			name:          plan.sid.clone(),
			status:        "running".to_owned(),
			pid:           meta
				.get("pid")
				.and_then(Value::as_i64)
				.and_then(|pid| i32::try_from(pid).ok()),
			source:        plan
				.params
				.image
				.clone()
				.or_else(|| Some(plan.template_dir.to_string_lossy().into_owned())),
			created_at:    now,
			timeout:       plan.timeout_secs.map(|secs| secs as f64),
			detail:        Value::Object(detail),
			tags:          plan.tags.clone(),
			last_active:   now,
			terminated_at: None,
			error:         None,
		};
		self.inner.runtimes.lock().insert(plan.sid.clone(), runtime);
		self
			.inner
			.registry
			.insert_persisted(self.home(), record.clone())?;
		if let Some(key) = plan
			.params
			.idempotency_key
			.as_deref()
			.filter(|key| !key.is_empty())
		{
			self.inner.registry.record_idempotency(&plan.sid, key);
		}
		self.inc_counter("created");
		{
			let mut latency = self.inner.latency.lock();
			latency.sum_ms = request_time
				.elapsed()
				.as_secs_f64()
				.mul_add(1000.0, latency.sum_ms);
			latency.count += 1;
		}
		self.publish_record_event("created", &record);
		self.publish_record_event("ready", &record);
		if let Some(command) = plan
			.params
			.command
			.clone()
			.filter(|command| !command.is_empty())
		{
			self.start_entry_command(plan.sid.clone(), command)?;
		}
		Ok(record.view())
	}

	fn list(&self, tags: Option<HashMap<String, String>>) -> Result<Vec<Value>> {
		let records = self.inner.registry.list();
		Ok(records
			.into_iter()
			// Refresh liveness so self-terminated VMs (timeout, poweroff) list
			// with their final status, mirroring Python's ps() refresh.
			.map(|record| self.refresh_record_status(record.clone()).unwrap_or(record))
			.filter(|record| {
				let Some(tags) = &tags else {
					return true;
				};
				let available = record
					.detail
					.get("tags")
					.and_then(Value::as_object)
					.map(|object| {
						object
							.iter()
							.filter_map(|(key, value)| value.as_str().map(|value| (key.as_str(), value)))
							.collect::<HashMap<_, _>>()
					})
					.unwrap_or_default();
				tags.iter().all(|(key, value)| {
					available
						.get(key.as_str())
						.is_some_and(|found| found == value)
				})
			})
			.map(|record| record.view())
			.collect())
	}

	fn get(&self, id: &str) -> Result<Value> {
		Ok(self.get_record(id, false)?.view())
	}

	fn stop(&self, id: &str) -> Result<Value> {
		self.stop_with_returncode(id, None)
	}

	fn stop_with_returncode(&self, id: &str, returncode: Option<i64>) -> Result<Value> {
		let record = self.get_record(id, false)?;
		if record.status == "terminated" {
			return Ok(json!({ "name": id, "status": record.status }));
		}
		let teardown_rc = self.teardown(&record);
		self.persist_status(id, "stopped", returncode.or(teardown_rc), None)?;
		let record = self.inner.registry.get(id).unwrap_or(record);
		self.publish_record_event("stopped", &record);
		Ok(json!({ "name": id, "status": "stopped" }))
	}

	fn remove(&self, id: &str) -> Result<Value> {
		let record = self.get_record(id, false)?;
		let _ = self.teardown(&record);
		self.remove_sandbox(&self.sandbox(id))?;
		self.inner.registry.remove(id);
		self.publish_record_event("removed", &record);
		Ok(json!({ "name": id, "removed": true }))
	}

	fn terminate(&self, id: &str, reason: &str) -> Result<Value> {
		let record = self.get_record(id, true)?;
		let returncode = self.teardown(&record);
		let terminated_at = unix_time();
		self.persist_status(id, "terminated", returncode, Some(terminated_at))?;
		let oom = detect_oom(id);
		let actual_reason = if oom { "oom" } else { reason };
		let mut update = Map::new();
		update.insert("terminated_reason".to_owned(), json!(actual_reason));
		update.insert("oom".to_owned(), json!(oom));
		let _ = self.sandbox(id).save_meta(update);
		self
			.inner
			.registry
			.set_detail_field(id, "terminated_reason", json!(actual_reason));
		if oom {
			self.inner.registry.set_detail_field(id, "oom", json!(true));
		}
		let record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		self.inc_counter("terminated");
		self.publish_record_event("terminated", &record);
		Ok(record.view())
	}

	fn pause(&self, id: &str) -> Result<Value> {
		let record = self.get_record(id, true)?;
		let result = control_for_vm(&self.sandbox(id))?.pause()?;
		self.publish_record_event("paused", &record);
		Ok(result)
	}

	fn resume(&self, id: &str) -> Result<Value> {
		let record = self.get_record(id, true)?;
		let result = control_for_vm(&self.sandbox(id))?.resume()?;
		self.publish_record_event("resumed", &record);
		Ok(result)
	}

	fn extend(&self, id: &str, secs: u64) -> Result<Value> {
		self.get_record(id, true)?;
		let deadline = control_for_vm(&self.sandbox(id))?.extend(secs)?;
		self.inner.registry.update(id, |record| {
			record.timeout = Some(secs as f64);
			record.touch();
		});
		let mut update = Map::new();
		update.insert("timeout_secs".to_owned(), json!(secs));
		let _ = self.sandbox(id).save_meta(update);
		if let Some(state) = self.inner.runtimes.lock().get_mut(id) {
			if let Some(stop) = state.timeout_stop.take() {
				let _ = stop.send(());
			}
			state.timeout_stop = Some(start_timeout_watchdog(
				id.to_owned(),
				secs,
				Arc::clone(&self.inner.sandbox_runtime),
			));
		}
		Ok(json!({ "deadline_unix": deadline }))
	}

	fn metrics(&self, id: &str) -> Result<Value> {
		self.get_record(id, true)?;
		control_for_vm(&self.sandbox(id))?.metrics()
	}

	fn logs(&self, id: &str) -> Result<Vec<u8>> {
		self.get_record(id, false)?;
		Ok(fs::read(self.sandbox(id).log_path()).unwrap_or_default())
	}

	fn logs_follow(&self, id: &str) -> Result<Receiver<Vec<u8>>> {
		self.get_record(id, false)?;
		let name = id.to_owned();
		let path = self.sandbox(id).log_path().to_path_buf();
		let (tx, rx) = flume::unbounded();
		let registry = Arc::clone(&self.inner);
		thread::Builder::new()
			.name(format!("vmon-log-follow-{id}"))
			.spawn(move || {
				let mut offset = 0;
				loop {
					if let Ok(mut file) = fs::File::open(&path)
						&& file.seek(SeekFrom::Start(offset)).is_ok()
					{
						let mut buf = Vec::new();
						if io::Read::read_to_end(&mut file, &mut buf).is_ok() && !buf.is_empty() {
							offset += buf.len() as u64;
							if tx.send(buf).is_err() {
								return;
							}
						}
					}
					let terminal = registry
						.registry
						.get(&name)
						.is_some_and(|record| record.status != "running")
						|| {
							let vm = registry.sandbox_runtime.sandbox(&name);
							!registry.sandbox_runtime.is_running(&vm).unwrap_or(false)
						};
					if terminal {
						return;
					}
					thread::sleep(LOG_FOLLOW_POLL);
				}
			})?;
		Ok(rx)
	}

	fn exec_capture(&self, id: &str, req: ExecRequest) -> Result<ExecCapture> {
		if req.cmd.is_empty() {
			return Err(EngineError::invalid("exec cmd must not be empty"));
		}
		self.get_record(id, true)?;
		self.inc_counter("exec");
		let timeout = clamp_exec_timeout(req.timeout);
		let agent = self.agent_for(id)?;
		let env = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.map_or_else(BTreeMap::new, |state| merged_env(state, req.env.as_ref()));
		let fallback_workdir = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.and_then(|state| state.workdir.clone());
		let workdir = req.workdir.as_deref().or(fallback_workdir.as_deref());
		let session =
			agent.exec(&req.cmd, workdir.map(Path::new), Some(&env), req.tty, Some(timeout))?;
		let stdout = session.stdout.iter().flatten().collect();
		let stderr = session.stderr.iter().flatten().collect();
		let exit = session.wait(Some(timeout))?;
		Ok(ExecCapture { exit, stdout, stderr })
	}

	fn exec_stream(&self, id: &str, req: ExecRequest) -> Result<ExecStream> {
		if req.cmd.is_empty() {
			return Err(EngineError::invalid("exec cmd must not be empty"));
		}
		self.get_record(id, true)?;
		self.inc_counter("exec");
		let agent = self.agent_for(id)?;
		let env = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.map_or_else(BTreeMap::new, |state| merged_env(state, req.env.as_ref()));
		let fallback_workdir = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.and_then(|state| state.workdir.clone());
		let workdir = req.workdir.as_deref().or(fallback_workdir.as_deref());
		let session = agent.exec(
			&req.cmd,
			workdir.map(Path::new),
			Some(&env),
			req.tty,
			req.timeout.map(Duration::from_secs_f64),
		)?;
		let parts = session.split();
		let stdout = bridge_bytes(parts.stdout);
		let stderr = bridge_bytes(parts.stderr);
		let exit = bridge_exit(parts.exit);
		Ok(ExecStream {
			control: Box::new(EngineExecControl { handle: parts.control }),
			stdout,
			stderr,
			exit,
		})
	}

	fn shell_start(&self, params: Value) -> Result<ShellSession> {
		let ref_name = params.get("ref").and_then(Value::as_str);
		let image = params
			.get("image")
			.and_then(Value::as_str)
			.map(ToOwned::to_owned);
		let cmd = params
			.get("cmd")
			.and_then(Value::as_array)
			.map(|items| {
				items
					.iter()
					.filter_map(Value::as_str)
					.map(ToOwned::to_owned)
					.collect::<Vec<_>>()
			})
			.filter(|cmd| !cmd.is_empty())
			.unwrap_or_else(|| {
				DEFAULT_SHELL_ARGV
					.iter()
					.map(|part| (*part).to_owned())
					.collect()
			});
		if let Some(ref_name) = ref_name
			&& self.inner.registry.get(ref_name).is_some()
			&& self.get_record(ref_name, true).is_ok()
		{
			let stream =
				self.exec_stream(ref_name, ExecRequest { cmd, tty: true, ..ExecRequest::default() })?;
			return Ok(ShellSession { name: ref_name.to_owned(), stream, ephemeral: false });
		}
		let mut create = SandboxCreate {
			image: image
				.or_else(|| ref_name.map(ToOwned::to_owned))
				.or_else(|| Some(DEFAULT_SHELL_IMAGE.to_owned())),
			cpus: params.get("cpus").and_then(Value::as_u64).unwrap_or(1) as u32,
			memory: params.get("mem").and_then(Value::as_u64).unwrap_or(512) as u32,
			disk_mb: params
				.get("disk_mb")
				.and_then(Value::as_u64)
				.unwrap_or(1024) as u32,
			timeout: Some(
				params
					.get("timeout")
					.and_then(Value::as_f64)
					.unwrap_or(300.0),
			),
			block_network: true,
			..SandboxCreate::default()
		};
		if let Some(ref_name) = ref_name
			&& self.home().templates_dir().join(ref_name).exists()
		{
			create.template = Some(ref_name.to_owned());
			create.image = None;
		}
		let view = self.create(create)?;
		let name = view
			.get("name")
			.and_then(Value::as_str)
			.ok_or_else(|| EngineError::engine("shell setup did not return a VM name"))?
			.to_owned();
		let stream =
			self.exec_stream(&name, ExecRequest { cmd, tty: true, ..ExecRequest::default() })?;
		Ok(ShellSession { name, stream, ephemeral: true })
	}

	fn shell_cleanup(&self, name: &str) {
		let _ = self.remove(name);
	}

	fn file_read(&self, id: &str, path: &str) -> Result<Vec<u8>> {
		self.inc_counter("file_read");
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_read(Path::new(path), AGENT_REQUEST_TIMEOUT)
	}

	fn file_write(&self, id: &str, path: &str, data: &[u8]) -> Result<()> {
		self.inc_counter("file_write");
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_write(Path::new(path), data, AGENT_REQUEST_TIMEOUT)?;
		Ok(())
	}

	fn file_delete(&self, id: &str, path: &str, recursive: bool) -> Result<()> {
		self.inc_counter("file_delete");
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_remove(Path::new(path), recursive, AGENT_REQUEST_TIMEOUT)?;
		Ok(())
	}

	fn file_list(&self, id: &str, path: &str) -> Result<Value> {
		self.get_record(id, true)?;
		Ok(json!(
			self
				.agent_for(id)?
				.fs_list(Path::new(path), AGENT_REQUEST_TIMEOUT)?
		))
	}

	fn file_stat(&self, id: &str, path: &str) -> Result<Value> {
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_stat(Path::new(path), AGENT_REQUEST_TIMEOUT)
	}

	fn network_get(&self, id: &str) -> Result<Value> {
		let record = self.get_record(id, false)?;
		Ok(json!({
			"block_network": record.detail.get("block_network").cloned().unwrap_or(Value::Null),
			"egress_allow": record.detail.get("egress_allow").cloned().unwrap_or(Value::Null),
			"egress_allow_domains": record.detail.get("egress_allow_domains").cloned().unwrap_or(Value::Null),
			"inbound_cidr_allowlist": record.detail.get("inbound_cidr_allowlist").cloned().unwrap_or(Value::Null),
		}))
	}

	fn network_set(&self, id: &str, policy: NetworkBody) -> Result<Value> {
		validate_cidrs("cidr_allow", policy.cidr_allow.as_deref())?;
		validate_domains("domain_allow", policy.domain_allow.as_deref())?;
		let mut runtimes = self.inner.runtimes.lock();
		if let Some(state) = runtimes.get_mut(id)
			&& let Some(network) = &state.network
		{
			let allow_list = if policy.block_network == Some(true) {
				Some(Vec::new())
			} else {
				policy
					.cidr_allow
					.clone()
					.or_else(|| state.network_policy.egress_allow.clone())
			};
			let domain_list = policy
				.domain_allow
				.clone()
				.or_else(|| state.network_policy.egress_allow_domains.clone());
			net::setup_tap(
				&network.guest_config.tap,
				&network.guest_config.guest_ip,
				&network.guest_config.host_ip,
				network.guest_config.prefix,
				allow_list.as_deref(),
				domain_list.as_deref(),
			)?;
			state.network_policy.block_network = policy.block_network.or(Some(false));
			state.network_policy.egress_allow = allow_list;
			state.network_policy.egress_allow_domains = domain_list;
		}
		drop(runtimes);
		if let Some(value) = policy.block_network {
			self
				.inner
				.registry
				.set_detail_field(id, "block_network", json!(value));
		}
		if let Some(value) = policy.cidr_allow {
			self
				.inner
				.registry
				.set_detail_field(id, "egress_allow", json!(value));
		}
		if let Some(value) = policy.domain_allow {
			self
				.inner
				.registry
				.set_detail_field(id, "egress_allow_domains", json!(value));
		}
		self.network_get(id)
	}

	fn tunnels(&self, id: &str) -> Result<Value> {
		self.get_record(id, false)?;
		let mut runtimes = self.inner.runtimes.lock();
		let state = runtimes.entry(id.to_owned()).or_default();
		if state.connect_token.is_none() {
			state.connect_token = Some(random_hex(32));
		}
		let tunnels = state
			.network
			.as_ref()
			.map(SandboxNetwork::tunnels)
			.unwrap_or_default();
		Ok(json!({ "tunnels": tunnels_json(&tunnels), "connect_token": state.connect_token }))
	}

	fn tunnel_target(&self, id: &str, port: u16) -> Result<(String, u16)> {
		self.get_record(id, false)?;
		self
			.inner
			.runtimes
			.lock()
			.get(id)
			.and_then(|state| state.network.as_ref())
			.and_then(|network| network.tunnels().get(&port).cloned())
			.ok_or_else(|| EngineError::engine("no tunnel for sandbox port"))
	}

	fn snapshot(&self, id: &str, name: Option<String>, stop: bool) -> Result<Value> {
		let record = self.get_record(id, true)?;
		let snapshot = name.unwrap_or_else(|| format!("{}-{}", id, unix_millis()));
		let vm = self.sandbox(id);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let dir = self.snapshot_machine(
			&vm,
			&snapshot,
			!stop,
			disk.as_deref(),
			&self.snapshot_root(),
			false,
		)?;
		if let Some(s3_mounts) = record
			.detail
			.get("s3_mounts")
			.filter(|mounts| mounts.as_object().is_some_and(|mounts| !mounts.is_empty()))
		{
			fs::write(dir.join(S3_MOUNTS_FILE), serde_json::to_vec(s3_mounts)?)?;
		}
		if stop {
			let rc = self.teardown(&record);
			self.persist_status(id, "stopped", rc, None)?;
		}
		self.inc_counter("snapshot");
		self.publish_event(
			"snapshot",
			Map::from_iter([
				("id".to_owned(), json!(record.id)),
				("name".to_owned(), json!(record.name)),
				("snapshot".to_owned(), json!(snapshot)),
			]),
		);
		Ok(json!({ "snapshot": snapshot, "dir": dir.to_string_lossy() }))
	}

	fn snapshot_fs(&self, id: &str, name: Option<String>) -> Result<Value> {
		self.get_record(id, true)?;
		let image = name.unwrap_or_else(|| format!("{id}-img-{}", unix_time() as u64));
		let tmp = self.snapshot(id, Some(image.clone()), false)?;
		let src_dir = PathBuf::from(tmp.get("dir").and_then(Value::as_str).unwrap_or_default());
		let dst_dir = self.home().templates_dir().join(&image);
		if src_dir != dst_dir {
			if dst_dir.exists() {
				fs::remove_dir_all(&dst_dir)?;
			}
			fs::rename(&src_dir, &dst_dir)?;
		}
		fs::write(
			dst_dir.join("image.json"),
			serde_json::to_string_pretty(
				&json!({ "created_unix": unix_time() as u64, "ttl": 30 * 24 * 3600, "image": image }),
			)?,
		)?;
		Ok(json!({ "image": image }))
	}

	fn snapshots(&self) -> Result<Vec<String>> {
		let mut names = list_child_dirs(&self.snapshot_root());
		names.sort();
		Ok(names)
	}

	fn restore(&self, snapshot: &str, body: RestoreBody) -> Result<Value> {
		let RestoreBody { name, agent, extra } = body;
		let name = name.unwrap_or_else(|| format!("restore-{}", random_hex(12)));
		let options = resolve_snapshot_options(extra, agent)?;
		let snapshot_dir = self.require_snapshot_dir(snapshot)?;
		let s3_mounts = self.snapshot_s3_mounts(&snapshot_dir, options.s3_mounts.clone())?;
		let (_, record) = self.launch_snapshot_vm(
			snapshot,
			&snapshot_dir,
			name,
			SnapshotLaunchMode::Restore,
			&options,
			&s3_mounts,
		)?;
		self.publish_record_event("restore", &record);
		Ok(record.view())
	}

	fn fork(&self, snapshot: &str, body: ForkBody) -> Result<Value> {
		let ForkBody { count, extra } = body;
		if !(1..=MAX_FORK_CLONES).contains(&count) {
			return Err(EngineError::invalid(format!(
				"count must be between 1 and {MAX_FORK_CLONES}"
			)));
		}
		let options = resolve_snapshot_options(extra, None)?;
		let snapshot_dir = self.require_snapshot_dir(snapshot)?;
		let s3_mounts = self.snapshot_s3_mounts(&snapshot_dir, options.s3_mounts.clone())?;
		let mut launched = Vec::with_capacity(count as usize);
		for _ in 0..count {
			let name = format!("fork-{}", random_hex(12));
			match self.launch_snapshot_vm(
				snapshot,
				&snapshot_dir,
				name,
				SnapshotLaunchMode::Fork,
				&options,
				&s3_mounts,
			) {
				Ok(launched_vm) => launched.push(launched_vm),
				Err(error) => {
					for (vm, _) in launched.iter().rev() {
						self.rollback_snapshot_vm(vm);
					}
					return Err(error);
				},
			}
		}
		let clones = launched
			.iter()
			.map(|(_, record)| record.view())
			.collect::<Vec<_>>();
		for (_, record) in &launched {
			self.publish_record_event("fork", record);
		}
		self.publish_event(
			"fork",
			Map::from_iter([
				("snapshot".to_owned(), json!(snapshot)),
				("count".to_owned(), json!(count)),
			]),
		);
		Ok(json!({ "clones": clones }))
	}

	fn volume_list(&self) -> Result<Vec<String>> {
		Ok(volumes::list_volumes(self.home().root()))
	}

	fn volume_create(&self, name: &str) -> Result<()> {
		Volume::new_in_home(self.home().root(), name).map(|_| ())
	}

	fn volume_delete(&self, name: &str) -> Result<()> {
		volumes::remove_volume_in_home(self.home().root(), name)
	}

	fn pool_list(&self) -> Result<Value> {
		Ok(json!(self
			.inner
			.pools
			.list()
			.into_iter()
			.map(|(reference, stats)| {
				(reference, json!({ "ready": stats.ready, "hits": stats.hits, "misses": stats.misses, "size": stats.size }))
			})
			.collect::<Map<_, _>>()))
	}

	fn pool_set(&self, reference: &str, body: PoolPutBody) -> Result<Value> {
		let request = template_request_from_pool(reference, &body.extra);
		let cached = image::cached_template(self, &request)?;
		let key = template_key_for_cached(&cached);
		let pool = WarmPool::with_runtime(
			cached.snapshot_dir,
			body.size as usize,
			Arc::clone(&self.inner.sandbox_runtime),
		)?;
		let old = self.inner.pools.set(key.clone(), pool);
		if let Some(old) = old {
			old.shutdown();
		}
		Ok(self
			.pool_list()?
			.get(&key)
			.cloned()
			.unwrap_or_else(|| json!({})))
	}

	fn pool_delete(&self, reference: &str) -> Result<()> {
		if let Some(pool) = self.inner.pools.delete(reference) {
			pool.shutdown();
		}
		Ok(())
	}

	fn info(&self) -> Result<Value> {
		Ok(json!({
			"version": env!("CARGO_PKG_VERSION"),
			"platform": std::env::consts::OS,
			"arch": std::env::consts::ARCH,
			"backend": backend_name(),
			"capabilities": {
				"snapshots": true,
				"fork": true,
				"exec": true,
				"files": true,
				"volumes": true,
				"pools": true,
				"user_net": cfg!(target_os = "macos"),
				"tap": net::has_net_admin(),
				"mesh": true,
			}
		}))
	}

	fn subscribe_events(&self) -> Receiver<Value> {
		let (tx, rx) = flume::unbounded();
		self.inner.events.lock().push(tx);
		rx
	}

	fn prometheus_metrics(&self) -> String {
		let records = self.inner.registry.list();
		let mut statuses = BTreeMap::<String, u64>::new();
		for record in records {
			*statuses.entry(record.status).or_default() += 1;
		}
		let counters = [
			("auth_failed", self.inner.counters.auth_failed.load(Ordering::Relaxed)),
			("created", self.inner.counters.created.load(Ordering::Relaxed)),
			("exec", self.inner.counters.exec.load(Ordering::Relaxed)),
			("file_delete", self.inner.counters.file_delete.load(Ordering::Relaxed)),
			("file_read", self.inner.counters.file_read.load(Ordering::Relaxed)),
			("file_write", self.inner.counters.file_write.load(Ordering::Relaxed)),
			("idle_reaped", self.inner.counters.idle_reaped.load(Ordering::Relaxed)),
			("snapshot", self.inner.counters.snapshot.load(Ordering::Relaxed)),
			("terminated", self.inner.counters.terminated.load(Ordering::Relaxed)),
		];
		let latency = self.inner.latency.lock();
		let (pool_hits, pool_misses) = self.inner.pools.total_hits_misses();
		let mut lines = vec![
			"# HELP vmon_server_sandboxes Number of sandboxes by status.".to_owned(),
			"# TYPE vmon_server_sandboxes gauge".to_owned(),
		];
		for (status, value) in statuses {
			lines.push(format!("vmon_server_sandboxes{{status=\"{status}\"}} {value}"));
		}
		lines.extend([
			"# HELP vmon_server_events_total Server supervisor events.".to_owned(),
			"# TYPE vmon_server_events_total counter".to_owned(),
		]);
		for (name, value) in counters {
			lines.push(format!("vmon_server_events_total{{event=\"{name}\"}} {value}"));
		}
		lines.extend([
			"# HELP vmon_server_create_latency_ms_sum Total sandbox create latency in milliseconds."
				.to_owned(),
			"# TYPE vmon_server_create_latency_ms_sum counter".to_owned(),
			format!("vmon_server_create_latency_ms_sum {}", latency.sum_ms),
			"# HELP vmon_server_create_latency_ms_count Count of observed sandbox creates.".to_owned(),
			"# TYPE vmon_server_create_latency_ms_count counter".to_owned(),
			format!("vmon_server_create_latency_ms_count {}", latency.count),
			"# HELP vmon_server_pool_hits Warm-pool claim hits observed by the server.".to_owned(),
			"# TYPE vmon_server_pool_hits counter".to_owned(),
			format!("vmon_server_pool_hits {pool_hits}"),
			"# HELP vmon_server_pool_misses Warm-pool claim misses observed by the server.".to_owned(),
			"# TYPE vmon_server_pool_misses counter".to_owned(),
			format!("vmon_server_pool_misses {pool_misses}"),
		]);
		format!("{}\n", lines.join("\n"))
	}

	fn migrate(&self, _id: &str, _target: &str) -> Result<Value> {
		Err(EngineError::unsupported("mesh migrate lands with the mesh port"))
	}
}

impl crate::engine::ExecControl for EngineExecControl {
	fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
		self.handle.write_stdin(data)
	}

	fn close_stdin(&mut self) -> Result<()> {
		self.handle.close_stdin()
	}

	fn resize(&mut self, rows: u16, cols: u16) -> Result<()> {
		self.handle.resize(rows, cols)
	}

	fn kill(&mut self, signal: i32) -> Result<()> {
		self.handle.kill(signal)
	}
}

fn align_process_home(home: &Path) {
	if crate::home::state_dir() == home {
		return;
	}
	// SAFETY: `Engine::new` is called during daemon startup before worker threads
	// are spawned; Wave-A path helpers intentionally read VMON_HOME process-wide.
	unsafe {
		std::env::set_var("VMON_HOME", home);
	}
}

fn validate_local_name(kind: &str, name: &str) -> Result<()> {
	if is_safe_snapshot_name(name) {
		Ok(())
	} else {
		Err(EngineError::invalid(format!(
			"{kind} must be a 1-128 byte ASCII basename using letters, digits, '.', '_', or '-'"
		)))
	}
}

fn read_snapshot_metadata(path: &Path) -> Result<Vec<u8>> {
	let file = fs::OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
		.open(path)
		.map_err(|error| {
			EngineError::invalid(format!("opening snapshot metadata {}: {error}", path.display()))
		})?;
	if !file
		.metadata()
		.map_err(|error| {
			EngineError::invalid(format!("stat snapshot metadata {}: {error}", path.display()))
		})?
		.is_file()
	{
		return Err(EngineError::invalid(format!(
			"snapshot metadata {} is not a regular file",
			path.display()
		)));
	}
	let mut bytes = Vec::new();
	Read::take(file, MAX_SNAPSHOT_METADATA_BYTES + 1).read_to_end(&mut bytes)?;
	if bytes.len() as u64 > MAX_SNAPSHOT_METADATA_BYTES {
		return Err(EngineError::invalid(format!(
			"snapshot metadata {} exceeds {MAX_SNAPSHOT_METADATA_BYTES} bytes",
			path.display()
		)));
	}
	Ok(bytes)
}

fn require_positive(name: &str, value: u64) -> Result<()> {
	if value == 0 {
		Err(EngineError::invalid(format!("{name} must be positive")))
	} else {
		Ok(())
	}
}

fn validate_ports(ports: Option<&[u16]>) -> Result<()> {
	for port in ports.unwrap_or(&[]) {
		if *port == 0 {
			return Err(EngineError::invalid("ports must be TCP port numbers from 1 to 65535"));
		}
	}
	Ok(())
}

fn validate_cidrs(name: &str, cidrs: Option<&[String]>) -> Result<()> {
	for cidr in cidrs.unwrap_or(&[]) {
		let (addr, prefix) = cidr
			.split_once('/')
			.map_or((cidr.as_str(), None), |(addr, prefix)| (addr, Some(prefix)));
		let ip = addr.parse::<IpAddr>().map_err(|_| {
			EngineError::invalid(format!("{name} entries must be valid CIDR networks"))
		})?;
		if let Some(prefix) = prefix {
			let prefix = prefix.parse::<u8>().map_err(|_| {
				EngineError::invalid(format!("{name} entries must be valid CIDR networks"))
			})?;
			let max = if ip.is_ipv4() { 32 } else { 128 };
			if prefix > max {
				return Err(EngineError::invalid(format!(
					"{name} entries must be valid CIDR networks"
				)));
			}
		}
	}
	Ok(())
}

fn validate_domains(name: &str, domains: Option<&[String]>) -> Result<()> {
	for domain in domains.unwrap_or(&[]) {
		if domain.trim().is_empty() {
			return Err(EngineError::invalid(format!("{name} entries must be non-empty")));
		}
	}
	Ok(())
}

fn validate_ha(value: Option<&str>) -> Result<()> {
	if let Some(value) = value
		&& !ALLOWED_HA.contains(&value)
	{
		return Err(EngineError::invalid(format!("ha must be one of: {}", ALLOWED_HA.join(", "))));
	}
	Ok(())
}

fn validate_arch(value: Option<&str>) -> Result<()> {
	if let Some(value) = value
		&& !matches!(value, "aarch64" | "x86_64")
	{
		return Err(EngineError::invalid("arch must be one of: aarch64, x86_64"));
	}
	Ok(())
}

fn restart_policy_for_ha(ha: &str) -> &'static str {
	if ha.contains("rerun") {
		"rerun"
	} else {
		"none"
	}
}

fn effective_timeout_secs(timeout_secs: Option<u64>, timeout: Option<f64>) -> Result<Option<u64>> {
	if timeout_secs == Some(0) {
		return Ok(None);
	}
	if let Some(timeout_secs) = timeout_secs {
		return Ok(Some(timeout_secs));
	}
	let secs = timeout.unwrap_or(DEFAULT_CREATE_TIMEOUT_SECS as f64);
	if !secs.is_finite() || secs < 0.0 {
		return Err(EngineError::invalid("timeout must be non-negative"));
	}
	Ok(Some(secs as u64))
}

fn resolve_snapshot_options(
	extra: HashMap<String, Value>,
	agent: Option<bool>,
) -> Result<ResolvedSnapshotOptions> {
	let options = serde_json::from_value::<SnapshotOptions>(json!(extra)).map_err(|error| {
		EngineError::invalid(format!("unsupported or invalid snapshot override: {error}"))
	})?;
	let secrets = parse_secrets(options.secrets)?;
	let timeout_secs = if options.timeout_secs.is_some() || options.timeout.is_some() {
		effective_timeout_secs(options.timeout_secs, options.timeout)?
	} else {
		None
	};
	Ok(ResolvedSnapshotOptions {
		agent: agent.or(options.agent).unwrap_or(false),
		block_network: options.block_network,
		env: options.env.unwrap_or_default().into_iter().collect(),
		secret_env: merge_secret_env(&secrets),
		secret_names: secrets.into_iter().map(|secret| secret.name).collect(),
		workdir: options.workdir,
		tags: options.tags.unwrap_or_default(),
		timeout_secs,
		readiness_probe: options.readiness_probe,
		s3_mounts: options.s3_mounts,
		command: options.command,
	})
}

fn parse_volume_spec(value: &Value) -> Result<(String, bool)> {
	if let Some(name) = value.as_str() {
		return Ok((name.to_owned(), false));
	}
	let object = value
		.as_object()
		.ok_or_else(|| EngineError::invalid("volume spec must be a string or object"))?;
	let name = object
		.get("name")
		.and_then(Value::as_str)
		.filter(|name| !name.is_empty())
		.ok_or_else(|| EngineError::invalid("volume object requires a name"))?;
	Ok((
		name.to_owned(),
		object
			.get("read_only")
			.and_then(Value::as_bool)
			.unwrap_or(false),
	))
}

fn s3_credentials(mountpoint: &str, spec: &S3MountSpec) -> Result<(Option<S3Credentials>, S3Auth)> {
	let inline_requested =
		spec.access_key.is_some() || spec.secret_key.is_some() || spec.session_token.is_some();
	let access_key = spec.access_key.as_deref().filter(|value| !value.is_empty());
	let secret_key = spec.secret_key.as_deref().filter(|value| !value.is_empty());
	if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
		return Ok((
			Some(S3Credentials {
				access_key:    access_key.to_owned(),
				secret_key:    secret_key.to_owned(),
				session_token: spec.session_token.clone().filter(|value| !value.is_empty()),
			}),
			S3Auth::Inline,
		));
	}
	if inline_requested {
		return Err(EngineError::invalid(format!(
			"S3 mount {mountpoint} requires both access_key and secret_key"
		)));
	}
	if let Some(creds) = environment_s3_credentials() {
		return Ok((Some(creds), S3Auth::Env));
	}
	Ok((None, S3Auth::Anonymous))
}

fn environment_s3_credentials() -> Option<S3Credentials> {
	let access_key = std::env::var("AWS_ACCESS_KEY_ID")
		.ok()
		.filter(|value| !value.is_empty())?;
	let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
		.ok()
		.filter(|value| !value.is_empty())?;
	Some(S3Credentials {
		access_key,
		secret_key,
		session_token: std::env::var("AWS_SESSION_TOKEN")
			.ok()
			.filter(|value| !value.is_empty()),
	})
}

fn apply_restored_s3_tags(
	mounts: &mut [ResolvedS3Mount],
	volumes: &[ResolvedVolume],
	tags: &HashMap<String, String>,
) -> Result<()> {
	if mounts.len() != tags.len() {
		return Err(EngineError::invalid(
			"restored S3 mount metadata does not match the requested mounts",
		));
	}
	let mut used = volumes
		.iter()
		.map(|volume| volume.tag.clone())
		.collect::<HashSet<_>>();
	for mount in mounts {
		let tag = tags.get(&mount.mountpoint).ok_or_else(|| {
			EngineError::invalid(format!(
				"restored S3 mount metadata is missing tag for {}",
				mount.mountpoint
			))
		})?;
		if !valid_virtiofs_tag(tag) {
			return Err(EngineError::invalid(format!(
				"restored S3 mount has invalid virtio-fs tag {tag:?}"
			)));
		}
		if !used.insert(tag.clone()) {
			return Err(EngineError::invalid(format!(
				"restored S3 mount tag {tag:?} collides with another filesystem mount"
			)));
		}
		mount.tag.clone_from(tag);
		mount
			.meta
			.as_object_mut()
			.expect("S3 mount metadata is always an object")
			.insert("tag".to_owned(), json!(tag));
	}
	Ok(())
}

fn valid_virtiofs_tag(tag: &str) -> bool {
	!tag.is_empty()
		&& tag.len() <= 32
		&& tag
			.bytes()
			.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn restore_s3_mount_params(
	params: &mut Map<String, Value>,
) -> Result<Option<HashMap<String, String>>> {
	let Some(source) = params.get("s3_mounts").cloned() else {
		return Ok(None);
	};
	let mounts = source
		.as_object()
		.ok_or_else(|| EngineError::invalid("restored S3 mounts must be an object"))?;
	let has_metadata = mounts.values().any(|mount| mount.get("auth").is_some());
	if !has_metadata {
		return Ok(None);
	}
	let mut restored = Map::new();
	let mut tags = HashMap::with_capacity(mounts.len());
	for (mountpoint, mount) in mounts {
		let object = mount.as_object().ok_or_else(|| {
			EngineError::invalid(format!("restored S3 mount {mountpoint} must be an object"))
		})?;
		let uri = object
			.get("uri")
			.and_then(Value::as_str)
			.filter(|uri| !uri.is_empty())
			.ok_or_else(|| {
				EngineError::invalid(format!("restored S3 mount {mountpoint} is missing uri"))
			})?;
		let tag = object
			.get("tag")
			.and_then(Value::as_str)
			.filter(|tag| !tag.is_empty())
			.ok_or_else(|| {
				EngineError::invalid(format!("restored S3 mount {mountpoint} is missing tag"))
			})?;
		let auth = object.get("auth").and_then(Value::as_str).ok_or_else(|| {
			EngineError::invalid(format!("restored S3 mount {mountpoint} is missing auth"))
		})?;
		match auth {
			"inline" => {
				if environment_s3_credentials().is_none() {
					return Err(EngineError::invalid(format!(
						"S3 mount {mountpoint} was created with inline credentials; set \
						 AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to restore"
					)));
				}
			},
			"env" => {
				if environment_s3_credentials().is_none() {
					return Err(EngineError::invalid(format!(
						"S3 mount {mountpoint} requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to \
						 restore"
					)));
				}
			},
			"anonymous" => {},
			_ => {
				return Err(EngineError::invalid(format!(
					"restored S3 mount {mountpoint} has unknown auth mode {auth:?}"
				)));
			},
		}
		let spec = Map::from_iter([
			("uri".to_owned(), json!(uri)),
			("endpoint".to_owned(), object.get("endpoint").cloned().unwrap_or(Value::Null)),
			("region".to_owned(), object.get("region").cloned().unwrap_or(Value::Null)),
			(
				"read_only".to_owned(),
				json!(
					object
						.get("read_only")
						.and_then(Value::as_bool)
						.unwrap_or(false)
				),
			),
		]);
		if let Some(endpoint) = spec.get("endpoint")
			&& !endpoint.is_null()
			&& !endpoint.is_string()
		{
			return Err(EngineError::invalid(format!(
				"restored S3 mount {mountpoint} has invalid endpoint"
			)));
		}
		if let Some(region) = spec.get("region")
			&& !region.is_null()
			&& !region.is_string()
		{
			return Err(EngineError::invalid(format!(
				"restored S3 mount {mountpoint} has invalid region"
			)));
		}
		restored.insert(mountpoint.clone(), Value::Object(spec));
		tags.insert(mountpoint.clone(), tag.to_owned());
	}
	params.insert("s3_mounts".to_owned(), Value::Object(restored));
	Ok(Some(tags))
}

fn unique_volume_tag(base: &str, used: &mut std::collections::HashSet<String>) -> String {
	let mut stem = base
		.to_ascii_lowercase()
		.chars()
		.map(|ch| {
			if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' {
				ch
			} else {
				'_'
			}
		})
		.take(32)
		.collect::<String>();
	if stem.is_empty() {
		"vol".clone_into(&mut stem);
	}
	let mut candidate = stem.clone();
	let mut suffix = 2;
	while used.contains(&candidate) {
		let tail = format!("_{suffix}");
		candidate = format!("{}{}", &stem[..stem.len().min(32 - tail.len())], tail);
		suffix += 1;
	}
	used.insert(candidate.clone());
	candidate
}

fn parse_secrets(secrets: Option<Vec<Value>>) -> Result<Vec<Secret>> {
	secrets
		.unwrap_or_default()
		.into_iter()
		.map(Secret::from_wire)
		.collect::<Result<Vec<_>>>()
}

fn merge_secret_env(secrets: &[Secret]) -> BTreeMap<String, String> {
	let mut env = BTreeMap::new();
	for secret in secrets {
		for (key, value) in secret.env_pairs() {
			env.insert(key, value);
		}
	}
	env
}

fn image_env(
	image_spec: Option<&image::ImageConfig>,
	overrides: Option<&HashMap<String, String>>,
) -> BTreeMap<String, String> {
	let mut env = image_spec
		.map(image::ImageConfig::env_dict)
		.unwrap_or_default()
		.into_iter()
		.collect::<BTreeMap<_, _>>();
	if let Some(overrides) = overrides {
		for (key, value) in overrides {
			env.insert(key.clone(), value.clone());
		}
	}
	env
}

fn merged_env(
	state: &RuntimeState,
	overrides: Option<&HashMap<String, String>>,
) -> BTreeMap<String, String> {
	let mut env = state.env.clone();
	env.extend(state.secret_env.clone());
	if let Some(overrides) = overrides {
		for (key, value) in overrides {
			env.insert(key.clone(), value.clone());
		}
	}
	env
}

fn readiness_argv(probe: &Value) -> Vec<String> {
	if let Some(text) = probe.as_str() {
		return vec!["sh".to_owned(), "-lc".to_owned(), text.to_owned()];
	}
	probe.as_array().map_or_else(
		|| vec![probe.to_string()],
		|items| {
			items
				.iter()
				.map(|item| {
					item
						.as_str()
						.map_or_else(|| item.to_string(), ToOwned::to_owned)
				})
				.collect()
		},
	)
}

fn stamp_checkpoint_rootfs(
	home: &Home,
	snapshot_dir: &Path,
	detail: Option<&Map<String, Value>>,
) -> Result<()> {
	let rootfs = snapshot_dir.join("rootfs.img");
	if rootfs.is_file() {
		return Ok(());
	}
	let mut tried = Vec::new();
	if let Some(detail) = detail {
		for candidate in ["rootfs", "template", "restored_from"] {
			let Some(value) = detail
				.get(candidate)
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
			else {
				continue;
			};
			for source in checkpoint_rootfs_candidates(home, value) {
				let src_rootfs = if source.is_dir() {
					source.join("rootfs.img")
				} else {
					source.clone()
				};
				tried.push(src_rootfs.clone());
				if src_rootfs.is_file() {
					fs::copy(src_rootfs, &rootfs)?;
					return Ok(());
				}
			}
		}
	}
	Err(EngineError::engine(format!(
		"mesh checkpoint {} has no rootfs.img; tried {}",
		snapshot_dir.display(),
		tried
			.iter()
			.map(|path| path.display().to_string())
			.collect::<Vec<_>>()
			.join(", ")
	)))
}

fn checkpoint_rootfs_candidates(home: &Home, value: &str) -> Vec<PathBuf> {
	let source = PathBuf::from(value);
	if source.is_absolute() {
		return vec![source];
	}
	vec![source, home.templates_dir().join(value), home.root().join(value)]
}

fn stamp_checkpoint_marker(snapshot_dir: &Path, detail: Option<&Map<String, Value>>) -> Result<()> {
	let marker = snapshot_dir.join("agent-ready.json");
	if marker.is_file() {
		return Ok(());
	}
	let Some(detail) = detail else {
		return Ok(());
	};
	for candidate in ["template", "restored_from"] {
		let Some(value) = detail
			.get(candidate)
			.and_then(Value::as_str)
			.filter(|value| !value.is_empty())
		else {
			continue;
		};
		let src_marker = PathBuf::from(value).join("agent-ready.json");
		if src_marker.is_file() {
			fs::copy(src_marker, marker)?;
			break;
		}
	}
	Ok(())
}

fn ensure_checkpoint_template_present(snapshot_dir: &Path) -> Result<()> {
	let rootfs = snapshot_dir.join("rootfs.img");
	let marker = snapshot_dir.join("agent-ready.json");
	if rootfs.is_file() && marker.is_file() {
		return Ok(());
	}
	Err(EngineError::engine(format!(
		"mesh checkpoint {} is not bootable; rootfs={} marker={}",
		snapshot_dir.display(),
		rootfs.is_file(),
		marker.is_file(),
	)))
}

/// Writable volume names described by a registry record's detail.
fn writable_volume_names_from_detail(detail: &Value) -> Vec<String> {
	detail.as_object().map_or_else(Vec::new, |params| {
		crate::mesh::runtime::writable_volumes(params).unwrap_or_default()
	})
}

fn capture_checkpoint_volumes(
	home: &Home,
	snapshot_dir: &Path,
	volumes: Option<&Value>,
) -> Result<()> {
	let Some(Value::Object(volumes)) = volumes else {
		return Ok(());
	};
	let dest_root = snapshot_dir.join("volumes");
	let mut seen = std::collections::HashSet::new();
	for spec in volumes.values() {
		let Some(name) = spec
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		if !seen.insert(name.to_owned()) {
			continue;
		}
		let src = home.volumes_dir().join(name);
		if !src.is_dir() {
			return Err(EngineError::unsupported(format!(
				"volume {name:?} has no host directory to checkpoint at {}",
				src.display()
			)));
		}
		copy_tree_without_locks(&src, &dest_root.join(name))?;
	}
	Ok(())
}

/// Restore checkpoint-carried volume data onto this node before create.
///
/// Returns the freshly-created host volume directories (for rollback on a
/// later create failure). Refuses to clobber a pre-existing same-named local
/// volume (there is no cluster-unique volume identity yet) and refuses a
/// checkpoint that is missing data for a declared volume. Every volume is
/// validated BEFORE any copy so a rejection leaves no partial state behind.
fn materialize_checkpoint_volumes(
	home: &Home,
	snapshot_dir: &Path,
	volumes: Option<&Map<String, Value>>,
) -> Result<Vec<PathBuf>> {
	let Some(volumes) = volumes.filter(|volumes| !volumes.is_empty()) else {
		return Ok(Vec::new());
	};
	let source_root = snapshot_dir.join("volumes");
	let mut names = Vec::new();
	let mut seen = std::collections::HashSet::new();
	for spec in volumes.values() {
		let Some(name) = spec
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		if seen.insert(name.to_owned()) {
			names.push(name.to_owned());
		}
	}
	for name in &names {
		let dest = home.volumes_dir().join(name);
		if dest.exists() || dest.is_symlink() {
			return Err(EngineError::unsupported(format!(
				"cannot restore volume {name:?}: a volume of that name already exists on this node \
				 (no cluster-unique volume identity yet)"
			)));
		}
		if !source_root.join(name).is_dir() {
			return Err(EngineError::unsupported(format!(
				"checkpoint is missing data for volume {name:?}"
			)));
		}
	}
	let mut created = Vec::new();
	for name in &names {
		let result: Result<()> = (|| {
			let volume = Volume::new_in_home(home.root(), name)?;
			created.push(volume.path());
			copy_tree_without_locks(&source_root.join(name), &volume.path())
		})();
		if let Err(err) = result {
			for path in &created {
				let _ = fs::remove_dir_all(path);
			}
			return Err(err);
		}
	}
	Ok(created)
}

/// Refuse cross-platform network restores. Port of Python
/// `Engine._validate_network_restore`: a checkpoint that carried a `network`
/// restore spec can only resume on a host with the same network flavor.
fn validate_network_restore(params: &Map<String, Value>) -> Result<()> {
	let Some(Value::Object(network)) = params.get("network") else {
		return Ok(());
	};
	let flavor = network
		.get("flavor")
		.and_then(Value::as_str)
		.unwrap_or_default();
	let macos = cfg!(target_os = "macos");
	match flavor {
		"tap" if macos => Err(EngineError::unsupported(
			"Linux TAP networking cannot be restored on macOS user-net hosts",
		)),
		"user" if !macos => Err(EngineError::unsupported(
			"macOS user-net checkpoints cannot be restored on Linux TAP hosts",
		)),
		"tap" | "user" => Ok(()),
		other => {
			Err(EngineError::unsupported(format!("unknown network checkpoint flavor '{other}'")))
		},
	}
}

fn copy_tree_without_locks(src: &Path, dst: &Path) -> Result<()> {
	fs::create_dir_all(dst)?;
	for entry in fs::read_dir(src)? {
		let entry = entry?;
		let path = entry.path();
		let target = dst.join(entry.file_name());
		if entry.file_type()?.is_dir() {
			copy_tree_without_locks(&path, &target)?;
			continue;
		}
		if path.file_name().and_then(|name| name.to_str()) == Some(".lock") {
			continue;
		}
		fs::copy(path, target)?;
	}
	Ok(())
}

fn volumes_meta(volumes: &[ResolvedVolume]) -> Value {
	Value::Object(
		volumes
			.iter()
			.map(|volume| {
				(
					volume.mountpoint.clone(),
					json!({ "name": volume.name, "tag": volume.tag, "read_only": volume.read_only }),
				)
			})
			.collect::<Map<_, _>>(),
	)
}

fn s3_mounts_meta(mounts: &[ResolvedS3Mount]) -> Value {
	Value::Object(
		mounts
			.iter()
			.map(|mount| (mount.mountpoint.clone(), mount.meta.clone()))
			.collect(),
	)
}

fn network_guest_json(config: &net::GuestNetworkConfig) -> Value {
	json!({
		"tap": config.tap,
		"guest_ip": config.guest_ip,
		"prefix": config.prefix,
		"host_ip": config.host_ip,
		"dns": config.dns,
	})
}

fn user_net_guest_config() -> Value {
	json!({
		"guest_ip": net::USER_NET_GUEST_IP,
		"prefix": net::USER_NET_PREFIX,
		"host_ip": net::USER_NET_GATEWAY,
		"dns": net::USER_NET_DNS,
	})
}

fn policy_json(policy: &NetworkPolicy) -> Value {
	json!({
		"block_network": policy.block_network,
		"egress_allow": policy.egress_allow,
		"egress_allow_domains": policy.egress_allow_domains,
		"inbound_cidr_allowlist": policy.inbound_cidr_allowlist,
	})
}

fn sorted_ports(ports: Option<&[u16]>, tunnels: &BTreeMap<u16, (String, u16)>) -> Vec<u16> {
	let mut ports =
		ports.map_or_else(|| tunnels.keys().copied().collect::<Vec<_>>(), <[u16]>::to_vec);
	ports.sort_unstable();
	ports
}

fn tunnels_json(tunnels: &BTreeMap<u16, (String, u16)>) -> Value {
	Value::Object(
		tunnels
			.iter()
			.map(|(port, (host, host_port))| {
				(port.to_string(), json!({ "host": host, "port": host_port }))
			})
			.collect::<Map<_, _>>(),
	)
}

fn reject_macos_host_network_features(params: &SandboxCreate) -> Result<()> {
	for (feature, requested) in [
		("ports", params.ports.as_ref().is_some_and(|v| !v.is_empty())),
		("egress_allow", params.egress_allow.as_ref().is_some_and(|v| !v.is_empty())),
		(
			"egress_allow_domains",
			params
				.egress_allow_domains
				.as_ref()
				.is_some_and(|v| !v.is_empty()),
		),
		(
			"inbound_cidr_allowlist",
			params
				.inbound_cidr_allowlist
				.as_ref()
				.is_some_and(|v| !v.is_empty()),
		),
	] {
		if requested {
			return Err(EngineError::invalid(format!(
				"{feature} requires Linux host networking (TAP); macOS user-mode NAT is \
				 outbound-egress only"
			)));
		}
	}
	Ok(())
}

fn template_key_for_cached(cached: &CachedTemplate) -> String {
	template_key(
		&cached.spec.reference,
		cached.disk_mb,
		cached.memory,
		cached.cpus,
		cached.fs_slots,
		cached.host_slot,
		cached.nic_slot,
		cached.tap_slot,
	)
}

fn template_request_from_pool(reference: &str, extra: &HashMap<String, Value>) -> TemplateRequest {
	TemplateRequest {
		image:      Some(reference.to_owned()),
		dockerfile: extra
			.get("dockerfile")
			.and_then(Value::as_str)
			.map(PathBuf::from),
		context:    extra
			.get("context")
			.and_then(Value::as_str)
			.map_or_else(|| PathBuf::from("."), PathBuf::from),
		disk_mb:    extra.get("disk_mb").and_then(Value::as_u64).unwrap_or(1024),
		timeout:    extra.get("timeout").and_then(Value::as_u64).unwrap_or(300),
		memory:     extra.get("memory").and_then(Value::as_u64).unwrap_or(512),
		cpus:       extra.get("cpus").and_then(Value::as_u64).unwrap_or(1),
		fs_slots:   extra.get("fs_slots").and_then(Value::as_u64).unwrap_or(0),
		host_slot:  extra
			.get("host_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false),
		// Default to the pool-eligible flavor (block_network templates have no
		// NIC slot); operators opt into the macOS networked-warm flavor with
		// `nic_slot: true`.
		nic_slot:   extra
			.get("nic_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false),
		tap_slot:   extra
			.get("tap_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false),
	}
}

fn control_for_vm(vm: &SandboxVm) -> Result<ControlClient> {
	ControlClient::connect(vm.control_sock()?, CONTROL_TIMEOUT)
}

fn snapshot_state_present(dir: &Path) -> bool {
	dir.join("current-generation").is_file()
}

fn teardown_network(name: &str) {
	let Some(lease) = net::lease_for(name) else {
		return;
	};
	let _ = net::teardown_tap(
		&lease.tap,
		Some(&lease.guest_ip),
		Some(&lease.host_ip),
		lease.prefix,
		None,
		None,
	);
	let _ = net::release_guest_config(name);
}

fn detect_oom(name: &str) -> bool {
	let Ok(meta) = SandboxVm::new(name).meta() else {
		return false;
	};
	let mut candidates = Vec::new();
	for key in ["memory_events", "memory_events_path"] {
		if let Some(path) = meta.get(key).and_then(Value::as_str) {
			candidates.push(PathBuf::from(path));
		}
	}
	if let Some(path) = meta
		.get("cgroup_path")
		.or_else(|| meta.get("cgroup"))
		.or_else(|| meta.get("cgroup_dir"))
		.and_then(Value::as_str)
	{
		candidates.push(PathBuf::from(path).join("memory.events"));
	}
	candidates.push(
		PathBuf::from("/sys/fs/cgroup")
			.join("vmon")
			.join(name)
			.join("memory.events"),
	);
	for path in candidates {
		let Ok(text) = fs::read_to_string(path) else {
			continue;
		};
		for line in text.lines() {
			let mut parts = line.split_whitespace();
			if parts.next() == Some("oom_kill")
				&& parts
					.next()
					.and_then(|value| value.parse::<u64>().ok())
					.unwrap_or(0)
					> 0
			{
				return true;
			}
		}
	}
	false
}

fn drain_entry_stream(
	rx: std::sync::mpsc::Receiver<Vec<u8>>,
	log: Arc<Mutex<fs::File>>,
) -> thread::JoinHandle<()> {
	thread::spawn(move || {
		for chunk in rx {
			let mut log = log.lock();
			if log.write_all(&chunk).and_then(|()| log.flush()).is_err() {
				break;
			}
		}
	})
}

fn bridge_bytes(rx: std::sync::mpsc::Receiver<Vec<u8>>) -> Receiver<Vec<u8>> {
	let (tx, out) = flume::unbounded();
	thread::spawn(move || {
		for chunk in rx {
			if tx.send(chunk).is_err() {
				break;
			}
		}
	});
	out
}

fn bridge_exit(
	rx: std::sync::mpsc::Receiver<Result<crate::engine::agent::ExitStatus>>,
) -> Receiver<ExecExit> {
	let (tx, out) = flume::bounded(1);
	thread::spawn(move || {
		let exit = match rx.recv() {
			Ok(Ok(status)) => ExecExit { code: status.code, signal: status.signal },
			Ok(Err(_)) | Err(_) => ExecExit { code: -1, signal: None },
		};
		let _ = tx.send(exit);
	});
	out
}

fn clamp_exec_timeout(timeout: Option<f64>) -> Duration {
	let Some(timeout) = timeout else {
		return EXEC_CAPTURE_CAP;
	};
	if !timeout.is_finite() || timeout <= 0.0 {
		return Duration::ZERO;
	}
	Duration::from_secs_f64(timeout).min(EXEC_CAPTURE_CAP)
}

/// Backstop for the VMM-armed deadline: every launch/claim path passes
/// `--timeout-secs` (or arms it via control `extend`), so the VMM self-kills
/// with return code 124 on time. This thread only cleans up a VM that
/// outlives its deadline by a grace period (a wedged VMM), so it never races
/// the self-kill's `status.json` write.
fn start_timeout_watchdog(name: String, secs: u64, runtime: Arc<dyn SandboxRuntime>) -> Sender<()> {
	const GRACE: Duration = Duration::from_secs(5);
	let (tx, rx) = flume::bounded(1);
	thread::Builder::new()
		.name(format!("vmon-timeout-{name}"))
		.spawn(move || {
			if rx
				.recv_timeout(Duration::from_secs(secs.max(1)) + GRACE)
				.is_err()
			{
				let vm = runtime.sandbox(&name);
				if runtime.is_running(&vm).unwrap_or(false) {
					let _ = runtime.stop(&vm, false);
				}
			}
		})
		.ok();
	tx
}

fn list_child_dirs(root: &Path) -> Vec<String> {
	let Ok(entries) = fs::read_dir(root) else {
		return Vec::new();
	};
	let mut names = Vec::new();
	for entry in entries.flatten() {
		if entry.file_type().is_ok_and(|kind| kind.is_dir())
			&& let Some(name) = entry.file_name().to_str()
		{
			names.push(name.to_owned());
		}
	}
	names
}

fn random_hex(bytes: usize) -> String {
	let mut out = String::with_capacity(bytes * 2);
	while out.len() < bytes * 2 {
		let _ = write!(out, "{:016x}", rand::random::<u64>());
	}
	out.truncate(bytes * 2);
	out
}

fn unix_time() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

fn unix_millis() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_millis())
}

const fn backend_name() -> &'static str {
	if cfg!(target_os = "macos") {
		"hvf"
	} else if cfg!(target_os = "linux") {
		"kvm"
	} else {
		"unknown"
	}
}

#[cfg(test)]
mod tests {
	use tempfile::TempDir;

	use super::*;
	use crate::image::cas;

	fn config_for(temp: &TempDir) -> ServeConfig {
		let mut config = ServeConfig::default();
		config.home = temp.path().to_path_buf();
		config.warm_images = Vec::new();
		config
	}

	fn checkpoint_fixture(temp: &TempDir, name: &str) -> PathBuf {
		let dir = temp.path().join(name);
		fs::create_dir(&dir).expect("checkpoint dir");
		fs::write(dir.join("rootfs.img"), name.as_bytes()).expect("rootfs");
		fs::write(dir.join("agent-ready.json"), b"{}").expect("agent marker");
		dir
	}

	fn indexed_checkpoint(temp: &TempDir, name: &str) -> (PathBuf, String) {
		let dir = checkpoint_fixture(temp, name);
		let digest = cas::index_template(&dir, None).expect("index checkpoint");
		assert_eq!(cas::lookup(&digest).expect("lookup indexed checkpoint"), Some(dir.clone()));
		(dir, digest)
	}

	fn write_meta(home: &Path, name: &str, meta: Value) {
		let dir = home.join("vms").join(name);
		fs::create_dir_all(&dir).expect("vm dir");
		fs::write(dir.join("meta.json"), serde_json::to_string_pretty(&meta).expect("json"))
			.expect("meta");
	}

	fn valid_create() -> SandboxCreate {
		SandboxCreate { cpus: 1, memory: 512, disk_mb: 1024, ..SandboxCreate::default() }
	}

	#[test]
	fn snapshot_metadata_must_be_a_bounded_regular_file() {
		let temp = TempDir::new().expect("temp");
		let regular = temp.path().join("regular.json");
		fs::write(&regular, b"{}").expect("regular metadata");
		assert_eq!(read_snapshot_metadata(&regular).expect("read metadata"), b"{}");

		let outside = temp.path().join("outside.json");
		fs::write(&outside, b"{}").expect("outside metadata");
		let symlink = temp.path().join("symlink.json");
		std::os::unix::fs::symlink(&outside, &symlink).expect("metadata symlink");
		assert!(read_snapshot_metadata(&symlink).is_err());

		let oversized = temp.path().join("oversized.json");
		fs::write(&oversized, vec![b' '; MAX_SNAPSHOT_METADATA_BYTES as usize + 1])
			.expect("oversized metadata");
		assert!(read_snapshot_metadata(&oversized).is_err());
	}

	#[test]
	fn snapshot_options_accept_blocked_network_metadata() {
		let options = resolve_snapshot_options(
			HashMap::from([("block_network".to_owned(), Value::Bool(true))]),
			None,
		)
		.expect("blocked-network snapshot options");

		assert_eq!(options.block_network, Some(true));
	}

	#[test]
	fn mesh_checkpoint_carries_secret_environment() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("secret", "secret", "running");
		record.detail = json!({"block_network": true});
		engine.insert_test_record(record);

		let mut runtime = RuntimeState::default();
		runtime
			.secret_env
			.insert("TOKEN".to_owned(), "sensitive".to_owned());
		engine
			.inner
			.runtimes
			.lock()
			.insert("secret".to_owned(), runtime);

		let (_, params) = engine
			.mesh_checkpoint_params("secret")
			.expect("secret-bearing sandbox can migrate");
		assert_eq!(params["secrets"][0]["name"], "carried");
		assert_eq!(params["secrets"][0]["values"]["TOKEN"], "sensitive");
	}

	#[test]
	fn lifecycle_events_are_sequenced_and_describe_resulting_state() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let events = engine.subscribe_events();
		let mut record = VmRecord::new("sandbox", "sandbox", "running");
		record.source = Some("alpine".to_owned());
		record.tags.insert("role".to_owned(), "worker".to_owned());

		engine.publish_record_event("ready", &record);
		record.status = "terminated".to_owned();
		record.detail = json!({"returncode": 137, "terminated_reason": "oom"});
		engine.publish_record_event("terminated", &record);
		engine.publish_record_event("removed", &record);

		let ready = events.recv().expect("ready event");
		assert_eq!(ready["type"], "ready");
		assert_eq!(ready["sequence"], 1);
		assert_eq!(ready["status"], "running");
		assert_eq!(ready["source"], "alpine");
		assert_eq!(ready["tags"]["role"], "worker");
		let terminated = events.recv().expect("terminated event");
		assert_eq!(terminated["type"], "terminated");
		assert_eq!(terminated["sequence"], 2);
		assert_eq!(terminated["status"], "terminated");
		assert_eq!(terminated["returncode"], 137);
		assert_eq!(terminated["reason"], "oom");
		let removed = events.recv().expect("removed event");
		assert_eq!(removed["type"], "removed");
		assert_eq!(removed["sequence"], 3);
		assert_eq!(removed["status"], "removed");
	}

	struct SnapshotRuntime {
		launches: std::sync::atomic::AtomicUsize,
		fail_at:  usize,
		names:    Mutex<Vec<String>>,
	}

	impl SnapshotRuntime {
		fn new(fail_at: usize) -> Arc<Self> {
			Arc::new(Self {
				launches: std::sync::atomic::AtomicUsize::new(0),
				fail_at,
				names: Mutex::new(Vec::new()),
			})
		}

		fn names(&self) -> Vec<String> {
			self.names.lock().clone()
		}
	}

	impl SandboxRuntime for SnapshotRuntime {
		fn name(&self) -> &'static str {
			"snapshot-test"
		}

		fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
			let attempt = self.launches.fetch_add(1, Ordering::Relaxed) + 1;
			self.names.lock().push(vm.name().to_owned());
			fs::create_dir_all(vm.dir())?;
			if attempt == self.fail_at {
				return Err(EngineError::engine("injected snapshot launch failure"));
			}
			vm.save_meta(Map::from_iter([("pid".to_owned(), json!(1000 + attempt))]))
		}

		fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			match fs::remove_dir_all(vm.dir()) {
				Ok(()) => Ok(()),
				Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
				Err(error) => Err(error.into()),
			}
		}

		fn is_running(&self, vm: &SandboxVm) -> Result<bool> {
			Ok(vm.dir().is_dir())
		}
	}

	fn snapshot_engine(
		temp: &TempDir,
		fail_at: usize,
	) -> (Engine, Arc<SnapshotRuntime>, crate::home::test_home::HomeGuard) {
		let home = crate::home::test_home::set(temp.path());
		let runtime = SnapshotRuntime::new(fail_at);
		let engine =
			Engine::with_runtime(config_for(temp), runtime.clone()).expect("snapshot engine");
		(engine, runtime, home)
	}

	#[test]
	fn restore_requires_a_snapshot_and_returns_a_canonical_view() {
		let temp = TempDir::new().expect("temp");
		let (engine, runtime, _home) = snapshot_engine(&temp, usize::MAX);

		let missing = engine
			.restore("missing", RestoreBody::default())
			.expect_err("missing snapshots must fail before launch");
		assert_eq!(missing.code.as_str(), "not_found");
		assert!(runtime.names().is_empty());

		for unsafe_name in ["../escape", "nested/snapshot", ".hidden", r"nested\snapshot"] {
			let error = engine
				.restore(unsafe_name, RestoreBody::default())
				.expect_err("unsafe snapshot name");
			assert_eq!(error.code.as_str(), "invalid");
			let error = engine
				.create(SandboxCreate { name: Some(unsafe_name.to_owned()), ..valid_create() })
				.expect_err("unsafe sandbox name");
			assert_eq!(error.code.as_str(), "invalid");
		}
		assert!(!temp.path().join("escape").exists());

		fs::create_dir_all(engine.snapshot_dir("base")).expect("snapshot");
		std::os::unix::fs::symlink(temp.path(), engine.snapshot_dir("linked"))
			.expect("snapshot symlink");
		let linked = engine
			.restore("linked", RestoreBody::default())
			.expect_err("snapshot symlinks must be rejected");
		assert_eq!(linked.code.as_str(), "invalid");
		let unsafe_target = engine
			.restore("base", RestoreBody {
				name: Some("../escape".to_owned()),
				..RestoreBody::default()
			})
			.expect_err("unsafe restore target");
		assert_eq!(unsafe_target.code.as_str(), "invalid");
		fs::create_dir_all(engine.home().templates_dir().join("image-template")).expect("template");
		assert_eq!(engine.snapshots().expect("snapshot list"), ["base"]);
		let view = engine
			.restore("base", RestoreBody {
				name: Some("restored".to_owned()),
				..RestoreBody::default()
			})
			.expect("restore");
		assert_eq!(view["id"], "restored");
		assert_eq!(view["name"], "restored");
		assert_eq!(view["status"], "running");
		assert_eq!(view["source"], "restore:base");
		assert!(
			view["created_at"]
				.as_f64()
				.is_some_and(|created| created > 0.0)
		);
		assert_eq!(engine.get("restored").expect("persisted view")["id"], "restored");

		let collision = engine
			.restore("base", RestoreBody {
				name: Some("restored".to_owned()),
				..RestoreBody::default()
			})
			.expect_err("restore must not overwrite a sandbox");
		assert_eq!(collision.code.as_str(), "busy");
		assert_eq!(runtime.names(), ["restored"]);
	}

	#[test]
	fn fork_rejects_invalid_counts_and_rolls_back_a_partial_batch() {
		let temp = TempDir::new().expect("temp");
		let (engine, runtime, _home) = snapshot_engine(&temp, 2);
		fs::create_dir_all(engine.snapshot_dir("base")).expect("snapshot");

		for count in [0, MAX_FORK_CLONES + 1] {
			let error = engine
				.fork("base", ForkBody { count, extra: HashMap::new() })
				.expect_err("invalid count");
			assert_eq!(error.code.as_str(), "invalid");
		}
		assert!(runtime.names().is_empty());

		let error = engine
			.fork("base", ForkBody { count: 3, extra: HashMap::new() })
			.expect_err("second clone fails");
		assert_eq!(error.message, "injected snapshot launch failure");
		let names = runtime.names();
		assert_eq!(names.len(), 2);
		assert!(engine.list(None).expect("registry list").is_empty());
		for name in names {
			assert!(!temp.path().join("vms").join(name).exists());
		}
	}

	#[test]
	fn fork_returns_full_canonical_views() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		fs::create_dir_all(engine.snapshot_dir("base")).expect("snapshot");

		let value = engine
			.fork("base", ForkBody { count: 2, extra: HashMap::new() })
			.expect("fork");
		let clones = value["clones"].as_array().expect("clone views");
		assert_eq!(clones.len(), 2);
		for clone in clones {
			assert!(
				clone["id"]
					.as_str()
					.is_some_and(|id| id.starts_with("fork-"))
			);
			assert_eq!(clone["name"], clone["id"]);
			assert_eq!(clone["status"], "running");
			assert_eq!(clone["source"], "fork:base");
			assert!(clone["created_at"].as_f64().is_some());
		}
		assert_eq!(engine.list(None).expect("registry list").len(), 2);
	}

	#[test]
	fn idempotency_replays_rehydrated_record_without_launching() {
		let temp = TempDir::new().expect("temp");
		write_meta(
			temp.path(),
			"sb-existing",
			json!({ "status": "stopped", "idempotency_key": "idem", "tags": {"k": "v"}, "timeout_secs": 30 }),
		);
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let view = engine
			.create(SandboxCreate {
				idempotency_key: Some("idem".to_owned()),
				cpus: 0,
				..SandboxCreate::default()
			})
			.expect("replayed before launch");
		assert_eq!(view["name"], "sb-existing");
	}

	#[test]
	fn connect_token_is_stable_per_sandbox() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		engine.insert_test_record(VmRecord::new("sb", "sb", "running"));
		let first = engine.tunnels("sb").expect("first")["connect_token"]
			.as_str()
			.expect("token")
			.to_owned();
		let second = engine.tunnels("sb").expect("second")["connect_token"]
			.as_str()
			.expect("token")
			.to_owned();
		assert_eq!(first, second);
		assert!(!first.is_empty());
	}

	#[test]
	fn prometheus_text_contains_counters_and_statuses() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		engine.insert_test_record(VmRecord::new("sb", "sb", "running"));
		engine.inc_counter("created");
		let text = engine.prometheus_metrics();
		assert!(text.contains("# HELP vmon_server_sandboxes"));
		assert!(text.contains("vmon_server_sandboxes{status=\"running\"} 1"));
		assert!(text.contains("vmon_server_events_total{event=\"created\"} 1"));
		assert!(text.ends_with('\n'));
	}

	#[test]
	fn pool_stats_view_uses_template_key_format() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let key = template_key("ref", 1, 2, 3, 4, false, true, false);
		let pool =
			WarmPool::with_options("/no/template", 0, false, false, Duration::ZERO).expect("pool");
		engine.inner.pools.set(key.clone(), pool);
		let view = engine.pool_list().expect("pool list");
		assert_eq!(view[&key]["ready"], 0);
		assert_eq!(view[&key]["size"], 0);
		engine.shutdown();
	}

	#[test]
	fn exec_capture_timeout_is_capped_at_sixty_seconds() {
		assert_eq!(clamp_exec_timeout(None), Duration::from_mins(1));
		assert_eq!(clamp_exec_timeout(Some(120.0)), Duration::from_mins(1));
		assert_eq!(clamp_exec_timeout(Some(2.5)), Duration::from_millis(2500));
	}

	#[test]
	fn create_validation_matches_python_messages() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let err = engine
			.create(SandboxCreate { ha: Some("bad".to_owned()), ..valid_create() })
			.expect_err("invalid ha");
		assert_eq!(err.message, "ha must be one of: async, async+rerun, off, rerun");
		let err = engine
			.create(SandboxCreate { block_network: true, ports: Some(vec![80]), ..valid_create() })
			.expect_err("bad ports");
		assert_eq!(err.message, "ports cannot be exposed when block_network=True");
		let err = engine
			.create(SandboxCreate {
				remote_page_url: Some("http://peer".to_owned()),
				..valid_create()
			})
			.expect_err("remote page rejected");
		assert_eq!(err.message, "remote_page_* fields are server-internal");
	}

	#[test]
	fn mesh_replicate_cleanup_drops_cas_pointer_and_deletes_checkpoint_dir() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (snapshot_dir, digest) = indexed_checkpoint(&temp, "replica-checkpoint");

		engine
			.mesh_replicate_cleanup(&digest, &snapshot_dir)
			.expect("replica cleanup succeeds");

		assert_eq!(cas::lookup(&digest).expect("lookup after cleanup"), None);
		assert!(!snapshot_dir.exists(), "replication cleanup must delete the checkpoint");
	}

	#[test]
	fn mesh_migrate_commit_unknown_sid_still_drops_both_checkpoints() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (base_dir, base_digest) = indexed_checkpoint(&temp, "commit-base-checkpoint");
		let (delta_dir, delta_digest) = indexed_checkpoint(&temp, "commit-delta-checkpoint");

		engine
			.mesh_migrate_commit("missing-source", &base_dir, &base_digest, &delta_dir, &delta_digest)
			.expect("unknown source must not block checkpoint cleanup");

		assert_eq!(cas::lookup(&base_digest).expect("base lookup after commit"), None);
		assert_eq!(cas::lookup(&delta_digest).expect("delta lookup after commit"), None);
		assert!(!base_dir.exists(), "migration commit must delete the pre-copy checkpoint");
		assert!(!delta_dir.exists(), "migration commit must delete the delta checkpoint");
	}

	#[test]
	fn mesh_migrate_abort_drops_pointers_but_keeps_checkpoint_dirs_after_create_success() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (base_dir, base_digest) = indexed_checkpoint(&temp, "abort-base-checkpoint");
		let (delta_dir, delta_digest) = indexed_checkpoint(&temp, "abort-success-checkpoint");
		let mut record = VmRecord::new("restored", "restored", "running");
		record.detail = json!({ "idempotency_key": "abort-replay" });
		engine.insert_test_record(record);
		engine
			.inner
			.registry
			.record_idempotency("restored", "abort-replay");
		let mut params = Map::new();
		params.insert("idempotency_key".to_owned(), json!("abort-replay"));

		let view = engine
			.mesh_migrate_abort("missing-source", &base_digest, &delta_dir, &delta_digest, params)
			.expect("abort cleanup succeeds after create returns");

		assert_eq!(view["name"], "restored");
		assert_eq!(cas::lookup(&base_digest).expect("base lookup after abort"), None);
		assert_eq!(cas::lookup(&delta_digest).expect("delta lookup after abort"), None);
		assert!(base_dir.is_dir(), "migration abort must keep the pre-copy checkpoint");
		assert!(delta_dir.is_dir(), "migration abort must keep the delta checkpoint");
	}

	#[test]
	fn mesh_migrate_abort_create_error_propagates_and_keeps_checkpoint_dirs() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (base_dir, base_digest) = indexed_checkpoint(&temp, "abort-error-base-checkpoint");
		let (delta_dir, delta_digest) = indexed_checkpoint(&temp, "abort-error-checkpoint");
		let mut params = Map::new();
		params.insert("cpus".to_owned(), json!(0));

		let err = engine
			.mesh_migrate_abort("missing-source", &base_digest, &delta_dir, &delta_digest, params)
			.expect_err("invalid create params must propagate");

		assert_eq!(err.message, "cpus must be positive");
		assert_eq!(
			cas::lookup(&base_digest).expect("base lookup after failed abort"),
			Some(base_dir.clone())
		);
		assert_eq!(
			cas::lookup(&delta_digest).expect("delta lookup after failed abort"),
			Some(delta_dir.clone())
		);
		assert!(base_dir.is_dir(), "failed migration abort must keep the pre-copy checkpoint");
		assert!(delta_dir.is_dir(), "failed migration abort must keep the delta checkpoint");
	}

	#[test]
	fn restored_s3_metadata_preserves_tags_without_persisting_credentials() {
		let mut params = Map::from_iter([(
			"s3_mounts".to_owned(),
			json!({
				"/mnt/data": {
					"uri": "s3://bucket/prefix",
					"endpoint": "http://127.0.0.1:9000",
					"region": "us-east-1",
					"read_only": false,
					"tag": "bucket",
					"auth": "anonymous"
				}
			}),
		)]);

		let tags = restore_s3_mount_params(&mut params)
			.expect("metadata parses")
			.expect("metadata carries tags");
		assert_eq!(tags.get("/mnt/data").map(String::as_str), Some("bucket"));
		assert_eq!(params["s3_mounts"]["/mnt/data"]["uri"], "s3://bucket/prefix");
		assert!(params["s3_mounts"]["/mnt/data"].get("tag").is_none());
		assert!(params["s3_mounts"]["/mnt/data"].get("auth").is_none());
		assert!(params["s3_mounts"]["/mnt/data"].get("access_key").is_none());
	}
}
