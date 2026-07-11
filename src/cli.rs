use std::{
	collections::HashMap,
	fs,
	io::{self, IsTerminal, Read, Write},
	path::{Path, PathBuf},
	thread,
	time::{Duration, Instant},
};

use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use serde_json::{Map, Value, json};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use vmon_proto::v1 as pb;

use crate::{
	contexts::{
		Context, ContextStore, LOCAL, normalize_server, now_secs, remote_client_from_context,
		roster_from_status,
	},
	error::{CliError, Result, err},
	transport::{ApiClient, Grpc, status_error},
};

#[derive(Clone, Debug, Default)]
pub struct TransportOptions {
	context: Option<String>,
	token:   Option<String>,
}

#[derive(Parser)]
#[command(
	name = "vmon",
	version,
	about = "Vibemon microVM CLI",
	arg_required_else_help = true,
	after_help = "Global transport options (place before the subcommand):\n  --context NAME   Use \
	              a saved remote context, or 'local' for the UDS daemon\n  --token TOKEN    Bearer \
	              token for the selected remote context"
)]
struct Cli {
	#[command(subcommand)]
	command: Commands,
}

#[derive(Subcommand)]
#[allow(
	clippy::large_enum_variant,
	reason = "clap parses this enum once and boxing ServeArgs would add indirection without \
	          runtime benefit"
)]
enum Commands {
	/// Boot an image as a sandbox and stream its output unless detached.
	Run(RunArgs),
	/// List sandboxes.
	Ps,
	/// Run a command inside an agent-enabled sandbox.
	Exec(ExecArgs),
	/// Run or attach to an interactive shell.
	Shell(ShellArgs),
	/// Copy a file between host and guest.
	Cp(CpArgs),
	/// Show a sandbox console log.
	Logs(LogsArgs),
	/// Stop a running sandbox but keep its record.
	Stop(NameArg),
	/// Remove a sandbox and its state.
	Rm(NameArg),
	/// Pause a running sandbox.
	Pause(NameArg),
	/// Resume a paused sandbox.
	Resume(NameArg),
	/// Reset a sandbox timeout deadline.
	Extend(ExtendArgs),
	/// Snapshot machine state into a named template.
	Snapshot(SnapshotArgs),
	/// Restore a sandbox from a snapshot.
	Restore(RestoreArgs),
	/// Fork copy-on-write clones from a snapshot.
	Fork(ForkArgs),
	/// Show live sandbox metrics.
	Stats(NameArg),
	/// Show Prometheus metrics, or sandbox metrics when NAME is supplied.
	Metrics(MetricsArgs),
	/// Guest filesystem operations.
	Fs {
		#[command(subcommand)]
		command: FsCommands,
	},
	/// Named volume operations.
	Volume {
		#[command(subcommand)]
		command: VolumeCommands,
	},
	/// Warm pool operations.
	Pool {
		#[command(subcommand)]
		command: PoolCommands,
	},
	/// Diagnose local prerequisites.
	Doctor(DoctorArgs),
	/// Manage the local daemon process.
	Daemon {
		#[command(subcommand)]
		command: DaemonCommands,
	},
	/// Serve the HTTP sandbox API.
	Serve(ServeArgs),
	/// Manage remote API contexts.
	Context {
		#[command(subcommand)]
		command: ContextCommands,
	},
	/// Mesh cluster operations.
	Mesh {
		#[command(subcommand)]
		command: MeshCommands,
	},
	/// Print a shell completion script.
	Completion(CompletionArgs),
}

#[derive(Args)]
struct RunArgs {
	/// OCI/container image reference.
	image:         Option<String>,
	/// Command to run in the sandbox.
	#[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "CMD")]
	cmd:           Vec<String>,
	/// Dockerfile to build server-side before running.
	#[arg(short = 'f', long)]
	dockerfile:    Option<String>,
	/// Dockerfile build context.
	#[arg(long = "context", default_value = ".")]
	build_context: String,
	/// Sandbox name.
	#[arg(long)]
	name:          Option<String>,
	/// Guest RAM in MiB.
	#[arg(long, default_value_t = 512)]
	mem:           u32,
	/// vCPU count.
	#[arg(long, default_value_t = 1)]
	cpus:          u32,
	/// Sandbox disk size in MiB.
	#[arg(long, default_value_t = 1024)]
	disk_mb:       u32,
	/// Create timeout in seconds.
	#[arg(long, default_value_t = 300.0)]
	timeout:       f64,
	/// Leave the sandbox running in the background.
	#[arg(short, long)]
	detach:        bool,
	/// Boot without networking.
	#[arg(long)]
	block_network: bool,
	/// Request an owner architecture.
	#[arg(long, value_enum)]
	arch:          Option<Arch>,
}

#[derive(Args)]
struct ExecArgs {
	name: String,
	/// Allocate a PTY and stream over the interactive exec protocol.
	#[arg(short = 't', long)]
	tty:  bool,
	/// Command to run.
	#[arg(trailing_var_arg = true, allow_hyphen_values = true, value_name = "CMD")]
	cmd:  Vec<String>,
}

#[derive(Args)]
struct ShellArgs {
	/// Running sandbox, snapshot, or image reference.
	reference: Option<String>,
	/// One-off command. Without this, an interactive shell is started.
	#[arg(short = 'c', long = "cmd")]
	command:   Option<String>,
	/// Image for a fresh shell if REF is omitted.
	#[arg(long)]
	image:     Option<String>,
	/// Environment variable KEY=VALUE; bare KEY copies from the host.
	#[arg(short = 'e', long = "env", value_name = "KEY=VALUE")]
	env:       Vec<String>,
	#[arg(long, default_value_t = 512)]
	mem:       u32,
	#[arg(long, default_value_t = 1)]
	cpus:      u32,
	#[arg(long, default_value_t = 1024)]
	disk_mb:   u32,
	#[arg(long, default_value_t = 300.0)]
	timeout:   f64,
	/// Force PTY allocation on.
	#[arg(long = "pty", conflicts_with = "no_pty")]
	pty:       bool,
	/// Force PTY allocation off.
	#[arg(long = "no-pty")]
	no_pty:    bool,
}

#[derive(Args)]
struct CpArgs {
	src: String,
	dst: String,
}

#[derive(Args)]
struct LogsArgs {
	name:   String,
	#[arg(short, long)]
	follow: bool,
}

#[derive(Args)]
struct NameArg {
	name: String,
}

#[derive(Args)]
struct ExtendArgs {
	name: String,
	secs: u64,
}

#[derive(Args)]
struct SnapshotArgs {
	name:     String,
	snapshot: String,
	#[arg(long)]
	stop:     bool,
}

#[derive(Args)]
struct RestoreArgs {
	snapshot: String,
	#[arg(long)]
	name:     Option<String>,
	#[arg(long)]
	agent:    bool,
	#[arg(short, long)]
	detach:   bool,
	#[arg(long, value_enum)]
	arch:     Option<Arch>,
}

#[derive(Args)]
struct ForkArgs {
	snapshot: String,
	#[arg(long, default_value_t = 2)]
	count:    u32,
	#[arg(long, value_enum)]
	arch:     Option<Arch>,
}

#[derive(Args)]
struct MetricsArgs {
	name: Option<String>,
}

#[derive(Subcommand)]
enum FsCommands {
	/// List a guest directory: NAME[:PATH].
	List { reference: String },
	/// Stat a guest path: NAME[:PATH].
	Stat { reference: String },
}

#[derive(Subcommand)]
enum VolumeCommands {
	/// List named volumes.
	Ls,
	/// Remove a named volume.
	Rm { name: String },
}

#[derive(Subcommand)]
enum PoolCommands {
	/// List warm pools.
	Ls,
	/// Set or resize a warm pool.
	Set(PoolSetArgs),
	/// Delete a warm pool.
	Rm { reference: String },
}

#[derive(Args)]
struct PoolSetArgs {
	reference:     String,
	#[arg(long)]
	size:          u32,
	#[arg(long)]
	image:         Option<String>,
	#[arg(long)]
	template:      Option<String>,
	#[arg(long)]
	mem:           Option<u32>,
	#[arg(long)]
	cpus:          Option<u32>,
	#[arg(long)]
	disk_mb:       Option<u32>,
	#[arg(long)]
	block_network: bool,
}

#[derive(Args)]
struct DoctorArgs {
	#[arg(long)]
	serve:       bool,
	#[arg(long = "config")]
	config_path: Option<PathBuf>,
}

#[derive(Subcommand)]
enum DaemonCommands {
	/// Show local daemon health.
	Status,
	/// Stop the local daemon with SIGTERM.
	Stop,
}

#[derive(Args, Default)]
struct ServeArgs {
	#[arg(long = "config")]
	config_path:             Option<PathBuf>,
	#[arg(long)]
	home:                    Option<PathBuf>,
	#[arg(long)]
	host:                    Option<String>,
	#[arg(long)]
	port:                    Option<u16>,
	#[arg(long)]
	token:                   Option<String>,
	#[arg(long)]
	client_token:            Option<String>,
	#[arg(long)]
	tls_cert:                Option<String>,
	#[arg(long)]
	tls_key:                 Option<String>,
	#[arg(long)]
	idle_timeout:            Option<f64>,
	#[arg(long)]
	replicate_sec:           Option<f64>,
	#[arg(long)]
	replicas:                Option<usize>,
	#[arg(long)]
	replicate_concurrency:   Option<usize>,
	#[arg(long = "restore-quorum", conflicts_with = "no_restore_quorum")]
	restore_quorum:          bool,
	#[arg(long = "no-restore-quorum")]
	no_restore_quorum:       bool,
	#[arg(long)]
	warm_pool_size:          Option<usize>,
	#[arg(long)]
	warm_images:             Option<String>,
	#[arg(long)]
	mesh_heartbeat_sec:      Option<f64>,
	#[arg(long)]
	mesh_reap_sec:           Option<f64>,
	#[arg(long)]
	mesh_idem_ttl_sec:       Option<f64>,
	#[arg(long)]
	mesh_create_timeout_sec: Option<f64>,
	#[arg(long)]
	mesh_w_warm:             Option<f64>,
	#[arg(long)]
	mesh_w_free:             Option<f64>,
	#[arg(long)]
	mesh_w_local:            Option<f64>,
	#[arg(long)]
	mesh_w_region:           Option<f64>,
	#[arg(long)]
	mesh_w_inflight:         Option<f64>,
}

#[derive(Subcommand)]
enum ContextCommands {
	/// Add a context from a gateway URL.
	#[command(alias = "create")]
	Add(ContextAddArgs),
	/// List configured contexts.
	Ls,
	/// Switch the active context.
	Use { name: String },
	/// Remove a context and saved token.
	Rm { name: String },
}

#[derive(Args)]
struct ContextAddArgs {
	name:       String,
	#[arg(short, long)]
	server:     String,
	#[arg(long)]
	token:      Option<String>,
	#[arg(long)]
	save_token: bool,
	#[arg(long, default_value = "")]
	region:     String,
}

#[derive(Subcommand)]
enum MeshCommands {
	/// Initialize this node as a cluster and print a join blob.
	Setup(MeshSetupArgs),
	/// Join an existing cluster.
	Join(MeshJoinArgs),
	/// Leave the cluster.
	Leave(MeshLeaveArgs),
	/// Show cluster status.
	Status,
}

#[derive(Args)]
struct MeshSetupArgs {
	#[arg(long)]
	advertise:   Option<String>,
	#[arg(long, default_value = "")]
	region:      String,
	#[arg(long)]
	max_vcpus:   Option<u32>,
	#[arg(long)]
	max_mem_mib: Option<u32>,
}

#[derive(Args)]
struct MeshJoinArgs {
	blob:      String,
	#[arg(long)]
	advertise: Option<String>,
	#[arg(long, default_value = "")]
	region:    String,
}

#[derive(Args)]
struct MeshLeaveArgs {
	#[arg(long)]
	drain: bool,
}

#[derive(Args)]
struct CompletionArgs {
	shell: CompletionShell,
}

#[derive(Clone, ValueEnum)]
enum Arch {
	#[value(name = "x86_64")]
	X86_64,
	Aarch64,
}

#[derive(Clone, ValueEnum)]
enum CompletionShell {
	Bash,
	Zsh,
	Fish,
}

pub fn run(raw_args: Vec<String>) -> Result<i32> {
	let (transport_options, args) = extract_transport_options(raw_args)?;
	let cli = Cli::parse_from(args);
	execute(cli.command, &transport_options)
}

fn execute(command: Commands, transport_options: &TransportOptions) -> Result<i32> {
	match command {
		Commands::Run(args) => cmd_run(args, transport_options),
		Commands::Ps => cmd_ps(transport_options),
		Commands::Exec(args) => cmd_exec(args, transport_options),
		Commands::Shell(args) => cmd_shell(args, transport_options),
		Commands::Cp(args) => cmd_cp(args, transport_options),
		Commands::Logs(args) => cmd_logs(args, transport_options),
		Commands::Stop(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Stop, "stopped")
		},
		Commands::Rm(args) => cmd_rm(args, transport_options),
		Commands::Pause(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Pause, "paused")
		},
		Commands::Resume(args) => {
			lifecycle(transport_options, &args.name, LifecycleVerb::Resume, "resumed")
		},
		Commands::Extend(args) => cmd_extend(args, transport_options),
		Commands::Snapshot(args) => cmd_snapshot(args, transport_options),
		Commands::Restore(args) => cmd_restore(args, transport_options),
		Commands::Fork(args) => cmd_fork(args, transport_options),
		Commands::Stats(args) => cmd_stats(&args.name, transport_options),
		Commands::Metrics(args) => cmd_metrics(args, transport_options),
		Commands::Fs { command } => cmd_fs(command, transport_options),
		Commands::Volume { command } => cmd_volume(command, transport_options),
		Commands::Pool { command } => cmd_pool(command, transport_options),
		Commands::Doctor(args) => Ok(cmd_doctor(args)),
		Commands::Daemon { command } => cmd_daemon(command),
		Commands::Serve(args) => cmd_serve(args),
		Commands::Context { command } => cmd_context(command),
		Commands::Mesh { command } => cmd_mesh(command, transport_options),
		Commands::Completion(args) => Ok(cmd_completion(args)),
	}
}

fn extract_transport_options(raw_args: Vec<String>) -> Result<(TransportOptions, Vec<String>)> {
	let mut out = Vec::new();
	let mut options = TransportOptions::default();
	let mut iter = raw_args.into_iter();
	if let Some(argv0) = iter.next() {
		out.push(argv0);
	}
	while let Some(arg) = iter.next() {
		if arg == "--context" {
			options.context = Some(
				iter
					.next()
					.ok_or_else(|| CliError::new("--context requires a value"))?,
			);
			continue;
		}
		if let Some(value) = arg.strip_prefix("--context=") {
			options.context = Some(value.to_owned());
			continue;
		}
		if arg == "--token" {
			options.token = Some(
				iter
					.next()
					.ok_or_else(|| CliError::new("--token requires a value"))?,
			);
			continue;
		}
		if let Some(value) = arg.strip_prefix("--token=") {
			options.token = Some(value.to_owned());
			continue;
		}
		out.push(arg);
		out.extend(iter);
		return Ok((options, out));
	}
	Ok((options, out))
}

fn client(options: &TransportOptions, autostart: bool) -> Result<ApiClient> {
	let store = ContextStore::load_default();
	let target = options.context.clone().or_else(|| store.current_name());
	let Some(name) = target.filter(|name| name != LOCAL) else {
		return Ok(ApiClient::local(autostart));
	};
	let context = store
		.get(&name)
		.ok_or_else(|| CliError::new(format!("no such context {name:?}")))?;
	let token = store.resolve_token(&name, options.token.as_deref())?;
	remote_client_from_context(context, token)
}

fn cmd_run(args: RunArgs, options: &TransportOptions) -> Result<i32> {
	if args.image.is_none() && args.dockerfile.is_none() {
		return err("provide an image (for example: vmon run alpine)");
	}
	let client = client(options, true)?;
	let mut body = Map::new();
	insert_opt(&mut body, "image", args.image.clone());
	insert_opt(&mut body, "dockerfile", args.dockerfile.clone());
	body.insert("context".to_owned(), json!(args.build_context));
	insert_opt(&mut body, "name", args.name.clone());
	body.insert("memory".to_owned(), json!(args.mem));
	body.insert("cpus".to_owned(), json!(args.cpus));
	body.insert("disk_mb".to_owned(), json!(args.disk_mb));
	body.insert("timeout".to_owned(), json!(args.timeout));
	body.insert("block_network".to_owned(), json!(args.block_network));
	if let Some(arch) = args.arch {
		body.insert("arch".to_owned(), json!(arch.as_str()));
	}
	let cmd = strip_dashdash(args.cmd);
	if args.detach && !cmd.is_empty() {
		body.insert("command".to_owned(), json!(&cmd));
	}
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request = pb::CreateSandboxRequest { spec_json: Value::Object(body).to_string() };
	let view = json_view(
		grpc
			.block_on(sandboxes.create(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	let name = view_name(&view)?.to_owned();
	if args.detach {
		println!("started {name}  image={}", string_field(&view, "image").unwrap_or("-"));
		println!(
			"hint: vmon exec {name} -- ... | vmon snapshot {name} <snapshot> | vmon stop {name}"
		);
		return Ok(0);
	}
	if cmd.is_empty() {
		let _ = attach_console(&grpc, &name);
		return Ok(exit_code_from_view(&sandbox_view(&grpc, &name)?));
	}
	let exit = pump_exec(&grpc, ExecRpc::Exec, exec_start(&name, cmd, false), false, false, false)?;
	let _ = grpc.block_on(
		sandboxes
			.stop(pb::StopSandboxRequest { id: name, returncode: Some(i64::from(exit)) }),
	);
	Ok(exit)
}

fn cmd_ps(options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let response = grpc
		.block_on(sandboxes.list(pb::ListSandboxesRequest { tags: Vec::new() }))
		.map_err(status_error)?
		.into_inner();
	if response.sandboxes_json.is_empty() {
		println!("no sandboxes — boot one with `vmon run <image>`");
		return Ok(0);
	}
	println!("{:<24} {:<12} {:<8} IMAGE", "NAME", "STATUS", "PID");
	for sandbox in response.sandboxes_json {
		let sandbox: Value = serde_json::from_str(&sandbox)?;
		let name = string_field(&sandbox, "name")
			.or_else(|| string_field(&sandbox, "id"))
			.unwrap_or("-");
		let status = string_field(&sandbox, "status").unwrap_or("-");
		let pid = value_to_string(sandbox.get("pid")).unwrap_or_else(|| "-".to_owned());
		let image = string_field(&sandbox, "image").unwrap_or("-");
		println!("{name:<24} {status:<12} {pid:<8} {image}");
	}
	Ok(0)
}

fn cmd_exec(args: ExecArgs, options: &TransportOptions) -> Result<i32> {
	let argv = strip_dashdash(args.cmd);
	if argv.is_empty() {
		return err("exec requires a command (vmon exec NAME -- <cmd>)");
	}
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	if args.tty {
		return pump_exec(&grpc, ExecRpc::Exec, exec_start(&args.name, argv, true), true, true, true);
	}
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExecCaptureRequest {
		id:   args.name,
		exec: Some(pb::ExecStart { cmd: argv, ..Default::default() }),
	};
	let response = grpc
		.block_on(sandboxes.exec_capture(request))
		.map_err(status_error)?
		.into_inner();
	io::stdout().write_all(&response.stdout)?;
	io::stdout().flush()?;
	io::stderr().write_all(&response.stderr)?;
	io::stderr().flush()?;
	Ok(clamp_exit(response.code))
}

fn cmd_shell(args: ShellArgs, options: &TransportOptions) -> Result<i32> {
	let tty = if args.pty {
		true
	} else if args.no_pty {
		false
	} else {
		io::stdin().is_terminal() && io::stdout().is_terminal()
	};
	if args.command.is_none() && !tty {
		return err("vmon shell needs a terminal; pass -c '<cmd>' for a one-off or --pty");
	}
	let client = client(options, true)?;
	let mut params = Map::new();
	insert_opt(&mut params, "ref", args.reference);
	insert_opt(&mut params, "image", args.image);
	if let Some(command) = args.command {
		params.insert("cmd".to_owned(), json!(["/bin/sh", "-c", command]));
	}
	let env = parse_env(&args.env)?;
	if !env.is_empty() {
		params.insert("env".to_owned(), json!(env));
	}
	params.insert("mem".to_owned(), json!(args.mem));
	params.insert("cpus".to_owned(), json!(args.cpus));
	params.insert("disk_mb".to_owned(), json!(args.disk_mb));
	params.insert("timeout".to_owned(), json!(args.timeout));
	let grpc = client.grpc()?;
	let params = pb::exec_input::Input::ShellParamsJson(Value::Object(params).to_string());
	pump_exec(&grpc, ExecRpc::Shell, params, tty, tty, true)
}

fn cmd_cp(args: CpArgs, options: &TransportOptions) -> Result<i32> {
	let src_remote = parse_remote(&args.src);
	let dst_remote = parse_remote(&args.dst);
	if src_remote.is_some() == dst_remote.is_some() {
		return err("cp requires exactly one remote operand of the form <name>:<path>");
	}
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	if let Some((name, remote_path)) = src_remote {
		let request = pb::FilePathRequest { id: name, path: remote_path.clone() };
		let content = grpc
			.block_on(sandboxes.file_read(request))
			.map_err(status_error)?
			.into_inner();
		let mut out = PathBuf::from(&args.dst);
		if out.is_dir() {
			out.push(Path::new(&remote_path).file_name().unwrap_or_default());
		}
		fs::write(out, content.data)?;
	} else if let Some((name, remote_path)) = dst_remote {
		let data = fs::read(&args.src)?;
		let request = pb::FileWriteRequest { id: name, path: remote_path, data };
		grpc
			.block_on(sandboxes.file_write(request))
			.map_err(status_error)?;
	}
	Ok(0)
}

fn cmd_logs(args: LogsArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request = pb::LogsRequest { id: args.name, follow: args.follow, tail: None };
	let mut stream = grpc
		.block_on(sandboxes.logs(request))
		.map_err(status_error)?
		.into_inner();
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(chunk)) => {
				io::stdout().write_all(&chunk.data)?;
				io::stdout().flush()?;
			},
			Ok(None) => return Ok(0),
			Err(status) => return Err(status_error(status)),
		}
	}
}

enum LifecycleVerb {
	Stop,
	Pause,
	Resume,
}

fn lifecycle(
	options: &TransportOptions,
	name: &str,
	verb: LifecycleVerb,
	label: &str,
) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(async {
			match verb {
				LifecycleVerb::Stop => {
					sandboxes
						.stop(pb::StopSandboxRequest { id: name.to_owned(), returncode: None })
						.await
				},
				LifecycleVerb::Pause => {
					sandboxes
						.pause(pb::SandboxRef { id: name.to_owned() })
						.await
				},
				LifecycleVerb::Resume => {
					sandboxes
						.resume(pb::SandboxRef { id: name.to_owned() })
						.await
				},
			}
		})
		.map_err(status_error)?;
	println!("{label} {name}");
	Ok(0)
}

fn cmd_rm(args: NameArg, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	grpc
		.block_on(sandboxes.remove(pb::SandboxRef { id: args.name.clone() }))
		.map_err(status_error)?;
	println!("removed {}", args.name);
	Ok(0)
}

fn cmd_extend(args: ExtendArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExtendSandboxRequest { id: args.name.clone(), secs: args.secs };
	let response = json_view(
		grpc
			.block_on(sandboxes.extend(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	if let Some(deadline) = response.get("deadline_unix") {
		println!("extended {} deadline to {deadline}", args.name);
	} else {
		println!("extended {} deadline by {}s", args.name, args.secs);
	}
	Ok(0)
}

fn cmd_snapshot(args: SnapshotArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request =
		pb::SnapshotRequest { id: args.name, name: Some(args.snapshot), stop: args.stop };
	let response = json_view(
		grpc
			.block_on(sandboxes.snapshot(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	println!(
		"snapshot {} -> {}",
		string_field(&response, "snapshot").unwrap_or("-"),
		string_field(&response, "dir").unwrap_or("-")
	);
	Ok(0)
}

fn cmd_restore(args: RestoreArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let mut body = Map::new();
	insert_opt(&mut body, "name", args.name);
	body.insert("agent".to_owned(), json!(args.agent));
	if let Some(arch) = args.arch {
		body.insert("arch".to_owned(), json!(arch.as_str()));
	}
	let grpc = client.grpc()?;
	let mut snapshots = grpc.snapshots();
	let request = pb::RestoreSnapshotRequest {
		name:      args.snapshot.clone(),
		body_json: Value::Object(body).to_string(),
	};
	let view = json_view(
		grpc
			.block_on(snapshots.restore(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	let name = view_name(&view)?.to_owned();
	println!("restored {name} from {}", args.snapshot);
	if args.detach {
		return Ok(0);
	}
	let _ = attach_console(&grpc, &name);
	Ok(exit_code_from_view(&sandbox_view(&grpc, &name)?))
}

fn cmd_fork(args: ForkArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let mut body = Map::new();
	body.insert("count".to_owned(), json!(args.count));
	if let Some(arch) = args.arch {
		body.insert("arch".to_owned(), json!(arch.as_str()));
	}
	let grpc = client.grpc()?;
	let mut snapshots = grpc.snapshots();
	let request = pb::ForkSnapshotRequest {
		name:      args.snapshot.clone(),
		body_json: Value::Object(body).to_string(),
	};
	let response = json_view(
		grpc
			.block_on(snapshots.fork(request))
			.map_err(status_error)?
			.into_inner(),
	)?;
	let clones = response
		.get("clones")
		.and_then(Value::as_array)
		.cloned()
		.unwrap_or_default();
	println!("forked {} clone(s) from {}", clones.len(), args.snapshot);
	for clone in clones {
		println!(
			"{}  (pid {}, CoW reconstruct={}ms)",
			string_field(&clone, "name").unwrap_or("-"),
			value_to_string(clone.get("pid")).unwrap_or_else(|| "-".to_owned()),
			value_to_string(clone.get("reconstruct_ms")).unwrap_or_else(|| "n/a".to_owned())
		);
	}
	Ok(0)
}

fn cmd_stats(name: &str, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(sandboxes.metrics(pb::SandboxRef { id: name.to_owned() }))
		.map_err(status_error)?
		.into_inner();
	print_json(&json_view(view)?)
}

fn cmd_metrics(args: MetricsArgs, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	if let Some(name) = args.name {
		return cmd_stats(&name, options);
	}
	print!("{}", client.request_text("GET", "/metrics")?);
	Ok(0)
}

fn cmd_fs(command: FsCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let (reference, list) = match command {
		FsCommands::List { reference } => (reference, true),
		FsCommands::Stat { reference } => (reference, false),
	};
	let (name, path) = parse_ref(&reference);
	let request = pb::FilePathRequest { id: name, path };
	let view = grpc
		.block_on(async {
			if list {
				sandboxes.file_list(request).await
			} else {
				sandboxes.file_stat(request).await
			}
		})
		.map_err(status_error)?
		.into_inner();
	print_json(&json_view(view)?)
}

fn cmd_volume(command: VolumeCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut volumes = grpc.volumes();
	match command {
		VolumeCommands::Ls => {
			let response = grpc
				.block_on(volumes.list(pb::ListVolumesRequest {}))
				.map_err(status_error)?
				.into_inner();
			for volume in response.volumes {
				println!("{volume}");
			}
			Ok(0)
		},
		VolumeCommands::Rm { name } => {
			grpc
				.block_on(volumes.delete(pb::VolumeRef { name: name.clone() }))
				.map_err(status_error)?;
			println!("removed volume {name}");
			Ok(0)
		},
	}
}

fn cmd_pool(command: PoolCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	let grpc = client.grpc()?;
	let mut pools = grpc.pools();
	match command {
		PoolCommands::Ls => {
			let view = grpc
				.block_on(pools.list(pb::ListPoolsRequest {}))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
		PoolCommands::Set(args) => {
			let mut body = Map::new();
			body.insert("size".to_owned(), json!(args.size));
			insert_opt(&mut body, "image", args.image);
			insert_opt(&mut body, "template", args.template);
			insert_opt(&mut body, "memory", args.mem);
			insert_opt(&mut body, "cpus", args.cpus);
			insert_opt(&mut body, "disk_mb", args.disk_mb);
			if args.block_network {
				body.insert("block_network".to_owned(), json!(true));
			}
			let request = pb::PoolSetRequest {
				reference: args.reference,
				body_json: Value::Object(body).to_string(),
			};
			let view = grpc
				.block_on(pools.set(request))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
		PoolCommands::Rm { reference } => {
			grpc
				.block_on(pools.delete(pb::PoolRef { reference: reference.clone() }))
				.map_err(status_error)?;
			println!("removed pool {reference}");
			Ok(0)
		},
	}
}

fn cmd_doctor(args: DoctorArgs) -> i32 {
	if args.serve {
		let mut overrides = HashMap::new();
		if let Some(path) = args.config_path {
			overrides.insert("config".to_owned(), path.display().to_string());
		}
		let (rows, checks) = vmond::doctor::collect_serve_doctor(&overrides);
		if !rows.is_empty() {
			println!("{:<28} {:<24} {:<10} {:<24} FLAG", "KEY", "VALUE", "SOURCE", "ENV");
			for row in rows {
				println!(
					"{:<28} {:<24} {:<10} {:<24} {}",
					row.key,
					truncate(&row.value, 24),
					row.source,
					row.env,
					row.flag
				);
			}
			println!();
		}
		return print_checks(checks);
	}
	print_checks(vmond::doctor::collect_checks())
}

fn cmd_daemon(command: DaemonCommands) -> Result<i32> {
	match command {
		DaemonCommands::Status => Ok(daemon_status()),
		DaemonCommands::Stop => daemon_stop(),
	}
}

fn daemon_status() -> i32 {
	let client = ApiClient::local(false);
	if client.request_json("GET", "/healthz", None).is_ok() {
		let info = client
			.grpc()
			.and_then(|grpc| {
				let mut system = grpc.system();
				let view = grpc
					.block_on(system.info(pb::InfoRequest {}))
					.map_err(status_error)?
					.into_inner();
				json_view(view)
			})
			.unwrap_or_else(|_| json!({}));
		let pid = read_pid().unwrap_or_else(|| "?".to_owned());
		let sock = vmond::home::Home::new(vmond::home::state_dir()).vmond_sock();
		println!(
			"vmond: running (pid {pid}) socket={} version={}",
			sock.display(),
			string_field(&info, "version").unwrap_or("?")
		);
		0
	} else {
		println!("vmond: not running");
		1
	}
}

fn daemon_stop() -> Result<i32> {
	let Some(pid_text) = read_pid() else {
		println!("vmond: not running");
		return Ok(0);
	};
	let pid = pid_text
		.trim()
		.parse::<libc::pid_t>()
		.map_err(|_| CliError::new(format!("invalid pid file contents: {pid_text:?}")))?;
	// SAFETY: `pid` comes from vmond's pid file and `SIGTERM` is a constant signal;
	// no pointers are passed.
	let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
	if rc != 0 {
		let err = io::Error::last_os_error();
		if err.raw_os_error() == Some(libc::ESRCH) {
			println!("vmond: not running");
			return Ok(0);
		}
		return Err(err.into());
	}
	let client = ApiClient::local(false);
	let deadline = Instant::now() + Duration::from_secs(5);
	while Instant::now() < deadline {
		if client.request_json("GET", "/healthz", None).is_err() {
			println!("vmond stopped (pid {pid})");
			return Ok(0);
		}
		thread::sleep(Duration::from_millis(100));
	}
	println!("sent SIGTERM to vmond (pid {pid})");
	Ok(0)
}

fn cmd_serve(args: ServeArgs) -> Result<i32> {
	vmond::api::serve(args.overrides())?;
	Ok(0)
}

fn cmd_context(command: ContextCommands) -> Result<i32> {
	match command {
		ContextCommands::Add(args) => context_add(args),
		ContextCommands::Ls => Ok(context_ls()),
		ContextCommands::Use { name } => {
			let mut store = ContextStore::load_default();
			store.use_context(&name)?;
			println!("using context {name}");
			Ok(0)
		},
		ContextCommands::Rm { name } => {
			let mut store = ContextStore::load_default();
			store.remove(&name)?;
			println!("removed context {name}");
			Ok(0)
		},
	}
}

fn context_add(args: ContextAddArgs) -> Result<i32> {
	if args.name == LOCAL {
		return err("'local' is reserved for the local daemon");
	}
	let token = args.token.or_else(|| std::env::var("VMON_API_TOKEN").ok());
	if token.as_deref().is_none_or(str::is_empty) {
		return err("a token is required: pass --token or set VMON_API_TOKEN");
	}
	let url = normalize_server(&args.server);
	let endpoints = match ApiClient::remote(vec![url.clone()], token.clone()).and_then(|client| {
		let grpc = client.grpc()?;
		let mut system = grpc.system();
		let view = grpc
			.block_on(system.mesh_status(pb::MeshStatusRequest {}))
			.map_err(status_error)?
			.into_inner();
		json_view(view)
	}) {
		Ok(status) => roster_from_status(&status, &url),
		Err(error) => {
			eprintln!("warning: could not fetch mesh status ({error}); storing the single endpoint");
			vec![url]
		},
	};
	let mut store = ContextStore::load_default();
	store.put(Context {
		name: args.name.clone(),
		endpoints,
		region: args.region,
		updated: now_secs(),
	})?;
	if args.save_token {
		store.save_token(&args.name, token.as_deref().unwrap_or_default())?;
	}
	if store.current().is_none() {
		store.use_context(&args.name)?;
	}
	println!("context {} added", args.name);
	if !args.save_token {
		println!("hint: set VMON_API_TOKEN to authenticate cluster commands");
	}
	Ok(0)
}

fn context_ls() -> i32 {
	let store = ContextStore::load_default();
	let current = store.current_name();
	println!("{:<3} {:<20} {:<7} ENDPOINTS", "", "NAME", "TOKEN");
	println!(
		"{:<3} {:<20} {:<7} UDS",
		if current.as_deref() == Some(LOCAL) {
			"*"
		} else {
			""
		},
		LOCAL,
		"-"
	);
	for context in store.list() {
		let marker = if current.as_deref() == Some(context.name.as_str()) {
			"*"
		} else {
			""
		};
		let token = if store.has_token(&context.name) {
			"saved"
		} else {
			"env"
		};
		println!("{marker:<3} {:<20} {:<7} {}", context.name, token, context.endpoints.join(","));
	}
	0
}

fn cmd_mesh(command: MeshCommands, options: &TransportOptions) -> Result<i32> {
	let client = client(options, true)?;
	match command {
		MeshCommands::Setup(args) => {
			let response = client.request_json(
				"POST",
				"/v1/mesh/setup",
				Some(json!({
					"advertise": args.advertise,
					"region": args.region,
					"max_vcpus": args.max_vcpus,
					"max_mem_mib": args.max_mem_mib,
				})),
			)?;
			if let Some(advertise) = string_field(&response, "advertise") {
				println!("cluster ready on {advertise}");
			}
			if let Some(blob) = string_field(&response, "blob") {
				println!("vmon mesh join {blob}");
			}
			Ok(0)
		},
		MeshCommands::Join(args) => {
			let response = client.request_json(
				"POST",
				"/v1/mesh/join",
				Some(json!({"blob": args.blob, "advertise": args.advertise, "region": args.region})),
			)?;
			print_json(&response)
		},
		MeshCommands::Leave(args) => {
			let response =
				client.request_json("POST", "/v1/mesh/leave", Some(json!({"drain": args.drain})))?;
			print_json(&response)
		},
		MeshCommands::Status => {
			let grpc = client.grpc()?;
			let mut system = grpc.system();
			let view = grpc
				.block_on(system.mesh_status(pb::MeshStatusRequest {}))
				.map_err(status_error)?
				.into_inner();
			print_json(&json_view(view)?)
		},
	}
}

fn cmd_completion(args: CompletionArgs) -> i32 {
	let shell = match args.shell {
		CompletionShell::Bash => clap_complete::Shell::Bash,
		CompletionShell::Zsh => clap_complete::Shell::Zsh,
		CompletionShell::Fish => clap_complete::Shell::Fish,
	};
	let mut command = Cli::command();
	clap_complete::generate(shell, &mut command, "vmon", &mut io::stdout());
	0
}

impl ServeArgs {
	fn overrides(self) -> HashMap<String, String> {
		let mut overrides = HashMap::new();
		insert_override(
			&mut overrides,
			"config",
			self.config_path.map(|path| path.display().to_string()),
		);
		insert_override(&mut overrides, "home", self.home.map(|path| path.display().to_string()));
		insert_override(&mut overrides, "host", self.host);
		insert_override(&mut overrides, "port", self.port);
		insert_override(&mut overrides, "token", self.token);
		insert_override(&mut overrides, "client_token", self.client_token);
		insert_override(&mut overrides, "tls_cert", self.tls_cert);
		insert_override(&mut overrides, "tls_key", self.tls_key);
		insert_override(&mut overrides, "idle_timeout", self.idle_timeout);
		insert_override(&mut overrides, "replicate_sec", self.replicate_sec);
		insert_override(&mut overrides, "replicas", self.replicas);
		insert_override(&mut overrides, "replicate_concurrency", self.replicate_concurrency);
		if self.restore_quorum {
			overrides.insert("restore_quorum".to_owned(), "true".to_owned());
		} else if self.no_restore_quorum {
			overrides.insert("restore_quorum".to_owned(), "false".to_owned());
		}
		insert_override(&mut overrides, "warm_pool_size", self.warm_pool_size);
		insert_override(&mut overrides, "warm_images", self.warm_images);
		insert_override(&mut overrides, "mesh_heartbeat_sec", self.mesh_heartbeat_sec);
		insert_override(&mut overrides, "mesh_reap_sec", self.mesh_reap_sec);
		insert_override(&mut overrides, "mesh_idem_ttl_sec", self.mesh_idem_ttl_sec);
		insert_override(&mut overrides, "mesh_create_timeout_sec", self.mesh_create_timeout_sec);
		insert_override(&mut overrides, "mesh_w_warm", self.mesh_w_warm);
		insert_override(&mut overrides, "mesh_w_free", self.mesh_w_free);
		insert_override(&mut overrides, "mesh_w_local", self.mesh_w_local);
		insert_override(&mut overrides, "mesh_w_region", self.mesh_w_region);
		insert_override(&mut overrides, "mesh_w_inflight", self.mesh_w_inflight);
		overrides
	}
}

impl Arch {
	const fn as_str(&self) -> &'static str {
		match self {
			Self::X86_64 => "x86_64",
			Self::Aarch64 => "aarch64",
		}
	}
}

fn attach_console(grpc: &Grpc, name: &str) -> Result<()> {
	let mut sandboxes = grpc.sandboxes();
	let mut stream = grpc
		.block_on(sandboxes.attach(pb::SandboxRef { id: name.to_owned() }))
		.map_err(status_error)?
		.into_inner();
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(output)) => {
				if let Some(pb::exec_output::Output::Chunk(chunk)) = output.output {
					write_chunk(&chunk)?;
				}
			},
			Ok(None) => return Ok(()),
			Err(status) => return Err(status_error(status)),
		}
	}
}

enum ExecRpc {
	Exec,
	Shell,
}

const fn exec_input(input: pb::exec_input::Input) -> pb::ExecInput {
	pb::ExecInput { input: Some(input) }
}

fn exec_start(sandbox_id: &str, cmd: Vec<String>, tty: bool) -> pb::exec_input::Input {
	pb::exec_input::Input::Start(pb::ExecStart {
		sandbox_id: sandbox_id.to_owned(),
		cmd,
		tty,
		..Default::default()
	})
}

/// Bridges an Exec/Shell bidi stream onto the synchronous terminal loop:
/// stdin is forwarded from a plain thread through the outbound channel while
/// this thread blocks on inbound frames.
fn pump_exec(
	grpc: &Grpc,
	rpc: ExecRpc,
	first: pb::exec_input::Input,
	raw_mode: bool,
	forward_stdin: bool,
	show_ready: bool,
) -> Result<i32> {
	let (tx, rx) = tokio::sync::mpsc::channel::<pb::ExecInput>(16);
	grpc
		.block_on(tx.send(exec_input(first)))
		.map_err(|_| CliError::new("exec stream closed before start"))?;
	let mut sandboxes = grpc.sandboxes();
	let outbound = ReceiverStream::new(rx);
	let response = grpc
		.block_on(async {
			match rpc {
				ExecRpc::Exec => sandboxes.exec(outbound).await,
				ExecRpc::Shell => sandboxes.shell(outbound).await,
			}
		})
		.map_err(status_error)?;
	let mut stream = response.into_inner();
	let _raw = RawModeGuard::enable(raw_mode)?;
	let _stdin = forward_stdin.then(|| spawn_stdin_forward(tx.clone()));
	let mut exit = None;
	loop {
		match grpc.block_on(stream.message()) {
			Ok(Some(output)) => match output.output {
				Some(pb::exec_output::Output::Chunk(chunk)) => write_chunk(&chunk)?,
				Some(pb::exec_output::Output::Exit(code)) => exit = Some(clamp_exit(code.code)),
				Some(pb::exec_output::Output::Ready(ready)) => {
					if show_ready {
						eprintln!("ready: {}", ready.sandbox_id);
					}
				},
				None => return err("invalid exec output frame"),
			},
			Ok(None) => break,
			Err(status) => return Err(status_error(status)),
		}
	}
	drop(tx);
	Ok(exit.unwrap_or(0))
}

fn write_chunk(chunk: &pb::Output) -> Result<()> {
	if pb::Stream::try_from(chunk.stream) == Ok(pb::Stream::Stderr) {
		io::stderr().write_all(&chunk.data)?;
		io::stderr().flush()?;
	} else {
		io::stdout().write_all(&chunk.data)?;
		io::stdout().flush()?;
	}
	Ok(())
}

fn clamp_exit(code: i64) -> i32 {
	code.clamp(0, 255) as i32
}

fn spawn_stdin_forward(tx: tokio::sync::mpsc::Sender<pb::ExecInput>) -> thread::JoinHandle<()> {
	thread::spawn(move || {
		let mut stdin = io::stdin();
		let mut buf = [0_u8; 8192];
		loop {
			match stdin.read(&mut buf) {
				Ok(0) => {
					let _ = tx.blocking_send(exec_input(pb::exec_input::Input::Eof(pb::Eof {})));
					break;
				},
				Ok(n) => {
					if tx
						.blocking_send(exec_input(pb::exec_input::Input::Stdin(buf[..n].to_vec())))
						.is_err()
					{
						break;
					}
				},
				Err(_) => break,
			}
		}
	})
}

struct RawModeGuard(bool);

impl RawModeGuard {
	fn enable(enable: bool) -> Result<Self> {
		if enable {
			crossterm::terminal::enable_raw_mode()
				.map_err(|error| CliError::new(format!("failed to enter raw mode: {error}")))?;
		}
		Ok(Self(enable))
	}
}

impl Drop for RawModeGuard {
	fn drop(&mut self) {
		if self.0 {
			let _ = crossterm::terminal::disable_raw_mode();
		}
	}
}

fn parse_env(pairs: &[String]) -> Result<HashMap<String, String>> {
	let mut env = HashMap::new();
	for pair in pairs {
		if let Some((key, value)) = pair.split_once('=') {
			env.insert(key.to_owned(), value.to_owned());
		} else if let Ok(value) = std::env::var(pair) {
			env.insert(pair.to_owned(), value);
		} else {
			return err(format!("environment variable {pair:?} is not set"));
		}
	}
	Ok(env)
}

fn parse_remote(spec: &str) -> Option<(String, String)> {
	let (name, path) = spec.split_once(':')?;
	if name.is_empty() || path.is_empty() {
		return None;
	}
	Some((name.to_owned(), path.to_owned()))
}

fn parse_ref(spec: &str) -> (String, String) {
	if let Some((name, path)) = spec.split_once(':') {
		(
			name.to_owned(),
			if path.is_empty() {
				"/".to_owned()
			} else {
				path.to_owned()
			},
		)
	} else {
		(spec.to_owned(), "/".to_owned())
	}
}

fn strip_dashdash(mut args: Vec<String>) -> Vec<String> {
	if args.first().is_some_and(|arg| arg == "--") {
		args.remove(0);
	}
	args
}

fn insert_opt<T: serde::Serialize>(map: &mut Map<String, Value>, key: &str, value: Option<T>) {
	if let Some(value) = value {
		map.insert(key.to_owned(), json!(value));
	}
}

fn insert_override<T: ToString>(map: &mut HashMap<String, String>, key: &str, value: Option<T>) {
	if let Some(value) = value {
		map.insert(key.to_owned(), value.to_string());
	}
}

fn view_name(value: &Value) -> Result<&str> {
	value
		.get("name")
		.or_else(|| value.get("id"))
		.and_then(Value::as_str)
		.ok_or_else(|| CliError::new("API response did not include a sandbox name"))
}

fn string_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
	value.get(key).and_then(Value::as_str)
}

fn value_to_string(value: Option<&Value>) -> Option<String> {
	match value? {
		Value::Null => None,
		Value::String(text) => Some(text.clone()),
		other => Some(other.to_string()),
	}
}

fn exit_code_from_view(value: &Value) -> i32 {
	let code = value.get("returncode").and_then(Value::as_i64).unwrap_or(0);
	if code < 0 {
		(128 + (-code as i32)).clamp(0, 255)
	} else {
		(code as i32).clamp(0, 255)
	}
}

fn json_view(view: pb::JsonView) -> Result<Value> {
	serde_json::from_str(&view.json).map_err(Into::into)
}

fn sandbox_view(grpc: &Grpc, name: &str) -> Result<Value> {
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(sandboxes.get(pb::SandboxRef { id: name.to_owned() }))
		.map_err(status_error)?
		.into_inner();
	json_view(view)
}

fn print_json(value: &Value) -> Result<i32> {
	println!("{}", serde_json::to_string_pretty(value)?);
	Ok(0)
}

fn print_checks(checks: Vec<vmond::doctor::Check>) -> i32 {
	let mut failed = false;
	println!("{:<8} {:<24} DETAIL", "STATUS", "CHECK");
	for check in checks {
		let status = match check.status {
			vmond::doctor::Status::Ok => "ok",
			vmond::doctor::Status::Warn => "warn",
			vmond::doctor::Status::Fail => {
				failed = true;
				"fail"
			},
		};
		println!("{status:<8} {:<24} {}", check.name, check.detail);
		if !check.hint.is_empty() {
			println!("{:8} {:<24} hint: {}", "", "", check.hint);
		}
	}
	i32::from(failed)
}

fn truncate(value: &str, width: usize) -> String {
	if value.chars().count() <= width {
		value.to_owned()
	} else {
		let mut out = value
			.chars()
			.take(width.saturating_sub(1))
			.collect::<String>();
		out.push('…');
		out
	}
}

fn read_pid() -> Option<String> {
	let home = vmond::home::Home::new(vmond::home::state_dir());
	fs::read_to_string(home.vmond_pid())
		.ok()
		.map(|text| text.trim().to_owned())
}
