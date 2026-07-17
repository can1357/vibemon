//! vmon — the single Vibemon binary. `vmon vmm …` runs the per-VM
//! monitor; every other subcommand is the user-facing CLI or Rust daemon.

#[cfg(not(target_os = "windows"))]
mod bench;
#[cfg(not(target_os = "windows"))]
mod cli;
#[cfg(not(target_os = "windows"))]
mod contexts;
#[cfg(not(target_os = "windows"))]
mod error;
#[cfg(not(target_os = "windows"))]
mod transport;

fn main() {
	let mut args = std::env::args().collect::<Vec<_>>();
	if args.get(1).is_some_and(|arg| arg == "vmm") {
		args.drain(0..2);
		exit_on_err(vmm::run_cli(args));
		return;
	}
	#[cfg(not(target_os = "windows"))]
	match cli::run(std::env::args().collect()) {
		Ok(code) => std::process::exit(code),
		Err(error) => {
			eprintln!("vmon: error: {error}");
			std::process::exit(1);
		},
	}
	#[cfg(target_os = "windows")]
	{
		eprintln!("vmon: error: only 'vmon vmm' is supported on Windows");
		std::process::exit(1);
	}
}

fn exit_on_err<T, E: std::fmt::Display>(result: std::result::Result<T, E>) {
	if let Err(error) = result {
		eprintln!("vmon: error: {error}");
		std::process::exit(1);
	}
}
