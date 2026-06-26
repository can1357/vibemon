//! vmm — a barebones VM monitor that boots a Linux guest with a serial
//! console and virtio IO. The hypervisor backend is selected at compile time:
//! KVM on Linux (`x86_64/aarch64`), Apple Hypervisor.framework on macOS
//! (`aarch64` / Apple Silicon), and Windows Hypervisor Platform on `x86_64`
//! Windows.

mod arch;
mod config;
mod control;
mod devices;
mod hv;
#[cfg(target_os = "linux")]
mod jail;
mod layout;
mod memory;
mod metrics;
mod os;
#[cfg(not(target_os = "windows"))]
mod pager;
mod result;
#[cfg(target_os = "linux")]
mod sandbox;
mod snapshot;
mod tap;
mod virtio;
mod vmm;

use config::{Config, LogFormat};
use tracing_subscriber::EnvFilter;

fn main() {
	if let Err(e) = run() {
		eprintln!("vmm: error: {e}");
		std::process::exit(1);
	}
}

fn run() -> result::Result<()> {
	let config = Config::from_args()?;
	init_logging(&config)?;
	vmm::run(config)
}

fn init_logging(config: &Config) -> result::Result<()> {
	let filter = EnvFilter::try_new(&config.log_level)
		.map_err(|e| result::err(format!("invalid --log-level {}: {e}", config.log_level)))?;

	match config.log_format {
		LogFormat::Text => {
			let _ = tracing_subscriber::fmt()
				.with_env_filter(filter)
				.with_writer(std::io::stderr)
				.try_init();
		},
		LogFormat::Json => {
			let _ = tracing_subscriber::fmt()
				.json()
				.with_env_filter(filter)
				.with_writer(std::io::stderr)
				.try_init();
		},
	}
	Ok(())
}
