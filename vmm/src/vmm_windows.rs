//! Windows WHP VM lifecycle.
//!
//! The Windows backend intentionally supports the portable direct-kernel,
//! virtio-mmio subset. Unix sockets, snapshots, TAP networking, virtio-fs,
//! PCI transport, UEFI and host sandboxing are rejected during startup.

use std::{fmt::Write as _, sync::Arc, thread};

use parking_lot::Mutex;
use tracing::{error, warn};
use vm_memory::GuestAddress;

use crate::{
	arch,
	config::{BootMode, Config, Transport},
	control::{PauseGate, RunState},
	devices::{Bus, serial::SerialDevice},
	hv::{Exit, Vcpu, Vm},
	layout::{IRQ_BASE, MMIO_DEVICE_SIZE, MMIO_MEM_START, SERIAL_IRQ},
	os::{EFD_NONBLOCK, EventFd},
	result::{Result, err},
	virtio::{
		Interrupt, VirtioDevice, WorkerControl, block::Block, console::Console, mmio::MmioTransport,
		rng::Rng, run_worker,
	},
};

const QUEUE_NOTIFY_OFFSET: u64 = 0x50;
const DEFAULT_CMDLINE: &str = "console=ttyS0 reboot=t panic=-1";

struct WorkerSpec {
	device:    Arc<Mutex<dyn VirtioDevice>>,
	queue_evt: EventFd,
	kill_evt:  EventFd,
	control:   WorkerControl,
}

/// Boot a Windows-hosted WHP virtual machine and block until it exits.
pub fn run(config: Config) -> Result<()> {
	validate_config(&config)?;

	let mem_bytes = config
		.mem_mib
		.checked_mul(1 << 20)
		.ok_or("guest memory size overflows usize")?;
	let vm = Arc::new(Vm::new(mem_bytes)?);
	let kernel = config
		.kernel
		.as_ref()
		.ok_or_else(|| err("--kernel <path> is required on Windows"))?;
	let loaded = arch::boot::load_kernel(kernel, vm.memory())?;
	let initrd = load_initrd(&config, &vm, &loaded)?;

	let mut vcpus = Vec::with_capacity(config.cpus as usize);
	for id in 0..config.cpus {
		vcpus.push(vm.create_vcpu(id)?);
	}

	let mut pio_bus = Bus::new();
	let mut mmio_bus = Bus::new();
	vm.register_ioapic(&mut mmio_bus)?;
	let serial = Arc::new(Mutex::new(SerialDevice::new(vm.irq_line(SERIAL_IRQ)?)));
	pio_bus.register(0x3f8, 8, serial);

	let mut cmdline = config
		.cmdline
		.clone()
		.unwrap_or_else(|| DEFAULT_CMDLINE.to_string());
	let mut workers = Vec::new();
	let mut mmio_base = MMIO_MEM_START;
	let mut gsi = IRQ_BASE;

	if let Some(path) = &config.rootfs {
		let block = Block::new(path, config.rootfs_read_only)?;
		wire_device(&vm, &mut mmio_bus, Arc::new(Mutex::new(block)), gsi, mmio_base, &mut workers)?;
		push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
		if !cmdline_has_key(&cmdline, "root") {
			cmdline.push_str(if config.rootfs_read_only {
				" root=/dev/vda ro"
			} else {
				" root=/dev/vda rw"
			});
		}
		mmio_base += MMIO_DEVICE_SIZE;
		gsi += 1;
	}

	if config.console_agent {
		wire_device(
			&vm,
			&mut mmio_bus,
			Arc::new(Mutex::new(Console::new()?)),
			gsi,
			mmio_base,
			&mut workers,
		)?;
		push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
		mmio_base += MMIO_DEVICE_SIZE;
		gsi += 1;
	}

	if config.rng {
		wire_device(
			&vm,
			&mut mmio_bus,
			Arc::new(Mutex::new(Rng::new()?)),
			gsi,
			mmio_base,
			&mut workers,
		)?;
		push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
	}

	let cmdline_obj = arch::boot::build_cmdline(&cmdline)?;
	arch::boot::configure_system(
		vm.memory(),
		config.cpus,
		&cmdline_obj,
		initrd,
		loaded.setup_header,
	)?;
	for (id, vcpu) in vcpus.iter().enumerate() {
		arch::configure_vcpu(vcpu, vm.memory(), vm.supported_cpuid(), id as u8, loaded.entry)?;
	}

	let pio_bus = Arc::new(pio_bus);
	let mmio_bus = Arc::new(mmio_bus);
	let gate = PauseGate::new(u32::from(config.cpus));
	let kickers: Vec<Vcpu> = vcpus.clone();
	gate.set_kicker(Arc::new(move || {
		for vcpu in &kickers {
			vcpu.kick();
		}
	}));

	let mut worker_handles = Vec::with_capacity(workers.len());
	let mut kill_events = Vec::with_capacity(workers.len());
	for spec in workers {
		kill_events.push(spec.kill_evt.try_clone()?);
		worker_handles.push(thread::spawn(move || {
			if let Err(e) = run_worker(spec.device, spec.queue_evt, spec.kill_evt, spec.control) {
				error!("virtio worker failed: {e}");
			}
		}));
	}

	let mut vcpu_handles = Vec::with_capacity(vcpus.len());
	for (id, vcpu) in vcpus.into_iter().enumerate() {
		let pio_bus = pio_bus.clone();
		let mmio_bus = mmio_bus.clone();
		let gate = gate.clone();
		vcpu_handles.push(thread::spawn(move || run_vcpu(vcpu, pio_bus, mmio_bus, id, gate)));
	}

	let mut failure = None;
	for handle in vcpu_handles {
		match handle.join() {
			Ok(Ok(())) => {},
			Ok(Err(e)) if failure.is_none() => failure = Some(e),
			Ok(Err(_)) => {},
			Err(_) if failure.is_none() => failure = Some(err("WHP vCPU thread panicked")),
			Err(_) => {},
		}
		gate.set_state(RunState::Stopping);
		gate.signal_all_vcpus();
	}
	for event in kill_events {
		let _ = event.write(1);
	}
	for handle in worker_handles {
		let _ = handle.join();
	}
	if let Some(e) = failure {
		Err(e)
	} else {
		Ok(())
	}
}

fn validate_config(config: &Config) -> Result<()> {
	if config.boot_mode != BootMode::Direct || config.firmware.is_some() {
		return Err(err("WHP currently supports direct kernel boot only"));
	}
	if config.transport != Transport::Mmio {
		return Err(err("WHP currently supports virtio-mmio transport only"));
	}
	if config.restore.is_some() || config.fork_from.is_some() || config.snapshot_root.is_some() {
		return Err(err("WHP snapshots, restore and fork are not supported"));
	}
	if config.api_sock.is_some() || config.agent_sock.is_some() || config.agent_exec.is_some() {
		return Err(err("Unix control and agent sockets are not supported on Windows"));
	}
	if config.tap.is_some() || config.user_net {
		return Err(err("virtio networking is not supported on Windows"));
	}
	if config.fs_tag.is_some() || !config.volumes.is_empty() || !config.remote_fs.is_empty() {
		return Err(err("virtio-fs is not supported on Windows"));
	}
	if config.disk_overlay_of.is_some() || config.count != 1 {
		return Err(err("copy-on-write VM cloning is not supported on Windows"));
	}
	if config.jail
		|| config.netns.is_some()
		|| config.sandbox_uid.is_some()
		|| config.sandbox_gid.is_some()
	{
		return Err(err("host sandbox and namespace options require Linux"));
	}
	if config.timeout_secs.is_some() {
		warn!("--timeout-secs is not yet enforced by the WHP backend");
	}
	Ok(())
}

fn wire_device(
	vm: &Vm,
	mmio_bus: &mut Bus,
	device: Arc<Mutex<dyn VirtioDevice>>,
	gsi: u32,
	mmio_base: u64,
	workers: &mut Vec<WorkerSpec>,
) -> Result<()> {
	let interrupt = Arc::new(Interrupt::new(vm.irq_line(gsi)?));
	let queue_evt = EventFd::new(EFD_NONBLOCK)?;
	vm.register_ioevent(&queue_evt, mmio_base + QUEUE_NOTIFY_OFFSET, None)?;
	let transport =
		Arc::new(Mutex::new(MmioTransport::new(device.clone(), vm.memory().clone(), interrupt)?));
	mmio_bus.register(mmio_base, MMIO_DEVICE_SIZE, transport);
	workers.push(WorkerSpec {
		device,
		queue_evt,
		kill_evt: EventFd::new(EFD_NONBLOCK)?,
		control: WorkerControl {
			pause_evt:   EventFd::new(EFD_NONBLOCK)?,
			resume_evt:  EventFd::new(EFD_NONBLOCK)?,
			ack_evt:     EventFd::new(EFD_NONBLOCK)?,
			failure_evt: EventFd::new(EFD_NONBLOCK)?,
			failure_msg: Arc::new(Mutex::new(None)),
		},
	});
	Ok(())
}

fn run_vcpu(
	vcpu: Vcpu,
	pio_bus: Arc<Bus>,
	mmio_bus: Arc<Bus>,
	id: usize,
	gate: Arc<PauseGate>,
) -> Result<()> {
	gate.register_tid(id);
	loop {
		if gate.state() == RunState::Stopping {
			return Ok(());
		}
		match vcpu.run_whp(&pio_bus, &mmio_bus)? {
			Exit::Continue | Exit::Other | Exit::Hlt => {},
			Exit::Shutdown | Exit::SystemEvent(_) => {
				gate.set_state(RunState::Stopping);
				gate.signal_all_vcpus();
				return Ok(());
			},
			Exit::FailEntry { reason, cpu } => {
				return Err(err(format!("WHP failed to enter vCPU {cpu}: {reason:#x}")));
			},
			Exit::InternalError => return Err(err("WHP reported an internal vCPU error")),
			Exit::MmioRead { .. }
			| Exit::MmioWrite { .. }
			| Exit::PioIn { .. }
			| Exit::PioOut { .. } => {
				return Err(err("WHP returned an unemulated device exit"));
			},
		}
	}
}

fn load_initrd(
	config: &Config,
	vm: &Vm,
	loaded: &arch::boot::LoadedKernel,
) -> Result<Option<(GuestAddress, usize)>> {
	let Some(path) = &config.initrd else {
		return Ok(None);
	};
	let max = loaded
		.setup_header
		.map(|header| header.initrd_addr_max)
		.filter(|max| *max != 0)
		.map_or(0x37ff_ffff, u64::from);
	Ok(Some(arch::boot::load_initrd(path, vm.memory(), max)?))
}

fn push_virtio_cmdline(cmdline: &mut String, mmio_base: u64, gsi: u32) {
	let _ = write!(cmdline, " virtio_mmio.device=4K@0x{mmio_base:x}:{gsi}");
}

fn cmdline_has_key(cmdline: &str, key: &str) -> bool {
	cmdline.split_whitespace().any(|arg| {
		arg.strip_prefix(key)
			.is_some_and(|rest| rest.starts_with('='))
	})
}
