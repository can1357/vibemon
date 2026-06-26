//! Host-process sandbox setup: uid/gid drop, Landlock and seccomp.
//!
//! Standalone `--sandbox` applies after VM backends are opened and before any
//! vCPU/worker thread starts. The jailer uses the same Stage-B filters after it
//! has pivoted into the jail and pre-bound control-plane sockets.

use std::path::PathBuf;

use crate::{config::SeccompAction, result::Result};
#[derive(Clone, Debug, Default)]
pub struct SandboxPaths {
	/// Read-only individual files (kernel, initrd, firmware, snapshot inputs).
	pub ro:      Vec<PathBuf>,
	/// Read/write individual files (writable block images/overlays).
	pub rw:      Vec<PathBuf>,
	/// Read-only directory trees (virtio-fs shares, restore templates).
	pub ro_dirs: Vec<PathBuf>,
	/// Read/write directory trees (snapshot roots, mutable state dirs).
	pub rw_dirs: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct SandboxConfig {
	/// Target UID for a root-launched process to drop to.
	pub uid:            Option<u32>,
	/// Target GID for a root-launched process to drop to.
	pub gid:            Option<u32>,
	/// Path policy installed with Landlock before the seccomp filter.
	pub paths:          SandboxPaths,
	/// Default action for syscalls outside the allowlist.
	pub seccomp_action: SeccompAction,
}

impl SandboxConfig {
	pub fn new(
		uid: Option<u32>,
		gid: Option<u32>,
		paths: SandboxPaths,
		seccomp_action: SeccompAction,
	) -> Self {
		SandboxConfig { uid, gid, paths, seccomp_action }
	}
}

/// Privilege phase: set no_new_privs and drop root. Runs BEFORE the VMM opens
/// /dev/kvm and backing files so every fd belongs to the dropped uid.
pub fn apply_privilege_phase(config: &SandboxConfig) -> Result<()> {
	set_no_new_privs()?;
	drop_root_privileges(config)?;
	Ok(())
}

/// Filter phase: tighten rlimits, then install Landlock and seccomp. Runs AFTER
/// the VMM is built (so the NOFILE clamp counts its fds) and BEFORE any
/// vCPU/worker thread starts, so every thread inherits the filters.
pub fn apply_filter_phase(config: &SandboxConfig) -> Result<()> {
	// RLIMIT_NOFILE is clamped here (not in the privilege phase) so the
	// reserve accounts for the KVM/vCPU/device fds opened by Vmm::build.
	tighten_resource_limits()?;
	apply_landlock_rules(&config.paths)?;
	apply_seccomp_filters(config.seccomp_action)?;
	Ok(())
}
use std::{
	collections::{BTreeMap, BTreeSet},
	convert::TryInto,
	io,
	path::Path,
};

use landlock::{
	ABI, Access, AccessFs, CompatLevel, Compatible, Ruleset, RulesetAttr, RulesetCreatedAttr,
	path_beneath_rules,
};
use seccompiler::{BpfProgram, SeccompAction as SeccompilerAction, SeccompFilter};

use crate::result::err;
const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;
const PR_CAPBSET_DROP: libc::c_int = 24;
const PR_CAP_AMBIENT: libc::c_int = 47;
const PR_CAP_AMBIENT_CLEAR_ALL: libc::c_ulong = 4;
const PR_SET_KEEPCAPS: libc::c_int = 8;

/// Keep enough descriptors for already-open devices plus control/snapshot
/// bookkeeping, but cap very large inherited limits once the launch surface
/// has been opened.
const NOFILE_LIMIT_CEILING: libc::rlim_t = 1024;
const NOFILE_RESERVE: libc::rlim_t = 64;
fn set_no_new_privs() -> Result<()> {
	let rc = unsafe {
		libc::prctl(
			PR_SET_NO_NEW_PRIVS,
			1 as libc::c_ulong,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
		)
	};
	if rc != 0 {
		return Err(os_error("prctl(PR_SET_NO_NEW_PRIVS)"));
	}
	Ok(())
}

fn disable_keepcaps() -> Result<()> {
	let rc = unsafe {
		libc::prctl(
			PR_SET_KEEPCAPS,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
		)
	};
	if rc != 0 {
		return Err(os_error("prctl(PR_SET_KEEPCAPS, 0)"));
	}
	Ok(())
}

fn tighten_resource_limits() -> Result<()> {
	clamp_nofile()?;
	set_limit(libc::RLIMIT_CORE, 0, 0, "setrlimit(RLIMIT_CORE)")?;
	freeze_soft_limit(libc::RLIMIT_FSIZE, "setrlimit(RLIMIT_FSIZE)")?;
	Ok(())
}

fn clamp_nofile() -> Result<()> {
	let mut current = get_limit(libc::RLIMIT_NOFILE, "getrlimit(RLIMIT_NOFILE)")?;
	let inherited_soft = current.rlim_cur;
	let open_fds = open_fd_count().unwrap_or(0);
	let reserve = open_fds.saturating_add(NOFILE_RESERVE);
	let mut target = inherited_soft.min(NOFILE_LIMIT_CEILING);
	if target < reserve && reserve <= inherited_soft {
		target = reserve;
	}
	target = target.min(current.rlim_max);
	current.rlim_cur = target;
	current.rlim_max = target;
	set_rlimit(libc::RLIMIT_NOFILE, current, "setrlimit(RLIMIT_NOFILE)")
}

fn freeze_soft_limit(resource: libc::__rlimit_resource_t, context: &str) -> Result<()> {
	let current = get_limit(resource, context)?;
	set_limit(resource, current.rlim_cur, current.rlim_cur, context)
}

fn set_limit(
	resource: libc::__rlimit_resource_t,
	soft: libc::rlim_t,
	hard: libc::rlim_t,
	context: &str,
) -> Result<()> {
	set_rlimit(resource, libc::rlimit { rlim_cur: soft, rlim_max: hard }, context)
}

fn get_limit(resource: libc::__rlimit_resource_t, context: &str) -> Result<libc::rlimit> {
	let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
	let rc = unsafe { libc::getrlimit(resource, &mut limit) };
	if rc != 0 {
		return Err(os_error(context));
	}
	Ok(limit)
}

fn set_rlimit(
	resource: libc::__rlimit_resource_t,
	limit: libc::rlimit,
	context: &str,
) -> Result<()> {
	let rc = unsafe { libc::setrlimit(resource, &limit) };
	if rc != 0 {
		return Err(os_error(context));
	}
	Ok(())
}

fn open_fd_count() -> Option<libc::rlim_t> {
	std::fs::read_dir(Path::new("/proc/self/fd"))
		.ok()
		.map(|fds| fds.count() as libc::rlim_t)
}

#[allow(dead_code)]
pub fn apply_landlock_rules(paths: &SandboxPaths) -> Result<()> {
	let abi = ABI::V3;
	let all = AccessFs::from_all(abi);
	let read = AccessFs::from_read(abi);
	let rw_file = landlock::make_bitflags!(AccessFs::{ReadFile | WriteFile | Truncate});
	let rw_dir = AccessFs::from_all(abi);

	Ruleset::default()
		.set_compatibility(CompatLevel::BestEffort)
		.handle_access(all)
		.map_err(|e| err(format!("creating Landlock ruleset: {e}")))?
		.create()
		.map_err(|e| err(format!("creating Landlock ruleset: {e}")))?
		.add_rules(path_beneath_rules(paths.ro.iter(), read))
		.map_err(|e| err(format!("adding Landlock read-only file rules: {e}")))?
		.add_rules(path_beneath_rules(paths.ro_dirs.iter(), read))
		.map_err(|e| err(format!("adding Landlock read-only directory rules: {e}")))?
		.add_rules(path_beneath_rules(paths.rw.iter(), rw_file))
		.map_err(|e| err(format!("adding Landlock read/write file rules: {e}")))?
		.add_rules(path_beneath_rules(paths.rw_dirs.iter(), rw_dir))
		.map_err(|e| err(format!("adding Landlock read/write directory rules: {e}")))?
		.restrict_self()
		.map_err(|e| err(format!("installing Landlock ruleset: {e}")))?;
	Ok(())
}

#[allow(dead_code)]
pub fn apply_seccomp_filters(action: SeccompAction) -> Result<()> {
	let filter = compile_seccomp_filter(action)?;
	seccompiler::apply_filter(&filter).map_err(|e| err(format!("installing seccomp filter: {e}")))
}

#[allow(dead_code)]
pub fn compile_seccomp_filter(action: SeccompAction) -> Result<BpfProgram> {
	// `SeccompFilter::new(rules, mismatch_action, match_action, arch)`:
	// `mismatch_action` hits syscalls outside the allowlist, `match_action`
	// hits allowlisted ones. Allowlisted syscalls must be allowed; all
	// others get the punishing default action.
	let filter: BpfProgram = SeccompFilter::new(
		seccomp_rules(),
		seccomp_default_action(action),
		SeccompilerAction::Allow,
		std::env::consts::ARCH
			.try_into()
			.map_err(|_| err(format!("unsupported seccomp arch {}", std::env::consts::ARCH)))?,
	)
	.map_err(|e| err(format!("building seccomp filter: {e}")))?
	.try_into()
	.map_err(|e| err(format!("compiling seccomp BPF: {e}")))?;
	Ok(filter)
}

fn seccomp_default_action(action: SeccompAction) -> SeccompilerAction {
	match action {
		// The CLI value is historically named `kill`, but Phase 5's audit
		// default is SIGSYS/Trap so denials are diagnosable.
		SeccompAction::Kill => SeccompilerAction::Trap,
		SeccompAction::Errno => SeccompilerAction::Errno(libc::EPERM as u32),
		SeccompAction::Log => SeccompilerAction::Log,
	}
}

fn seccomp_rules() -> BTreeMap<i64, Vec<seccompiler::SeccompRule>> {
	allowlisted_syscalls()
		.into_iter()
		.map(|nr| (nr, Vec::new()))
		.collect()
}

fn allowlisted_syscalls() -> BTreeSet<i64> {
	#[allow(unused_mut)]
	let mut syscalls = BTreeSet::from([
		libc::SYS_accept4,
		libc::SYS_brk,
		libc::SYS_clock_nanosleep,
		libc::SYS_clone,
		libc::SYS_clone3,
		libc::SYS_close,
		libc::SYS_epoll_create1,
		libc::SYS_epoll_ctl,
		libc::SYS_epoll_pwait,
		libc::SYS_eventfd2,
		libc::SYS_exit,
		libc::SYS_exit_group,
		libc::SYS_fallocate,
		libc::SYS_fchmodat,
		libc::SYS_fchownat,
		libc::SYS_fcntl,
		libc::SYS_fdatasync,
		libc::SYS_fstat,
		libc::SYS_fsync,
		libc::SYS_ftruncate,
		libc::SYS_futex,
		libc::SYS_getdents64,
		libc::SYS_getpid,
		libc::SYS_getrandom,
		libc::SYS_getrlimit,
		libc::SYS_getsockopt,
		libc::SYS_gettid,
		libc::SYS_io_uring_enter,
		libc::SYS_io_uring_register,
		libc::SYS_io_uring_setup,
		libc::SYS_ioctl,
		libc::SYS_kill,
		libc::SYS_lseek,
		libc::SYS_madvise,
		libc::SYS_memfd_create,
		libc::SYS_mkdirat,
		libc::SYS_mmap,
		libc::SYS_mprotect,
		libc::SYS_mremap,
		libc::SYS_munmap,
		libc::SYS_nanosleep,
		libc::SYS_newfstatat,
		libc::SYS_openat,
		libc::SYS_ppoll,
		libc::SYS_prctl,
		libc::SYS_pread64,
		libc::SYS_preadv2,
		libc::SYS_pwrite64,
		libc::SYS_pwritev2,
		libc::SYS_read,
		libc::SYS_readlinkat,
		libc::SYS_readv,
		libc::SYS_recvfrom,
		libc::SYS_recvmsg,
		// glibc `rename`/`fs::rename` uses renameat on aarch64 (no plain rename
		// syscall there) and renameat2 elsewhere; snapshot publish needs both.
		libc::SYS_renameat,
		libc::SYS_renameat2,
		libc::SYS_restart_syscall,
		libc::SYS_rseq,
		libc::SYS_rt_sigaction,
		libc::SYS_rt_sigprocmask,
		libc::SYS_rt_sigreturn,
		libc::SYS_sched_getaffinity,
		libc::SYS_sched_yield,
		libc::SYS_sendmsg,
		libc::SYS_sendto,
		libc::SYS_set_robust_list,
		libc::SYS_setrlimit,
		// The control/agent servers set SO_RCVTIMEO/SO_SNDTIMEO on accepted
		// connections via setsockopt; without this the sandbox drops every
		// control client before the banner.
		libc::SYS_setsockopt,
		libc::SYS_sigaltstack,
		libc::SYS_statx,
		libc::SYS_tgkill,
		libc::SYS_unlinkat,
		libc::SYS_wait4,
		libc::SYS_write,
		libc::SYS_writev,
	]);
	#[cfg(target_arch = "x86_64")]
	{
		// x86_64 glibc sets the TLS base via arch_prctl when threads are
		// created (e.g. the agent-bridge thread spawned after the filter is
		// installed); aarch64 uses a register and needs no syscall here.
		syscalls.insert(libc::SYS_arch_prctl);
		syscalls.insert(libc::SYS_lchown);
		syscalls.insert(libc::SYS_poll);
	}
	syscalls
}

#[allow(dead_code)]
pub fn drop_root_privileges(config: &SandboxConfig) -> Result<()> {
	let euid = unsafe { libc::geteuid() as u32 };
	if euid != 0 {
		if let Some(uid) = config.uid
			&& uid != euid
		{
			return Err(err(format!("--sandbox-uid {uid} does not match current uid {euid}")));
		}
		if let Some(gid) = config.gid {
			let egid = unsafe { libc::getegid() as u32 };
			if gid != egid {
				return Err(err(format!("--sandbox-gid {gid} does not match current gid {egid}")));
			}
		}
		return Ok(());
	}

	let uid = config
		.uid
		.ok_or_else(|| err("--sandbox as root requires --sandbox-uid or VMON_SANDBOX_UID"))?;
	let gid = config
		.gid
		.ok_or_else(|| err("--sandbox as root requires --sandbox-gid or VMON_SANDBOX_GID"))?;
	if uid == 0 {
		return Err(err("--sandbox-uid must not be 0 when starting as root"));
	}
	if gid == 0 {
		return Err(err("--sandbox-gid must not be 0 when starting as root"));
	}

	clear_ambient_capabilities()?;
	drop_capability_bounding_set()?;
	disable_keepcaps()?;
	clear_supplementary_groups()?;

	let rc = unsafe { libc::setgid(gid as libc::gid_t) };
	if rc != 0 {
		return Err(os_error("setgid(--sandbox-gid)"));
	}
	let rc = unsafe { libc::setuid(uid as libc::uid_t) };
	if rc != 0 {
		return Err(os_error("setuid(--sandbox-uid)"));
	}
	if unsafe { libc::geteuid() } == 0 {
		return Err(err("--sandbox uid drop left effective uid 0"));
	}
	Ok(())
}

fn clear_supplementary_groups() -> Result<()> {
	let rc = unsafe { libc::setgroups(0, std::ptr::null()) };
	if rc != 0 {
		return Err(os_error("setgroups(0)"));
	}
	Ok(())
}

fn clear_ambient_capabilities() -> Result<()> {
	let rc = unsafe {
		libc::prctl(
			PR_CAP_AMBIENT,
			PR_CAP_AMBIENT_CLEAR_ALL,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
			0 as libc::c_ulong,
		)
	};
	if rc != 0 {
		let error = io::Error::last_os_error();
		if error.raw_os_error() != Some(libc::EINVAL) {
			return Err(err(format!("prctl(PR_CAP_AMBIENT_CLEAR_ALL): {error}")));
		}
	}
	Ok(())
}

fn drop_capability_bounding_set() -> Result<()> {
	for cap in 0..=last_capability() {
		let rc = unsafe {
			libc::prctl(
				PR_CAPBSET_DROP,
				cap as libc::c_ulong,
				0 as libc::c_ulong,
				0 as libc::c_ulong,
				0 as libc::c_ulong,
			)
		};
		if rc != 0 {
			let error = io::Error::last_os_error();
			if error.raw_os_error() != Some(libc::EINVAL) {
				return Err(err(format!("prctl(PR_CAPBSET_DROP, {cap}): {error}")));
			}
		}
	}
	Ok(())
}

fn last_capability() -> u32 {
	std::fs::read_to_string("/proc/sys/kernel/cap_last_cap")
		.ok()
		.and_then(|s| s.trim().parse().ok())
		.unwrap_or(63)
}

fn os_error(context: &str) -> crate::result::Error {
	err(format!("{context}: {}", io::Error::last_os_error()))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn seccomp_filter_compiles() {
		compile_seccomp_filter(SeccompAction::Errno).expect("compile seccomp allowlist");
	}

	/// Guard against the allowlist becoming a denylist: an allowlisted syscall
	/// must run while a non-allowlisted one is rejected. Runs in a forked child
	/// because installing a seccomp filter is irreversible.
	#[test]
	fn seccomp_filter_allows_listed_and_denies_others() {
		// Compile in the parent so the child only performs async-signal-safe
		// work (prctl + apply + raw syscalls) after fork.
		let prog = compile_seccomp_filter(SeccompAction::Errno).expect("compile");
		// SAFETY: the child path touches only fork-safe libc/seccomp calls and
		// exits via `_exit`; the parent only reaps it.
		let pid = unsafe { libc::fork() };
		assert!(pid >= 0, "fork failed");
		if pid == 0 {
			let code = unsafe {
				if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
					10
				} else if seccompiler::apply_filter(&prog).is_err() {
					11
				} else if libc::syscall(libc::SYS_getpid) < 0 {
					// getpid is allowlisted -> must succeed.
					12
				} else if libc::syscall(libc::SYS_getppid) != -1 {
					// getppid is NOT allowlisted -> Errno action must reject it.
					13
				} else if std::io::Error::last_os_error().raw_os_error() != Some(libc::EPERM) {
					14
				} else {
					0
				}
			};
			unsafe { libc::_exit(code) };
		}
		let mut status = 0i32;
		assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid, "waitpid");
		assert!(libc::WIFEXITED(status), "child killed, status {status}");
		assert_eq!(
			libc::WEXITSTATUS(status),
			0,
			"seccomp enforcement check failed (child code in WEXITSTATUS)"
		);
	}
}
