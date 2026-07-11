mod common;

use std::{fs, time::Duration};

/// `--fs-tag`/`--fs-dir`: a host directory shared read-only over virtio-fs.
/// Skips when the guest kernel lacks virtio-fs (guest emits `FSSHARE_NODEV`).
#[test]
fn virtiofs_host_share_readonly() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("fs-share");
	let share = dir.join("share");
	fs::create_dir_all(&share).unwrap_or_else(|e| panic!("creating {}: {e}", share.display()));
	fs::write(share.join("SHARE.txt"), "vmon share fixture\n")
		.unwrap_or_else(|e| panic!("writing SHARE.txt: {e}"));

	let mut args = common::base_args("fsshare");
	args.push("--fs-tag".into());
	args.push("host".into());
	args.push("--fs-dir".into());
	args.push(share.display().to_string());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "FSSHARE_", Duration::from_secs(90));
	if output.contains("FSSHARE_NODEV") {
		eprintln!("SKIP virtiofs_host_share_readonly: guest kernel lacks virtio-fs");
		return;
	}

	assert!(output.contains("FSSHARE_OK"), "virtio-fs share marker missing:\n{output}");
	common::assert_no_panic(&output);
}

/// `--volume`: a writable volume round-trips a guest write to the host dir,
/// and a `:ro` volume rejects guest writes.
#[test]
fn virtiofs_volumes_rw_and_ro() {
	if !common::require_hv() {
		return;
	}

	let dir = common::test_dir("fs-volume");
	let data = dir.join("data");
	let snap = dir.join("snap");
	fs::create_dir_all(&data).unwrap_or_else(|e| panic!("creating {}: {e}", data.display()));
	fs::create_dir_all(&snap).unwrap_or_else(|e| panic!("creating {}: {e}", snap.display()));

	let mut args = common::base_args("volume");
	args.push("--volume".into());
	args.push(format!("data:{}", data.display()));
	args.push("--volume".into());
	args.push(format!("snap:{}:ro", snap.display()));

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "VOLUME_", Duration::from_secs(90));
	if output.contains("VOLUME_NODEV") {
		eprintln!("SKIP virtiofs_volumes_rw_and_ro: guest kernel lacks virtio-fs");
		return;
	}

	assert!(output.contains("VOLUME_OK"), "rw volume marker missing:\n{output}");
	assert!(output.contains("VOLUME_RO_OK"), "ro volume marker missing:\n{output}");
	common::assert_no_panic(&output);

	let out = data.join("OUT.txt");
	let written =
		fs::read_to_string(&out).unwrap_or_else(|e| panic!("reading {}: {e}", out.display()));
	assert_eq!(written, "hello", "guest volume write did not round-trip to the host");
}
