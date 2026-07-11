mod common;

use std::{
	fs,
	hash::{DefaultHasher, Hasher},
	path::Path,
	time::Duration,
};

#[test]
fn virtio_blk_writes_and_reads_back() {
	if !common::require_hv() {
		return;
	}

	let rootfs = common::copy_rootfs("blk");
	let mut args = common::base_args("blk");
	args.push("--rootfs".into());
	args.push(rootfs.display().to_string());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "BLK_OK", Duration::from_secs(90));

	assert!(output.contains("BLK_OK"), "block marker missing:\n{output}");
	common::assert_no_panic(&output);
}

/// `--rootfs-ro`: the guest sees a read-only virtio-blk device — a rw mount
/// fails, a ro mount reads the fixture marker.
#[test]
fn rootfs_ro_rejects_guest_writes() {
	if !common::require_hv() {
		return;
	}

	let rootfs = common::copy_rootfs("rootfs-ro");
	let mut args = common::base_args("rootfs_ro");
	args.push("--rootfs".into());
	args.push(rootfs.display().to_string());
	args.push("--rootfs-ro".into());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "ROOTFS_RO_OK", Duration::from_secs(90));

	assert!(output.contains("ROOTFS_RO_OK"), "read-only rootfs marker missing:\n{output}");
	common::assert_no_panic(&output);
}

/// `--disk-overlay-of`: guest writes land in a fresh `CoW` overlay while the
/// base image stays byte-identical.
#[test]
fn disk_overlay_leaves_base_pristine() {
	if !common::require_hv() {
		return;
	}

	let base = common::copy_rootfs("disk-overlay");
	let base_hash = hash_file(&base);
	let overlay = base
		.parent()
		.expect("rootfs copy has a parent dir")
		.join("overlay.img");

	let mut args = common::base_args("blk");
	args.push("--disk-overlay-of".into());
	args.push(base.display().to_string());
	args.push("--rootfs".into());
	args.push(overlay.display().to_string());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "BLK_OK", Duration::from_secs(90));

	assert!(output.contains("BLK_OK"), "block marker missing:\n{output}");
	common::assert_no_panic(&output);

	assert_eq!(hash_file(&base), base_hash, "overlay boot mutated the base image");
	assert!(overlay.is_file(), "overlay file was not created");
	assert_ne!(
		hash_file(&overlay),
		base_hash,
		"overlay is byte-identical to the base after guest writes"
	);
}
fn hash_file(path: &Path) -> u64 {
	let bytes = fs::read(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
	let mut hasher = DefaultHasher::new();
	hasher.write(&bytes);
	hasher.finish()
}
