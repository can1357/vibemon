mod common;

use std::time::Duration;

#[test]
fn boots_initramfs_and_prints_marker() {
	if !common::require_hv() {
		return;
	}

	let args = common::base_args("boot");
	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "VMON_OK", Duration::from_mins(1));

	assert!(output.contains("VMON_OK"), "boot marker missing:\n{output}");
	common::assert_no_panic(&output);
}
