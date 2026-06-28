mod common;

use std::time::Duration;

/// Boot with `--rng` and confirm the guest sees a working `/dev/hwrng`: the
/// test init waits for the char device and reads 32 bytes of entropy from it
/// before printing `RNG_OK` (see the `rng)` arm in
/// `demo/fetch-test-assets.sh`). This exercises the whole data path — device
/// activation, the requestq worker, and the used-ring completion the guest
/// hwrng core consumes.
#[test]
fn rng_device_serves_entropy_to_guest() {
	if !common::require_hv() {
		return;
	}

	let mut args = common::base_args("rng");
	args.push("--rng".into());
	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "RNG_OK", Duration::from_mins(1));

	assert!(output.contains("RNG_OK"), "rng marker missing:\n{output}");
	common::assert_no_panic(&output);
}
