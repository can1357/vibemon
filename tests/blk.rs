mod common;

use std::time::Duration;

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
