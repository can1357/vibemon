# Security Policy

## Supported versions

`vmon` has not published a stable release. Security fixes target the current `main` branch and any maintained release branch listed here.

| Version | Supported |
| --- | --- |
| Unreleased / 0.1.x | Yes |
| Earlier versions | No |

## Firmware supply policy

`vmon` does not vendor firmware blobs. Operators must supply UEFI firmware explicitly with `--firmware`; production deployments should pin the firmware build, record its source URL, and verify a sha256 digest before use. Treat firmware updates like hypervisor updates: stage them, keep rollback artifacts, and do not fetch unsigned firmware at VM launch time.

## Reporting a vulnerability

Please do not report suspected vulnerabilities in public issues.

Use GitHub's private vulnerability reporting for this repository (`Security` -> `Report a vulnerability`). If private reporting is unavailable, contact the repository maintainers out of band before publishing details.

Include:

- affected commit or version;
- host architecture and Linux/KVM version;
- guest inputs needed to reproduce the issue;
- whether `/dev/kvm`, TAP, virtio-fs, snapshots, or the control socket are involved;
- expected and observed impact.

Maintainers should acknowledge reports within 7 days when possible, coordinate a fix on a private branch or advisory, and publish credit if the reporter wants it.
