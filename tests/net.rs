mod common;

use std::{
	io::Read,
	net::TcpListener,
	thread,
	time::{Duration, Instant},
};

#[test]
fn virtio_net_pings_host_tap() {
	if !common::require_hv() || !common::supports_tap() {
		return;
	}

	let tap = match std::env::var("VMON_TAP") {
		Ok(tap) if !tap.is_empty() => tap,
		_ => return,
	};
	let host_ip = std::env::var("VMON_HOST_IP").unwrap_or_else(|_| "192.168.249.1".into());
	let cmdline = common::cmdline_with("net", &[("vmon.host_ip", host_ip.as_str())]);
	let mut args = common::base_args_with_cmdline(cmdline);
	args.push("--tap".into());
	args.push(tap);

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "NET_OK", Duration::from_secs(90));

	assert!(output.contains("NET_OK"), "network marker missing:\n{output}");
	common::assert_no_panic(&output);
}

#[test]
fn virtio_net_bulk_throughput() {
	if !common::require_hv() || !common::supports_tap() {
		return;
	}

	let tap = match std::env::var("VMON_TAP") {
		Ok(tap) if !tap.is_empty() => tap,
		_ => return,
	};
	let host_ip = std::env::var("VMON_HOST_IP").unwrap_or_else(|_| "192.168.249.1".into());
	let port = common::parse_env_usize("VMON_TPUT_PORT", 5050) as u16;
	let mib = common::parse_env_usize("VMON_TPUT_MIB", 64);
	let floor = common::parse_env_usize("VMON_TPUT_FLOOR", 100);

	// Host sink: accept the guest's bulk stream and count bytes. Bind before boot
	// so the SYN is never refused; nonblocking accept + a read deadline guarantee
	// the drain thread can never wedge the test if the guest dies early.
	let listener = TcpListener::bind(("0.0.0.0", port))
		.unwrap_or_else(|e| panic!("binding throughput sink on 0.0.0.0:{port}: {e}"));
	listener
		.set_nonblocking(true)
		.expect("setting throughput sink nonblocking");
	let sink = thread::spawn(move || drain_one(&listener, Duration::from_mins(2)));

	let port_s = port.to_string();
	let mib_s = mib.to_string();
	let floor_s = floor.to_string();
	let cmdline = common::cmdline_with("net_throughput", &[
		("vmon.host_ip", host_ip.as_str()),
		("vmon.tput_port", port_s.as_str()),
		("vmon.tput_mib", mib_s.as_str()),
		("vmon.tput_floor", floor_s.as_str()),
	]);
	let mut args = common::base_args_with_cmdline(cmdline);
	args.push("--tap".into());
	args.push(tap);

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "THROUGHPUT_OK", Duration::from_mins(2));

	assert!(output.contains("THROUGHPUT_OK"), "throughput marker missing:\n{output}");
	common::assert_no_panic(&output);

	// The guest's THROUGHPUT_OK already gates on the MiB/s floor; additionally
	// confirm the host actually received the bulk stream, tolerating a small
	// short read if the connection is torn down at the very end of the transfer.
	let received = sink.join().expect("throughput sink thread panicked");
	let expected = mib * 1024 * 1024 * 9 / 10;
	assert!(
		received >= expected,
		"host sink received only {received} bytes, expected >= {expected}"
	);
}

#[test]
fn virtio_user_net_static_nat_reaches_host() {
	if !common::require_hv() || !common::supports_user_net() {
		return;
	}
	user_net_transfer("usernet", &[]);
}

#[test]
fn virtio_user_net_dhcp_assigns_lease() {
	if !common::require_hv() || !common::supports_user_net() {
		return;
	}
	user_net_transfer("usernet_dhcp", &["USERNET_DHCP_OK"]);
}

/// Boot with `--net user`, drive the guest `mode`, and confirm the guest
/// streamed bulk data outbound through the slirp NAT to a host listener on the
/// loopback. libslirp maps the gateway 10.0.2.2 to the host's 127.0.0.1, so the
/// guest's connection to 10.0.2.2:<port> lands on the host sink.
/// `extra_markers` are additional guest serial markers that must appear (e.g. a
/// DHCP lease).
fn user_net_transfer(mode: &str, extra_markers: &[&str]) {
	let port = common::parse_env_usize("VMON_TPUT_PORT", 5050) as u16;
	let mib = common::parse_env_usize("VMON_USERNET_MIB", 8);

	let listener = TcpListener::bind(("127.0.0.1", port))
		.unwrap_or_else(|e| panic!("binding user-net sink on 127.0.0.1:{port}: {e}"));
	listener
		.set_nonblocking(true)
		.expect("setting user-net sink nonblocking");
	let sink = thread::spawn(move || drain_one(&listener, Duration::from_mins(2)));

	let port_s = port.to_string();
	let mib_s = mib.to_string();
	let cmdline = common::cmdline_with(mode, &[
		("vmon.host_ip", "10.0.2.2"),
		("vmon.tput_port", port_s.as_str()),
		("vmon.tput_mib", mib_s.as_str()),
	]);
	let mut args = common::base_args_with_cmdline(cmdline);
	args.push("--net".into());
	args.push("user".into());

	let refs = common::as_refs(&args);
	let output = common::boot_capture(&refs, "USERNET_OK", Duration::from_mins(2));

	assert!(output.contains("USERNET_OK"), "user-net marker missing:\n{output}");
	for marker in extra_markers {
		assert!(output.contains(marker), "expected user-net marker {marker:?} missing:\n{output}");
	}
	common::assert_no_panic(&output);

	// Confirm the host actually received the bulk stream over the NAT, tolerating
	// a small short read if the connection is torn down at the very end.
	let received = sink.join().expect("user-net sink thread panicked");
	let expected = mib * 1024 * 1024 * 9 / 10;
	assert!(
		received >= expected,
		"host sink received only {received} bytes, expected >= {expected}"
	);
}

/// Accept a single connection on `listener` and drain it to EOF, returning the
/// byte count. Bounded by `deadline` so a stalled or absent guest can't hang
/// the test; returns whatever was read so far when the deadline elapses.
fn drain_one(listener: &TcpListener, deadline: Duration) -> usize {
	let deadline = Instant::now() + deadline;
	let mut stream = loop {
		match listener.accept() {
			Ok((stream, _)) => break stream,
			Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
				if Instant::now() >= deadline {
					return 0;
				}
				thread::sleep(Duration::from_millis(20));
			},
			Err(_) => return 0,
		}
	};
	// Switch to blocking reads with a timeout so EOF or a dead peer both unblock.
	let _ = stream.set_nonblocking(false);
	let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
	let mut buf = vec![0u8; 256 * 1024];
	let mut total = 0usize;
	loop {
		match stream.read(&mut buf) {
			Ok(0) => break,
			Ok(n) => total += n,
			Err(ref e)
				if e.kind() == std::io::ErrorKind::WouldBlock
					|| e.kind() == std::io::ErrorKind::TimedOut => {},
			Err(_) => break,
		}
		if Instant::now() >= deadline {
			break;
		}
	}
	total
}
