//! Narrow privileged Linux networking broker protocol and Unix-socket server.

#[cfg(any(test, target_os = "linux"))]
use std::io::{Read, Write};
use std::path::Path;
#[cfg(target_os = "linux")]
use std::{fs, os::unix::fs::MetadataExt};

use serde::{Deserialize, Serialize};

use super::{TapLease, slots::SlotSpec};
#[cfg(target_os = "linux")]
use crate::security::CREDENTIAL_GATEWAY_PORT;
use crate::{EngineError, Result};

/// Typed requests accepted by the privileged network broker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
	/// Create a TAP and install its fail-closed forwarding policy.
	Setup {
		name:                  String,
		guest_ip:              String,
		host_ip:               String,
		prefix:                u8,
		egress_allow:          Option<Vec<String>>,
		egress_allow_domains:  Option<Vec<String>>,
		previous_egress_allow: Option<Vec<String>>,
	},
	/// Permit the fixed credential gateway port for one existing lease.
	AllowCredential { lease: Lease },
	/// Remove a TAP and every policy rule associated with it.
	Teardown {
		name:                 String,
		guest_ip:             Option<String>,
		host_ip:              Option<String>,
		prefix:               u8,
		egress_allow:         Option<Vec<String>>,
		egress_allow_domains: Option<Vec<String>>,
	},
	/// Create a batch of pooled slot TAPs with the base policy plus the
	/// pooled-slot nftables skeleton preinstalled. `reset` wipes and
	/// recreates the slot table (first batch of a pool fill).
	PreallocateSlots { slots: Vec<SlotSpec>, reset: bool },
	/// Apply a custom egress policy to one pooled slot as a single batched
	/// `nft -f -` invocation.
	ClaimSlot {
		slot:                 SlotSpec,
		egress_allow:         Vec<String>,
		egress_allow_domains: Vec<String>,
		already_custom:       bool,
	},
	/// Reset one pooled slot's dynamic policy back to the preinstalled base.
	RecycleSlot { slot: SlotSpec },
}

/// A validated TAP identity sent to the broker.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
	pub name:         String,
	pub guest_ip:     String,
	pub host_ip:      String,
	pub prefix:       u8,
	pub egress_allow: Vec<String>,
}

impl From<&TapLease> for Lease {
	fn from(value: &TapLease) -> Self {
		Self {
			name:         value.name.clone(),
			guest_ip:     value.guest_ip.clone(),
			host_ip:      value.host_ip.clone(),
			prefix:       value.prefix,
			egress_allow: value.egress_allow.clone(),
		}
	}
}

#[cfg(target_os = "linux")]
#[derive(Debug, Serialize, Deserialize)]
enum Response {
	Ok,
	Lease(TapLease),
	Error(String),
}

/// Connect to a configured broker and execute one typed request.
pub fn call(socket: &Path, request: Request) -> Result<Option<TapLease>> {
	#[cfg(target_os = "linux")]
	{
		use std::os::unix::net::UnixStream;
		let metadata = fs::metadata(socket).map_err(|error| {
			EngineError::engine(format!(
				"network broker socket {} is unavailable: {error}",
				socket.display()
			))
		})?;
		if metadata.mode() & 0o077 != 0 {
			return Err(EngineError::engine(
				"network broker socket must not be accessible to group or other users",
			));
		}
		let mut stream = UnixStream::connect(socket).map_err(|error| {
			EngineError::engine(format!(
				"cannot connect to network broker {}: {error}",
				socket.display()
			))
		})?;
		write_frame(&mut stream, &request)?;
		match read_frame(&mut stream)? {
			Response::Ok => Ok(None),
			Response::Lease(lease) => Ok(Some(lease)),
			Response::Error(message) => {
				Err(EngineError::engine(format!("network broker rejected request: {message}")))
			},
		}
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = (socket, request);
		Err(EngineError::unsupported("host TAP networking requires Linux"))
	}
}
/// Run the privileged broker until the process is terminated.
pub fn serve(socket: &Path, owner_uid: Option<u32>) -> Result<()> {
	#[cfg(target_os = "linux")]
	{
		use std::os::unix::{
			fs::{FileTypeExt, PermissionsExt},
			net::UnixListener,
		};
		if socket.exists() {
			let metadata = fs::symlink_metadata(socket)?;
			if !metadata.file_type().is_socket() {
				return Err(EngineError::invalid(
					"network broker socket path already exists and is not a socket",
				));
			}
			fs::remove_file(socket)?;
		}
		let listener = UnixListener::bind(socket)?;
		let owner_uid = authorized_owner_uid(owner_uid)?;
		if owner_uid != current_uid() {
			set_socket_owner(socket, owner_uid)?;
		}
		fs::set_permissions(socket, fs::Permissions::from_mode(0o600))?;
		let socket_uid = fs::metadata(socket)?.uid();
		// Connections are short-lived request/response pairs from the single
		// authorized owner uid, so unbounded detached spawn is acceptable here.
		for connection in listener.incoming() {
			match connection {
				Ok(mut stream) => {
					let _ = std::thread::Builder::new()
						.name("vmon-net-broker-conn".to_owned())
						.spawn(move || {
							let response = match peer_uid(&stream) {
								Ok(peer_uid) if peer_uid == socket_uid => {
									match read_frame::<_, Request>(&mut stream)
										.and_then(|request| dispatch(request, peer_uid))
									{
										Ok(response) => response,
										Err(error) => Response::Error(error.to_string()),
									}
								},
								_ => Response::Error(
									"network broker caller is not the socket owner".to_owned(),
								),
							};
							let _ = write_frame(&mut stream, &response);
						});
				},
				Err(error) => return Err(EngineError::from(error)),
			}
		}
		Ok(())
	}
	#[cfg(not(target_os = "linux"))]
	{
		let _ = (socket, owner_uid);
		Err(EngineError::unsupported("network broker requires Linux"))
	}
}

#[cfg(target_os = "linux")]
fn dispatch(request: Request, owner_uid: u32) -> Result<Response> {
	match request {
		Request::Setup {
			name,
			guest_ip,
			host_ip,
			prefix,
			egress_allow,
			egress_allow_domains,
			previous_egress_allow,
		} => {
			validate(
				&name,
				&guest_ip,
				&host_ip,
				prefix,
				egress_allow.as_deref(),
				egress_allow_domains.as_deref(),
			)?;
			Ok(Response::Lease(super::setup_tap_direct(
				&name,
				&guest_ip,
				&host_ip,
				prefix,
				egress_allow.as_deref(),
				egress_allow_domains.as_deref(),
				previous_egress_allow.as_deref(),
				owner_uid,
			)?))
		},
		Request::AllowCredential { lease } => {
			validate(
				&lease.name,
				&lease.guest_ip,
				&lease.host_ip,
				lease.prefix,
				Some(&lease.egress_allow),
				None,
			)?;
			validate_port(CREDENTIAL_GATEWAY_PORT)?;
			super::allow_credential_gateway_direct(&TapLease::new(
				&lease.name,
				&lease.guest_ip,
				&lease.host_ip,
				lease.prefix,
				Some(&lease.egress_allow),
			)?)?;
			Ok(Response::Ok)
		},
		Request::Teardown { name, guest_ip, host_ip, prefix, egress_allow, egress_allow_domains } => {
			if guest_ip.is_some() != host_ip.is_some() {
				return Err(EngineError::invalid(
					"network broker teardown requires both guest and host IP addresses",
				));
			}
			if let (Some(guest_ip), Some(host_ip)) = (&guest_ip, &host_ip) {
				validate(
					&name,
					guest_ip,
					host_ip,
					prefix,
					egress_allow.as_deref(),
					egress_allow_domains.as_deref(),
				)?;
			} else if !valid_name(&name) {
				return Err(EngineError::invalid("invalid TAP name"));
			}
			super::teardown_tap_direct(
				&name,
				guest_ip.as_deref(),
				host_ip.as_deref(),
				prefix,
				egress_allow.as_deref(),
				egress_allow_domains.as_deref(),
			)?;
			Ok(Response::Ok)
		},
		Request::PreallocateSlots { slots, reset } => {
			if slots.is_empty() || slots.len() > 1024 {
				return Err(EngineError::invalid("network broker slot batch must hold 1..=1024 slots"));
			}
			for slot in &slots {
				validate_slot(slot)?;
			}
			super::preallocate_slots_direct(&slots, reset, owner_uid)?;
			Ok(Response::Ok)
		},
		Request::ClaimSlot { slot, egress_allow, egress_allow_domains, already_custom } => {
			validate_slot(&slot)?;
			validate(
				&slot.name,
				&slot.guest_ip,
				&slot.host_ip,
				slot.prefix,
				Some(&egress_allow),
				Some(&egress_allow_domains),
			)?;
			super::claim_slot_direct(&slot, &egress_allow, &egress_allow_domains, already_custom)?;
			Ok(Response::Ok)
		},
		Request::RecycleSlot { slot } => {
			validate_slot(&slot)?;
			super::recycle_slot_direct(&slot)?;
			Ok(Response::Ok)
		},
	}
}

/// Validate a pooled slot's identity: name derives from the index (so nft
/// set names cannot diverge from the TAP) and addresses form a valid lease.
#[cfg(any(test, target_os = "linux"))]
fn validate_slot(slot: &SlotSpec) -> Result<()> {
	if slot.name != super::slots::slot_tap_name(slot.index) {
		return Err(EngineError::invalid("slot TAP name does not match its index"));
	}
	validate(&slot.name, &slot.guest_ip, &slot.host_ip, slot.prefix, None, None)
}

#[cfg(any(test, target_os = "linux"))]
fn validate(
	name: &str,
	guest_ip: &str,
	host_ip: &str,
	prefix: u8,
	cidrs: Option<&[String]>,
	domains: Option<&[String]>,
) -> Result<()> {
	if !valid_name(name) {
		return Err(EngineError::invalid("invalid TAP name"));
	}
	let _ = TapLease::new(name, guest_ip, host_ip, prefix, cidrs)?;
	for domain in domains.unwrap_or(&[]) {
		let domain = domain.trim();
		if domain.is_empty()
			|| domain.len() > 253
			|| !domain
				.bytes()
				.all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-')
			|| domain.starts_with('.')
			|| domain.ends_with('.')
		{
			return Err(EngineError::invalid("invalid egress domain"));
		}
	}
	Ok(())
}

#[cfg(any(test, target_os = "linux"))]
fn valid_name(name: &str) -> bool {
	name.len() == 12
		&& name.starts_with("tv")
		&& name.as_bytes()[2..].iter().all(u8::is_ascii_hexdigit)
}

#[cfg(target_os = "linux")]
fn validate_port(port: u16) -> Result<()> {
	if port == 0 {
		Err(EngineError::invalid("invalid credential gateway port"))
	} else {
		Ok(())
	}
}

#[cfg(any(test, target_os = "linux"))]
fn write_frame<T: Serialize>(stream: &mut impl Write, value: &T) -> Result<()> {
	let payload = serde_json::to_vec(value).map_err(EngineError::from)?;
	let length = u32::try_from(payload.len())
		.map_err(|_| EngineError::invalid("network broker frame too large"))?;
	stream.write_all(&length.to_be_bytes())?;
	stream.write_all(&payload)?;
	Ok(())
}

#[cfg(any(test, target_os = "linux"))]
fn read_frame<R: Read, T: for<'de> Deserialize<'de>>(stream: &mut R) -> Result<T> {
	let mut header = [0_u8; 4];
	stream.read_exact(&mut header)?;
	let length = u32::from_be_bytes(header) as usize;
	if length > 64 * 1024 {
		return Err(EngineError::invalid("network broker frame exceeds 64 KiB"));
	}
	let mut payload = vec![0; length];
	stream.read_exact(&mut payload)?;
	serde_json::from_slice(&payload).map_err(EngineError::from)
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &std::os::unix::net::UnixStream) -> std::io::Result<u32> {
	use std::os::fd::AsRawFd;
	let mut credentials = libc::ucred { pid: 0, uid: 0, gid: 0 };
	let mut length = std::mem::size_of_val(&credentials) as libc::socklen_t;
	// SAFETY: credentials points to writable storage of the advertised size for
	// this socket.
	let result = unsafe {
		libc::getsockopt(
			stream.as_raw_fd(),
			libc::SOL_SOCKET,
			libc::SO_PEERCRED,
			(&raw mut credentials).cast(),
			&mut length,
		)
	};
	if result == -1 {
		Err(std::io::Error::last_os_error())
	} else {
		Ok(credentials.uid)
	}
}

#[cfg(target_os = "linux")]
fn current_uid() -> u32 {
	// SAFETY: geteuid has no preconditions and reads the current process
	// credentials.
	unsafe { libc::geteuid() }
}

#[cfg(target_os = "linux")]
fn authorized_owner_uid(owner_uid: Option<u32>) -> Result<u32> {
	let owner_uid = owner_uid.unwrap_or_else(current_uid);
	if owner_uid == u32::MAX {
		return Err(EngineError::invalid("network broker owner UID is invalid"));
	}
	if owner_uid != current_uid() && current_uid() != 0 {
		return Err(EngineError::engine(
			"network broker needs root to assign a different socket owner",
		));
	}
	Ok(owner_uid)
}

#[cfg(target_os = "linux")]
fn set_socket_owner(socket: &Path, owner_uid: u32) -> Result<()> {
	use std::os::unix::ffi::OsStrExt;
	let path = std::ffi::CString::new(socket.as_os_str().as_bytes())
		.map_err(|_| EngineError::invalid("network broker socket path contains a NUL byte"))?;
	// SAFETY: path is a NUL-terminated pathname and `-1` leaves the socket group
	// unchanged.
	if unsafe { libc::chown(path.as_ptr(), owner_uid, u32::MAX) } == -1 {
		return Err(EngineError::from(std::io::Error::last_os_error()));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn protocol_round_trip_and_bounded_decode() {
		let request = Request::Setup {
			name:                  "tv0123456789".to_owned(),
			guest_ip:              "172.20.0.2".to_owned(),
			host_ip:               "172.20.0.1".to_owned(),
			prefix:                30,
			egress_allow:          Some(vec!["1.1.1.1/32".to_owned()]),
			egress_allow_domains:  Some(vec!["example.com".to_owned()]),
			previous_egress_allow: None,
		};
		let mut bytes = Vec::new();
		write_frame(&mut bytes, &request).unwrap();
		assert_eq!(read_frame::<_, Request>(&mut bytes.as_slice()).unwrap(), request);
		assert!(read_frame::<_, Request>(&mut [0, 1, 0, 1].as_slice()).is_err());
	}

	#[test]
	fn slot_protocol_round_trip_and_identity_validation() {
		let slot = SlotSpec {
			index:    0,
			name:     super::super::slots::slot_tap_name(0),
			guest_ip: "172.20.0.2".to_owned(),
			host_ip:  "172.20.0.1".to_owned(),
			prefix:   30,
		};
		let request = Request::ClaimSlot {
			slot:                 slot.clone(),
			egress_allow:         vec!["203.0.113.0/24".to_owned()],
			egress_allow_domains: vec!["example.com".to_owned()],
			already_custom:       false,
		};
		let mut bytes = Vec::new();
		write_frame(&mut bytes, &request).unwrap();
		assert_eq!(read_frame::<_, Request>(&mut bytes.as_slice()).unwrap(), request);

		assert!(validate_slot(&slot).is_ok());
		// Index/name divergence would let set names drift from the TAP.
		let mut mismatched = slot.clone();
		mismatched.index = 7;
		assert!(validate_slot(&mismatched).is_err());
		let mut bad_ip = slot;
		bad_ip.guest_ip = "10.0.0.2".to_owned();
		assert!(validate_slot(&bad_ip).is_err());
	}

	#[test]
	fn broker_validation_rejects_injection_and_invalid_networks() {
		assert!(validate("tv012345678x", "172.20.0.2", "172.20.0.1", 30, None, None).is_err());
		assert!(validate("tv0123456789", "not-an-ip", "172.20.0.1", 30, None, None).is_err());
		assert!(validate("tv0123456789", "172.20.0.2", "172.20.0.1", 24, None, None).is_err());
		assert!(validate("tv0123456789", "10.0.0.2", "10.0.0.1", 30, None, None).is_err());
		assert!(validate("tv0123456789", "172.20.0.1", "172.20.0.2", 30, None, None).is_err());
		assert!(
			validate(
				"tv0123456789",
				"172.20.0.2",
				"172.20.0.1",
				30,
				Some(&["not-a-cidr".to_owned()]),
				None
			)
			.is_err()
		);
		assert!(
			validate(
				"tv0123456789",
				"172.20.0.2",
				"172.20.0.1",
				30,
				None,
				Some(&["bad domain!".to_owned()])
			)
			.is_err()
		);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn owner_uid_accepts_the_authorized_uid_and_rejects_invalid_uid() {
		assert_eq!(authorized_owner_uid(Some(current_uid())).unwrap(), current_uid());
		assert!(authorized_owner_uid(Some(u32::MAX)).is_err());
	}
}
