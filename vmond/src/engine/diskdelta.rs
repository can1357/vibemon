//! Live-migration disk block delta between a pre-copy base rootfs image and
//! the final paused image.
//!
//! A delta file ([`DISK_DELTA_FILE`]) opens with the 8-byte magic `VMONDSK1`
//! followed by the base and current image lengths as `u64`. The rest is
//! records until EOF: `u64` offset, `u32` data length, then that many raw
//! bytes — all integers little-endian, offsets strictly ascending and
//! non-overlapping, every record within the current length. The producer
//! compares the images in 64 KiB blocks and coalesces adjacent changed
//! blocks, capping a record at 4 MiB of data; the consumer resizes a copy of
//! the base image to the current length and patches the records in place.

use std::{
	fs::{File, OpenOptions},
	io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
	path::Path,
};

use crate::error::{EngineError, Result};

/// Side file carried in a live-migration delta checkpoint next to
/// `vmstate.*`/`memory.*`.
pub const DISK_DELTA_FILE: &str = "rootfs-delta.bin";

/// Magic + format version opening every disk delta file.
const MAGIC: [u8; 8] = *b"VMONDSK1";
/// Comparison granularity of [`write_disk_delta`].
const BLOCK_LEN: usize = 64 * 1024;
/// Hard cap on a single record's data, coalescing included.
const MAX_RECORD_LEN: usize = 4 * 1024 * 1024;

/// Diff `current` against `base` in [`BLOCK_LEN`] blocks and write the block
/// delta to `out`; returns the total data bytes recorded (identical images
/// produce a header-only delta and return 0).
pub fn write_disk_delta(base: &Path, current: &Path, out: &Path) -> Result<u64> {
	let base_file = open_for_delta(base)?;
	let current_file = open_for_delta(current)?;
	let base_len = file_len(&base_file, base)?;
	let current_len = file_len(&current_file, current)?;

	let mut base_reader = BufReader::with_capacity(BLOCK_LEN, base_file);
	let mut current_reader = BufReader::with_capacity(BLOCK_LEN, current_file);
	let out_file = File::create(out).map_err(|err| {
		EngineError::engine(format!("creating disk delta {}: {err}", out.display()))
	})?;
	let mut writer = BufWriter::with_capacity(BLOCK_LEN, out_file);

	let write_err =
		|err: io::Error| EngineError::engine(format!("writing disk delta {}: {err}", out.display()));
	writer.write_all(&MAGIC).map_err(write_err)?;
	writer
		.write_all(&base_len.to_le_bytes())
		.map_err(write_err)?;
	writer
		.write_all(&current_len.to_le_bytes())
		.map_err(write_err)?;

	let mut base_block = vec![0u8; BLOCK_LEN];
	let mut current_block = vec![0u8; BLOCK_LEN];
	let mut pending: Vec<u8> = Vec::new();
	let mut pending_offset = 0u64;
	let mut total = 0u64;
	let mut offset = 0u64;
	while offset < current_len {
		let want = (current_len - offset).min(BLOCK_LEN as u64) as usize;
		read_block(&mut current_reader, &mut current_block[..want], current)?;
		let base_want = base_len.saturating_sub(offset).min(BLOCK_LEN as u64) as usize;
		read_block(&mut base_reader, &mut base_block[..base_want], base)?;

		// A block is changed when the current bytes are not covered
		// byte-for-byte by the base; any region past min(base_len,
		// current_len) counts as changed.
		let changed = want > base_want || current_block[..want] != base_block[..want];
		if changed {
			let contiguous = !pending.is_empty()
				&& pending_offset + pending.len() as u64 == offset
				&& pending.len() + want <= MAX_RECORD_LEN;
			if !contiguous {
				total += flush_record(&mut writer, pending_offset, &mut pending, out)?;
				pending_offset = offset;
			}
			pending.extend_from_slice(&current_block[..want]);
		}
		offset += want as u64;
	}
	total += flush_record(&mut writer, pending_offset, &mut pending, out)?;
	writer.flush().map_err(write_err)?;
	Ok(total)
}

/// Apply a delta produced by [`write_disk_delta`] onto `target` in place.
///
/// `target` must already hold the base image (exactly `base_len` bytes); it
/// is resized to the current length, patched record by record, and fsynced.
pub fn apply_disk_delta(delta: &Path, target: &Path) -> Result<()> {
	let delta_file = File::open(delta).map_err(|err| {
		EngineError::engine(format!("opening disk delta {}: {err}", delta.display()))
	})?;
	let mut reader = BufReader::with_capacity(BLOCK_LEN, delta_file);

	let mut magic = [0u8; 8];
	reader
		.read_exact(&mut magic)
		.map_err(|err| corrupt(delta, format!("reading magic: {err}")))?;
	if magic != MAGIC {
		return Err(corrupt(delta, "bad magic; not a vmond disk delta"));
	}
	let base_len = read_u64(&mut reader, delta, "base length")?;
	let current_len = read_u64(&mut reader, delta, "current length")?;

	let mut target_file = OpenOptions::new().write(true).open(target).map_err(|err| {
		EngineError::engine(format!("opening delta target {}: {err}", target.display()))
	})?;
	let target_len = file_len(&target_file, target)?;
	if target_len != base_len {
		return Err(EngineError::invalid(format!(
			"disk delta {} expects a {base_len}-byte base, but target {} is {target_len} bytes",
			delta.display(),
			target.display()
		)));
	}
	target_file.set_len(current_len).map_err(|err| {
		EngineError::engine(format!("resizing delta target {}: {err}", target.display()))
	})?;

	let mut next_offset = 0u64;
	let mut data: Vec<u8> = Vec::new();
	loop {
		let mut offset_bytes = [0u8; 8];
		let got = read_full(&mut reader, &mut offset_bytes)
			.map_err(|err| corrupt(delta, format!("reading record offset: {err}")))?;
		if got == 0 {
			break;
		}
		if got < offset_bytes.len() {
			return Err(corrupt(delta, "truncated record header"));
		}
		let offset = u64::from_le_bytes(offset_bytes);
		let mut len_bytes = [0u8; 4];
		reader
			.read_exact(&mut len_bytes)
			.map_err(|err| corrupt(delta, format!("truncated record header: {err}")))?;
		let len = u64::from(u32::from_le_bytes(len_bytes));

		if len == 0 {
			return Err(corrupt(delta, format!("zero-length record at offset {offset}")));
		}
		if len > MAX_RECORD_LEN as u64 {
			return Err(corrupt(
				delta,
				format!(
					"record at offset {offset} carries {len} bytes, over the {MAX_RECORD_LEN}-byte cap"
				),
			));
		}
		if offset < next_offset {
			return Err(corrupt(
				delta,
				format!(
					"record offset {offset} overlaps or precedes the previous record (expected >= \
					 {next_offset})"
				),
			));
		}
		let end = offset
			.checked_add(len)
			.filter(|end| *end <= current_len)
			.ok_or_else(|| {
				corrupt(
					delta,
					format!(
						"record at offset {offset} ({len} bytes) ends past the {current_len}-byte image"
					),
				)
			})?;

		data.resize(len as usize, 0);
		reader.read_exact(&mut data).map_err(|err| {
			corrupt(delta, format!("truncated record data at offset {offset}: {err}"))
		})?;
		target_file.seek(SeekFrom::Start(offset)).map_err(|err| {
			EngineError::engine(format!("seeking delta target {}: {err}", target.display()))
		})?;
		target_file.write_all(&data).map_err(|err| {
			EngineError::engine(format!("patching delta target {}: {err}", target.display()))
		})?;
		next_offset = end;
	}

	target_file.sync_all().map_err(|err| {
		EngineError::engine(format!("syncing delta target {}: {err}", target.display()))
	})
}

/// Open an image for delta production with the path in the error.
fn open_for_delta(path: &Path) -> Result<File> {
	File::open(path).map_err(|err| {
		EngineError::engine(format!("opening {} for disk delta: {err}", path.display()))
	})
}

/// Length of an open file with the path in the error.
fn file_len(file: &File, path: &Path) -> Result<u64> {
	let meta = file.metadata().map_err(|err| {
		EngineError::engine(format!("sizing {} for disk delta: {err}", path.display()))
	})?;
	Ok(meta.len())
}

/// Fill `buf` exactly from a sequentially-read image.
fn read_block(reader: &mut impl Read, buf: &mut [u8], path: &Path) -> Result<()> {
	reader.read_exact(buf).map_err(|err| {
		EngineError::engine(format!("reading {} for disk delta: {err}", path.display()))
	})
}

/// Write one pending record and clear the buffer; empty pending is a no-op.
/// Returns the data bytes written.
fn flush_record(
	writer: &mut impl Write,
	offset: u64,
	data: &mut Vec<u8>,
	out: &Path,
) -> Result<u64> {
	if data.is_empty() {
		return Ok(0);
	}
	let write_err =
		|err: io::Error| EngineError::engine(format!("writing disk delta {}: {err}", out.display()));
	writer.write_all(&offset.to_le_bytes()).map_err(write_err)?;
	writer
		.write_all(&(data.len() as u32).to_le_bytes())
		.map_err(write_err)?;
	writer.write_all(data).map_err(write_err)?;
	let written = data.len() as u64;
	data.clear();
	Ok(written)
}

/// Fill `buf` tolerating short reads; returns bytes read (0 at clean EOF).
fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
	let mut filled = 0;
	while filled < buf.len() {
		match reader.read(&mut buf[filled..]) {
			Ok(0) => break,
			Ok(n) => filled += n,
			Err(err) if err.kind() == io::ErrorKind::Interrupted => {},
			Err(err) => return Err(err),
		}
	}
	Ok(filled)
}

/// Little-endian `u64` header field with the delta path in the error.
fn read_u64(reader: &mut impl Read, delta: &Path, what: &str) -> Result<u64> {
	let mut bytes = [0u8; 8];
	reader
		.read_exact(&mut bytes)
		.map_err(|err| corrupt(delta, format!("reading {what}: {err}")))?;
	Ok(u64::from_le_bytes(bytes))
}

/// Invalid-delta error with the delta path prefixed.
fn corrupt(delta: &Path, detail: impl std::fmt::Display) -> EngineError {
	EngineError::invalid(format!("disk delta {}: {detail}", delta.display()))
}

#[cfg(test)]
mod tests {
	use std::{fs, path::PathBuf};

	use tempfile::TempDir;

	use super::*;

	type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

	const MIB: usize = 1024 * 1024;

	/// Deterministic xorshift64 byte stream.
	fn pseudo_bytes(seed: u64, len: usize) -> Vec<u8> {
		let mut state = seed | 1;
		let mut out = Vec::with_capacity(len + 8);
		while out.len() < len {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			out.extend_from_slice(&state.to_le_bytes());
		}
		out.truncate(len);
		out
	}

	/// On-disk base/current pair plus a target seeded with a copy of base.
	struct Fixture {
		base:    PathBuf,
		current: PathBuf,
		delta:   PathBuf,
		target:  PathBuf,
		_dir:    TempDir,
	}

	fn fixture(base: &[u8], current: &[u8]) -> TestResult<Fixture> {
		let dir = tempfile::tempdir()?;
		let fx = Fixture {
			base:    dir.path().join("base.img"),
			current: dir.path().join("current.img"),
			delta:   dir.path().join(DISK_DELTA_FILE),
			target:  dir.path().join("target.img"),
			_dir:    dir,
		};
		fs::write(&fx.base, base)?;
		fs::write(&fx.current, current)?;
		fs::write(&fx.target, base)?;
		Ok(fx)
	}

	/// Write the delta, apply it onto the base copy, and require the target
	/// to match `current` exactly; returns the recorded data bytes.
	fn round_trip(fx: &Fixture) -> TestResult<u64> {
		let written = write_disk_delta(&fx.base, &fx.current, &fx.delta)?;
		apply_disk_delta(&fx.delta, &fx.target)?;
		let got = fs::read(&fx.target)?;
		let want = fs::read(&fx.current)?;
		assert_eq!(got.len(), want.len(), "target length after apply");
		if got != want {
			let at = got.iter().zip(&want).position(|(a, b)| a != b);
			panic!("target diverges from current at byte {at:?}");
		}
		Ok(written)
	}

	/// Header lengths and `(offset, len)` per record, with framing checks.
	fn parse_delta(path: &Path) -> (u64, u64, Vec<(u64, u64)>) {
		let bytes = fs::read(path).expect("read delta");
		assert_eq!(&bytes[..8], MAGIC.as_slice(), "delta magic");
		let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("u64 field"));
		let base_len = u64_at(8);
		let current_len = u64_at(16);
		let mut records = Vec::new();
		let mut at = 24;
		while at < bytes.len() {
			let offset = u64_at(at);
			let len =
				u64::from(u32::from_le_bytes(bytes[at + 8..at + 12].try_into().expect("u32 field")));
			records.push((offset, len));
			at += 12 + len as usize;
		}
		assert_eq!(at, bytes.len(), "trailing bytes in delta");
		(base_len, current_len, records)
	}

	/// Hand-build a delta with the given records.
	fn craft_delta(path: &Path, base_len: u64, current_len: u64, records: &[(u64, &[u8])]) {
		let mut buf = Vec::new();
		buf.extend_from_slice(&MAGIC);
		buf.extend_from_slice(&base_len.to_le_bytes());
		buf.extend_from_slice(&current_len.to_le_bytes());
		for (offset, data) in records {
			buf.extend_from_slice(&offset.to_le_bytes());
			buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
			buf.extend_from_slice(data);
		}
		fs::write(path, buf).expect("write crafted delta");
	}

	#[test]
	fn scattered_changes_round_trip() -> TestResult {
		let base = pseudo_bytes(1, 12 * MIB + 12_345);
		let mut current = base.clone();
		current[..64].copy_from_slice(&pseudo_bytes(2, 64));
		let run = pseudo_bytes(3, 5 * MIB + 999);
		current[2 * MIB..2 * MIB + run.len()].copy_from_slice(&run);
		let tail = current.len() - 5;
		current[tail..].fill(0xa5);

		let fx = fixture(&base, &current)?;
		let written = round_trip(&fx)?;

		let (base_len, current_len, records) = parse_delta(&fx.delta);
		assert_eq!(base_len, base.len() as u64);
		assert_eq!(current_len, current.len() as u64);
		// Block 0, an 81-block middle run split at the 4 MiB cap, and the
		// short tail block.
		let expected = vec![
			(0, BLOCK_LEN as u64),
			(2 * MIB as u64, MAX_RECORD_LEN as u64),
			((2 * MIB + MAX_RECORD_LEN) as u64, 17 * BLOCK_LEN as u64),
			(192 * BLOCK_LEN as u64, 12_345),
		];
		assert_eq!(records, expected);
		assert_eq!(written, records.iter().map(|(_, len)| len).sum::<u64>());
		Ok(())
	}

	#[test]
	fn identical_images_write_header_only_delta() -> TestResult {
		let data = pseudo_bytes(5, MIB + 7);
		let fx = fixture(&data, &data)?;
		assert_eq!(write_disk_delta(&fx.base, &fx.current, &fx.delta)?, 0);
		assert_eq!(fs::metadata(&fx.delta)?.len(), 24, "header-only delta");
		apply_disk_delta(&fx.delta, &fx.target)?;
		assert_eq!(fs::read(&fx.target)?, data, "apply must be a no-op");
		Ok(())
	}

	#[test]
	fn grow_round_trip() -> TestResult {
		let base = pseudo_bytes(7, MIB + 100);
		let mut current = base.clone();
		current[MIB / 2..MIB / 2 + 16].fill(0x11);
		current.extend_from_slice(&pseudo_bytes(8, 300 * 1024));
		let fx = fixture(&base, &current)?;
		let written = round_trip(&fx)?;
		assert!(written > 300 * 1024, "grown data must be recorded, wrote {written}");
		Ok(())
	}

	#[test]
	fn shrink_round_trip() -> TestResult {
		let base = pseudo_bytes(9, 3 * MIB / 2 + 33);
		let mut current = base[..900 * 1024].to_vec();
		current[100 * 1024..100 * 1024 + 20].fill(0x22);
		let fx = fixture(&base, &current)?;
		round_trip(&fx)?;
		assert_eq!(fs::metadata(&fx.target)?.len(), current.len() as u64);
		Ok(())
	}

	#[test]
	fn wrong_length_target_errors() -> TestResult {
		let base = pseudo_bytes(11, 256 * 1024);
		let mut current = base.clone();
		current[0] ^= 0xff;
		let fx = fixture(&base, &current)?;
		write_disk_delta(&fx.base, &fx.current, &fx.delta)?;
		fs::write(&fx.target, &base[..base.len() - 1])?;
		let err = apply_disk_delta(&fx.delta, &fx.target)
			.unwrap_err()
			.to_string();
		assert!(err.contains(&fx.target.display().to_string()), "missing target path: {err}");
		assert!(err.contains("262143") && err.contains("262144"), "missing lengths: {err}");
		Ok(())
	}

	#[test]
	fn corrupt_magic_errors() -> TestResult {
		let base = pseudo_bytes(13, 128 * 1024);
		let mut current = base.clone();
		current[0] ^= 0xff;
		let fx = fixture(&base, &current)?;
		write_disk_delta(&fx.base, &fx.current, &fx.delta)?;
		let mut bytes = fs::read(&fx.delta)?;
		bytes[0] = b'X';
		fs::write(&fx.delta, bytes)?;
		let err = apply_disk_delta(&fx.delta, &fx.target)
			.unwrap_err()
			.to_string();
		assert!(err.contains("magic"), "unexpected error: {err}");
		assert_eq!(fs::read(&fx.target)?, base, "target must stay untouched");
		Ok(())
	}

	#[test]
	fn descending_offsets_error() -> TestResult {
		let base = pseudo_bytes(15, 256 * 1024);
		let fx = fixture(&base, &base)?;
		let len = base.len() as u64;
		craft_delta(&fx.delta, len, len, &[(BLOCK_LEN as u64, &[1; 16]), (0, &[2; 16])]);
		let err = apply_disk_delta(&fx.delta, &fx.target)
			.unwrap_err()
			.to_string();
		assert!(err.contains("overlaps or precedes"), "unexpected error: {err}");
		Ok(())
	}

	#[test]
	fn malformed_records_error() -> TestResult {
		let base = pseudo_bytes(17, 128 * 1024);
		let fx = fixture(&base, &base)?;
		let len = base.len() as u64;

		craft_delta(&fx.delta, len, len, &[(0, &[])]);
		let err = apply_disk_delta(&fx.delta, &fx.target)
			.unwrap_err()
			.to_string();
		assert!(err.contains("zero-length"), "unexpected error: {err}");

		craft_delta(&fx.delta, len, len, &[(len - 8, &[0; 16])]);
		let err = apply_disk_delta(&fx.delta, &fx.target)
			.unwrap_err()
			.to_string();
		assert!(err.contains("ends past"), "unexpected error: {err}");

		let mut oversize = Vec::new();
		oversize.extend_from_slice(&MAGIC);
		oversize.extend_from_slice(&len.to_le_bytes());
		oversize.extend_from_slice(&len.to_le_bytes());
		oversize.extend_from_slice(&0u64.to_le_bytes());
		oversize.extend_from_slice(&(MAX_RECORD_LEN as u32 + 1).to_le_bytes());
		fs::write(&fx.delta, oversize)?;
		let err = apply_disk_delta(&fx.delta, &fx.target)
			.unwrap_err()
			.to_string();
		assert!(err.contains("cap"), "unexpected error: {err}");
		Ok(())
	}
}
