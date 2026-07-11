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
	thread,
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
/// Cap on compare worker threads; past this disk bandwidth, not CPU, is the
/// bottleneck.
const MAX_COMPARE_WORKERS: u64 = 8;
/// Smallest per-worker compare chunk; images below this stay on the
/// single-threaded path where thread fan-out cannot pay for itself.
const MIN_COMPARE_CHUNK_LEN: u64 = 64 * 1024 * 1024;
/// Read slab of the compare phase: whole blocks per read to cut syscalls;
/// the compare grid itself stays [`BLOCK_LEN`].
const COMPARE_BUF_LEN: usize = 16 * BLOCK_LEN;

/// Diff `current` against `base` in [`BLOCK_LEN`] blocks and write the block
/// delta to `out`; returns the total data bytes recorded (identical images
/// produce a header-only delta and return 0).
///
/// The compare runs inside the live-migration blackout, so it fans out over
/// up to [`MAX_COMPARE_WORKERS`] threads; the emitted delta stays
/// byte-identical to a sequential scan.
pub fn write_disk_delta(base: &Path, current: &Path, out: &Path) -> Result<u64> {
	write_disk_delta_chunked(base, current, out, None)
}

/// [`write_disk_delta`] with an explicit compare chunk length; `None` sizes
/// chunks via [`compare_chunk_len`]. Tests pass small chunks to exercise the
/// parallel merge without gigabyte images.
fn write_disk_delta_chunked(
	base: &Path,
	current: &Path,
	out: &Path,
	chunk_len: Option<u64>,
) -> Result<u64> {
	let base_file = open_for_delta(base)?;
	let base_len = file_len(&base_file, base)?;
	drop(base_file);
	let mut current_file = open_for_delta(current)?;
	let current_len = file_len(&current_file, current)?;

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

	// Only blocks fully covered by the base need a content compare; from the
	// first block not fully covered onward (a shorter base's ragged last
	// block plus any grown tail) every byte counts as changed.
	let compare_end = if current_len <= base_len {
		current_len
	} else {
		base_len - base_len % BLOCK_LEN as u64
	};
	let chunk_len = chunk_len.unwrap_or_else(|| compare_chunk_len(compare_end));
	let mut runs = compare_chunks(base, current, compare_end, chunk_len)?;
	if current_len > compare_end {
		push_run(&mut runs, compare_end, current_len - compare_end);
	}

	// Emit the runs as records, splitting at the cap exactly like the
	// sequential scan did and re-reading record data from `current`.
	let read_err = |err: io::Error| {
		EngineError::engine(format!("reading {} for disk delta: {err}", current.display()))
	};
	let mut data: Vec<u8> = Vec::new();
	let mut total = 0u64;
	for (run_offset, run_len) in runs {
		let mut offset = run_offset;
		let mut remaining = run_len;
		while remaining > 0 {
			let len = remaining.min(MAX_RECORD_LEN as u64);
			data.resize(len as usize, 0);
			current_file
				.seek(SeekFrom::Start(offset))
				.map_err(read_err)?;
			current_file.read_exact(&mut data).map_err(read_err)?;
			total += flush_record(&mut writer, offset, &mut data, out)?;
			offset += len;
			remaining -= len;
		}
	}
	writer.flush().map_err(write_err)?;
	Ok(total)
}

/// Default compare chunk length: the compared range split over up to
/// [`MAX_COMPARE_WORKERS`] threads in whole blocks, floored at
/// [`MIN_COMPARE_CHUNK_LEN`] so small images stay single-threaded.
fn compare_chunk_len(compare_end: u64) -> u64 {
	let workers = thread::available_parallelism()
		.map_or(1, |count| count.get() as u64)
		.min(MAX_COMPARE_WORKERS);
	compare_end
		.div_ceil(workers)
		.next_multiple_of(BLOCK_LEN as u64)
		.max(MIN_COMPARE_CHUNK_LEN)
}

/// Changed-block runs over `[0, compare_end)`, one worker thread per
/// `chunk_len`-sized chunk (inline for a single chunk), joined in chunk
/// order and coalesced across chunk boundaries exactly like a sequential
/// scan.
fn compare_chunks(
	base: &Path,
	current: &Path,
	compare_end: u64,
	chunk_len: u64,
) -> Result<Vec<(u64, u64)>> {
	debug_assert!(
		chunk_len > 0 && chunk_len.is_multiple_of(BLOCK_LEN as u64),
		"compare chunks must be whole blocks"
	);
	let chunk_count = compare_end.div_ceil(chunk_len);
	if chunk_count <= 1 {
		return diff_chunk(base, current, 0, compare_end);
	}
	let per_chunk = thread::scope(|scope| {
		let workers: Vec<_> = (0..chunk_count)
			.map(|index| {
				let start = index * chunk_len;
				let end = compare_end.min(start + chunk_len);
				scope.spawn(move || diff_chunk(base, current, start, end))
			})
			.collect();
		workers
			.into_iter()
			.map(thread::ScopedJoinHandle::join)
			.collect::<Vec<_>>()
	});
	let mut runs = Vec::new();
	for joined in per_chunk {
		let chunk_runs =
			joined.map_err(|_| EngineError::engine("disk delta compare worker panicked"))??;
		for (offset, len) in chunk_runs {
			push_run(&mut runs, offset, len);
		}
	}
	Ok(runs)
}

/// Changed-block runs (offset plus length, adjacent changed blocks coalesced
/// without a cap) for the compare chunk `[start, end)`; `start` must be
/// block-aligned and both images must fully cover the range.
fn diff_chunk(base: &Path, current: &Path, start: u64, end: u64) -> Result<Vec<(u64, u64)>> {
	if start >= end {
		return Ok(Vec::new());
	}
	let mut base_file = chunk_file(base, start)?;
	let mut current_file = chunk_file(current, start)?;
	let mut base_buf = vec![0u8; COMPARE_BUF_LEN];
	let mut current_buf = vec![0u8; COMPARE_BUF_LEN];
	let mut runs = Vec::new();
	let mut offset = start;
	while offset < end {
		let want = (end - offset).min(COMPARE_BUF_LEN as u64) as usize;
		read_block(&mut current_file, &mut current_buf[..want], current)?;
		read_block(&mut base_file, &mut base_buf[..want], base)?;
		for at in (0..want).step_by(BLOCK_LEN) {
			let len = want.min(at + BLOCK_LEN) - at;
			if current_buf[at..at + len] != base_buf[at..at + len] {
				push_run(&mut runs, offset + at as u64, len as u64);
			}
		}
		offset += want as u64;
	}
	Ok(runs)
}

/// Image file positioned at `start` for a compare chunk.
fn chunk_file(path: &Path, start: u64) -> Result<File> {
	let mut file = open_for_delta(path)?;
	file.seek(SeekFrom::Start(start)).map_err(|err| {
		EngineError::engine(format!("seeking {} for disk delta: {err}", path.display()))
	})?;
	Ok(file)
}

/// Append a changed run, merging it into the previous one when contiguous.
fn push_run(runs: &mut Vec<(u64, u64)>, offset: u64, len: u64) {
	if len == 0 {
		return;
	}
	match runs.last_mut() {
		Some((last_offset, last_len)) if *last_offset + *last_len == offset => *last_len += len,
		_ => runs.push((offset, len)),
	}
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

	/// Byte equality with a first-divergence index instead of a byte dump.
	fn assert_same_bytes(got: &[u8], want: &[u8], what: &str) {
		assert_eq!(got.len(), want.len(), "{what}: length");
		if got != want {
			let at = got.iter().zip(want).position(|(a, b)| a != b);
			panic!("{what}: bytes diverge at {at:?}");
		}
	}

	/// In-memory sequential reference of the block diff: `(offset, data)`
	/// records with the same coalescing and [`MAX_RECORD_LEN`] cap as
	/// [`write_disk_delta`].
	fn reference_records(base: &[u8], current: &[u8]) -> Vec<(u64, Vec<u8>)> {
		let mut records: Vec<(u64, Vec<u8>)> = Vec::new();
		let mut offset = 0usize;
		while offset < current.len() {
			let want = (current.len() - offset).min(BLOCK_LEN);
			let block = &current[offset..offset + want];
			let changed = base.len() < offset + want || block != &base[offset..offset + want];
			if changed {
				match records.last_mut() {
					Some((record_offset, data))
						if *record_offset as usize + data.len() == offset
							&& data.len() + want <= MAX_RECORD_LEN =>
					{
						data.extend_from_slice(block);
					},
					_ => records.push((offset as u64, block.to_vec())),
				}
			}
			offset += want;
		}
		records
	}

	/// Overwrite the whole-block range with a deterministic pattern distinct
	/// from the base.
	fn paint_blocks(image: &mut [u8], blocks: std::ops::Range<usize>, seed: u64) {
		let start = blocks.start * BLOCK_LEN;
		let end = (blocks.end * BLOCK_LEN).min(image.len());
		image[start..end].copy_from_slice(&pseudo_bytes(seed, end - start));
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

	#[test]
	fn chunked_compare_matches_sequential_bytes() -> TestResult {
		// Four-block compare chunks push the ~10 MiB image across dozens of
		// worker chunks without allocating anything huge.
		const CHUNK_LEN: u64 = 4 * BLOCK_LEN as u64;

		let base = pseudo_bytes(19, 9 * MIB + 4321);
		let mut current = base.clone();
		// Changed runs placed against the 4-block chunk grid: crossing a
		// boundary mid-run, filling a chunk exactly, starting on a boundary,
		// a 5 MiB run spanning many chunks and the 4 MiB record cap, and the
		// last compared blocks merging into the ragged-then-grown tail.
		paint_blocks(&mut current, 2..6, 20);
		paint_blocks(&mut current, 8..12, 21);
		paint_blocks(&mut current, 16..17, 22);
		paint_blocks(&mut current, 31..111, 23);
		paint_blocks(&mut current, 142..145, 24);
		current.extend_from_slice(&pseudo_bytes(25, 700_000));

		let fx = fixture(&base, &current)?;
		let written = write_disk_delta_chunked(&fx.base, &fx.current, &fx.delta, Some(CHUNK_LEN))?;
		let chunked = fs::read(&fx.delta)?;

		// Byte-identical to an independently computed sequential diff...
		let records = reference_records(&base, &current);
		let borrowed: Vec<(u64, &[u8])> = records
			.iter()
			.map(|(offset, data)| (*offset, data.as_slice()))
			.collect();
		let reference = fx.delta.with_extension("reference");
		craft_delta(&reference, base.len() as u64, current.len() as u64, &borrowed);
		assert_same_bytes(&chunked, &fs::read(&reference)?, "chunked delta vs reference");

		// ...and to this module's own single-chunk sequential path.
		let single = fx.delta.with_extension("single");
		write_disk_delta_chunked(&fx.base, &fx.current, &single, Some(1 << 40))?;
		assert_same_bytes(&chunked, &fs::read(&single)?, "chunked delta vs single chunk");

		let (_, _, parsed) = parse_delta(&fx.delta);
		let expected = vec![
			(2 * BLOCK_LEN as u64, 4 * BLOCK_LEN as u64),
			(8 * BLOCK_LEN as u64, 4 * BLOCK_LEN as u64),
			(16 * BLOCK_LEN as u64, BLOCK_LEN as u64),
			(31 * BLOCK_LEN as u64, MAX_RECORD_LEN as u64),
			(31 * BLOCK_LEN as u64 + MAX_RECORD_LEN as u64, 16 * BLOCK_LEN as u64),
			(142 * BLOCK_LEN as u64, current.len() as u64 - 142 * BLOCK_LEN as u64),
		];
		assert_eq!(parsed, expected);
		assert_eq!(written, parsed.iter().map(|(_, len)| len).sum::<u64>());

		apply_disk_delta(&fx.delta, &fx.target)?;
		assert_same_bytes(&fs::read(&fx.target)?, &current, "applied target vs current");
		Ok(())
	}
}
