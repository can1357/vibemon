//! Checkpoint bundle: vmon's own sparse-aware streaming archive.
//!
//! Replaces external `tar` for peer template transfers. A bundle is a single
//! multithreaded-zstd stream of directory/file/symlink entries; file bytes
//! travel as `(offset, len, data)` segments enumerated with
//! `SEEK_DATA`/`SEEK_HOLE`, so holes (untouched guest RAM in sparse memory
//! dumps) cost nothing to create, transfer, or extract. Writer and reader are
//! generic over `io::Write`/`io::Read`, and [`ChannelWriter`]/[`ChannelReader`]
//! bridge them onto tokio channels so compression overlaps the HTTP transfer
//! in both directions — no temp archive ever touches disk.
//!
//! Layout (all integers little-endian, inside the zstd stream):
//! `"VMONBNDL"` magic, `u32` version, then entries — `u8` kind, `u16` path
//! length, path — where directories carry a `u32` mode, symlinks a `u16`
//! target length + target, and files a `u32` mode, `u64` logical length, and
//! data segments (`u64` offset, `u64` len, bytes) closed by a `u64::MAX`
//! offset. A kind of 0 ends the bundle. Paths are `/`-separated and relative
//! to the bundle root's parent (the root directory itself is the first
//! entry), mirroring what the previous tar archives contained.

use std::{
	cmp::Reverse,
	collections::HashSet,
	fs::{self, File, OpenOptions},
	io::{self, BufReader, Read, Write},
	os::unix::{
		fs::{FileExt, OpenOptionsExt, PermissionsExt},
		io::AsRawFd,
	},
	path::{Path, PathBuf},
	thread,
};

use bytes::{Buf, Bytes};

use crate::error::{EngineError, Result};

const MAGIC: &[u8; 8] = b"VMONBNDL";
const VERSION: u32 = 1;

const KIND_END: u8 = 0;
const KIND_DIR: u8 = 1;
const KIND_FILE: u8 = 2;
const KIND_SYMLINK: u8 = 3;

const SEGMENT_TERMINATOR: u64 = u64::MAX;
const ZSTD_LEVEL: i32 = 3;
const COPY_BUF: usize = 1 << 20;
/// Sanity bound on decoded path/target lengths.
const MAX_PATH: usize = 4096;
/// Maximum entries accepted from one untrusted portable/replica bundle.
const MAX_BUNDLE_ENTRIES: u64 = 100_000;
/// Maximum sparse data segments accepted from one untrusted portable/replica
/// bundle.
const MAX_BUNDLE_SEGMENTS: u64 = 100_000;
/// Maximum logical length of one restored sparse file (1 TiB).
const MAX_BUNDLE_FILE_LOGICAL_BYTES: u64 = 1 << 40;
/// Maximum cumulative logical file length restored from one bundle (1 TiB),
/// bounding post-extract canonical scans while allowing sparse disks far larger
/// than the VMM's 64 GiB RAM cap.
const MAX_BUNDLE_LOGICAL_BYTES: u64 = 1 << 40;

#[derive(Default)]
struct BundleBudget {
	entries:               u64,
	segments:              u64,
	logical_bytes:         u64,
	decoded_segment_bytes: u64,
}

impl BundleBudget {
	fn add_entry(&mut self) -> Result<()> {
		charge_budget(&mut self.entries, 1, MAX_BUNDLE_ENTRIES, "bundle entry")
	}

	fn add_file(&mut self, logical: u64) -> Result<()> {
		if logical > MAX_BUNDLE_FILE_LOGICAL_BYTES {
			return Err(EngineError::invalid("bundle file logical length exceeds limit"));
		}
		charge_budget(
			&mut self.logical_bytes,
			logical,
			MAX_BUNDLE_LOGICAL_BYTES,
			"bundle logical length",
		)
	}

	fn add_segment(&mut self, len: u64) -> Result<()> {
		charge_budget(&mut self.segments, 1, MAX_BUNDLE_SEGMENTS, "bundle data segment")?;
		charge_budget(
			&mut self.decoded_segment_bytes,
			len,
			MAX_BUNDLE_LOGICAL_BYTES,
			"bundle decoded segment bytes",
		)
	}
}

/// Bundle `root` (the directory itself, like `tar -C parent root`) into
/// `out`, including only entries the filter accepts.
pub fn write_bundle(root: &Path, out: impl Write, include: &dyn Fn(&Path) -> bool) -> Result<()> {
	let root_name = root
		.file_name()
		.and_then(|name| name.to_str())
		.ok_or_else(|| EngineError::invalid("bundle root has no utf-8 name"))?;
	write_bundle_named(root, root_name, out, include)
}

/// Bundle `root` using a caller-provided archive root name.
///
/// This is for content-addressed formats whose on-disk materialization name
/// must not become part of their immutable identity.
pub fn write_bundle_named(
	root: &Path,
	root_name: &str,
	out: impl Write,
	include: &dyn Fn(&Path) -> bool,
) -> Result<()> {
	if root_name.is_empty() || root_name.contains('/') || root_name.contains('\\') {
		return Err(EngineError::invalid("bundle root name is invalid"));
	}
	let mut encoder = zstd::stream::write::Encoder::new(out, ZSTD_LEVEL)
		.map_err(|err| EngineError::engine(format!("zstd encoder init failed: {err}")))?;
	let workers = thread::available_parallelism().map_or(1, |n| n.get().min(8)) as u32;
	encoder
		.multithread(workers)
		.map_err(|err| EngineError::engine(format!("zstd multithread init failed: {err}")))?;
	encoder.write_all(MAGIC)?;
	encoder.write_all(&VERSION.to_le_bytes())?;
	let mut budget = BundleBudget::default();
	write_tree(&mut encoder, root, root_name, include, &mut budget)?;
	encoder.write_all(&[KIND_END])?;
	let mut out = encoder
		.finish()
		.map_err(|err| EngineError::engine(format!("finishing bundle: {err}")))?;
	out.flush()?;
	Ok(())
}

/// Extract a bundle into `dest_root`, which is created or must already be
/// empty, refusing absolute or traversal paths.
pub fn read_bundle(source: impl Read, dest_root: &Path) -> Result<()> {
	prepare_destination(dest_root)?;
	let mut reader = zstd::stream::read::Decoder::new(BufReader::new(source))
		.map_err(|err| EngineError::engine(format!("zstd decoder init failed: {err}")))?;
	let mut magic = [0u8; 8];
	reader.read_exact(&mut magic)?;
	if &magic != MAGIC {
		return Err(EngineError::invalid("not a vmon bundle"));
	}
	let version = read_u32(&mut reader)?;
	if version != VERSION {
		return Err(EngineError::invalid(format!("unsupported bundle version {version}")));
	}
	let mut buf = vec![0u8; COPY_BUF];
	let mut directories: Vec<(PathBuf, u32)> = Vec::new();
	let mut declared_directories = HashSet::new();
	let mut declared_paths = HashSet::new();
	let mut entries = 0;
	let mut segments = 0;
	let mut logical_bytes = 0;
	let mut decoded_segment_bytes = 0;
	loop {
		let mut kind = [0u8; 1];
		reader.read_exact(&mut kind)?;
		if kind[0] == KIND_END {
			directories.sort_by_key(|(path, _)| Reverse(path.components().count()));
			for (path, mode) in directories {
				fs::set_permissions(&path, fs::Permissions::from_mode(mode & 0o7777))?;
			}
			return Ok(());
		}
		charge_budget(&mut entries, 1, MAX_BUNDLE_ENTRIES, "bundle entry")?;
		match kind[0] {
			KIND_DIR => {
				let (rel, path) =
					read_entry_path(&mut reader, dest_root, &mut declared_paths, &declared_directories)?;
				fs::create_dir(&path)?;
				if !fs::symlink_metadata(&path)?.file_type().is_dir() {
					return Err(EngineError::invalid(format!(
						"bundle directory {rel:?} is not a directory"
					)));
				}
				let mode = read_u32(&mut reader)?;
				declared_directories.insert(rel);
				directories.push((path, mode));
			},
			KIND_FILE => {
				let (_rel, path) =
					read_entry_path(&mut reader, dest_root, &mut declared_paths, &declared_directories)?;
				let mode = read_u32(&mut reader)?;
				let logical = read_u64(&mut reader)?;
				if logical > MAX_BUNDLE_FILE_LOGICAL_BYTES {
					return Err(EngineError::invalid("bundle file logical length exceeds limit"));
				}
				charge_budget(
					&mut logical_bytes,
					logical,
					MAX_BUNDLE_LOGICAL_BYTES,
					"bundle logical length",
				)?;
				let out = create_output_file(&path)?;
				out.set_len(logical)?;
				let mut decoded_file_bytes = 0;
				let mut previous_end = None;
				loop {
					let offset = read_u64(&mut reader)?;
					if offset == SEGMENT_TERMINATOR {
						break;
					}
					charge_budget(&mut segments, 1, MAX_BUNDLE_SEGMENTS, "bundle data segment")?;
					let len = read_u64(&mut reader)?;
					let end = offset
						.checked_add(len)
						.filter(|&end| end <= logical)
						.ok_or_else(|| EngineError::invalid("bundle segment out of bounds"))?;
					if let Some(previous_end) = previous_end
						&& offset < previous_end
					{
						return Err(EngineError::invalid("bundle segments overlap or are out of order"));
					}
					charge_budget(
						&mut decoded_file_bytes,
						len,
						logical,
						"bundle file decoded segment bytes",
					)?;
					charge_budget(
						&mut decoded_segment_bytes,
						len,
						MAX_BUNDLE_LOGICAL_BYTES,
						"bundle decoded segment bytes",
					)?;
					previous_end = Some(end);
					let mut at = offset;
					while at < end {
						let take =
							usize::try_from((end - at).min(COPY_BUF as u64)).expect("bounded by COPY_BUF");
						reader.read_exact(&mut buf[..take])?;
						out.write_all_at(&buf[..take], at)?;
						at += take as u64;
					}
				}
				out.set_permissions(fs::Permissions::from_mode(mode & 0o7777))?;
			},
			KIND_SYMLINK => {
				let (_rel, path) =
					read_entry_path(&mut reader, dest_root, &mut declared_paths, &declared_directories)?;
				let target = read_path(&mut reader)?;
				std::os::unix::fs::symlink(&target, &path)?;
			},
			other => return Err(EngineError::invalid(format!("unknown bundle entry kind {other}"))),
		}
	}
}

/// `io::Write` bridge feeding 1 MiB chunks into a bounded tokio channel; the
/// HTTP body consumer provides backpressure.
pub struct ChannelWriter {
	tx:  tokio::sync::mpsc::Sender<io::Result<Bytes>>,
	buf: Vec<u8>,
}

impl ChannelWriter {
	pub fn new(tx: tokio::sync::mpsc::Sender<io::Result<Bytes>>) -> Self {
		Self { tx, buf: Vec::with_capacity(COPY_BUF) }
	}

	fn send_buf(&mut self) -> io::Result<()> {
		if self.buf.is_empty() {
			return Ok(());
		}
		let chunk = Bytes::from(std::mem::replace(&mut self.buf, Vec::with_capacity(COPY_BUF)));
		self
			.tx
			.blocking_send(Ok(chunk))
			.map_err(|_| io::Error::other("bundle consumer went away"))
	}

	/// Flush the tail on success, or surface the producer's error to the
	/// consumer so the transfer visibly fails instead of truncating silently.
	pub fn finish(mut self, result: Result<()>) {
		match result {
			Ok(()) => {
				let _ = self.send_buf();
			},
			Err(err) => {
				let _ = self.tx.blocking_send(Err(io::Error::other(err.message)));
			},
		}
	}
}

impl Write for ChannelWriter {
	fn write(&mut self, data: &[u8]) -> io::Result<usize> {
		self.buf.extend_from_slice(data);
		if self.buf.len() >= COPY_BUF {
			self.send_buf()?;
		}
		Ok(data.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		self.send_buf()
	}
}

/// `io::Read` bridge draining a tokio channel fed by the HTTP response body.
pub struct ChannelReader {
	rx:      tokio::sync::mpsc::Receiver<io::Result<Bytes>>,
	current: Bytes,
}

impl ChannelReader {
	pub const fn new(rx: tokio::sync::mpsc::Receiver<io::Result<Bytes>>) -> Self {
		Self { rx, current: Bytes::new() }
	}
}

impl Read for ChannelReader {
	fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
		while self.current.is_empty() {
			match self.rx.blocking_recv() {
				None => return Ok(0),
				Some(Ok(chunk)) => self.current = chunk,
				Some(Err(err)) => return Err(err),
			}
		}
		let take = out.len().min(self.current.len());
		out[..take].copy_from_slice(&self.current[..take]);
		self.current.advance(take);
		Ok(take)
	}
}

fn write_tree(
	writer: &mut impl Write,
	dir: &Path,
	rel: &str,
	include: &dyn Fn(&Path) -> bool,
	budget: &mut BundleBudget,
) -> Result<()> {
	budget.add_entry()?;
	write_entry_header(writer, KIND_DIR, rel)?;
	writer.write_all(&mode_of(dir)?.to_le_bytes())?;
	let mut entries: Vec<_> = fs::read_dir(dir)?.collect::<io::Result<_>>()?;
	entries.sort_by_key(std::fs::DirEntry::file_name);
	for entry in entries {
		let path = entry.path();
		if !include(&path) {
			continue;
		}
		let name = entry
			.file_name()
			.into_string()
			.map_err(|_| EngineError::invalid("bundle entry name is not utf-8"))?;
		let child_rel = format!("{rel}/{name}");
		let meta = fs::symlink_metadata(&path)?;
		if meta.is_dir() {
			write_tree(writer, &path, &child_rel, include, budget)?;
		} else if meta.file_type().is_symlink() {
			budget.add_entry()?;
			let target = fs::read_link(&path)?;
			let target = target
				.to_str()
				.ok_or_else(|| EngineError::invalid("bundle symlink target is not utf-8"))?;
			write_entry_header(writer, KIND_SYMLINK, &child_rel)?;
			write_len_prefixed(writer, target)?;
		} else if meta.is_file() {
			budget.add_entry()?;
			budget.add_file(meta.len())?;
			write_file(writer, &path, &child_rel, meta.len(), meta.permissions().mode(), budget)?;
		}
	}
	Ok(())
}

/// Zero-run detection granularity inside a data segment: sources that were
/// materialized without holes (plain copies of disk images) still bundle —
/// and later extract — as sparse.
const ZERO_BLOCK: usize = 64 * 1024;

fn write_file(
	writer: &mut impl Write,
	path: &Path,
	rel: &str,
	logical: u64,
	mode: u32,
	budget: &mut BundleBudget,
) -> Result<()> {
	write_entry_header(writer, KIND_FILE, rel)?;
	writer.write_all(&mode.to_le_bytes())?;
	writer.write_all(&logical.to_le_bytes())?;
	let file = File::open(path)?;
	let mut buf = vec![0u8; COPY_BUF];
	let mut extents = Vec::new();
	for_each_data_segment(&file, logical, |offset, len| {
		let end = offset
			.checked_add(len)
			.ok_or_else(|| EngineError::invalid("bundle segment out of bounds"))?;
		let mut at = offset;
		while at < end {
			let take = usize::try_from((end - at).min(COPY_BUF as u64)).expect("bounded by COPY_BUF");
			file.read_exact_at(&mut buf[..take], at)?;
			collect_nonzero_runs(&buf[..take], at, &mut extents)?;
			at += take as u64;
		}
		Ok(())
	})?;

	let mut decoded_file_bytes = 0;
	for (offset, end) in extents {
		let len = end
			.checked_sub(offset)
			.ok_or_else(|| EngineError::invalid("bundle segment out of bounds"))?;
		charge_budget(&mut decoded_file_bytes, len, logical, "bundle file decoded segment bytes")?;
		budget.add_segment(len)?;
		writer.write_all(&offset.to_le_bytes())?;
		writer.write_all(&len.to_le_bytes())?;
		write_extent(writer, &file, offset, end, &mut buf)?;
	}
	writer.write_all(&SEGMENT_TERMINATOR.to_le_bytes())?;
	Ok(())
}

/// True when `bytes` is all zero. Compares 64 KiB chunks against a static
/// zero block (compiles to memcmp), so scans run at memory bandwidth even in
/// unoptimized dev builds.
pub(crate) fn is_zero(bytes: &[u8]) -> bool {
	static ZEROES: [u8; ZERO_BLOCK] = [0; ZERO_BLOCK];
	bytes
		.chunks(ZEROES.len())
		.all(|chunk| chunk == &ZEROES[..chunk.len()])
}

/// Collect nonzero [`ZERO_BLOCK`] runs in ascending order, coalescing the
/// oldest adjacent pairs whenever the bounded extent table fills.
fn collect_nonzero_runs(chunk: &[u8], base: u64, extents: &mut Vec<(u64, u64)>) -> Result<()> {
	let mut cursor = 0usize;
	while cursor < chunk.len() {
		let block_end = chunk.len().min(cursor + ZERO_BLOCK);
		if is_zero(&chunk[cursor..block_end]) {
			cursor = block_end;
			continue;
		}
		let start = cursor;
		let mut end = block_end;
		while end < chunk.len() {
			let next = chunk.len().min(end + ZERO_BLOCK);
			if is_zero(&chunk[end..next]) {
				break;
			}
			end = next;
		}
		record_extent(
			extents,
			base
				.checked_add(start as u64)
				.ok_or_else(|| EngineError::invalid("bundle segment out of bounds"))?,
			base
				.checked_add(end as u64)
				.ok_or_else(|| EngineError::invalid("bundle segment out of bounds"))?,
		);
		cursor = end;
	}
	Ok(())
}

fn record_extent(extents: &mut Vec<(u64, u64)>, start: u64, end: u64) {
	if let Some((_, previous_end)) = extents.last_mut()
		&& start <= *previous_end
	{
		*previous_end = (*previous_end).max(end);
		return;
	}
	if extents.len() == MAX_BUNDLE_SEGMENTS as usize {
		coalesce_extents(extents);
	}
	extents.push((start, end));
}

fn coalesce_extents(extents: &mut Vec<(u64, u64)>) {
	let mut source = 0;
	let mut destination = 0;
	while source + 1 < extents.len() {
		extents[destination] = (extents[source].0, extents[source + 1].1);
		source += 2;
		destination += 1;
	}
	if source < extents.len() {
		extents[destination] = extents[source];
		destination += 1;
	}
	extents.truncate(destination);
}

fn write_extent(
	writer: &mut impl Write,
	file: &File,
	start: u64,
	end: u64,
	buf: &mut [u8],
) -> Result<()> {
	let mut at = start;
	while at < end {
		let take = usize::try_from((end - at).min(buf.len() as u64)).expect("bounded by COPY_BUF");
		file.read_exact_at(&mut buf[..take], at)?;
		writer.write_all(&buf[..take])?;
		at += take as u64;
	}
	Ok(())
}

/// Enumerate `(offset, len)` data runs via `SEEK_DATA`/`SEEK_HOLE`, treating
/// the whole file as one run where the filesystem lacks hole support.
fn for_each_data_segment(
	file: &File,
	logical: u64,
	mut visit: impl FnMut(u64, u64) -> Result<()>,
) -> Result<()> {
	let mut offset: i64 = 0;
	let len =
		i64::try_from(logical).map_err(|_| EngineError::invalid("bundle file length exceeds i64"))?;
	while offset < len {
		// SAFETY: lseek on an owned open fd with well-formed arguments.
		let data = unsafe { libc::lseek(file.as_raw_fd(), offset, libc::SEEK_DATA) };
		if data < 0 {
			let errno = io::Error::last_os_error();
			match errno.raw_os_error() {
				// No data past `offset`: the remainder is one hole.
				Some(libc::ENXIO) => break,
				// Filesystem without hole enumeration: ship everything.
				Some(code) if code == libc::EINVAL || code == libc::ENOTSUP => {
					visit(0, logical)?;
					break;
				},
				_ => return Err(EngineError::engine(format!("SEEK_DATA failed: {errno}"))),
			}
		}
		// SAFETY: as above.
		let hole = unsafe { libc::lseek(file.as_raw_fd(), data, libc::SEEK_HOLE) };
		if hole < 0 {
			return Err(EngineError::engine(format!(
				"SEEK_HOLE failed: {}",
				io::Error::last_os_error()
			)));
		}
		let end = hole.min(len);
		if end > data {
			visit(data as u64, (end - data) as u64)?;
		}
		offset = hole.max(data + 1);
	}
	Ok(())
}

fn write_entry_header(writer: &mut impl Write, kind: u8, rel: &str) -> Result<()> {
	writer.write_all(&[kind])?;
	write_len_prefixed(writer, rel)
}

fn write_len_prefixed(writer: &mut impl Write, value: &str) -> Result<()> {
	let len = u16::try_from(value.len())
		.map_err(|_| EngineError::invalid("bundle path exceeds u16 length"))?;
	writer.write_all(&len.to_le_bytes())?;
	writer.write_all(value.as_bytes())?;
	Ok(())
}

fn mode_of(path: &Path) -> Result<u32> {
	Ok(fs::symlink_metadata(path)?.permissions().mode())
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
	let mut buf = [0u8; 4];
	reader.read_exact(&mut buf)?;
	Ok(u32::from_le_bytes(buf))
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
	let mut buf = [0u8; 8];
	reader.read_exact(&mut buf)?;
	Ok(u64::from_le_bytes(buf))
}

fn read_path(reader: &mut impl Read) -> Result<String> {
	let mut buf = [0u8; 2];
	reader.read_exact(&mut buf)?;
	let len = usize::from(u16::from_le_bytes(buf));
	if len == 0 || len > MAX_PATH {
		return Err(EngineError::invalid("bundle path length out of range"));
	}
	let mut path = vec![0u8; len];
	reader.read_exact(&mut path)?;
	String::from_utf8(path).map_err(|_| EngineError::invalid("bundle path is not utf-8"))
}

/// Resolve a bundle-relative path under `dest_root`, rejecting traversal.
fn dest_path(dest_root: &Path, rel: &str) -> Result<PathBuf> {
	let mut path = dest_root.to_path_buf();
	for component in rel.split('/') {
		if component.is_empty() || component == "." || component == ".." {
			return Err(EngineError::invalid(format!("bundle path {rel:?} is not safe")));
		}
		path.push(component);
	}
	Ok(path)
}

fn prepare_destination(dest_root: &Path) -> Result<()> {
	if let Some(parent) = dest_root
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
	{
		fs::create_dir_all(parent)?;
	}
	match fs::create_dir(dest_root) {
		Ok(()) => Ok(()),
		Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
			if !fs::symlink_metadata(dest_root)?.file_type().is_dir() {
				return Err(EngineError::invalid("bundle destination is not a directory"));
			}
			if fs::read_dir(dest_root)?.next().transpose()?.is_some() {
				return Err(EngineError::invalid("bundle destination must be empty"));
			}
			Ok(())
		},
		Err(err) => Err(err.into()),
	}
}

fn read_entry_path(
	reader: &mut impl Read,
	dest_root: &Path,
	declared_paths: &mut HashSet<String>,
	declared_directories: &HashSet<String>,
) -> Result<(String, PathBuf)> {
	let rel = read_path(reader)?;
	let path = dest_path(dest_root, &rel)?;
	claim_entry_path(&rel, declared_paths)?;
	require_declared_parent(&rel, declared_directories)?;
	Ok((rel, path))
}

fn claim_entry_path(path: &str, declared_paths: &mut HashSet<String>) -> Result<()> {
	if !declared_paths.insert(path.to_owned()) {
		return Err(EngineError::invalid(format!("bundle path {path:?} is declared more than once")));
	}
	Ok(())
}

fn create_output_file(path: &Path) -> Result<File> {
	Ok(OpenOptions::new()
		.write(true)
		.create_new(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(path)?)
}

fn charge_budget(used: &mut u64, amount: u64, limit: u64, name: &str) -> Result<()> {
	let next = used
		.checked_add(amount)
		.filter(|&next| next <= limit)
		.ok_or_else(|| EngineError::invalid(format!("{name} exceeds extraction limit")))?;
	*used = next;
	Ok(())
}

fn require_declared_parent(path: &str, directories: &HashSet<String>) -> Result<()> {
	if let Some((parent, _)) = path.rsplit_once('/')
		&& !directories.contains(parent)
	{
		return Err(EngineError::invalid(format!(
			"bundle parent directory {parent:?} was not declared"
		)));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::io::{Seek, SeekFrom, Write as _};

	use super::*;

	fn test_bundle(entries: impl FnOnce(&mut dyn std::io::Write)) -> Vec<u8> {
		let mut writer = zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL).unwrap();
		writer.write_all(MAGIC).unwrap();
		writer.write_all(&VERSION.to_le_bytes()).unwrap();
		entries(&mut writer);
		writer.write_all(&[KIND_END]).unwrap();
		writer.finish().unwrap()
	}

	fn write_test_header(writer: &mut dyn std::io::Write, kind: u8, path: &str) {
		writer.write_all(&[kind]).unwrap();
		writer
			.write_all(&(path.len() as u16).to_le_bytes())
			.unwrap();
		writer.write_all(path.as_bytes()).unwrap();
	}

	fn write_test_dir(writer: &mut dyn std::io::Write, path: &str) {
		write_test_header(writer, KIND_DIR, path);
		writer.write_all(&0o755u32.to_le_bytes()).unwrap();
	}

	fn write_test_file(
		writer: &mut dyn std::io::Write,
		path: &str,
		logical: u64,
		segments: &[(u64, &[u8])],
	) {
		write_test_header(writer, KIND_FILE, path);
		writer.write_all(&0o644u32.to_le_bytes()).unwrap();
		writer.write_all(&logical.to_le_bytes()).unwrap();
		for (offset, data) in segments {
			writer.write_all(&offset.to_le_bytes()).unwrap();
			writer
				.write_all(&(data.len() as u64).to_le_bytes())
				.unwrap();
			writer.write_all(data).unwrap();
		}
		writer.write_all(&SEGMENT_TERMINATOR.to_le_bytes()).unwrap();
	}

	fn write_test_symlink(writer: &mut dyn std::io::Write, path: &str, target: &str) {
		write_test_header(writer, KIND_SYMLINK, path);
		writer
			.write_all(&(target.len() as u16).to_le_bytes())
			.unwrap();
		writer.write_all(target.as_bytes()).unwrap();
	}

	fn bundle_roundtrip(filter: &dyn Fn(&Path) -> bool) -> (tempfile::TempDir, PathBuf) {
		let tmp = tempfile::tempdir().expect("tempdir");
		let root = tmp.path().join("chk");
		fs::create_dir_all(root.join("volumes/data")).unwrap();
		fs::write(root.join("vmstate.1.bin"), b"state").unwrap();
		fs::write(root.join("volumes/data/file.txt"), b"volume bytes").unwrap();
		let mut exe = File::create(root.join("volumes/data/tool.sh")).unwrap();
		exe.write_all(b"#!/bin/sh\n").unwrap();
		exe.set_permissions(fs::Permissions::from_mode(0o755))
			.unwrap();
		std::os::unix::fs::symlink("file.txt", root.join("volumes/data/link")).unwrap();
		// 8 MiB sparse memory dump: one data page in the middle, one at the end.
		let mut mem = File::create(root.join("memory.1.bin")).unwrap();
		mem.seek(SeekFrom::Start(4 * 1024 * 1024)).unwrap();
		mem.write_all(&[0xaa; 4096]).unwrap();
		mem.seek(SeekFrom::Start(8 * 1024 * 1024 - 4096)).unwrap();
		mem.write_all(&[0xbb; 4096]).unwrap();
		drop(mem);

		let archive = tmp.path().join("chk.vbundle");
		write_bundle(&root, File::create(&archive).unwrap(), filter).expect("write bundle");
		let out = tmp.path().join("out");
		read_bundle(File::open(&archive).unwrap(), &out).expect("read bundle");
		(tmp, out)
	}

	#[test]
	fn roundtrip_preserves_content_holes_modes_and_symlinks() {
		let (_tmp, out) = bundle_roundtrip(&|_| true);
		let root = out.join("chk");
		assert_eq!(fs::read(root.join("vmstate.1.bin")).unwrap(), b"state");
		assert_eq!(fs::read(root.join("volumes/data/file.txt")).unwrap(), b"volume bytes");
		assert_eq!(
			fs::symlink_metadata(root.join("volumes/data/tool.sh"))
				.unwrap()
				.permissions()
				.mode() & 0o777,
			0o755
		);
		assert_eq!(fs::read_link(root.join("volumes/data/link")).unwrap(), PathBuf::from("file.txt"));
		let mem = fs::read(root.join("memory.1.bin")).unwrap();
		assert_eq!(mem.len(), 8 * 1024 * 1024, "logical length restored");

		assert_eq!(mem[4 * 1024 * 1024], 0xaa);
		assert_eq!(mem[8 * 1024 * 1024 - 1], 0xbb);
		assert!(mem[..4 * 1024 * 1024].iter().all(|&b| b == 0), "hole reads as zeros");
	}

	#[test]
	fn named_root_decouples_bundle_identity_from_local_directory_name() {
		let tmp = tempfile::tempdir().unwrap();
		let left = tmp.path().join("capture-left");
		let right = tmp.path().join("capture-right");
		for root in [&left, &right] {
			fs::create_dir(root).unwrap();
			fs::write(root.join("state"), b"same checkpoint").unwrap();
		}
		let mut left_bundle = Vec::new();
		let mut right_bundle = Vec::new();
		write_bundle_named(&left, "replica", &mut left_bundle, &|_| true).unwrap();
		write_bundle_named(&right, "replica", &mut right_bundle, &|_| true).unwrap();
		let left_out = tmp.path().join("left-out");
		let right_out = tmp.path().join("right-out");
		read_bundle(left_bundle.as_slice(), &left_out).unwrap();
		read_bundle(right_bundle.as_slice(), &right_out).unwrap();
		assert_eq!(fs::read(left_out.join("replica/state")).unwrap(), b"same checkpoint");
		assert_eq!(fs::read(right_out.join("replica/state")).unwrap(), b"same checkpoint");
	}

	#[test]
	fn extraction_defers_restrictive_directory_modes() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let archive = tmp.path().join("chk.vbundle");
		let mut writer =
			zstd::stream::write::Encoder::new(File::create(&archive).unwrap(), ZSTD_LEVEL)
				.expect("create encoder");
		writer.write_all(MAGIC).unwrap();
		writer.write_all(&VERSION.to_le_bytes()).unwrap();

		for (path, mode) in
			[("chk", 0o555u32), ("chk/locked", 0o000u32), ("chk/locked/nested", 0o555u32)]
		{
			write_entry_header(&mut writer, KIND_DIR, path).unwrap();
			writer.write_all(&mode.to_le_bytes()).unwrap();
		}
		for (path, contents) in [
			("chk/root-file", b"root" as &[u8]),
			("chk/locked/locked-file", b"locked" as &[u8]),
			("chk/locked/nested/nested-file", b"nested" as &[u8]),
		] {
			write_entry_header(&mut writer, KIND_FILE, path).unwrap();
			writer.write_all(&0o644u32.to_le_bytes()).unwrap();
			writer
				.write_all(&(contents.len() as u64).to_le_bytes())
				.unwrap();
			writer.write_all(&0u64.to_le_bytes()).unwrap();
			writer
				.write_all(&(contents.len() as u64).to_le_bytes())
				.unwrap();
			writer.write_all(contents).unwrap();
			writer.write_all(&SEGMENT_TERMINATOR.to_le_bytes()).unwrap();
		}
		writer.write_all(&[KIND_END]).unwrap();
		writer.finish().unwrap();

		let out = tmp.path().join("out");
		read_bundle(File::open(&archive).unwrap(), &out).expect("extract restrictive directories");
		let root = out.join("chk");
		let locked = root.join("locked");
		assert_eq!(fs::read(root.join("root-file")).unwrap(), b"root");
		assert_eq!(fs::symlink_metadata(&root).unwrap().permissions().mode() & 0o7777, 0o555);
		assert_eq!(fs::symlink_metadata(&locked).unwrap().permissions().mode() & 0o7777, 0o000);

		fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();
		let nested = locked.join("nested");
		assert_eq!(fs::read(locked.join("locked-file")).unwrap(), b"locked");
		assert_eq!(fs::read(nested.join("nested-file")).unwrap(), b"nested");
		assert_eq!(fs::symlink_metadata(&nested).unwrap().permissions().mode() & 0o7777, 0o555);

		fs::set_permissions(&nested, fs::Permissions::from_mode(0o700)).unwrap();
		fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
	}

	#[test]
	fn extraction_budget_boundaries_and_overflow_are_rejected() {
		assert_eq!(MAX_BUNDLE_LOGICAL_BYTES, 1 << 40, "aggregate cap is 1 TiB");
		let mut decoded_file = MAX_BUNDLE_FILE_LOGICAL_BYTES - 1;
		charge_budget(&mut decoded_file, 1, MAX_BUNDLE_FILE_LOGICAL_BYTES, "decoded file bytes")
			.unwrap();
		assert!(
			charge_budget(&mut decoded_file, 1, MAX_BUNDLE_FILE_LOGICAL_BYTES, "decoded file bytes")
				.is_err()
		);

		let mut decoded_total = MAX_BUNDLE_LOGICAL_BYTES - 1;
		charge_budget(&mut decoded_total, 1, MAX_BUNDLE_LOGICAL_BYTES, "decoded total bytes")
			.unwrap();
		assert!(
			charge_budget(&mut decoded_total, 1, MAX_BUNDLE_LOGICAL_BYTES, "decoded total bytes")
				.is_err()
		);

		let mut used = 1;
		assert!(charge_budget(&mut used, u64::MAX, u64::MAX, "overflow").is_err());
	}

	#[test]
	fn extraction_rejects_huge_sparse_file_before_creation() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			write_test_file(writer, "chk/huge", MAX_BUNDLE_FILE_LOGICAL_BYTES + 1, &[]);
		});
		let out = tmp.path().join("out");
		let err = read_bundle(archive.as_slice(), &out).unwrap_err();
		assert!(err.message.contains("logical length exceeds limit"));
		assert!(!out.join("chk/huge").exists(), "oversized file must not be created");
	}

	#[test]
	fn extraction_rejects_entry_flood() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			for entry in 0..MAX_BUNDLE_ENTRIES {
				write_test_file(writer, &format!("chk/{entry}"), 0, &[]);
			}
		});
		let err = read_bundle(archive.as_slice(), &tmp.path().join("out")).unwrap_err();
		assert!(err.message.contains("entry exceeds extraction limit"));
	}

	#[test]
	fn extraction_rejects_segment_flood() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			write_test_header(writer, KIND_FILE, "chk/sparse");
			writer.write_all(&0o644u32.to_le_bytes()).unwrap();
			writer.write_all(&0u64.to_le_bytes()).unwrap();
			for _ in 0..=MAX_BUNDLE_SEGMENTS {
				writer.write_all(&0u64.to_le_bytes()).unwrap();
				writer.write_all(&0u64.to_le_bytes()).unwrap();
			}
			writer.write_all(&SEGMENT_TERMINATOR.to_le_bytes()).unwrap();
		});
		let err = read_bundle(archive.as_slice(), &tmp.path().join("out")).unwrap_err();
		assert!(
			err.message
				.contains("data segment exceeds extraction limit")
		);
	}

	#[test]
	fn extraction_rejects_repeated_overlapping_and_out_of_order_segments() {
		let tmp = tempfile::tempdir().expect("tempdir");
		for (name, logical, segments) in [
			("repeated", 2, vec![(0, b"a" as &[u8]), (0, b"b" as &[u8])]),
			("overlapping", 3, vec![(0, b"ab" as &[u8]), (1, b"c" as &[u8])]),
			("out-of-order", 3, vec![(2, b"c" as &[u8]), (0, b"ab" as &[u8])]),
		] {
			let archive = test_bundle(|writer| {
				write_test_dir(writer, "chk");
				write_test_file(writer, "chk/file", logical, &segments);
			});
			let err =
				read_bundle(archive.as_slice(), &tmp.path().join(format!("out-{name}"))).unwrap_err();
			assert!(err.message.contains("overlap or are out of order"));
		}
	}

	#[test]
	fn extraction_rejects_symlink_ancestor() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let outside = tmp.path().join("outside");
		fs::create_dir(&outside).unwrap();
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			write_test_symlink(writer, "chk/a", outside.to_str().unwrap());
			write_test_file(writer, "chk/a/escaped", 0, &[]);
		});
		let err = read_bundle(archive.as_slice(), &tmp.path().join("out")).unwrap_err();
		assert!(err.message.contains("was not declared"));
		assert!(
			!outside.join("escaped").exists(),
			"symlink ancestor must not escape extraction root"
		);
	}

	#[test]
	fn extraction_rejects_duplicate_symlink_file_path_before_outside_write() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let outside = tmp.path().join("outside");
		fs::write(&outside, b"unchanged").unwrap();
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			write_test_symlink(writer, "chk/a", outside.to_str().unwrap());
			write_test_file(writer, "chk/a", 0, &[]);
		});
		let err = read_bundle(archive.as_slice(), &tmp.path().join("out")).unwrap_err();
		assert!(err.message.contains("declared more than once"));
		assert_eq!(fs::read(&outside).unwrap(), b"unchanged");
	}

	#[test]
	fn extraction_rejects_preexisting_final_symlink() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let outside = tmp.path().join("outside");
		let final_path = tmp.path().join("final");
		fs::write(&outside, b"unchanged").unwrap();
		std::os::unix::fs::symlink(&outside, &final_path).unwrap();

		assert!(create_output_file(&final_path).is_err());
		assert_eq!(fs::read(&outside).unwrap(), b"unchanged");
	}

	#[test]
	fn extraction_requires_an_empty_destination() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let out = tmp.path().join("out");
		fs::create_dir(&out).unwrap();
		fs::write(out.join("existing"), b"keep").unwrap();

		let err = read_bundle(&b""[..], &out).unwrap_err();
		assert!(err.message.contains("destination must be empty"));
	}

	#[test]
	fn extraction_rejects_non_directory_ancestor() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			write_test_file(writer, "chk/a", 0, &[]);
			write_test_file(writer, "chk/a/child", 0, &[]);
		});
		let err = read_bundle(archive.as_slice(), &tmp.path().join("out")).unwrap_err();
		assert!(err.message.contains("was not declared"));
	}

	#[test]
	fn bundle_stays_small_for_sparse_payloads() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let root = tmp.path().join("chk");
		fs::create_dir_all(&root).unwrap();
		let mem = File::create(root.join("memory.1.bin")).unwrap();
		mem.set_len(64 * 1024 * 1024).unwrap();
		drop(mem);
		let archive = tmp.path().join("chk.vbundle");
		write_bundle(&root, File::create(&archive).unwrap(), &|_| true).unwrap();
		let size = fs::metadata(&archive).unwrap().len();
		assert!(size < 4096, "64 MiB of holes must bundle to bytes, got {size}");
	}

	#[test]
	fn writer_rejects_file_exceeding_logical_limit() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let root = tmp.path().join("chk");
		fs::create_dir(&root).unwrap();
		let file = File::create(root.join("huge")).unwrap();
		file.set_len(MAX_BUNDLE_FILE_LOGICAL_BYTES + 1).unwrap();
		drop(file);

		let err = write_bundle(&root, Vec::new(), &|_| true).unwrap_err();
		assert!(err.message.contains("logical length exceeds limit"));
	}

	#[test]
	fn writer_allows_last_segment_at_cap_and_roundtrips() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let source = tmp.path().join("source");
		fs::write(&source, b"final segment").unwrap();
		let mut writer = zstd::stream::write::Encoder::new(Vec::new(), ZSTD_LEVEL).unwrap();
		writer.write_all(MAGIC).unwrap();
		writer.write_all(&VERSION.to_le_bytes()).unwrap();
		write_entry_header(&mut writer, KIND_DIR, "chk").unwrap();
		writer.write_all(&0o755u32.to_le_bytes()).unwrap();

		let mut budget = BundleBudget { segments: MAX_BUNDLE_SEGMENTS - 1, ..Default::default() };
		write_file(
			&mut writer,
			&source,
			"chk/file",
			fs::metadata(&source).unwrap().len(),
			0o644,
			&mut budget,
		)
		.unwrap();
		assert_eq!(budget.segments, MAX_BUNDLE_SEGMENTS);
		writer.write_all(&[KIND_END]).unwrap();
		let archive = writer.finish().unwrap();

		let out = tmp.path().join("out");
		read_bundle(archive.as_slice(), &out).unwrap();
		assert_eq!(fs::read(out.join("chk/file")).unwrap(), b"final segment");
	}

	#[test]
	fn writer_hierarchically_coalesces_over_cap_sparse_extents() {
		let stride = 1 << 20;
		let logical = (MAX_BUNDLE_SEGMENTS + 1) * stride;
		let mut extents = Vec::new();
		for index in 0..=MAX_BUNDLE_SEGMENTS {
			let start = index * stride;
			record_extent(&mut extents, start, start + 1);
		}

		assert!(extents.len() <= MAX_BUNDLE_SEGMENTS as usize);
		let encoded_bytes: u64 = extents.iter().map(|(start, end)| end - start).sum();
		assert!(encoded_bytes < logical * 3 / 4, "coalescing must not densify the sparse file");
		assert_eq!(extents.last().unwrap().1 - extents.last().unwrap().0, 1);
	}

	#[test]
	fn fragmented_bundle_at_segment_cap_roundtrips() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let logical = MAX_BUNDLE_SEGMENTS * 2;
		let archive = test_bundle(|writer| {
			write_test_dir(writer, "chk");
			write_test_header(writer, KIND_FILE, "chk/fragmented");
			writer.write_all(&0o644u32.to_le_bytes()).unwrap();
			writer.write_all(&logical.to_le_bytes()).unwrap();
			for segment in 0..MAX_BUNDLE_SEGMENTS {
				writer.write_all(&(segment * 2).to_le_bytes()).unwrap();
				writer.write_all(&1u64.to_le_bytes()).unwrap();
				writer.write_all(&[1]).unwrap();
			}
			writer.write_all(&SEGMENT_TERMINATOR.to_le_bytes()).unwrap();
		});
		let out = tmp.path().join("out");
		read_bundle(archive.as_slice(), &out).unwrap();
		let bytes = fs::read(out.join("chk/fragmented")).unwrap();
		assert_eq!(bytes.len(), logical as usize);
		assert_eq!(bytes[0], 1);
		assert_eq!(bytes[(MAX_BUNDLE_SEGMENTS - 1) as usize], 0);
		assert_eq!(bytes[(MAX_BUNDLE_SEGMENTS * 2 - 2) as usize], 1);
	}

	#[test]
	fn filter_excludes_entries() {
		let (_tmp, out) = bundle_roundtrip(&|path| {
			!path
				.file_name()
				.is_some_and(|name| name.to_string_lossy().starts_with("memory."))
		});
		assert!(out.join("chk/vmstate.1.bin").is_file());
		assert!(!out.join("chk/memory.1.bin").exists(), "filtered file must not travel");
	}

	#[test]
	fn traversal_paths_are_rejected() {
		let err = dest_path(Path::new("/safe"), "chk/../evil").unwrap_err();
		assert!(err.message.contains("not safe"));
		let err = dest_path(Path::new("/safe"), "").unwrap_err();
		assert!(err.message.contains("not safe"));
	}

	#[test]
	fn channel_bridges_roundtrip_and_propagate_errors() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let root = tmp.path().join("chk");
		fs::create_dir_all(&root).unwrap();
		fs::write(root.join("vmstate.1.bin"), vec![7u8; 3 * COPY_BUF / 2]).unwrap();

		let (tx, rx) = tokio::sync::mpsc::channel(4);
		let out = tmp.path().join("out");
		let root_clone = root;
		let writer = std::thread::spawn(move || {
			let mut bridge = ChannelWriter::new(tx);
			let result = write_bundle(&root_clone, &mut bridge, &|_| true);
			bridge.finish(result);
		});
		read_bundle(ChannelReader::new(rx), &out).expect("streamed extract");
		writer.join().expect("writer thread");
		assert_eq!(fs::read(out.join("chk/vmstate.1.bin")).unwrap(), vec![7u8; 3 * COPY_BUF / 2]);

		// A producer error must surface on the reader side.
		let (tx, rx) = tokio::sync::mpsc::channel(4);
		ChannelWriter::new(tx).finish(Err(EngineError::engine("boom")));
		let err = read_bundle(ChannelReader::new(rx), &tmp.path().join("out2")).unwrap_err();
		assert!(err.message.contains("boom"), "unexpected error: {}", err.message);
	}
}
