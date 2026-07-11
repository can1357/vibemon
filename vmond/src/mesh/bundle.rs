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
	fs::{self, File},
	io::{self, BufReader, Read, Write},
	os::unix::{
		fs::{FileExt, PermissionsExt},
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

/// Bundle `root` (the directory itself, like `tar -C parent root`) into
/// `out`, including only entries the filter accepts.
pub fn write_bundle(root: &Path, out: impl Write, include: &dyn Fn(&Path) -> bool) -> Result<()> {
	let root_name = root
		.file_name()
		.and_then(|name| name.to_str())
		.ok_or_else(|| EngineError::invalid("bundle root has no utf-8 name"))?;
	let mut encoder = zstd::stream::write::Encoder::new(out, ZSTD_LEVEL)
		.map_err(|err| EngineError::engine(format!("zstd encoder init failed: {err}")))?;
	let workers = thread::available_parallelism().map_or(1, |n| n.get().min(8)) as u32;
	encoder
		.multithread(workers)
		.map_err(|err| EngineError::engine(format!("zstd multithread init failed: {err}")))?;
	encoder.write_all(MAGIC)?;
	encoder.write_all(&VERSION.to_le_bytes())?;
	write_tree(&mut encoder, root, root_name, include)?;
	encoder.write_all(&[KIND_END])?;
	let mut out = encoder
		.finish()
		.map_err(|err| EngineError::engine(format!("finishing bundle: {err}")))?;
	out.flush()?;
	Ok(())
}

/// Extract a bundle into `dest_root` (which is created), refusing absolute or
/// traversal paths.
pub fn read_bundle(source: impl Read, dest_root: &Path) -> Result<()> {
	fs::create_dir_all(dest_root)?;
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
	loop {
		let mut kind = [0u8; 1];
		reader.read_exact(&mut kind)?;
		match kind[0] {
			KIND_END => return Ok(()),
			KIND_DIR => {
				let path = dest_path(dest_root, &read_path(&mut reader)?)?;
				fs::create_dir_all(&path)?;
				let mode = read_u32(&mut reader)?;
				fs::set_permissions(&path, fs::Permissions::from_mode(mode & 0o7777))?;
			},
			KIND_FILE => {
				let path = dest_path(dest_root, &read_path(&mut reader)?)?;
				let mode = read_u32(&mut reader)?;
				let logical = read_u64(&mut reader)?;
				let out = File::create(&path)?;
				out.set_len(logical)?;
				loop {
					let offset = read_u64(&mut reader)?;
					if offset == SEGMENT_TERMINATOR {
						break;
					}
					let len = read_u64(&mut reader)?;
					let end = offset
						.checked_add(len)
						.filter(|&end| end <= logical)
						.ok_or_else(|| EngineError::invalid("bundle segment out of bounds"))?;
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
				let path = dest_path(dest_root, &read_path(&mut reader)?)?;
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
) -> Result<()> {
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
			write_tree(writer, &path, &child_rel, include)?;
		} else if meta.file_type().is_symlink() {
			let target = fs::read_link(&path)?;
			let target = target
				.to_str()
				.ok_or_else(|| EngineError::invalid("bundle symlink target is not utf-8"))?;
			write_entry_header(writer, KIND_SYMLINK, &child_rel)?;
			write_len_prefixed(writer, target)?;
		} else if meta.is_file() {
			write_file(writer, &path, &child_rel, meta.len(), meta.permissions().mode())?;
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
) -> Result<()> {
	write_entry_header(writer, KIND_FILE, rel)?;
	writer.write_all(&mode.to_le_bytes())?;
	writer.write_all(&logical.to_le_bytes())?;
	let file = File::open(path)?;
	let mut buf = vec![0u8; COPY_BUF];
	for (offset, len) in data_segments(&file, logical)? {
		let mut at = offset;
		let end = offset + len;
		while at < end {
			let take = usize::try_from((end - at).min(COPY_BUF as u64)).expect("bounded by COPY_BUF");
			file.read_exact_at(&mut buf[..take], at)?;
			write_nonzero_runs(writer, &buf[..take], at)?;
			at += take as u64;
		}
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

/// Emit the nonzero [`ZERO_BLOCK`] runs of `chunk` (whose file offset is
/// `base`) as bundle segments, coalescing adjacent nonzero blocks.
fn write_nonzero_runs(writer: &mut impl Write, chunk: &[u8], base: u64) -> Result<()> {
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
		writer.write_all(&(base + start as u64).to_le_bytes())?;
		writer.write_all(&((end - start) as u64).to_le_bytes())?;
		writer.write_all(&chunk[start..end])?;
		cursor = end;
	}
	Ok(())
}

/// Enumerate `(offset, len)` data runs via `SEEK_DATA`/`SEEK_HOLE`, treating
/// the whole file as one run where the filesystem lacks hole support.
fn data_segments(file: &File, logical: u64) -> Result<Vec<(u64, u64)>> {
	let mut segments = Vec::new();
	let mut offset: i64 = 0;
	let len =
		i64::try_from(logical).map_err(|_| EngineError::invalid("bundle file length exceeds i64"))?;
	while offset < len {
		// SAFETY: lseek on an owned open fd with well-formed arguments.
		let data = unsafe { libc::lseek(file.as_raw_fd(), offset, libc::SEEK_DATA) };
		if data < 0 {
			let errno = io::Error::last_os_error();
			return match errno.raw_os_error() {
				// No data past `offset`: the remainder is one hole.
				Some(libc::ENXIO) => Ok(segments),
				// Filesystem without hole enumeration: ship everything.
				Some(code) if code == libc::EINVAL || code == libc::ENOTSUP => Ok(vec![(0, logical)]),
				_ => Err(EngineError::engine(format!("SEEK_DATA failed: {errno}"))),
			};
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
			segments.push((data as u64, (end - data) as u64));
		}
		offset = hole.max(data + 1);
	}
	Ok(segments)
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

#[cfg(test)]
mod tests {
	use std::io::{Seek, SeekFrom, Write as _};

	use super::*;

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
