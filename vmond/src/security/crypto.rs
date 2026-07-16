//! Chunked authenticated encryption for credentials, snapshots, and detached
//! volumes.

use std::{
	fs::{self, File, OpenOptions, Permissions},
	io::{self, Read, Write},
	os::unix::fs::{OpenOptionsExt, PermissionsExt},
	path::{Path, PathBuf},
};

use chacha20poly1305::{
	Key, XChaCha20Poly1305, XNonce,
	aead::{Aead, KeyInit, Payload},
};
use zeroize::{Zeroize, Zeroizing};

use crate::{EngineError, Result, home::Home, mesh::bundle};

const MAGIC: &[u8; 8] = b"VMONENC1";
const CHUNK_SIZE: usize = 1024 * 1024;
const TAG_SIZE: usize = 16;
const MAX_KEY_ID: usize = 128;
const KEY_MODE: u32 = 0o600;
const DIR_MODE: u32 = 0o700;

/// Customer-managed key lookup rooted at `$VMON_HOME/security/keys`.
///
/// Each key is a mode-0600 file named `<key-id>.key` containing exactly 64
/// lowercase or uppercase hexadecimal characters. Removing a customer key
/// immediately prevents new decryptions of every object protected by it.
#[derive(Clone, Debug)]
pub struct Keyring {
	dir: PathBuf,
}

impl Keyring {
	/// Open the key directory and create a host-owned default key when absent.
	pub fn open(home: &Home) -> Result<Self> {
		let dir = home.keys_dir();
		ensure_private_dir(&dir)?;
		let keyring = Self { dir };
		keyring.ensure_default()?;
		Ok(keyring)
	}

	/// Load a named 256-bit key without caching it, so revocation takes effect.
	pub fn load(&self, key_id: &str) -> Result<Zeroizing<[u8; 32]>> {
		validate_key_id(key_id)?;
		let path = self.dir.join(format!("{key_id}.key"));
		let metadata = fs::symlink_metadata(&path).map_err(|error| {
			EngineError::not_found(format!("encryption key {key_id:?} is unavailable: {error}"))
		})?;
		if metadata.file_type().is_symlink() || !metadata.is_file() {
			return Err(EngineError::invalid(format!(
				"encryption key {} must be a regular file",
				path.display()
			)));
		}
		if metadata.permissions().mode() & 0o077 != 0 {
			return Err(EngineError::invalid(format!(
				"encryption key {} must not be accessible by group or other users",
				path.display()
			)));
		}
		let text = fs::read_to_string(&path)?;
		let decoded = hex::decode(text.trim())
			.map_err(|_| EngineError::invalid(format!("encryption key {key_id:?} is not hex")))?;
		let mut key = Zeroizing::new([0_u8; 32]);
		if decoded.len() != key.len() {
			return Err(EngineError::invalid(format!(
				"encryption key {key_id:?} must contain exactly 32 bytes"
			)));
		}
		key.copy_from_slice(&decoded);
		Ok(key)
	}

	fn ensure_default(&self) -> Result<()> {
		let path = self.dir.join("default.key");
		match fs::symlink_metadata(&path) {
			Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
				return Err(EngineError::invalid(format!(
					"default encryption key {} must be a regular file",
					path.display()
				)));
			},
			Ok(_) => return self.load("default").map(|mut key| key.zeroize()),
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error.into()),
		}
		let mut key = Zeroizing::new(rand::random::<[u8; 32]>());
		let mut file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(KEY_MODE)
			.open(&path)?;
		file.write_all(hex::encode(*key).as_bytes())?;
		file.write_all(b"\n")?;
		file.sync_all()?;
		key.zeroize();
		Ok(())
	}
}

/// Encrypted sparse-aware directory archives used for durable sandbox state.
pub struct EncryptedArchive;

impl EncryptedArchive {
	/// Atomically encrypt a directory bundle under `key_id`.
	pub fn seal(root: &Path, destination: &Path, keyring: &Keyring, key_id: &str) -> Result<()> {
		let key = keyring.load(key_id)?;
		let parent = destination.parent().ok_or_else(|| {
			EngineError::invalid(format!("encrypted archive {} has no parent", destination.display()))
		})?;
		ensure_private_dir(parent)?;
		let temporary = temporary_path(destination);
		let file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(KEY_MODE)
			.open(&temporary)?;
		let mut encrypted = EncryptWriter::new(file, key_id, &key)?;
		let result = bundle::write_bundle(root, &mut encrypted, &|_| true)
			.and_then(|()| encrypted.finish().map_err(EngineError::from))
			.and_then(|file| {
				file.sync_all()?;
				fs::rename(&temporary, destination)?;
				Ok(())
			});
		if result.is_err() {
			let _ = fs::remove_file(&temporary);
		}
		result
	}

	/// Decrypt and extract an archive, returning the embedded root directory.
	pub fn open(source: &Path, destination: &Path, keyring: &Keyring) -> Result<PathBuf> {
		if destination.exists() {
			return Err(EngineError::busy(format!(
				"archive extraction destination {} already exists",
				destination.display()
			)));
		}
		let file = File::open(source)?;
		let mut decrypted = DecryptReader::new(file, keyring)?;
		if let Err(error) = bundle::read_bundle(&mut decrypted, destination).and_then(|()| {
			io::copy(&mut decrypted, &mut io::sink())?;
			Ok(())
		}) {
			let _ = fs::remove_dir_all(destination);
			return Err(error);
		}
		let mut entries = fs::read_dir(destination)?
			.filter_map(std::result::Result::ok)
			.map(|entry| entry.path())
			.collect::<Vec<_>>();
		entries.sort();
		if entries.len() != 1 || !entries[0].is_dir() {
			let _ = fs::remove_dir_all(destination);
			return Err(EngineError::invalid("encrypted archive must contain one root directory"));
		}
		Ok(entries.remove(0))
	}

	/// Encrypt a small sensitive record atomically.
	pub fn seal_bytes(
		bytes: &[u8],
		destination: &Path,
		keyring: &Keyring,
		key_id: &str,
	) -> Result<()> {
		let key = keyring.load(key_id)?;
		let parent = destination.parent().ok_or_else(|| {
			EngineError::invalid(format!("encrypted record {} has no parent", destination.display()))
		})?;
		ensure_private_dir(parent)?;
		let temporary = temporary_path(destination);
		let file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(KEY_MODE)
			.open(&temporary)?;
		let mut encrypted = EncryptWriter::new(file, key_id, &key)?;
		let result = encrypted
			.write_all(bytes)
			.and_then(|()| encrypted.finish())
			.and_then(|file| file.sync_all())
			.map_err(EngineError::from)
			.and_then(|()| fs::rename(&temporary, destination).map_err(EngineError::from));
		if result.is_err() {
			let _ = fs::remove_file(&temporary);
		}
		result
	}

	/// Decrypt a bounded sensitive record.
	pub fn open_bytes(source: &Path, keyring: &Keyring, max_bytes: usize) -> Result<Vec<u8>> {
		let mut decrypted = DecryptReader::new(File::open(source)?, keyring)?;
		let mut bytes = Vec::new();
		decrypted
			.by_ref()
			.take(u64::try_from(max_bytes).unwrap_or(u64::MAX) + 1)
			.read_to_end(&mut bytes)?;
		if bytes.len() > max_bytes {
			bytes.zeroize();
			return Err(EngineError::invalid("encrypted record exceeds size limit"));
		}
		Ok(bytes)
	}
}

struct EncryptWriter<W: Write> {
	inner:      Option<W>,
	cipher:     XChaCha20Poly1305,
	key_id:     Vec<u8>,
	nonce_seed: [u8; 16],
	counter:    u64,
	buffer:     Vec<u8>,
}

impl<W: Write> EncryptWriter<W> {
	fn new(mut inner: W, key_id: &str, key: &[u8; 32]) -> io::Result<Self> {
		validate_key_id(key_id).map_err(engine_io)?;
		let key_id = key_id.as_bytes().to_vec();
		let key_len = u16::try_from(key_id.len()).map_err(|_| {
			io::Error::new(io::ErrorKind::InvalidInput, "encryption key id is too long")
		})?;
		let nonce_seed = rand::random::<[u8; 16]>();
		inner.write_all(MAGIC)?;
		inner.write_all(&key_len.to_le_bytes())?;
		inner.write_all(&key_id)?;
		inner.write_all(&nonce_seed)?;
		inner.write_all(&(CHUNK_SIZE as u32).to_le_bytes())?;
		Ok(Self {
			inner: Some(inner),
			cipher: XChaCha20Poly1305::new(Key::from_slice(key)),
			key_id,
			nonce_seed,
			counter: 0,
			buffer: Vec::with_capacity(CHUNK_SIZE),
		})
	}

	fn seal_buffer(&mut self, final_chunk: bool) -> io::Result<()> {
		if self.buffer.is_empty() && !final_chunk {
			return Ok(());
		}
		let plain_len = u32::try_from(self.buffer.len())
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "encryption chunk too large"))?;
		let nonce = nonce(self.nonce_seed, self.counter);
		let aad = chunk_aad(&self.key_id, self.counter, plain_len);
		let ciphertext = self
			.cipher
			.encrypt(XNonce::from_slice(&nonce), Payload { msg: &self.buffer, aad: &aad })
			.map_err(|_| {
				io::Error::new(io::ErrorKind::InvalidData, "encrypting archive chunk failed")
			})?;
		let inner = self.inner.as_mut().expect("encrypt writer is unfinished");
		inner.write_all(&plain_len.to_le_bytes())?;
		inner.write_all(&ciphertext)?;
		self.buffer.zeroize();
		self.buffer.clear();
		self.counter = self
			.counter
			.checked_add(1)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "too many encrypted chunks"))?;
		Ok(())
	}

	fn finish(mut self) -> io::Result<W> {
		self.seal_buffer(false)?;
		self.seal_buffer(true)?;
		let mut inner = self.inner.take().expect("encrypt writer is unfinished");
		inner.flush()?;
		Ok(inner)
	}
}

impl<W: Write> Write for EncryptWriter<W> {
	fn write(&mut self, mut bytes: &[u8]) -> io::Result<usize> {
		let original = bytes.len();
		while !bytes.is_empty() {
			let available = CHUNK_SIZE - self.buffer.len();
			let copied = available.min(bytes.len());
			self.buffer.extend_from_slice(&bytes[..copied]);
			bytes = &bytes[copied..];
			if self.buffer.len() == CHUNK_SIZE {
				self.seal_buffer(false)?;
			}
		}
		Ok(original)
	}

	fn flush(&mut self) -> io::Result<()> {
		self.seal_buffer(false)?;
		self
			.inner
			.as_mut()
			.expect("encrypt writer is unfinished")
			.flush()
	}
}

struct DecryptReader<R: Read> {
	inner:      R,
	cipher:     XChaCha20Poly1305,
	key_id:     Vec<u8>,
	nonce_seed: [u8; 16],
	counter:    u64,
	chunk_size: usize,
	buffer:     Vec<u8>,
	offset:     usize,
	finished:   bool,
}

impl<R: Read> DecryptReader<R> {
	fn new(mut inner: R, keyring: &Keyring) -> Result<Self> {
		let mut magic = [0_u8; MAGIC.len()];
		inner.read_exact(&mut magic)?;
		if &magic != MAGIC {
			return Err(EngineError::invalid("not a vmon encrypted archive"));
		}
		let key_len = usize::from(read_u16(&mut inner)?);
		if key_len == 0 || key_len > MAX_KEY_ID {
			return Err(EngineError::invalid("encrypted archive key id is invalid"));
		}
		let mut key_id = vec![0_u8; key_len];
		inner.read_exact(&mut key_id)?;
		let key_id_text = std::str::from_utf8(&key_id)
			.map_err(|_| EngineError::invalid("encrypted archive key id is not UTF-8"))?;
		validate_key_id(key_id_text)?;
		let key = keyring.load(key_id_text)?;
		let mut nonce_seed = [0_u8; 16];
		inner.read_exact(&mut nonce_seed)?;
		let chunk_size = read_u32(&mut inner)? as usize;
		if chunk_size == 0 || chunk_size > CHUNK_SIZE {
			return Err(EngineError::invalid("encrypted archive chunk size is invalid"));
		}
		Ok(Self {
			inner,
			cipher: XChaCha20Poly1305::new(Key::from_slice(&key[..])),
			key_id,
			nonce_seed,
			counter: 0,
			chunk_size,
			buffer: Vec::new(),
			offset: 0,
			finished: false,
		})
	}

	fn read_chunk(&mut self) -> io::Result<()> {
		let plain_len = read_u32_io(&mut self.inner)? as usize;
		if plain_len > self.chunk_size {
			return Err(io::Error::new(io::ErrorKind::InvalidData, "encrypted chunk exceeds limit"));
		}
		let cipher_len = plain_len.checked_add(TAG_SIZE).ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidData, "encrypted chunk length overflow")
		})?;
		let mut ciphertext = vec![0_u8; cipher_len];
		self.inner.read_exact(&mut ciphertext)?;
		let nonce = nonce(self.nonce_seed, self.counter);
		let plain_len_u32 = u32::try_from(plain_len).map_err(|_| {
			io::Error::new(io::ErrorKind::InvalidData, "encrypted chunk length overflow")
		})?;
		let aad = chunk_aad(&self.key_id, self.counter, plain_len_u32);
		self.buffer = self
			.cipher
			.decrypt(XNonce::from_slice(&nonce), Payload { msg: &ciphertext, aad: &aad })
			.map_err(|_| {
				io::Error::new(io::ErrorKind::InvalidData, "encrypted archive authentication failed")
			})?;
		ciphertext.zeroize();
		self.offset = 0;
		self.counter = self
			.counter
			.checked_add(1)
			.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "too many encrypted chunks"))?;
		if plain_len == 0 {
			let mut trailing = [0_u8; 1];
			if self.inner.read(&mut trailing)? != 0 {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					"encrypted archive has trailing data",
				));
			}
			self.finished = true;
		}
		Ok(())
	}
}

impl<R: Read> Read for DecryptReader<R> {
	fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
		if out.is_empty() {
			return Ok(0);
		}
		if self.offset == self.buffer.len() && !self.finished {
			self.buffer.zeroize();
			self.buffer.clear();
			self.read_chunk()?;
		}
		if self.offset == self.buffer.len() {
			return Ok(0);
		}
		let copied = out.len().min(self.buffer.len() - self.offset);
		out[..copied].copy_from_slice(&self.buffer[self.offset..self.offset + copied]);
		self.offset += copied;
		Ok(copied)
	}
}

impl<R: Read> Drop for DecryptReader<R> {
	fn drop(&mut self) {
		self.buffer.zeroize();
	}
}

fn nonce(seed: [u8; 16], counter: u64) -> [u8; 24] {
	let mut nonce = [0_u8; 24];
	nonce[..16].copy_from_slice(&seed);
	nonce[16..].copy_from_slice(&counter.to_le_bytes());
	nonce
}

fn chunk_aad(key_id: &[u8], counter: u64, plain_len: u32) -> Vec<u8> {
	let mut aad = Vec::with_capacity(MAGIC.len() + key_id.len() + 12);
	aad.extend_from_slice(MAGIC);
	aad.extend_from_slice(key_id);
	aad.extend_from_slice(&counter.to_le_bytes());
	aad.extend_from_slice(&plain_len.to_le_bytes());
	aad
}

fn temporary_path(destination: &Path) -> PathBuf {
	let suffix = hex::encode(rand::random::<[u8; 8]>());
	let name = destination
		.file_name()
		.map_or_else(|| "archive".into(), |name| name.to_string_lossy());
	destination.with_file_name(format!(".{name}.{suffix}.tmp"))
}

fn validate_key_id(key_id: &str) -> Result<()> {
	if key_id.is_empty()
		|| key_id.len() > MAX_KEY_ID
		|| !key_id
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
	{
		return Err(EngineError::invalid("encryption key id must match [A-Za-z0-9_.-]{1,128}"));
	}
	Ok(())
}

fn ensure_private_dir(path: &Path) -> Result<()> {
	fs::create_dir_all(path)?;
	let metadata = fs::symlink_metadata(path)?;
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(EngineError::invalid(format!(
			"security path {} must be a directory",
			path.display()
		)));
	}
	fs::set_permissions(path, Permissions::from_mode(DIR_MODE))?;
	Ok(())
}

fn read_u16(reader: &mut impl Read) -> Result<u16> {
	let mut bytes = [0_u8; 2];
	reader.read_exact(&mut bytes)?;
	Ok(u16::from_le_bytes(bytes))
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
	read_u32_io(reader).map_err(EngineError::from)
}

fn read_u32_io(reader: &mut impl Read) -> io::Result<u32> {
	let mut bytes = [0_u8; 4];
	reader.read_exact(&mut bytes)?;
	Ok(u32::from_le_bytes(bytes))
}

fn engine_io(error: EngineError) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

#[cfg(test)]
mod tests {
	use std::{
		fs::{self, OpenOptions},
		io::Write,
		os::unix::fs::OpenOptionsExt,
	};

	use tempfile::TempDir;

	use super::{EncryptedArchive, KEY_MODE, Keyring};
	use crate::home::Home;

	#[test]
	fn encrypted_archive_roundtrip_and_authentication() {
		let temp = TempDir::new().unwrap();
		let home = Home::new(temp.path().join("home"));
		let keyring = Keyring::open(&home).unwrap();
		let source = temp.path().join("source");
		fs::create_dir(&source).unwrap();
		fs::write(source.join("data"), b"secret payload").unwrap();
		let archive = temp.path().join("state.venc");
		EncryptedArchive::seal(&source, &archive, &keyring, "default").unwrap();
		assert!(
			!fs::read(&archive)
				.unwrap()
				.windows(6)
				.any(|window| window == b"secret")
		);

		let opened = EncryptedArchive::open(&archive, &temp.path().join("opened"), &keyring).unwrap();
		assert_eq!(fs::read(opened.join("data")).unwrap(), b"secret payload");

		let mut bytes = fs::read(&archive).unwrap();

		let mut truncated = fs::read(&archive).unwrap();
		truncated.truncate(truncated.len() - 20);
		truncated.extend_from_slice(&0_u32.to_le_bytes());
		let truncated_path = temp.path().join("truncated.venc");
		fs::write(&truncated_path, truncated).unwrap();
		assert!(
			EncryptedArchive::open(&truncated_path, &temp.path().join("truncated"), &keyring).is_err()
		);
		let last = bytes.len() - 5;
		bytes[last] ^= 0x80;
		let tampered = temp.path().join("tampered.venc");
		let mut file = fs::File::create(&tampered).unwrap();
		file.write_all(&bytes).unwrap();
		assert!(EncryptedArchive::open(&tampered, &temp.path().join("bad"), &keyring).is_err());
	}

	#[test]
	fn revoking_customer_key_blocks_decryption() {
		let temp = TempDir::new().unwrap();
		let home = Home::new(temp.path().join("home"));
		let keyring = Keyring::open(&home).unwrap();
		let mut key_file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(KEY_MODE)
			.open(home.keys_dir().join("customer.key"))
			.unwrap();
		writeln!(key_file, "{}", "11".repeat(32)).unwrap();
		let record = temp.path().join("record.venc");
		EncryptedArchive::seal_bytes(b"credential", &record, &keyring, "customer").unwrap();
		fs::remove_file(home.keys_dir().join("customer.key")).unwrap();
		assert!(EncryptedArchive::open_bytes(&record, &keyring, 1024).is_err());
	}
}
