//! Complete, immutable objects. A store/namespace must have one exclusive owner.
use crate::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

/// Strongly consistent object operations within an exclusively owned namespace.
/// Successful create means durable, complete bytes; errors may have committed.
/// `get` and `list` expose only complete objects. Existing objects cannot change.
/// Listing must be complete, but need not be sorted. Missing/corrupt durable data
/// is an error, never an excuse to serve a partially recovered database.
pub trait ObjectStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn list(&self) -> Result<Vec<String>>;
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()>;
}

/// Development backend; callers must ensure exclusive ownership of this path,
/// including across processes. Requires a filesystem honoring file/directory sync.
pub struct LocalStore {
    root: PathBuf,
}
const MAGIC: &[u8; 8] = b"VTOBJ001";
const SEAL: &[u8; 8] = b"VTSEALED";

impl LocalStore {
    /// Parent must already exist. Creates and synchronizes one namespace directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        match fs::create_dir(&root) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        File::open(&root)?.sync_all()?;
        let parent = root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        let store = Self { root };
        // A previous process may have published a full seal but failed before
        // syncing it. Stabilize that recovered prefix before accepting new writes.
        for key in store.list()? {
            File::open(store.path(&format!("{key}-body"))?)?.sync_all()?;
            File::open(store.path(&format!("{key}-seal"))?)?.sync_all()?;
        }
        File::open(&store.root)?.sync_all()?;
        Ok(store)
    }
    fn path(&self, key: &str) -> Result<PathBuf> {
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return Err(Error::Invalid("invalid object key".into()));
        }
        Ok(self.root.join(key))
    }
}

impl ObjectStore for LocalStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.path(key)?;
        let marker = match fs::read(self.path(&format!("{key}-seal"))?) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if marker.len() < SEAL.len() {
            return Ok(None);
        }
        if marker != SEAL {
            return Err(Error::Corrupt(format!("invalid seal: {key}")));
        }
        let bytes = fs::read(self.path(&format!("{key}-body"))?)?;
        if bytes.len() < 48 || &bytes[..8] != MAGIC {
            return Err(Error::Corrupt(format!("invalid envelope: {key}")));
        }
        let len = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        if len != (bytes.len() - 48) as u64 {
            return Err(Error::Corrupt(format!("invalid length: {key}")));
        }
        let body_end = bytes.len() - 32;
        let digest = Sha256::digest(&bytes[..body_end]);
        if digest[..] != bytes[body_end..body_end + 32] {
            return Err(Error::Corrupt(format!("checksum mismatch: {key}")));
        }
        Ok(Some(bytes[16..body_end].to_vec()))
    }
    fn list(&self) -> Result<Vec<String>> {
        let mut candidates = std::collections::BTreeSet::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Corrupt("non-UTF8 object key".into()))?;
            let key = name
                .strip_suffix("-body")
                .or_else(|| name.strip_suffix("-seal"))
                .ok_or_else(|| Error::Corrupt(format!("unexpected local file: {name}")))?;
            candidates.insert(key.to_owned());
        }
        let mut keys = Vec::new();
        for key in candidates {
            if self.get(&key)?.is_some() {
                keys.push(key);
            }
        }
        Ok(keys)
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if self.get(key)?.is_some() {
            return Err(Error::Exists(key.into()));
        }
        let mut body = Vec::with_capacity(value.len() + 48);
        body.extend_from_slice(MAGIC);
        body.extend_from_slice(&(value.len() as u64).to_le_bytes());
        body.extend_from_slice(value);
        let digest = Sha256::digest(&body);
        body.extend_from_slice(&digest);
        // Truncation only reclaims an unpublished attempt, never a logical object.
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.path(&format!("{key}-body"))?)?;
        file.write_all(&body)?;
        file.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        let mut seal = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.path(&format!("{key}-seal"))?)?;
        seal.write_all(SEAL)?;
        seal.sync_all()?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
}
