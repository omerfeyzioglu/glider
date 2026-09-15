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
/// `get` and `list` expose only complete, durable objects. After an uncertain
/// create, a backend must stabilize visible objects or reject access until reopened.
/// Existing objects cannot change.
/// Listing must be complete, but need not be sorted. Missing/corrupt durable data
/// is an error, never an excuse to serve a partially recovered database.
pub trait ObjectStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    fn list(&self) -> Result<Vec<String>>;
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()>;
    /// Durably remove an object; absence is success. Errors may have removed it.
    /// After uncertainty, stabilize or reject access until reopened. The engine
    /// must never reuse reclaimed keys: an old remote DELETE may arrive late.
    fn remove(&mut self, key: &str) -> Result<()>;
}

/// Development backend; callers must ensure exclusive ownership of this path,
/// including across processes. Requires a filesystem honoring file/directory sync.
/// After a publication error or panic, discard this handle and call `open` again;
/// all object operations reject access until a fresh handle stabilizes recovery.
pub struct LocalStore {
    root: PathBuf,
    poisoned: bool,
    #[cfg(test)]
    fault: std::rc::Rc<std::cell::RefCell<tests::Fault>>,
}
const MAGIC: &[u8; 8] = b"VTOBJ001";
const SEAL: &[u8; 8] = b"VTSEALED";

impl LocalStore {
    /// Parent must already exist. Creates and synchronizes one namespace directory.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_inner(
            path.as_ref(),
            #[cfg(test)]
            Default::default(),
        )
    }
    fn open_inner(
        path: &Path,
        #[cfg(test)] fault: std::rc::Rc<std::cell::RefCell<tests::Fault>>,
    ) -> Result<Self> {
        let root = path.to_path_buf();
        let store = Self {
            root,
            poisoned: false,
            #[cfg(test)]
            fault,
        };
        let root = &store.root;
        match fs::create_dir(root) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        store.io("open-directory-sync", || File::open(root)?.sync_all())?;
        let parent = root
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        store.io("open-parent-sync", || File::open(parent)?.sync_all())?;
        // A previous process may have published a full seal but failed before
        // syncing it. Stabilize that recovered prefix before accepting new writes.
        for key in store.candidates()? {
            if store.get(&key)?.is_none() {
                // Includes interrupted creates and seal-first deletes. Otherwise
                // invisible orphan bodies would accumulate after failed cleanup.
                store.remove_files(&key)?;
                continue;
            }
            let body = store.path(&format!("{key}-body"))?;
            let seal = store.path(&format!("{key}-seal"))?;
            store.io("recover-body-sync", || File::open(body)?.sync_all())?;
            store.io("recover-seal-sync", || File::open(seal)?.sync_all())?;
        }
        store.io("recover-directory-sync", || {
            File::open(&store.root)?.sync_all()
        })?;
        Ok(store)
    }
    fn candidates(&self) -> Result<std::collections::BTreeSet<String>> {
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
        Ok(candidates)
    }
    fn remove_files(&self, key: &str) -> Result<()> {
        let seal = self.path(&format!("{key}-seal"))?;
        let body = self.path(&format!("{key}-body"))?;
        self.io("remove-seal", || remove_if_present(&seal))?;
        // A durable seal removal must precede body removal; otherwise a crash
        // could leave a published seal pointing to a missing body.
        self.io("remove-seal-directory-sync", || {
            File::open(&self.root)?.sync_all()
        })?;
        self.io("remove-body", || remove_if_present(&body))?;
        self.io("remove-body-directory-sync", || {
            File::open(&self.root)?.sync_all()
        })?;
        Ok(())
    }
    fn ready(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    // All injected errors surround real filesystem operations. Hooks are per-store
    // and test-only; they do not change the production storage interface.
    fn io<T>(&self, _name: &str, operation: impl FnOnce() -> std::io::Result<T>) -> Result<T> {
        #[cfg(test)]
        self.fault.borrow_mut().hit(&format!("{_name}-before"))?;
        let value = operation()?;
        #[cfg(test)]
        self.fault.borrow_mut().hit(&format!("{_name}-after"))?;
        Ok(value)
    }
    fn write(&self, name: &str, file: &mut File, bytes: &[u8]) -> Result<()> {
        self.io(name, || {
            #[cfg(test)]
            {
                file.write_all(&bytes[..1])?;
                self.fault.borrow_mut().hit(&format!("{name}-partial"))?;
                file.write_all(&bytes[1..])
            }
            #[cfg(not(test))]
            file.write_all(bytes)
        })
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
        self.ready()?;
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
        decode_envelope(&bytes, key).map(Some)
    }
    fn list(&self) -> Result<Vec<String>> {
        self.ready()?;
        let candidates = self.candidates()?;
        let mut keys = Vec::new();
        for key in candidates {
            if self.get(&key)?.is_some() {
                keys.push(key);
            }
        }
        Ok(keys)
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.ready()?;
        self.path(key)?;
        self.poisoned = true;
        self.remove_files(key)?;
        self.poisoned = false;
        Ok(())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if self.get(key)?.is_some() {
            return Err(Error::Exists(key.into()));
        }
        let body = encode_envelope(value);
        // Poison before any filesystem mutation, including a caught panic. Only a
        // fresh open may validate and stabilize an uncertain publication.
        self.poisoned = true;
        // Truncation only reclaims an unpublished attempt, never a logical object.
        let body_path = self.path(&format!("{key}-body"))?;
        let mut file = self.io("body-create", || {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(body_path)
        })?;
        self.write("body-write", &mut file, &body)?;
        self.io("body-sync", || file.sync_all())?;
        self.io("body-directory-sync", || File::open(&self.root)?.sync_all())?;
        let seal_path = self.path(&format!("{key}-seal"))?;
        let mut seal = self.io("seal-create", || {
            OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(seal_path)
        })?;
        self.write("seal-write", &mut seal, SEAL)?;
        self.io("seal-sync", || seal.sync_all())?;
        self.io("seal-directory-sync", || File::open(&self.root)?.sync_all())?;
        self.poisoned = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

fn encode_envelope(value: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(value.len() + 48);
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&(value.len() as u64).to_le_bytes());
    body.extend_from_slice(value);
    let digest = Sha256::digest(&body);
    body.extend_from_slice(&digest);
    body
}

fn decode_envelope(bytes: &[u8], key: &str) -> Result<Vec<u8>> {
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
    Ok(bytes[16..body_end].to_vec())
}

#[cfg(feature = "s3")]
pub mod s3;

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}
