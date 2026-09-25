//! Object-store-safe exclusive ownership for a deployed single writer.
//!
//! A claim survives process death. Operators clear a stale claim only after
//! verifying the previous process can no longer write. Dropping a handle does
//! not acknowledge release; call [`OwnedDatabase::close`] for graceful shutdown.
use crate::{store::ObjectStore, Config, Database, Error, Result};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};

const ROOT: &str = "owner-root-v1";
const PREFIX: &str = "owner-v1-";
const ROOT_BYTES: &[u8] = b"{\"version\":1}";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    version: u8,
    token: String,
}

fn valid_claim_key(key: &str) -> bool {
    key.strip_prefix(PREFIX).is_some_and(|token| {
        token.len() == 32
            && token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

pub(crate) fn is_control_key(key: &str) -> bool {
    key == ROOT || valid_claim_key(key)
}

fn validate_root<S: ObjectStore>(store: &S) -> Result<()> {
    let bytes = store
        .get(ROOT)?
        .ok_or_else(|| Error::Corrupt("ownership root missing".into()))?;
    if bytes != ROOT_BYTES {
        return Err(Error::Corrupt("invalid ownership root version".into()));
    }
    Ok(())
}

fn listed_claim_keys<S: ObjectStore>(store: &S) -> Result<Vec<String>> {
    validate_root(store)?;
    let keys = store.list()?;
    if keys.iter().filter(|key| *key == ROOT).count() != 1 {
        return Err(Error::Corrupt("ownership root missing from listing".into()));
    }
    let mut owners = Vec::new();
    for key in keys {
        if key.starts_with(PREFIX) {
            if !valid_claim_key(&key) {
                return Err(Error::Corrupt(format!("invalid owner claim key: {key}")));
            }
            owners.push(key);
        }
    }
    owners.sort();
    if owners.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::Corrupt("duplicate owner claim in listing".into()));
    }
    Ok(owners)
}

/// Published claims visible in a strongly consistent listing. A surviving
/// claim blocks takeover, even if its original process crashed. This also
/// validates the persisted version and identity of each listed claim.
pub fn claims<S: ObjectStore>(store: &S) -> Result<Vec<String>> {
    let owners = listed_claim_keys(store)?;
    for key in &owners {
        let bytes = store
            .get(key)?
            .ok_or_else(|| Error::Corrupt(format!("listed owner claim missing: {key}")))?;
        let marker: Marker = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Corrupt(format!("invalid owner claim: {e}")))?;
        if marker.version != 1 || marker.token != key[PREFIX.len()..] {
            return Err(Error::Corrupt(format!(
                "invalid owner claim identity: {key}"
            )));
        }
    }
    Ok(owners)
}

/// Manually release a claim after proving that its process is stopped. This
/// method never guesses which claim is stale. A failed removal is uncertain:
/// reopen the store and inspect claims again before attempting ownership.
pub fn clear_stale_claim<S: ObjectStore>(store: &mut S, key: &str) -> Result<()> {
    if !valid_claim_key(key) || !listed_claim_keys(store)?.iter().any(|claim| claim == key) {
        return Err(Error::Invalid("expected a listed owner claim key".into()));
    }
    store.remove(key)
}

/// Store view that hides ownership control objects from database recovery and
/// maintenance. Construct it through [`OwnedDatabase::open`].
pub struct OwnedStore<S> {
    inner: S,
    key: String,
}

impl<S: ObjectStore> OwnedStore<S> {
    fn claim(mut inner: S) -> Result<Self> {
        match inner.get(ROOT)? {
            Some(_) => validate_root(&inner)?,
            None => inner.create(ROOT, ROOT_BYTES)?,
        }
        let mut nonce = [0_u8; 16];
        getrandom::getrandom(&mut nonce).map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "OS randomness unavailable: {e}"
            )))
        })?;
        let key = format!(
            "{PREFIX}{}",
            nonce.iter().map(|b| format!("{b:02x}")).collect::<String>()
        );
        let marker = serde_json::to_vec(&Marker {
            version: 1,
            token: key[PREFIX.len()..].to_owned(),
        })
        .map_err(|e| Error::Invalid(e.to_string()))?;
        inner.create(&key, &marker)?;
        let owners = listed_claim_keys(&inner)?;
        if !owners.iter().any(|owner| owner == &key) {
            return Err(Error::Corrupt(
                "published owner claim missing from listing".into(),
            ));
        }
        if owners.len() != 1 || owners[0] != key {
            inner.remove(&key)?;
            return Err(Error::Busy(format!(
                "{} other claim(s)",
                owners.len().saturating_sub(1)
            )));
        }
        Ok(Self { inner, key })
    }

    fn release(mut self) -> Result<()> {
        self.inner.remove(&self.key)
    }
}

impl<S: ObjectStore> ObjectStore for OwnedStore<S> {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self
            .inner
            .list()?
            .into_iter()
            .filter(|key| !is_control_key(key))
            .collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.inner.create(key, value)
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

/// A single mutable database whose owner claim is enforced by conditional
/// creation and strongly consistent listing. Multiple claimants may cause all
/// of them to fail, but two claimants cannot both become active. Legacy raw
/// `Database::open` rejects a claimed namespace's ownership keys.
pub struct OwnedDatabase<S: ObjectStore> {
    db: Database<OwnedStore<S>>,
}

impl<S: ObjectStore> OwnedDatabase<S> {
    /// Establish ownership before reading or initializing database state.
    /// A failed claim may leave an orphan marker; use `claims` and the recovery
    /// procedure before clearing one. Existing unclaimed namespaces must have
    /// all old writer processes stopped before the first owned open.
    pub fn open(store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let store = OwnedStore::claim(store)?;
        Ok(Self {
            db: Database::open(store, config)?,
        })
    }

    /// Stop all writes and durably remove this handle's owner marker. Failure
    /// is uncertain and requires a fresh inspection before another owner opens.
    pub fn close(self) -> Result<()> {
        self.db.into_store().release()
    }
}

impl<S: ObjectStore> Deref for OwnedDatabase<S> {
    type Target = Database<OwnedStore<S>>;
    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

impl<S: ObjectStore> DerefMut for OwnedDatabase<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.db
    }
}
