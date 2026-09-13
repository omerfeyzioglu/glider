//! Single-writer durable vectors with exhaustive, deterministic nearest neighbors.
//!
//! The caller must exclusively own the storage namespace until the database drops.
//! The parent directory must exist when opening a local namespace.
//! ```
//! use glider::{Config, Database, Metric, store::LocalStore};
//! # let temp = tempfile::tempdir()?;
//! # let path = temp.path().join("vectors");
//! let config = Config { dimensions: 2, metric: Metric::SquaredEuclidean };
//! let mut db = Database::open(LocalStore::open(&path)?, config)?;
//! db.put(42, vec![1.0, 2.0])?;
//! assert_eq!(db.search(&[1.0, 2.0], 1)?[0].id, 42);
//! drop(db);
//! let mut db = Database::open(LocalStore::open(&path)?, config)?;
//! assert_eq!(db.get(42), Some([1.0, 2.0].as_slice()));
//! db.delete(42)?;
//! # Ok::<(), glider::Error>(())
//! ```
pub mod store;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use store::ObjectStore;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("storage I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("corrupt or unsupported database: {0}")]
    Corrupt(String),
    #[error("object already exists: {0}")]
    Exists(String),
    #[error("write outcome uncertain; reopen the database before writing again")]
    RecoveryRequired,
}

/// Stable persisted metric identifiers; scores use f64 to avoid f32 overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    SquaredEuclidean,
    Manhattan,
}
impl Metric {
    fn score(self, a: &[f32], b: &[f32]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(&a, &b)| {
                let d = f64::from(a) - f64::from(b);
                match self {
                    Self::SquaredEuclidean => d * d,
                    Self::Manhattan => d.abs(),
                }
            })
            .sum()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub dimensions: usize,
    pub metric: Metric,
}
impl Config {
    fn validate(self) -> Result<()> {
        if self.dimensions == 0 {
            return Err(Error::Invalid("dimensions must be positive".into()));
        }
        Ok(())
    }
    fn vector(self, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dimensions || vector.iter().any(|x| !x.is_finite()) {
            return Err(Error::Invalid(
                "vector must have configured dimension and finite components".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct Neighbor {
    pub id: u64,
    pub distance: f64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    version: u32,
    config: Config,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    sequence: u64,
    mutation: Mutation,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum Mutation {
    Put { id: u64, vector: Vec<f32> },
    Delete { id: u64 },
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::Corrupt(e.to_string()))
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::Invalid(e.to_string()))
}
fn key(sequence: u64) -> String {
    format!("mutation-{sequence:020}")
}

/// In-memory state is derived exclusively from immutable, durable log objects.
/// Exclusive namespace ownership is a caller precondition, not a lock service.
pub struct Database<S> {
    store: S,
    config: Config,
    documents: BTreeMap<u64, Vec<f32>>,
    sequence: u64,
    poisoned: bool,
}
impl<S: ObjectStore> Database<S> {
    /// Open/recover, or initialize an empty namespace. Config must match on restart.
    pub fn open(mut store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let mut keys = store.list()?;
        let metadata = store.get("metadata")?;
        match metadata {
            Some(bytes) => {
                let metadata: Metadata = decode(&bytes)?;
                if metadata.version != 1 {
                    return Err(Error::Corrupt("unsupported metadata version".into()));
                }
                metadata
                    .config
                    .validate()
                    .map_err(|e| Error::Corrupt(e.to_string()))?;
                if metadata.config != config {
                    return Err(Error::Invalid(
                        "configuration differs from stored metadata".into(),
                    ));
                }
                if !keys.iter().any(|k| k == "metadata") {
                    return Err(Error::Corrupt("metadata absent from listing".into()));
                }
            }
            None => {
                if !keys.is_empty() {
                    return Err(Error::Corrupt("objects exist without metadata".into()));
                }
                store.create("metadata", &encode(&Metadata { version: 1, config })?)?;
            }
        }
        keys.retain(|k| k != "metadata");
        keys.sort();
        let mut db = Self {
            store,
            config,
            documents: BTreeMap::new(),
            sequence: 0,
            poisoned: false,
        };
        for object in keys {
            let next = db
                .sequence
                .checked_add(1)
                .ok_or_else(|| Error::Corrupt("sequence overflow".into()))?;
            if object != key(next) {
                return Err(Error::Corrupt(format!(
                    "unexpected object or log gap: {object}"
                )));
            }
            let bytes = db
                .store
                .get(&object)?
                .ok_or_else(|| Error::Corrupt(format!("listed object missing: {object}")))?;
            let record: Record = decode(&bytes)?;
            if record.version != 1 || record.sequence != next {
                return Err(Error::Corrupt(format!(
                    "invalid record version or sequence: {object}"
                )));
            }
            if let Mutation::Put { vector, .. } = &record.mutation {
                config
                    .vector(vector)
                    .map_err(|e| Error::Corrupt(e.to_string()))?;
            }
            db.apply(record.mutation);
            db.sequence = next;
        }
        Ok(db)
    }
    pub fn config(&self) -> Config {
        self.config
    }
    pub fn get(&self, id: u64) -> Option<&[f32]> {
        self.documents.get(&id).map(Vec::as_slice)
    }
    pub fn put(&mut self, id: u64, vector: Vec<f32>) -> Result<()> {
        self.config.vector(&vector)?;
        self.commit(Mutation::Put { id, vector })
    }
    /// Deleting an absent ID is an idempotent logical operation, still logged.
    pub fn delete(&mut self, id: u64) -> Result<()> {
        self.commit(Mutation::Delete { id })
    }
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<Neighbor>> {
        self.config.vector(query)?;
        let mut results: Vec<_> = self
            .documents
            .iter()
            .map(|(&id, vector)| Neighbor {
                id,
                distance: self.config.metric.score(query, vector),
            })
            .collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        results.truncate(k);
        Ok(results)
    }
    fn commit(&mut self, mutation: Mutation) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("mutation sequence exhausted".into()))?;
        let record = Record {
            version: 1,
            sequence,
            mutation,
        };
        let bytes = encode(&record)?;
        // Set before entering storage: even a caught backend panic cannot permit reuse.
        self.poisoned = true;
        self.store.create(&key(sequence), &bytes)?;
        self.apply(record.mutation);
        self.sequence = sequence;
        self.poisoned = false;
        Ok(())
    }
    fn apply(&mut self, mutation: Mutation) {
        match mutation {
            Mutation::Put { id, vector } => {
                self.documents.insert(id, vector);
            }
            Mutation::Delete { id } => {
                self.documents.remove(&id);
            }
        }
    }
}
