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
    #[error("write outcome uncertain; reopen storage and the database before writing again")]
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
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Segment {
    version: u32,
    sequence: u64,
    config: Config,
    // A sorted array, not a JSON map: duplicate IDs must be rejected on decode.
    documents: Vec<(u64, Vec<f32>)>,
}
fn segment_key(sequence: u64) -> String {
    format!("segment-{sequence:020}")
}
fn segment_sequence(key: &str) -> Result<u64> {
    let sequence = key
        .strip_prefix("segment-")
        .and_then(|s| s.parse::<u64>().ok());
    match sequence {
        Some(sequence) if key == segment_key(sequence) => Ok(sequence),
        _ => Err(Error::Corrupt(format!("invalid segment key: {key}"))),
    }
}
fn key(sequence: u64) -> String {
    format!("mutation-{sequence:020}")
}

/// In-memory state is derived from immutable segments and durable mutation objects.
/// Exclusive namespace ownership is a caller precondition, not a lock service.
pub struct Database<S> {
    store: S,
    config: Config,
    documents: BTreeMap<u64, Vec<f32>>,
    sequence: u64,
    poisoned: bool,
    checkpoint_sequence: Option<u64>,
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
                if keys.iter().filter(|k| *k == "metadata").count() != 1 {
                    return Err(Error::Corrupt(
                        "metadata missing or duplicated in listing".into(),
                    ));
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
        let mut latest_segment = None;
        let mut last_mutation = 0_u64;
        let mut previous = None;
        // M3 retains the entire log. Validate its key continuity even when a
        // segment covers its payloads; deletion/garbage collection belongs to M4.
        for object in &keys {
            if previous == Some(object) {
                return Err(Error::Corrupt(format!("duplicate listed key: {object}")));
            }
            previous = Some(object);
            if object.starts_with("segment-") {
                latest_segment = Some(segment_sequence(object)?);
            } else {
                let next = last_mutation
                    .checked_add(1)
                    .ok_or_else(|| Error::Corrupt("sequence overflow".into()))?;
                if *object != key(next) {
                    return Err(Error::Corrupt(format!(
                        "unexpected object or log gap: {object}"
                    )));
                }
                last_mutation = next;
            }
        }
        if latest_segment.is_some_and(|s| s > last_mutation) {
            return Err(Error::Corrupt("segment extends beyond retained log".into()));
        }
        let mut db = Self {
            store,
            config,
            documents: BTreeMap::new(),
            sequence: 0,
            poisoned: false,
            checkpoint_sequence: latest_segment,
        };
        if let Some(sequence) = latest_segment {
            let object = segment_key(sequence);
            let bytes = db
                .store
                .get(&object)?
                .ok_or_else(|| Error::Corrupt(format!("listed segment missing: {object}")))?;
            let segment: Segment = decode(&bytes)?;
            if segment.version != 1 || segment.sequence != sequence || segment.config != config {
                return Err(Error::Corrupt(
                    "invalid segment version, sequence or configuration".into(),
                ));
            }
            let mut previous_id = None;
            for (id, vector) in segment.documents {
                if previous_id.is_some_and(|previous| previous >= id) {
                    return Err(Error::Corrupt(
                        "segment IDs must be strictly increasing".into(),
                    ));
                }
                config
                    .vector(&vector)
                    .map_err(|e| Error::Corrupt(e.to_string()))?;
                previous_id = Some(id);
                db.documents.insert(id, vector);
            }
            db.sequence = sequence;
        }
        for object in keys.into_iter().filter(|k| k.starts_with("mutation-")) {
            if latest_segment.is_some_and(|sequence| object <= key(sequence)) {
                continue;
            }
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
    /// Persist a complete immutable snapshot at the current mutation sequence.
    /// Success means durable publication; errors/panics poison further writes and
    /// checkpoints until reopen. Reads retain their acknowledged state. Existing
    /// logs/segments are retained. Repeating at an already checkpointed sequence
    /// is a no-op. Checkpointing does not allocate a mutation sequence.
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.checkpoint_sequence == Some(self.sequence) {
            return Ok(());
        }
        let bytes = encode(&Segment {
            version: 1,
            sequence: self.sequence,
            config: self.config,
            documents: self
                .documents
                .iter()
                .map(|(&id, vector)| (id, vector.clone()))
                .collect(),
        })?;
        self.poisoned = true;
        self.store.create(&segment_key(self.sequence), &bytes)?;
        self.checkpoint_sequence = Some(self.sequence);
        self.poisoned = false;
        Ok(())
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
