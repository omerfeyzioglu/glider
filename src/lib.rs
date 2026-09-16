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
pub mod ivf;
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
fn numbered_sequence(object: &str, prefix: &str) -> Result<u64> {
    let sequence = object
        .strip_prefix(prefix)
        .and_then(|s| s.parse::<u64>().ok());
    match sequence {
        Some(sequence) if object == format!("{prefix}{sequence:020}") => Ok(sequence),
        _ => Err(Error::Corrupt(format!("invalid object key: {object}"))),
    }
}
fn compacted_key(sequence: u64) -> String {
    format!("compacted-{sequence:020}")
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
    compacted_sequence: Option<u64>,
    ivf: Option<ivf::Index>,
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
        let mut latest_compacted = None;
        let mut previous = None;
        for object in &keys {
            if previous == Some(object) {
                return Err(Error::Corrupt(format!("duplicate listed key: {object}")));
            }
            previous = Some(object);
            if object.starts_with("compacted-") {
                latest_compacted = Some(numbered_sequence(object, "compacted-")?);
            } else if object.starts_with("segment-") {
                latest_segment = Some(numbered_sequence(object, "segment-")?);
            } else {
                let sequence = numbered_sequence(object, "mutation-")?;
                if sequence == 0 {
                    return Err(Error::Corrupt("mutation sequence zero".into()));
                }
            }
        }
        // Only a compaction snapshot authorizes missing covered keys. Ordinary
        // M3 snapshots retain their original complete-log validation semantics.
        let floor = latest_compacted.unwrap_or(0);
        let mut last_mutation = floor;
        for object in keys.iter().filter(|k| k.starts_with("mutation-")) {
            let sequence = numbered_sequence(object, "mutation-")?;
            if sequence <= floor {
                continue;
            }
            if last_mutation.checked_add(1) != Some(sequence) {
                return Err(Error::Corrupt(format!("log gap: {object}")));
            }
            last_mutation = sequence;
        }
        if latest_segment.is_some_and(|s| s > last_mutation) {
            return Err(Error::Corrupt(
                "segment extends beyond retained history".into(),
            ));
        }
        let checkpoint_sequence = latest_segment.max(latest_compacted);
        let mut db = Self {
            store,
            config,
            documents: BTreeMap::new(),
            sequence: 0,
            poisoned: false,
            checkpoint_sequence,
            compacted_sequence: latest_compacted,
            ivf: None,
        };
        // Validate the reclamation boundary even if a newer ordinary snapshot
        // supplies the live state. Never fall back from an invalid boundary.
        if let Some(sequence) = latest_compacted {
            db.load_snapshot(&compacted_key(sequence), sequence)?;
        }
        if let Some(sequence) = latest_segment.filter(|s| latest_compacted.is_none_or(|c| *s > c)) {
            db.load_snapshot(&segment_key(sequence), sequence)?;
        }
        for object in keys.into_iter().filter(|k| k.starts_with("mutation-")) {
            if checkpoint_sequence.is_some_and(|sequence| object <= key(sequence)) {
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
    fn load_snapshot(&mut self, object: &str, sequence: u64) -> Result<()> {
        let bytes = self
            .store
            .get(object)?
            .ok_or_else(|| Error::Corrupt(format!("listed snapshot missing: {object}")))?;
        let segment: Segment = decode(&bytes)?;
        if segment.version != 1 || segment.sequence != sequence || segment.config != self.config {
            return Err(Error::Corrupt(
                "invalid snapshot version, sequence or configuration".into(),
            ));
        }
        let mut documents = BTreeMap::new();
        let mut previous_id = None;
        for (id, vector) in segment.documents {
            if previous_id.is_some_and(|previous| previous >= id) {
                return Err(Error::Corrupt(
                    "snapshot IDs must be strictly increasing".into(),
                ));
            }
            self.config
                .vector(&vector)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
            previous_id = Some(id);
            documents.insert(id, vector);
        }
        self.documents = documents;
        self.sequence = sequence;
        Ok(())
    }
    fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        encode(&Segment {
            version: 1,
            sequence: self.sequence,
            config: self.config,
            documents: self
                .documents
                .iter()
                .map(|(&id, v)| (id, v.clone()))
                .collect(),
        })
    }
    /// Consolidate live state into a durable snapshot, then reclaim covered logs
    /// and older snapshots. Success acknowledges publication and all listed
    /// removals. On any storage error/panic, reopen before further writes or
    /// maintenance; already removed objects were covered by a durable snapshot.
    /// A repeated call at the same sequence resumes cleanup without republishing.
    pub fn compact(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.compacted_sequence != Some(self.sequence) {
            let bytes = self.snapshot_bytes()?;
            self.poisoned = true;
            self.store.create(&compacted_key(self.sequence), &bytes)?;
            self.compacted_sequence = Some(self.sequence);
            self.checkpoint_sequence = Some(self.sequence);
        }
        self.poisoned = true;
        // Build and validate the full deletion plan before removing anything.
        let mut keys = self.store.list()?;
        keys.sort();
        if keys.windows(2).any(|pair| pair[0] == pair[1])
            || !keys.iter().any(|k| k == "metadata")
            || !keys.contains(&compacted_key(self.sequence))
        {
            return Err(Error::Corrupt("invalid listing during compaction".into()));
        }
        let mut obsolete = Vec::new();
        for object in keys {
            if object == "metadata" {
                continue;
            }
            let (prefix, inclusive) = if object.starts_with("compacted-") {
                ("compacted-", false)
            } else if object.starts_with("segment-") {
                ("segment-", true)
            } else {
                ("mutation-", true)
            };
            let sequence = numbered_sequence(&object, prefix)?;
            if sequence > self.sequence || (prefix == "mutation-" && sequence == 0) {
                return Err(Error::Corrupt(format!(
                    "unexpected object during compaction: {object}"
                )));
            }
            if sequence < self.sequence || inclusive {
                obsolete.push(object);
            }
        }
        for object in obsolete {
            self.store.remove(&object)?;
        }
        self.poisoned = false;
        Ok(())
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
        let bytes = self.snapshot_bytes()?;
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
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut results: Vec<_> = self
            .documents
            .iter()
            .map(|(&id, vector)| Neighbor {
                id,
                distance: self.config.metric.score(query, vector),
            })
            .collect();
        let order =
            |a: &Neighbor, b: &Neighbor| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id));
        if k < results.len() {
            // The total (distance, ID) order makes cutoff ties deterministic.
            results.select_nth_unstable_by(k, order);
            results.truncate(k);
        }
        results.sort_by(order);
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
        self.ivf = None;
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
