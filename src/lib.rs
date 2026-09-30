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
pub mod admission;
pub mod ivf;
pub mod ownership;
pub mod recovery;
pub mod retry;
#[cfg(any(test, feature = "experimental-segmented"))]
pub mod segmented;
pub mod serving;
pub mod store;
pub mod streaming;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
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
    #[error("storage namespace already has an owner: {0}")]
    Busy(String),
    #[error("maintenance required before more writes; compact the database")]
    MaintenanceRequired,
    #[error("request ID reused with a different payload")]
    RequestConflict,
    #[error("request ID expired; its outcome is no longer resolvable")]
    RequestExpired,
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

/// Optional write-side bounds for a single-machine deployment. Soft limits
/// request maintenance; hard limits reject new mutations before publication.
#[derive(Debug, Clone, Copy)]
pub struct MaintenanceLimits {
    pub soft_tail_objects: u64,
    pub hard_tail_objects: u64,
    pub soft_visible_objects: usize,
    pub hard_visible_objects: usize,
    pub max_batch_mutations: usize,
}

impl MaintenanceLimits {
    /// M8's 2,000-row, 64-dimension initial envelope. The tail leaves room for
    /// a selected chunked snapshot within the 64-GET recovery budget.
    pub const fn m8() -> Self {
        Self {
            soft_tail_objects: 16,
            hard_tail_objects: 24,
            soft_visible_objects: 64,
            hard_visible_objects: 96,
            max_batch_mutations: 100,
        }
    }
    fn validate(self) -> Result<()> {
        if self.soft_tail_objects == 0
            || self.soft_tail_objects > self.hard_tail_objects
            || self.soft_visible_objects == 0
            || self.soft_visible_objects > self.hard_visible_objects
            || self.max_batch_mutations == 0
        {
            return Err(Error::Invalid("invalid maintenance limits".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MaintenanceStatus {
    pub sequence: u64,
    pub tail_objects: u64,
    pub visible_objects: usize,
    pub should_compact: bool,
    pub writes_blocked: bool,
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
#[serde(deny_unknown_fields)]
struct BatchRecord {
    version: u32,
    sequence: u64,
    mutations: Vec<Mutation>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordV1 {
    version: u32,
    sequence: u64,
    mutation: MutationV1,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum MutationV1 {
    Put { id: u64, vector: Vec<f32> },
    Delete { id: u64 },
}
/// One ordered write inside an atomic batch. Later operations on the same ID win.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Put {
        id: u64,
        vector: Vec<f32>,
        #[serde(deserialize_with = "deserialize_metadata")]
        metadata: BTreeMap<String, String>,
    },
    Delete {
        id: u64,
    },
}
#[derive(Deserialize)]
struct Version {
    version: u32,
}
#[derive(Deserialize)]
struct SnapshotHeader {
    version: u32,
    sequence: u64,
    config: Config,
}
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|e| Error::Corrupt(e.to_string()))
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::Invalid(e.to_string()))
}
/// The existing snapshot schema accepts JSON numbers as f32. Omit the redundant
/// decimal suffix for exactly represented small integers, preserving negative
/// zero and the standard representation of every other value. This formatter
/// is deliberately NOT used for retry identity, log records or chunk byte reuse.
/// Segmented blocks also use it; their digests cover whatever bytes were written.
struct SnapshotNumbers;
impl serde_json::ser::Formatter for SnapshotNumbers {
    fn write_f32<W: std::io::Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: f32,
    ) -> std::io::Result<()> {
        let integer = value as i32;
        if (-16_777_216.0..=16_777_216.0).contains(&value)
            && integer as f32 == value
            && !(value == 0.0 && value.is_sign_negative())
        {
            serde_json::ser::CompactFormatter.write_i32(writer, integer)
        } else {
            serde_json::ser::CompactFormatter.write_f32(writer, value)
        }
    }
}
/// JSON with `SnapshotNumbers`: same schema and decoded values as `encode`.
fn encode_compact<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    value
        .serialize(&mut serde_json::Serializer::with_formatter(
            &mut bytes,
            SnapshotNumbers,
        ))
        .map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(bytes)
}
struct CountBytes(usize);
impl std::io::Write for CountBytes {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("serialized size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded_len<T: Serialize>(value: &T) -> Result<usize> {
    let mut counter = CountBytes(0);
    serde_json::to_writer(&mut counter, value).map_err(|e| Error::Invalid(e.to_string()))?;
    Ok(counter.0)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Segment {
    version: u32,
    sequence: u64,
    config: Config,
    // A sorted array, not a JSON map: duplicate IDs must be rejected on decode.
    documents: Vec<(u64, Document)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry: Option<retry::State>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentV1 {
    version: u32,
    sequence: u64,
    config: Config,
    documents: Vec<(u64, Vec<f32>)>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotManifest {
    version: u32,
    sequence: u64,
    config: Config,
    max_chunk_bytes: usize,
    chunks: Vec<ChunkRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry: Option<retry::State>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkRef {
    first_id: u64,
    last_id: u64,
    rows: usize,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotChunk {
    version: u32,
    sequence: u64,
    config: Config,
    documents: Vec<(u64, Document)>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    vector: Vec<f32>,
    #[serde(deserialize_with = "deserialize_metadata")]
    metadata: BTreeMap<String, String>,
}

fn deserialize_metadata<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct UniqueMetadata;
    impl<'de> serde::de::Visitor<'de> for UniqueMetadata {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string-to-string metadata map with unique keys")
        }

        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut metadata = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, String>()? {
                if metadata.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate metadata key"));
                }
            }
            Ok(metadata)
        }
    }
    deserializer.deserialize_map(UniqueMetadata)
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
fn chunk_key(kind: &str, sequence: u64, max_bytes: usize, ordinal: usize) -> String {
    format!("{kind}chunk-{sequence:020}-{max_bytes:020}-{ordinal:010}")
}
fn chunk_sequence(object: &str, kind: &str) -> Result<u64> {
    let prefix = format!("{kind}chunk-");
    let (number, rest) = object
        .strip_prefix(&prefix)
        .and_then(|suffix| suffix.split_once('-'))
        .ok_or_else(|| Error::Corrupt(format!("invalid object key: {object}")))?;
    let (limit, ordinal) = rest
        .split_once('-')
        .ok_or_else(|| Error::Corrupt(format!("invalid object key: {object}")))?;
    let sequence = number
        .parse::<u64>()
        .map_err(|_| Error::Corrupt(format!("invalid object key: {object}")))?;
    let max_bytes = limit
        .parse::<usize>()
        .map_err(|_| Error::Corrupt(format!("invalid object key: {object}")))?;
    let index = ordinal
        .parse::<usize>()
        .map_err(|_| Error::Corrupt(format!("invalid object key: {object}")))?;
    if object != chunk_key(kind, sequence, max_bytes, index)
        || limit.len() != 20
        || ordinal.len() != 10
        || max_bytes == 0
    {
        return Err(Error::Corrupt(format!("invalid object key: {object}")));
    }
    Ok(sequence)
}
fn key(sequence: u64) -> String {
    format!("mutation-{sequence:020}")
}

fn parse_chunked_manifest(bytes: &[u8], sequence: u64, config: Config) -> Result<SnapshotManifest> {
    let manifest: SnapshotManifest = decode(bytes)?;
    if !matches!(manifest.version, 3 | 5)
        || manifest.sequence != sequence
        || manifest.config != config
        || manifest.max_chunk_bytes == 0
    {
        return Err(Error::Corrupt("invalid chunked snapshot identity".into()));
    }
    validate_retry_snapshot(manifest.version, manifest.retry.as_ref(), sequence)?;
    let mut previous_id = None;
    for reference in &manifest.chunks {
        if reference.rows == 0
            || reference.first_id > reference.last_id
            || previous_id.is_some_and(|previous| previous >= reference.first_id)
        {
            return Err(Error::Corrupt("invalid snapshot chunk range".into()));
        }
        previous_id = Some(reference.last_id);
    }
    Ok(manifest)
}

fn read_snapshot_chunk<S: ObjectStore>(
    store: &S,
    config: Config,
    kind: &str,
    manifest: &SnapshotManifest,
    ordinal: usize,
) -> Result<SnapshotChunk> {
    let reference = &manifest.chunks[ordinal];
    let key = chunk_key(kind, manifest.sequence, manifest.max_chunk_bytes, ordinal);
    let bytes = store
        .get(&key)?
        .ok_or_else(|| Error::Corrupt(format!("snapshot chunk missing: {key}")))?;
    if bytes.len() > manifest.max_chunk_bytes {
        return Err(Error::Corrupt(format!("oversized snapshot chunk: {key}")));
    }
    if format!("{:x}", Sha256::digest(&bytes)) != reference.sha256 {
        return Err(Error::Corrupt(format!(
            "snapshot chunk digest mismatch: {key}"
        )));
    }
    let chunk: SnapshotChunk = decode(&bytes)?;
    if chunk.version != 1
        || chunk.sequence != manifest.sequence
        || chunk.config != config
        || chunk.documents.len() != reference.rows
        || chunk.documents.first().map(|entry| entry.0) != Some(reference.first_id)
        || chunk.documents.last().map(|entry| entry.0) != Some(reference.last_id)
    {
        return Err(Error::Corrupt(format!("invalid snapshot chunk: {key}")));
    }
    let mut previous_id = None;
    for (id, document) in &chunk.documents {
        if previous_id.is_some_and(|previous| previous >= *id) {
            return Err(Error::Corrupt(
                "snapshot IDs must be strictly increasing".into(),
            ));
        }
        config
            .vector(&document.vector)
            .map_err(|e| Error::Corrupt(e.to_string()))?;
        previous_id = Some(*id);
    }
    Ok(chunk)
}

fn scan_snapshot<S: ObjectStore>(
    store: &S,
    config: Config,
    kind: &str,
    manifest: &SnapshotManifest,
    mut visit: impl FnMut(u64, Document) -> Result<()>,
) -> Result<()> {
    for ordinal in 0..manifest.chunks.len() {
        let chunk = read_snapshot_chunk(store, config, kind, manifest, ordinal)?;
        for (id, document) in chunk.documents {
            visit(id, document)?;
        }
    }
    Ok(())
}

struct Catalog {
    keys: Vec<String>,
    latest_segment: Option<u64>,
    latest_compacted: Option<u64>,
    checkpoint_sequence: Option<u64>,
    last_mutation: u64,
}

fn inspect_namespace<S: ObjectStore>(
    store: &mut S,
    config: Config,
    initialize: bool,
) -> Result<Catalog> {
    config.validate()?;
    let mut keys = store.list()?;
    match store.get("metadata")? {
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
            if !initialize {
                return Err(Error::Invalid(
                    "streaming reader requires an initialized namespace".into(),
                ));
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
        } else if object.starts_with("compactedchunk-") {
            chunk_sequence(object, "compacted")?;
        } else if object.starts_with("segmentchunk-") {
            chunk_sequence(object, "segment")?;
        } else if object.starts_with("ivf-") {
            ivf::cache_sequence(object)?;
        } else {
            let sequence = numbered_sequence(object, "mutation-")?;
            if sequence == 0 {
                return Err(Error::Corrupt("mutation sequence zero".into()));
            }
        }
    }
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
    for object in keys.iter().filter(|k| k.starts_with("ivf-")) {
        if ivf::cache_sequence(object)? > last_mutation {
            return Err(Error::Corrupt(format!(
                "IVF cache extends beyond retained history: {object}"
            )));
        }
    }
    for object in keys
        .iter()
        .filter(|k| k.starts_with("segmentchunk-") || k.starts_with("compactedchunk-"))
    {
        let kind = if object.starts_with("segmentchunk-") {
            "segment"
        } else {
            "compacted"
        };
        if chunk_sequence(object, kind)? > last_mutation {
            return Err(Error::Corrupt(format!(
                "snapshot chunk extends beyond retained history: {object}"
            )));
        }
    }
    if latest_segment.is_some_and(|s| s > last_mutation) {
        return Err(Error::Corrupt(
            "segment extends beyond retained history".into(),
        ));
    }
    Ok(Catalog {
        keys,
        latest_segment,
        latest_compacted,
        checkpoint_sequence: latest_segment.max(latest_compacted),
        last_mutation,
    })
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestRecord {
    version: u32,
    sequence: u64,
    request: retry::Request,
    outcome: retry::Outcome,
}

fn validate_retry_snapshot(
    version: u32,
    state: Option<&retry::State>,
    sequence: u64,
) -> Result<()> {
    match (version, state) {
        (4 | 5, Some(state)) => state.validate(sequence),
        (1..=3, None) => Ok(()),
        _ => Err(Error::Corrupt(
            "snapshot missing or unexpected retry state".into(),
        )),
    }
}

fn read_mutations<S: ObjectStore>(
    store: &S,
    object: &str,
    next: u64,
    config: Config,
    state: &mut retry::State,
) -> Result<Vec<Mutation>> {
    let bytes = store
        .get(object)?
        .ok_or_else(|| Error::Corrupt(format!("listed object missing: {object}")))?;
    if decode::<Version>(&bytes)?.version == 4 {
        let record: RequestRecord = decode(&bytes)?;
        record
            .request
            .validate(config)
            .map_err(|e| Error::Corrupt(e.to_string()))?;
        if record.sequence != next
            || record.outcome.sequence != next
            || state
                .duplicate(&record.request, next - 1)
                .map_err(|e| Error::Corrupt(e.to_string()))?
                .is_some()
            || state.decide(&record.request, next) != record.outcome
        {
            return Err(Error::Corrupt("invalid durable request decision".into()));
        }
        let mutations = if record.outcome.conflict.is_none() {
            record.request.mutations.clone()
        } else {
            Vec::new()
        };
        state.advance(next, &mutations);
        state.retain(&record.request, record.outcome)?;
        return Ok(mutations);
    }
    let (record_sequence, mutations) = match decode::<Version>(&bytes)?.version {
        1 => {
            let old: RecordV1 = decode(&bytes)?;
            if old.version != 1 {
                return Err(Error::Corrupt(format!("invalid record version: {object}")));
            }
            let mutation = match old.mutation {
                MutationV1::Put { id, vector } => Mutation::Put {
                    id,
                    vector,
                    metadata: BTreeMap::new(),
                },
                MutationV1::Delete { id } => Mutation::Delete { id },
            };
            (old.sequence, vec![mutation])
        }
        2 => {
            let record: Record = decode(&bytes)?;
            (record.sequence, vec![record.mutation])
        }
        3 => {
            let record: BatchRecord = decode(&bytes)?;
            if record.mutations.is_empty() {
                return Err(Error::Corrupt(format!("empty batch record: {object}")));
            }
            (record.sequence, record.mutations)
        }
        _ => {
            return Err(Error::Corrupt(format!(
                "unsupported record version: {object}"
            )))
        }
    };
    if record_sequence != next {
        return Err(Error::Corrupt(format!("invalid record sequence: {object}")));
    }
    for mutation in &mutations {
        if let Mutation::Put { vector, .. } = mutation {
            config
                .vector(vector)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
        }
    }
    state.advance(next, &mutations);
    Ok(mutations)
}

/// In-memory state is derived from immutable segments and durable mutation objects.
/// Exclusive namespace ownership is a caller precondition, not a lock service.
pub struct Database<S> {
    store: S,
    config: Config,
    documents: BTreeMap<u64, Document>,
    sequence: u64,
    poisoned: bool,
    checkpoint_sequence: Option<u64>,
    compacted_sequence: Option<u64>,
    ivf: Option<ivf::Index>,
    visible_objects: usize,
    maintenance_limits: Option<MaintenanceLimits>,
    retry: retry::State,
}
impl<S: ObjectStore> Database<S> {
    /// Open/recover, or initialize an empty namespace. Config must match on restart.
    pub fn open(mut store: S, config: Config) -> Result<Self> {
        let catalog = inspect_namespace(&mut store, config, true)?;
        let Catalog {
            keys,
            latest_segment,
            latest_compacted,
            checkpoint_sequence,
            ..
        } = catalog;
        let visible_objects = keys.len() + 1; // metadata is omitted from Catalog::keys.
        let mut db = Self {
            store,
            config,
            documents: BTreeMap::new(),
            sequence: 0,
            poisoned: false,
            checkpoint_sequence,
            compacted_sequence: latest_compacted,
            ivf: None,
            visible_objects,
            maintenance_limits: None,
            retry: retry::State::default(),
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
            let mutations = read_mutations(&db.store, &object, next, config, &mut db.retry)?;
            for mutation in mutations {
                db.apply(mutation);
            }
            db.sequence = next;
        }
        Ok(db)
    }
    pub(crate) fn into_store(self) -> S {
        self.store
    }
    /// Enable write bounds after opening. Reopen and set the same policy after
    /// a crash; counters are reconstructed from the authoritative listing.
    pub fn set_maintenance_limits(&mut self, limits: MaintenanceLimits) -> Result<()> {
        limits.validate()?;
        self.maintenance_limits = Some(limits);
        Ok(())
    }
    pub fn maintenance_status(&self) -> MaintenanceStatus {
        let tail_objects = self.sequence - self.checkpoint_sequence.unwrap_or(0);
        let (should_compact, writes_blocked) =
            self.maintenance_limits.map_or((false, false), |limits| {
                (
                    tail_objects >= limits.soft_tail_objects
                        || self.visible_objects >= limits.soft_visible_objects,
                    tail_objects >= limits.hard_tail_objects
                        || self.visible_objects >= limits.hard_visible_objects,
                )
            });
        MaintenanceStatus {
            sequence: self.sequence,
            tail_objects,
            visible_objects: self.visible_objects,
            should_compact,
            writes_blocked,
        }
    }
    fn ensure_write_capacity(&self, mutations: usize) -> Result<()> {
        if let Some(limits) = self.maintenance_limits {
            if mutations > limits.max_batch_mutations {
                return Err(Error::Invalid("batch exceeds maintenance limit".into()));
            }
            if self.maintenance_status().writes_blocked {
                return Err(Error::MaintenanceRequired);
            }
        }
        Ok(())
    }
    pub(crate) fn tracked_create(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        self.store.create(key, bytes)?;
        self.visible_objects += 1;
        Ok(())
    }
    fn tracked_remove_many(&mut self, keys: &[String]) -> Result<()> {
        self.store.remove_many(keys)?;
        self.visible_objects -= keys.len();
        Ok(())
    }
    fn load_snapshot(&mut self, object: &str, sequence: u64) -> Result<()> {
        let bytes = self
            .store
            .get(object)?
            .ok_or_else(|| Error::Corrupt(format!("listed snapshot missing: {object}")))?;
        let version = decode::<Version>(&bytes)?.version;
        if matches!(version, 3 | 5) {
            return self.load_chunked_snapshot(object, sequence, &bytes);
        }
        let segment = match version {
            1 => {
                let old: SegmentV1 = decode(&bytes)?;
                Segment {
                    version: old.version,
                    sequence: old.sequence,
                    config: old.config,
                    retry: None,
                    documents: old
                        .documents
                        .into_iter()
                        .map(|(id, vector)| {
                            (
                                id,
                                Document {
                                    vector,
                                    metadata: BTreeMap::new(),
                                },
                            )
                        })
                        .collect(),
                }
            }
            2 | 4 => decode::<Segment>(&bytes)?,
            _ => return Err(Error::Corrupt("unsupported snapshot version".into())),
        };
        if segment.sequence != sequence || segment.config != self.config {
            return Err(Error::Corrupt(
                "invalid snapshot version, sequence or configuration".into(),
            ));
        }
        validate_retry_snapshot(segment.version, segment.retry.as_ref(), sequence)?;
        self.retry = segment
            .retry
            .unwrap_or_else(|| retry::State::legacy(sequence));
        let mut documents = BTreeMap::new();
        let mut previous_id = None;
        for (id, document) in segment.documents {
            if previous_id.is_some_and(|previous| previous >= id) {
                return Err(Error::Corrupt(
                    "snapshot IDs must be strictly increasing".into(),
                ));
            }
            self.config
                .vector(&document.vector)
                .map_err(|e| Error::Corrupt(e.to_string()))?;
            previous_id = Some(id);
            documents.insert(id, document);
        }
        self.documents = documents;
        self.sequence = sequence;
        Ok(())
    }
    fn load_chunked_snapshot(&mut self, object: &str, sequence: u64, bytes: &[u8]) -> Result<()> {
        let manifest = parse_chunked_manifest(bytes, sequence, self.config)?;
        let kind = if object == compacted_key(sequence) {
            "compacted"
        } else if object == segment_key(sequence) {
            "segment"
        } else {
            return Err(Error::Corrupt("invalid chunked snapshot key".into()));
        };
        let mut documents = BTreeMap::new();
        scan_snapshot(&self.store, self.config, kind, &manifest, |id, document| {
            documents.insert(id, document);
            Ok(())
        })?;
        self.documents = documents;
        self.retry = manifest
            .retry
            .unwrap_or_else(|| retry::State::legacy(sequence));
        self.sequence = sequence;
        Ok(())
    }
    fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        let snapshot = Segment {
            version: 4,
            retry: Some(self.retry.clone()),
            sequence: self.sequence,
            config: self.config,
            documents: self
                .documents
                .iter()
                .map(|(&id, document)| (id, document.clone()))
                .collect(),
        };
        let mut bytes = Vec::new();
        snapshot
            .serialize(&mut serde_json::Serializer::with_formatter(
                &mut bytes,
                SnapshotNumbers,
            ))
            .map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(bytes)
    }
    fn publish_chunked_snapshot(&mut self, kind: &str, max_bytes: usize) -> Result<()> {
        let empty_size = encode(&SnapshotChunk {
            version: 1,
            sequence: self.sequence,
            config: self.config,
            documents: Vec::new(),
        })?
        .len();
        if max_bytes < empty_size {
            return Err(Error::Invalid(
                "snapshot chunk byte limit is too small".into(),
            ));
        }
        // Size the complete layout before writing anything. Each encoded row is
        // measured independently, so this pass uses bounded temporary memory.
        let mut ranges = Vec::new();
        let mut first = None;
        let mut last = 0;
        let mut current_size = empty_size;
        let mut count = 0;
        for (&id, document) in &self.documents {
            let row_size = encoded_len(&(id, document))?;
            if empty_size
                .checked_add(row_size)
                .is_none_or(|size| size > max_bytes)
            {
                return Err(Error::Invalid(format!(
                    "document {id} exceeds snapshot chunk byte limit"
                )));
            }
            let next_size = current_size
                .checked_add(row_size)
                .and_then(|size| size.checked_add(usize::from(count > 0)));
            if count > 0 && next_size.is_none_or(|size| size > max_bytes) {
                ranges.push((first.expect("nonempty chunk"), last));
                first = None;
                current_size = empty_size;
                count = 0;
            }
            first.get_or_insert(id);
            last = id;
            current_size = current_size + row_size + usize::from(count > 0);
            count += 1;
        }
        if let Some(first) = first {
            ranges.push((first, last));
        }
        if (ranges.len() as u128) > 10_000_000_000_u128 {
            return Err(Error::Invalid("too many snapshot chunks".into()));
        }

        self.poisoned = true;
        let listed = self.store.list()?;
        let existing: BTreeSet<_> = listed.iter().cloned().collect();
        if existing.len() != listed.len() {
            return Err(Error::Corrupt(
                "duplicate listed key during snapshot".into(),
            ));
        }
        let mut references = Vec::with_capacity(ranges.len());
        for (ordinal, (first_id, last_id)) in ranges.into_iter().enumerate() {
            let documents: Vec<_> = self
                .documents
                .range(first_id..=last_id)
                .map(|(&id, document)| (id, document.clone()))
                .collect();
            let rows = documents.len();
            let bytes = encode(&SnapshotChunk {
                version: 1,
                sequence: self.sequence,
                config: self.config,
                documents,
            })?;
            if bytes.len() > max_bytes {
                return Err(Error::Corrupt("chunk sizing disagreement".into()));
            }
            let key = chunk_key(kind, self.sequence, max_bytes, ordinal);
            if existing.contains(&key) {
                if self.store.get(&key)?.as_deref() != Some(bytes.as_slice()) {
                    return Err(Error::Corrupt(format!("conflicting snapshot chunk: {key}")));
                }
            } else {
                self.tracked_create(&key, &bytes)?;
            }
            references.push(ChunkRef {
                first_id,
                last_id,
                rows,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
            });
        }
        let manifest = SnapshotManifest {
            version: 5,
            retry: Some(self.retry.clone()),
            sequence: self.sequence,
            config: self.config,
            max_chunk_bytes: max_bytes,
            chunks: references,
        };
        let key = if kind == "compacted" {
            compacted_key(self.sequence)
        } else {
            segment_key(self.sequence)
        };
        self.tracked_create(&key, &encode(&manifest)?)?;
        Ok(())
    }
    /// Consolidate live state into a durable snapshot, then reclaim covered logs
    /// and older snapshots. Success acknowledges publication and all listed
    /// removals. On any storage error/panic, reopen before further writes or
    /// maintenance; already removed objects were covered by a durable snapshot.
    /// A repeated call at the same sequence resumes cleanup without republishing.
    pub fn compact(&mut self) -> Result<()> {
        self.compact_with(None)
    }
    /// Compact using version 5 manifests and version 1 chunks with a maximum encoded payload
    /// size per chunk. The manifest is the single authoritative boundary.
    pub fn compact_chunked(&mut self, max_chunk_bytes: usize) -> Result<()> {
        self.compact_with(Some(max_chunk_bytes))
    }
    fn compact_with(&mut self, max_chunk_bytes: Option<usize>) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let new_legacy_snapshot =
            self.compacted_sequence != Some(self.sequence) && max_chunk_bytes.is_none();
        if self.compacted_sequence != Some(self.sequence) {
            if let Some(max_bytes) = max_chunk_bytes {
                self.publish_chunked_snapshot("compacted", max_bytes)?;
            } else {
                let bytes = self.snapshot_bytes()?;
                self.poisoned = true;
                self.tracked_create(&compacted_key(self.sequence), &bytes)?;
            }
            self.compacted_sequence = Some(self.sequence);
            self.checkpoint_sequence = Some(self.sequence);
        }
        self.poisoned = true;
        let retained_chunks = if new_legacy_snapshot {
            BTreeSet::new()
        } else {
            self.current_compacted_chunks()?
        };
        // Build and validate the full deletion plan before removing anything.
        let mut keys = self.store.list()?;
        keys.sort();
        if keys.windows(2).any(|pair| pair[0] == pair[1])
            || !keys.iter().any(|k| k == "metadata")
            || !keys.contains(&compacted_key(self.sequence))
            || retained_chunks
                .iter()
                .any(|key| keys.binary_search(key).is_err())
        {
            return Err(Error::Corrupt("invalid listing during compaction".into()));
        }
        self.visible_objects = keys.len();
        let mut obsolete = Vec::new();
        for object in keys {
            if object == "metadata" {
                continue;
            }
            if object.starts_with("ivf-") {
                let sequence = ivf::cache_sequence(&object)?;
                if sequence > self.sequence {
                    return Err(Error::Corrupt(format!(
                        "unexpected object during compaction: {object}"
                    )));
                }
                if sequence < self.sequence {
                    obsolete.push(object);
                }
                continue;
            }
            if object.starts_with("segmentchunk-") || object.starts_with("compactedchunk-") {
                let kind = if object.starts_with("segmentchunk-") {
                    "segment"
                } else {
                    "compacted"
                };
                if chunk_sequence(&object, kind)? > self.sequence {
                    return Err(Error::Corrupt(format!(
                        "unexpected object during compaction: {object}"
                    )));
                }
                if !retained_chunks.contains(&object) {
                    obsolete.push(object);
                }
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
        self.tracked_remove_many(&obsolete)?;
        self.poisoned = false;
        Ok(())
    }
    fn current_compacted_chunks(&self) -> Result<BTreeSet<String>> {
        let key = compacted_key(self.sequence);
        let bytes = self
            .store
            .get(&key)?
            .ok_or_else(|| Error::Corrupt(format!("compaction snapshot missing: {key}")))?;
        match decode::<Version>(&bytes)?.version {
            1 | 2 | 4 => {
                let header: SnapshotHeader = decode(&bytes)?;
                if !matches!(header.version, 1 | 2 | 4)
                    || header.sequence != self.sequence
                    || header.config != self.config
                {
                    return Err(Error::Corrupt("invalid compacted snapshot".into()));
                }
                return Ok(BTreeSet::new());
            }
            3 | 5 => {}
            _ => return Err(Error::Corrupt("unsupported compacted snapshot".into())),
        }
        let manifest: SnapshotManifest = decode(&bytes)?;
        if !matches!(manifest.version, 3 | 5)
            || manifest.sequence != self.sequence
            || manifest.config != self.config
            || manifest.max_chunk_bytes == 0
        {
            return Err(Error::Corrupt("invalid compacted manifest".into()));
        }
        let mut keys = BTreeSet::new();
        let mut previous_id = None;
        for (ordinal, reference) in manifest.chunks.iter().enumerate() {
            if reference.rows == 0
                || reference.first_id > reference.last_id
                || previous_id.is_some_and(|previous| previous >= reference.first_id)
            {
                return Err(Error::Corrupt("invalid compacted chunk range".into()));
            }
            previous_id = Some(reference.last_id);
            keys.insert(chunk_key(
                "compacted",
                self.sequence,
                manifest.max_chunk_bytes,
                ordinal,
            ));
        }
        Ok(keys)
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
        self.tracked_create(&segment_key(self.sequence), &bytes)?;
        self.checkpoint_sequence = Some(self.sequence);
        self.poisoned = false;
        Ok(())
    }
    /// Persist the current state as bounded immutable chunks plus one version 5
    /// manifest. Chunks alone are not authoritative; an uncertain outcome
    /// requires reopening before another durable operation.
    pub fn checkpoint_chunked(&mut self, max_chunk_bytes: usize) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.checkpoint_sequence == Some(self.sequence) {
            return Ok(());
        }
        self.publish_chunked_snapshot("segment", max_chunk_bytes)?;
        self.checkpoint_sequence = Some(self.sequence);
        self.poisoned = false;
        Ok(())
    }
    pub fn config(&self) -> Config {
        self.config
    }
    pub fn get(&self, id: u64) -> Option<&[f32]> {
        self.documents
            .get(&id)
            .map(|document| document.vector.as_slice())
    }
    /// Return the acknowledged metadata for a live document.
    pub fn get_metadata(&self, id: u64) -> Option<&BTreeMap<String, String>> {
        self.documents.get(&id).map(|document| &document.metadata)
    }
    pub fn put(&mut self, id: u64, vector: Vec<f32>) -> Result<()> {
        self.put_with_metadata(id, vector, BTreeMap::new())
    }
    /// Replace a document's vector and metadata in one durable mutation.
    pub fn put_with_metadata(
        &mut self,
        id: u64,
        vector: Vec<f32>,
        metadata: BTreeMap<String, String>,
    ) -> Result<()> {
        self.config.vector(&vector)?;
        self.commit(Mutation::Put {
            id,
            vector,
            metadata,
        })
    }
    /// Deleting an absent ID is an idempotent logical operation, still logged.
    pub fn delete(&mut self, id: u64) -> Result<()> {
        self.commit(Mutation::Delete { id })
    }
    /// Publish a nonempty ordered batch in one immutable object. Success makes
    /// every operation visible together; an uncertain write requires reopening.
    /// A batch uses one log sequence, regardless of its number of operations.
    pub fn apply_batch(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        if mutations.is_empty() {
            return Err(Error::Invalid("batch must not be empty".into()));
        }
        for mutation in &mutations {
            if let Mutation::Put { vector, .. } = mutation {
                self.config.vector(vector)?;
            }
        }
        self.commit_batch(mutations)
    }
    /// Observe an ID, including absence. Conditions expire explicitly at the
    /// reported revision floor and never allow delete/reinsert ABA.
    pub fn revision(&self, id: u64) -> retry::Revision {
        retry::Revision {
            id,
            boundary: self.sequence,
        }
    }
    pub fn revision_floor(&self) -> u64 {
        self.retry.revision_floor
    }
    /// Generate locally without publishing; retain the ID for all retries.
    pub fn request_id(&self) -> Result<retry::RequestId> {
        let mut nonce = [0; 16];
        getrandom::getrandom(&mut nonce).map_err(|e| Error::Invalid(e.to_string()))?;
        Ok(retry::RequestId {
            boundary: self.sequence,
            nonce,
        })
    }
    /// Poisoned handles cannot resolve uncertainty from their stale read view.
    pub fn lookup_request(&self, id: retry::RequestId) -> Result<retry::Lookup> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        Ok(self.retry.lookup(id, self.sequence))
    }
    pub(crate) fn retained_request(
        &self,
        request: &retry::Request,
    ) -> Result<Option<retry::Outcome>> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        request.validate(self.config)?;
        self.retry.duplicate(request, self.sequence)
    }
    /// Persist the conditional decision and mutations in the SAME immutable
    /// object. A retained duplicate returns its original outcome without I/O.
    pub fn apply_request(&mut self, request: retry::Request) -> Result<retry::Outcome> {
        if let Some(outcome) = self.retained_request(&request)? {
            return Ok(outcome);
        }
        self.ensure_write_capacity(request.mutations.len())?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("mutation sequence exhausted".into()))?;
        let outcome = self.retry.decide(&request, sequence);
        let record = RequestRecord {
            version: 4,
            sequence,
            request,
            outcome,
        };
        let bytes = encode(&record)?;
        // Prepare all fallible metadata encoding before publication.
        let mut state = self.retry.clone();
        let mutations = if outcome.conflict.is_none() {
            record.request.mutations.clone()
        } else {
            Vec::new()
        };
        state.advance(sequence, &mutations);
        state.retain(&record.request, outcome)?;
        self.poisoned = true;
        self.tracked_create(&key(sequence), &bytes)?;
        for mutation in mutations {
            self.apply(mutation);
        }
        self.retry = state;
        self.sequence = sequence;
        self.poisoned = false;
        Ok(outcome)
    }
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<Neighbor>> {
        self.search_filtered(query, k, &[])
    }
    /// Exact top-k among documents matching every metadata key/value pair.
    /// Missing keys do not match. An empty filter includes every document.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> Result<Vec<Neighbor>> {
        self.config.vector(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut results: Vec<_> = self
            .documents
            .iter()
            .filter(|(_, document)| matches_filter(&document.metadata, filter))
            .map(|(&id, document)| Neighbor {
                id,
                distance: self.config.metric.score(query, &document.vector),
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
        // Completed results may outlive the query. Do not transfer the full
        // scoring allocation to a caller retaining only k neighbors.
        Ok(results.into_boxed_slice().into_vec())
    }
    fn commit(&mut self, mutation: Mutation) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        self.ensure_write_capacity(1)?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("mutation sequence exhausted".into()))?;
        let record = Record {
            version: 2,
            sequence,
            mutation,
        };
        let bytes = encode(&record)?;
        // Set before entering storage: even a caught backend panic cannot permit reuse.
        self.poisoned = true;
        self.tracked_create(&key(sequence), &bytes)?;
        self.retry
            .advance(sequence, std::slice::from_ref(&record.mutation));
        self.apply(record.mutation);
        self.sequence = sequence;
        self.poisoned = false;
        Ok(())
    }
    fn commit_batch(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        self.ensure_write_capacity(mutations.len())?;
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("mutation sequence exhausted".into()))?;
        let record = BatchRecord {
            version: 3,
            sequence,
            mutations,
        };
        let bytes = encode(&record)?;
        // A lost acknowledgement may leave the complete batch remotely visible.
        self.poisoned = true;
        self.tracked_create(&key(sequence), &bytes)?;
        self.retry.advance(sequence, &record.mutations);
        for mutation in record.mutations {
            self.apply(mutation);
        }
        self.sequence = sequence;
        self.poisoned = false;
        Ok(())
    }
    fn apply(&mut self, mutation: Mutation) {
        self.ivf = None;
        match mutation {
            Mutation::Put {
                id,
                vector,
                metadata,
            } => {
                self.documents.insert(id, Document { vector, metadata });
            }
            Mutation::Delete { id } => {
                self.documents.remove(&id);
            }
        }
    }
}

fn matches_filter(metadata: &BTreeMap<String, String>, filter: &[(&str, &str)]) -> bool {
    filter
        .iter()
        .all(|&(key, value)| metadata.get(key).is_some_and(|found| found == value))
}
