//! Experimental versioned segmented layout. This is not yet a serving engine.
use crate::{
    decode, encode, matches_filter, retry, store::ObjectStore, streaming::OwnedDocument, Config,
    Document, Error, Mutation, Neighbor, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
};

pub(crate) const MAX_BLOCK_BYTES: usize = 128 * 1024;
pub(crate) const MAX_PACK_BYTES: usize = 1024 * 1024;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONSOLIDATION_INDEX_BYTES: usize = 1024 * 1024;
const INDEX_MAGIC: &[u8; 8] = b"GLRIDX01";
const MAX_TAIL_OBJECTS: usize = 64;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockRecord {
    pub(crate) sequence: u64,
    pub(crate) mutation: Mutation,
}

impl BlockRecord {
    fn id(&self) -> u64 {
        match &self.mutation {
            Mutation::Put { id, .. } | Mutation::Delete { id } => *id,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Block {
    version: u32,
    config: Config,
    partition: u32,
    records: Vec<BlockRecord>,
}

impl Block {
    pub(crate) fn new(config: Config, partition: u32, records: Vec<BlockRecord>) -> Result<Self> {
        let block = Self {
            version: 1,
            config,
            partition,
            records,
        };
        block.validate(config)?;
        Ok(block)
    }

    fn validate(&self, config: Config) -> Result<()> {
        if self.version != 1 || self.config != config || self.records.is_empty() {
            return Err(Error::Corrupt("invalid segmented block identity".into()));
        }
        let mut previous = None;
        for record in &self.records {
            if record.sequence == 0 || previous.is_some_and(|id| id >= record.id()) {
                return Err(Error::Corrupt(
                    "invalid segmented block record order".into(),
                ));
            }
            if let Mutation::Put { vector, .. } = &record.mutation {
                config
                    .vector(vector)
                    .map_err(|error| Error::Corrupt(error.to_string()))?;
            }
            previous = Some(record.id());
        }
        Ok(())
    }
}

/// Root-v1 metadata for one block inside an immutable physical pack.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BlockRef {
    pub(crate) object: String,
    pub(crate) payload_len: usize,
    pub(crate) offset: usize,
    pub(crate) length: usize,
    pub(crate) sha256: String,
    pub(crate) partition: u32,
    pub(crate) first_id: u64,
    pub(crate) last_id: u64,
    pub(crate) rows: usize,
}

/// Version-1 fixed-width ID locator. The block ordinal indexes `RunRef.blocks`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) id: u64,
    pub(crate) sequence: u64,
    pub(crate) block: u32,
    pub(crate) deleted: bool,
}

pub(crate) struct RunIndex {
    pub(crate) sequence: u64,
    pub(crate) entries: Vec<IndexEntry>,
}

impl RunIndex {
    fn validate(&self, first_sequence: u64, last_sequence: u64, blocks: usize) -> Result<()> {
        if self.sequence != last_sequence || self.entries.is_empty() {
            return Err(Error::Corrupt(
                "invalid segmented run index identity".into(),
            ));
        }
        let mut previous = None;
        for entry in &self.entries {
            if previous.is_some_and(|id| id >= entry.id)
                || entry.sequence < first_sequence
                || entry.sequence > last_sequence
                || entry.block as usize >= blocks
            {
                return Err(Error::Corrupt("invalid segmented run index entry".into()));
            }
            previous = Some(entry.id);
        }
        Ok(())
    }

    /// Header: magic, covered sequence, u32 count, u32 zero. Each entry is
    /// id:u64, sequence:u64, block:u32, deleted:u8, three zero padding bytes.
    pub(crate) fn encode(&self, first_sequence: u64, blocks: usize) -> Result<Vec<u8>> {
        self.validate(first_sequence, self.sequence, blocks)?;
        let count = u32::try_from(self.entries.len())
            .map_err(|_| Error::Invalid("too many segmented index entries".into()))?;
        let length = 24_usize
            .checked_add(
                self.entries
                    .len()
                    .checked_mul(24)
                    .ok_or_else(|| Error::Invalid("segmented index size overflow".into()))?,
            )
            .ok_or_else(|| Error::Invalid("segmented index size overflow".into()))?;
        if length > MAX_INDEX_BYTES {
            return Err(Error::Invalid("segmented index exceeds byte limit".into()));
        }
        let mut bytes = Vec::with_capacity(length);
        bytes.extend_from_slice(INDEX_MAGIC);
        bytes.extend_from_slice(&self.sequence.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        for entry in &self.entries {
            bytes.extend_from_slice(&entry.id.to_le_bytes());
            bytes.extend_from_slice(&entry.sequence.to_le_bytes());
            bytes.extend_from_slice(&entry.block.to_le_bytes());
            bytes.push(u8::from(entry.deleted));
            bytes.extend_from_slice(&[0; 3]);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(
        bytes: &[u8],
        first_sequence: u64,
        last_sequence: u64,
        blocks: usize,
    ) -> Result<Self> {
        if bytes.len() < 24
            || bytes.len() > MAX_INDEX_BYTES
            || &bytes[..8] != INDEX_MAGIC
            || bytes[20..24] != [0; 4]
        {
            return Err(Error::Corrupt("invalid segmented index header".into()));
        }
        let sequence = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        if 24_usize.saturating_add(count.saturating_mul(24)) != bytes.len() {
            return Err(Error::Corrupt("invalid segmented index length".into()));
        }
        let mut entries = Vec::with_capacity(count);
        for row in bytes[24..].as_chunks::<24>().0 {
            if row[21..24] != [0; 3] || row[20] > 1 {
                return Err(Error::Corrupt("invalid segmented index entry flags".into()));
            }
            entries.push(IndexEntry {
                id: u64::from_le_bytes(row[0..8].try_into().unwrap()),
                sequence: u64::from_le_bytes(row[8..16].try_into().unwrap()),
                block: u32::from_le_bytes(row[16..20].try_into().unwrap()),
                deleted: row[20] == 1,
            });
        }
        let index = Self { sequence, entries };
        index.validate(first_sequence, last_sequence, blocks)?;
        Ok(index)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunRef {
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
    pub(crate) index_object: String,
    pub(crate) index_len: usize,
    pub(crate) index_sha256: String,
    pub(crate) blocks: Vec<BlockRef>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    version: u32,
    generation: u64,
    pub(crate) sequence: u64,
    config: Config,
    retry: retry::State,
    pub(crate) runs: Vec<RunRef>,
}

impl Root {
    pub(crate) fn empty(config: Config) -> Self {
        Self {
            version: 1,
            generation: 0,
            sequence: 0,
            config,
            retry: retry::State::default(),
            runs: Vec::new(),
        }
    }

    pub(crate) fn validate(&self, config: Config) -> Result<()> {
        if self.version != 1 || self.config != config || self.runs.len() > 64 {
            return Err(Error::Corrupt("invalid segmented root identity".into()));
        }
        self.retry.validate(self.sequence)?;
        let mut previous_sequence = 0;
        for run in &self.runs {
            if run.first_sequence == 0
                || run.first_sequence <= previous_sequence
                || run.first_sequence > run.last_sequence
                || run.last_sequence > self.sequence
                || run.blocks.is_empty()
                || run.index_len < 48
                || run.index_len > MAX_INDEX_BYTES
                || !(run.index_len - 24).is_multiple_of(24)
                || !valid_digest(&run.index_sha256)
            {
                return Err(Error::Corrupt("invalid segmented run reference".into()));
            }
            previous_sequence = run.last_sequence;
            for reference in &run.blocks {
                validate_block_ref(reference)?;
            }
        }
        Ok(())
    }
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn validate_block_ref(reference: &BlockRef) -> Result<()> {
    if reference.length == 0
        || reference.length > MAX_BLOCK_BYTES
        || reference.payload_len > MAX_PACK_BYTES
        || reference
            .offset
            .checked_add(reference.length)
            .is_none_or(|end| end > reference.payload_len)
        || reference.rows == 0
        || reference.first_id > reference.last_id
        || !valid_digest(&reference.sha256)
    {
        return Err(Error::Corrupt("invalid segmented block reference".into()));
    }
    Ok(())
}

pub(crate) fn read_run_index<S: ObjectStore>(store: &S, run: &RunRef) -> Result<RunIndex> {
    let bytes = store
        .get(&run.index_object)?
        .ok_or_else(|| Error::Corrupt(format!("segmented index missing: {}", run.index_object)))?;
    if bytes.len() != run.index_len || format!("{:x}", Sha256::digest(&bytes)) != run.index_sha256 {
        return Err(Error::Corrupt(format!(
            "segmented index digest mismatch: {}",
            run.index_object
        )));
    }
    RunIndex::decode(
        &bytes,
        run.first_sequence,
        run.last_sequence,
        run.blocks.len(),
    )
}

/// Encode at most one physical pack. The caller publishes it before publishing
/// a root that references these block digests; an orphan pack is not state.
pub(crate) fn encode_pack(
    object: &str,
    config: Config,
    blocks: &[Block],
) -> Result<(Vec<u8>, Vec<BlockRef>)> {
    if blocks.is_empty() {
        return Err(Error::Invalid("empty segmented pack".into()));
    }
    let mut payload = Vec::new();
    let mut references = Vec::with_capacity(blocks.len());
    for block in blocks {
        block.validate(config)?;
        let bytes = encode(block)?;
        let end = payload
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| Error::Invalid("segmented pack size overflow".into()))?;
        if bytes.len() > MAX_BLOCK_BYTES || end > MAX_PACK_BYTES {
            return Err(Error::Invalid(
                "segmented block or pack exceeds byte limit".into(),
            ));
        }
        references.push(BlockRef {
            object: object.into(),
            payload_len: 0,
            offset: payload.len(),
            length: bytes.len(),
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            partition: block.partition,
            first_id: block.records.first().unwrap().id(),
            last_id: block.records.last().unwrap().id(),
            rows: block.records.len(),
        });
        payload.extend_from_slice(&bytes);
    }
    for reference in &mut references {
        reference.payload_len = payload.len();
    }
    Ok((payload, references))
}

/// A selected block is authenticated before its records can affect a result.
pub(crate) fn read_block<S: ObjectStore>(
    store: &S,
    config: Config,
    reference: &BlockRef,
) -> Result<Block> {
    validate_block_ref(reference)?;
    let bytes = store
        .get_range(
            &reference.object,
            reference.offset,
            reference.length,
            reference.payload_len,
        )?
        .ok_or_else(|| Error::Corrupt(format!("segmented pack missing: {}", reference.object)))?;
    if format!("{:x}", Sha256::digest(&bytes)) != reference.sha256 {
        return Err(Error::Corrupt(format!(
            "segmented block digest mismatch: {}",
            reference.object
        )));
    }
    let block: Block = decode(&bytes)?;
    block.validate(config)?;
    if block.partition != reference.partition
        || block.records.len() != reference.rows
        || block.records.first().unwrap().id() != reference.first_id
        || block.records.last().unwrap().id() != reference.last_id
    {
        return Err(Error::Corrupt("segmented block reference mismatch".into()));
    }
    Ok(block)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataV2 {
    version: u32,
    config: Config,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRecordV1 {
    version: u32,
    sequence: u64,
    request: retry::Request,
    outcome: retry::Outcome,
}

fn root_key(generation: u64) -> String {
    format!("sgroot-{generation:020}")
}

fn log_key(sequence: u64) -> String {
    format!("sglog-{sequence:020}")
}

fn attempt_id() -> Result<String> {
    let mut nonce = [0_u8; 16];
    getrandom::getrandom(&mut nonce).map_err(|error| {
        Error::Io(std::io::Error::other(format!(
            "OS randomness unavailable: {error}"
        )))
    })?;
    Ok(nonce.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn numbered_key(key: &str, prefix: &str) -> Result<u64> {
    let sequence = key
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .ok_or_else(|| Error::Corrupt(format!("invalid segmented key: {key}")))?;
    if key != format!("{prefix}{sequence:020}") {
        return Err(Error::Corrupt(format!("invalid segmented key: {key}")));
    }
    Ok(sequence)
}

#[derive(Clone, Copy)]
struct Location {
    run: usize,
    entry: IndexEntry,
}

struct Ranked(Neighbor);
impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .distance
            .total_cmp(&other.0.distance)
            .then(self.0.id.cmp(&other.0.id))
    }
}

fn consider(
    heap: &mut BinaryHeap<Ranked>,
    k: usize,
    config: Config,
    query: &[f32],
    id: u64,
    vector: &[f32],
) {
    let candidate = Ranked(Neighbor {
        id,
        distance: config.metric.score(query, vector),
    });
    if heap.len() < k {
        heap.push(candidate);
    } else if heap.peek().is_some_and(|worst| candidate < *worst) {
        heap.pop();
        heap.push(candidate);
    }
}

struct SealState {
    attempt: String,
    boundary: u64,
    first_sequence: u64,
    retry: retry::State,
    blocks: Vec<Block>,
    entries: Vec<IndexEntry>,
    references: Vec<BlockRef>,
    next_block: usize,
    next_pack: u32,
    index_published: bool,
}

/// Experimental segmented namespace. Vectors in committed runs are fetched by
/// block. The caller must enforce one exclusive object-store owner and use a
/// fresh namespace; the current serving API does not use this engine yet.
pub struct SegmentedDatabase<S> {
    store: S,
    config: Config,
    root: Root,
    sequence: u64,
    retry: retry::State,
    latest: BTreeMap<u64, Location>,
    tail: BTreeMap<u64, (u64, Option<Document>)>,
    tail_objects: usize,
    known_keys: BTreeSet<String>,
    obsolete: VecDeque<String>,
    seal: Option<SealState>,
    poisoned: bool,
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    pub fn run_count(&self) -> usize {
        self.root.runs.len()
    }

    pub fn open(mut store: S, config: Config) -> Result<Self> {
        config.validate()?;
        let mut keys = store.list()?;
        keys.sort();
        if keys.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::Corrupt("duplicate segmented object key".into()));
        }
        if keys.is_empty() {
            store.create("metadata", &encode(&MetadataV2 { version: 2, config })?)?;
            store.create(&root_key(0), &encode(&Root::empty(config))?)?;
            keys = vec!["metadata".into(), root_key(0)];
        } else if keys == ["metadata"] {
            // An interrupted first open may have published metadata only.
            let bytes = store
                .get("metadata")?
                .ok_or_else(|| Error::Corrupt("listed metadata missing".into()))?;
            let metadata: MetadataV2 = decode(&bytes)?;
            if metadata.version != 2 || metadata.config != config {
                return Err(Error::Corrupt("invalid segmented metadata".into()));
            }
            store.create(&root_key(0), &encode(&Root::empty(config))?)?;
            keys.push(root_key(0));
        }
        if keys.iter().filter(|key| key.as_str() == "metadata").count() != 1
            || !keys.contains(&root_key(0))
        {
            return Err(Error::Corrupt(
                "segmented metadata or root zero missing".into(),
            ));
        }
        let metadata: MetadataV2 = decode(
            &store
                .get("metadata")?
                .ok_or_else(|| Error::Corrupt("listed segmented metadata missing".into()))?,
        )?;
        if metadata.version != 2 || metadata.config != config {
            return Err(Error::Corrupt("invalid segmented metadata".into()));
        }
        let listed: BTreeSet<_> = keys.iter().cloned().collect();
        let mut latest_generation = 0;
        let mut logs = Vec::new();
        for key in &keys {
            if key == "metadata" {
                continue;
            }
            if key.starts_with("sgroot-") {
                latest_generation = latest_generation.max(numbered_key(key, "sgroot-")?);
            } else if key.starts_with("sglog-") {
                let sequence = numbered_key(key, "sglog-")?;
                if sequence == 0 {
                    return Err(Error::Corrupt("segmented log sequence zero".into()));
                }
                logs.push(sequence);
            } else if key.starts_with("sgpack-") || key.starts_with("sgindex-") {
                // Unreferenced objects from an incomplete root are not state.
            } else {
                return Err(Error::Corrupt(format!("unexpected segmented key: {key}")));
            }
        }
        let root: Root = decode(
            &store
                .get(&root_key(latest_generation))?
                .ok_or_else(|| Error::Corrupt("selected segmented root missing".into()))?,
        )?;
        root.validate(config)?;
        if root.generation != latest_generation {
            return Err(Error::Corrupt("segmented root generation mismatch".into()));
        }
        let mut latest = BTreeMap::new();
        for (run_ordinal, run) in root.runs.iter().enumerate() {
            if !listed.contains(&run.index_object)
                || run
                    .blocks
                    .iter()
                    .any(|block| !listed.contains(&block.object))
            {
                return Err(Error::Corrupt(
                    "selected segmented run object missing".into(),
                ));
            }
            let index = read_run_index(&store, run)?;
            for entry in index.entries {
                latest.insert(
                    entry.id,
                    Location {
                        run: run_ordinal,
                        entry,
                    },
                );
            }
        }
        logs.sort_unstable();
        let tail_logs: Vec<_> = logs
            .into_iter()
            .filter(|&sequence| sequence > root.sequence)
            .collect();
        if tail_logs.len() > MAX_TAIL_OBJECTS {
            return Err(Error::Corrupt("segmented replay tail exceeds bound".into()));
        }
        let mut db = Self {
            store,
            config,
            sequence: root.sequence,
            retry: root.retry.clone(),
            root,
            latest,
            tail: BTreeMap::new(),
            tail_objects: 0,
            known_keys: listed,
            obsolete: VecDeque::new(),
            seal: None,
            poisoned: false,
        };
        for log_sequence in tail_logs {
            if db.sequence.checked_add(1) != Some(log_sequence) {
                return Err(Error::Corrupt("segmented mutation log gap".into()));
            }
            let record: LogRecordV1 = decode(
                &db.store
                    .get(&log_key(log_sequence))?
                    .ok_or_else(|| Error::Corrupt("listed segmented log missing".into()))?,
            )?;
            db.replay(record, log_sequence)?;
            db.tail_objects += 1;
        }
        db.schedule_obsolete();
        Ok(db)
    }

    fn schedule_obsolete(&mut self) {
        let mut retained = BTreeSet::from([
            "metadata".to_owned(),
            root_key(0),
            root_key(self.root.generation),
        ]);
        for run in &self.root.runs {
            retained.insert(run.index_object.clone());
            for block in &run.blocks {
                retained.insert(block.object.clone());
            }
        }
        for sequence in self.root.sequence.saturating_add(1)..=self.sequence {
            retained.insert(log_key(sequence));
        }
        self.obsolete = self.known_keys.difference(&retained).cloned().collect();
    }

    fn create_staged(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        if self.known_keys.contains(key) {
            if self.store.get(key)?.as_deref() != Some(bytes) {
                return Err(Error::Corrupt(format!(
                    "conflicting segmented object: {key}"
                )));
            }
        } else {
            self.store.create(key, bytes)?;
            self.known_keys.insert(key.to_owned());
        }
        Ok(())
    }

    fn publish_pack(
        &mut self,
        attempt: &str,
        ordinal: u32,
        blocks: &[Block],
    ) -> Result<Vec<BlockRef>> {
        let key = format!("sgpack-{attempt}-{ordinal:08}");
        let (bytes, references) = encode_pack(&key, self.config, blocks)?;
        self.create_staged(&key, &bytes)?;
        Ok(references)
    }

    fn replay(&mut self, record: LogRecordV1, expected: u64) -> Result<()> {
        record
            .request
            .validate(self.config)
            .map_err(|error| Error::Corrupt(error.to_string()))?;
        if record.version != 1
            || record.sequence != expected
            || record.outcome.sequence != expected
            || self
                .retry
                .duplicate(&record.request, expected - 1)
                .map_err(|error| Error::Corrupt(error.to_string()))?
                .is_some()
            || self.retry.decide(&record.request, expected) != record.outcome
        {
            return Err(Error::Corrupt("invalid segmented durable decision".into()));
        }
        let applied = if record.outcome.conflict.is_none() {
            record.request.mutations.as_slice()
        } else {
            &[]
        };
        self.retry.advance(expected, applied);
        self.retry.retain(&record.request, record.outcome)?;
        self.apply_tail(expected, applied);
        self.sequence = expected;
        Ok(())
    }

    fn apply_tail(&mut self, sequence: u64, mutations: &[Mutation]) {
        for mutation in mutations {
            match mutation {
                Mutation::Put {
                    id,
                    vector,
                    metadata,
                } => {
                    self.tail.insert(
                        *id,
                        (
                            sequence,
                            Some(Document {
                                vector: vector.clone(),
                                metadata: metadata.clone(),
                            }),
                        ),
                    );
                }
                Mutation::Delete { id } => {
                    self.tail.insert(*id, (sequence, None));
                }
            }
        }
    }

    pub fn apply_request(&mut self, request: retry::Request) -> Result<retry::Outcome> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        request.validate(self.config)?;
        if let Some(outcome) = self.retry.duplicate(&request, self.sequence)? {
            return Ok(outcome);
        }
        if self.tail_objects >= MAX_TAIL_OBJECTS {
            return Err(Error::MaintenanceRequired);
        }
        let next = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("sequence exhausted".into()))?;
        let outcome = self.retry.decide(&request, next);
        let bytes = encode(&LogRecordV1 {
            version: 1,
            sequence: next,
            request: request.clone(),
            outcome,
        })?;
        self.poisoned = true;
        let object = log_key(next);
        self.store.create(&object, &bytes)?;
        self.known_keys.insert(object);
        let applied = if outcome.conflict.is_none() {
            request.mutations.as_slice()
        } else {
            &[]
        };
        self.retry.advance(next, applied);
        self.retry.retain(&request, outcome)?;
        self.apply_tail(next, applied);
        self.sequence = next;
        self.tail_objects += 1;
        self.poisoned = false;
        Ok(outcome)
    }

    pub fn get(&self, id: u64) -> Result<Option<OwnedDocument>> {
        if let Some((_, document)) = self.tail.get(&id) {
            return Ok(document.as_ref().map(|document| OwnedDocument {
                vector: document.vector.clone(),
                metadata: document.metadata.clone(),
            }));
        }
        let Some(location) = self.latest.get(&id) else {
            return Ok(None);
        };
        if location.entry.deleted {
            return Ok(None);
        }
        let run = &self.root.runs[location.run];
        let block = read_block(
            &self.store,
            self.config,
            &run.blocks[location.entry.block as usize],
        )?;
        let record = block
            .records
            .binary_search_by_key(&id, BlockRecord::id)
            .ok()
            .and_then(|index| block.records.get(index))
            .ok_or_else(|| Error::Corrupt("segmented directory entry missing in block".into()))?;
        if record.sequence != location.entry.sequence {
            return Err(Error::Corrupt(
                "segmented directory sequence mismatch".into(),
            ));
        }
        match &record.mutation {
            Mutation::Put {
                vector, metadata, ..
            } => Ok(Some(OwnedDocument {
                vector: vector.clone(),
                metadata: metadata.clone(),
            })),
            Mutation::Delete { .. } => Err(Error::Corrupt(
                "segmented live directory points to tombstone".into(),
            )),
        }
    }

    /// Exact correctness oracle. Reads committed blocks on demand without
    /// retaining their vectors; it may require many remote range GETs.
    pub fn search_exact(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> Result<Vec<Neighbor>> {
        self.config.vector(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut heap = BinaryHeap::new();
        let expected = self
            .latest
            .keys()
            .filter(|id| !self.tail.contains_key(id))
            .count();
        let mut seen = 0;
        for (run_ordinal, run) in self.root.runs.iter().enumerate() {
            for (block_ordinal, reference) in run.blocks.iter().enumerate() {
                let block = read_block(&self.store, self.config, reference)?;
                for record in block.records {
                    let id = record.id();
                    if self.tail.contains_key(&id) {
                        continue;
                    }
                    let Some(location) = self.latest.get(&id) else {
                        continue;
                    };
                    if location.run != run_ordinal
                        || location.entry.block as usize != block_ordinal
                        || location.entry.sequence != record.sequence
                    {
                        continue;
                    }
                    if location.entry.deleted != matches!(record.mutation, Mutation::Delete { .. })
                    {
                        return Err(Error::Corrupt(
                            "segmented directory deletion flag mismatch".into(),
                        ));
                    }
                    seen += 1;
                    if let Mutation::Put {
                        vector, metadata, ..
                    } = record.mutation
                    {
                        if matches_filter(&metadata, filter) {
                            consider(&mut heap, k, self.config, query, id, &vector);
                        }
                    }
                }
            }
        }
        if seen != expected {
            return Err(Error::Corrupt(
                "segmented directory entry missing from blocks".into(),
            ));
        }
        for (&id, (_, document)) in &self.tail {
            if let Some(document) = document {
                if matches_filter(&document.metadata, filter) {
                    consider(&mut heap, k, self.config, query, id, &document.vector);
                }
            }
        }
        let mut results: Vec<_> = heap.into_iter().map(|ranked| ranked.0).collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        Ok(results)
    }

    /// Freeze an acknowledged prefix for publication. Subsequent writes may
    /// continue in the log tail while bounded maintenance steps publish it.
    pub fn start_seal(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.seal.is_some() {
            return Err(Error::MaintenanceRequired);
        }
        if self.tail_objects == 0 {
            return Ok(());
        }
        let first_sequence = self.root.sequence + 1;
        let boundary = self.sequence;
        let empty = Block {
            version: 1,
            config: self.config,
            partition: 0,
            records: Vec::new(),
        };
        let empty_size = encode(&empty)?.len();
        let mut blocks = Vec::new();
        let mut current = Vec::new();
        let mut size = empty_size;
        for (&id, &(sequence, ref document)) in &self.tail {
            let mutation = match document {
                Some(document) => Mutation::Put {
                    id,
                    vector: document.vector.clone(),
                    metadata: document.metadata.clone(),
                },
                None => Mutation::Delete { id },
            };
            let record = BlockRecord { sequence, mutation };
            let row_size = encode(&record)?.len();
            if empty_size
                .checked_add(row_size)
                .is_none_or(|n| n > MAX_BLOCK_BYTES)
            {
                return Err(Error::Invalid(format!(
                    "segmented row {id} exceeds block limit"
                )));
            }
            let additional = row_size + usize::from(!current.is_empty());
            if size + additional > MAX_BLOCK_BYTES {
                blocks.push(Block::new(self.config, 0, std::mem::take(&mut current))?);
                size = empty_size;
            }
            size += row_size + usize::from(!current.is_empty());
            current.push(record);
        }
        if !current.is_empty() {
            blocks.push(Block::new(self.config, 0, current)?);
        }
        let mut entries = Vec::new();
        for (ordinal, block) in blocks.iter().enumerate() {
            let block_ordinal = u32::try_from(ordinal)
                .map_err(|_| Error::Invalid("too many segmented blocks".into()))?;
            for record in &block.records {
                entries.push(IndexEntry {
                    id: record.id(),
                    sequence: record.sequence,
                    block: block_ordinal,
                    deleted: matches!(record.mutation, Mutation::Delete { .. }),
                });
            }
        }
        if !blocks.is_empty() && self.root.runs.len() >= 64 {
            return Err(Error::MaintenanceRequired);
        }
        let attempt = attempt_id()?;
        self.seal = Some(SealState {
            attempt,
            boundary,
            first_sequence,
            retry: self.retry.clone(),
            blocks,
            entries,
            references: Vec::new(),
            next_block: 0,
            next_pack: 0,
            index_published: false,
        });
        Ok(())
    }

    /// Publish at most one pack, index, or root per call. The root is the only
    /// authority switch; an uncertain create poisons the handle until reopen.
    pub fn seal_step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let Some(mut seal) = self.seal.take() else {
            return Ok(false);
        };
        if seal.next_block < seal.blocks.len() {
            let start = seal.next_block;
            let mut end = start;
            let mut bytes = 0;
            while end < seal.blocks.len() {
                let length = encode(&seal.blocks[end])?.len();
                if end > start && bytes + length > MAX_PACK_BYTES {
                    break;
                }
                bytes += length;
                end += 1;
            }
            self.poisoned = true;
            seal.references.extend(self.publish_pack(
                &seal.attempt,
                seal.next_pack,
                &seal.blocks[start..end],
            )?);
            self.poisoned = false;
            seal.next_block = end;
            seal.next_pack += 1;
            self.seal = Some(seal);
            return Ok(true);
        }
        if !seal.entries.is_empty() && !seal.index_published {
            let index = RunIndex {
                sequence: seal.boundary,
                entries: seal.entries.clone(),
            };
            let bytes = index.encode(seal.first_sequence, seal.references.len())?;
            let key = format!("sgindex-{}", seal.attempt);
            self.poisoned = true;
            self.create_staged(&key, &bytes)?;
            self.poisoned = false;
            seal.index_published = true;
            self.seal = Some(seal);
            return Ok(true);
        }
        let mut root = self.root.clone();
        root.generation = self
            .root
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segmented root generation exhausted".into()))?;
        root.sequence = seal.boundary;
        root.retry = seal.retry;
        if !seal.entries.is_empty() {
            let index = RunIndex {
                sequence: seal.boundary,
                entries: seal.entries.clone(),
            };
            let bytes = index.encode(seal.first_sequence, seal.references.len())?;
            root.runs.push(RunRef {
                first_sequence: seal.first_sequence,
                last_sequence: seal.boundary,
                index_object: format!("sgindex-{}", seal.attempt),
                index_len: bytes.len(),
                index_sha256: format!("{:x}", Sha256::digest(&bytes)),
                blocks: seal.references,
            });
        }
        root.validate(self.config)?;
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &encode(&root)?)?;
        self.poisoned = false;
        if !seal.entries.is_empty() {
            let run = root.runs.len() - 1;
            for entry in seal.entries {
                self.latest.insert(entry.id, Location { run, entry });
            }
        }
        self.tail
            .retain(|_, (sequence, _)| *sequence > seal.boundary);
        self.tail_objects -= (seal.boundary - self.root.sequence) as usize;
        self.root = root;
        self.schedule_obsolete();
        Ok(true)
    }

    /// Synchronous helper for callers that can tolerate completing all steps.
    pub fn seal_delta(&mut self) -> Result<()> {
        self.start_seal()?;
        while self.seal.is_some() {
            self.seal_step()?;
        }
        Ok(())
    }

    /// Coalesce one adjacent pair of small runs when the older index is no
    /// larger than the newer index's size tier. The new index
    /// reuses authenticated immutable blocks and drops block references with
    /// no surviving ID. Physical packs with mixed live/stale rows remain.
    /// The <=1 MiB index cap bounds this synchronous maintenance step; larger
    /// data reclamation needs a separate staged protocol.
    pub fn consolidate_runs_step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.seal.is_some() {
            return Err(Error::MaintenanceRequired);
        }
        let runs = &self.root.runs;
        let pair = (0..runs.len().saturating_sub(1)).rev().find(|&i| {
            let left = (runs[i].index_len - 24) / 24;
            let right = (runs[i + 1].index_len - 24) / 24;
            left.ilog2() <= right.ilog2()
                && runs[i]
                    .index_len
                    .checked_add(runs[i + 1].index_len)
                    .is_some_and(|bytes| bytes <= MAX_CONSOLIDATION_INDEX_BYTES)
        });
        let Some(pair) = pair else {
            return Ok(false);
        };
        let first = &self.root.runs[pair];
        let second = &self.root.runs[pair + 1];
        let left = read_run_index(&self.store, first)?;
        let right = read_run_index(&self.store, second)?;
        let mut latest_pair = BTreeMap::new();
        for entry in left.entries {
            if self.latest.get(&entry.id).is_some_and(|at| at.run == pair) {
                latest_pair.insert(entry.id, (pair, entry));
            }
        }
        for entry in right.entries {
            if self
                .latest
                .get(&entry.id)
                .is_some_and(|at| at.run == pair + 1)
            {
                latest_pair.insert(entry.id, (pair + 1, entry));
            }
        }
        let mut blocks = Vec::new();
        let mut block_map = BTreeMap::new();
        let mut entries = Vec::new();
        for (id, (run_ordinal, mut entry)) in latest_pair {
            if pair == 0 && entry.deleted {
                continue;
            }
            let ordinal = *block_map
                .entry((run_ordinal, entry.block))
                .or_insert_with(|| {
                    let reference =
                        self.root.runs[run_ordinal].blocks[entry.block as usize].clone();
                    blocks.push(reference);
                    u32::try_from(blocks.len() - 1).expect("bounded block reference count")
                });
            entry.block = ordinal;
            debug_assert_eq!(id, entry.id);
            entries.push(entry);
        }
        let mut root = self.root.clone();
        root.generation = root
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segmented root generation exhausted".into()))?;
        let attempt = attempt_id()?;
        let replacement = if entries.is_empty() {
            None
        } else {
            let index = RunIndex {
                sequence: second.last_sequence,
                entries: entries.clone(),
            };
            let bytes = index.encode(first.first_sequence, blocks.len())?;
            let object = format!("sgindex-{attempt}");
            Some((
                RunRef {
                    first_sequence: first.first_sequence,
                    last_sequence: second.last_sequence,
                    index_object: object,
                    index_len: bytes.len(),
                    index_sha256: format!("{:x}", Sha256::digest(&bytes)),
                    blocks,
                },
                bytes,
            ))
        };
        root.runs.splice(
            pair..pair + 2,
            replacement.as_ref().map(|(run, _)| run.clone()),
        );
        root.validate(self.config)?;
        let root_bytes = encode(&root)?;
        if let Some((run, bytes)) = &replacement {
            self.poisoned = true;
            self.create_staged(&run.index_object, bytes)?;
            self.poisoned = false;
        }
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &root_bytes)?;
        self.poisoned = false;
        let replacement_count = usize::from(replacement.is_some());
        self.latest.retain(|id, location| {
            if location.run == pair || location.run == pair + 1 {
                if let Ok(index) = entries.binary_search_by_key(id, |entry| entry.id) {
                    location.run = pair;
                    location.entry = entries[index];
                    true
                } else {
                    false
                }
            } else {
                if location.run > pair + 1 {
                    location.run -= 2 - replacement_count;
                }
                true
            }
        });
        self.root = root;
        self.schedule_obsolete();
        Ok(true)
    }

    /// Remove at most `max_objects` objects after the selected root has made
    /// them obsolete. A DELETE error is uncertain and poisons this handle.
    pub fn cleanup_step(&mut self, max_objects: usize) -> Result<usize> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if max_objects == 0 {
            return Err(Error::Invalid("cleanup step must allow an object".into()));
        }
        let keys: Vec<_> = self.obsolete.iter().take(max_objects).cloned().collect();
        if keys.is_empty() {
            return Ok(0);
        }
        self.poisoned = true;
        self.store.remove_many(&keys)?;
        for key in &keys {
            self.known_keys.remove(key);
            self.obsolete.pop_front();
        }
        self.poisoned = false;
        Ok(keys.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{store::LocalStore, Metric};
    use std::collections::BTreeMap;

    #[test]
    fn packed_blocks_round_trip_and_reject_corrupt_references() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let block = Block::new(
            config,
            7,
            vec![BlockRecord {
                sequence: 1,
                mutation: Mutation::Put {
                    id: 42,
                    vector: vec![1., 2.],
                    metadata: BTreeMap::new(),
                },
            }],
        )
        .unwrap();
        let (payload, refs) = encode_pack("pack-1", config, &[block]).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mut store = LocalStore::open(temp.path().join("db")).unwrap();
        store.create("pack-1", &payload).unwrap();
        let loaded = read_block(&store, config, &refs[0]).unwrap();
        assert_eq!(loaded.records[0].id(), 42);
        let mut wrong = refs[0].clone();
        wrong.sha256 = "0".repeat(64);
        assert!(matches!(
            read_block(&store, config, &wrong),
            Err(Error::Corrupt(_))
        ));
        let mut missing = refs[0].clone();
        missing.object = "pack-absent".into();
        assert!(matches!(
            read_block(&store, config, &missing),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn root_and_fixed_width_directory_validate_before_data_reads() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let block = Block::new(
            config,
            7,
            vec![BlockRecord {
                sequence: 1,
                mutation: Mutation::Put {
                    id: 42,
                    vector: vec![1., 2.],
                    metadata: BTreeMap::new(),
                },
            }],
        )
        .unwrap();
        let (_, blocks) = encode_pack("pack-1", config, &[block]).unwrap();
        let index = RunIndex {
            sequence: 1,
            entries: vec![IndexEntry {
                id: 42,
                sequence: 1,
                block: 0,
                deleted: false,
            }],
        };
        let bytes = index.encode(1, blocks.len()).unwrap();
        assert_eq!(
            RunIndex::decode(&bytes, 1, 1, 1).unwrap().entries,
            index.entries
        );
        let mut corrupt = bytes.clone();
        corrupt[44] = 2;
        assert!(RunIndex::decode(&corrupt, 1, 1, 1).is_err());
        let temp = tempfile::tempdir().unwrap();
        let mut store = LocalStore::open(temp.path().join("db")).unwrap();
        store.create("index-1", &bytes).unwrap();
        let run = RunRef {
            first_sequence: 1,
            last_sequence: 1,
            index_object: "index-1".into(),
            index_len: bytes.len(),
            index_sha256: format!("{:x}", Sha256::digest(&bytes)),
            blocks,
        };
        let mut root = Root::empty(config);
        root.sequence = 1;
        root.runs.push(run.clone());
        root.validate(config).unwrap();
        assert_eq!(read_run_index(&store, &run).unwrap().entries, index.entries);
        let mut wrong = run;
        wrong.index_sha256 = "0".repeat(64);
        assert!(read_run_index(&store, &wrong).is_err());
    }

    #[test]
    fn durable_log_replays_without_loading_vectors_from_a_root() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        let put = retry::Request {
            id: retry::RequestId {
                boundary: 0,
                nonce: [1; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: 42,
                vector: vec![1., 2.],
                metadata: BTreeMap::from([("kind".into(), "test".into())]),
            }],
        };
        assert_eq!(db.apply_request(put.clone()).unwrap().sequence, 1);
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![1., 2.]);
        drop(db);
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.apply_request(put).unwrap().sequence, 1);
        assert_eq!(db.get(42).unwrap().unwrap().metadata["kind"], "test");
        let delete = retry::Request {
            id: retry::RequestId {
                boundary: 1,
                nonce: [2; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Delete { id: 42 }],
        };
        assert_eq!(db.apply_request(delete).unwrap().sequence, 2);
        assert!(db.get(42).unwrap().is_none());
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert!(db.get(42).unwrap().is_none());
        assert!(db.search_exact(&[1., 2.], 10, &[]).unwrap().is_empty());
    }

    #[test]
    fn sealed_delta_survives_reopen_and_reclaims_only_unreferenced_objects() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        let request = |boundary, nonce, mutation| retry::Request {
            id: retry::RequestId {
                boundary,
                nonce: [nonce; 16],
            },
            conditions: Vec::new(),
            mutations: vec![mutation],
        };
        let put = request(
            0,
            1,
            Mutation::Put {
                id: 42,
                vector: vec![1., 2.],
                metadata: BTreeMap::new(),
            },
        );
        db.apply_request(put.clone()).unwrap();
        db.seal_delta().unwrap();
        assert_eq!(db.root.runs.len(), 1);
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![1., 2.]);
        drop(db);

        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.apply_request(put).unwrap().sequence, 1);
        let conflict = retry::Request {
            id: retry::RequestId {
                boundary: 1,
                nonce: [2; 16],
            },
            conditions: vec![retry::Revision {
                id: 42,
                boundary: 0,
            }],
            mutations: vec![Mutation::Delete { id: 42 }],
        };
        assert_eq!(
            db.apply_request(conflict.clone()).unwrap().conflict,
            Some(retry::Conflict::StaleRevision)
        );
        db.seal_delta().unwrap();
        assert_eq!(db.root.runs.len(), 1);
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![1., 2.]);
        assert!(db.cleanup_step(1).unwrap() <= 1);
        while db.cleanup_step(2).unwrap() != 0 {}
        drop(db);

        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.apply_request(conflict).unwrap().sequence, 2);
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![1., 2.]);
        db.apply_request(request(2, 3, Mutation::Delete { id: 42 }))
            .unwrap();
        db.seal_delta().unwrap();
        assert!(db.get(42).unwrap().is_none());
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert!(db.get(42).unwrap().is_none());
    }

    #[test]
    fn sealing_a_fixed_prefix_preserves_newer_acknowledged_writes() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        let put = |boundary, nonce, id, value| retry::Request {
            id: retry::RequestId {
                boundary,
                nonce: [nonce; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id,
                vector: vec![value, 2.],
                metadata: BTreeMap::new(),
            }],
        };
        db.apply_request(put(0, 1, 42, 1.)).unwrap();
        db.start_seal().unwrap();
        assert!(db.seal_step().unwrap()); // pack for sequence 1
        db.apply_request(put(1, 2, 42, 3.)).unwrap();
        assert!(db.seal_step().unwrap()); // index for sequence 1
        db.apply_request(put(2, 3, 7, 4.)).unwrap();
        assert!(db.seal_step().unwrap()); // root for sequence 1
        assert_eq!(db.root.sequence, 1);
        assert_eq!(db.tail_objects, 2);
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![3., 2.]);
        assert_eq!(db.get(7).unwrap().unwrap().vector, vec![4., 2.]);
        drop(db);
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![3., 2.]);
        assert_eq!(db.get(7).unwrap().unwrap().vector, vec![4., 2.]);
        db.seal_delta().unwrap();
        while db.cleanup_step(4).unwrap() != 0 {}
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.get(42).unwrap().unwrap().vector, vec![3., 2.]);
        assert_eq!(db.get(7).unwrap().unwrap().vector, vec![4., 2.]);
        assert_eq!(
            db.search_exact(&[3., 2.], 2, &[]).unwrap(),
            vec![
                Neighbor {
                    id: 42,
                    distance: 0.
                },
                Neighbor {
                    id: 7,
                    distance: 1.
                },
            ]
        );
        assert!(db
            .search_exact(&[3., 2.], 2, &[("kind", "missing")])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn run_consolidation_discards_shadowed_rows_and_oldest_tombstones() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        for (index, mutation) in [
            Mutation::Put {
                id: 1,
                vector: vec![1., 0.],
                metadata: BTreeMap::new(),
            },
            Mutation::Put {
                id: 2,
                vector: vec![2., 0.],
                metadata: BTreeMap::new(),
            },
            Mutation::Delete { id: 1 },
            Mutation::Put {
                id: 3,
                vector: vec![3., 0.],
                metadata: BTreeMap::new(),
            },
        ]
        .into_iter()
        .enumerate()
        {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: index as u64,
                    nonce: [index as u8; 16],
                },
                conditions: Vec::new(),
                mutations: vec![mutation],
            })
            .unwrap();
            db.seal_delta().unwrap();
        }
        assert_eq!(db.root.runs.len(), 4);
        assert!(db.consolidate_runs_step().unwrap());
        assert!(db.consolidate_runs_step().unwrap());
        assert!(db.consolidate_runs_step().unwrap());
        assert_eq!(db.root.runs.len(), 1);
        assert_eq!(db.root.sequence, 4);
        assert!(!db.consolidate_runs_step().unwrap());
        assert!(db.get(1).unwrap().is_none());
        assert_eq!(db.get(2).unwrap().unwrap().vector, vec![2., 0.]);
        assert_eq!(
            db.search_exact(&[2., 0.], 10, &[]).unwrap(),
            vec![
                Neighbor {
                    id: 2,
                    distance: 0.
                },
                Neighbor {
                    id: 3,
                    distance: 1.
                },
            ]
        );
        while db.cleanup_step(16).unwrap() != 0 {}
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert!(db.get(1).unwrap().is_none());
        assert_eq!(db.get(3).unwrap().unwrap().vector, vec![3., 0.]);
        assert_eq!(db.root.runs.len(), 1);
    }

    #[cfg(feature = "s3")]
    #[test]
    #[ignore = "requires disposable MinIO from tools/test_s3.py"]
    fn minio_segmented_publication_recovers_before_and_after_root_create() {
        use crate::store::s3::{AmazonS3Builder, S3Store};

        struct UncertainCreate<S> {
            inner: S,
            fail_prefix: &'static str,
            fired: bool,
            fail_remove_once: bool,
        }
        impl<S: ObjectStore> ObjectStore for UncertainCreate<S> {
            fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
                self.inner.get(key)
            }
            fn get_range(
                &self,
                key: &str,
                offset: usize,
                length: usize,
                expected_payload_len: usize,
            ) -> Result<Option<Vec<u8>>> {
                self.inner
                    .get_range(key, offset, length, expected_payload_len)
            }
            fn list(&self) -> Result<Vec<String>> {
                self.inner.list()
            }
            fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
                self.inner.create(key, value)?;
                if !self.fired && key.starts_with(self.fail_prefix) {
                    self.fired = true;
                    return Err(Error::Io(std::io::Error::other(
                        "simulated response loss after durable create",
                    )));
                }
                Ok(())
            }
            fn remove(&mut self, key: &str) -> Result<()> {
                self.inner.remove(key)?;
                if self.fail_remove_once {
                    self.fail_remove_once = false;
                    return Err(Error::Io(std::io::Error::other(
                        "simulated response loss after durable delete",
                    )));
                }
                Ok(())
            }
        }

        let mut random = [0_u8; 8];
        getrandom::getrandom(&mut random).unwrap();
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let failed_log_namespace = format!("segmented-{}-sglog", u64::from_le_bytes(random));
        let open_log_store = || {
            S3Store::open(
                AmazonS3Builder::new()
                    .with_bucket_name(std::env::var("GLIDER_S3_BUCKET").unwrap())
                    .with_region("us-east-1")
                    .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap())
                    .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap())
                    .with_endpoint(std::env::var("GLIDER_S3_ENDPOINT").unwrap())
                    .with_allow_http(true),
                &failed_log_namespace,
            )
            .unwrap()
        };
        let request = retry::Request {
            id: retry::RequestId {
                boundary: 0,
                nonce: [9; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: 99,
                vector: vec![5., 6.],
                metadata: BTreeMap::new(),
            }],
        };
        let mut uncertain_log = SegmentedDatabase::open(
            UncertainCreate {
                inner: open_log_store(),
                fail_prefix: "sglog-",
                fired: false,
                fail_remove_once: false,
            },
            config,
        )
        .unwrap();
        assert!(matches!(
            uncertain_log.apply_request(request.clone()),
            Err(Error::Io(_))
        ));
        assert!(matches!(
            uncertain_log.apply_request(request.clone()),
            Err(Error::RecoveryRequired)
        ));
        drop(uncertain_log);
        let mut recovered_log = SegmentedDatabase::open(open_log_store(), config).unwrap();
        assert_eq!(recovered_log.apply_request(request).unwrap().sequence, 1);
        assert_eq!(recovered_log.get(99).unwrap().unwrap().vector, vec![5., 6.]);
        recovered_log.seal_delta().unwrap();
        drop(recovered_log);

        for prefix in ["sgpack-", "sgroot-00000000000000000001"] {
            let namespace = format!("segmented-{}-{prefix}", u64::from_le_bytes(random));
            let store = || {
                S3Store::open(
                    AmazonS3Builder::new()
                        .with_bucket_name(std::env::var("GLIDER_S3_BUCKET").unwrap())
                        .with_region("us-east-1")
                        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap())
                        .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap())
                        .with_endpoint(std::env::var("GLIDER_S3_ENDPOINT").unwrap())
                        .with_allow_http(true),
                    &namespace,
                )
                .unwrap()
            };
            let mut db = SegmentedDatabase::open(
                UncertainCreate {
                    inner: store(),
                    fail_prefix: prefix,
                    fired: false,
                    fail_remove_once: false,
                },
                config,
            )
            .unwrap();
            let request = retry::Request {
                id: retry::RequestId {
                    boundary: 0,
                    nonce: [1; 16],
                },
                conditions: Vec::new(),
                mutations: vec![Mutation::Put {
                    id: 42,
                    vector: vec![1., 2.],
                    metadata: BTreeMap::new(),
                }],
            };
            db.apply_request(request.clone()).unwrap();
            assert!(db.seal_delta().is_err());
            assert!(matches!(
                db.apply_request(request.clone()),
                Err(Error::RecoveryRequired)
            ));
            drop(db);
            let mut recovered = SegmentedDatabase::open(
                UncertainCreate {
                    inner: store(),
                    fail_prefix: "never-match",
                    fired: false,
                    fail_remove_once: true,
                },
                config,
            )
            .unwrap();
            assert_eq!(recovered.get(42).unwrap().unwrap().vector, vec![1., 2.]);
            assert_eq!(recovered.apply_request(request).unwrap().sequence, 1);
            if prefix == "sgpack-" {
                assert_eq!(recovered.root.sequence, 0);
            } else {
                assert_eq!(recovered.root.sequence, 1);
            }
            recovered
                .apply_request(retry::Request {
                    id: retry::RequestId {
                        boundary: 1,
                        nonce: [2; 16],
                    },
                    conditions: Vec::new(),
                    mutations: vec![Mutation::Put {
                        id: 7,
                        vector: vec![3., 4.],
                        metadata: BTreeMap::new(),
                    }],
                })
                .unwrap();
            recovered.seal_delta().unwrap();
            assert!(matches!(recovered.cleanup_step(1), Err(Error::Io(_))));
            assert!(matches!(
                recovered.cleanup_step(1),
                Err(Error::RecoveryRequired)
            ));
            drop(recovered);
            let mut reopened = SegmentedDatabase::open(store(), config).unwrap();
            assert_eq!(reopened.get(42).unwrap().unwrap().vector, vec![1., 2.]);
            assert_eq!(reopened.get(7).unwrap().unwrap().vector, vec![3., 4.]);
            while reopened.cleanup_step(8).unwrap() != 0 {}
            if prefix != "sgpack-" {
                assert_eq!(reopened.root.runs.len(), 2);
                drop(reopened);
                let mut uncertain_consolidation = SegmentedDatabase::open(
                    UncertainCreate {
                        inner: store(),
                        fail_prefix: "sgroot-00000000000000000003",
                        fired: false,
                        fail_remove_once: false,
                    },
                    config,
                )
                .unwrap();
                assert!(matches!(
                    uncertain_consolidation.consolidate_runs_step(),
                    Err(Error::Io(_))
                ));
                drop(uncertain_consolidation);
                reopened = SegmentedDatabase::open(store(), config).unwrap();
                assert_eq!(reopened.root.runs.len(), 1);
                assert_eq!(reopened.get(42).unwrap().unwrap().vector, vec![1., 2.]);
                assert_eq!(reopened.get(7).unwrap().unwrap().vector, vec![3., 4.]);
                while reopened.cleanup_step(8).unwrap() != 0 {}
            }
            let required_pack = reopened.root.runs[0].blocks[0].object.clone();
            drop(reopened);
            let mut damaged = store();
            damaged.remove(&required_pack).unwrap();
            drop(damaged);
            assert!(matches!(
                SegmentedDatabase::open(store(), config),
                Err(Error::Corrupt(_))
            ));
        }
    }
}
