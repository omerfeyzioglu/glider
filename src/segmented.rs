//! Versioned segmented layout: immutable logs, packs, run indexes and roots.
use crate::{
    decode, encode, lease::is_lease_key, matches_filter, ownership, retry, store::ObjectStore,
    streaming::OwnedDocument, Config, Document, Error, Mutation, Neighbor, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
    path::Path,
    sync::{Arc, Mutex, Weak},
};

mod cache;
use cache::{open_key, BlockCache};
mod clustered;
mod codec;
use clustered::{ClusterIndex, ExtentKind};
mod convert;
pub use convert::{automatic_centroids, ConversionProgress, ConversionSummary, ConvertOptions};
mod directory;
use directory::Directory;
mod manifest;
mod merge;
pub use merge::{ClusteredLayout, MergeSummary};
mod serving;
pub use cache::CacheStats;
pub use serving::{
    ClusteringState, SegmentedServing, SegmentedServingOptions, ServingCounters,
    DEFAULT_AUTO_CLUSTER_ROWS, DEFAULT_AUTO_RECLUSTER_FACTOR,
};
mod sketch;
use sketch::{frame, unframe, Framed, PackSketch, SketchSet, FRAME_PREFIX_READ, MAX_SKETCH_BYTES};

/// Wall time of the successive phases of a segmented open. Durations include
/// store waits and validation in that phase; the store can profile its calls
/// separately to isolate I/O from CPU work.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenProfile {
    pub list_metadata: std::time::Duration,
    pub root_manifests: std::time::Duration,
    pub run_indexes: std::time::Duration,
    pub run_index_reads: std::time::Duration,
    pub tail_replay: std::time::Duration,
    pub routing: std::time::Duration,
    pub catalog: std::time::Duration,
    pub catalog_reads: std::time::Duration,
    pub sketch_frames: std::time::Duration,
    pub sketch_reads: std::time::Duration,
    pub finish: std::time::Duration,
}
pub use sketch::{ReadBudget, SegmentedOptions};

pub(crate) const MAX_BLOCK_BYTES: usize = 128 * 1024;
pub(crate) const MAX_PACK_BYTES: usize = 1024 * 1024;
/// Sealed packs hold at most this many blocks, which bounds the region each
/// per-pack sketch codebook covers.
const MAX_PACK_BLOCKS: usize = 12;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024;
/// Two run indexes are merged only while their sum stays within this bound,
/// which caps a consolidation unit's transient memory and time.
const MAX_CONSOLIDATION_INDEX_BYTES: usize = 2 * 1024 * 1024;
const INDEX_MAGIC: &[u8; 8] = b"GLRIDX01";
const MAX_TAIL_OBJECTS: usize = 64;
/// Postings a clustered query probes unless configured otherwise: the M37
/// stage 2 measurements reached the quality gates with 16.
pub const DEFAULT_CLUSTER_PROBES: usize = 16;

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

/// Group a bounded sealed prefix by vector proximity without changing the
/// authoritative ID index: returns the IDs of each block, each sorted by ID,
/// put blocks first. Rows are borrowed as `(id, vector, raw length)`;
/// deletes as `(id, raw length)`.
fn plan_vector_local(
    config: Config,
    puts: &[(u64, &[f32], usize)],
    deletes: &[(u64, usize)],
) -> Result<Vec<Vec<u64>>> {
    const TARGET_ROWS: usize = 170;
    let id = |index: usize| puts[index].0;
    let vector = |index: usize| puts[index].1;
    let empty_size = codec::block_len([]);
    let mut planned = Vec::new();
    let mut groups = vec![(0..puts.len()).collect::<Vec<_>>()];
    while let Some(mut group) = groups.pop() {
        if group.is_empty() {
            continue;
        }
        if group.len() <= TARGET_ROWS {
            group.sort_unstable_by_key(|&index| id(index));
            let size = empty_size + group.iter().map(|&index| puts[index].2).sum::<usize>();
            if size <= codec::MAX_RAW_BLOCK_BYTES {
                planned.push(group.iter().map(|&index| id(index)).collect());
                continue;
            }
            if group.len() == 1 {
                debug_assert!(!codec::row_fits(puts[group[0]].2));
                return Err(Error::Invalid(format!(
                    "segmented row {} exceeds block limit",
                    id(group[0])
                )));
            }
        }
        let leaves = group.len().div_ceil(TARGET_ROWS).max(2);
        let left_leaves = leaves / 2;
        let left_len = ((group.len() as u128 * left_leaves as u128) / leaves as u128) as usize;
        let mut means = vec![0_f64; config.dimensions];
        let mut squares = vec![0_f64; config.dimensions];
        for &index in &group {
            for (axis, &value) in vector(index).iter().enumerate() {
                let value = f64::from(value);
                means[axis] += value;
                squares[axis] += value * value;
            }
        }
        let axis = (0..config.dimensions)
            .max_by(|&a, &b| {
                let variance = |i| squares[i] - means[i] * means[i] / group.len() as f64;
                variance(a).total_cmp(&variance(b)).then_with(|| b.cmp(&a))
            })
            .unwrap();
        group.sort_unstable_by(|&a, &b| {
            vector(a)[axis]
                .total_cmp(&vector(b)[axis])
                .then_with(|| id(a).cmp(&id(b)))
        });
        for _ in 0..3 {
            let mut direction = vec![0_f64; config.dimensions];
            for &index in &group[..left_len] {
                for (axis, &value) in vector(index).iter().enumerate() {
                    direction[axis] -= f64::from(value) / left_len as f64;
                }
            }
            for &index in &group[left_len..] {
                for (axis, &value) in vector(index).iter().enumerate() {
                    direction[axis] += f64::from(value) / (group.len() - left_len) as f64;
                }
            }
            let mut projected: Vec<_> = group
                .iter()
                .map(|&index| {
                    let score = vector(index)
                        .iter()
                        .zip(&direction)
                        .map(|(&value, &weight)| f64::from(value) * weight)
                        .sum::<f64>();
                    (score, index)
                })
                .collect();
            projected.sort_unstable_by(|&(score_a, a), &(score_b, b)| {
                score_a.total_cmp(&score_b).then_with(|| id(a).cmp(&id(b)))
            });
            group = projected.into_iter().map(|(_, index)| index).collect();
        }
        let right = group.split_off(left_len);
        groups.push(right);
        groups.push(group);
    }
    planned.extend(plan_deletes(deletes).into_iter().map(|(ids, _)| ids));
    Ok(planned)
}

/// ID-sorted tombstone blocks within the raw block limit, with raw lengths.
fn plan_deletes(deletes: &[(u64, usize)]) -> Vec<(Vec<u64>, usize)> {
    let empty_size = codec::block_len([]);
    let mut deletes = deletes.to_vec();
    deletes.sort_unstable();
    let mut planned = Vec::new();
    let mut current = Vec::new();
    let mut size = empty_size;
    for (id, row_size) in deletes {
        if size + row_size > codec::MAX_RAW_BLOCK_BYTES {
            planned.push((std::mem::take(&mut current), size));
            size = empty_size;
        }
        size += row_size;
        current.push(id);
    }
    if !current.is_empty() {
        planned.push((current, size));
    }
    planned
}

/// Root-v1 metadata for one block inside an immutable physical pack.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    /// The run manifest holding `blocks` in a root v5. Roots v1, v2 and v4
    /// embed `blocks` instead; a root about to be published is bound by
    /// `stage_manifest`, which never trusts this field's previous value.
    #[serde(skip)]
    manifest: Option<clustered::ObjectRef>,
}

/// Keys of every takeover's fence objects: takeover records (log
/// sequences) and fence markers (root generations). A deposed writer may
/// resume at any time and its next log and root keys are exactly these, so
/// they are never reclaimed.
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fences {
    logs: BTreeSet<u64>,
    roots: BTreeSet<u64>,
}

impl Fences {
    fn is_empty(&self) -> bool {
        self.logs.is_empty() && self.roots.is_empty()
    }
}

/// Root v1 has no fences, v2 adds takeover fences, and v4 adds a clustered
/// view. Version 3 is reserved for fence markers. Version 5 (`manifest`)
/// names each run's blocks through a run manifest object instead of
/// embedding them, with optional fences and view; every publication writes
/// it. Older versions remain readable.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    version: u32,
    generation: u64,
    pub(crate) sequence: u64,
    config: Config,
    retry: retry::State,
    pub(crate) runs: Vec<RunRef>,
    #[serde(default, skip_serializing_if = "Fences::is_empty")]
    fences: Fences,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "clustered::deserialize_view_ref"
    )]
    clustered: Option<clustered::ViewRef>,
}

/// A fence marker: occupies one root generation so that a deposed writer's
/// next root publication conflicts. It holds no state; root selection skips
/// it. Version 3 shares the root keys' version field.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootFence {
    version: u32,
    generation: u64,
}

enum RootObject {
    Root(Box<Root>),
    Fence,
}

/// Decode an object at `root_key(generation)`.
fn decode_root(bytes: &[u8], config: Config, generation: u64) -> Result<RootObject> {
    let version = decode::<MetadataVersion>(bytes)?.version;
    let object = if version == 3 {
        let fence: RootFence = decode(bytes)?;
        if fence.generation != generation {
            return Err(Error::Corrupt("segmented fence generation mismatch".into()));
        }
        RootObject::Fence
    } else {
        // A root v5's runs name their blocks through manifests, which the
        // opener loads only for the selected root.
        let root = if version == 5 {
            manifest::decode_root(bytes, config)?
        } else {
            let root: Root = decode(bytes)?;
            root.validate(config)?;
            root
        };
        if root.generation != generation {
            return Err(Error::Corrupt("segmented root generation mismatch".into()));
        }
        RootObject::Root(Box::new(root))
    };
    Ok(object)
}

/// The newest root at or below the listed generations, skipping fence
/// markers above it, which it returns. `Error::Exists` means a listed object
/// vanished: only a newer root supersedes one, so the caller retries. A root
/// v5 is returned without its block lists (`manifest::load_manifests`).
fn select_root<S: ObjectStore>(
    store: &S,
    config: Config,
    generations: &BTreeSet<u64>,
) -> Result<(Root, Vec<u64>)> {
    let mut markers = Vec::new();
    for &generation in generations.iter().rev() {
        let key = root_key(generation);
        match decode_root(
            &store.get(&key)?.ok_or(Error::Exists(key))?,
            config,
            generation,
        )? {
            RootObject::Root(root) => return Ok((*root, markers)),
            RootObject::Fence => markers.push(generation),
        }
    }
    Err(Error::Corrupt("no segmented root below the fences".into()))
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
            fences: Fences::default(),
            clustered: None,
        }
    }

    /// Validate the complete root, including every run's block list.
    pub(crate) fn validate(&self, config: Config) -> Result<()> {
        self.validate_header(config)?;
        for run in &self.runs {
            if run.blocks.is_empty() {
                return Err(Error::Corrupt("invalid segmented run reference".into()));
            }
            for reference in &run.blocks {
                validate_block_ref(reference)?;
            }
        }
        Ok(())
    }

    /// Validate everything but the runs' block lists, which a decoded root
    /// v5 has not loaded yet.
    fn validate_header(&self, config: Config) -> Result<()> {
        let fences_valid = match self.version {
            1 => self.fences.is_empty() && self.clustered.is_none(),
            2 => {
                self.clustered.is_none()
                    && !self.fences.is_empty()
                    && !self.fences.logs.contains(&0)
                    && self
                        .fences
                        .roots
                        .iter()
                        .all(|&generation| generation > 0 && generation < self.generation)
            }
            4 | 5 => {
                self.generation > 0
                    && match &self.clustered {
                        Some(view) => view.validate().is_ok(),
                        None => self.version == 5,
                    }
                    && !self.fences.logs.contains(&0)
                    && self
                        .fences
                        .roots
                        .iter()
                        .all(|&generation| generation > 0 && generation < self.generation)
            }
            _ => false,
        };
        if !fences_valid || self.config != config || self.runs.len() > 64 {
            return Err(Error::Corrupt("invalid segmented root identity".into()));
        }
        self.retry.validate(self.sequence)?;
        let mut previous_sequence = 0;
        for run in &self.runs {
            if run.first_sequence == 0
                || run.first_sequence <= previous_sequence
                || run.first_sequence > run.last_sequence
                || run.last_sequence > self.sequence
                || run.index_len < 48
                || run.index_len > MAX_INDEX_BYTES
                || !(run.index_len - 24).is_multiple_of(24)
                || !valid_digest(&run.index_sha256)
            {
                return Err(Error::Corrupt("invalid segmented run reference".into()));
            }
            previous_sequence = run.last_sequence;
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
        || reference.payload_len > MAX_PACK_BYTES + sketch::FRAME_HEADER + MAX_SKETCH_BYTES
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
    decode_run_index(run, store.get(&run.index_object)?)
}

fn decode_run_index(run: &RunRef, bytes: Option<Vec<u8>>) -> Result<RunIndex> {
    let bytes = bytes
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
        let bytes = codec::encode(block)?;
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

/// Encode a pack that starts with the sketch frame for its blocks; block
/// offsets follow the frame. A posting pack passes its centers'
/// fingerprints by cluster ID and gets a `GLSKT003` sketch.
fn encode_pack_with_sketch(
    object: &str,
    config: Config,
    options: &SegmentedOptions,
    options_digest: &[u8; 32],
    blocks: &[Block],
    fingerprints: Option<&BTreeMap<u32, [u8; 32]>>,
) -> Result<(Vec<u8>, Vec<BlockRef>, PackSketch)> {
    let (payload, mut references) = encode_pack(object, config, blocks)?;
    let pairs: Vec<_> = references.iter().zip(blocks).collect();
    let sketch = PackSketch::build_with(config, options, object, &pairs, fingerprints)?;
    let encoded = sketch.encode(config, options_digest);
    if encoded.len() > MAX_SKETCH_BYTES {
        return Err(Error::Invalid(
            "segmented pack sketch exceeds byte limit".into(),
        ));
    }
    let mut bytes = frame(&encoded);
    let shift = bytes.len();
    let mut sketch = sketch;
    sketch.frame_len = Some(shift);
    bytes.extend_from_slice(&payload);
    for reference in &mut references {
        reference.offset += shift;
        reference.payload_len = bytes.len();
    }
    sketch.relocate_posting(&references);
    Ok((bytes, references, sketch))
}

/// A block's bytes within a shared fetched buffer.
pub(crate) struct Slice {
    data: std::sync::Arc<Vec<u8>>,
    start: usize,
    end: usize,
}

impl From<Vec<u8>> for Slice {
    fn from(data: Vec<u8>) -> Self {
        let end = data.len();
        Self {
            data: std::sync::Arc::new(data),
            start: 0,
            end,
        }
    }
}

impl std::ops::Deref for Slice {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.data[self.start..self.end]
    }
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
    decode_block_bytes(config, reference, &bytes)
}

/// Bytes must match the committed length and digest before decoding.
fn authenticate(reference: &BlockRef, bytes: &[u8]) -> Result<()> {
    if bytes.len() != reference.length || format!("{:x}", Sha256::digest(bytes)) != reference.sha256
    {
        return Err(Error::Corrupt(format!(
            "segmented block digest mismatch: {}",
            reference.object
        )));
    }
    Ok(())
}

/// Decode an authenticated block of version 2 (binary) or 1 (JSON).
fn decode_block_bytes(config: Config, reference: &BlockRef, bytes: &[u8]) -> Result<Block> {
    authenticate(reference, bytes)?;
    let block = decode_block(config, bytes)?;
    if block.partition != reference.partition
        || block.records.len() != reference.rows
        || block.records.first().unwrap().id() != reference.first_id
        || block.records.last().unwrap().id() != reference.last_id
    {
        return Err(Error::Corrupt("segmented block reference mismatch".into()));
    }
    Ok(block)
}

/// Decode block bytes the caller has already authenticated.
fn decode_block(config: Config, bytes: &[u8]) -> Result<Block> {
    if codec::is_v2(bytes) {
        codec::decode(config, bytes)
    } else {
        let block: Block = decode(bytes)?;
        block.validate(config)?;
        Ok(block)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataV2 {
    version: u32,
    config: Config,
}

/// Version 3 adds the namespace's derived-index declaration.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataV3 {
    version: u32,
    config: Config,
    options: SegmentedOptions,
}

/// Version 4 declares routed equality keys as well as resident state.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataV4 {
    version: u32,
    config: Config,
    options: SegmentedOptions,
}

#[derive(Deserialize)]
struct MetadataVersion {
    version: u32,
}

fn metadata_bytes(config: Config, options: &SegmentedOptions) -> Result<Vec<u8>> {
    if !options.routed_keys.is_empty() {
        encode(&MetadataV4 {
            version: 4,
            config,
            options: options.clone(),
        })
    } else if *options == SegmentedOptions::default() {
        encode(&MetadataV2 { version: 2, config })
    } else {
        encode(&MetadataV3 {
            version: 3,
            config,
            options: options.clone(),
        })
    }
}

fn check_metadata(bytes: &[u8], config: Config, options: &SegmentedOptions) -> Result<()> {
    let (stored_config, stored_options) = match decode::<MetadataVersion>(bytes)?.version {
        1 => {
            return Err(Error::Invalid(
                "namespace uses the resident Database format (metadata v1)".into(),
            ))
        }
        2 => {
            let metadata: MetadataV2 = decode(bytes)?;
            (metadata.config, SegmentedOptions::default())
        }
        3 => {
            let metadata: MetadataV3 = decode(bytes)?;
            if !metadata.options.routed_keys.is_empty() {
                return Err(Error::Corrupt("metadata v3 contains routed keys".into()));
            }
            (metadata.config, metadata.options)
        }
        4 => {
            let metadata: MetadataV4 = decode(bytes)?;
            if metadata.options.routed_keys.is_empty() {
                return Err(Error::Corrupt("metadata v4 has no routed keys".into()));
            }
            (metadata.config, metadata.options)
        }
        version => {
            return Err(Error::Corrupt(format!(
                "unsupported segmented metadata version {version}"
            )))
        }
    };
    if stored_config != config || stored_options != *options {
        return Err(Error::Invalid(format!(
            "namespace was created with {stored_config:?} and {stored_options:?}, \
             not {config:?} and {options:?}"
        )));
    }
    Ok(())
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

/// Log version 2: several independent requests published by one
/// conditional create, with consecutive sequences starting at
/// `first_sequence`. Its key is the first sequence; a single request still
/// uses version 1.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRecordV2 {
    version: u32,
    first_sequence: u64,
    entries: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogEntry {
    request: retry::Request,
    outcome: retry::Outcome,
}

/// Log version 3: a writer takeover record. It carries no request and
/// changes no document; its sequence is the new writer's epoch. Occupying
/// that key fences every earlier writer, whose next log key it is.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TakeoverRecord {
    version: u32,
    sequence: u64,
}

enum LogObject {
    Requests(Vec<LogRecordV1>),
    Takeover,
}

impl LogObject {
    fn last_sequence(&self, first_sequence: u64) -> u64 {
        match self {
            LogObject::Requests(records) => first_sequence + records.len() as u64 - 1,
            LogObject::Takeover => first_sequence,
        }
    }
}

/// Decode a log object of any version; request entries are in order.
fn decode_log(bytes: &[u8], first_sequence: u64) -> Result<LogObject> {
    match decode::<MetadataVersion>(bytes)?.version {
        1 => Ok(LogObject::Requests(vec![decode(bytes)?])),
        2 => {
            let record: LogRecordV2 = decode(bytes)?;
            if record.first_sequence != first_sequence || record.entries.len() < 2 {
                return Err(Error::Corrupt("invalid segmented log group".into()));
            }
            Ok(LogObject::Requests(
                record
                    .entries
                    .into_iter()
                    .enumerate()
                    .map(|(offset, entry)| LogRecordV1 {
                        version: 1,
                        sequence: first_sequence + offset as u64,
                        request: entry.request,
                        outcome: entry.outcome,
                    })
                    .collect(),
            ))
        }
        3 => {
            let record: TakeoverRecord = decode(bytes)?;
            if record.sequence != first_sequence {
                return Err(Error::Corrupt("invalid segmented takeover record".into()));
            }
            Ok(LogObject::Takeover)
        }
        _ => Err(Error::Corrupt("unsupported segmented log version".into())),
    }
}

/// Keys that are not database state: lease objects and the claim objects of
/// the earlier ownership protocol, which takeover fencing supersedes.
fn is_control_key(key: &str) -> bool {
    is_lease_key(key) || ownership::is_control_key(key)
}

/// Fence every earlier writer of an initialized namespace whose state keys
/// are `keys`: publish a takeover record at the next log sequence, then a
/// fence marker at the next root generation. A writer publishes logs and
/// roots only at the key after its newest one, with a conditional create,
/// so a deposed writer's next publication on either chain conflicts; both
/// objects are permanent, so it conflicts however late it resumes.
///
/// `Error::Exists` means another writer published or superseded an object
/// after the listing; retry with a fresh listing.
fn fence<S: ObjectStore>(store: &S, config: Config, keys: &[String]) -> Result<()> {
    let mut generations = BTreeSet::new();
    let mut last_log = 0;
    for key in keys {
        if key.starts_with("sgroot-") {
            generations.insert(numbered_key(key, "sgroot-")?);
        } else if key.starts_with("sglog-") {
            last_log = last_log.max(numbered_key(key, "sglog-")?);
        }
    }
    let (root, _) = select_root(store, config, &generations)?;
    let mut sequence = root.sequence;
    if last_log > root.sequence {
        // Like a root, a listed log vanishes only under a newer root.
        let object = log_key(last_log);
        let bytes = store.get(&object)?.ok_or(Error::Exists(object))?;
        sequence = decode_log(&bytes, last_log)?.last_sequence(last_log);
    }
    let epoch = sequence
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("sequence exhausted".into()))?;
    store.create(
        &log_key(epoch),
        &encode(&TakeoverRecord {
            version: 3,
            sequence: epoch,
        })?,
    )?;
    let generation = generations
        .last()
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("segmented root generation exhausted".into()))?;
    store.create(
        &root_key(generation),
        &encode(&RootFence {
            version: 3,
            generation,
        })?,
    )
}

/// Reject options a namespace cannot declare.
fn validate_options(options: &SegmentedOptions) -> Result<()> {
    if options
        .resident_filter
        .as_ref()
        .is_some_and(|(key, _)| key.is_empty())
    {
        return Err(Error::Invalid(
            "resident filter key must be nonempty".into(),
        ));
    }
    if options.routed_keys.len() > 4
        || options.routed_keys.iter().any(String::is_empty)
        || options
            .routed_keys
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(Error::Invalid(
            "routed keys must be sorted, unique, nonempty and at most four".into(),
        ));
    }
    Ok(())
}

/// List the namespace's state keys, sorted, after creating metadata and
/// root zero on a fresh (or interrupted first) open, and check the stored
/// metadata against `config` and `options`.
fn initialize<S: ObjectStore>(
    store: &S,
    config: Config,
    options: &SegmentedOptions,
) -> Result<Vec<String>> {
    let mut keys: Vec<_> = store
        .list()?
        .into_iter()
        .filter(|key| !is_control_key(key))
        .collect();
    keys.sort();
    if keys.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::Corrupt("duplicate segmented object key".into()));
    }
    if keys.is_empty() {
        store.create("metadata", &metadata_bytes(config, options)?)?;
        store.create(&root_key(0), &encode(&Root::empty(config))?)?;
        return Ok(vec!["metadata".into(), root_key(0)]);
    }
    if keys.iter().filter(|key| key.as_str() == "metadata").count() != 1 {
        return Err(Error::Corrupt("segmented metadata missing".into()));
    }
    check_metadata(
        &store
            .get("metadata")?
            .ok_or_else(|| Error::Corrupt("listed segmented metadata missing".into()))?,
        config,
        options,
    )?;
    if keys == ["metadata"] {
        // An interrupted first open may have published metadata only.
        store.create(&root_key(0), &encode(&Root::empty(config))?)?;
        keys.push(root_key(0));
    }
    if !keys.contains(&root_key(0)) {
        return Err(Error::Corrupt("segmented root zero missing".into()));
    }
    Ok(keys)
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

/// Bit `index` of a bitset; false beyond its end.
fn bit(bits: &[u64], index: usize) -> bool {
    bits.get(index / 64)
        .is_some_and(|word| word & (1 << (index % 64)) != 0)
}

#[derive(Clone, Copy)]
struct Location {
    run: usize,
    entry: IndexEntry,
}

struct PackStats {
    payload_len: usize,
    /// Where the pack's block data starts: after its sketch frame when that
    /// length is known, else at the smallest referenced block offset.
    data_start: usize,
    estimated_live_bytes: usize,
    live_rows: usize,
    locations: Vec<(usize, usize)>,
    has_empty_block: bool,
}

struct ReclaimState {
    locations: Vec<(usize, usize)>,
    expected: Vec<BTreeMap<u64, (u64, bool)>>,
    blocks: Vec<Block>,
    key: String,
    references: Option<Vec<BlockRef>>,
    sketch: Option<PackSketch>,
}

struct PruneState {
    run: usize,
    root: Root,
    entries: Vec<IndexEntry>,
    index: Option<(String, Vec<u8>)>,
    index_published: bool,
}

/// Fields requested for each query hit. Both default to false.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryOptions {
    pub include_metadata: bool,
    pub include_vector: bool,
}

/// A neighbor and optional fields from the version used to score it.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryHit {
    pub id: u64,
    pub distance: f64,
    pub metadata: Option<BTreeMap<String, String>>,
    pub vector: Option<Vec<f32>>,
}

impl QueryHit {
    pub fn neighbor(&self) -> Neighbor {
        Neighbor {
            id: self.id,
            distance: self.distance,
        }
    }
}

struct Ranked(QueryHit);
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
    consider_with(heap, k, config, query, id, vector, || (None, None));
}

fn consider_with(
    heap: &mut BinaryHeap<Ranked>,
    k: usize,
    config: Config,
    query: &[f32],
    id: u64,
    vector: &[f32],
    fields: impl FnOnce() -> (Option<BTreeMap<String, String>>, Option<Vec<f32>>),
) {
    let distance = config.metric.score(query, vector);
    if heap.len() < k
        || heap.peek().is_some_and(|worst| {
            distance
                .total_cmp(&worst.0.distance)
                .then(id.cmp(&worst.0.id))
                .is_lt()
        })
    {
        let (metadata, vector) = fields();
        if heap.len() == k {
            heap.pop();
        }
        heap.push(Ranked(QueryHit {
            id,
            distance,
            metadata,
            vector,
        }));
    }
}

struct SealState {
    attempt: String,
    boundary: u64,
    first_sequence: u64,
    retry: retry::State,
    /// Planned block membership by ID; records are materialized per pack.
    blocks: Vec<Vec<u64>>,
    /// Present when the seal assigns puts to a loaded clustered view.
    clustered: Option<ClusteredSeal>,
    /// Sealed versions a newer tail write displaced during this seal.
    displaced: BTreeMap<u64, (u64, Option<Arc<Document>>)>,
    entries: Vec<IndexEntry>,
    references: Vec<BlockRef>,
    sketches: Vec<PackSketch>,
    next_block: usize,
    next_pack: u32,
    index_published: bool,
}

/// An M37 clustered seal: puts are planned as cluster-contiguous posting
/// blocks (canonical extents of the view) and tombstones as ID-sorted blocks
/// in separate packs.
struct ClusteredSeal {
    view: Arc<ClusterIndex>,
    /// Each planned block's partition: its cluster ID, or 0 for tombstones.
    partitions: Vec<u32>,
    /// Planned packs as block ranges, flagged when they are posting packs.
    packs: Vec<(std::ops::Range<usize>, bool)>,
    /// The staged catalog and the view it selects.
    catalog: Option<(clustered::ObjectRef, ClusterIndex)>,
}

/// Plan a clustered seal: assign each put to its nearest center (ties by
/// cluster ID), lay clusters out in ID order as posting packs, then pack the
/// ID-sorted tombstone blocks. Returns each block's IDs in block order.
fn plan_clustered_seal(
    config: Config,
    view: Arc<ClusterIndex>,
    puts: &[(u64, &[f32], usize)],
    deletes: &[(u64, usize)],
) -> Result<(Vec<Vec<u64>>, ClusteredSeal)> {
    let vectors: Vec<&[f32]> = puts.iter().map(|put| put.1).collect();
    let mut groups: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (index, cluster) in view.assign(config.metric, &vectors).into_iter().enumerate() {
        groups.entry(cluster).or_default().push(index);
    }
    let groups: Vec<(u32, Vec<usize>)> = groups.into_iter().collect();
    let lengths: Vec<Vec<usize>> = groups
        .iter()
        .map(|(_, rows)| rows.iter().map(|&index| puts[index].2).collect())
        .collect();
    let mut blocks = Vec::new();
    let mut partitions = Vec::new();
    let mut packs = Vec::new();
    for planned in convert::plan_layout(&lengths)? {
        let start = blocks.len();
        for (group, range) in planned {
            let (cluster, rows) = &groups[group];
            blocks.push(rows[range].iter().map(|&index| puts[index].0).collect());
            partitions.push(*cluster);
        }
        packs.push((start..blocks.len(), true));
    }
    let mut start = blocks.len();
    let mut raw = 0;
    for (ids, length) in plan_deletes(deletes) {
        if blocks.len() > start
            && (blocks.len() - start == MAX_PACK_BLOCKS
                || raw + length > MAX_PACK_BYTES - 64 * 1024)
        {
            packs.push((start..blocks.len(), false));
            (start, raw) = (blocks.len(), 0);
        }
        raw += length;
        blocks.push(ids);
        partitions.push(0);
    }
    if blocks.len() > start {
        packs.push((start..blocks.len(), false));
    }
    Ok((
        blocks,
        ClusteredSeal {
            view,
            partitions,
            packs,
            catalog: None,
        },
    ))
}

/// Acknowledged log-tail versions by ID. Documents are shared, so copying the
/// tail for a new view copies only its nodes.
type Tail = BTreeMap<u64, (u64, Option<Arc<Document>>)>;

/// Segmented namespace. Vectors in committed runs are fetched by block.
/// [`SegmentedDatabase::take_over`] fences earlier writers before opening;
/// a plain [`SegmentedDatabase::open`] leaves exclusivity to the caller.
///
/// The state queries read (root, directory, tail and sketches) is held in
/// copy-on-write `Arc`s that `view` shares; a later mutation copies a part
/// only while a view still holds it.
pub struct SegmentedDatabase<S> {
    store: Arc<S>,
    config: Config,
    root: Arc<Root>,
    sequence: u64,
    /// Every known fence: the selected root's, markers above it and
    /// takeover records in the tail. Carried into each published root.
    fences: Fences,
    retry: retry::State,
    latest: Arc<Directory>,
    tail: Arc<Tail>,
    tail_objects: usize,
    /// First sequence of each unsealed log object, in order.
    tail_logs: VecDeque<u64>,
    known_keys: BTreeSet<String>,
    obsolete: VecDeque<String>,
    /// Run manifests created for the root being prepared, by SHA-256 of
    /// their bytes, so a step that rebuilds that root reuses them. Cleared
    /// when a root is selected: an unreferenced one is then obsolete.
    staged_manifests: BTreeMap<[u8; 32], clustered::ObjectRef>,
    /// Replaced roots; one that a view still holds pins its packs.
    retired: Vec<Weak<Root>>,
    cache: Option<Arc<Mutex<BlockCache>>>,
    options: Arc<SegmentedOptions>,
    options_digest: [u8; 32],
    sketches: Arc<SketchSet>,
    query_threads: usize,
    reclaim_min_garbage: usize,
    seal: Option<SealState>,
    reclaim: Option<ReclaimState>,
    prune: Option<PruneState>,
    convert: Option<convert::ConvertState>,
    merge: Option<merge::MergeState>,
    /// The selected root's decoded clustered view, if it has one and it
    /// loaded; `cluster_error` says why a selected view did not load.
    cluster: Option<Arc<ClusterIndex>>,
    cluster_error: Option<String>,
    /// Replaced views; one that a query view still holds pins its packs.
    retired_views: Vec<Weak<ClusterIndex>>,
    probes: usize,
    poisoned: bool,
}

/// One acknowledged state of a segmented namespace for queries that run
/// beside the committer: the selected root with its directory and sketches,
/// and the log tail through `sequence`. A view never changes; the committer
/// publishes a new one after each change, so a query never observes a
/// partially applied write or root switch.
pub(crate) struct View<S> {
    store: Arc<S>,
    cache: Option<Arc<Mutex<BlockCache>>>,
    config: Config,
    options: Arc<SegmentedOptions>,
    query_threads: usize,
    sequence: u64,
    root: Arc<Root>,
    latest: Arc<Directory>,
    tail: Arc<Tail>,
    sketches: Arc<SketchSet>,
    cluster: Option<Arc<ClusterIndex>>,
    probes: usize,
}

/// Remote range reads and payload bytes caused by one query.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RemoteReads {
    pub(crate) requests: u64,
    pub(crate) bytes: u64,
}

impl RemoteReads {
    fn count(&mut self, bytes: usize) {
        self.requests += 1;
        self.bytes += bytes as u64;
    }
}

fn lock_cache(cache: &Mutex<BlockCache>) -> Result<std::sync::MutexGuard<'_, BlockCache>> {
    cache
        .lock()
        .map_err(|_| Error::Corrupt("segmented cache lock poisoned".into()))
}

impl<S: ObjectStore> View<S> {
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Read and authenticate one block through the cache. Corrupt cached
    /// bytes are discarded and read again. The cache lock is never held
    /// across a remote read, so concurrent queries do not wait on it.
    fn read_data_block(&self, reference: &BlockRef, reads: &mut RemoteReads) -> Result<Block> {
        if let Some(cache) = &self.cache {
            loop {
                // A separate statement releases the lock before decoding.
                let hit = lock_cache(cache)?.lookup(reference)?;
                let Some((bytes, source)) = hit else {
                    break;
                };
                match decode_block_bytes(self.config, reference, &bytes) {
                    Ok(block) => {
                        lock_cache(cache)?.accept(reference, source, &bytes);
                        return Ok(block);
                    }
                    Err(_) => lock_cache(cache)?.reject(reference, source),
                }
            }
        }
        validate_block_ref(reference)?;
        let bytes = self
            .store
            .get_range(
                &reference.object,
                reference.offset,
                reference.length,
                reference.payload_len,
            )?
            .ok_or_else(|| {
                Error::Corrupt(format!("segmented pack missing: {}", reference.object))
            })?;
        reads.count(bytes.len());
        let Some(cache) = &self.cache else {
            return decode_block_bytes(self.config, reference, &bytes);
        };
        lock_cache(cache)?.count_remote(bytes.len());
        let block = decode_block_bytes(self.config, reference, &bytes)?;
        lock_cache(cache)?.accept(reference, cache::Source::Remote, &bytes);
        Ok(block)
    }

    /// Fetch unauthenticated candidate bytes for the chosen blocks. Cache hits
    /// are local; every coalesced `range` containing a miss is fetched whole
    /// in one batched read the backend may issue concurrently, then split.
    /// The caller authenticates each block against its reference, then
    /// settles the cache with `accept` or `reject`.
    fn fetch_blocks(
        &self,
        references: &[&BlockRef],
        ranges: &[(&str, usize, usize, usize)],
        reads: &mut RemoteReads,
    ) -> Result<Vec<(Slice, cache::Source)>> {
        let mut hits = Vec::with_capacity(references.len());
        {
            let mut cache = self.cache.as_deref().map(lock_cache).transpose()?;
            for reference in references {
                validate_block_ref(reference)?;
                hits.push(match cache.as_mut() {
                    Some(cache) => cache.lookup(reference)?,
                    None => None,
                });
            }
        }
        let within = |reference: &BlockRef, range: &(&str, usize, usize, usize)| {
            range.0 == reference.object
                && range.1 <= reference.offset
                && reference.offset + reference.length <= range.1 + range.2
        };
        let needed: Vec<_> = ranges
            .iter()
            .filter(|range| {
                references
                    .iter()
                    .zip(&hits)
                    .any(|(reference, hit)| hit.is_none() && within(reference, range))
            })
            .copied()
            .collect();
        let payloads: Vec<_> = if needed.is_empty() {
            Vec::new()
        } else {
            self.store.get_ranges(&needed)?
        }
        .into_iter()
        .map(|payload| payload.map(Arc::new))
        .collect();
        for payload in payloads.iter().flatten() {
            reads.count(payload.len());
        }
        if let Some(cache) = &self.cache {
            let mut cache = lock_cache(cache)?;
            for payload in payloads.iter().flatten() {
                cache.count_remote(payload.len());
            }
        }
        references
            .iter()
            .zip(hits)
            .map(|(reference, hit)| {
                if let Some((bytes, source)) = hit {
                    let end = bytes.len();
                    return Ok((
                        Slice {
                            data: Arc::new(bytes),
                            start: 0,
                            end,
                        },
                        source,
                    ));
                }
                let (range, payload) = needed
                    .iter()
                    .zip(&payloads)
                    .find(|(range, _)| within(reference, range))
                    .ok_or_else(|| Error::Invalid("chosen block outside its range".into()))?;
                let payload = payload.as_ref().ok_or_else(|| {
                    Error::Corrupt(format!("segmented pack missing: {}", reference.object))
                })?;
                // Blocks share their span's buffer instead of copying it.
                let start = reference.offset - range.1;
                Ok((
                    Slice {
                        data: payload.clone(),
                        start,
                        end: start + reference.length,
                    },
                    cache::Source::Remote,
                ))
            })
            .collect()
    }

    /// Return the stored document; cosine vectors are normalized to unit length.
    pub(crate) fn get(&self, id: u64) -> Result<Option<OwnedDocument>> {
        self.document(id, &mut RemoteReads::default())
    }

    /// `get`, adding any remote block read to `reads`.
    fn document(&self, id: u64, reads: &mut RemoteReads) -> Result<Option<OwnedDocument>> {
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
        let block = self.read_data_block(&run.blocks[location.entry.block as usize], reads)?;
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
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// The writer epoch: the sequence of the namespace's newest takeover
    /// record, or 0 if it was never taken over. Every takeover increases it.
    pub fn epoch(&self) -> u64 {
        self.fences.logs.last().copied().unwrap_or(0)
    }

    /// A copy of the selected root at the next free generation (above every
    /// fence marker) carrying the namespace's fences, as a root v5 whose
    /// manifests `stage_manifest` binds before publication.
    fn next_root(&self) -> Result<Root> {
        let mut root = Root::clone(&self.root);
        root.generation = self
            .root
            .generation
            .max(self.fences.roots.last().copied().unwrap_or(0))
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("segmented root generation exhausted".into()))?;
        root.fences = self.fences.clone();
        root.version = 5;
        Ok(root)
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    pub fn tail_objects(&self) -> usize {
        self.tail_objects
    }

    /// Observe an ID at the current acknowledged sequence.
    pub fn revision(&self, id: u64) -> retry::Revision {
        retry::Revision {
            id,
            boundary: self.sequence,
        }
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

    pub fn lookup_request(&self, id: retry::RequestId) -> Result<retry::Lookup> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        Ok(self.retry.lookup(id, self.sequence))
    }

    /// Share the current acknowledged state with a reader. Later mutations
    /// do not change the returned view.
    pub(crate) fn view(&self) -> View<S> {
        View {
            store: self.store.clone(),
            cache: self.cache.clone(),
            config: self.config,
            options: self.options.clone(),
            query_threads: self.query_threads,
            sequence: self.sequence,
            root: self.root.clone(),
            latest: self.latest.clone(),
            tail: self.tail.clone(),
            sketches: self.sketches.clone(),
            cluster: self.cluster.clone(),
            probes: self.probes,
        }
    }

    fn maintenance_active(&self) -> bool {
        self.seal.is_some()
            || self.reclaim.is_some()
            || self.prune.is_some()
            || self.convert.is_some()
            || self.merge.is_some()
    }

    /// Whether a seal or run consolidation must wait. Both may publish
    /// while a conversion is staged: they only append or merge runs newer
    /// than the ones it froze, and the view it then publishes routes the
    /// newer versions through their canonical packs (a clustered seal's
    /// packs, extents of the previous epoch, are not in the new catalog).
    fn root_maintenance_blocked(&self) -> bool {
        self.seal.is_some()
            || self.reclaim.is_some()
            || self.prune.is_some()
            || self.merge.is_some()
    }

    /// Live (not deleted) IDs of the sealed runs, excluding the log tail.
    pub fn sealed_live_rows(&self) -> usize {
        self.latest
            .iter()
            .filter(|(_, location)| !location.entry.deleted)
            .count()
    }

    /// Postings a clustered selective query probes (default
    /// [`DEFAULT_CLUSTER_PROBES`]); at least the number of clusters probes
    /// them all.
    pub fn with_cluster_probes(mut self, probes: usize) -> Self {
        self.set_cluster_probes(probes);
        self
    }

    pub fn set_cluster_probes(&mut self, probes: usize) {
        self.probes = probes.max(1);
    }

    /// Epoch of the selected root's clustered view, if it has one.
    pub fn clustered_epoch(&self) -> Option<u64> {
        self.root.clustered.as_ref().map(|view| view.epoch)
    }

    /// Clusters of the loaded clustered view.
    pub fn cluster_count(&self) -> Option<usize> {
        self.cluster.as_ref().map(|cluster| cluster.ids.len())
    }

    /// Why the selected root's clustered view could not be loaded (a
    /// missing or corrupt centroid, catalog or posting object). Selective
    /// queries then fail until a conversion rebuilds the view; exact
    /// search, reads and writes still work.
    pub fn clustered_view_error(&self) -> Option<&str> {
        self.cluster_error.as_deref()
    }

    /// Reclaim a mostly dead pack only if it frees at least this many
    /// encoded bytes (default 64 KiB), so tiny gains do not cost a PUT.
    pub fn with_reclaim_min_garbage(mut self, bytes: usize) -> Self {
        self.reclaim_min_garbage = bytes;
        self
    }

    pub fn run_count(&self) -> usize {
        self.root.runs.len()
    }

    /// Diagnostic locator for an ID in the selected root. A newer log-tail
    /// mutation shadows its root location and therefore returns no block.
    pub fn current_block_of(&self, id: u64) -> Option<(usize, usize)> {
        if self.tail.contains_key(&id) {
            return None;
        }
        self.latest.get(&id).and_then(|location| {
            (!location.entry.deleted).then_some((location.run, location.entry.block as usize))
        })
    }

    pub fn block_count(&self) -> usize {
        self.root.runs.iter().map(|run| run.blocks.len()).sum()
    }

    /// Encoded payload length for one block in the selected root.
    pub fn block_payload_len(&self, run: usize, block: usize) -> Option<usize> {
        self.root.runs.get(run)?.blocks.get(block).map(|r| r.length)
    }

    /// Attach a disposable block cache. The namespace still opens and recovers
    /// from the object store; the cache never participates in acknowledgement.
    pub fn with_block_cache(
        mut self,
        directory: impl AsRef<Path>,
        ram_bytes: usize,
        nvme_bytes: usize,
    ) -> Result<Self> {
        self.cache = Some(Arc::new(Mutex::new(BlockCache::open(
            directory.as_ref(),
            ram_bytes,
            nvme_bytes,
        ))));
        Ok(self)
    }

    pub fn cache_stats(&self) -> Result<Option<CacheStats>> {
        self.cache
            .as_ref()
            .map(|cache| {
                cache
                    .lock()
                    .map_err(|_| Error::Corrupt("segmented cache lock poisoned".into()))
                    .map(|cache| cache.stats())
            })
            .transpose()
    }

    /// Copy part of the selected root's blocks into the NVMe cache tier: one
    /// authenticated range read of at most `unit_bytes` (at least one block).
    /// The pass fills only free space unless every block of the root fits
    /// the NVMe limit, and restarts after a root change or a lost entry.
    /// Returns false when there is nothing left to warm or no cache. Warm-up
    /// reads are reported separately from query reads in `cache_stats`. The
    /// cache lock is not held across the range read, so queries running on
    /// views never wait for it.
    pub fn warm_cache_step(&self, unit_bytes: usize) -> Result<bool> {
        let Some(cache) = &self.cache else {
            return Ok(false);
        };
        // A separate statement releases the lock before the remote read.
        let unit = lock_cache(cache)?.warm_plan(
            self.root.generation,
            || self.sketches.routable_blocks(&self.root),
            unit_bytes,
        )?;
        let read = match unit {
            cache::WarmUnit::Done => return Ok(false),
            cache::WarmUnit::Skipped => return Ok(true),
            cache::WarmUnit::Read(read) => read,
        };
        let bytes = self
            .store
            .get_range(&read.object, read.offset, read.length, read.payload_len)?
            .ok_or_else(|| Error::Corrupt(format!("segmented pack missing: {}", read.object)))?;
        lock_cache(cache)?.warm_admit(read, &bytes)
    }

    pub fn open(store: S, config: Config) -> Result<Self> {
        Self::open_with_options(store, config, SegmentedOptions::default())
    }

    /// [`SegmentedDatabase::take_over_with_options`] with default options.
    pub fn take_over(store: S, config: Config) -> Result<Self> {
        Self::take_over_with_options(store, config, SegmentedOptions::default())
    }

    /// Become the namespace's only writer, creating it if absent, then open
    /// it. Takeover publishes a takeover record at the next log sequence and
    /// a fence marker at the next root generation, both permanent, so every
    /// earlier writer (dead, paused or partitioned) fails its next log or
    /// root publication at the object store, however late, and acknowledges
    /// nothing more. The open lists after both fences, so it includes every
    /// write an earlier writer acknowledged. Correctness needs no clocks; a
    /// [`crate::lease::Lease`] only keeps processes from deposing a live
    /// writer.
    ///
    /// Each takeover consumes one sequence and one root generation and
    /// raises [`SegmentedDatabase::epoch`]. An error may leave either fence
    /// published; both are valid state and the next takeover adds its own.
    /// `Error::Exists` means another writer published concurrently; retry
    /// with a fresh store handle.
    pub fn take_over_with_options(
        store: S,
        config: Config,
        options: SegmentedOptions,
    ) -> Result<Self> {
        Self::take_over_with_options_cached(store, config, options, None)
    }

    /// Take over and use an optional disposable NVMe cache while opening.
    pub fn take_over_with_options_cached(
        store: S,
        config: Config,
        options: SegmentedOptions,
        cache: Option<(&Path, usize, usize)>,
    ) -> Result<Self> {
        config.validate()?;
        validate_options(&options)?;
        let keys = initialize(&store, config, &options)?;
        fence(&store, config, &keys)?;
        Self::open_with_options_profiled_cached(
            store,
            config,
            options,
            &mut OpenProfile::default(),
            cache,
        )
    }

    /// Open or create a namespace whose metadata declares `options`. Opening
    /// with options different from the persisted declaration fails. This
    /// fences nothing: the caller must ensure no other writer is active.
    pub fn open_with_options(store: S, config: Config, options: SegmentedOptions) -> Result<Self> {
        Self::open_with_options_profiled(store, config, options, &mut OpenProfile::default())
    }

    /// Open with a phase breakdown for reproducible storage latency probes.
    pub fn open_with_options_profiled(
        store: S,
        config: Config,
        options: SegmentedOptions,
        profile: &mut OpenProfile,
    ) -> Result<Self> {
        Self::open_with_options_profiled_cached(store, config, options, profile, None)
    }

    /// The cache stores only authenticated, disposable routing bytes and
    /// shares the same capacity and directory as cached vector blocks.
    pub fn open_with_options_profiled_cached(
        store: S,
        config: Config,
        options: SegmentedOptions,
        profile: &mut OpenProfile,
        cache: Option<(&Path, usize, usize)>,
    ) -> Result<Self> {
        let cache = cache.map(|(directory, ram, nvme)| {
            Arc::new(Mutex::new(BlockCache::open(directory, ram, nvme)))
        });
        let phase = std::time::Instant::now();
        config.validate()?;
        validate_options(&options)?;
        let keys = initialize(&store, config, &options)?;
        profile.list_metadata = phase.elapsed();
        let phase = std::time::Instant::now();
        let listed: BTreeSet<_> = keys.iter().cloned().collect();
        let mut generations = BTreeSet::new();
        let mut logs = Vec::new();
        for key in &keys {
            if key == "metadata" {
                continue;
            }
            if key.starts_with("sgroot-") {
                generations.insert(numbered_key(key, "sgroot-")?);
            } else if key.starts_with("sglog-") {
                let sequence = numbered_key(key, "sglog-")?;
                if sequence == 0 {
                    return Err(Error::Corrupt("segmented log sequence zero".into()));
                }
                logs.push(sequence);
            } else if key.starts_with("sgpack-")
                || key.starts_with("sgindex-")
                || key.starts_with(manifest::PREFIX)
                || key.starts_with("sgcentroid-")
                || key.starts_with("sgcluster-")
            {
                // Unreferenced objects from an incomplete root are not state.
            } else {
                return Err(Error::Corrupt(format!("unexpected segmented key: {key}")));
            }
        }
        let (mut root, markers) =
            select_root(&store, config, &generations).map_err(|error| match error {
                Error::Exists(_) => Error::Corrupt("selected segmented root missing".into()),
                error => error,
            })?;
        manifest::load_manifests(&store, config, &mut root)?;
        profile.root_manifests = phase.elapsed();
        let phase = std::time::Instant::now();
        let mut fences = root.fences.clone();
        fences.roots.extend(markers);
        let mut latest = Directory::default();
        // Size the directory once from the index entry counts, so merging
        // runs does not reallocate it repeatedly.
        latest.reserve(root.runs.iter().map(|run| (run.index_len - 24) / 24).sum());
        for run in &root.runs {
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
        }
        // Bound outstanding index bytes while using the store's read parallelism.
        let mut indexes = Vec::new();
        let mut chunk_start = 0;
        while chunk_start < root.runs.len() {
            let mut chunk_end = chunk_start;
            let mut batch_bytes = 0_usize;
            while chunk_end < root.runs.len() && chunk_end - chunk_start < 32 {
                let next = root.runs[chunk_end].index_len;
                if chunk_end > chunk_start && batch_bytes + next > 32 * 1024 * 1024 {
                    break;
                }
                batch_bytes += next;
                chunk_end += 1;
            }
            let chunk = &root.runs[chunk_start..chunk_end];
            let mut keys = Vec::new();
            let mut pending = Vec::new();
            let mut values = vec![None; chunk.len()];
            for (offset, run) in chunk.iter().enumerate() {
                if let Some(cache) = &cache {
                    let key = open_key(
                        b"index",
                        &run.index_object,
                        run.index_len,
                        &run.index_sha256,
                    );
                    let hit = lock_cache(cache)?.lookup_open(&key);
                    if let Some(bytes) = hit {
                        if bytes.len() == run.index_len
                            && format!("{:x}", Sha256::digest(&bytes)) == run.index_sha256
                        {
                            values[offset] = Some(bytes);
                            continue;
                        }
                        lock_cache(cache)?.reject_open(&key);
                    }
                }
                keys.push(run.index_object.clone());
                pending.push(offset);
            }
            let read = std::time::Instant::now();
            let fetched = store.get_many(&keys)?;
            profile.run_index_reads += read.elapsed();
            for (offset, bytes) in pending.into_iter().zip(fetched) {
                if let (Some(cache), Some(bytes)) = (&cache, &bytes) {
                    let run = &chunk[offset];
                    if bytes.len() == run.index_len
                        && format!("{:x}", Sha256::digest(bytes)) == run.index_sha256
                    {
                        let key = open_key(
                            b"index",
                            &run.index_object,
                            run.index_len,
                            &run.index_sha256,
                        );
                        lock_cache(cache)?.admit_open(&key, bytes);
                    }
                }
                values[offset] = bytes;
            }
            for (offset, bytes) in values.into_iter().enumerate() {
                let run_ordinal = chunk_start + offset;
                indexes.push((
                    run_ordinal,
                    decode_run_index(&root.runs[run_ordinal], bytes)?,
                ));
            }
            for (run_ordinal, index) in indexes.drain(..) {
                latest.merge_sorted(index.entries.into_iter().map(|entry| {
                    (
                        entry.id,
                        Location {
                            run: run_ordinal,
                            entry,
                        },
                    )
                }));
            }
            chunk_start = chunk_end;
        }
        profile.run_indexes = phase.elapsed();
        let phase = std::time::Instant::now();
        logs.sort_unstable();
        let tail_logs: Vec<_> = logs
            .into_iter()
            .filter(|&sequence| sequence > root.sequence)
            .collect();
        let mut db = Self {
            store: Arc::new(store),
            config,
            sequence: root.sequence,
            fences,
            retry: root.retry.clone(),
            root: Arc::new(root),
            latest: Arc::new(latest),
            tail: Arc::default(),
            tail_objects: 0,
            tail_logs: VecDeque::new(),
            known_keys: listed,
            obsolete: VecDeque::new(),
            staged_manifests: BTreeMap::new(),
            retired: Vec::new(),
            cache,
            options_digest: Sha256::digest(encode(&options)?).into(),
            options: Arc::new(options),
            sketches: Arc::default(),
            query_threads: 1,
            reclaim_min_garbage: 64 * 1024,
            seal: None,
            reclaim: None,
            prune: None,
            convert: None,
            merge: None,
            cluster: None,
            cluster_error: None,
            retired_views: Vec::new(),
            probes: DEFAULT_CLUSTER_PROBES,
            poisoned: false,
        };
        // Takeover records hold no request, so only request objects count
        // toward the replay bound; repeated takeovers cannot block opening.
        let mut request_objects = 0;
        for chunk in tail_logs.chunks(8) {
            let keys: Vec<_> = chunk.iter().map(|&sequence| log_key(sequence)).collect();
            let logs = db.store.get_many(&keys)?;
            for (&log_sequence, bytes) in chunk.iter().zip(logs) {
                if db.sequence.checked_add(1) != Some(log_sequence) {
                    return Err(Error::Corrupt("segmented mutation log gap".into()));
                }
                let log = decode_log(
                    &bytes.ok_or_else(|| Error::Corrupt("listed segmented log missing".into()))?,
                    log_sequence,
                )?;
                match log {
                    LogObject::Requests(records) => {
                        request_objects += 1;
                        if request_objects > MAX_TAIL_OBJECTS {
                            return Err(Error::Corrupt(
                                "segmented replay tail exceeds bound".into(),
                            ));
                        }
                        for record in records {
                            let expected = db.sequence + 1;
                            db.replay(record, expected)?;
                        }
                    }
                    LogObject::Takeover => {
                        db.retry.advance(log_sequence, &[]);
                        db.sequence = log_sequence;
                        db.fences.logs.insert(log_sequence);
                    }
                }
                db.tail_logs.push_back(log_sequence);
                db.tail_objects += 1;
            }
        }
        profile.tail_replay = phase.elapsed();
        let phase = std::time::Instant::now();
        db.load_routing(profile)?;
        profile.routing = phase.elapsed();
        let phase = std::time::Instant::now();
        db.schedule_obsolete();
        profile.finish = phase.elapsed();
        Ok(db)
    }

    /// Load the routing state the selected root needs. Without a clustered
    /// view that is the sketch of every referenced pack. With one, it is the
    /// view's centroids, catalog and posting sketches, plus the sketches of
    /// the canonical packs holding live versions that no posting copy covers
    /// (versions a stage 3 binary sealed after its conversion); covered
    /// canonical rows are never routed, so each current version has exactly
    /// one routing row. A missing or corrupt derived view object, a duplicate
    /// posting copy, or an uncovered version inside a posting pack leaves the
    /// view unavailable (`clustered_view_error`); other storage errors fail.
    /// Invalid sketch frames are rebuilt from authenticated blocks.
    fn load_routing(&mut self, profile: &mut OpenProfile) -> Result<()> {
        Arc::make_mut(&mut self.sketches).clear();
        self.cluster = None;
        self.cluster_error = None;
        let mut covered = Vec::new();
        let mut uncovered = BTreeSet::new();
        if let Some(view) = self.root.clustered.clone() {
            let loaded = self.load_view(&view, profile).and_then(|index| {
                covered = self.covered_versions()?;
                uncovered = self.uncovered_blocks(&covered, Some(&index))?;
                Ok(index)
            });
            match loaded {
                Ok(index) => self.cluster = Some(Arc::new(index)),
                Err(Error::Corrupt(message)) => {
                    Arc::make_mut(&mut self.sketches).clear();
                    covered.clear();
                    uncovered.clear();
                    self.cluster_error = Some(message);
                }
                Err(error) => return Err(error),
            }
        }
        let mut packs: BTreeMap<String, Vec<BlockRef>> = BTreeMap::new();
        if self.root.clustered.is_none() {
            for run in &self.root.runs {
                for block in &run.blocks {
                    packs
                        .entry(block.object.clone())
                        .or_default()
                        .push(block.clone());
                }
            }
        } else if self.cluster.is_some() {
            for (run, block) in uncovered {
                let block = &self.root.runs[run].blocks[block];
                packs
                    .entry(block.object.clone())
                    .or_default()
                    .push(block.clone());
            }
            // A pack's sketch covers every block of it the root references.
            for run in &self.root.runs {
                for block in &run.blocks {
                    if let Some(references) = packs.get_mut(&block.object) {
                        if !references.iter().any(|known| known.offset == block.offset) {
                            references.push(block.clone());
                        }
                    }
                }
            }
        }
        let packs: Vec<_> = packs.into_iter().collect();
        let keys: Vec<_> = packs
            .iter()
            .map(|(pack, references)| {
                let mut digest = Sha256::new();
                for reference in references {
                    digest.update(reference.offset.to_le_bytes());
                    digest.update(reference.length.to_le_bytes());
                    digest.update(reference.sha256.as_bytes());
                }
                (
                    pack.as_str(),
                    references[0].payload_len,
                    format!("{:x}", digest.finalize()),
                )
            })
            .collect();
        let decoded = self.read_sketch_frames(&keys, false, profile)?;
        for ((pack, references), decoded) in packs.iter().zip(decoded) {
            let sketch = match decoded {
                Some(sketch) => sketch,
                None => {
                    let mut references = references.clone();
                    references.sort_by_key(|reference| reference.offset);
                    let blocks = references
                        .iter()
                        .map(|reference| read_block(&*self.store, self.config, reference))
                        .collect::<Result<Vec<_>>>()?;
                    let pairs: Vec<_> = references.iter().zip(&blocks).collect();
                    Arc::make_mut(&mut self.sketches).rebuilt += 1;
                    PackSketch::build(self.config, &self.options, pack, &pairs)?
                }
            };
            Arc::make_mut(&mut self.sketches).install(sketch);
        }
        Arc::make_mut(&mut self.sketches).refresh(&self.root, self.root.clustered.is_some())?;
        let latest = self.latest.clone();
        self.activate_sketches(None, |id| {
            latest
                .index_of(id)
                .is_none_or(|(position, _)| !bit(&covered, position))
        });
        let (latest, tail) = (&self.latest, &self.tail);
        Arc::make_mut(&mut self.sketches).activate_postings(None, |id, sequence| {
            sketch::posting_current(tail, latest, id, sequence)
        });
        Arc::make_mut(&mut self.sketches).compact_all();
        Ok(())
    }

    /// One bit per latest-ID directory entry: set when a loaded posting row
    /// copies exactly that live version. A second copy of a version is a
    /// corrupt view.
    fn covered_versions(&self) -> Result<Vec<u64>> {
        let mut covered = vec![0_u64; self.latest.len().div_ceil(64)];
        for (id, sequence) in self.sketches.posting_rows() {
            let Some((position, location)) = self.latest.index_of(id) else {
                continue;
            };
            if location.entry.sequence != sequence || location.entry.deleted {
                continue;
            }
            if bit(&covered, position) {
                return Err(Error::Corrupt(format!(
                    "clustered view holds two copies of ID {id}"
                )));
            }
            covered[position / 64] |= 1 << (position % 64);
        }
        Ok(covered)
    }

    /// Root blocks `(run, block)` holding live versions that no posting copy
    /// covers and no tail write shadows. With a view, such a version inside
    /// one of its posting packs means the catalog lost its copy.
    fn uncovered_blocks(
        &self,
        covered: &[u64],
        cluster: Option<&ClusterIndex>,
    ) -> Result<BTreeSet<(usize, usize)>> {
        let mut blocks = BTreeSet::new();
        for (position, (id, location)) in self.latest.iter().enumerate() {
            if location.entry.deleted || bit(covered, position) || self.tail.contains_key(&id) {
                continue;
            }
            let block = (location.run, location.entry.block as usize);
            let object = &self.root.runs[block.0].blocks[block.1].object;
            if cluster.is_some_and(|cluster| cluster.packs.contains_key(object)) {
                return Err(Error::Corrupt(format!(
                    "clustered view lost the posting copy of ID {id}"
                )));
            }
            blocks.insert(block);
        }
        Ok(blocks)
    }

    /// Decode and authenticate the clustered view `view` selects and load
    /// its posting sketches. `Error::Corrupt` means a derived object is
    /// missing or invalid.
    fn load_view(
        &mut self,
        view: &clustered::ViewRef,
        profile: &mut OpenProfile,
    ) -> Result<ClusterIndex> {
        let phase = std::time::Instant::now();
        let read = |key: &str| {
            let started = std::time::Instant::now();
            self.store
                .get(key)?
                .ok_or_else(|| Error::Corrupt(format!("clustered object missing: {key}")))
                .map(|bytes| (bytes, started.elapsed()))
        };
        let (centroid_bytes, elapsed) = read(&view.centroid.key)?;
        profile.catalog_reads += elapsed;
        let centroids = view.decode_centroids(self.config, &centroid_bytes)?;
        if centroids.source_generation >= self.root.generation
            || centroids.source_sequence > self.root.sequence
        {
            return Err(Error::Corrupt("clustered view source is not older".into()));
        }
        let (catalog_bytes, elapsed) = read(&view.catalog.key)?;
        profile.catalog_reads += elapsed;
        let catalog = view.decode_catalog(&centroids, &catalog_bytes)?;
        let index = ClusterIndex::new(&centroids, catalog);
        profile.catalog += phase.elapsed();
        if let Some(pack) = index
            .packs
            .keys()
            .find(|pack| !self.known_keys.contains(*pack))
        {
            return Err(Error::Corrupt(format!("posting pack missing: {pack}")));
        }
        let layouts = index.pack_blocks();
        let keys: Vec<_> = layouts
            .iter()
            .map(|(pack, layout)| {
                let mut digest = Sha256::new();
                for (offset, length, sha256) in &layout.blocks {
                    digest.update(offset.to_le_bytes());
                    digest.update(length.to_le_bytes());
                    digest.update(sha256);
                }
                (
                    *pack,
                    layout.payload_len,
                    format!("{:x}", digest.finalize()),
                )
            })
            .collect();
        let decoded = self.read_sketch_frames(&keys, true, profile)?;
        let mut sketches = Vec::with_capacity(layouts.len());
        for ((pack, layout), decoded) in layouts.iter().zip(decoded) {
            sketches.push(self.posting_sketch(&index, pack, layout, decoded)?);
        }
        let set = Arc::make_mut(&mut self.sketches);
        for sketch in sketches {
            set.install(sketch);
        }
        Ok(index)
    }

    /// A posting pack's sketch: the decoded frame if it matches the catalog
    /// and the view's centers, else rebuilt from the pack's blocks after
    /// authenticating them against the catalog digests.
    fn posting_sketch(
        &mut self,
        index: &ClusterIndex,
        pack: &str,
        layout: &clustered::PackLayout,
        decoded: Option<PackSketch>,
    ) -> Result<PackSketch> {
        let matches = |sketch: &PackSketch| {
            let Some(posting) = sketch.posting() else {
                return false;
            };
            let rows: Vec<_> = sketch.block_rows().collect();
            layout
                .extents
                .iter()
                .all(|&(first, count, extent_rows, cluster)| {
                    let blocks = first..first + count;
                    rows.get(blocks.clone()).map(|rows| rows.iter().sum()) == Some(extent_rows)
                        && blocks.into_iter().all(|block| {
                            posting.clusters.get(block) == Some(&cluster)
                                && index.fingerprints.get(&cluster)
                                    == posting.fingerprints.get(block)
                        })
                })
        };
        if let Some(mut sketch) = decoded {
            // The catalog may list only some of a pack's blocks; the others
            // were merged into other extents and are not routed.
            let listed: BTreeSet<&[u8; 32]> = layout.blocks.iter().map(|block| &block.2).collect();
            let keep: Vec<bool> = sketch
                .block_digests()
                .map(|digest| listed.contains(digest))
                .collect();
            if keep.iter().filter(|&&kept| kept).count() == layout.blocks.len() {
                if keep.contains(&false) {
                    let rows: usize = sketch.block_rows().sum();
                    let mut live = vec![u64::MAX; rows.div_ceil(64)];
                    sketch.retain_blocks(&mut live, &keep);
                }
                if sketch
                    .bind_posting(layout.payload_len, &layout.blocks)
                    .is_ok()
                    && matches(&sketch)
                {
                    return Ok(sketch);
                }
            }
        }
        let mut references = Vec::with_capacity(layout.blocks.len());
        let mut blocks = Vec::with_capacity(layout.blocks.len());
        for &(offset, length, digest) in &layout.blocks {
            let bytes = self
                .store
                .get_range(pack, offset, length, layout.payload_len)?
                .ok_or_else(|| Error::Corrupt(format!("posting pack missing: {pack}")))?;
            if Sha256::digest(&bytes).as_slice() != digest {
                return Err(Error::Corrupt(format!(
                    "posting block digest mismatch: {pack}"
                )));
            }
            let block = decode_block(self.config, &bytes)?;
            references.push(BlockRef {
                object: pack.to_owned(),
                payload_len: layout.payload_len,
                offset,
                length,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                partition: block.partition,
                first_id: block.records[0].id(),
                last_id: block.records.last().unwrap().id(),
                rows: block.records.len(),
            });
            blocks.push(block);
        }
        let pairs: Vec<_> = references.iter().zip(&blocks).collect();
        let sketch = PackSketch::build_with(
            self.config,
            &self.options,
            pack,
            &pairs,
            Some(&index.fingerprints),
        )?;
        if !matches(&sketch) {
            return Err(Error::Corrupt(format!(
                "catalog disagrees with posting pack: {pack}"
            )));
        }
        Arc::make_mut(&mut self.sketches).rebuilt += 1;
        Ok(sketch)
    }

    /// Read and decode the sketch frame of each `(pack, payload length,
    /// committed block-layout digest)`,
    /// up to 32 prefix reads at a time. `None` marks a pack whose frame is
    /// absent or invalid, or whose range the store reported corrupt.
    fn read_sketch_frames(
        &self,
        packs: &[(&str, usize, String)],
        posting: bool,
        profile: &mut OpenProfile,
    ) -> Result<Vec<Option<PackSketch>>> {
        let phase = std::time::Instant::now();
        let mut decoded = Vec::with_capacity(packs.len());
        // Match the store's bounded read parallelism.
        for chunk in packs.chunks(32) {
            let mut requests = Vec::new();
            let mut pending = Vec::new();
            let mut cached = vec![None; chunk.len()];
            for (index, (pack, payload_len, layout_digest)) in chunk.iter().enumerate() {
                if let Some(cache) = &self.cache {
                    let key = open_key(b"sketch", pack, *payload_len, layout_digest);
                    let hit = lock_cache(cache)?.lookup_open(&key);
                    if let Some(bytes) = hit {
                        let sketch = match unframe(&bytes, *payload_len) {
                            Framed::Sketch(body)
                                if bytes.len() == sketch::FRAME_HEADER + body.len() =>
                            {
                                PackSketch::decode_with(
                                    body,
                                    self.config,
                                    &self.options_digest,
                                    pack,
                                    &self.options.routed_keys,
                                    posting,
                                )
                                .ok()
                                .map(|mut sketch| {
                                    sketch.frame_len = Some(bytes.len());
                                    sketch
                                })
                            }
                            _ => None,
                        };
                        if sketch.is_some() {
                            cached[index] = sketch;
                            continue;
                        }
                        lock_cache(cache)?.reject_open(&key);
                    }
                }
                pending.push(index);
                requests.push((*pack, 0, FRAME_PREFIX_READ.min(*payload_len), *payload_len));
            }
            // A store-reported corrupt range fails the batch; read that chunk
            // serially so each pack is judged on its own bytes.
            let read = std::time::Instant::now();
            let batch = self.store.get_ranges(&requests);
            profile.sketch_reads += read.elapsed();
            let fetched = match batch {
                Ok(values) => values,
                Err(Error::Corrupt(_)) => requests
                    .iter()
                    .map(|&(key, offset, length, payload)| {
                        match self.store.get_range(key, offset, length, payload) {
                            Err(Error::Corrupt(_)) => Ok(None),
                            other => other,
                        }
                    })
                    .collect::<Result<Vec<_>>>()?,
                Err(error) => return Err(error),
            };
            let mut prefixes = vec![None; chunk.len()];
            for (index, bytes) in pending.into_iter().zip(fetched) {
                prefixes[index] = bytes;
            }
            for (index, (pack, payload_len, layout_digest)) in chunk.iter().enumerate() {
                if let Some(sketch) = cached[index].take() {
                    decoded.push(Some(sketch));
                    continue;
                }
                let mut sketch = None;
                for _ in 0..2 {
                    let Some(bytes) = prefixes[index].as_deref() else {
                        break;
                    };
                    match unframe(bytes, *payload_len) {
                        Framed::Sketch(bytes) => {
                            sketch = PackSketch::decode_with(
                                bytes,
                                self.config,
                                &self.options_digest,
                                pack,
                                &self.options.routed_keys,
                                posting,
                            )
                            .ok()
                            .map(|mut sketch| {
                                sketch.frame_len = Some(sketch::FRAME_HEADER + bytes.len());
                                sketch
                            });
                            break;
                        }
                        Framed::Need(length) => {
                            let read = std::time::Instant::now();
                            prefixes[index] =
                                match self.store.get_range(pack, 0, length, *payload_len) {
                                    Err(Error::Corrupt(_)) => None,
                                    other => other?,
                                };
                            profile.sketch_reads += read.elapsed();
                        }
                        Framed::Invalid => break,
                    }
                }
                if let (Some(cache), Some(sketch)) = (&self.cache, &sketch) {
                    if let (Some(prefix), Some(length)) = (&prefixes[index], sketch.frame_len) {
                        let key = open_key(b"sketch", pack, *payload_len, layout_digest);
                        lock_cache(cache)?.admit_open(&key, &prefix[..length]);
                    }
                }
                prefixes[index] = None;
                decoded.push(sketch);
            }
        }
        profile.sketch_frames += phase.elapsed();
        Ok(decoded)
    }

    /// Recompute canonical row liveness for the named packs (all when
    /// `None`): a row is live when it is the current version and `routed`
    /// accepts its ID. With a clustered view, versions a posting covers are
    /// routed through it instead.
    fn activate_sketches(
        &mut self,
        packs: Option<&BTreeSet<String>>,
        routed: impl Fn(u64) -> bool,
    ) {
        let (latest, tail) = (&self.latest, &self.tail);
        Arc::make_mut(&mut self.sketches).activate(packs, |id, run, block| {
            !tail.contains_key(&id)
                && latest.get(&id).is_some_and(|location| {
                    location.run == run
                        && location.entry.block as usize == block
                        && !location.entry.deleted
                })
                && routed(id)
        });
    }

    fn schedule_obsolete(&mut self) {
        let mut retained = BTreeSet::from([
            "metadata".to_owned(),
            root_key(0),
            root_key(self.root.generation),
        ]);
        for run in &self.root.runs {
            retained.insert(run.index_object.clone());
            retained.extend(run.manifest.iter().map(|manifest| manifest.key.clone()));
            for block in &run.blocks {
                retained.insert(block.object.clone());
            }
        }
        for &sequence in self.tail_logs.iter().chain(&self.fences.logs) {
            retained.insert(log_key(sequence));
        }
        retained.extend(
            self.fences
                .roots
                .iter()
                .map(|&generation| root_key(generation)),
        );
        // A staged conversion's objects are not referenced until its root.
        retained.extend(self.conversion_staged().iter().cloned());
        if let Some(view) = &self.root.clustered {
            let Some(cluster) = &self.cluster else {
                // An unreadable catalog does not name its packs: remove
                // nothing until a conversion rebuilds the view.
                self.obsolete.clear();
                return;
            };
            retained.insert(view.centroid.key.clone());
            retained.insert(view.catalog.key.clone());
            retained.extend(cluster.packs.keys().cloned());
        }
        self.obsolete = self.known_keys.difference(&retained).cloned().collect();
    }

    /// Select a new root. A view holding the old one keeps its packs from
    /// cleanup until the view is dropped.
    fn replace_root(&mut self, root: Root) {
        self.staged_manifests.clear();
        let old = std::mem::replace(&mut self.root, Arc::new(root));
        self.retired.push(Arc::downgrade(&old));
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

    /// Publish one pack beginning with its derived sketch frame (a `GLSKT003`
    /// posting sketch when `fingerprints` maps its blocks' clusters). Neither
    /// is state until a root references the pack.
    fn publish_pack(
        &mut self,
        key: &str,
        blocks: &[Block],
        fingerprints: Option<&BTreeMap<u32, [u8; 32]>>,
    ) -> Result<(Vec<BlockRef>, PackSketch)> {
        let (bytes, references, sketch) = encode_pack_with_sketch(
            key,
            self.config,
            &self.options,
            &self.options_digest,
            blocks,
            fingerprints,
        )?;
        self.create_staged(key, &bytes)?;
        Ok((references, sketch))
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
        let tail = Arc::make_mut(&mut self.tail);
        for mutation in mutations {
            let (id, entry) = match mutation {
                Mutation::Put {
                    id,
                    vector,
                    metadata,
                } => (
                    *id,
                    (
                        sequence,
                        Some(Arc::new(Document {
                            vector: self.config.normalized(vector.clone()),
                            metadata: metadata.clone(),
                        })),
                    ),
                ),
                Mutation::Delete { id } => (*id, (sequence, None)),
            };
            let old = tail.insert(id, entry);
            // A version an in-progress seal will publish must outlive its
            // replacement in the tail until that seal materializes it.
            if let (Some(seal), Some(old)) = (self.seal.as_mut(), old) {
                if old.0 <= seal.boundary {
                    seal.displaced.entry(id).or_insert(old);
                }
            }
        }
    }

    pub fn apply_request(&mut self, request: retry::Request) -> Result<retry::Outcome> {
        self.apply_requests(vec![request])
            .pop()
            .expect("one result per request")
    }

    /// Publish independent requests in one conditional log create (group
    /// commit). Each accepted request gets its own consecutive sequence and
    /// retry receipt, decided in order against the state left by earlier
    /// requests in the group; duplicates and invalid requests are resolved
    /// individually without publication. No request is acknowledged or
    /// visible before the shared create succeeds; if it fails, every
    /// accepted request's outcome is uncertain and the handle is poisoned.
    pub fn apply_requests(&mut self, requests: Vec<retry::Request>) -> Vec<Result<retry::Outcome>> {
        if self.poisoned {
            return requests
                .iter()
                .map(|_| Err(Error::RecoveryRequired))
                .collect();
        }
        let mut results: Vec<Option<Result<retry::Outcome>>> =
            requests.iter().map(|_| None).collect();
        let mut retry = self.retry.clone();
        let mut sequence = self.sequence;
        let mut accepted = Vec::new();
        for (index, request) in requests.iter().enumerate() {
            let decided = (|| {
                request.validate(self.config)?;
                if let Some(outcome) = retry.duplicate(request, sequence)? {
                    return Ok((outcome, false));
                }
                // A row that cannot share a block with anything can never be
                // sealed; refuse it before it is acknowledged. Replay does not
                // apply this check: older logs may hold such rows.
                for mutation in &request.mutations {
                    if let Mutation::Put {
                        id,
                        vector,
                        metadata,
                    } = mutation
                    {
                        codec::check_put_fits(*id, vector, metadata)?;
                    }
                }
                let next = sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("sequence exhausted".into()))?;
                let outcome = retry.decide(request, next);
                let applied: &[Mutation] = if outcome.conflict.is_none() {
                    &request.mutations
                } else {
                    &[]
                };
                retry.advance(next, applied);
                retry.retain(request, outcome)?;
                sequence = next;
                Ok((outcome, true))
            })();
            results[index] = Some(match decided {
                Ok((outcome, publish)) => {
                    if publish {
                        accepted.push(index);
                    }
                    Ok(outcome)
                }
                Err(error) => Err(error),
            });
        }
        if accepted.is_empty() {
            return results.into_iter().map(Option::unwrap).collect();
        }
        if self.tail_objects >= MAX_TAIL_OBJECTS {
            for &index in &accepted {
                results[index] = Some(Err(Error::MaintenanceRequired));
            }
            return results.into_iter().map(Option::unwrap).collect();
        }
        let first = self.sequence + 1;
        let outcome = |index: usize| match &results[index] {
            Some(Ok(outcome)) => *outcome,
            _ => unreachable!("accepted requests have outcomes"),
        };
        let bytes = if accepted.len() == 1 {
            encode(&LogRecordV1 {
                version: 1,
                sequence: first,
                request: requests[accepted[0]].clone(),
                outcome: outcome(accepted[0]),
            })
        } else {
            encode(&LogRecordV2 {
                version: 2,
                first_sequence: first,
                entries: accepted
                    .iter()
                    .map(|&index| LogEntry {
                        request: requests[index].clone(),
                        outcome: outcome(index),
                    })
                    .collect(),
            })
        };
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(error) => {
                let message = error.to_string();
                for &index in &accepted {
                    results[index] = Some(Err(Error::Invalid(message.clone())));
                }
                return results.into_iter().map(Option::unwrap).collect();
            }
        };
        self.poisoned = true;
        let object = log_key(first);
        if let Err(error) = self.store.create(&object, &bytes) {
            // A conditional-create conflict proves these bytes were not
            // written: a later writer took over and fenced this handle.
            let fenced = matches!(error, Error::Exists(_));
            let message = error.to_string();
            for &index in &accepted {
                results[index] = Some(Err(if fenced {
                    Error::Busy(format!(
                        "this writer was fenced by a takeover at {object}; not committed"
                    ))
                } else {
                    Error::Io(std::io::Error::other(message.clone()))
                }));
            }
            return results.into_iter().map(Option::unwrap).collect();
        }
        self.known_keys.insert(object);
        self.retry = retry;
        for &index in &accepted {
            let request = &requests[index];
            let outcome = outcome(index);
            let applied: &[Mutation] = if outcome.conflict.is_none() {
                &request.mutations
            } else {
                &[]
            };
            self.apply_tail(outcome.sequence, applied);
            for mutation in applied {
                let (Mutation::Put { id, .. } | Mutation::Delete { id }) = mutation;
                if let Some(location) = self.latest.get(id) {
                    Arc::make_mut(&mut self.sketches).mark(
                        location.run,
                        location.entry.block as usize,
                        *id,
                        false,
                    );
                }
            }
        }
        self.sequence = sequence;
        self.tail_logs.push_back(first);
        self.tail_objects += 1;
        self.poisoned = false;
        results.into_iter().map(Option::unwrap).collect()
    }

    /// Return the stored document; cosine vectors are normalized to unit length.
    pub fn get(&self, id: u64) -> Result<Option<OwnedDocument>> {
        self.view().get(id)
    }

    /// Exact correctness oracle. Reads committed blocks on demand without
    /// retaining their vectors; it may require many remote range GETs.
    pub fn search_exact(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> Result<Vec<Neighbor>> {
        Ok(self
            .search_exact_with_options(query, k, filter, QueryOptions::default())?
            .iter()
            .map(QueryHit::neighbor)
            .collect())
    }

    /// Exact search with optional fields from each scored document.
    pub fn search_exact_with_options(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
        options: QueryOptions,
    ) -> Result<Vec<QueryHit>> {
        let query = self.config.query(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut heap = BinaryHeap::new();
        self.scan_live(|id, vector, metadata| {
            if matches_filter(metadata, filter) {
                consider_with(&mut heap, k, self.config, &query, id, vector, || {
                    (
                        options.include_metadata.then(|| metadata.clone()),
                        options.include_vector.then(|| vector.to_vec()),
                    )
                });
            }
            Ok(())
        })?;
        let mut results: Vec<_> = heap.into_iter().map(|ranked| ranked.0).collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        Ok(results)
    }

    /// Visit every live document exactly once: authenticated current records
    /// of the selected root in physical order, then the acknowledged log tail.
    /// Fails if any directory entry is missing from its block.
    pub fn scan_live(
        &self,
        mut visit: impl FnMut(u64, &[f32], &BTreeMap<String, String>) -> Result<()>,
    ) -> Result<()> {
        let expected = self
            .latest
            .iter()
            .filter(|(id, _)| !self.tail.contains_key(id))
            .count();
        let mut seen = 0;
        let view = self.view();
        for (run_ordinal, run) in self.root.runs.iter().enumerate() {
            for (block_ordinal, reference) in run.blocks.iter().enumerate() {
                let block = view.read_data_block(reference, &mut RemoteReads::default())?;
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
                        visit(id, &vector, &metadata)?;
                    }
                }
            }
        }
        if seen != expected {
            return Err(Error::Corrupt(
                "segmented directory entry missing from blocks".into(),
            ));
        }
        for (&id, (_, document)) in self.tail.iter() {
            if let Some(document) = document {
                visit(id, &document.vector, &document.metadata)?;
            }
        }
        Ok(())
    }

    /// Freeze an acknowledged prefix for publication. Subsequent writes may
    /// continue in the log tail while bounded maintenance steps publish it.
    pub fn start_seal(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.root_maintenance_blocked() {
            return Err(Error::MaintenanceRequired);
        }
        if self.tail_objects == 0 {
            return Ok(());
        }
        let first_sequence = self.root.sequence + 1;
        let boundary = self.sequence;
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        for (&id, (_, document)) in self.tail.iter() {
            match document {
                Some(document) => puts.push((
                    id,
                    document.vector.as_slice(),
                    codec::put_row_len(&document.vector, &document.metadata),
                )),
                None => deletes.push((id, codec::DELETE_ROW_LEN)),
            }
        }
        // With a loaded clustered view every put joins its nearest cluster's
        // posting extent; otherwise blocks are vector-local groups.
        let (blocks, clustered) = match self.cluster.clone() {
            Some(view) => {
                let (blocks, clustered) = plan_clustered_seal(self.config, view, &puts, &deletes)?;
                (blocks, Some(clustered))
            }
            None => (plan_vector_local(self.config, &puts, &deletes)?, None),
        };
        drop(puts);
        let mut entries = Vec::new();
        for (ordinal, ids) in blocks.iter().enumerate() {
            let block_ordinal = u32::try_from(ordinal)
                .map_err(|_| Error::Invalid("too many segmented blocks".into()))?;
            for id in ids {
                let (sequence, document) = &self.tail[id];
                entries.push(IndexEntry {
                    id: *id,
                    sequence: *sequence,
                    block: block_ordinal,
                    deleted: document.is_none(),
                });
            }
        }
        entries.sort_unstable_by_key(|entry| entry.id);
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
            clustered,
            displaced: BTreeMap::new(),
            entries,
            references: Vec::new(),
            sketches: Vec::new(),
            next_block: 0,
            next_pack: 0,
            index_published: false,
        });
        Ok(())
    }

    /// Materialize the frozen versions of the planned blocks `range`.
    fn seal_records(&self, seal: &SealState, ids: &[u64]) -> Result<Vec<BlockRecord>> {
        let mut records = Vec::with_capacity(ids.len());
        for id in ids {
            let (sequence, document) = seal
                .displaced
                .get(id)
                .or_else(|| self.tail.get(id))
                .filter(|(sequence, _)| *sequence <= seal.boundary)
                .ok_or_else(|| Error::Corrupt("sealed version missing".into()))?;
            records.push(BlockRecord {
                sequence: *sequence,
                mutation: match document {
                    Some(document) => Mutation::Put {
                        id: *id,
                        vector: document.vector.clone(),
                        metadata: document.metadata.clone(),
                    },
                    None => Mutation::Delete { id: *id },
                },
            });
        }
        Ok(records)
    }

    /// Publish at most one pack, index, catalog, run manifest or root per
    /// call. The root
    /// is the only authority switch; an uncertain create poisons the handle
    /// until reopen. A clustered seal stages a catalog listing its posting
    /// extents, and its root selects that catalog.
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
            let mut blocks = Vec::new();
            let mut fingerprints = None;
            let view = seal
                .clustered
                .as_ref()
                .map(|clustered| clustered.view.clone());
            if let (Some(clustered), Some(view)) = (&seal.clustered, &view) {
                let (range, posting) = clustered.packs[seal.next_pack as usize].clone();
                for block in range.clone() {
                    let records = self.seal_records(&seal, &seal.blocks[block])?;
                    blocks.push(Block::new(
                        self.config,
                        clustered.partitions[block],
                        records,
                    )?);
                }
                end = range.end;
                fingerprints = posting.then_some(&view.fingerprints);
            } else {
                // Materialize only this pack's sealed versions. Raw lengths
                // bound compressed lengths from above, up to zstd's small
                // framing overhead covered by the 64 KiB margin.
                let mut bytes = 0;
                while end < seal.blocks.len() && end - start < MAX_PACK_BLOCKS {
                    let records = self.seal_records(&seal, &seal.blocks[end])?;
                    let length = codec::block_len(records.iter().map(codec::record_len));
                    if end > start && bytes + length > MAX_PACK_BYTES - 64 * 1024 {
                        break;
                    }
                    bytes += length;
                    blocks.push(Block::new(self.config, 0, records)?);
                    end += 1;
                }
            }
            let key = format!("sgpack-{}-{:08}", seal.attempt, seal.next_pack);
            self.poisoned = true;
            let (references, sketch) = self.publish_pack(&key, &blocks, fingerprints)?;
            self.poisoned = false;
            drop(blocks);
            seal.references.extend(references);
            seal.sketches.push(sketch);
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
        if let Some(clustered) = seal.clustered.as_mut() {
            if clustered.catalog.is_none() && clustered.packs.iter().any(|(_, posting)| *posting) {
                let mut added = Vec::new();
                for (range, posting) in &clustered.packs {
                    if *posting {
                        added.extend(clustered::extents_of(
                            &seal.references[range.clone()],
                            clustered.view.epoch,
                            ExtentKind::Canonical,
                        )?);
                    }
                }
                let catalog = clustered
                    .view
                    .catalog
                    .with_changes(&BTreeSet::new(), added)?;
                let bytes = clustered.view.encode_catalog(&catalog)?;
                let reference =
                    clustered::object_ref(format!("sgcluster-{}", seal.attempt), &bytes);
                self.poisoned = true;
                self.create_staged(&reference.key, &bytes)?;
                self.poisoned = false;
                clustered.catalog = Some((reference, clustered.view.with_catalog(catalog)));
                self.seal = Some(seal);
                return Ok(true);
            }
        }
        let mut root = self.next_root()?;
        root.sequence = seal.boundary;
        root.retry = seal.retry.clone();
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
                blocks: seal.references.clone(),
                manifest: None,
            });
        }
        if let Some((reference, _)) = seal
            .clustered
            .as_ref()
            .and_then(|clustered| clustered.catalog.as_ref())
        {
            root.clustered
                .as_mut()
                .expect("clustered seals keep the view")
                .catalog = reference.clone();
        }
        root.validate(self.config)?;
        // The new run's manifest (and, for a legacy root, every other run's)
        // is created one per step before the root.
        if self.stage_manifest(&mut root)? {
            self.seal = Some(seal);
            return Ok(true);
        }
        let view = seal
            .clustered
            .and_then(|clustered| clustered.catalog)
            .map(|(_, index)| index);
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &manifest::encode_root(&root)?)?;
        self.poisoned = false;
        if !seal.entries.is_empty() {
            let run = root.runs.len() - 1;
            Arc::make_mut(&mut self.latest).merge_sorted(
                seal.entries
                    .into_iter()
                    .map(|entry| (entry.id, Location { run, entry })),
            );
        }
        Arc::make_mut(&mut self.tail).retain(|_, (sequence, _)| *sequence > seal.boundary);
        self.tail_logs.retain(|&first| first > seal.boundary);
        self.tail_objects = self.tail_logs.len();
        self.replace_root(root);
        if let Some(view) = view {
            self.replace_view(view);
        }
        let packs: BTreeSet<_> = seal.sketches.iter().map(|s| s.pack().to_owned()).collect();
        for sketch in seal.sketches {
            Arc::make_mut(&mut self.sketches).install(sketch);
        }
        self.refresh_sketches()?;
        // New canonical rows are new versions no posting covers; new posting
        // rows are current unless a later tail write shadows them.
        self.activate_sketches(Some(&packs), |_| true);
        let (latest, tail) = (&self.latest, &self.tail);
        Arc::make_mut(&mut self.sketches).activate_postings(Some(&packs), |id, sequence| {
            sketch::posting_current(tail, latest, id, sequence)
        });
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
    /// The <=2 MiB index cap bounds this synchronous maintenance step; larger
    /// data reclamation needs a separate staged protocol.
    pub fn consolidate_runs_step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.root_maintenance_blocked() {
            return Err(Error::MaintenanceRequired);
        }
        // A staged conversion reads the runs it froze by position.
        let frozen = self.conversion_frozen_runs();
        let runs = &self.root.runs;
        let pair = (frozen..runs.len().saturating_sub(1)).rev().find(|&i| {
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
        let left = read_run_index(&*self.store, first)?;
        let right = read_run_index(&*self.store, second)?;
        // Both indexes are ID-sorted and an ID is current in at most one run,
        // so a linear merge yields the surviving entries in ID order.
        let latest = &self.latest;
        let keep = |run: usize| {
            move |entry: &IndexEntry| latest.get(&entry.id).is_some_and(|at| at.run == run)
        };
        let mut left = left.entries.into_iter().filter(keep(pair)).peekable();
        let mut right = right.entries.into_iter().filter(keep(pair + 1)).peekable();
        let mut blocks = Vec::new();
        let mut block_map = BTreeMap::new();
        let mut entries = Vec::new();
        loop {
            let take_left = match (left.peek(), right.peek()) {
                (Some(a), Some(b)) => a.id < b.id,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let (run_ordinal, mut entry) = if take_left {
                (pair, left.next().unwrap())
            } else {
                (pair + 1, right.next().unwrap())
            };
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
            entries.push(entry);
        }
        drop((left, right));
        let mut root = self.next_root()?;
        let attempt = attempt_id()?;
        let replacement = if entries.is_empty() {
            None
        } else {
            let index = RunIndex {
                sequence: second.last_sequence,
                entries: std::mem::take(&mut entries),
            };
            let bytes = index.encode(first.first_sequence, blocks.len())?;
            entries = index.entries;
            let object = format!("sgindex-{attempt}");
            Some((
                RunRef {
                    first_sequence: first.first_sequence,
                    last_sequence: second.last_sequence,
                    index_object: object,
                    index_len: bytes.len(),
                    index_sha256: format!("{:x}", Sha256::digest(&bytes)),
                    blocks,
                    manifest: None,
                },
                bytes,
            ))
        };
        root.runs.splice(
            pair..pair + 2,
            replacement.as_ref().map(|(run, _)| run.clone()),
        );
        root.validate(self.config)?;
        if let Some((run, bytes)) = &replacement {
            self.poisoned = true;
            self.create_staged(&run.index_object, bytes)?;
            self.poisoned = false;
        }
        // The merged run's manifest; a legacy root also gets the others'.
        self.stage_manifests(&mut root)?;
        let root_bytes = manifest::encode_root(&root)?;
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &root_bytes)?;
        self.poisoned = false;
        let replacement_count = usize::from(replacement.is_some());
        Arc::make_mut(&mut self.latest).retain(|id, location| {
            if location.run == pair || location.run == pair + 1 {
                if let Ok(index) = entries.binary_search_by_key(&id, |entry| entry.id) {
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
        self.replace_root(root);
        self.refresh_sketches()?;
        self.schedule_obsolete();
        Ok(true)
    }

    /// Rebind sketches after a root change. Liveness of retained rows does not
    /// change; the caller activates newly installed packs.
    fn refresh_sketches(&mut self) -> Result<()> {
        let covered = self.root.clustered.is_some();
        if let Err(error) = Arc::make_mut(&mut self.sketches).refresh(&self.root, covered) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Select a new clustered view after a root that changed its catalog.
    /// A query view holding the old one keeps its packs from cleanup.
    fn replace_view(&mut self, view: ClusterIndex) {
        if let Some(old) = self.cluster.replace(Arc::new(view)) {
            self.retired_views.push(Arc::downgrade(&old));
        }
    }

    /// Remove fully dead block references from one <=1 MiB index run. This
    /// publishes a new index before a root and lets obsolete packs disappear
    /// through ordinary cleanup. Mixed packs become eligible for repacking.
    pub fn start_prune(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.maintenance_active() {
            return Err(Error::MaintenanceRequired);
        }
        let mut live_counts: Vec<Vec<usize>> = self
            .root
            .runs
            .iter()
            .map(|run| vec![0; run.blocks.len()])
            .collect();
        for (id, location) in self.latest.iter() {
            if !self.tail.contains_key(&id) {
                *live_counts
                    .get_mut(location.run)
                    .and_then(|counts| counts.get_mut(location.entry.block as usize))
                    .ok_or_else(|| Error::Corrupt("segmented directory block missing".into()))? +=
                    1;
            }
        }
        let Some(run) = self
            .root
            .runs
            .iter()
            .enumerate()
            .find_map(|(run, reference)| {
                (reference.index_len <= MAX_CONSOLIDATION_INDEX_BYTES
                    && live_counts[run].contains(&0))
                .then_some(run)
            })
        else {
            return Ok(false);
        };
        let source = &self.root.runs[run];
        let mut remap = vec![None; source.blocks.len()];
        let mut blocks = Vec::new();
        for (old, reference) in source.blocks.iter().enumerate() {
            if live_counts[run][old] != 0 {
                let ordinal = u32::try_from(blocks.len())
                    .map_err(|_| Error::Invalid("segmented block count exhausted".into()))?;
                remap[old] = Some(ordinal);
                blocks.push(reference.clone());
            }
        }
        let mut entries = Vec::new();
        for (id, location) in self.latest.iter() {
            if location.run == run && !self.tail.contains_key(&id) {
                let mut entry = location.entry;
                entry.block = remap[entry.block as usize]
                    .ok_or_else(|| Error::Corrupt("live ID in dead segmented block".into()))?;
                entries.push(entry);
            }
        }
        let mut root = self.next_root()?;
        let index = if entries.is_empty() {
            root.runs.remove(run);
            None
        } else {
            let bytes = RunIndex {
                sequence: source.last_sequence,
                entries: entries.clone(),
            }
            .encode(source.first_sequence, blocks.len())?;
            let key = format!("sgindex-{}", attempt_id()?);
            root.runs[run].blocks = blocks;
            root.runs[run].index_object = key.clone();
            root.runs[run].index_len = bytes.len();
            root.runs[run].index_sha256 = format!("{:x}", Sha256::digest(&bytes));
            Some((key, bytes))
        };
        root.validate(self.config)?;
        self.prune = Some(PruneState {
            run,
            root,
            entries,
            index_published: index.is_none(),
            index,
        });
        Ok(true)
    }

    /// Advance one index, run manifest or root PUT. Writes may extend the log tail, but
    /// the frozen root directory cannot change until this publication ends.
    pub fn prune_step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let Some(mut state) = self.prune.take() else {
            return Ok(false);
        };
        if !state.index_published {
            let (key, bytes) = state.index.as_ref().expect("index pending");
            self.poisoned = true;
            self.create_staged(key, bytes)?;
            self.poisoned = false;
            state.index_published = true;
            state.index = None;
            self.prune = Some(state);
            return Ok(true);
        }
        if self.stage_manifest(&mut state.root)? {
            self.prune = Some(state);
            return Ok(true);
        }
        self.poisoned = true;
        self.create_staged(
            &root_key(state.root.generation),
            &manifest::encode_root(&state.root)?,
        )?;
        self.poisoned = false;
        let removed = state.entries.is_empty();
        Arc::make_mut(&mut self.latest).retain(|_, location| {
            if location.run == state.run {
                false
            } else {
                if removed && location.run > state.run {
                    location.run -= 1;
                }
                true
            }
        });
        Arc::make_mut(&mut self.latest).merge_sorted(state.entries.into_iter().map(|entry| {
            (
                entry.id,
                Location {
                    run: state.run,
                    entry,
                },
            )
        }));
        self.replace_root(state.root);
        self.refresh_sketches()?;
        self.schedule_obsolete();
        Ok(true)
    }

    /// Freeze reclaimable packs whose live bytes fit one new pack. The live set is fixed before
    /// reading blocks; later acknowledged log writes may safely shadow it.
    pub fn start_reclaim(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.maintenance_active() {
            return Err(Error::MaintenanceRequired);
        }
        let mut live_counts: BTreeMap<(usize, usize), usize> = BTreeMap::new();
        for (id, location) in self.latest.iter() {
            if !self.tail.contains_key(&id) {
                *live_counts
                    .entry((location.run, location.entry.block as usize))
                    .or_default() += 1;
            }
        }
        let mut packs: BTreeMap<String, PackStats> = BTreeMap::new();
        for (run, run_ref) in self.root.runs.iter().enumerate() {
            for (block, reference) in run_ref.blocks.iter().enumerate() {
                let live = live_counts.get(&(run, block)).copied().unwrap_or(0);
                if live > reference.rows {
                    return Err(Error::Corrupt(
                        "segmented block has more live IDs than records".into(),
                    ));
                }
                let entry = packs.entry(reference.object.clone()).or_insert(PackStats {
                    payload_len: reference.payload_len,
                    data_start: reference.offset,
                    estimated_live_bytes: 0,
                    live_rows: 0,
                    locations: Vec::new(),
                    has_empty_block: false,
                });
                if entry.payload_len != reference.payload_len {
                    return Err(Error::Corrupt(
                        "segmented pack references disagree on length".into(),
                    ));
                }
                entry.estimated_live_bytes +=
                    ((reference.length as u128 * live as u128) / reference.rows as u128) as usize;
                entry.live_rows += live;
                entry.data_start = match self.sketches.frame_len(&reference.object) {
                    Some(frame) => frame,
                    None => entry.data_start.min(reference.offset),
                };
                entry.locations.push((run, block));
                entry.has_empty_block |= live == 0;
            }
        }
        // Merge the packs with the most garbage into one new pack while their
        // estimated live bytes stay within 7/8 of the pack limit; the margin
        // covers estimation error so encoding cannot exceed the limit.
        // A posting pack of the clustered view stays readable through the
        // catalog, so rewriting its canonical blocks frees nothing; without
        // a loaded catalog, nothing is known not to be one.
        if self.root.clustered.is_some() && self.cluster.is_none() {
            return Ok(false);
        }
        let postings = self.cluster.as_ref().map(|cluster| &cluster.packs);
        let mut candidates: Vec<_> = packs
            .into_iter()
            .filter(|(key, _)| postings.is_none_or(|postings| !postings.contains_key(key)))
            .filter(|(_, pack)| {
                // Garbage is measured over block data only; the sketch frame
                // is not garbage, or a fully live pack could be reclaimed
                // forever.
                let data = pack.payload_len - pack.data_start;
                !pack.has_empty_block
                    && pack.estimated_live_bytes <= data / 2
                    && data.saturating_sub(pack.estimated_live_bytes) >= self.reclaim_min_garbage
            })
            .collect();
        let garbage = |pack: &PackStats| {
            (pack.payload_len - pack.data_start).saturating_sub(pack.estimated_live_bytes)
        };
        candidates.sort_by(|(a_key, a), (b_key, b)| {
            garbage(b).cmp(&garbage(a)).then_with(|| a_key.cmp(b_key))
        });
        // Live rows are capped too: reclaimed blocks are decoded in memory
        // until the new pack is written.
        const MAX_RECLAIM_ROWS: usize = MAX_PACK_BLOCKS * 170;
        let mut locations = Vec::new();
        let (mut live, mut rows) = (0_usize, 0_usize);
        for (_, pack) in candidates {
            if !locations.is_empty()
                && (live + pack.estimated_live_bytes > MAX_PACK_BYTES / 8 * 7
                    || rows + pack.live_rows > MAX_RECLAIM_ROWS)
            {
                continue;
            }
            live += pack.estimated_live_bytes;
            rows += pack.live_rows;
            let mut pack_locations = pack.locations;
            pack_locations.sort_by_key(|&(run, block)| self.root.runs[run].blocks[block].offset);
            locations.extend(pack_locations);
        }
        if locations.is_empty() {
            return Ok(false);
        }
        let pack = PackStats {
            payload_len: 0,
            data_start: 0,
            estimated_live_bytes: live,
            live_rows: rows,
            locations,
            has_empty_block: false,
        };
        let mut expected_by_location: BTreeMap<_, BTreeMap<_, _>> = pack
            .locations
            .iter()
            .copied()
            .map(|location| (location, BTreeMap::new()))
            .collect();
        for (id, location) in self.latest.iter() {
            if !self.tail.contains_key(&id) {
                if let Some(expected) =
                    expected_by_location.get_mut(&(location.run, location.entry.block as usize))
                {
                    expected.insert(id, (location.entry.sequence, location.entry.deleted));
                }
            }
        }
        let expected = pack
            .locations
            .iter()
            .map(|location| {
                expected_by_location
                    .remove(location)
                    .expect("location present")
            })
            .collect();
        self.reclaim = Some(ReclaimState {
            locations: pack.locations,
            expected,
            blocks: Vec::new(),
            key: format!("sgpack-{}-00000000", attempt_id()?),
            references: None,
            sketch: None,
        });
        Ok(true)
    }

    /// Advance reclamation by one block GET, pack, run manifest or root PUT. Only the
    /// root PUT changes authoritative visibility. An uncertain write poisons
    /// this handle, and reopen determines whether publication happened.
    pub fn reclaim_step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let Some(mut state) = self.reclaim.take() else {
            return Ok(false);
        };
        if state.references.is_none() && state.blocks.len() < state.locations.len() {
            let ordinal = state.blocks.len();
            let (run, block) = state.locations[ordinal];
            let reference = &self.root.runs[run].blocks[block];
            let old = match read_block(&*self.store, self.config, reference) {
                Ok(block) => block,
                Err(error) => {
                    self.reclaim = Some(state);
                    return Err(error);
                }
            };
            let expected = &state.expected[ordinal];
            let records: Vec<_> = old
                .records
                .into_iter()
                .filter(|record| {
                    expected
                        .get(&record.id())
                        .is_some_and(|&(sequence, deleted)| {
                            sequence == record.sequence
                                && deleted == matches!(record.mutation, Mutation::Delete { .. })
                        })
                })
                .collect();
            if records.len() != expected.len() {
                return Err(Error::Corrupt(
                    "segmented live directory disagrees with physical block".into(),
                ));
            }
            state
                .blocks
                .push(Block::new(self.config, old.partition, records)?);
            self.reclaim = Some(state);
            return Ok(true);
        }
        if state.references.is_none() {
            self.poisoned = true;
            let (references, sketch) =
                self.publish_pack(&state.key.clone(), &state.blocks, None)?;
            self.poisoned = false;
            state.references = Some(references);
            state.sketch = Some(sketch);
            state.blocks.clear();
            state.expected.clear();
            self.reclaim = Some(state);
            return Ok(true);
        }
        let mut root = self.next_root()?;
        for (index, &(run, block)) in state.locations.iter().enumerate() {
            root.runs[run].blocks[block] = state.references.as_ref().unwrap()[index].clone();
        }
        root.validate(self.config)?;
        // Every run that lost a block to the new pack gets a new manifest,
        // one create per step.
        if self.stage_manifest(&mut root)? {
            self.reclaim = Some(state);
            return Ok(true);
        }
        let root_bytes = manifest::encode_root(&root)?;
        // With a clustered view a moved row stays routed exactly when its old
        // canonical row was: postings cover the others.
        let routed: Option<BTreeSet<u64>> = self.root.clustered.is_some().then(|| {
            state
                .locations
                .iter()
                .flat_map(|&(run, block)| self.sketches.live_ids(run, block))
                .collect()
        });
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &root_bytes)?;
        self.poisoned = false;
        self.replace_root(root);
        let sketch = state.sketch.expect("reclaim sketch staged with its pack");
        let packs = BTreeSet::from([sketch.pack().to_owned()]);
        Arc::make_mut(&mut self.sketches).install(sketch);
        self.refresh_sketches()?;
        self.activate_sketches(Some(&packs), |id| {
            routed.as_ref().is_none_or(|routed| routed.contains(&id))
        });
        self.schedule_obsolete();
        Ok(true)
    }

    /// Synchronous convenience for callers without a maintenance scheduler.
    pub fn reclaim_pack_step(&mut self) -> Result<bool> {
        if !self.start_reclaim()? {
            return Ok(false);
        }
        while self.reclaim.is_some() {
            self.reclaim_step()?;
        }
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
        // A query on an older view may still read a pack its root or its
        // clustered view references.
        self.retired.retain(|root| root.strong_count() > 0);
        self.retired_views.retain(|view| view.strong_count() > 0);
        let pinned: Vec<_> = self.retired.iter().filter_map(Weak::upgrade).collect();
        let pinned_views: Vec<_> = self
            .retired_views
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let keys: Vec<_> = self
            .obsolete
            .iter()
            .filter(|key| {
                !pinned.iter().any(|root| {
                    root.runs
                        .iter()
                        .any(|run| run.blocks.iter().any(|block| &block.object == *key))
                }) && !pinned_views
                    .iter()
                    .any(|view| view.packs.contains_key(key.as_str()))
            })
            .take(max_objects)
            .cloned()
            .collect();
        drop((pinned, pinned_views));
        if keys.is_empty() {
            return Ok(0);
        }
        self.poisoned = true;
        self.store.remove_many(&keys)?;
        for key in &keys {
            self.known_keys.remove(key);
        }
        self.obsolete.retain(|key| !keys.contains(key));
        self.poisoned = false;
        Ok(keys.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{store::LocalStore, Metric};
    use std::collections::BTreeMap;

    struct PropertyRng(u64);

    impl PropertyRng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn value(&mut self) -> f32 {
            match self.next() % 8 {
                0 => -0.,
                1 => f32::MAX,
                2 => f32::MIN,
                _ => (self.next() as i32 as f32) / 2048.,
            }
        }
    }

    #[test]
    fn run_index_decoder_round_trips_and_rejects_mutations() {
        let seed = 0x7a31_90cc_d551_2e04_u64;
        let mut rng = PropertyRng(seed);
        for case in 0..40 {
            let first = 1 + rng.next() % 100;
            let last = first + rng.next() % 100;
            let blocks = 1 + rng.next() as usize % 8;
            let entries = (0..1 + rng.next() % 30)
                .map(|row| IndexEntry {
                    id: row * 3 + rng.next() % 3,
                    sequence: first + rng.next() % (last - first + 1),
                    block: rng.next() as u32 % blocks as u32,
                    deleted: rng.next().is_multiple_of(2),
                })
                .collect();
            let index = RunIndex {
                sequence: last,
                entries,
            };
            let bytes = index.encode(first, blocks).unwrap();
            let decoded = RunIndex::decode(&bytes, first, last, blocks).unwrap();
            assert_eq!(
                decoded.entries, index.entries,
                "seed {seed:#x}, case {case}"
            );
            assert_eq!(
                decoded.encode(first, blocks).unwrap(),
                bytes,
                "seed {seed:#x}, case {case}"
            );
            let check = |candidate: &[u8], mutation: &str| {
                let result =
                    std::panic::catch_unwind(|| RunIndex::decode(candidate, first, last, blocks));
                let decoded = result.unwrap_or_else(|_| {
                    panic!("index panicked: seed {seed:#x}, case {case}, {mutation}")
                });
                if let Ok(index) = decoded {
                    assert_eq!(
                        index.encode(first, blocks).unwrap(),
                        candidate,
                        "seed {seed:#x}, case {case}, {mutation}"
                    );
                }
            };
            for length in 0..bytes.len().min(160) {
                check(&bytes[..length], &format!("truncate {length}"));
            }
            check(&bytes[..bytes.len() - 1], "truncate final byte");
            for flip in 0..24 {
                let mut changed = bytes.clone();
                let offset = rng.next() as usize % changed.len();
                changed[offset] ^= 1 << (rng.next() % 8);
                check(&changed, &format!("flip {flip} at {offset}"));
            }
            let mut changed = bytes.clone();
            changed.push(0);
            check(&changed, "append");
            let mut changed = bytes.clone();
            changed[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
            check(&changed, "huge entry count");
        }
    }

    #[test]
    fn authenticated_block_dispatch_round_trips_and_rejects_mutations() {
        let seed = 0x29ac_773e_0861_b4d2_u64;
        let mut rng = PropertyRng(seed);
        for case in 0..40 {
            let config = Config {
                dimensions: 1 + case as usize,
                metric: if case % 2 == 0 {
                    Metric::SquaredEuclidean
                } else {
                    Metric::Manhattan
                },
            };
            let records = (0..1 + rng.next() % 6)
                .map(|row| {
                    let id = row * 3 + rng.next() % 3;
                    let mutation = if rng.next().is_multiple_of(4) {
                        Mutation::Delete { id }
                    } else {
                        let metadata = (0..rng.next() % 3)
                            .map(|key| (format!("key-{key}"), format!("value-{}", rng.next())))
                            .collect();
                        Mutation::Put {
                            id,
                            vector: (0..config.dimensions).map(|_| rng.value()).collect(),
                            metadata,
                        }
                    };
                    BlockRecord {
                        sequence: 1 + rng.next() % 100,
                        mutation,
                    }
                })
                .collect();
            let block = Block::new(config, case, records).unwrap();
            let (_, refs) =
                encode_pack("pack-property", config, std::slice::from_ref(&block)).unwrap();
            for (version, bytes) in [
                (1, encode(&block).unwrap()),
                (2, codec::encode(&block).unwrap()),
            ] {
                let mut reference = refs[0].clone();
                reference.length = bytes.len();
                reference.payload_len = bytes.len();
                reference.sha256 = format!("{:x}", Sha256::digest(&bytes));
                let decoded = decode_block_bytes(config, &reference, &bytes).unwrap();
                let reencoded = if version == 1 {
                    encode(&decoded).unwrap()
                } else {
                    codec::encode(&decoded).unwrap()
                };
                assert_eq!(
                    reencoded, bytes,
                    "seed {seed:#x}, case {case}, version {version}"
                );
                let check = |candidate: &[u8], mutation: &str, authenticated: bool| {
                    let mut reference = reference.clone();
                    if authenticated {
                        reference.length = candidate.len();
                        reference.sha256 = format!("{:x}", Sha256::digest(candidate));
                    }
                    let result = std::panic::catch_unwind(|| {
                        decode_block_bytes(config, &reference, candidate)
                    });
                    let decoded = result.unwrap_or_else(|_| panic!("block dispatch panicked: seed {seed:#x}, case {case}, version {version}, {mutation}"));
                    // Accepted bytes must at least decode to a valid block
                    // whose re-encoding is stable; digests reject altered
                    // bytes in production before decoding.
                    if let Ok(block) = decoded {
                        let encode_version = |block: &Block| {
                            if version == 1 {
                                encode(block).unwrap()
                            } else {
                                codec::encode(block).unwrap()
                            }
                        };
                        let reencoded = encode_version(&block);
                        let again = decode_block_bytes(
                            config,
                            &BlockRef {
                                length: reencoded.len(),
                                sha256: format!("{:x}", Sha256::digest(&reencoded)),
                                ..reference.clone()
                            },
                            &reencoded,
                        )
                        .unwrap();
                        assert_eq!(
                            encode_version(&again),
                            reencoded,
                            "seed {seed:#x}, case {case}, version {version}, {mutation}"
                        );
                    }
                };
                for length in 0..bytes.len().min(96) {
                    check(
                        &bytes[..length],
                        &format!("truncate {length}"),
                        version == 2,
                    );
                }
                check(
                    &bytes[..bytes.len() - 1],
                    "truncate final byte",
                    version == 2,
                );
                for flip in 0..12 {
                    let mut changed = bytes.clone();
                    let offset = rng.next() as usize % changed.len();
                    changed[offset] ^= 1 << (rng.next() % 8);
                    check(&changed, &format!("flip {flip} at {offset}"), version == 2);
                    check(&changed, "wrong digest", false);
                }
                let mut changed = bytes.clone();
                changed.push(0);
                check(&changed, "append", version == 2);
                if version == 2 {
                    let mut changed = bytes.clone();
                    changed[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
                    check(&changed, "huge raw length", true);
                    let mut raw =
                        zstd::bulk::decompress(&bytes[8..], codec::MAX_RAW_BLOCK_BYTES).unwrap();
                    raw[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
                    let mut changed = bytes[..8].to_vec();
                    changed.extend_from_slice(&zstd::bulk::compress(&raw, 3).unwrap());
                    check(&changed, "huge record count", true);
                } else {
                    let mut changed = bytes.clone();
                    changed[0] = b'!';
                    check(&changed, "authenticated invalid JSON", true);
                }
                let mut wrong = reference.clone();
                wrong.rows += 1;
                assert!(
                    decode_block_bytes(config, &wrong, &bytes).is_err(),
                    "seed {seed:#x}, case {case}, version {version}"
                );
            }
        }
    }

    /// `base + id` on every axis plus under 0.01 of noise on axes after the
    /// first, so test blocks do not compress to a trivial size.
    fn noisy(base: f32, id: u64) -> Vec<f32> {
        (0..128_u64)
            .map(|axis| {
                let noise = (id.wrapping_mul(2_654_435_761) ^ axis.wrapping_mul(40_503)) % 1_000;
                base + id as f32
                    + if axis == 0 {
                        0.
                    } else {
                        noise as f32 / 100_000.
                    }
            })
            .collect()
    }

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
        let store = LocalStore::open(temp.path().join("db")).unwrap();
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
    fn block_formats_preserve_every_component_bit() {
        let config = Config {
            dimensions: 6,
            metric: Metric::SquaredEuclidean,
        };
        let vector = vec![3., -0., 0.1, -7., 1e30, 16_777_216.];
        let block = Block::new(
            config,
            0,
            vec![BlockRecord {
                sequence: 1,
                mutation: Mutation::Put {
                    id: 9,
                    vector: vector.clone(),
                    metadata: BTreeMap::new(),
                },
            }],
        )
        .unwrap();
        let bits = |v: &[f32]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
        let (bytes, references) =
            encode_pack("sgpack-x-00000000", config, std::slice::from_ref(&block)).unwrap();
        assert!(codec::is_v2(&bytes));
        // Version 1 JSON blocks written by earlier binaries remain readable.
        let json = encode(&block).unwrap();
        let json_reference = BlockRef {
            length: json.len(),
            payload_len: json.len(),
            sha256: format!("{:x}", Sha256::digest(&json)),
            ..references[0].clone()
        };
        for (reference, bytes) in [(&references[0], &bytes), (&json_reference, &json)] {
            let decoded = decode_block_bytes(config, reference, bytes).unwrap();
            let Mutation::Put { vector: found, .. } = &decoded.records[0].mutation else {
                panic!("put expected");
            };
            assert_eq!(bits(found), bits(&vector));
        }
        let mut corrupt = bytes.clone();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(codec::decode(config, &corrupt).is_err());
    }

    #[test]
    fn reclamation_merges_several_mostly_dead_packs_into_one() {
        let config = Config {
            dimensions: 128,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let mut db = SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config)
            .unwrap()
            .with_reclaim_min_garbage(1);
        let mut nonce = 0_u8;
        let mut write =
            |db: &mut SegmentedDatabase<LocalStore>, ids: std::ops::Range<u64>, value: f32| {
                for chunk in ids.collect::<Vec<_>>().chunks(100) {
                    nonce += 1;
                    db.apply_request(retry::Request {
                        id: retry::RequestId {
                            boundary: db.sequence(),
                            nonce: [nonce; 16],
                        },
                        conditions: Vec::new(),
                        mutations: chunk
                            .iter()
                            .map(|&id| Mutation::Put {
                                id,
                                vector: noisy(value, id),
                                metadata: BTreeMap::new(),
                            })
                            .collect(),
                    })
                    .unwrap();
                }
            };
        write(&mut db, 0..400, 1_000.);
        db.seal_delta().unwrap();
        write(&mut db, 400..800, 2_000.);
        db.seal_delta().unwrap();
        let packs = |db: &SegmentedDatabase<LocalStore>| {
            db.root
                .runs
                .iter()
                .flat_map(|run| run.blocks.iter().map(|block| block.object.clone()))
                .collect::<BTreeSet<_>>()
        };
        let before = packs(&db);
        assert_eq!(before.len(), 2);
        write(&mut db, 0..300, 5_000.);
        write(&mut db, 400..700, 6_000.);
        db.seal_delta().unwrap();
        // Fully dead blocks are pruned first; reclamation needs live blocks.
        while db.start_prune().unwrap() {
            while db.prune_step().unwrap() {}
        }
        assert!(before.is_subset(&packs(&db)));
        let generation = db.root.generation;
        assert!(db.reclaim_pack_step().unwrap());
        assert_eq!(db.root.generation, generation + 1);
        let after = packs(&db);
        assert!(before.iter().all(|pack| !after.contains(pack)));
        assert_eq!(after.len(), 2);
        let exact = db.search_exact(&[1_350.; 128], 5, &[]).unwrap();
        assert_eq!(
            db.search_selective(&[1_350.; 128], 5, 1_000, &[]).unwrap(),
            exact
        );
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
        assert_eq!(db.sketch_rebuilds(), 0);
        assert_eq!(db.search_exact(&[1_350.; 128], 5, &[]).unwrap(), exact);
    }

    #[test]
    fn group_commit_publishes_independent_requests_in_one_log_object() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let mut db =
            SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
        let put = |nonce: u8, id: u64, value: f32, conditions| retry::Request {
            id: retry::RequestId {
                boundary: 0,
                nonce: [nonce; 16],
            },
            conditions,
            mutations: vec![Mutation::Put {
                id,
                vector: vec![value, 0.],
                metadata: BTreeMap::new(),
            }],
        };
        let first = put(1, 7, 1., Vec::new());
        // Observed id 7 before the group; the first request changes it.
        let stale = put(2, 8, 2., vec![retry::Revision { id: 7, boundary: 0 }]);
        let results = db.apply_requests(vec![first.clone(), stale, first.clone()]);
        let outcomes: Vec<_> = results.into_iter().map(Result::unwrap).collect();
        assert_eq!(outcomes[0].sequence, 1);
        assert_eq!(outcomes[1].sequence, 2);
        assert_eq!(outcomes[1].conflict, Some(retry::Conflict::StaleRevision));
        assert_eq!(outcomes[2], outcomes[0]);
        assert_eq!((db.sequence(), db.tail_objects), (2, 1));
        let logs = |db: &SegmentedDatabase<LocalStore>| {
            db.store
                .list()
                .unwrap()
                .into_iter()
                .filter(|key| key.starts_with("sglog-"))
                .count()
        };
        assert_eq!(logs(&db), 1);
        db.apply_request(put(3, 9, 3., Vec::new())).unwrap();
        drop(db);
        let mut db =
            SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
        assert_eq!((db.sequence(), db.tail_objects, logs(&db)), (3, 2, 2));
        assert_eq!(db.get(7).unwrap().unwrap().vector, vec![1., 0.]);
        assert!(db.get(8).unwrap().is_none());
        assert_eq!(db.apply_request(first).unwrap(), outcomes[0]);
        db.seal_delta().unwrap();
        assert_eq!(db.tail_objects, 0);
        while db.cleanup_step(16).unwrap() > 0 {}
        assert_eq!(logs(&db), 0);
        assert_eq!(db.get(9).unwrap().unwrap().vector, vec![3., 0.]);
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
        let store = LocalStore::open(temp.path().join("db")).unwrap();
        store.create("index-1", &bytes).unwrap();
        let run = RunRef {
            first_sequence: 1,
            last_sequence: 1,
            index_object: "index-1".into(),
            index_len: bytes.len(),
            index_sha256: format!("{:x}", Sha256::digest(&bytes)),
            blocks,
            manifest: None,
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
    fn clustered_root_v4_is_strict_and_old_reader_rejects_it() {
        #[allow(dead_code)]
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct OldRoot {
            version: u32,
            generation: u64,
            sequence: u64,
            config: Config,
            retry: retry::State,
            runs: Vec<RunRef>,
            fences: Fences,
        }

        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let mut root = Root::empty(config);
        root.generation = 1;
        let legacy = encode(&root).unwrap();
        assert!(matches!(
            decode_root(&legacy, config, 1),
            Ok(RootObject::Root(_))
        ));
        root.version = 4;
        root.sequence = 1;
        assert!(matches!(root.validate(config), Err(Error::Corrupt(_))));
        root.clustered = Some(clustered::ViewRef {
            epoch: 1,
            centroid: clustered::ObjectRef {
                key: "sgcentroid-attempt".into(),
                length: 100,
                sha256: "a".repeat(64),
            },
            catalog: clustered::ObjectRef {
                key: "sgcluster-attempt".into(),
                length: 100,
                sha256: "b".repeat(64),
            },
        });
        let bytes = encode(&root).unwrap();
        assert!(matches!(
            decode_root(&bytes, config, 1),
            Ok(RootObject::Root(_))
        ));
        assert!(serde_json::from_slice::<OldRoot>(&bytes).is_err());
        let temp = tempfile::tempdir().unwrap();
        let store = LocalStore::open(temp.path()).unwrap();
        store
            .create(
                "metadata",
                &metadata_bytes(config, &SegmentedOptions::default()).unwrap(),
            )
            .unwrap();
        store
            .create(&root_key(0), &encode(&Root::empty(config)).unwrap())
            .unwrap();
        store.create(&root_key(1), &bytes).unwrap();
        // The view's objects are missing: exact reads work, but selective
        // queries fail until a conversion rebuilds the view.
        let db = SegmentedDatabase::open(store, config).unwrap();
        assert!(db
            .clustered_view_error()
            .is_some_and(|error| error.contains("missing")));
        assert_eq!(db.clustered_epoch(), Some(1));
        assert!(db.search_exact(&[0., 0.], 1, &[]).unwrap().is_empty());
        assert!(matches!(
            db.search_selective(&[0., 0.], 1, 1, &[]),
            Err(Error::Corrupt(_))
        ));
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("v1");
        drop(SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap());
        let store = LocalStore::open(&path).unwrap();
        store.create("sgcentroid-orphan", b"incomplete").unwrap();
        store.create("sgcluster-orphan", b"incomplete").unwrap();
        let db = SegmentedDatabase::open(store, config).unwrap();
        assert!(db.search_exact(&[0., 0.], 1, &[]).unwrap().is_empty());
        for candidate in [
            &bytes[..bytes.len() - 1],
            &bytes[..bytes.len() / 2],
            b"{\"version\":4}".as_slice(),
        ] {
            assert!(matches!(
                decode_root(candidate, config, 1),
                Err(Error::Corrupt(_))
            ));
        }
        let mut bad: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        bad["clustered"]["centroid"]["sha256"] = "bad".into();
        assert!(matches!(
            decode_root(&encode(&bad).unwrap(), config, 1),
            Err(Error::Corrupt(_))
        ));
        let mut bad: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        bad["clustered"]["epoch"] = 0.into();
        assert!(matches!(
            decode_root(&encode(&bad).unwrap(), config, 1),
            Err(Error::Corrupt(_))
        ));
        let mut bad: serde_json::Value = serde_json::from_slice(&legacy).unwrap();
        bad["clustered"] =
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["clustered"].clone();
        assert!(matches!(
            decode_root(&encode(&bad).unwrap(), config, 1),
            Err(Error::Corrupt(_))
        ));
        bad["clustered"] = serde_json::Value::Null;
        assert!(matches!(
            decode_root(&encode(&bad).unwrap(), config, 1),
            Err(Error::Corrupt(_))
        ));
        let mut old_v2 = root.clone();
        old_v2.version = 2;
        old_v2.fences.logs.insert(1);
        old_v2.fences.roots.insert(1);
        old_v2.generation = 2;
        assert!(matches!(old_v2.validate(config), Err(Error::Corrupt(_))));
        old_v2.clustered = None;
        assert!(matches!(
            decode_root(&encode(&old_v2).unwrap(), config, 2),
            Ok(RootObject::Root(_))
        ));
        let seed = 0x3704_5f30_995a_11c2_u64;
        let mut rng = PropertyRng(seed);
        for length in 0..bytes.len() {
            let outcome = std::panic::catch_unwind(|| decode_root(&bytes[..length], config, 1));
            assert!(
                outcome.is_ok(),
                "root truncation panicked: seed {seed:#x}, {length}"
            );
            assert!(matches!(outcome.unwrap(), Err(Error::Corrupt(_))));
        }
        for flip in 0..128 {
            let mut changed = bytes.clone();
            let offset = rng.next() as usize % changed.len();
            changed[offset] ^= 1 << (rng.next() % 8);
            let outcome = std::panic::catch_unwind(|| decode_root(&changed, config, 1));
            assert!(
                outcome.is_ok(),
                "root mutation panicked: seed {seed:#x}, flip {flip}"
            );
        }
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
    fn sealing_vector_local_blocks_keeps_the_id_directory_exact() {
        let config = Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let mut db =
            SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
        for (batch, ids) in (0..340_u64).collect::<Vec<_>>().chunks(100).enumerate() {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: batch as u64,
                    nonce: [batch as u8; 16],
                },
                conditions: Vec::new(),
                mutations: ids
                    .iter()
                    .map(|&id| Mutation::Put {
                        id,
                        vector: if id % 2 == 0 {
                            vec![0., 0.]
                        } else {
                            vec![100., 100.]
                        },
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
        }
        db.seal_delta().unwrap();
        assert_eq!(db.block_count(), 2);
        assert_ne!(db.current_block_of(0), db.current_block_of(1));
        assert_eq!(db.current_block_of(0), db.current_block_of(2));
        assert_eq!(db.current_block_of(1), db.current_block_of(3));
        for reference in &db.root.runs[0].blocks {
            let block = read_block(&*db.store, config, reference).unwrap();
            assert_eq!(block.records.len(), 170);
            assert!(block
                .records
                .iter()
                .all(|record| record.id() % 2 == block.records[0].id() % 2));
        }
        assert_eq!(
            db.search_exact(&[0., 0.], 3, &[])
                .unwrap()
                .iter()
                .map(|neighbor| neighbor.id)
                .collect::<Vec<_>>(),
            vec![0, 2, 4]
        );
        assert!(db.selective_index_bytes() < 1024 * 1024);
        let selected_ids = |db: &SegmentedDatabase<LocalStore>| {
            db.search_selective(&[0., 0.], 3, 1, &[])
                .unwrap()
                .into_iter()
                .map(|neighbor| neighbor.id)
                .collect::<Vec<_>>()
        };
        assert_eq!(selected_ids(&db), vec![0, 2, 4]);
        db.apply_request(retry::Request {
            id: retry::RequestId {
                boundary: 4,
                nonce: [4; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: 0,
                vector: vec![200., 200.],
                metadata: BTreeMap::new(),
            }],
        })
        .unwrap();
        assert_eq!(selected_ids(&db), vec![2, 4, 6]);
        db.seal_delta().unwrap();
        assert_eq!(selected_ids(&db), vec![2, 4, 6]);
        assert!(db
            .search_selective(&[0., 0.], 3, 1, &[("a", "b")])
            .unwrap()
            .is_empty());
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
        assert_eq!(db.sketch_rebuilds(), 0);
        assert_eq!(selected_ids(&db), vec![2, 4, 6]);
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
        assert!(db.seal_step().unwrap()); // pack and sketch for sequence 1
        db.apply_request(put(1, 2, 42, 3.)).unwrap();
        assert!(db.seal_step().unwrap()); // index for sequence 1
        db.apply_request(put(2, 3, 7, 4.)).unwrap();
        assert!(db.seal_step().unwrap()); // run manifest for sequence 1
        assert_eq!(db.root.sequence, 0);
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

    #[test]
    fn reclaiming_a_mixed_pack_preserves_latest_rows_and_log_tail() {
        let config = Config {
            dimensions: 128,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        for batch in 0..4 {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: batch,
                    nonce: [batch as u8; 16],
                },
                conditions: Vec::new(),
                mutations: (0..100)
                    .map(|offset| {
                        let id = batch * 100 + offset;
                        Mutation::Put {
                            id,
                            vector: noisy(0., id),
                            metadata: BTreeMap::new(),
                        }
                    })
                    .collect(),
            })
            .unwrap();
        }
        db.seal_delta().unwrap();
        let old_pack = db.root.runs[0].blocks[0].object.clone();
        let overwritten: Vec<_> = (0..400_u64).filter(|id| id % 4 != 3).collect();
        for (batch, ids) in overwritten.chunks(100).enumerate() {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: 4 + batch as u64,
                    nonce: [4 + batch as u8; 16],
                },
                conditions: Vec::new(),
                mutations: ids
                    .iter()
                    .map(|&id| Mutation::Put {
                        id,
                        vector: noisy(1000., id),
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
        }
        assert!(db.start_reclaim().unwrap());
        assert!(db.reclaim_step().unwrap());
        assert!(matches!(db.start_seal(), Err(Error::MaintenanceRequired)));
        assert!(matches!(
            db.consolidate_runs_step(),
            Err(Error::MaintenanceRequired)
        ));
        db.apply_request(retry::Request {
            id: retry::RequestId {
                boundary: 7,
                nonce: [7; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: 1,
                vector: vec![2001.; 128],
                metadata: BTreeMap::new(),
            }],
        })
        .unwrap();
        while db.reclaim_step().unwrap() {}
        assert!(!db.root.runs[0]
            .blocks
            .iter()
            .any(|reference| reference.object == old_pack));
        assert_eq!(db.root.sequence, 4);
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert_eq!(db.get(1).unwrap().unwrap().vector[0], 2001.);
        while db.cleanup_step(8).unwrap() != 0 {}
        assert!(!db.known_keys.contains(&old_pack));
        drop(db);
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert_eq!(db.get(1).unwrap().unwrap().vector[0], 2001.);
        assert_eq!(db.search_exact(&vec![2001.; 128], 1, &[]).unwrap()[0].id, 1);
        db.seal_delta().unwrap();
        while db.cleanup_step(8).unwrap() != 0 {}
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert_eq!(db.get(1).unwrap().unwrap().vector[0], 2001.);
    }

    #[test]
    fn pruning_dead_blocks_reclaims_mixed_pack_then_removes_empty_run() {
        let config = Config {
            dimensions: 128,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_reclaim_min_garbage(1);
        for batch in 0..4_u64 {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: batch,
                    nonce: [batch as u8; 16],
                },
                conditions: Vec::new(),
                mutations: (batch * 100..(batch + 1) * 100)
                    .map(|id| Mutation::Put {
                        id,
                        vector: noisy(0., id),
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
        }
        db.seal_delta().unwrap();
        assert_eq!(db.root.runs.len(), 1);
        assert!(db.root.runs[0].blocks.len() >= 2);
        let initial_blocks = db.root.runs[0].blocks.len();
        let dead_through = db.root.runs[0].blocks[initial_blocks - 2].last_id;
        let old_pack = db.root.runs[0].blocks[0].object.clone();
        assert_eq!(db.root.runs[0].blocks[1].object, old_pack);
        let mut boundary = 4_u64;
        for chunk in (0..=dead_through).collect::<Vec<_>>().chunks(100) {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary,
                    nonce: [boundary as u8; 16],
                },
                conditions: Vec::new(),
                mutations: chunk
                    .iter()
                    .map(|&id| Mutation::Put {
                        id,
                        vector: noisy(1000., id),
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
            boundary += 1;
        }
        assert!(!db.start_reclaim().unwrap());
        assert!(db.start_prune().unwrap());
        assert!(db.prune_step().unwrap());
        assert!(matches!(db.start_seal(), Err(Error::MaintenanceRequired)));
        db.apply_request(retry::Request {
            id: retry::RequestId {
                boundary,
                nonce: [boundary as u8; 16],
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: dead_through + 1,
                vector: vec![2000.; 128],
                metadata: BTreeMap::new(),
            }],
        })
        .unwrap();
        boundary += 1;
        assert!(db.prune_step().unwrap()); // run manifest
        assert!(db.prune_step().unwrap()); // root
        assert!(!db.prune_step().unwrap());
        assert_eq!(db.root.runs[0].blocks.len(), 1);
        assert!(db.reclaim_pack_step().unwrap());
        while db.cleanup_step(8).unwrap() != 0 {}
        assert!(!db.known_keys.contains(&old_pack));
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert_eq!(db.get(dead_through + 1).unwrap().unwrap().vector[0], 2000.);
        drop(db);
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert_eq!(db.get(dead_through + 1).unwrap().unwrap().vector[0], 2000.);
        for chunk in (dead_through + 1..400).collect::<Vec<_>>().chunks(100) {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary,
                    nonce: [boundary as u8; 16],
                },
                conditions: Vec::new(),
                mutations: chunk.iter().map(|&id| Mutation::Delete { id }).collect(),
            })
            .unwrap();
            boundary += 1;
        }
        assert!(db.start_prune().unwrap());
        assert!(db.prune_step().unwrap());
        assert_eq!(db.run_count(), 0);
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert!(db.get(dead_through + 1).unwrap().is_none());
        while db.cleanup_step(8).unwrap() != 0 {}
        drop(db);
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.run_count(), 0);
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert!(db.get(dead_through + 1).unwrap().is_none());
        db.seal_delta().unwrap();
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 1000.);
        assert!(db.get(dead_through + 1).unwrap().is_none());
    }

    #[test]
    fn disposable_block_cache_survives_restart_and_recovers_from_loss_and_corruption() {
        let config = Config {
            dimensions: 128,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let cache_path = temp.path().join("cache");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        for batch in 0..4_u64 {
            db.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: batch,
                    nonce: [batch as u8; 16],
                },
                conditions: Vec::new(),
                mutations: (batch * 100..(batch + 1) * 100)
                    .map(|id| Mutation::Put {
                        id,
                        vector: noisy(0., id),
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
        }
        db.seal_delta().unwrap();
        drop(db);
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_block_cache(&cache_path, 2 * MAX_BLOCK_BYTES, 256 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        let stats = db.cache_stats().unwrap().unwrap();
        assert_eq!((stats.remote_fetches, stats.ram_hits), (1, 1));
        assert_eq!(stats.nvme_entries, 1);
        assert!(stats.ram_bytes <= 2 * MAX_BLOCK_BYTES);
        drop(db);
        db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_block_cache(&cache_path, 2 * MAX_BLOCK_BYTES, 256 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        let stats = db.cache_stats().unwrap().unwrap();
        assert_eq!((stats.nvme_hits, stats.remote_fetches), (1, 0));
        drop(db);
        let disk = cache_path.join("glider-block-cache-v1");
        let entry = std::fs::read_dir(&disk)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(entry, b"damaged disposable cache bytes").unwrap();
        db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_block_cache(&cache_path, 2 * MAX_BLOCK_BYTES, 256 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        let stats = db.cache_stats().unwrap().unwrap();
        assert_eq!((stats.corrupt_entries, stats.remote_fetches), (1, 1));
        drop(db);
        db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_block_cache(&cache_path, 2 * MAX_BLOCK_BYTES, 256 * MAX_BLOCK_BYTES)
            .unwrap();
        let entry = std::fs::read_dir(&disk)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::remove_file(entry).unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        assert_eq!(db.cache_stats().unwrap().unwrap().remote_fetches, 1);
        drop(db);
        std::fs::remove_dir_all(&cache_path).unwrap();
        std::fs::create_dir_all(&cache_path).unwrap();
        std::fs::write(cache_path.join("glider-block-cache-v1"), b"unavailable").unwrap();
        let fallback = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_block_cache(&cache_path, 2 * MAX_BLOCK_BYTES, 256 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(fallback.get(0).unwrap().unwrap().vector[0], 0.);
        let stats = fallback.cache_stats().unwrap().unwrap();
        assert!(!stats.nvme_available);
        assert_eq!(stats.remote_fetches, 1);
        drop(fallback);
        std::fs::remove_dir_all(&cache_path).unwrap();
        #[cfg(unix)]
        {
            let unrelated = temp.path().join("unrelated");
            std::fs::create_dir(&unrelated).unwrap();
            std::fs::write(unrelated.join("keep"), b"unrelated").unwrap();
            std::fs::create_dir(&cache_path).unwrap();
            std::os::unix::fs::symlink(&unrelated, cache_path.join("glider-block-cache-v1"))
                .unwrap();
            let fallback = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
                .unwrap()
                .with_block_cache(&cache_path, 2 * MAX_BLOCK_BYTES, 256 * MAX_BLOCK_BYTES)
                .unwrap();
            assert_eq!(fallback.get(0).unwrap().unwrap().vector[0], 0.);
            assert!(!fallback.cache_stats().unwrap().unwrap().nvme_available);
            assert_eq!(std::fs::read(unrelated.join("keep")).unwrap(), b"unrelated");
            drop(fallback);
            std::fs::remove_dir_all(&cache_path).unwrap();
        }
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config).unwrap();
        // The NVMe budget fits the largest block's charge but not two blocks.
        let one_block = db.root.runs[0]
            .blocks
            .iter()
            .map(|block| block.length.div_ceil(4096) * 4096 + 4096)
            .max()
            .unwrap();
        let db = db.with_block_cache(&cache_path, 0, one_block).unwrap();
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        assert_eq!(db.get(399).unwrap().unwrap().vector[0], 399.);
        assert_eq!(db.get(0).unwrap().unwrap().vector[0], 0.);
        let stats = db.cache_stats().unwrap().unwrap();
        assert_eq!(stats.remote_fetches, 3);
        assert!(stats.nvme_bytes <= one_block);
        assert_eq!(stats.ram_bytes, 0);
    }

    #[cfg(feature = "s3")]
    #[test]
    #[ignore = "requires disposable MinIO from tools/test_s3.py"]
    fn minio_segmented_publication_recovers_before_and_after_root_create() {
        use crate::store::s3::{AmazonS3Builder, S3Store};

        struct UncertainCreate<S> {
            inner: S,
            fail_prefix: &'static str,
            fired: std::cell::Cell<bool>,
            fail_remove_once: std::cell::Cell<bool>,
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
            fn create(&self, key: &str, value: &[u8]) -> Result<()> {
                self.inner.create(key, value)?;
                if !self.fired.get() && key.starts_with(self.fail_prefix) {
                    self.fired.set(true);
                    return Err(Error::Io(std::io::Error::other(
                        "simulated response loss after durable create",
                    )));
                }
                Ok(())
            }
            fn remove(&self, key: &str) -> Result<()> {
                self.inner.remove(key)?;
                if self.fail_remove_once.take() {
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
                fired: Default::default(),
                fail_remove_once: false.into(),
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
                    fired: Default::default(),
                    fail_remove_once: false.into(),
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
                    fired: Default::default(),
                    fail_remove_once: true.into(),
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
                        fired: Default::default(),
                        fail_remove_once: false.into(),
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
            let damaged = store();
            damaged.remove(&required_pack).unwrap();
            drop(damaged);
            assert!(matches!(
                SegmentedDatabase::open(store(), config),
                Err(Error::Corrupt(_))
            ));
        }

        let reclaim_namespace = format!("segmented-{}-reclaim", u64::from_le_bytes(random));
        let reclaim_store = || {
            S3Store::open(
                AmazonS3Builder::new()
                    .with_bucket_name(std::env::var("GLIDER_S3_BUCKET").unwrap())
                    .with_region("us-east-1")
                    .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap())
                    .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap())
                    .with_endpoint(std::env::var("GLIDER_S3_ENDPOINT").unwrap())
                    .with_allow_http(true),
                &reclaim_namespace,
            )
            .unwrap()
        };
        let vector_config = Config {
            dimensions: 128,
            metric: Metric::SquaredEuclidean,
        };
        let mut base = SegmentedDatabase::open(reclaim_store(), vector_config).unwrap();
        for batch in 0..4_u64 {
            base.apply_request(retry::Request {
                id: retry::RequestId {
                    boundary: batch,
                    nonce: [batch as u8; 16],
                },
                conditions: Vec::new(),
                mutations: (0..100)
                    .map(|offset| {
                        let id = batch * 100 + offset;
                        Mutation::Put {
                            id,
                            vector: noisy(0., id),
                            metadata: BTreeMap::new(),
                        }
                    })
                    .collect(),
            })
            .unwrap();
        }
        base.seal_delta().unwrap();
        let old_pack = base.root.runs[0].blocks[0].object.clone();
        drop(base);
        let mut uncertain_reclaim = SegmentedDatabase::open(
            UncertainCreate {
                inner: reclaim_store(),
                fail_prefix: "sgroot-00000000000000000002",
                fired: Default::default(),
                fail_remove_once: false.into(),
            },
            vector_config,
        )
        .unwrap();
        let overwritten: Vec<_> = (0..400_u64).filter(|id| id % 4 != 3).collect();
        for (batch, ids) in overwritten.chunks(100).enumerate() {
            uncertain_reclaim
                .apply_request(retry::Request {
                    id: retry::RequestId {
                        boundary: 4 + batch as u64,
                        nonce: [4 + batch as u8; 16],
                    },
                    conditions: Vec::new(),
                    mutations: ids
                        .iter()
                        .map(|&id| Mutation::Put {
                            id,
                            vector: noisy(1000., id),
                            metadata: BTreeMap::new(),
                        })
                        .collect(),
                })
                .unwrap();
        }
        assert!(matches!(
            uncertain_reclaim.reclaim_pack_step(),
            Err(Error::Io(_))
        ));
        drop(uncertain_reclaim);
        let mut recovered_reclaim =
            SegmentedDatabase::open(reclaim_store(), vector_config).unwrap();
        assert_eq!(recovered_reclaim.root.generation, 2);
        assert_eq!(recovered_reclaim.root.sequence, 4);
        assert_eq!(recovered_reclaim.get(0).unwrap().unwrap().vector[0], 1000.);
        assert_eq!(recovered_reclaim.get(3).unwrap().unwrap().vector[0], 3.);
        assert_eq!(
            recovered_reclaim
                .search_exact(&vec![3.; 128], 1, &[])
                .unwrap()[0]
                .id,
            3
        );
        while recovered_reclaim.cleanup_step(8).unwrap() != 0 {}
        assert!(!recovered_reclaim.known_keys.contains(&old_pack));
        let cache_dir = tempfile::tempdir().unwrap();
        let cached = recovered_reclaim
            .with_block_cache(cache_dir.path(), 2 * MAX_BLOCK_BYTES, 4 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(cached.get(3).unwrap().unwrap().vector[0], 3.);
        assert_eq!(cached.get(3).unwrap().unwrap().vector[0], 3.);
        assert_eq!(cached.cache_stats().unwrap().unwrap().remote_fetches, 1);
        assert_eq!(cached.cache_stats().unwrap().unwrap().ram_hits, 1);
        drop(cached);
        let warm = SegmentedDatabase::open(reclaim_store(), vector_config)
            .unwrap()
            .with_block_cache(cache_dir.path(), 2 * MAX_BLOCK_BYTES, 4 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(warm.get(3).unwrap().unwrap().vector[0], 3.);
        assert_eq!(warm.cache_stats().unwrap().unwrap().nvme_hits, 1);
        drop(warm);
        let disk = cache_dir.path().join("glider-block-cache-v1");
        let entry = std::fs::read_dir(&disk)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::write(entry, b"corrupt cache").unwrap();
        let corrupt = SegmentedDatabase::open(reclaim_store(), vector_config)
            .unwrap()
            .with_block_cache(cache_dir.path(), 2 * MAX_BLOCK_BYTES, 4 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(corrupt.get(3).unwrap().unwrap().vector[0], 3.);
        assert_eq!(corrupt.cache_stats().unwrap().unwrap().corrupt_entries, 1);
        assert_eq!(corrupt.cache_stats().unwrap().unwrap().remote_fetches, 1);
        drop(corrupt);
        std::fs::remove_dir_all(&disk).unwrap();
        let lost = SegmentedDatabase::open(reclaim_store(), vector_config)
            .unwrap()
            .with_block_cache(cache_dir.path(), 2 * MAX_BLOCK_BYTES, 4 * MAX_BLOCK_BYTES)
            .unwrap();
        assert_eq!(lost.get(3).unwrap().unwrap().vector[0], 3.);
        assert_eq!(lost.cache_stats().unwrap().unwrap().remote_fetches, 1);
        let selected_pack = lost.root.runs[0].blocks[0].object.clone();
        drop(lost);
        let damaged = reclaim_store();
        damaged.remove(&selected_pack).unwrap();
        drop(damaged);
        assert!(matches!(
            SegmentedDatabase::open(reclaim_store(), vector_config),
            Err(Error::Corrupt(_))
        ));

        for fail_prefix in ["sgindex-", "sgroot-00000000000000000002"] {
            let namespace = format!(
                "segmented-{}-prune-{fail_prefix}",
                u64::from_le_bytes(random)
            );
            let prune_store = || {
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
            let mut base = SegmentedDatabase::open(prune_store(), vector_config).unwrap();
            for batch in 0..4_u64 {
                base.apply_request(retry::Request {
                    id: retry::RequestId {
                        boundary: batch,
                        nonce: [batch as u8; 16],
                    },
                    conditions: Vec::new(),
                    mutations: (batch * 100..(batch + 1) * 100)
                        .map(|id| Mutation::Put {
                            id,
                            vector: noisy(0., id),
                            metadata: BTreeMap::new(),
                        })
                        .collect(),
                })
                .unwrap();
            }
            base.seal_delta().unwrap();
            let blocks = &base.root.runs[0].blocks;
            let dead_through = blocks[blocks.len() - 2].last_id;
            drop(base);
            let mut uncertain = SegmentedDatabase::open(
                UncertainCreate {
                    inner: prune_store(),
                    fail_prefix,
                    fired: Default::default(),
                    fail_remove_once: false.into(),
                },
                vector_config,
            )
            .unwrap();
            for (batch, chunk) in (0..=dead_through)
                .collect::<Vec<_>>()
                .chunks(100)
                .enumerate()
            {
                let boundary = 4 + batch as u64;
                uncertain
                    .apply_request(retry::Request {
                        id: retry::RequestId {
                            boundary,
                            nonce: [boundary as u8; 16],
                        },
                        conditions: Vec::new(),
                        mutations: chunk
                            .iter()
                            .map(|&id| Mutation::Put {
                                id,
                                vector: noisy(1000., id),
                                metadata: BTreeMap::new(),
                            })
                            .collect(),
                    })
                    .unwrap();
            }
            assert!(uncertain.start_prune().unwrap());
            // Manifest creates may precede the root create; step until the
            // injected failure is reached.
            let error = (0..16)
                .find_map(|_| uncertain.prune_step().err())
                .expect("the injected failure is reached");
            assert!(matches!(error, Error::Io(_)));
            assert!(matches!(
                uncertain.start_prune(),
                Err(Error::RecoveryRequired)
            ));
            drop(uncertain);
            let mut recovered = SegmentedDatabase::open(prune_store(), vector_config).unwrap();
            assert_eq!(recovered.get(0).unwrap().unwrap().vector[0], 1000.);
            assert_eq!(
                recovered.get(dead_through + 1).unwrap().unwrap().vector[0],
                (dead_through + 1) as f32
            );
            if fail_prefix == "sgindex-" {
                assert_eq!(recovered.root.generation, 1);
                assert!(recovered.start_prune().unwrap());
                while recovered.prune_step().unwrap() {}
            } else {
                assert_eq!(recovered.root.generation, 2);
            }
            assert_eq!(recovered.root.runs[0].blocks.len(), 1);
            while recovered.cleanup_step(8).unwrap() != 0 {}
        }
    }

    fn limit_request(nonce: u8, boundary: u64, mutations: Vec<Mutation>) -> retry::Request {
        retry::Request {
            id: retry::RequestId {
                boundary,
                nonce: [nonce; 16],
            },
            conditions: Vec::new(),
            mutations,
        }
    }

    fn limit_put(id: u64, value_len: usize) -> Mutation {
        Mutation::Put {
            id,
            vector: vec![1., 2.],
            metadata: BTreeMap::from([("blob".into(), "x".repeat(value_len))]),
        }
    }

    /// Value length at which a two-component put with one "blob" metadata
    /// entry exactly fills a block.
    fn exact_fit_value_len() -> usize {
        let empty =
            codec::put_row_len(&[1., 2.], &BTreeMap::from([("blob".into(), String::new())]));
        codec::MAX_RAW_BLOCK_BYTES - codec::block_len([]) - empty
    }

    fn limit_config() -> Config {
        Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        }
    }

    #[test]
    fn a_row_that_cannot_fit_one_block_is_rejected_before_acknowledgement() {
        let temp = tempfile::tempdir().unwrap();
        let mut db =
            SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), limit_config())
                .unwrap();
        let fits = exact_fit_value_len();
        // One byte over the seal planner's limit is refused with the limit named.
        let error = db
            .apply_request(limit_request(1, 0, vec![limit_put(1, fits + 1)]))
            .unwrap_err();
        let Error::Invalid(message) = &error else {
            panic!("expected Invalid, got {error:?}");
        };
        assert!(message.contains("document 1") && message.contains("fit in one block"));
        assert_eq!((db.sequence(), db.tail_objects), (0, 0));
        // An exact fit is acknowledged and seals.
        db.apply_request(limit_request(2, 0, vec![limit_put(2, fits)]))
            .unwrap();
        db.seal_delta().unwrap();
        assert_eq!(db.tail_objects, 0);
        assert_eq!(db.get(2).unwrap().unwrap().metadata["blob"].len(), fits);
        // One oversized put rejects its whole request: nothing is applied.
        let error = db
            .apply_request(limit_request(
                3,
                1,
                vec![limit_put(3, 10), limit_put(4, fits + 1)],
            ))
            .unwrap_err();
        assert!(matches!(error, Error::Invalid(_)));
        assert!(db.get(3).unwrap().is_none());
        assert_eq!(db.sequence(), 1);
        // Grouped requests are decided individually; the group still commits.
        let results = db.apply_requests(vec![
            limit_request(4, 1, vec![limit_put(5, 10)]),
            limit_request(5, 1, vec![limit_put(6, fits + 1)]),
            limit_request(6, 1, vec![limit_put(7, 10)]),
        ]);
        assert!(results[0].is_ok() && results[2].is_ok());
        assert!(matches!(results[1], Err(Error::Invalid(_))));
        assert_eq!(db.sequence(), 3);
        assert!(db.get(6).unwrap().is_none());
        assert!(db.get(5).unwrap().is_some() && db.get(7).unwrap().is_some());
    }

    /// The original wedge: acknowledging an unsealable row made every seal
    /// fail until the tail reached 64 objects and all writes returned 503.
    #[test]
    fn rejected_oversized_puts_cannot_wedge_sealing_or_writes() {
        let temp = tempfile::tempdir().unwrap();
        let mut db =
            SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), limit_config())
                .unwrap();
        let oversized = exact_fit_value_len() + 1;
        for round in 0..(MAX_TAIL_OBJECTS as u64 + 8) {
            let nonce = (round % 100) as u8 + 1;
            let boundary = db.sequence();
            assert!(
                matches!(
                    db.apply_request(limit_request(
                        nonce,
                        boundary,
                        vec![limit_put(1, oversized)]
                    )),
                    Err(Error::Invalid(_))
                ),
                "round {round}"
            );
            db.apply_request(limit_request(
                nonce + 100,
                boundary,
                vec![limit_put(100 + round, 8)],
            ))
            .unwrap_or_else(|error| panic!("round {round}: {error}"));
            if round % 4 == 3 {
                db.start_seal()
                    .unwrap_or_else(|error| panic!("round {round}: {error}"));
                while db.seal_step().unwrap() {}
            }
        }
        assert_eq!(db.tail_objects, 0);
        let boundary = db.sequence();
        db.apply_request(limit_request(
            250,
            boundary,
            vec![Mutation::Delete { id: 1 }],
        ))
        .unwrap();
        assert!(db.get(1).unwrap().is_none());
        assert_eq!(db.get(100).unwrap().unwrap().metadata["blob"].len(), 8);
    }

    /// A row an older binary acknowledged is still replayed on recovery. It
    /// blocks sealing until a delete or smaller replacement supersedes it in
    /// the tail; no acknowledged row is dropped silently.
    #[test]
    fn a_legacy_oversized_row_recovers_and_can_be_deleted_or_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        drop(SegmentedDatabase::open(LocalStore::open(&path).unwrap(), limit_config()).unwrap());
        let store = LocalStore::open(&path).unwrap();
        for (sequence, id) in [(1_u64, 1_u64), (2, 2)] {
            let request = limit_request(sequence as u8, sequence - 1, vec![limit_put(id, 300_000)]);
            let record = LogRecordV1 {
                version: 1,
                sequence,
                request,
                outcome: retry::Outcome {
                    sequence,
                    conflict: None,
                },
            };
            store
                .create(&log_key(sequence), &encode(&record).unwrap())
                .unwrap();
        }
        drop(store);
        let mut db =
            SegmentedDatabase::open(LocalStore::open(&path).unwrap(), limit_config()).unwrap();
        assert_eq!(db.get(1).unwrap().unwrap().metadata["blob"].len(), 300_000);
        let error = db.start_seal().unwrap_err();
        assert!(matches!(error, Error::Invalid(_)), "{error:?}");
        db.apply_request(limit_request(10, 2, vec![Mutation::Delete { id: 1 }]))
            .unwrap();
        db.apply_request(limit_request(11, 3, vec![limit_put(2, 8)]))
            .unwrap();
        db.seal_delta().unwrap();
        assert_eq!(db.tail_objects, 0);
        assert!(db.get(1).unwrap().is_none());
        assert_eq!(db.get(2).unwrap().unwrap().metadata["blob"].len(), 8);
    }
}
