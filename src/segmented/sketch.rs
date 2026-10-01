//! Persisted per-pack five-bit routing sketches for the experimental segmented
//! reader. Each immutable pack starts with a derived sketch frame bound to the
//! digests of its blocks; the root and logs remain authoritative.
use super::{
    authenticate, codec, consider, decode_block_bytes, Block, BlockRef, Ranked, Root,
    SegmentedDatabase,
};
use crate::{store::ObjectStore, Config, Error, Metric, Mutation, Neighbor, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

const BITS: usize = 5;
const LEVELS: f64 = 31.;
const MAGIC: &[u8; 8] = b"GLSKT001";

/// Blocks an unfiltered selective query reads. Routing ranks `blocks`
/// candidates. In rank order each candidate widens its pack's span, a single
/// byte range from the first to the last chosen block of that pack, if all
/// spans still total at most `bytes` and number at most `requests`. Every
/// live block inside a span is reranked, since its bytes are read anyway.
/// The choice never depends on cache contents, so each query issues at most
/// `requests` range GETs and downloads at most `bytes` payload bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadBudget {
    pub blocks: usize,
    pub requests: usize,
    pub bytes: usize,
}

impl ReadBudget {
    /// Spans for the first `blocks` routed blocks, without a byte limit.
    pub fn uniform(blocks: usize) -> Self {
        Self {
            blocks,
            requests: blocks,
            bytes: usize::MAX,
        }
    }
}

/// Pack spans `(object, offset, length, payload_len)` chosen in rank order.
fn choose<'a>(
    candidates: &[&'a BlockRef],
    budget: ReadBudget,
) -> Vec<(&'a str, usize, usize, usize)> {
    let mut spans: Vec<(&str, usize, usize, usize)> = Vec::new();
    let mut bytes = 0_usize;
    for reference in candidates {
        let (start, end) = (reference.offset, reference.offset + reference.length);
        match spans.iter().position(|span| span.0 == reference.object) {
            Some(index) => {
                let (_, old_start, old_end, _) = spans[index];
                let (new_start, new_end) = (old_start.min(start), old_end.max(end));
                let total = bytes - (old_end - old_start) + (new_end - new_start);
                if total <= budget.bytes {
                    spans[index].1 = new_start;
                    spans[index].2 = new_end;
                    bytes = total;
                }
            }
            None => {
                if spans.len() < budget.requests
                    && bytes
                        .checked_add(end - start)
                        .is_some_and(|t| t <= budget.bytes)
                {
                    spans.push((reference.object.as_str(), start, end, reference.payload_len));
                    bytes += end - start;
                }
            }
        }
    }
    spans
        .into_iter()
        .map(|(object, start, end, payload)| (object, start, end - start, payload))
        .collect()
}

/// Namespace-level derived-index declaration, persisted in segmented metadata
/// version 3. `resident_filter` keeps full-precision vectors for one equality
/// predicate inside every pack sketch, so that predicate is answered exactly
/// without block reads. Other filters are rejected by the selective reader.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentedOptions {
    pub resident_filter: Option<(String, String)>,
}

/// Packs begin with a sketch frame: this versioned magic, the sketch length
/// as u64 little-endian and the SHA-256 of the sketch bytes, then the sketch.
/// The digest authenticates the frame for range reads, which do not check
/// the whole-object envelope.
const FRAME_MAGIC: &[u8; 8] = b"GLPKSK01";
pub(super) const FRAME_HEADER: usize = 48;
/// Bytes read speculatively from the start of each pack when opening.
pub(super) const FRAME_PREFIX_READ: usize = 256 * 1024;
pub(super) const MAX_SKETCH_BYTES: usize = 1024 * 1024;

pub(super) fn frame(sketch: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(FRAME_HEADER + sketch.len());
    bytes.extend_from_slice(FRAME_MAGIC);
    bytes.extend_from_slice(&(sketch.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&Sha256::digest(sketch));
    bytes.extend_from_slice(sketch);
    bytes
}

/// Result of inspecting the leading bytes of a pack.
pub(super) enum Framed<'a> {
    /// A frame whose sketch digest matches.
    Sketch(&'a [u8]),
    /// A frame longer than the bytes read: read `0..len` again.
    Need(usize),
    /// No valid frame: rebuild the sketch from the pack's blocks.
    Invalid,
}

pub(super) fn unframe(prefix: &[u8], payload_len: usize) -> Framed<'_> {
    if prefix.len() < FRAME_HEADER || &prefix[..8] != FRAME_MAGIC {
        return Framed::Invalid;
    }
    let length = u64::from_le_bytes(prefix[8..16].try_into().unwrap());
    let Some(end) = usize::try_from(length)
        .ok()
        .filter(|&length| length <= MAX_SKETCH_BYTES)
        .map(|length| FRAME_HEADER + length)
        .filter(|&end| end <= payload_len)
    else {
        return Framed::Invalid;
    };
    if prefix.len() < end {
        return Framed::Need(end);
    }
    let sketch = &prefix[FRAME_HEADER..end];
    if Sha256::digest(sketch).as_slice() != &prefix[16..48] {
        return Framed::Invalid;
    }
    Framed::Sketch(sketch)
}

fn code_bytes(dimensions: usize) -> usize {
    (dimensions * BITS).div_ceil(8)
}

fn set_code(bytes: &mut [u8], axis: usize, value: u8) {
    let bit = axis * BITS;
    let byte = bit / 8;
    let shift = bit % 8;
    bytes[byte] |= value << shift;
    if shift > 3 {
        bytes[byte + 1] |= value >> (8 - shift);
    }
}

pub(super) fn digest_bytes(hex: &str) -> Result<[u8; 32]> {
    let mut digest = [0; 32];
    if hex.len() != 64 {
        return Err(Error::Corrupt("invalid segmented digest".into()));
    }
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| Error::Corrupt("invalid segmented digest".into()))?;
    }
    Ok(digest)
}

/// Approximate distance from a query lookup table to one packed row. Codes are
/// little-endian bit fields, so each five-byte group holds eight codes.
#[cfg(test)]
fn approximate(codes: &[u8], table: &[[f64; 32]]) -> f64 {
    let mut sum = 0.;
    let mut axis = 0;
    for group in codes.chunks(BITS) {
        let mut word = 0_u64;
        for (index, &byte) in group.iter().enumerate() {
            word |= u64::from(byte) << (8 * index);
        }
        for slot in 0..8 {
            let Some(row) = table.get(axis) else {
                return sum;
            };
            sum += row[((word >> (BITS * slot)) & 31) as usize];
            axis += 1;
        }
    }
    sum
}

/// Like `approximate`, but returns `None` once a prefix sum exceeds `limit`.
fn approximate_within(codes: &[u8], table: &[[f64; 32]], limit: f64) -> Option<f64> {
    let mut sum = 0.;
    let mut axis = 0;
    for (group_index, group) in codes.chunks(BITS).enumerate() {
        let mut word = 0_u64;
        for (index, &byte) in group.iter().enumerate() {
            word |= u64::from(byte) << (8 * index);
        }
        for slot in 0..8 {
            let Some(row) = table.get(axis) else {
                return Some(sum);
            };
            sum += row[((word >> (BITS * slot)) & 31) as usize];
            axis += 1;
        }
        if group_index % 2 == 1 && sum > limit {
            return None;
        }
    }
    (sum <= limit).then_some(sum)
}

/// Row IDs, stored as u32 offsets from the smallest ID when the pack's ID
/// span allows it.
enum Ids {
    Narrow { base: u64, offsets: Vec<u32> },
    Wide(Vec<u64>),
}

impl Ids {
    fn new(ids: Vec<u64>) -> Self {
        let base = ids.iter().copied().min().unwrap_or(0);
        if ids.iter().all(|&id| id - base <= u64::from(u32::MAX)) {
            Self::Narrow {
                base,
                offsets: ids.iter().map(|&id| (id - base) as u32).collect(),
            }
        } else {
            Self::Wide(ids)
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Narrow { offsets, .. } => offsets.len(),
            Self::Wide(ids) => ids.len(),
        }
    }

    fn get(&self, row: usize) -> u64 {
        match self {
            Self::Narrow { base, offsets } => base + u64::from(offsets[row]),
            Self::Wide(ids) => ids[row],
        }
    }

    /// Row of `id` within `start..end`, whose IDs are strictly increasing.
    fn find(&self, start: usize, end: usize, id: u64) -> Option<usize> {
        match self {
            Self::Narrow { base, offsets } => {
                let offset = u32::try_from(id.checked_sub(*base)?).ok()?;
                offsets[start..end].binary_search(&offset).ok()
            }
            Self::Wide(ids) => ids[start..end].binary_search(&id).ok(),
        }
        .map(|index| start + index)
    }

    fn copy(&mut self, from: usize, to: usize) {
        match self {
            Self::Narrow { offsets, .. } => offsets[to] = offsets[from],
            Self::Wide(ids) => ids[to] = ids[from],
        }
    }

    fn truncate(&mut self, length: usize) {
        match self {
            Self::Narrow { offsets, .. } => {
                offsets.truncate(length);
                offsets.shrink_to_fit();
            }
            Self::Wide(ids) => {
                ids.truncate(length);
                ids.shrink_to_fit();
            }
        }
    }

    fn charged_bytes(&self) -> usize {
        match self {
            Self::Narrow { offsets, .. } => offsets.capacity() * size_of::<u32>(),
            Self::Wide(ids) => ids.capacity() * size_of::<u64>(),
        }
    }
}

struct SketchBlock {
    digest: [u8; 32],
    start: usize,
    end: usize,
    /// Current root location, recomputed on every root change.
    root: Option<(usize, usize)>,
}

pub(super) struct PackSketch {
    pack: String,
    blocks: Vec<SketchBlock>,
    minima: Vec<f32>,
    scales: Vec<f32>,
    ids: Ids,
    codes: Vec<u8>,
    live: Vec<u64>,
    resident_rows: Vec<u32>,
    resident_vectors: Vec<f32>,
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    if bytes.len() < length {
        return Err(Error::Corrupt("truncated segmented sketch".into()));
    }
    let (head, rest) = bytes.split_at(length);
    *bytes = rest;
    Ok(head)
}

fn take_u32(bytes: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()))
}

fn take_f32s(bytes: &mut &[u8], count: usize) -> Result<Vec<f32>> {
    let raw = take(
        bytes,
        count
            .checked_mul(4)
            .ok_or_else(|| Error::Corrupt("segmented sketch size overflow".into()))?,
    )?;
    let values: Vec<_> = raw
        .as_chunks::<4>()
        .0
        .iter()
        .map(|part| f32::from_le_bytes(*part))
        .collect();
    if values.iter().any(|value| !value.is_finite()) {
        return Err(Error::Corrupt("non-finite segmented sketch value".into()));
    }
    Ok(values)
}

impl PackSketch {
    /// Build from the authenticated blocks of one pack, in pack order.
    pub(super) fn build(
        config: Config,
        options: &SegmentedOptions,
        pack: &str,
        blocks: &[(&BlockRef, &Block)],
    ) -> Result<Self> {
        let dimensions = config.dimensions;
        let mut minima = vec![f64::INFINITY; dimensions];
        let mut maxima = vec![f64::NEG_INFINITY; dimensions];
        let puts = || {
            blocks.iter().flat_map(|(_, block)| {
                block
                    .records
                    .iter()
                    .filter_map(|record| match &record.mutation {
                        Mutation::Put {
                            id,
                            vector,
                            metadata,
                        } => Some((*id, vector, metadata)),
                        Mutation::Delete { .. } => None,
                    })
            })
        };
        for (_, vector, _) in puts() {
            for (axis, &value) in vector.iter().enumerate() {
                minima[axis] = minima[axis].min(f64::from(value));
                maxima[axis] = maxima[axis].max(f64::from(value));
            }
        }
        let mut scales = Vec::with_capacity(dimensions);
        for (minimum, maximum) in minima.iter_mut().zip(maxima) {
            if !minimum.is_finite() {
                *minimum = 0.;
                scales.push(1.);
                continue;
            }
            let span = (maximum - *minimum) / LEVELS;
            scales.push(if span > 0. { span } else { 1. });
        }
        let minima: Vec<f32> = minima.into_iter().map(|value| value as f32).collect();
        let scales: Vec<f32> = scales.into_iter().map(|value| value as f32).collect();
        let width = code_bytes(dimensions);
        let mut sketch = Self {
            pack: pack.to_owned(),
            blocks: Vec::with_capacity(blocks.len()),
            minima,
            scales,
            ids: Ids::Wide(Vec::new()),
            codes: Vec::new(),
            live: Vec::new(),
            resident_rows: Vec::new(),
            resident_vectors: Vec::new(),
        };
        let mut ids = Vec::new();
        for (reference, block) in blocks {
            let start = ids.len();
            for record in &block.records {
                let Mutation::Put {
                    id,
                    vector,
                    metadata,
                } = &record.mutation
                else {
                    continue;
                };
                let row = ids.len();
                ids.push(*id);
                sketch.codes.resize(sketch.codes.len() + width, 0);
                let bytes = &mut sketch.codes[row * width..];
                for (axis, &value) in vector.iter().enumerate() {
                    let code = ((f64::from(value) - f64::from(sketch.minima[axis]))
                        / f64::from(sketch.scales[axis]))
                    .round_ties_even()
                    .clamp(0., LEVELS) as u8;
                    set_code(bytes, axis, code);
                }
                if options
                    .resident_filter
                    .as_ref()
                    .is_some_and(|(key, value)| metadata.get(key) == Some(value))
                {
                    sketch.resident_rows.push(
                        u32::try_from(row)
                            .map_err(|_| Error::Invalid("too many sketch rows".into()))?,
                    );
                    sketch.resident_vectors.extend_from_slice(vector);
                }
            }
            sketch.blocks.push(SketchBlock {
                digest: digest_bytes(&reference.sha256)?,
                start,
                end: ids.len(),
                root: None,
            });
        }
        sketch.live = vec![0; ids.len().div_ceil(64)];
        sketch.ids = Ids::new(ids);
        Ok(sketch)
    }

    /// Header: magic, dimensions:u32, metric:u8, bits:u8, two zero bytes,
    /// options digest:[u8;32], block/row/resident counts and pack-key length
    /// as u32, the pack key, then per block digest:[u8;32] and rows:u32,
    /// minima/scales f32, ids u64, packed codes, resident rows u32 and vectors.
    pub(super) fn encode(&self, config: Config, options_digest: &[u8; 32]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(config.dimensions as u32).to_le_bytes());
        bytes.push(match config.metric {
            Metric::SquaredEuclidean => 0,
            Metric::Manhattan => 1,
            Metric::Cosine => 2,
        });
        bytes.push(BITS as u8);
        bytes.extend_from_slice(&[0; 2]);
        bytes.extend_from_slice(options_digest);
        for count in [
            self.blocks.len(),
            self.ids.len(),
            self.resident_rows.len(),
            self.pack.len(),
        ] {
            bytes.extend_from_slice(&(count as u32).to_le_bytes());
        }
        bytes.extend_from_slice(self.pack.as_bytes());
        for block in &self.blocks {
            bytes.extend_from_slice(&block.digest);
            bytes.extend_from_slice(&((block.end - block.start) as u32).to_le_bytes());
        }
        for value in self.minima.iter().chain(&self.scales) {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        for row in 0..self.ids.len() {
            bytes.extend_from_slice(&self.ids.get(row).to_le_bytes());
        }
        bytes.extend_from_slice(&self.codes);
        for row in &self.resident_rows {
            bytes.extend_from_slice(&row.to_le_bytes());
        }
        for value in &self.resident_vectors {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    pub(super) fn decode(
        mut bytes: &[u8],
        config: Config,
        options_digest: &[u8; 32],
        pack: &str,
    ) -> Result<Self> {
        let invalid = || Error::Corrupt(format!("invalid segmented sketch for {pack}"));
        let input = &mut bytes;
        if take(input, 8)? != MAGIC
            || take_u32(input)? as usize != config.dimensions
            || take(input, 4)?
                != [
                    match config.metric {
                        Metric::SquaredEuclidean => 0,
                        Metric::Manhattan => 1,
                        Metric::Cosine => 2,
                    },
                    BITS as u8,
                    0,
                    0,
                ]
            || take(input, 32)? != options_digest
        {
            return Err(invalid());
        }
        let blocks = take_u32(input)? as usize;
        let rows = take_u32(input)? as usize;
        let resident = take_u32(input)? as usize;
        let key_len = take_u32(input)? as usize;
        if take(input, key_len)? != pack.as_bytes() || blocks == 0 || resident > rows {
            return Err(invalid());
        }
        let mut sketch_blocks = Vec::with_capacity(blocks.min(1024));
        let mut start = 0;
        for _ in 0..blocks {
            let digest = take(input, 32)?.try_into().unwrap();
            let end = start + take_u32(input)? as usize;
            sketch_blocks.push(SketchBlock {
                digest,
                start,
                end,
                root: None,
            });
            start = end;
        }
        if start != rows {
            return Err(invalid());
        }
        let minima = take_f32s(input, config.dimensions)?;
        let scales = take_f32s(input, config.dimensions)?;
        if scales.iter().any(|&scale| scale <= 0.) {
            return Err(invalid());
        }
        let ids: Vec<_> = take(input, rows.checked_mul(8).ok_or_else(invalid)?)?
            .as_chunks::<8>()
            .0
            .iter()
            .map(|part| u64::from_le_bytes(*part))
            .collect();
        for block in &sketch_blocks {
            if ids[block.start..block.end]
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            {
                return Err(invalid());
            }
        }
        let codes = take(
            input,
            rows.checked_mul(code_bytes(config.dimensions))
                .ok_or_else(invalid)?,
        )?
        .to_vec();
        let resident_rows: Vec<_> = take(input, resident * 4)?
            .as_chunks::<4>()
            .0
            .iter()
            .map(|part| u32::from_le_bytes(*part))
            .collect();
        if resident_rows.windows(2).any(|pair| pair[0] >= pair[1])
            || resident_rows
                .last()
                .is_some_and(|&row| row as usize >= rows)
        {
            return Err(invalid());
        }
        let resident_vectors = take_f32s(
            input,
            resident
                .checked_mul(config.dimensions)
                .ok_or_else(invalid)?,
        )?;
        if !input.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            pack: pack.to_owned(),
            blocks: sketch_blocks,
            minima,
            scales,
            live: vec![0; rows.div_ceil(64)],
            ids: Ids::new(ids),
            codes,
            resident_rows,
            resident_vectors,
        })
    }

    pub(super) fn pack(&self) -> &str {
        &self.pack
    }

    fn charged_bytes(&self) -> usize {
        size_of::<Self>()
            + self.pack.capacity()
            + self.blocks.capacity() * size_of::<SketchBlock>()
            + (self.minima.capacity() + self.scales.capacity()) * size_of::<f32>()
            + self.ids.charged_bytes()
            + self.codes.capacity()
            + self.live.capacity() * size_of::<u64>()
            + self.resident_rows.capacity() * size_of::<u32>()
            + self.resident_vectors.capacity() * size_of::<f32>()
    }

    fn dead_rows(&self) -> usize {
        self.ids.len()
            - self
                .live
                .iter()
                .map(|word| word.count_ones() as usize)
                .sum::<usize>()
    }

    /// Drop rows whose bit is clear. A cleared row is shadowed by an
    /// acknowledged newer mutation and never becomes live again; reopening
    /// reloads the persisted sketch and recomputes liveness.
    fn compact(&mut self) {
        let width = self.codes.len() / self.ids.len().max(1);
        let dimensions = self.minima.len();
        // Rows move only toward the front, so compaction is in place and the
        // shrink below needs no second full-size buffer.
        let (mut kept, mut kept_resident, mut resident_index) = (0, 0, 0);
        for block in &mut self.blocks {
            let start = kept;
            for row in block.start..block.end {
                let resident = self
                    .resident_rows
                    .get(resident_index)
                    .is_some_and(|&at| at as usize == row);
                if resident {
                    resident_index += 1;
                }
                if self.live[row / 64] & (1 << (row % 64)) == 0 {
                    continue;
                }
                if resident {
                    self.resident_rows[kept_resident] = kept as u32;
                    self.resident_vectors.copy_within(
                        (resident_index - 1) * dimensions..resident_index * dimensions,
                        kept_resident * dimensions,
                    );
                    kept_resident += 1;
                }
                self.ids.copy(row, kept);
                self.codes
                    .copy_within(row * width..(row + 1) * width, kept * width);
                kept += 1;
            }
            block.start = start;
            block.end = kept;
        }
        self.ids.truncate(kept);
        self.codes.truncate(kept * width);
        self.codes.shrink_to_fit();
        self.resident_rows.truncate(kept_resident);
        self.resident_rows.shrink_to_fit();
        self.resident_vectors.truncate(kept_resident * dimensions);
        self.resident_vectors.shrink_to_fit();
        self.live = vec![u64::MAX; kept.div_ceil(64)];
        if !kept.is_multiple_of(64) {
            *self.live.last_mut().unwrap() = (1 << (kept % 64)) - 1;
        }
    }

    fn is_live(&self, row: usize) -> bool {
        self.live[row / 64] & (1 << (row % 64)) != 0
    }

    fn set_live(&mut self, row: usize, live: bool) {
        if live {
            self.live[row / 64] |= 1 << (row % 64);
        } else {
            self.live[row / 64] &= !(1 << (row % 64));
        }
    }
}

/// Loaded sketches for every pack referenced by the selected root. A row is
/// live when it is the current committed version of its ID and no acknowledged
/// log-tail mutation shadows it.
#[derive(Default)]
pub(super) struct SketchSet {
    packs: Vec<PackSketch>,
    /// Root `(run, block)` to `(pack slot, block within pack)`.
    locations: Vec<Vec<(usize, usize)>>,
    pub(super) rebuilt: usize,
}

impl SketchSet {
    pub(super) fn charged_bytes(&self) -> usize {
        self.packs
            .iter()
            .map(PackSketch::charged_bytes)
            .sum::<usize>()
            + self
                .locations
                .iter()
                .map(|run| run.capacity() * size_of::<(usize, usize)>())
                .sum::<usize>()
    }

    /// Compact every pack with a dead row.
    pub(super) fn compact_all(&mut self) {
        for sketch in &mut self.packs {
            if sketch.dead_rows() > 0 {
                sketch.compact();
            }
        }
    }

    /// Compact the pack with the most dead rows if it has at least
    /// `min_dead`. Returns whether a pack was compacted.
    pub(super) fn compact_step(&mut self, min_dead: usize) -> bool {
        let Some(sketch) = self
            .packs
            .iter_mut()
            .max_by_key(|sketch| sketch.dead_rows())
            .filter(|sketch| sketch.dead_rows() >= min_dead.max(1))
        else {
            return false;
        };
        sketch.compact();
        true
    }

    pub(super) fn install(&mut self, sketch: PackSketch) {
        self.packs.retain(|existing| existing.pack != sketch.pack);
        self.packs.push(sketch);
    }

    /// Bind loaded sketches to the root. Every referenced block must have a
    /// sketch block with the same digest; unreferenced packs are dropped.
    pub(super) fn refresh(&mut self, root: &Root) -> Result<()> {
        let referenced: BTreeSet<&str> = root
            .runs
            .iter()
            .flat_map(|run| run.blocks.iter().map(|block| block.object.as_str()))
            .collect();
        self.packs
            .retain(|sketch| referenced.contains(sketch.pack.as_str()));
        let slots: BTreeMap<&str, usize> = self
            .packs
            .iter()
            .enumerate()
            .map(|(slot, sketch)| (sketch.pack.as_str(), slot))
            .collect();
        let mut locations = Vec::with_capacity(root.runs.len());
        let mut bound = Vec::new();
        for (run, run_ref) in root.runs.iter().enumerate() {
            let mut blocks = Vec::with_capacity(run_ref.blocks.len());
            for (block, reference) in run_ref.blocks.iter().enumerate() {
                let slot = *slots.get(reference.object.as_str()).ok_or_else(|| {
                    Error::Corrupt(format!("segmented sketch missing: {}", reference.object))
                })?;
                let digest = digest_bytes(&reference.sha256)?;
                let sketch_block = self.packs[slot]
                    .blocks
                    .iter()
                    .position(|candidate| candidate.digest == digest)
                    .filter(|&index| {
                        let rows = &self.packs[slot].blocks[index];
                        rows.end - rows.start <= reference.rows
                    })
                    .ok_or_else(|| Error::Corrupt("segmented sketch block mismatch".into()))?;
                bound.push((slot, sketch_block, run, block));
                blocks.push((slot, sketch_block));
            }
            locations.push(blocks);
        }
        for sketch in &mut self.packs {
            for block in &mut sketch.blocks {
                block.root = None;
            }
        }
        for (slot, sketch_block, run, block) in bound {
            if self.packs[slot].blocks[sketch_block]
                .root
                .replace((run, block))
                .is_some()
            {
                return Err(Error::Corrupt(
                    "segmented block referenced more than once".into(),
                ));
            }
        }
        self.locations = locations;
        Ok(())
    }

    /// Set or clear the row for an ID stored in root block `(run, block)`.
    /// Tombstones have no sketch row.
    pub(super) fn mark(&mut self, run: usize, block: usize, id: u64, live: bool) {
        let Some(&(slot, sketch_block)) = self.locations.get(run).and_then(|r| r.get(block)) else {
            return;
        };
        let sketch = &mut self.packs[slot];
        let range = &sketch.blocks[sketch_block];
        if let Some(row) = sketch.ids.find(range.start, range.end, id) {
            sketch.set_live(row, live);
        }
    }

    /// Recompute liveness for the named packs, or all packs when `None`.
    pub(super) fn activate(
        &mut self,
        packs: Option<&BTreeSet<String>>,
        mut current: impl FnMut(u64, usize, usize) -> bool,
    ) {
        for sketch in &mut self.packs {
            if packs.is_some_and(|names| !names.contains(&sketch.pack)) {
                continue;
            }
            for index in 0..sketch.blocks.len() {
                let (start, end, root) = {
                    let block = &sketch.blocks[index];
                    (block.start, block.end, block.root)
                };
                for row in start..end {
                    let live =
                        root.is_some_and(|(run, block)| current(sketch.ids.get(row), run, block));
                    sketch.set_live(row, live);
                }
            }
        }
    }
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    /// Charged bytes of the loaded sketches, including resident vectors.
    pub fn selective_index_bytes(&self) -> usize {
        self.sketches.charged_bytes()
    }

    /// Score sketches on up to `threads` scoped threads per unfiltered
    /// selective query. Results do not depend on the thread count.
    pub fn with_query_threads(mut self, threads: usize) -> Self {
        self.query_threads = threads.max(1);
        self
    }

    /// Drop shadowed rows from the in-memory sketch of the pack with the most
    /// such rows, if it has at least `min_dead`. Performs no storage I/O.
    pub fn compact_sketch(&mut self, min_dead: usize) -> bool {
        self.sketches.compact_step(min_dead)
    }

    /// Pack sketches rebuilt from authoritative blocks because the derived
    /// object was missing or invalid when this handle loaded it.
    pub fn sketch_rebuilds(&self) -> usize {
        self.sketches.rebuilt
    }

    /// Approximate unfiltered search, or exact search of the declared resident
    /// predicate. Unfiltered queries score every live packed code, read at most
    /// `max_blocks` selected blocks and rerank their current records plus the
    /// acknowledged log tail exactly. The resident predicate reads no blocks.
    /// Any other filter is rejected; use `search_exact` for it.
    pub fn search_selective(
        &self,
        query: &[f32],
        k: usize,
        max_blocks: usize,
        filter: &[(&str, &str)],
    ) -> Result<Vec<Neighbor>> {
        self.search_selective_within(query, k, ReadBudget::uniform(max_blocks), filter)
    }

    /// `search_selective` with separate budgets for routed blocks and remote
    /// fetches. Blocks are considered in routing order; a cached block is
    /// always read, an uncached one only while the remote fetch and byte
    /// budgets allow, otherwise it is skipped.
    pub fn search_selective_within(
        &self,
        query: &[f32],
        k: usize,
        budget: ReadBudget,
        filter: &[(&str, &str)],
    ) -> Result<Vec<Neighbor>> {
        let query = self.config.query(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let mut heap = BinaryHeap::new();
        match filter {
            [] => self.route_and_rerank(&query, k, budget, &mut heap)?,
            [(key, value)]
                if self
                    .options
                    .resident_filter
                    .as_ref()
                    .is_some_and(|(k, v)| k == key && v == value) =>
            {
                let dimensions = self.config.dimensions;
                for sketch in &self.sketches.packs {
                    for (index, &row) in sketch.resident_rows.iter().enumerate() {
                        if sketch.is_live(row as usize) {
                            let vector = &sketch.resident_vectors
                                [index * dimensions..(index + 1) * dimensions];
                            consider(
                                &mut heap,
                                k,
                                self.config,
                                &query,
                                sketch.ids.get(row as usize),
                                vector,
                            );
                        }
                    }
                }
            }
            _ => {
                return Err(Error::Invalid(
                    "selective search supports no filter or the declared resident filter".into(),
                ))
            }
        }
        for (&id, (_, document)) in &self.tail {
            if let Some(document) = document {
                if crate::matches_filter(&document.metadata, filter) {
                    consider(&mut heap, k, self.config, &query, id, &document.vector);
                }
            }
        }
        let mut results: Vec<_> = heap.into_iter().map(|ranked: Ranked| ranked.0).collect();
        results.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        Ok(results)
    }

    fn route_and_rerank(
        &self,
        query: &[f32],
        k: usize,
        budget: ReadBudget,
        heap: &mut BinaryHeap<Ranked>,
    ) -> Result<()> {
        if budget.blocks == 0 || budget.requests == 0 {
            return Err(Error::Invalid(
                "selective search needs a block budget".into(),
            ));
        }
        let ranked = self.route(query, budget.blocks);
        self.rerank(query, k, &ranked, budget, heap)
    }

    /// The `max_blocks` rooted blocks with the smallest minimum approximate
    /// live-row distance, ordered by (distance, pack slot, block).
    fn route(&self, query: &[f32], max_blocks: usize) -> Vec<(f64, usize, usize)> {
        let packs = &self.sketches.packs;
        let (config, query) = (self.config, query);
        let level = |minimum: f32, scale: f32, axis: usize, code: usize| {
            let difference =
                f64::from(minimum) + f64::from(scale) * code as f64 - f64::from(query[axis]);
            match config.metric {
                Metric::SquaredEuclidean | Metric::Cosine => difference * difference,
                Metric::Manhattan => difference.abs(),
            }
        };
        // A pack's lower bound sums, per axis, the nearest representable
        // level. Visiting packs by bound tightens the pruning threshold early.
        let mut order: Vec<(f64, usize)> = packs
            .iter()
            .enumerate()
            .map(|(slot, sketch)| {
                let bound = (0..config.dimensions)
                    .map(|axis| {
                        let (minimum, scale) = (sketch.minima[axis], sketch.scales[axis]);
                        let nearest = ((f64::from(query[axis]) - f64::from(minimum))
                            / f64::from(scale))
                        .round()
                        .clamp(0., LEVELS);
                        let code = nearest as usize;
                        [code.saturating_sub(1), code, (code + 1).min(31)]
                            .into_iter()
                            .map(|code| level(minimum, scale, axis, code))
                            .fold(f64::INFINITY, f64::min)
                    })
                    .sum::<f64>();
                (bound, slot)
            })
            .collect();
        order.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        let threads = self.query_threads.clamp(1, packs.len().max(1));
        let order_by = |a: &(f64, usize, usize), b: &(f64, usize, usize)| {
            a.0.total_cmp(&b.0).then((a.1, a.2).cmp(&(b.1, b.2)))
        };
        // Exact top blocks over this thread's packs. A row is abandoned once
        // its partial sum exceeds both its block's best and the current last
        // kept block; sums of nonnegative terms never decrease, so kept block
        // scores equal those of a full scan and the selection is identical.
        let score = |thread: usize| {
            let width = code_bytes(config.dimensions);
            let mut table = vec![[0_f64; 32]; config.dimensions];
            let mut top: Vec<(f64, usize, usize)> = Vec::with_capacity(max_blocks + 1);
            for &(bound, slot) in order.iter().skip(thread).step_by(threads) {
                let threshold = |top: &Vec<(f64, usize, usize)>| {
                    if top.len() < max_blocks {
                        f64::INFINITY
                    } else {
                        top[max_blocks - 1].0
                    }
                };
                if bound > threshold(&top) {
                    continue;
                }
                let sketch = &packs[slot];
                for (axis, row) in table.iter_mut().enumerate() {
                    for (code, distance) in row.iter_mut().enumerate() {
                        *distance = level(sketch.minima[axis], sketch.scales[axis], axis, code);
                    }
                }
                for (index, block) in sketch.blocks.iter().enumerate() {
                    if block.root.is_none() {
                        continue;
                    }
                    let limit = threshold(&top);
                    let mut best = f64::INFINITY;
                    for row in block.start..block.end {
                        if sketch.is_live(row) {
                            if let Some(distance) = approximate_within(
                                &sketch.codes[row * width..(row + 1) * width],
                                &table,
                                best.min(limit),
                            ) {
                                best = best.min(distance);
                            }
                        }
                    }
                    if best.is_finite() {
                        let candidate = (best, slot, index);
                        let at = top.partition_point(|kept| order_by(kept, &candidate).is_lt());
                        if at < max_blocks {
                            top.insert(at, candidate);
                            top.truncate(max_blocks);
                        }
                    }
                }
            }
            top
        };
        let mut ranked = if threads == 1 {
            score(0)
        } else {
            std::thread::scope(|scope| {
                let workers: Vec<_> = (0..threads)
                    .map(|thread| scope.spawn(move || score(thread)))
                    .collect();
                workers
                    .into_iter()
                    .flat_map(|worker| worker.join().expect("routing worker panicked"))
                    .collect::<Vec<_>>()
            })
        };
        ranked.sort_unstable_by(order_by);
        ranked.truncate(max_blocks);
        ranked
    }

    fn rerank(
        &self,
        query: &[f32],
        k: usize,
        ranked: &[(f64, usize, usize)],
        budget: ReadBudget,
        heap: &mut BinaryHeap<Ranked>,
    ) -> Result<()> {
        let candidates: Vec<_> = ranked
            .iter()
            .map(|&(_, slot, index)| {
                let (run, ordinal) = self.sketches.packs[slot].blocks[index]
                    .root
                    .expect("routed blocks are rooted");
                &self.root.runs[run].blocks[ordinal]
            })
            .collect();
        let ranges = choose(&candidates, budget);
        // Every rooted block inside a chosen span, in span and offset order.
        let mut targets = Vec::new();
        let mut references = Vec::new();
        for &(object, offset, length, _) in &ranges {
            let slot = ranked
                .iter()
                .zip(&candidates)
                .find(|(_, reference)| reference.object == object)
                .map(|(&(_, slot, _), _)| slot)
                .expect("each span comes from a candidate");
            let sketch = &self.sketches.packs[slot];
            let mut inside: Vec<_> = sketch
                .blocks
                .iter()
                .filter_map(|block| {
                    let (run, ordinal) = block.root?;
                    let reference = &self.root.runs[run].blocks[ordinal];
                    (reference.offset >= offset
                        && reference.offset + reference.length <= offset + length)
                        .then(|| {
                            let live = (block.start..block.end)
                                .filter(|&row| sketch.is_live(row))
                                .count();
                            (reference.offset, (run, ordinal, live), reference)
                        })
                })
                .collect();
            inside.sort_by_key(|&(at, _, _)| at);
            for (_, target, reference) in inside {
                targets.push(target);
                references.push(reference);
            }
        }
        let fetched = self.fetch_blocks(&references, &ranges)?;
        let (config, latest, tail) = (self.config, &self.latest, &self.tail);
        // Current records of one authenticated block, as a local top-k.
        let scan = |index: usize, block: Block| -> Result<Vec<Ranked>> {
            let (run, ordinal, live) = targets[index];
            let mut local = BinaryHeap::new();
            let mut seen = 0;
            for record in block.records {
                let id = record.id();
                if tail.contains_key(&id) {
                    continue;
                }
                let Some(location) = latest.get(&id) else {
                    continue;
                };
                if location.run != run
                    || location.entry.block as usize != ordinal
                    || location.entry.sequence != record.sequence
                {
                    continue;
                }
                match record.mutation {
                    Mutation::Put { vector, .. } if !location.entry.deleted => {
                        seen += 1;
                        consider(&mut local, k, config, query, id, &vector);
                    }
                    Mutation::Delete { .. } if location.entry.deleted => {}
                    _ => {
                        return Err(Error::Corrupt(
                            "selective block disagrees with latest-ID directory".into(),
                        ))
                    }
                }
            }
            if seen != live {
                return Err(Error::Corrupt(
                    "segmented sketch disagrees with selected block".into(),
                ));
            }
            Ok(local.into_vec())
        };
        // Version 2 blocks stream records from a reused buffer; version 1
        // blocks decode to a `Block` first.
        let stream = |index: usize, bytes: &[u8]| -> Result<Vec<Ranked>> {
            let (run, ordinal, live) = targets[index];
            let reference = references[index];
            let mut local = BinaryHeap::new();
            let (mut seen, mut first, mut last) = (0, None, 0);
            let mut vector = Vec::with_capacity(config.dimensions);
            let (partition, count) = codec::visit(config, bytes, |view| {
                first.get_or_insert(view.id);
                last = view.id;
                if tail.contains_key(&view.id) {
                    return Ok(());
                }
                let Some(location) = latest.get(&view.id) else {
                    return Ok(());
                };
                if location.run != run
                    || location.entry.block as usize != ordinal
                    || location.entry.sequence != view.sequence
                {
                    return Ok(());
                }
                match view.put {
                    Some((components, _, _)) if !location.entry.deleted => {
                        seen += 1;
                        codec::components(components, &mut vector);
                        consider(&mut local, k, config, query, view.id, &vector);
                    }
                    None if location.entry.deleted => {}
                    _ => {
                        return Err(Error::Corrupt(
                            "selective block disagrees with latest-ID directory".into(),
                        ))
                    }
                }
                Ok(())
            })?;
            if partition != reference.partition
                || count != reference.rows
                || first != Some(reference.first_id)
                || last != reference.last_id
            {
                return Err(Error::Corrupt("segmented block reference mismatch".into()));
            }
            if seen != live {
                return Err(Error::Corrupt(
                    "segmented sketch disagrees with selected block".into(),
                ));
            }
            Ok(local.into_vec())
        };
        // Outer error: bytes failed authentication. Inner error: fatal.
        let work = |index: usize| -> std::result::Result<Result<Vec<Ranked>>, Error> {
            let bytes = &fetched[index].0;
            authenticate(references[index], bytes)?;
            Ok(if codec::is_v2(bytes) {
                stream(index, bytes)
            } else {
                decode_block_bytes(config, references[index], bytes)
                    .and_then(|block| scan(index, block))
            })
        };
        let threads = self.query_threads.clamp(1, targets.len().max(1));
        let outcomes: Vec<_> = if threads == 1 {
            (0..targets.len()).map(work).collect()
        } else {
            let indexes: Vec<_> = (0..targets.len()).collect();
            let chunk = targets.len().div_ceil(threads);
            std::thread::scope(|scope| {
                let workers: Vec<_> = indexes
                    .chunks(chunk)
                    .map(|part| {
                        scope.spawn(move || part.iter().map(|&i| work(i)).collect::<Vec<_>>())
                    })
                    .collect();
                workers
                    .into_iter()
                    .flat_map(|worker| worker.join().expect("rerank worker panicked"))
                    .collect()
            })
        };
        let mut cache = self.lock_cache()?;
        for (index, outcome) in outcomes.into_iter().enumerate() {
            let (bytes, source) = &fetched[index];
            let scanned = match (outcome, cache.as_mut()) {
                (Ok(scanned), cache) => {
                    if let Some(cache) = cache {
                        cache.accept(references[index], *source, bytes);
                    }
                    scanned?
                }
                (Err(_), Some(cache)) if *source != super::cache::Source::Remote => {
                    cache.reject(references[index], *source);
                    let block = cache.read_block(&self.store, config, references[index])?;
                    scan(index, block)?
                }
                (Err(error), _) => return Err(error),
            };
            for candidate in scanned {
                if heap.len() < k {
                    heap.push(candidate);
                } else if heap.peek().is_some_and(|worst| candidate < *worst) {
                    heap.pop();
                    heap.push(candidate);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{approximate, approximate_within, code_bytes, set_code};
    use crate::{
        retry::{Request, RequestId},
        segmented::SegmentedDatabase,
        store::LocalStore,
        Config, Metric, Mutation,
    };
    use std::collections::BTreeMap;

    #[test]
    fn five_bit_codec_crosses_byte_boundaries() {
        let mut bytes = [0_u8; 10];
        for axis in 0..16 {
            set_code(&mut bytes, axis, ((axis * 7) % 32) as u8);
        }
        let mut table = vec![[0_f64; 32]; 16];
        for (axis, row) in table.iter_mut().enumerate() {
            row[(axis * 7) % 32] = 1. / (1 << axis) as f64;
        }
        let expected: f64 = (0..16).map(|axis| 1. / (1 << axis) as f64).sum();
        assert_eq!(approximate(&bytes, &table), expected);
        assert_eq!(
            approximate_within(&bytes, &table, f64::INFINITY),
            Some(expected)
        );
        assert_eq!(approximate_within(&bytes, &table, 1.), None);
    }

    #[test]
    fn read_budget_widens_pack_spans_within_request_and_byte_caps() {
        use super::{choose, ReadBudget};
        use crate::segmented::BlockRef;
        let block = |object: &str, offset: usize, length: usize| BlockRef {
            object: object.into(),
            payload_len: 1_000,
            offset,
            length,
            sha256: "0".repeat(64),
            partition: 0,
            first_id: 0,
            last_id: 0,
            rows: 1,
        };
        let refs = [
            block("a", 100, 100),
            block("b", 0, 300),
            block("a", 300, 100), // widens a to 100..400 (300 bytes)
            block("c", 0, 100),   // a third span exceeds two requests
            block("a", 600, 100), // widening a to 100..700 exceeds 700 bytes
            block("b", 300, 50),  // widens b to 0..350
        ];
        let refs: Vec<_> = refs.iter().collect();
        let budget = ReadBudget {
            blocks: 6,
            requests: 2,
            bytes: 700,
        };
        assert_eq!(
            choose(&refs, budget),
            vec![("a", 100, 300, 1_000), ("b", 0, 350, 1_000)]
        );
        assert_eq!(
            choose(&refs, ReadBudget::uniform(6)),
            vec![
                ("a", 100, 600, 1_000),
                ("b", 0, 350, 1_000),
                ("c", 0, 100, 1_000)
            ]
        );
    }

    /// Pruned, threaded routing must choose exactly the blocks a full scan of
    /// every live code chooses, including after overwrites and tail writes.
    #[test]
    fn pruned_routing_selects_the_full_scan_blocks() {
        let seed = 0x9e37_79b9_u64;
        let mut state = seed;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
            let config = Config {
                dimensions: 19,
                metric,
            };
            let temp = tempfile::tempdir().unwrap();
            let mut db =
                SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
            for batch in 0..40_u64 {
                let mutations = (0..100)
                    .map(|_| Mutation::Put {
                        id: next() % 3_000,
                        vector: (0..19).map(|_| (next() % 2_000) as f32 / 7.).collect(),
                        metadata: BTreeMap::new(),
                    })
                    .collect();
                db.apply_request(Request {
                    id: RequestId {
                        boundary: db.sequence(),
                        nonce: u128::from(batch).to_le_bytes(),
                    },
                    conditions: Vec::new(),
                    mutations,
                })
                .unwrap();
                if batch % 7 == 6 {
                    db.seal_delta().unwrap();
                }
            }
            assert!(db.sketches.packs.len() >= 5, "seed {seed}");
            if metric == Metric::Manhattan {
                while db.compact_sketch(1) {}
            }
            let width = code_bytes(19);
            for threads in [1, 3] {
                db.query_threads = threads;
                for _ in 0..20 {
                    let query: Vec<f32> = (0..19).map(|_| (next() % 2_000) as f32 / 7.).collect();
                    let mut brute = Vec::new();
                    for (slot, sketch) in db.sketches.packs.iter().enumerate() {
                        let table: Vec<[f64; 32]> = (0..19)
                            .map(|axis| {
                                std::array::from_fn(|code| {
                                    let difference = f64::from(sketch.minima[axis])
                                        + f64::from(sketch.scales[axis]) * code as f64
                                        - f64::from(query[axis]);
                                    match metric {
                                        Metric::SquaredEuclidean | Metric::Cosine => {
                                            difference * difference
                                        }
                                        Metric::Manhattan => difference.abs(),
                                    }
                                })
                            })
                            .collect();
                        for (index, block) in sketch.blocks.iter().enumerate() {
                            let best = (block.start..block.end)
                                .filter(|&row| block.root.is_some() && sketch.is_live(row))
                                .map(|row| {
                                    approximate(
                                        &sketch.codes[row * width..(row + 1) * width],
                                        &table,
                                    )
                                })
                                .fold(f64::INFINITY, f64::min);
                            if best.is_finite() {
                                brute.push((best, slot, index));
                            }
                        }
                    }
                    brute.sort_by(|a, b| a.0.total_cmp(&b.0).then((a.1, a.2).cmp(&(b.1, b.2))));
                    for blocks in [1, 3, 8] {
                        let expected: Vec<_> = brute.iter().copied().take(blocks).collect();
                        assert_eq!(db.route(&query, blocks), expected, "seed {seed}");
                    }
                }
            }
        }
    }
}
