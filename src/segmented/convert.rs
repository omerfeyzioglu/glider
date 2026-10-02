//! M37 stage 3: conversion of a namespace's sealed rows into a clustered
//! view, published by one root v4, explicitly (`convert_clustered`) or as
//! idle serving maintenance (`SegmentedServing`'s automatic conversion).
//!
//! A conversion freezes the selected root's runs and the live sealed puts
//! they hold, then advances in bounded steps, each at most one canonical
//! pack read or one object create: it samples live sealed rows, trains
//! centroids, assigns every row, gathers cluster ranges into
//! cluster-contiguous posting packs, then creates the centroid object, the
//! catalog and finally the root. Only that root create switches serving;
//! every earlier object is an orphan that cleanup removes after a reopen or
//! an abandoned attempt. The canonical runs, log tail, sequence and retry
//! state are unchanged and stay authoritative. Without a selected view,
//! seals and consolidation of runs newer than the frozen ones may publish
//! between steps; a frozen row they shadow is simply not copied, and the
//! versions they seal stay routed through their canonical packs.
use super::{
    attempt_id,
    clustered::{
        Catalog, CatalogBlock, Center, Centroids, Cluster, ClusterIndex, Extent, ExtentKind,
        ObjectRef, PostingRole, ViewRef,
    },
    codec, decode_block_bytes, encode_pack_with_sketch, root_key, Block, BlockRecord, BlockRef,
    SegmentedDatabase, MAX_PACK_BLOCKS, MAX_PACK_BYTES,
};
use crate::{
    ivf::{nearest_centers, train_bounded, TrainingSample},
    store::ObjectStore,
    Error, Metric, Mutation, Result,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    mem,
    ops::Range,
    sync::Arc,
};

/// The automatic centroid count targets about this many live rows per
/// cluster (`benchmarks/M37.md`: 256 centroids at 1,000,000 rows).
pub(super) const TARGET_CLUSTER_ROWS: usize = 4_000;
/// Bounded training profile: sample rows and Lloyd iterations.
const SAMPLE_ROWS: usize = 16_384;
const ITERATIONS: usize = 2;
const MAX_CENTROIDS: usize = 4_096;
/// Posting blocks hold at most this many rows, like sealed blocks.
const BLOCK_ROWS: usize = 170;
/// Raw block bytes per posting pack, as seals bound them (1 MiB less a
/// 64 KiB margin for compression framing).
const MAX_PACK_RAW_BYTES: usize = MAX_PACK_BYTES - 64 * 1024;
/// Charge per gathered record beyond its raw length, for allocations.
const RECORD_OVERHEAD: usize = 96;

/// How [`SegmentedDatabase::convert_clustered`] builds a view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConvertOptions {
    /// Centroid count, at most 4,096; `None` targets about 4,000 live rows
    /// per cluster, rounded to a power of two.
    pub centroids: Option<usize>,
    /// Seed of the hash priority that selects the training sample.
    pub seed: u64,
    /// Bytes of decoded rows one gather pass may hold. A pass reads every
    /// canonical pack once, so a smaller bound means more passes.
    pub gather_bytes: usize,
}

impl Default for ConvertOptions {
    fn default() -> Self {
        Self {
            centroids: None,
            seed: 42,
            gather_bytes: 64 * 1024 * 1024,
        }
    }
}

/// What a completed conversion published.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ConversionSummary {
    pub epoch: u64,
    pub root_generation: u64,
    /// The source root sequence: postings cover every live version sealed
    /// at or below it that is still current.
    pub source_sequence: u64,
    /// Posting rows: the frozen live sealed puts not shadowed by a seal
    /// before they were gathered.
    pub rows: u64,
    pub centroids: usize,
    pub sample_rows: usize,
    pub gather_passes: usize,
    pub posting_packs: usize,
    pub extents: usize,
    pub posting_bytes: u64,
}

/// One canonical pack holding live sealed puts: its referenced blocks with
/// live puts, as root `(run, ordinal)` in offset order, and their live put
/// count.
struct Source {
    key: String,
    blocks: Vec<(usize, usize)>,
    puts: usize,
}

enum Phase {
    Sample(TrainingSample),
    Assign,
    Gather {
        range: usize,
        buffers: Vec<Vec<BlockRecord>>,
    },
    Write {
        range: usize,
        packs: VecDeque<Vec<Block>>,
    },
    Catalog,
    Root,
}

/// Marks a put that was not current when its source was assigned; it
/// never becomes current again, so gathering skips it.
const UNASSIGNED: u16 = u16::MAX;

/// Where a running conversion is; see
/// [`SegmentedDatabase::conversion_progress`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ConversionProgress {
    /// `sample`, `assign`, `gather`, `write`, `catalog` or `root`.
    pub phase: &'static str,
    /// Canonical packs holding the frozen live sealed puts; the sample,
    /// assignment and every gather pass read each once.
    pub sources: usize,
    /// Source packs the current pass has read.
    pub sources_done: usize,
    /// Current gather pass (1-based) and the number of passes, both 0
    /// until assignment has sized them.
    pub pass: usize,
    pub passes: usize,
    pub posting_packs: usize,
    /// Live sealed puts frozen when the conversion started.
    pub rows: u64,
    /// The epoch being built: 1 for a first view, higher for a rebuild.
    pub epoch: u64,
    /// Its centroid count, 0 until training.
    pub centroids: usize,
}

impl ConversionProgress {
    /// Stable numeric code of `phase` for metrics: 1 sample, 2 assign,
    /// 3 gather, 4 write, 5 catalog, 6 root.
    pub fn phase_code(&self) -> u64 {
        match self.phase {
            "sample" => 1,
            "assign" => 2,
            "gather" => 3,
            "write" => 4,
            "catalog" => 5,
            "root" => 6,
            _ => 0,
        }
    }
}

pub(super) struct ConvertState {
    attempt: String,
    seed: u64,
    gather_bytes: usize,
    /// Generation and sequence of the root the conversion froze.
    generation: u64,
    sequence: u64,
    /// Index objects of the frozen root's runs. Runs sealed later are
    /// appended after them and only those may be consolidated, so frozen
    /// `(run, block)` locations stay valid until publication.
    frozen_runs: Vec<String>,
    /// Keys of every object this attempt created or tried to create; they
    /// stay out of cleanup until the attempt publishes or is abandoned.
    staged: Vec<String>,
    /// Current rows copied into posting packs so far.
    gathered: u64,
    /// Live sealed puts of the frozen root.
    frozen_rows: u64,
    epoch: u64,
    centroid_count: usize,
    sources: Vec<Source>,
    /// Next source pack of the current pass.
    next: usize,
    phase: Phase,
    centroids: Option<Centroids>,
    /// Centers in cluster-ID order; cluster IDs are their indexes.
    centers: Vec<Vec<f32>>,
    fingerprints: BTreeMap<u32, [u8; 32]>,
    /// Per source pack, the cluster of each put in scan order, or
    /// [`UNASSIGNED`] for a put that was no longer current.
    assignments: Vec<Vec<u16>>,
    charges: Vec<usize>,
    ranges: Vec<Range<usize>>,
    extents: Vec<Vec<Extent>>,
    centroid_object: Option<ObjectRef>,
    catalog: Option<ObjectRef>,
    summary: ConversionSummary,
}

/// The centroid count a conversion without an explicit count chooses for
/// `rows` live sealed rows: `2^round(log2(rows / 4,000))`, from 1 to 4,096.
pub fn automatic_centroids(rows: usize) -> usize {
    let exponent = (rows as f64 / TARGET_CLUSTER_ROWS as f64).log2().round();
    if exponent <= 0. {
        1
    } else {
        1_usize << (exponent as u32).min(12)
    }
}

fn object_ref(key: String, bytes: &[u8]) -> ObjectRef {
    ObjectRef {
        key,
        length: bytes.len(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
    }
}

/// Nearest center of each vector, on up to eight scoped threads.
pub(super) fn assign(metric: Metric, centers: &[Vec<f32>], vectors: &[&[f32]]) -> Vec<u16> {
    let threads = std::thread::available_parallelism()
        .map_or(1, |threads| threads.get())
        .clamp(1, 8);
    let nearest = |vector: &&[f32]| nearest_centers(metric, vector, centers, 1)[0].0 as u16;
    if threads == 1 || vectors.len() < 256 {
        return vectors.iter().map(nearest).collect();
    }
    let chunk = vectors.len().div_ceil(threads);
    std::thread::scope(|scope| {
        let workers: Vec<_> = vectors
            .chunks(chunk)
            .map(|part| scope.spawn(move || part.iter().map(nearest).collect::<Vec<_>>()))
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("assignment worker panicked"))
            .collect()
    })
}

/// One planned posting block: an index into the planned clusters and the
/// range of that cluster's ID-sorted rows it holds.
pub(super) type PlannedBlock = (usize, Range<usize>);

/// Split each cluster's ID-sorted rows, given as raw record lengths, into
/// blocks of at most 170 rows and the raw block limit, then place clusters in
/// the given order into packs of at most 12 blocks and the raw pack bound.
/// A cluster that does not fit the open pack's remaining room starts a new
/// pack, so it spans packs only if it exceeds one; its blocks are contiguous
/// either way. Empty clusters get no block.
pub(super) fn plan_layout(clusters: &[Vec<usize>]) -> Result<Vec<Vec<PlannedBlock>>> {
    let empty = codec::block_len([]);
    let mut packs = Vec::new();
    let (mut open, mut open_raw) = (Vec::new(), 0);
    for (cluster, rows) in clusters.iter().enumerate() {
        if rows.is_empty() {
            continue;
        }
        let mut blocks = Vec::new();
        let (mut start, mut raw) = (0, empty);
        for (row, &length) in rows.iter().enumerate() {
            if row > start
                && (row - start == BLOCK_ROWS || raw + length > codec::MAX_RAW_BLOCK_BYTES)
            {
                blocks.push(((cluster, start..row), raw));
                (start, raw) = (row, empty);
            }
            if empty + length > codec::MAX_RAW_BLOCK_BYTES {
                return Err(Error::Invalid("segmented row exceeds block limit".into()));
            }
            raw += length;
        }
        blocks.push(((cluster, start..rows.len()), raw));
        let total: usize = blocks.iter().map(|(_, raw)| raw).sum();
        let mut fits =
            open.len() + blocks.len() <= MAX_PACK_BLOCKS && open_raw + total <= MAX_PACK_RAW_BYTES;
        for (block, raw) in blocks {
            if !fits || open.len() == MAX_PACK_BLOCKS || open_raw + raw > MAX_PACK_RAW_BYTES {
                if !open.is_empty() {
                    packs.push(mem::take(&mut open));
                    open_raw = 0;
                }
                fits = true;
            }
            open.push(block);
            open_raw += raw;
        }
    }
    if !open.is_empty() {
        packs.push(open);
    }
    Ok(packs)
}

/// [`plan_layout`] over owned records: sorts each cluster's rows by ID and
/// returns the packs' blocks, each block's partition its cluster ID.
pub(super) fn layout(
    config: crate::Config,
    mut buffers: Vec<(u32, Vec<BlockRecord>)>,
) -> Result<VecDeque<Vec<Block>>> {
    for (_, rows) in &mut buffers {
        rows.sort_unstable_by_key(BlockRecord::id);
    }
    let lengths: Vec<Vec<usize>> = buffers
        .iter()
        .map(|(_, rows)| rows.iter().map(codec::record_len).collect())
        .collect();
    let plan = plan_layout(&lengths)?;
    let mut rows: Vec<VecDeque<BlockRecord>> = buffers
        .iter_mut()
        .map(|(_, rows)| mem::take(rows).into())
        .collect();
    let mut packs = VecDeque::with_capacity(plan.len());
    for planned in plan {
        let mut blocks = Vec::with_capacity(planned.len());
        for (cluster, range) in planned {
            let records: Vec<_> = rows[cluster].drain(..range.len()).collect();
            blocks.push(Block::new(config, buffers[cluster].0, records)?);
        }
        packs.push_back(blocks);
    }
    Ok(packs)
}

impl<S: ObjectStore> SegmentedDatabase<S> {
    /// Convert the selected root's sealed rows into a clustered view and
    /// publish it with a root v4, or replace an existing (possibly
    /// unavailable) view with a new epoch. Runs every bounded step in turn;
    /// run it under exclusive ownership while no other maintenance runs.
    ///
    /// Memory is bounded by the 16,384-row training sample, two bytes per
    /// sealed put of the source packs for assignments and `gather_bytes` of
    /// rows per pass, plus one pack. Queries keep using the previous root
    /// until the new root's create succeeds; interrupted conversions leave
    /// only orphans that cleanup removes. Any create error poisons the
    /// handle: reopen to learn whether the root was published. Any other
    /// error abandons the attempt. Writes may continue in the log tail,
    /// which shadows converted rows like any older version.
    pub fn convert_clustered(&mut self, options: ConvertOptions) -> Result<ConversionSummary> {
        self.start_conversion(options)?;
        loop {
            match self.conversion_step() {
                Ok(Some(summary)) => return Ok(summary),
                Ok(None) => {}
                Err(error) => {
                    self.abandon_conversion();
                    return Err(error);
                }
            }
        }
    }

    /// Whether a conversion is staged.
    pub fn conversion_active(&self) -> bool {
        self.convert.is_some()
    }

    /// The running conversion's phase and counters, if one is staged.
    pub fn conversion_progress(&self) -> Option<ConversionProgress> {
        let state = self.convert.as_ref()?;
        let (phase, pass) = match &state.phase {
            Phase::Sample(_) => ("sample", 0),
            Phase::Assign => ("assign", 0),
            Phase::Gather { range, .. } => ("gather", range + 1),
            Phase::Write { range, .. } => ("write", range + 1),
            Phase::Catalog => ("catalog", state.ranges.len()),
            Phase::Root => ("root", state.ranges.len()),
        };
        Some(ConversionProgress {
            phase,
            sources: state.sources.len(),
            sources_done: match state.phase {
                Phase::Sample(_) | Phase::Assign | Phase::Gather { .. } => state.next,
                _ => state.sources.len(),
            },
            pass,
            passes: state.ranges.len(),
            posting_packs: state.summary.posting_packs,
            rows: state.frozen_rows,
            epoch: state.epoch,
            centroids: state.summary.centroids,
        })
    }

    /// Drop a staged conversion. Its staged objects were never selected;
    /// unless the handle is poisoned they become obsolete for cleanup
    /// (a reopen's cleanup removes them otherwise).
    pub(super) fn abandon_conversion(&mut self) {
        if self.convert.take().is_some() && !self.poisoned {
            self.schedule_obsolete();
        }
    }

    /// Keys a staged conversion created, which cleanup must retain.
    pub(super) fn conversion_staged(&self) -> &[String] {
        self.convert
            .as_ref()
            .map_or(&[], |state| state.staged.as_slice())
    }

    /// Runs of the root a staged conversion froze, which consolidation
    /// must leave in place; 0 without a conversion.
    pub(super) fn conversion_frozen_runs(&self) -> usize {
        self.convert
            .as_ref()
            .map_or(0, |state| state.frozen_runs.len())
    }

    /// Freeze the selected root's live sealed puts for a conversion.
    pub(super) fn start_conversion(&mut self, options: ConvertOptions) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if self.maintenance_active() {
            return Err(Error::MaintenanceRequired);
        }
        if options.centroids == Some(0)
            || options.centroids.is_some_and(|count| count > MAX_CENTROIDS)
            || options.gather_bytes == 0
        {
            return Err(Error::Invalid(
                "conversion needs 1 to 4,096 centroids and a gather bound".into(),
            ));
        }
        let mut puts: BTreeMap<(usize, usize), usize> = BTreeMap::new();
        for (_, location) in self.latest.iter() {
            if !location.entry.deleted {
                *puts
                    .entry((location.run, location.entry.block as usize))
                    .or_default() += 1;
            }
        }
        let rows: usize = puts.values().sum();
        if rows == 0 {
            return Err(Error::Invalid(
                "conversion needs at least one sealed live row".into(),
            ));
        }
        let mut sources: BTreeMap<&str, Source> = BTreeMap::new();
        for (&(run, ordinal), &count) in &puts {
            let reference = &self.root.runs[run].blocks[ordinal];
            let source = sources
                .entry(reference.object.as_str())
                .or_insert_with(|| Source {
                    key: reference.object.clone(),
                    blocks: Vec::new(),
                    puts: 0,
                });
            source.blocks.push((run, ordinal));
            source.puts += count;
        }
        let root = &self.root;
        let mut sources: Vec<_> = sources.into_values().collect();
        for source in &mut sources {
            source
                .blocks
                .sort_by_key(|&(run, ordinal)| root.runs[run].blocks[ordinal].offset);
        }
        let sample_rows = rows.min(SAMPLE_ROWS);
        let centroid_count = options
            .centroids
            .unwrap_or_else(|| automatic_centroids(rows))
            .min(sample_rows);
        self.convert = Some(ConvertState {
            attempt: attempt_id()?,
            seed: options.seed,
            gather_bytes: options.gather_bytes,
            generation: self.root.generation,
            sequence: self.root.sequence,
            frozen_runs: self
                .root
                .runs
                .iter()
                .map(|run| run.index_object.clone())
                .collect(),
            staged: Vec::new(),
            gathered: 0,
            frozen_rows: rows as u64,
            epoch: self
                .root
                .clustered
                .as_ref()
                .map_or(1, |view| view.epoch + 1),
            centroid_count,
            assignments: Vec::with_capacity(sources.len()),
            sources,
            next: 0,
            phase: Phase::Sample(TrainingSample::new(SAMPLE_ROWS, options.seed)),
            centroids: None,
            centers: Vec::new(),
            fingerprints: BTreeMap::new(),
            charges: Vec::new(),
            ranges: Vec::new(),
            extents: Vec::new(),
            centroid_object: None,
            catalog: None,
            summary: ConversionSummary {
                source_sequence: self.root.sequence,
                ..ConversionSummary::default()
            },
        });
        Ok(())
    }

    /// Whether the frozen runs are still the selected root's first runs,
    /// so frozen `(run, block)` locations still name the same blocks.
    fn frozen_runs_intact(&self, state: &ConvertState) -> bool {
        let runs = &self.root.runs;
        runs.len() >= state.frozen_runs.len()
            && runs
                .iter()
                .zip(&state.frozen_runs)
                .all(|(run, frozen)| &run.index_object == frozen)
    }

    /// Every sealed put of one source pack's referenced blocks in scan
    /// order, each with whether it is still the current version, from one
    /// range read whose blocks are each authenticated. Reads everything
    /// before returning, so a failed read has no partial effect.
    fn source_puts(
        &self,
        state: &ConvertState,
        source: &Source,
    ) -> Result<Vec<(BlockRecord, bool)>> {
        let references: Vec<&BlockRef> = source
            .blocks
            .iter()
            .map(|&(run, ordinal)| &self.root.runs[run].blocks[ordinal])
            .collect();
        let start = references[0].offset;
        let end = references
            .iter()
            .map(|reference| reference.offset + reference.length)
            .max()
            .unwrap();
        let bytes = self
            .store
            .get_range(&source.key, start, end - start, references[0].payload_len)?
            .ok_or_else(|| Error::Corrupt(format!("segmented pack missing: {}", source.key)))?;
        let mut puts = Vec::new();
        let mut current_puts = 0;
        for (&(run, ordinal), reference) in source.blocks.iter().zip(references) {
            let at = reference.offset - start;
            let block = decode_block_bytes(
                self.config,
                reference,
                bytes
                    .get(at..at + reference.length)
                    .ok_or_else(|| Error::Corrupt("segmented range read is short".into()))?,
            )?;
            for record in block.records {
                if !matches!(record.mutation, Mutation::Put { .. }) {
                    continue;
                }
                let current = self.latest.get(&record.id()).is_some_and(|location| {
                    location.run == run
                        && location.entry.block as usize == ordinal
                        && location.entry.sequence == record.sequence
                        && !location.entry.deleted
                });
                current_puts += usize::from(current);
                puts.push((record, current));
            }
        }
        // Only a later seal moves a frozen ID's directory entry, and a
        // version once shadowed never becomes current again.
        let unchanged = self.root.runs.len() == state.frozen_runs.len();
        if current_puts > source.puts || (unchanged && current_puts != source.puts) {
            return Err(Error::Corrupt(
                "segmented directory disagrees with a canonical block".into(),
            ));
        }
        Ok(puts)
    }

    /// Advance a conversion by one bounded step: one canonical pack read
    /// (sampling, assignment or gathering), training, or one create of a
    /// centroid object, posting pack, catalog or root. Returns the summary
    /// once the root is published.
    ///
    /// A failed read leaves the conversion staged for a retry. A create
    /// error poisons the handle; any other error abandons the attempt, so
    /// its staged objects become obsolete.
    pub(super) fn conversion_step(&mut self) -> Result<Option<ConversionSummary>> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let Some(mut state) = self.convert.take() else {
            return Ok(None);
        };
        if !self.frozen_runs_intact(&state) {
            self.convert = Some(state);
            self.abandon_conversion();
            return Err(Error::Corrupt(
                "frozen runs changed during conversion".into(),
            ));
        }
        let reads = match state.phase {
            Phase::Sample(_) | Phase::Gather { .. } => state.next < state.sources.len(),
            Phase::Assign => state.centroid_object.is_some() && state.next < state.sources.len(),
            Phase::Root => {
                if self.seal.is_some() {
                    // A staged seal publishes from the root it froze.
                    self.convert = Some(state);
                    return Err(Error::MaintenanceRequired);
                }
                false
            }
            _ => false,
        };
        let puts = if reads {
            match self.source_puts(&state, &state.sources[state.next]) {
                Ok(puts) => Some(puts),
                Err(error) => {
                    self.convert = Some(state);
                    return Err(error);
                }
            }
        } else {
            None
        };
        match self.advance_conversion(&mut state, puts) {
            Ok(true) => Ok(Some(state.summary)),
            Ok(false) => {
                self.convert = Some(state);
                Ok(None)
            }
            Err(error) => {
                if !self.poisoned {
                    // Keep the staged keys out of cleanup until now.
                    self.convert = Some(state);
                    self.abandon_conversion();
                }
                Err(error)
            }
        }
    }

    /// One step after its read, if any. Returns true once published.
    fn advance_conversion(
        &mut self,
        state: &mut ConvertState,
        puts: Option<Vec<(BlockRecord, bool)>>,
    ) -> Result<bool> {
        let config = self.config;
        let phase = mem::replace(&mut state.phase, Phase::Root);
        state.phase = match (phase, puts) {
            (Phase::Sample(mut sample), Some(puts)) => {
                for (record, current) in &puts {
                    if let (true, Mutation::Put { id, vector, .. }) = (current, &record.mutation) {
                        sample.offer(*id, vector);
                    }
                }
                state.next += 1;
                Phase::Sample(sample)
            }
            (Phase::Sample(sample), None) => {
                self.train(state, sample)?;
                state.next = 0;
                Phase::Assign
            }
            (Phase::Assign, None) if state.centroid_object.is_none() => {
                let centroids = state.centroids.as_ref().expect("trained before assignment");
                let bytes = centroids.encode()?;
                let reference = object_ref(format!("sgcentroid-{}", state.attempt), &bytes);
                self.create_conversion_object(state, &reference.key, &bytes)?;
                state.centroid_object = Some(reference);
                Phase::Assign
            }
            (Phase::Assign, Some(puts)) => {
                let vectors: Vec<&[f32]> = puts
                    .iter()
                    .filter(|(_, current)| *current)
                    .map(|(record, _)| match &record.mutation {
                        Mutation::Put { vector, .. } => vector.as_slice(),
                        Mutation::Delete { .. } => unreachable!("sources yield puts"),
                    })
                    .collect();
                let mut clusters = assign(config.metric, &state.centers, &vectors).into_iter();
                let mut assigned = Vec::with_capacity(puts.len());
                for (record, current) in &puts {
                    if *current {
                        let cluster = clusters.next().expect("one cluster per current put");
                        state.charges[cluster as usize] +=
                            codec::record_len(record) + RECORD_OVERHEAD;
                        assigned.push(cluster);
                    } else {
                        assigned.push(UNASSIGNED);
                    }
                }
                state.assignments.push(assigned);
                state.next += 1;
                Phase::Assign
            }
            (Phase::Assign, None) => {
                state.ranges = gather_ranges(&state.charges, state.gather_bytes);
                state.summary.gather_passes = state.ranges.len();
                state.next = 0;
                Phase::Gather {
                    range: 0,
                    buffers: vec![Vec::new(); state.ranges[0].len()],
                }
            }
            (Phase::Gather { range, mut buffers }, Some(puts)) => {
                let clusters = state.ranges[range].clone();
                let assignments = &state.assignments[state.next];
                if assignments.len() != puts.len() {
                    return Err(Error::Corrupt(
                        "conversion source changed between passes".into(),
                    ));
                }
                for ((record, current), &cluster) in puts.into_iter().zip(assignments) {
                    let cluster = usize::from(cluster);
                    // A put shadowed since assignment is not copied.
                    if current && cluster != usize::from(UNASSIGNED) && clusters.contains(&cluster)
                    {
                        buffers[cluster - clusters.start].push(record);
                        state.gathered += 1;
                    }
                }
                state.next += 1;
                Phase::Gather { range, buffers }
            }
            (Phase::Gather { range, buffers }, None) => {
                let clusters = state.ranges[range].clone();
                let buffers = buffers
                    .into_iter()
                    .enumerate()
                    .map(|(offset, rows)| ((clusters.start + offset) as u32, rows))
                    .collect();
                state.next = 0;
                Phase::Write {
                    range,
                    packs: layout(config, buffers)?,
                }
            }
            (Phase::Write { range, mut packs }, None) => {
                if let Some(blocks) = packs.pop_front() {
                    self.write_posting_pack(state, &blocks)?;
                    Phase::Write { range, packs }
                } else if range + 1 < state.ranges.len() {
                    Phase::Gather {
                        range: range + 1,
                        buffers: vec![Vec::new(); state.ranges[range + 1].len()],
                    }
                } else {
                    Phase::Catalog
                }
            }
            (Phase::Catalog, None) => {
                let centroids = state.centroids.as_ref().expect("trained before catalog");
                let catalog = Catalog {
                    epoch: state.epoch,
                    clusters: mem::take(&mut state.extents)
                        .into_iter()
                        .enumerate()
                        .map(|(id, extents)| Cluster {
                            id: id as u32,
                            extents,
                        })
                        .collect(),
                };
                let posting_rows: u64 = catalog
                    .clusters
                    .iter()
                    .flat_map(|cluster| &cluster.extents)
                    .map(|extent| u64::from(extent.rows))
                    .sum();
                // Every gathered put has exactly one posting row and, unless
                // a seal shadowed some since the freeze, every frozen one.
                let unchanged = self.root.runs.len() == state.frozen_runs.len();
                if posting_rows != state.gathered
                    || (unchanged && posting_rows != state.frozen_rows)
                {
                    return Err(Error::Corrupt(
                        "conversion postings do not cover the sealed rows".into(),
                    ));
                }
                let bytes = catalog.encode(centroids)?;
                let reference = object_ref(format!("sgcluster-{}", state.attempt), &bytes);
                self.create_conversion_object(state, &reference.key, &bytes)?;
                state.catalog = Some(reference);
                Phase::Root
            }
            (Phase::Root, None) => {
                // A legacy root's runs first get their manifests, one create
                // per step; the root itself is published on the last step.
                if self.publish_conversion(state)? {
                    return Ok(true);
                }
                Phase::Root
            }
            (_, Some(_)) => unreachable!("only sample, assign and gather steps read"),
        };
        Ok(false)
    }

    /// Create one staged object of a conversion; an error poisons the
    /// handle because the create's outcome is unknown.
    fn create_conversion_object(
        &mut self,
        state: &mut ConvertState,
        key: &str,
        bytes: &[u8],
    ) -> Result<()> {
        state.staged.push(key.to_owned());
        self.poisoned = true;
        self.create_staged(key, bytes)?;
        self.poisoned = false;
        Ok(())
    }

    /// Train centers on the sample, then stage the centroid object.
    fn train(&self, state: &mut ConvertState, sample: TrainingSample) -> Result<()> {
        let metric = self.config.metric;
        let rows = sample.into_rows();
        let mut digest = Sha256::new();
        for (id, vector) in &rows {
            digest.update(id.to_le_bytes());
            for value in vector {
                digest.update(value.to_le_bytes());
            }
        }
        let mut centers = train_bounded(metric, &rows, state.centroid_count, ITERATIONS);
        if metric == Metric::Cosine {
            // Means of unit vectors are not unit vectors; a degenerate mean
            // keeps its (unit) initial sample row.
            for (index, center) in centers.iter_mut().enumerate() {
                let norm = center
                    .iter()
                    .map(|&value| f64::from(value).powi(2))
                    .sum::<f64>()
                    .sqrt();
                if norm.is_finite() && norm > 1e-6 {
                    *center = self.config.normalized(mem::take(center));
                } else {
                    center.clone_from(&rows[index].1);
                }
            }
        }
        let centroids = Centroids {
            config: self.config,
            source_generation: state.generation,
            source_sequence: state.sequence,
            seed: state.seed,
            sample_rule: 1,
            sample_rows: rows.len() as u32,
            iterations: ITERATIONS as u32,
            sample_sha256: digest.finalize().into(),
            epoch: state.epoch,
            centers: centers
                .iter()
                .enumerate()
                .map(|(id, coordinates)| Center {
                    id: id as u32,
                    coordinates: coordinates.clone(),
                })
                .collect(),
        };
        let index = ClusterIndex::new(
            &centroids,
            Catalog {
                epoch: state.epoch,
                clusters: Vec::new(),
            },
        );
        state.fingerprints = index.fingerprints;
        state.summary.centroids = centers.len();
        state.summary.sample_rows = rows.len();
        state.charges = vec![0; centers.len()];
        state.extents = vec![Vec::new(); centers.len()];
        state.centers = centers;
        state.centroids = Some(centroids);
        Ok(())
    }

    /// Create one posting pack and record its cluster extents.
    fn write_posting_pack(&mut self, state: &mut ConvertState, blocks: &[Block]) -> Result<()> {
        let key = format!(
            "sgpack-{}-{:08}",
            state.attempt, state.summary.posting_packs
        );
        let (bytes, references, _) = encode_pack_with_sketch(
            &key,
            self.config,
            &self.options,
            &self.options_digest,
            blocks,
            Some(&state.fingerprints),
        )?;
        self.create_conversion_object(state, &key, &bytes)?;
        state.summary.posting_packs += 1;
        state.summary.posting_bytes += bytes.len() as u64;
        let mut last: Option<u32> = None;
        for reference in &references {
            let cluster = reference.partition;
            let block = CatalogBlock {
                offset: reference.offset as u32,
                length: reference.length as u32,
                sha256: super::sketch::digest_bytes(&reference.sha256)?,
            };
            let extents = &mut state.extents[cluster as usize];
            match extents.last_mut() {
                Some(extent) if last == Some(cluster) && extent.pack == key => {
                    extent.length += block.length;
                    extent.rows += reference.rows as u32;
                    extent.blocks.push(block);
                }
                _ => {
                    state.summary.extents += 1;
                    extents.push(Extent {
                        pack: key.clone(),
                        payload_len: reference.payload_len as u32,
                        offset: block.offset,
                        length: block.length,
                        rows: reference.rows as u32,
                        epoch: state.epoch,
                        cluster_id: cluster,
                        kind: ExtentKind::Derived,
                        role: PostingRole::Primary,
                        blocks: vec![block],
                    });
                }
            }
            last = Some(cluster);
        }
        Ok(())
    }

    /// Publish the root selecting the staged view over the current runs
    /// (the frozen ones plus any sealed since), then load it the way an
    /// open does. Versions sealed after the freeze have no posting copy and
    /// stay routed through their canonical packs. The previous view stays
    /// readable by query views that still hold it. A legacy root's runs
    /// first get their manifests, one create per call; returns true once the
    /// root is published.
    fn publish_conversion(&mut self, state: &mut ConvertState) -> Result<bool> {
        let mut root = self.next_root()?;
        root.clustered = Some(ViewRef {
            epoch: state.epoch,
            centroid: state.centroid_object.clone().expect("centroids staged"),
            catalog: state.catalog.clone().expect("catalog staged"),
        });
        root.validate(self.config)?;
        if self.stage_manifest(&mut root)? {
            return Ok(false);
        }
        let bytes = super::manifest::encode_root(&root)?;
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &bytes)?;
        state.summary.root_generation = root.generation;
        state.summary.epoch = state.epoch;
        state.summary.rows = state.gathered;
        if let Some(old) = self.cluster.take() {
            self.retired_views.push(Arc::downgrade(&old));
        }
        self.replace_root(root);
        // Release the old routing state before loading the new view.
        self.load_routing()?;
        if let Some(error) = &self.cluster_error {
            return Err(Error::Corrupt(format!(
                "published clustered view did not load: {error}"
            )));
        }
        self.poisoned = false;
        self.schedule_obsolete();
        Ok(true)
    }
}

/// Consecutive cluster ranges whose gathered rows fit `budget`; a cluster
/// above it gets a range of its own. Empty clusters cost nothing.
fn gather_ranges(charges: &[usize], budget: usize) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let (mut start, mut total) = (0, 0);
    for (cluster, &charge) in charges.iter().enumerate() {
        if cluster > start && total + charge > budget {
            ranges.push(start..cluster);
            (start, total) = (cluster, 0);
        }
        total += charge;
    }
    ranges.push(start..charges.len());
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        retry::{Request, RequestId},
        segmented::ReadBudget,
        store::LocalStore,
        Config,
    };

    fn put(id: u64, value: f32) -> Mutation {
        Mutation::Put {
            id,
            vector: vec![value, value * 0.5 + (id % 7) as f32, (id % 13) as f32, 1.],
            metadata: BTreeMap::new(),
        }
    }

    fn apply(db: &mut SegmentedDatabase<LocalStore>, mutations: Vec<Mutation>) {
        let nonce = u128::from(db.sequence()).to_le_bytes();
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce,
            },
            conditions: Vec::new(),
            mutations,
        })
        .unwrap();
    }

    fn assert_exact(db: &SegmentedDatabase<LocalStore>, context: &str) {
        let budget = ReadBudget {
            blocks: 1 << 20,
            requests: 1 << 20,
            bytes: usize::MAX,
            local_blocks: 0,
        };
        for query in 0..12 {
            let query = [query as f32 * 9., query as f32, (query % 5) as f32, 1.];
            let exact = db.search_exact(&query, 8, &[]).unwrap();
            let selective = db.search_selective_within(&query, 8, budget, &[]).unwrap();
            assert_eq!(selective, exact, "{context}: query {query:?}");
        }
    }

    /// Writes acknowledged between conversion steps shadow the frozen rows
    /// through the tail and then the directory; pruning, reclamation and
    /// merges wait for the conversion.
    #[test]
    fn writes_during_conversion_shadow_converted_rows() {
        let config = Config {
            dimensions: 4,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_cluster_probes(usize::MAX);
        for batch in 0..6_u64 {
            apply(
                &mut db,
                (0..100).map(|n| put(batch * 100 + n, n as f32)).collect(),
            );
        }
        db.seal_delta().unwrap();
        db.start_conversion(ConvertOptions {
            centroids: Some(5),
            seed: 3,
            gather_bytes: 8 * 1024,
        })
        .unwrap();
        assert!(matches!(db.start_prune(), Err(Error::MaintenanceRequired)));
        assert!(matches!(
            db.start_reclaim(),
            Err(Error::MaintenanceRequired)
        ));
        assert!(matches!(db.start_merge(), Err(Error::MaintenanceRequired)));
        let mut steps = 0;
        let summary = loop {
            if let Some(summary) = db.conversion_step().unwrap() {
                break summary;
            }
            steps += 1;
            if steps % 3 == 0 {
                let id = (steps * 37) % 600;
                apply(
                    &mut db,
                    vec![
                        put(id, 1_000. + steps as f32),
                        Mutation::Delete { id: id + 1 },
                    ],
                );
                assert_exact(&db, &format!("step {steps}"));
            }
        };
        assert_eq!(summary.rows, 600);
        assert_eq!(summary.centroids, 5);
        assert!(summary.gather_passes > 1 && steps > 20, "{summary:?}");
        assert_eq!(db.clustered_epoch(), Some(1));
        assert_exact(&db, "published");
        db.seal_delta().unwrap();
        assert_exact(&db, "sealed after conversion");
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_cluster_probes(usize::MAX);
        assert_exact(&db, "reopened");
    }

    /// Without a selected view, seals and consolidation of runs newer than
    /// the frozen ones publish between conversion steps. The view covers
    /// the frozen rows they did not shadow, newer versions stay routed
    /// through their canonical packs, and cleanup never removes a staged
    /// object before the conversion's root.
    #[test]
    fn seals_and_newer_run_consolidation_publish_during_conversion() {
        let config = Config {
            dimensions: 4,
            metric: Metric::SquaredEuclidean,
        };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let mut db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_cluster_probes(usize::MAX);
        for batch in 0..6_u64 {
            apply(
                &mut db,
                (0..100).map(|n| put(batch * 100 + n, n as f32)).collect(),
            );
            db.seal_delta().unwrap();
        }
        while db.consolidate_runs_step().unwrap() {}
        let frozen_runs = db.run_count();
        let frozen_index: Vec<_> = db
            .root
            .runs
            .iter()
            .map(|run| run.index_object.clone())
            .collect();
        db.start_conversion(ConvertOptions {
            centroids: Some(5),
            seed: 5,
            gather_bytes: 8 * 1024,
        })
        .unwrap();
        let (mut steps, mut seals, mut consolidations) = (0, 0, 0);
        let summary = loop {
            if let Some(summary) = db.conversion_step().unwrap() {
                break summary;
            }
            steps += 1;
            if steps % 4 == 0 {
                let id = (steps * 53) % 600;
                apply(
                    &mut db,
                    vec![
                        put(id, 2_000. + steps as f32),
                        put(600 + steps, steps as f32),
                        Mutation::Delete { id: (id + 7) % 600 },
                    ],
                );
                db.seal_delta().unwrap();
                seals += 1;
                while db.consolidate_runs_step().unwrap() {
                    consolidations += 1;
                }
                // Cleanup may run: staged objects are retained.
                while db.cleanup_step(4).unwrap() > 0 {}
                let runs: Vec<_> = db
                    .root
                    .runs
                    .iter()
                    .map(|run| run.index_object.clone())
                    .collect();
                assert_eq!(&runs[..frozen_runs], &frozen_index[..], "step {steps}");
                assert_exact(&db, &format!("step {steps}"));
            }
        };
        assert!(
            seals > 5 && consolidations > 0,
            "{seals} seals, {consolidations} consolidations"
        );
        assert!(summary.rows < 600, "{summary:?}");
        assert_eq!(db.clustered_epoch(), Some(1));
        let layout = db.clustered_layout().unwrap();
        assert!(layout.uncovered_packs > 0, "{layout:?}");
        assert_exact(&db, "published");
        while db.cleanup_step(4).unwrap() > 0 {}
        db.seal_delta().unwrap();
        while db.merge_postings().unwrap().is_some() {}
        assert_exact(&db, "sealed after conversion");
        drop(db);
        let db = SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config)
            .unwrap()
            .with_cluster_probes(usize::MAX);
        assert!(db.clustered_view_error().is_none());
        assert_exact(&db, "reopened");
    }

    #[test]
    fn layout_keeps_clusters_contiguous_within_block_and_pack_limits() {
        let config = crate::Config {
            dimensions: 8,
            metric: Metric::SquaredEuclidean,
        };
        let rows = |cluster: u32, count: u64| {
            (
                cluster,
                (0..count)
                    .rev()
                    .map(|n| BlockRecord {
                        sequence: n + 1,
                        mutation: Mutation::Put {
                            id: u64::from(cluster) * 10_000 + n,
                            vector: vec![cluster as f32; 8],
                            metadata: BTreeMap::new(),
                        },
                    })
                    .collect(),
            )
        };
        // 2,500 rows need 15 blocks: more than one pack.
        let packs = layout(
            config,
            vec![rows(0, 10), rows(1, 0), rows(2, 2_500), rows(3, 171)],
        )
        .unwrap();
        let mut order = Vec::new();
        for pack in &packs {
            assert!(pack.len() <= MAX_PACK_BLOCKS);
            for block in pack {
                assert!(block.records.len() <= BLOCK_ROWS);
                assert!(block
                    .records
                    .windows(2)
                    .all(|pair| pair[0].id() < pair[1].id()));
                order.push(block.partition);
            }
        }
        // Clusters appear once each, in ID order. Cluster 2 does not fit
        // beside cluster 0, so it starts a new pack and spans two; cluster
        // 3 fits the room it leaves.
        let mut runs = order.clone();
        runs.dedup();
        assert_eq!(runs, [0, 2, 3]);
        let partitions: Vec<Vec<u32>> = packs
            .iter()
            .map(|pack| pack.iter().map(|block| block.partition).collect())
            .collect();
        assert_eq!(
            partitions,
            [vec![0], vec![2; 12], vec![2, 2, 2, 3, 3]],
            "{order:?}"
        );
    }

    #[test]
    fn automatic_centroids_target_four_thousand_rows_per_cluster() {
        assert_eq!(automatic_centroids(1), 1);
        assert_eq!(automatic_centroids(5_000), 1);
        assert_eq!(automatic_centroids(250_000), 64);
        assert_eq!(automatic_centroids(1_000_000), 256);
        assert_eq!(automatic_centroids(usize::MAX), 4_096);
    }

    #[test]
    fn gather_ranges_respect_the_budget_and_cover_every_cluster() {
        assert_eq!(
            gather_ranges(&[5, 5, 5, 20, 1], 10),
            vec![0..2, 2..3, 3..4, 4..5]
        );
        assert_eq!(gather_ranges(&[0, 0, 0], 1), vec![0..3]);
        assert_eq!(gather_ranges(&[3], 1), vec![0..1]);
    }
}
