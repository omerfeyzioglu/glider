//! M37 stage 3: explicit conversion of a namespace's sealed rows into a
//! clustered view, published by one root v4.
//!
//! A conversion freezes the selected root and its latest-ID directory, then
//! advances in bounded steps, each at most one canonical pack read or one
//! object create: it samples live sealed rows, trains centroids, assigns
//! every row, gathers cluster ranges into cluster-contiguous posting packs,
//! then creates the centroid object, the catalog and finally the root. Only
//! that root create switches serving; every earlier object is an orphan
//! that cleanup removes after a reopen. The canonical runs, log tail,
//! sequence and retry state are unchanged and stay authoritative.
use super::{
    attempt_id,
    clustered::{
        Catalog, CatalogBlock, Center, Centroids, Cluster, ClusterIndex, Extent, ExtentKind,
        ObjectRef, PostingRole, ViewRef,
    },
    codec, decode_block_bytes, encode, encode_pack_with_sketch, root_key, Block, BlockRecord,
    BlockRef, SegmentedDatabase, MAX_PACK_BLOCKS, MAX_PACK_BYTES,
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
const TARGET_CLUSTER_ROWS: f64 = 4_000.;
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
    /// at or below it.
    pub source_sequence: u64,
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

pub(super) struct ConvertState {
    attempt: String,
    seed: u64,
    gather_bytes: usize,
    generation: u64,
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
    /// Per source pack, the cluster of each live put in scan order.
    assignments: Vec<Vec<u16>>,
    charges: Vec<usize>,
    ranges: Vec<Range<usize>>,
    extents: Vec<Vec<Extent>>,
    centroid_object: Option<ObjectRef>,
    catalog: Option<ObjectRef>,
    summary: ConversionSummary,
}

/// `2^round(log2(rows / 4,000))`, at least one.
fn automatic_centroids(rows: usize) -> usize {
    let exponent = (rows as f64 / TARGET_CLUSTER_ROWS).log2().round();
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
fn assign(metric: Metric, centers: &[Vec<f32>], vectors: &[&[f32]]) -> Vec<u16> {
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

/// Split each cluster's ID-sorted rows into blocks of at most 170 rows and
/// the raw block limit, then place clusters in ID order into packs of at
/// most 12 blocks and the raw pack bound. A cluster that does not fit the
/// open pack's remaining room starts a new pack, so it spans packs only if
/// it exceeds one; its blocks are contiguous either way.
fn layout(
    config: crate::Config,
    buffers: Vec<(u32, Vec<BlockRecord>)>,
) -> Result<VecDeque<Vec<Block>>> {
    let empty = codec::block_len([]);
    let mut packs = VecDeque::new();
    let (mut open, mut open_raw) = (Vec::new(), 0);
    for (cluster, mut rows) in buffers {
        if rows.is_empty() {
            continue;
        }
        rows.sort_unstable_by_key(BlockRecord::id);
        let mut blocks = Vec::new();
        let (mut current, mut raw) = (Vec::new(), empty);
        for record in rows {
            let length = codec::record_len(&record);
            if !current.is_empty()
                && (current.len() == BLOCK_ROWS || raw + length > codec::MAX_RAW_BLOCK_BYTES)
            {
                blocks.push((Block::new(config, cluster, mem::take(&mut current))?, raw));
                raw = empty;
            }
            if empty + length > codec::MAX_RAW_BLOCK_BYTES {
                return Err(Error::Invalid(format!(
                    "segmented row {} exceeds block limit",
                    record.id()
                )));
            }
            raw += length;
            current.push(record);
        }
        blocks.push((Block::new(config, cluster, current)?, raw));
        let total: usize = blocks.iter().map(|(_, raw)| raw).sum();
        let mut fits =
            open.len() + blocks.len() <= MAX_PACK_BLOCKS && open_raw + total <= MAX_PACK_RAW_BYTES;
        for (block, raw) in blocks {
            if !fits || open.len() == MAX_PACK_BLOCKS || open_raw + raw > MAX_PACK_RAW_BYTES {
                if !open.is_empty() {
                    packs.push_back(mem::take(&mut open));
                    open_raw = 0;
                }
                fits = true;
            }
            open.push(block);
            open_raw += raw;
        }
    }
    if !open.is_empty() {
        packs.push_back(open);
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
    /// live sealed row for assignments and `gather_bytes` of rows per pass,
    /// plus one pack. Queries keep using the previous root until the new
    /// root's create succeeds; interrupted conversions leave only orphans
    /// that cleanup removes. Any create error poisons the handle: reopen to
    /// learn whether the root was published. Writes may continue in the log
    /// tail, which shadows converted rows like any older version.
    pub fn convert_clustered(&mut self, options: ConvertOptions) -> Result<ConversionSummary> {
        self.start_conversion(options)?;
        loop {
            if let Some(summary) = self.conversion_step()? {
                return Ok(summary);
            }
        }
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
                rows: rows as u64,
                ..ConversionSummary::default()
            },
        });
        Ok(())
    }

    /// Visit the live sealed puts of one source pack in scan order, from
    /// one range read of its referenced blocks, each authenticated.
    fn source_puts(
        &self,
        source: &Source,
        mut visit: impl FnMut(BlockRecord) -> Result<()>,
    ) -> Result<()> {
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
        let mut seen = 0;
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
                let id = record.id();
                let current = matches!(record.mutation, Mutation::Put { .. })
                    && self.latest.get(&id).is_some_and(|location| {
                        location.run == run
                            && location.entry.block as usize == ordinal
                            && location.entry.sequence == record.sequence
                            && !location.entry.deleted
                    });
                if current {
                    seen += 1;
                    visit(record)?;
                }
            }
        }
        if seen != source.puts {
            return Err(Error::Corrupt(
                "segmented directory disagrees with a canonical block".into(),
            ));
        }
        Ok(())
    }

    /// Advance a conversion by one bounded step: one canonical pack read
    /// (sampling, assignment or gathering), training, or one create of a
    /// centroid object, posting pack, catalog or root. Returns the summary
    /// once the root is published.
    pub(super) fn conversion_step(&mut self) -> Result<Option<ConversionSummary>> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let Some(mut state) = self.convert.take() else {
            return Ok(None);
        };
        if state.generation != self.root.generation {
            return Err(Error::Corrupt("root changed during conversion".into()));
        }
        let config = self.config;
        let phase = mem::replace(&mut state.phase, Phase::Root);
        state.phase = match phase {
            Phase::Sample(mut sample) => {
                if state.next < state.sources.len() {
                    self.source_puts(&state.sources[state.next], |record| {
                        if let Mutation::Put { id, vector, .. } = &record.mutation {
                            sample.offer(*id, vector);
                        }
                        Ok(())
                    })?;
                    state.next += 1;
                    Phase::Sample(sample)
                } else {
                    self.train(&mut state, sample)?;
                    state.next = 0;
                    Phase::Assign
                }
            }
            Phase::Assign if state.centroid_object.is_none() => {
                let centroids = state.centroids.as_ref().expect("trained before assignment");
                let bytes = centroids.encode()?;
                let reference = object_ref(format!("sgcentroid-{}", state.attempt), &bytes);
                self.poisoned = true;
                self.create_staged(&reference.key, &bytes)?;
                self.poisoned = false;
                state.centroid_object = Some(reference);
                Phase::Assign
            }
            Phase::Assign => {
                if state.next < state.sources.len() {
                    let mut rows = Vec::new();
                    self.source_puts(&state.sources[state.next], |record| {
                        rows.push(record);
                        Ok(())
                    })?;
                    let vectors: Vec<&[f32]> = rows
                        .iter()
                        .map(|record| match &record.mutation {
                            Mutation::Put { vector, .. } => vector.as_slice(),
                            Mutation::Delete { .. } => unreachable!("sources yield puts"),
                        })
                        .collect();
                    let clusters = assign(config.metric, &state.centers, &vectors);
                    for (record, &cluster) in rows.iter().zip(&clusters) {
                        state.charges[cluster as usize] +=
                            codec::record_len(record) + RECORD_OVERHEAD;
                    }
                    state.assignments.push(clusters);
                    state.next += 1;
                    Phase::Assign
                } else {
                    state.ranges = gather_ranges(&state.charges, state.gather_bytes);
                    state.summary.gather_passes = state.ranges.len();
                    state.next = 0;
                    Phase::Gather {
                        range: 0,
                        buffers: vec![Vec::new(); state.ranges[0].len()],
                    }
                }
            }
            Phase::Gather { range, mut buffers } => {
                let clusters = state.ranges[range].clone();
                if state.next < state.sources.len() {
                    let assignments = &state.assignments[state.next];
                    let mut row = 0;
                    self.source_puts(&state.sources[state.next], |record| {
                        let cluster = usize::from(assignments[row]);
                        row += 1;
                        if clusters.contains(&cluster) {
                            buffers[cluster - clusters.start].push(record);
                        }
                        Ok(())
                    })?;
                    state.next += 1;
                    Phase::Gather { range, buffers }
                } else {
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
            }
            Phase::Write { range, mut packs } => {
                if let Some(blocks) = packs.pop_front() {
                    self.write_posting_pack(&mut state, &blocks)?;
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
            Phase::Catalog => {
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
                // Every live sealed put has exactly one posting row.
                if posting_rows != state.summary.rows {
                    return Err(Error::Corrupt(
                        "conversion postings do not cover the sealed rows".into(),
                    ));
                }
                let bytes = catalog.encode(centroids)?;
                let reference = object_ref(format!("sgcluster-{}", state.attempt), &bytes);
                self.poisoned = true;
                self.create_staged(&reference.key, &bytes)?;
                self.poisoned = false;
                state.catalog = Some(reference);
                Phase::Root
            }
            Phase::Root => return self.publish_conversion(state).map(Some),
        };
        self.convert = Some(state);
        Ok(None)
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
            source_generation: self.root.generation,
            source_sequence: self.root.sequence,
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
        self.poisoned = true;
        self.create_staged(&key, &bytes)?;
        self.poisoned = false;
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

    /// Publish the root v4 selecting the staged view, then load it the way
    /// an open does. The previous view stays readable by query views that
    /// still hold it.
    fn publish_conversion(&mut self, mut state: ConvertState) -> Result<ConversionSummary> {
        let catalog_ref = state.catalog.take().expect("catalog staged");
        let centroid_ref = state.centroid_object.take().expect("centroids staged");
        let mut root = self.next_root()?;
        root.clustered = Some(ViewRef {
            epoch: state.epoch,
            centroid: centroid_ref,
            catalog: catalog_ref,
        });
        root.version = 4;
        root.validate(self.config)?;
        let bytes = encode(&root)?;
        self.poisoned = true;
        self.create_staged(&root_key(root.generation), &bytes)?;
        state.summary.root_generation = root.generation;
        state.summary.epoch = state.epoch;
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
        Ok(state.summary)
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
    /// through the tail and then the directory; root-changing maintenance
    /// waits for the conversion.
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
        assert!(matches!(db.start_seal(), Err(Error::MaintenanceRequired)));
        assert!(matches!(
            db.consolidate_runs_step(),
            Err(Error::MaintenanceRequired)
        ));
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
