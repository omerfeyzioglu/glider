//! Single-owner segmented serving. Queries use the persisted sketches and run
//! on published views beside the admission committer; seal, consolidation,
//! pruning, reclamation, cleanup and cache warm-up advance in bounded units
//! that the committer runs only while no command is queued.
use super::{
    root_key, QueryHit, QueryOptions, ReadBudget, SegmentedDatabase, SegmentedOptions, View,
};
use crate::{
    admission::{Engine, EngineMetrics, QueryResult, Snapshot},
    retry::{Lookup, Outcome, Request, RequestId, Revision},
    store::ObjectStore,
    streaming::OwnedDocument,
    Config, Error, Neighbor, Result,
};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct SegmentedServingOptions {
    /// Start a seal when this many acknowledged log objects are unsealed.
    /// The engine rejects writes at 64, which forces synchronous maintenance.
    pub seal_tail_objects: usize,
    /// Blocks a selective query may read: its remote budget, charged only
    /// for uncached blocks, and its limit on cached blocks read locally.
    pub read_budget: ReadBudget,
    /// Obsolete objects removed by one cleanup unit.
    pub cleanup_objects: usize,
    /// Opening fails if loaded sketches charge more than this many bytes.
    pub max_index_bytes: usize,
    /// Optional disposable block cache: directory, RAM bytes, NVMe bytes.
    pub cache: Option<(PathBuf, usize, usize)>,
    /// Scoped threads used to score sketches for one query.
    pub query_threads: usize,
    /// Bytes one idle warm-up unit may read into the NVMe cache; 0 disables
    /// warm-up.
    pub warm_unit_bytes: usize,
}

impl SegmentedServingOptions {
    /// The M21 250,000-row envelope: 64 MiB engine RSS and a 256 MiB NVMe
    /// cache. The RAM block tier is disabled: NVMe (and the OS page cache)
    /// hold the working set, leaving the RAM budget for engine state. Idle
    /// warm-up copies the namespace into NVMe 256 KiB at a time, and up to
    /// 24 cached blocks (twice the routed candidates) are reranked locally
    /// in addition to the remote budget.
    pub fn m21(cache_directory: PathBuf) -> Self {
        Self {
            seal_tail_objects: 32,
            read_budget: ReadBudget {
                blocks: 12,
                requests: 8,
                bytes: 1024 * 1024,
                local_blocks: 24,
            },
            cleanup_objects: 4,
            max_index_bytes: 24 * 1024 * 1024,
            cache: Some((cache_directory, 0, 256 * 1024 * 1024)),
            query_threads: 4,
            warm_unit_bytes: 256 * 1024,
        }
    }

    /// The M31 1,000,000-row envelope: the M21 profile with a sketch budget
    /// for about 90 bytes of resident routing state per row (192 MiB RSS)
    /// and 8 routing threads, since routing scans four times the sketches.
    pub fn m31(cache_directory: PathBuf) -> Self {
        Self {
            max_index_bytes: 128 * 1024 * 1024,
            query_threads: 8,
            ..Self::m21(cache_directory)
        }
    }
}

/// Completed maintenance units, per handle.
#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct ServingCounters {
    pub seal_starts: u64,
    pub seal_steps: u64,
    pub consolidations: u64,
    pub prune_starts: u64,
    pub prune_steps: u64,
    pub reclaim_starts: u64,
    pub reclaim_steps: u64,
    pub removed_objects: u64,
    /// Seals forced inside a write because the log tail reached its bound.
    pub forced_seals: u64,
    pub sketch_compactions: u64,
    pub warm_steps: u64,
}

pub struct SegmentedServing<S: ObjectStore> {
    db: SegmentedDatabase<S>,
    options: SegmentedServingOptions,
    maintenance_time: Duration,
    /// A root changed since prune/reclaim last found no candidate.
    scan_pending: bool,
    counters: ServingCounters,
    last_unit: &'static str,
}

impl<S: ObjectStore> SegmentedServing<S> {
    /// Take over the namespace (fencing every earlier writer), then open it.
    /// Hold a [`crate::lease::Lease`] first so a live writer is not deposed;
    /// see [`SegmentedDatabase::take_over_with_options`] for errors.
    pub fn open(
        store: S,
        config: Config,
        segmented: SegmentedOptions,
        options: SegmentedServingOptions,
    ) -> Result<Self> {
        if options.seal_tail_objects == 0
            || options.seal_tail_objects >= 64
            || options.read_budget.blocks == 0
            || options.read_budget.requests == 0
            || options.cleanup_objects == 0
        {
            return Err(Error::Invalid(
                "segmented serving bounds are invalid".into(),
            ));
        }
        let mut db = SegmentedDatabase::take_over_with_options(store, config, segmented)?;
        // Serving fails closed rather than answer queries without the
        // selected view; a conversion rebuilds it from the canonical runs.
        if let Some(error) = db.clustered_view_error() {
            return Err(Error::Corrupt(format!(
                "clustered view unavailable ({error}); run glider-admin convert"
            )));
        }
        if db.selective_index_bytes() > options.max_index_bytes {
            return Err(Error::Invalid(
                "segmented sketches exceed the serving index budget".into(),
            ));
        }
        db = db.with_query_threads(options.query_threads);
        if let Some((directory, ram, nvme)) = &options.cache {
            db = db.with_block_cache(directory, *ram, *nvme)?;
        }
        Ok(Self {
            db,
            options,
            maintenance_time: Duration::ZERO,
            scan_pending: true,
            counters: ServingCounters::default(),
            last_unit: "none",
        })
    }

    pub fn database(&self) -> &SegmentedDatabase<S> {
        &self.db
    }

    pub fn counters(&self) -> ServingCounters {
        self.counters
    }

    /// Stop using the handle. An uncertain handle reports `RecoveryRequired`;
    /// the next takeover fences its late requests either way.
    pub fn close(self) -> Result<()> {
        if self.db.poisoned {
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }

    /// Kind of the most recent maintenance unit, for diagnostics.
    pub fn last_unit(&self) -> &'static str {
        self.last_unit
    }

    /// One bounded unit: a staged step, a seal plan, a run consolidation, a
    /// prune/reclaim plan, a cleanup batch or, when nothing else is due, one
    /// cache warm-up read. Returns false when idle.
    pub fn maintenance_step(&mut self) -> Result<bool> {
        if self.db.poisoned {
            return Err(Error::RecoveryRequired);
        }
        let started = Instant::now();
        let worked = self.step();
        self.maintenance_time += started.elapsed();
        worked
    }

    fn step(&mut self) -> Result<bool> {
        let db = &mut self.db;
        let counters = &mut self.counters;
        if db.seal.is_some() {
            self.last_unit = "seal_step";
            db.seal_step()?;
            counters.seal_steps += 1;
            self.scan_pending |= db.seal.is_none();
            return Ok(true);
        }
        if db.prune.is_some() {
            self.last_unit = "prune_step";
            db.prune_step()?;
            counters.prune_steps += 1;
            self.scan_pending |= db.prune.is_none();
            return Ok(true);
        }
        if db.reclaim.is_some() {
            self.last_unit = "reclaim_step";
            db.reclaim_step()?;
            counters.reclaim_steps += 1;
            self.scan_pending |= db.reclaim.is_none();
            return Ok(true);
        }
        // In-memory only: drop shadowed sketch rows so resident routing state
        // tracks the live set rather than accumulated overwrites.
        self.last_unit = "sketch_compaction";
        if db.compact_sketch(64) {
            counters.sketch_compactions += 1;
            return Ok(true);
        }
        if db.tail_objects >= self.options.seal_tail_objects {
            self.last_unit = "seal_plan";
            db.start_seal()?;
            counters.seal_starts += 1;
            return Ok(true);
        }
        self.last_unit = "consolidation";
        if db.consolidate_runs_step()? {
            counters.consolidations += 1;
            self.scan_pending = true;
            return Ok(true);
        }
        if self.scan_pending {
            self.last_unit = "prune_reclaim_scan";
            if db.start_prune()? {
                counters.prune_starts += 1;
                return Ok(true);
            }
            if db.start_reclaim()? {
                counters.reclaim_starts += 1;
                return Ok(true);
            }
            self.scan_pending = false;
        }
        self.last_unit = "cleanup";
        let removed = db.cleanup_step(self.options.cleanup_objects)?;
        counters.removed_objects += removed as u64;
        if removed > 0 {
            return Ok(true);
        }
        // Lowest priority: keep the selected root on local NVMe so queries
        // read it without remote requests.
        self.last_unit = "warm";
        if self.options.warm_unit_bytes > 0 && db.warm_cache_step(self.options.warm_unit_bytes)? {
            counters.warm_steps += 1;
            return Ok(true);
        }
        Ok(false)
    }

    /// At the hard log-tail bound, finish any staged maintenance and a seal
    /// synchronously before publishing; that time is command maintenance.
    fn make_room(&mut self) -> Result<()> {
        if self.db.tail_objects < super::MAX_TAIL_OBJECTS || self.db.poisoned {
            return Ok(());
        }
        let started = Instant::now();
        self.counters.forced_seals += 1;
        let result = (|| -> Result<()> {
            while self.db.prune.is_some() {
                self.db.prune_step()?;
            }
            while self.db.reclaim.is_some() {
                self.db.reclaim_step()?;
            }
            // Finish a seal started while idle; it may already free the
            // tail. Only then start another.
            while self.db.seal.is_some() {
                self.db.seal_step()?;
            }
            if self.db.tail_objects >= super::MAX_TAIL_OBJECTS {
                self.db.seal_delta()?;
            }
            Ok(())
        })();
        self.maintenance_time += started.elapsed();
        result?;
        self.scan_pending = true;
        Ok(())
    }

    /// Stage the current committed root, its referenced objects (packs carry
    /// their sketches), its clustered view's centroids, catalog and posting
    /// packs, and the acknowledged log tail into an empty, nonoverlapping
    /// destination.
    /// Every copied pack is checked against the root's or catalog's block
    /// digests before its PUT. Metadata is written last; the destination is
    /// then opened and compared. Destination failure does not poison this handle, and a failed
    /// destination must not be promoted or reused.
    pub fn backup_to<D: ObjectStore>(&mut self, mut destination: D) -> Result<()> {
        if self.db.poisoned {
            return Err(Error::RecoveryRequired);
        }
        if !destination.list()?.is_empty() {
            return Err(Error::Invalid("backup destination must be empty".into()));
        }
        let db = &self.db;
        let mut keys = vec![root_key(0), root_key(db.root.generation)];
        keys.dedup();
        let mut packs = std::collections::BTreeMap::<&str, Vec<_>>::new();
        for run in &db.root.runs {
            keys.push(run.index_object.clone());
            for block in &run.blocks {
                packs.entry(block.object.as_str()).or_default().push(block);
            }
        }
        for &sequence in &db.tail_logs {
            keys.push(super::log_key(sequence));
        }
        let copy = |destination: &mut D, key: &str| -> Result<Vec<u8>> {
            let bytes = db
                .store
                .get(key)?
                .ok_or_else(|| Error::Corrupt(format!("backup source missing: {key}")))?;
            destination.create(key, &bytes)?;
            Ok(bytes)
        };
        for key in &keys {
            copy(&mut destination, key)?;
        }
        for (pack, blocks) in &packs {
            let bytes = db
                .store
                .get(pack)?
                .ok_or_else(|| Error::Corrupt(format!("backup source missing: {pack}")))?;
            for block in blocks {
                let range = bytes
                    .get(block.offset..block.offset + block.length)
                    .filter(|_| bytes.len() == block.payload_len);
                if range.is_none_or(|range| format!("{:x}", Sha256::digest(range)) != block.sha256)
                {
                    return Err(Error::Corrupt(format!(
                        "backup pack digest mismatch: {pack}"
                    )));
                }
            }
            destination.create(pack, &bytes)?;
        }
        if let Some(view) = &db.root.clustered {
            let cluster = db.cluster.as_ref().ok_or_else(|| {
                Error::Corrupt("backup source clustered view is unavailable".into())
            })?;
            for reference in [&view.centroid, &view.catalog] {
                reference.authenticate(&copy(&mut destination, &reference.key)?)?;
            }
            // Posting packs are checked against the catalog's block digests.
            for (pack, layout) in cluster.pack_blocks() {
                let bytes = db
                    .store
                    .get(pack)?
                    .ok_or_else(|| Error::Corrupt(format!("backup source missing: {pack}")))?;
                let valid = bytes.len() == layout.payload_len
                    && layout.blocks.iter().all(|&(offset, length, digest)| {
                        bytes
                            .get(offset..offset + length)
                            .is_some_and(|block| Sha256::digest(block).as_slice() == digest)
                    });
                if !valid {
                    return Err(Error::Corrupt(format!(
                        "backup posting pack digest mismatch: {pack}"
                    )));
                }
                destination.create(pack, &bytes)?;
            }
        }
        copy(&mut destination, "metadata")?;
        let restored =
            SegmentedDatabase::open_with_options(destination, db.config, (*db.options).clone())?;
        if restored.sequence != db.sequence
            || restored.root.generation != db.root.generation
            || restored.latest.len() != db.latest.len()
            || restored.tail.len() != db.tail.len()
            || restored.cluster.is_some() != db.cluster.is_some()
        {
            return Err(Error::Corrupt(
                "backup does not match its source view".into(),
            ));
        }
        Ok(())
    }
}

/// A published view with the serving read budget, for queries that run
/// beside the admission committer.
struct Published<S> {
    view: View<S>,
    read_budget: ReadBudget,
}

impl<S: ObjectStore + Send + Sync> Snapshot for Published<S> {
    fn sequence(&self) -> u64 {
        self.view.sequence()
    }
    fn get(&self, id: u64) -> Result<Option<OwnedDocument>> {
        self.view.get(id)
    }
    fn query(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
        options: QueryOptions,
    ) -> Result<QueryResult> {
        let (hits, reads) =
            self.view
                .search_selective_within(query, k, self.read_budget, filter, options)?;
        Ok(QueryResult {
            sequence: self.view.sequence(),
            neighbors: hits.iter().map(QueryHit::neighbor).collect(),
            hits,
            remote_reads: reads.requests,
            remote_bytes: reads.bytes,
        })
    }
}

impl<S: ObjectStore + Send + Sync + 'static> Engine for SegmentedServing<S> {
    fn config(&self) -> Config {
        self.db.config
    }
    fn sequence(&self) -> u64 {
        self.db.sequence
    }
    fn recovery_required(&self) -> bool {
        self.db.poisoned
    }
    fn maintenance_time(&self) -> Duration {
        self.maintenance_time
    }
    fn apply_request(&mut self, request: Request) -> Result<Outcome> {
        self.make_room()?;
        self.db.apply_request(request)
    }
    fn apply_requests(&mut self, requests: Vec<Request>) -> Vec<Result<Outcome>> {
        if let Err(error) = self.make_room() {
            let message = error.to_string();
            return requests
                .iter()
                .map(|_| Err(Error::Io(std::io::Error::other(message.clone()))))
                .collect();
        }
        self.db.apply_requests(requests)
    }
    fn revision(&self, id: u64) -> Revision {
        self.db.revision(id)
    }
    fn request_id(&self) -> Result<RequestId> {
        self.db.request_id()
    }
    fn lookup_request(&self, id: RequestId) -> Result<Lookup> {
        self.db.lookup_request(id)
    }
    fn get(&self, id: u64) -> Result<Option<OwnedDocument>> {
        self.db.get(id)
    }
    /// Unfiltered queries are approximate within the sketch/block budget; the
    /// declared resident predicate is exact; other filters are applied to the
    /// routed blocks (approximate, possibly fewer than k).
    fn query(&mut self, query: &[f32], k: usize, filter: &[(&str, &str)]) -> Result<Vec<Neighbor>> {
        self.db
            .search_selective_within(query, k, self.options.read_budget, filter)
    }
    fn query_with_options(
        &mut self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
        options: QueryOptions,
    ) -> Result<Vec<QueryHit>> {
        self.db
            .search_selective_within_options(query, k, self.options.read_budget, filter, options)
    }
    /// Admission runs queries on these views, beside the committer.
    fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
        Some(Arc::new(Published {
            view: self.db.view(),
            read_budget: self.options.read_budget,
        }))
    }
    fn idle_step(&mut self) -> Result<bool> {
        self.maintenance_step()
    }
    /// Cumulative query reads through the cache; each query's own reads are
    /// reported in its `QueryResult`.
    fn remote_reads(&self) -> (u64, u64) {
        self.db
            .cache_stats()
            .ok()
            .flatten()
            .map_or((0, 0), |stats| {
                (stats.remote_fetches, stats.remote_payload_bytes)
            })
    }
    fn metrics(&self) -> Result<EngineMetrics> {
        let cache = self.db.cache_stats()?.unwrap_or_default();
        let counters = self.counters;
        Ok(EngineMetrics {
            sequence: self.db.sequence,
            samples: vec![
                ("glider_segmented_seal_starts_total", counters.seal_starts),
                ("glider_segmented_seal_steps_total", counters.seal_steps),
                (
                    "glider_segmented_consolidations_total",
                    counters.consolidations,
                ),
                ("glider_segmented_prune_starts_total", counters.prune_starts),
                ("glider_segmented_prune_steps_total", counters.prune_steps),
                (
                    "glider_segmented_reclaim_starts_total",
                    counters.reclaim_starts,
                ),
                (
                    "glider_segmented_reclaim_steps_total",
                    counters.reclaim_steps,
                ),
                (
                    "glider_segmented_removed_objects_total",
                    counters.removed_objects,
                ),
                ("glider_segmented_forced_seals_total", counters.forced_seals),
                (
                    "glider_segmented_sketch_compactions_total",
                    counters.sketch_compactions,
                ),
                ("glider_segmented_warm_steps_total", counters.warm_steps),
                ("glider_cache_ram_hits_total", cache.ram_hits),
                ("glider_cache_nvme_hits_total", cache.nvme_hits),
                ("glider_cache_remote_fetches_total", cache.remote_fetches),
                (
                    "glider_cache_remote_payload_bytes_total",
                    cache.remote_payload_bytes,
                ),
                ("glider_cache_corrupt_entries_total", cache.corrupt_entries),
                ("glider_cache_nvme_bytes", cache.nvme_bytes as u64),
                ("glider_cache_nvme_entries", cache.nvme_entries as u64),
                ("glider_cache_nvme_limit_bytes", cache.nvme_limit as u64),
                ("glider_cache_namespace_bytes", cache.namespace_bytes as u64),
                ("glider_cache_warm_bytes", cache.warm_bytes as u64),
                ("glider_cache_warm_complete", u64::from(cache.warm_complete)),
                ("glider_cache_warm_fetches_total", cache.warm_fetches),
                (
                    "glider_cache_warm_payload_bytes_total",
                    cache.warm_payload_bytes,
                ),
                ("glider_writer_epoch", self.db.epoch()),
                (
                    "glider_sketch_index_bytes",
                    self.db.selective_index_bytes() as u64,
                ),
            ],
        })
    }
    /// A clean handle closes; an uncertain one reports `RecoveryRequired`.
    fn close(self) -> Result<()> {
        SegmentedServing::close(self)
    }
}
