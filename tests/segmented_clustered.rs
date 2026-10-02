//! M37 stage 3: conversion to a clustered view and clustered queries.
//! Exact search over the canonical runs and the log tail is the oracle.
use glider::{
    retry::{Request, RequestId},
    segmented::{
        ConvertOptions, QueryOptions, ReadBudget, SegmentedDatabase, SegmentedOptions,
        SegmentedServing, SegmentedServingOptions,
    },
    store::ObjectStore,
    Config, Error, Metric, Mutation, Neighbor, Result,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

const DIMENSIONS: usize = 12;
const METRIC: Metric = Metric::SquaredEuclidean;

fn options() -> SegmentedOptions {
    SegmentedOptions {
        resident_filter: Some(("tag".into(), "hot".into())),
        routed_keys: vec!["route".into()],
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    /// Clustered points: a few well separated centers plus noise.
    fn vector(&mut self) -> Vec<f32> {
        let center = self.below(6) as f32 * 40.;
        (0..DIMENSIONS)
            .map(|_| center + self.below(1_000) as f32 / 100. + 0.5)
            .collect()
    }
}

/// Complete-object store in memory that counts range reads and lets a test
/// remove or rewrite objects behind the engine's back.
#[derive(Clone, Default)]
struct MemoryStore {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    ranges: Arc<AtomicU64>,
    range_bytes: Arc<AtomicU64>,
}

impl MemoryStore {
    fn keys(&self, prefix: &str) -> Vec<String> {
        let objects = self.objects.lock().unwrap();
        objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }
    fn reads(&self) -> (u64, u64) {
        (
            self.ranges.swap(0, Ordering::SeqCst),
            self.range_bytes.swap(0, Ordering::SeqCst),
        )
    }
}

impl ObjectStore for MemoryStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        let objects = self.objects.lock().unwrap();
        let Some(bytes) = objects.get(key) else {
            return Ok(None);
        };
        if bytes.len() != payload_len || offset + length > bytes.len() {
            return Err(Error::Corrupt(format!("range outside {key}")));
        }
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.range_bytes.fetch_add(length as u64, Ordering::SeqCst);
        Ok(Some(bytes[offset..offset + length].to_vec()))
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

type Model = BTreeMap<u64, Option<(Vec<f32>, BTreeMap<String, String>)>>;

fn metadata(rng: &mut Rng) -> BTreeMap<String, String> {
    let mut metadata = BTreeMap::new();
    if rng.below(4) == 0 {
        metadata.insert("tag".into(), "hot".into());
    }
    if rng.below(3) != 0 {
        metadata.insert("route".into(), format!("r{}", rng.below(5)));
    }
    if rng.below(2) == 0 {
        metadata.insert("color".into(), format!("c{}", rng.below(3)));
    }
    metadata
}

/// Apply one request of `count` random puts and deletes over `ids` IDs.
fn write(
    db: &mut SegmentedDatabase<MemoryStore>,
    model: &mut Model,
    rng: &mut Rng,
    count: u64,
    metric: Metric,
) {
    let ids = 1_500;
    let mutations: Vec<_> = (0..count)
        .map(|_| {
            let id = rng.below(ids);
            if rng.below(8) == 0 {
                Mutation::Delete { id }
            } else {
                Mutation::Put {
                    id,
                    vector: rng.vector(),
                    metadata: metadata(rng),
                }
            }
        })
        .collect();
    let nonce = rng.next();
    db.apply_request(Request {
        id: RequestId {
            boundary: db.sequence(),
            nonce: u128::from(nonce).to_le_bytes(),
        },
        conditions: Vec::new(),
        mutations: mutations.clone(),
    })
    .unwrap();
    for mutation in mutations {
        match mutation {
            Mutation::Put {
                id,
                vector,
                metadata,
            } => {
                // Cosine namespaces store unit vectors.
                let vector = if metric == Metric::Cosine {
                    let norm = vector
                        .iter()
                        .map(|&x| f64::from(x).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    vector
                        .iter()
                        .map(|&x| (f64::from(x) / norm) as f32)
                        .collect()
                } else {
                    vector
                };
                model.insert(id, Some((vector, metadata)))
            }
            Mutation::Delete { id } => model.insert(id, None),
        };
    }
}

fn everything() -> ReadBudget {
    ReadBudget {
        blocks: 1 << 20,
        requests: 1 << 20,
        bytes: usize::MAX,
        local_blocks: 0,
    }
}

fn bits(neighbors: &[Neighbor]) -> Vec<(u64, u64)> {
    neighbors
        .iter()
        .map(|neighbor| (neighbor.id, neighbor.distance.to_bits()))
        .collect()
}

/// With every posting probed and an unbounded budget, selective search
/// reads every current version once, so it equals exact search for every
/// filter kind; requested fields come from the scored version.
fn assert_full_budget_is_exact(
    db: &mut SegmentedDatabase<MemoryStore>,
    model: &Model,
    rng: &mut Rng,
    context: &str,
) {
    db.set_cluster_probes(usize::MAX);
    let filters: [&[(&str, &str)]; 5] = [
        &[],
        &[("tag", "hot")],
        &[("route", "r1")],
        &[("color", "c2")],
        &[("route", "r3"), ("color", "c0")],
    ];
    for round in 0..6 {
        let query = rng.vector();
        for filter in filters {
            let exact = db.search_exact(&query, 10, filter).unwrap();
            let selective = db
                .search_selective_within(&query, 10, everything(), filter)
                .unwrap();
            assert_eq!(
                bits(&selective),
                bits(&exact),
                "{context}: round {round}, filter {filter:?}"
            );
        }
        let options = QueryOptions {
            include_metadata: true,
            include_vector: true,
        };
        for filter in [&[][..], &[("tag", "hot")][..]] {
            let hits = db
                .search_selective_within_options(&query, 10, everything(), filter, options)
                .unwrap();
            for hit in hits {
                let (vector, metadata) = model[&hit.id].clone().expect("hit is live");
                assert_eq!(hit.metadata.as_ref(), Some(&metadata), "{context}");
                assert_eq!(hit.vector.as_ref(), Some(&vector), "{context}");
            }
        }
    }
    for (&id, expected) in model {
        let found = db
            .get(id)
            .unwrap()
            .map(|document| (document.vector, document.metadata));
        assert_eq!(&found, expected, "{context}: id {id}");
    }
    db.set_cluster_probes(glider::segmented::DEFAULT_CLUSTER_PROBES);
}

fn open(store: &MemoryStore, metric: Metric) -> SegmentedDatabase<MemoryStore> {
    SegmentedDatabase::open_with_options(
        store.clone(),
        Config {
            dimensions: DIMENSIONS,
            metric,
        },
        options(),
    )
    .unwrap()
    .with_reclaim_min_garbage(1)
}

fn small(centroids: usize) -> ConvertOptions {
    ConvertOptions {
        centroids: Some(centroids),
        seed: 7,
        // Several gather passes and posting packs even for small data.
        gather_bytes: 64 * 1024,
    }
}

fn maintain(db: &mut SegmentedDatabase<MemoryStore>) {
    db.seal_delta().unwrap();
    while db.consolidate_runs_step().unwrap() {}
    if db.start_prune().unwrap() {
        while db.prune_step().unwrap() {}
    }
    while db.reclaim_pack_step().unwrap() {}
    while db.cleanup_step(16).unwrap() > 0 {}
}

#[test]
fn converted_namespaces_match_exact_search_through_writes_maintenance_and_reconversion() {
    let seed = 0x3703_c0de_5eed_0001_u64;
    for metric in [Metric::SquaredEuclidean, Metric::Cosine, Metric::Manhattan] {
        let context = format!("seed {seed:#x}, {metric:?}");
        let mut rng = Rng(seed ^ metric as u64);
        let store = MemoryStore::default();
        let mut db = open(&store, metric);
        let mut model = Model::new();
        for batch in 0..30 {
            write(&mut db, &mut model, &mut rng, 80, metric);
            if batch % 6 == 5 {
                maintain(&mut db);
            }
        }
        // Unsealed tail rows stay in the tail through conversion.
        write(&mut db, &mut model, &mut rng, 40, metric);
        let sequence = db.sequence();
        let summary = db.convert_clustered(small(9)).unwrap();
        assert_eq!(summary.epoch, 1, "{context}");
        assert_eq!(summary.centroids, 9, "{context}");
        assert!(summary.gather_passes > 1, "{context}: {summary:?}");
        assert!(summary.posting_packs > 1, "{context}: {summary:?}");
        assert_eq!(db.sequence(), sequence, "{context}");
        assert_eq!(db.clustered_epoch(), Some(1), "{context}");
        assert!(db.tail_objects() > 0, "{context}");
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, converted"));
        // Overwrites and deletes shadow posting copies; later seals stay
        // per-seal and are routed beside the postings.
        for batch in 0..24 {
            write(&mut db, &mut model, &mut rng, 60, metric);
            if batch % 5 == 4 {
                maintain(&mut db);
            }
        }
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, updated"));
        maintain(&mut db);
        drop(db);
        let mut db = open(&store, metric);
        assert_eq!(db.clustered_epoch(), Some(1), "{context}");
        assert!(db.clustered_view_error().is_none(), "{context}");
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, reopened"));
        let first_view: BTreeSet<_> = store
            .keys("sgcentroid-")
            .into_iter()
            .chain(store.keys("sgcluster-"))
            .collect();
        let summary = db.convert_clustered(small(4)).unwrap();
        assert_eq!(summary.epoch, 2, "{context}");
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, epoch 2"));
        while db.cleanup_step(16).unwrap() > 0 {}
        let view: BTreeSet<_> = store
            .keys("sgcentroid-")
            .into_iter()
            .chain(store.keys("sgcluster-"))
            .collect();
        assert_eq!(view.len(), 2, "{context}");
        assert!(view.is_disjoint(&first_view), "{context}");
        drop(db);
        let mut db = open(&store, metric);
        assert_eq!(db.clustered_epoch(), Some(2), "{context}");
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, final"));
    }
}

#[test]
fn clustered_queries_stay_within_the_remote_budget() {
    let seed = 0x3703_b0d9_e7ee_0002_u64;
    let mut rng = Rng(seed);
    let store = MemoryStore::default();
    let mut db = open(&store, Metric::SquaredEuclidean);
    let mut model = Model::new();
    for batch in 0..40 {
        write(&mut db, &mut model, &mut rng, 100, METRIC);
        if batch % 8 == 7 {
            maintain(&mut db);
        }
    }
    maintain(&mut db);
    db.convert_clustered(small(16)).unwrap();
    for batch in 0..5 {
        write(&mut db, &mut model, &mut rng, 50, METRIC);
        if batch == 2 {
            maintain(&mut db);
        }
    }
    let budget = ReadBudget {
        blocks: 12,
        requests: 3,
        bytes: 64 * 1024,
        local_blocks: 0,
    };
    let mut found = 0;
    store.reads();
    for round in 0..30 {
        let query = rng.vector();
        let results = db.search_selective_within(&query, 10, budget, &[]).unwrap();
        let (requests, bytes) = store.reads();
        assert!(
            requests <= 3 && bytes <= 64 * 1024,
            "seed {seed:#x}, round {round}: {requests} requests, {bytes} bytes"
        );
        let exact = db.search_exact(&query, 10, &[]).unwrap();
        store.reads();
        let truth: BTreeSet<_> = exact.iter().map(|neighbor| neighbor.id).collect();
        found += results
            .iter()
            .filter(|neighbor| truth.contains(&neighbor.id))
            .count();
        // Results are exact distances of current versions.
        for neighbor in results {
            let (vector, _) = model[&neighbor.id].clone().expect("live");
            let distance: f64 = query
                .iter()
                .zip(&vector)
                .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
                .sum();
            assert_eq!(distance.to_bits(), neighbor.distance.to_bits());
        }
    }
    assert!(found > 0, "seed {seed:#x}");
}

/// Remove or rewrite one derived object and reopen: exact reads work,
/// selective queries and serving fail, and a conversion repairs the view.
#[test]
fn missing_or_corrupt_derived_objects_fail_clustered_serving_until_reconverted() {
    let seed = 0x3703_dead_0b1e_0003_u64;
    let damages = [
        "remove centroid",
        "remove catalog",
        "remove posting pack",
        "flip centroid",
        "flip catalog",
        "flip posting block",
        "flip posting sketch",
    ];
    for damage in damages {
        let context = format!("seed {seed:#x}, {damage}");
        let mut rng = Rng(seed);
        let store = MemoryStore::default();
        let mut db = open(&store, Metric::SquaredEuclidean);
        let mut model = Model::new();
        for batch in 0..12 {
            write(&mut db, &mut model, &mut rng, 80, METRIC);
            if batch % 4 == 3 {
                maintain(&mut db);
            }
        }
        db.convert_clustered(small(5)).unwrap();
        drop(db);
        let posting = {
            let canonical: BTreeSet<_> = store.keys("sgpack-").into_iter().collect();
            let objects = store.objects.lock().unwrap();
            // The posting packs are those whose sketch is GLSKT003.
            canonical
                .into_iter()
                .find(|key| objects[key][48..56] == *b"GLSKT003")
                .unwrap()
        };
        {
            let mut objects = store.objects.lock().unwrap();
            let target = match damage {
                "remove centroid" | "flip centroid" => store.keys_locked(&objects, "sgcentroid-"),
                "remove catalog" | "flip catalog" => store.keys_locked(&objects, "sgcluster-"),
                _ => posting.clone(),
            };
            let bytes = objects.get_mut(&target).unwrap();
            let frame = 48 + u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
            match damage {
                "remove centroid" | "remove catalog" | "remove posting pack" => {
                    objects.remove(&target);
                }
                "flip posting block" => bytes[frame + 20] ^= 0x40,
                "flip posting sketch" => bytes[60] ^= 0x01,
                _ => *bytes.last_mut().unwrap() ^= 0x01,
            }
        }
        let mut db = open(&store, Metric::SquaredEuclidean);
        if damage == "flip posting sketch" {
            // A sketch frame is derived: it is rebuilt from the
            // authenticated posting blocks and serving continues.
            assert!(db.clustered_view_error().is_none(), "{context}");
            assert!(db.sketch_rebuilds() > 0, "{context}");
            assert_full_budget_is_exact(&mut db, &model, &mut rng, &context);
            continue;
        }
        let query = rng.vector();
        db.set_cluster_probes(usize::MAX);
        assert!(
            matches!(
                db.search_selective_within(&query, 10, everything(), &[]),
                Err(Error::Corrupt(_))
            ),
            "{context}"
        );
        assert_eq!(db.search_exact(&query, 10, &[]).unwrap().len(), 10);
        if damage == "flip posting block" {
            // Open validates the authenticated sketches, not every block:
            // a query reading the corrupt block fails closed.
            assert!(db.clustered_view_error().is_none(), "{context}");
        } else {
            assert!(db.clustered_view_error().is_some(), "{context}");
            // Writes and seals continue; cleanup removes nothing it
            // cannot name.
            write(&mut db, &mut model, &mut rng, 30, METRIC);
            db.seal_delta().unwrap();
            assert_eq!(db.cleanup_step(64).unwrap(), 0, "{context}");
            drop(db);
            let temp = tempfile::tempdir().unwrap();
            assert!(
                matches!(
                    SegmentedServing::open(
                        store.clone(),
                        Config {
                            dimensions: DIMENSIONS,
                            metric: Metric::SquaredEuclidean,
                        },
                        options(),
                        SegmentedServingOptions {
                            cache: None,
                            ..SegmentedServingOptions::m21(temp.path().to_path_buf())
                        },
                    ),
                    Err(Error::Corrupt(_))
                ),
                "{context}"
            );
            db = open(&store, Metric::SquaredEuclidean);
        }
        let mut db = open(&store, Metric::SquaredEuclidean);
        assert_eq!(
            db.convert_clustered(small(5)).unwrap().epoch,
            2,
            "{context}"
        );
        assert!(db.clustered_view_error().is_none(), "{context}");
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, repaired"));
        while db.cleanup_step(16).unwrap() > 0 {}
        assert_eq!(store.keys("sgcentroid-").len(), 1, "{context}");
        assert_eq!(store.keys("sgcluster-").len(), 1, "{context}");
        drop(db);
        let mut db = open(&store, Metric::SquaredEuclidean);
        assert_full_budget_is_exact(&mut db, &model, &mut rng, &format!("{context}, reopened"));
    }
}

impl MemoryStore {
    fn keys_locked(&self, objects: &BTreeMap<String, Vec<u8>>, prefix: &str) -> String {
        objects
            .keys()
            .find(|key| key.starts_with(prefix))
            .cloned()
            .unwrap()
    }
}

/// The NVMe cache holds posting blocks under their catalog digests: losing
/// or corrupting it changes no result, and a restart serves the same view.
#[test]
fn block_cache_loss_and_corruption_keep_clustered_results() {
    let seed = 0x3703_cac4_e105_0004_u64;
    let mut rng = Rng(seed);
    let store = MemoryStore::default();
    let mut db = open(&store, Metric::SquaredEuclidean);
    let mut model = Model::new();
    for batch in 0..20 {
        write(&mut db, &mut model, &mut rng, 80, METRIC);
        if batch % 5 == 4 {
            maintain(&mut db);
        }
    }
    db.convert_clustered(small(8)).unwrap();
    write(&mut db, &mut model, &mut rng, 50, METRIC);
    drop(db);
    let budget = ReadBudget {
        blocks: 12,
        requests: 4,
        bytes: 256 * 1024,
        local_blocks: 0,
    };
    let queries: Vec<_> = (0..20).map(|_| rng.vector()).collect();
    let cold: Vec<_> = {
        let db = open(&store, Metric::SquaredEuclidean);
        queries
            .iter()
            .map(|query| bits(&db.search_selective_within(query, 10, budget, &[]).unwrap()))
            .collect()
    };
    let temp = tempfile::tempdir().unwrap();
    let cache = temp.path().join("cache");
    let cached = || {
        open(&store, Metric::SquaredEuclidean)
            .with_block_cache(&cache, 0, 64 * 1024 * 1024)
            .unwrap()
    };
    let db = cached();
    while db.warm_cache_step(64 * 1024).unwrap() {}
    let stats = db.cache_stats().unwrap().unwrap();
    assert!(stats.warm_complete && stats.warm_bytes == stats.namespace_bytes);
    store.reads();
    for (query, expected) in queries.iter().zip(&cold) {
        let results = db.search_selective_within(query, 10, budget, &[]).unwrap();
        assert_eq!(&bits(&results), expected, "seed {seed:#x}");
    }
    // Warm queries read no remote ranges.
    assert_eq!(store.reads().0, 0, "seed {seed:#x}");
    drop(db);
    for entry in std::fs::read_dir(cache.join("glider-block-cache-v1")).unwrap() {
        let path = entry.unwrap().path();
        let mut bytes = std::fs::read(&path).unwrap();
        if let Some(byte) = bytes.get_mut(10) {
            *byte ^= 0xff;
        }
        std::fs::write(&path, bytes).unwrap();
    }
    let db = cached();
    for (query, expected) in queries.iter().zip(&cold) {
        let results = db.search_selective_within(query, 10, budget, &[]).unwrap();
        assert_eq!(&bits(&results), expected, "seed {seed:#x}");
    }
    assert!(db.cache_stats().unwrap().unwrap().corrupt_entries > 0);
    drop(db);
    std::fs::remove_dir_all(&cache).unwrap();
    let db = cached();
    for (query, expected) in queries.iter().zip(&cold) {
        let results = db.search_selective_within(query, 10, budget, &[]).unwrap();
        assert_eq!(&bits(&results), expected, "seed {seed:#x}");
    }
}

/// Backup copies the selected view, checked against the catalog digests.
#[test]
fn backup_carries_the_clustered_view() {
    let seed = 0x3703_bac0_0b0b_0005_u64;
    let mut rng = Rng(seed);
    let store = MemoryStore::default();
    let mut db = open(&store, Metric::SquaredEuclidean);
    let mut model = Model::new();
    for batch in 0..10 {
        write(&mut db, &mut model, &mut rng, 80, METRIC);
        if batch % 4 == 3 {
            maintain(&mut db);
        }
    }
    db.convert_clustered(small(4)).unwrap();
    write(&mut db, &mut model, &mut rng, 20, METRIC);
    drop(db);
    let temp = tempfile::tempdir().unwrap();
    let config = Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    };
    let mut serving = SegmentedServing::open(
        store.clone(),
        config,
        options(),
        SegmentedServingOptions {
            cache: None,
            ..SegmentedServingOptions::m21(temp.path().to_path_buf())
        },
    )
    .unwrap();
    let destination = MemoryStore::default();
    serving.backup_to(destination.clone()).unwrap();
    serving.close().unwrap();
    assert_eq!(destination.keys("sgcentroid-").len(), 1);
    assert_eq!(destination.keys("sgcluster-").len(), 1);
    let mut restored = open(&destination, Metric::SquaredEuclidean);
    assert_eq!(restored.clustered_epoch(), Some(1));
    assert_full_budget_is_exact(&mut restored, &model, &mut rng, &format!("seed {seed:#x}"));
}
