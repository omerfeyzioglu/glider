//! Automatic clustered conversion (M37) as idle `SegmentedServing`
//! maintenance: it starts once the sealed runs hold the configured number
//! of live rows, advances in bounded units between writes, seals and
//! queries, and publishes the view with one root. Full-budget queries equal
//! exact search before, during and after it; every create and remove of the
//! incremental conversion (and the seals between its steps) is a crash
//! point; a restart mid-conversion starts over safely; 0 disables it.

use glider::{
    admission::{Engine, Limits, Service, Shutdown},
    retry::{Request, RequestId},
    segmented::{
        ClusteringState, ConvertOptions, ReadBudget, SegmentedDatabase, SegmentedOptions,
        SegmentedServing, SegmentedServingOptions,
    },
    store::ObjectStore,
    Config, Error, Metric, Mutation, Neighbor, Result,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

const DIMENSIONS: usize = 8;
/// Live sealed rows that start a conversion in these tests.
const THRESHOLD: usize = 300;

fn config() -> Config {
    Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    }
}

/// Every block, request and byte: selective queries equal exact search.
fn full_budget() -> ReadBudget {
    ReadBudget {
        blocks: 1 << 20,
        requests: 1 << 20,
        bytes: usize::MAX,
        local_blocks: 0,
    }
}

/// Small seals and many bounded conversion steps: 4 centroids and 8 KiB
/// gather passes over about 2,000 rows of 8 dimensions.
fn serving_options(threshold: usize, seed: u64) -> SegmentedServingOptions {
    SegmentedServingOptions {
        seal_tail_objects: 4,
        read_budget: full_budget(),
        cleanup_objects: 4,
        max_index_bytes: 64 * 1024 * 1024,
        cache: None,
        query_threads: 2,
        warm_unit_bytes: 0,
        cluster_probes: usize::MAX,
        auto_cluster_rows: threshold,
        auto_cluster: ConvertOptions {
            centroids: Some(4),
            seed,
            gather_bytes: 8 * 1024,
        },
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }
    fn vector(&mut self) -> Vec<f32> {
        (0..DIMENSIONS)
            .map(|_| (self.next() % 1_000) as f32 / 10.)
            .collect()
    }
}

/// One write: puts of fresh and overwritten IDs below 2,000 and a delete.
fn mutations(rng: &mut Rng, ids: impl Fn(u64) -> u64) -> Vec<Mutation> {
    let mut mutations: Vec<Mutation> = (0..30)
        .map(|_| Mutation::Put {
            id: ids(rng.next() % 1_000),
            vector: rng.vector(),
            metadata: BTreeMap::new(),
        })
        .collect();
    mutations.push(Mutation::Delete {
        id: ids(rng.next() % 1_000),
    });
    mutations
}

type Model = BTreeMap<u64, Vec<f32>>;

fn apply(model: &mut Model, mutations: &[Mutation]) {
    for mutation in mutations {
        match mutation {
            Mutation::Put { id, vector, .. } => {
                model.insert(*id, vector.clone());
            }
            Mutation::Delete { id } => {
                model.remove(id);
            }
        }
    }
}

/// Exact top-k of the model: f64 squared distances, ties by ID.
fn oracle(model: &Model, query: &[f32], k: usize) -> Vec<(u64, u64)> {
    let mut all: Vec<_> = model
        .iter()
        .map(|(&id, vector)| {
            let distance: f64 = query
                .iter()
                .zip(vector)
                .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
                .sum();
            (distance.to_bits(), id)
        })
        .collect();
    all.sort_unstable();
    all.truncate(k);
    all
}

fn bits(neighbors: &[Neighbor]) -> Vec<(u64, u64)> {
    neighbors
        .iter()
        .map(|neighbor| (neighbor.distance.to_bits(), neighbor.id))
        .collect()
}

/// Mutating operations counted for fault injection, and an optional
/// failure of one of them or of one range read.
#[derive(Default)]
struct Plan {
    operations: usize,
    fail_at: Option<(usize, bool)>,
    fired: bool,
    range_reads: usize,
    fail_range_read: Option<usize>,
    range_delay: Option<Duration>,
}

/// Complete-object store in memory (creates never replace a key) with
/// injected faults: a create or remove fails before landing or after
/// (response lost), or one range read fails.
#[derive(Clone)]
struct TestStore {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    plan: Arc<Mutex<Plan>>,
}

impl TestStore {
    fn new() -> Self {
        Self {
            objects: Arc::default(),
            plan: Arc::default(),
        }
    }
    /// The same objects under a fresh plan.
    fn with_plan(&self, plan: Plan) -> Self {
        Self {
            objects: self.objects.clone(),
            plan: Arc::new(Mutex::new(plan)),
        }
    }
    fn count(&self, prefix: &str) -> usize {
        self.objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .count()
    }
    /// Whether this operation fails, and if so whether after landing.
    fn fault(&self) -> Option<bool> {
        let mut plan = self.plan.lock().unwrap();
        let index = plan.operations;
        plan.operations += 1;
        match plan.fail_at {
            Some((at, after)) if at == index => {
                plan.fired = true;
                Some(after)
            }
            _ => None,
        }
    }
}

fn injected() -> Error {
    Error::Io(std::io::Error::other("injected storage failure"))
}

impl ObjectStore for TestStore {
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
        let delay = {
            let mut plan = self.plan.lock().unwrap();
            let index = plan.range_reads;
            plan.range_reads += 1;
            if plan.fail_range_read == Some(index) {
                plan.fired = true;
                return Err(injected());
            }
            plan.range_delay
        };
        if let Some(delay) = delay {
            // Models remote read latency so conversion units overlap
            // concurrent requests.
            std::thread::sleep(delay);
        }
        let Some(bytes) = self.get(key)? else {
            return Ok(None);
        };
        if bytes.len() != payload_len || offset + length > payload_len {
            return Err(Error::Corrupt(format!("object length mismatch: {key}")));
        }
        Ok(Some(bytes[offset..offset + length].to_vec()))
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let fault = self.fault();
        if fault == Some(false) {
            return Err(injected());
        }
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        if fault == Some(true) {
            return Err(injected());
        }
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        let fault = self.fault();
        if fault == Some(false) {
            return Err(injected());
        }
        self.objects.lock().unwrap().remove(key);
        if fault == Some(true) {
            return Err(injected());
        }
        Ok(())
    }
}

fn open(store: &TestStore, threshold: usize, seed: u64) -> Result<SegmentedServing<TestStore>> {
    SegmentedServing::open(
        store.clone(),
        config(),
        SegmentedOptions::default(),
        serving_options(threshold, seed),
    )
}

fn request(nonce: u64, boundary: u64, mutations: Vec<Mutation>) -> Request {
    Request {
        id: RequestId {
            boundary,
            nonce: u128::from(nonce).to_le_bytes(),
        },
        conditions: Vec::new(),
        mutations,
    }
}

/// Full-budget queries through the serving engine equal exact search and
/// the model.
fn check_serving(
    serving: &mut SegmentedServing<TestStore>,
    model: &Model,
    rng: &mut Rng,
    context: &str,
) {
    for _ in 0..3 {
        let query = rng.vector();
        let exact = serving.database().search_exact(&query, 10, &[]).unwrap();
        assert_eq!(bits(&exact), oracle(model, &query, 10), "{context}: exact");
        let found = Engine::query(serving, &query, 10, &[]).unwrap();
        assert_eq!(bits(&found), bits(&exact), "{context}: selective");
    }
}

/// Reads, exact search and full-budget selective search of a reopened
/// namespace equal the model.
fn check_database(db: &SegmentedDatabase<TestStore>, model: &Model, rng: &mut Rng, context: &str) {
    assert!(db.clustered_view_error().is_none(), "{context}");
    for id in (0..2_000).step_by(7) {
        let found = db.get(id).unwrap().map(|document| document.vector);
        assert_eq!(found.as_ref(), model.get(&id), "{context}: id {id}");
    }
    for _ in 0..3 {
        let query = rng.vector();
        let expected = oracle(model, &query, 10);
        assert_eq!(
            bits(&db.search_exact(&query, 10, &[]).unwrap()),
            expected,
            "{context}: exact"
        );
        assert_eq!(
            bits(
                &db.search_selective_within(&query, 10, full_budget(), &[])
                    .unwrap()
            ),
            expected,
            "{context}: selective"
        );
    }
}

fn drain(serving: &mut SegmentedServing<TestStore>) {
    while serving.maintenance_step().unwrap() {}
}

/// Interleave writes with single maintenance units, checking full-budget
/// queries against exact search after every unit: the conversion starts
/// only once the sealed runs hold the threshold, seals publish between its
/// steps, and the view it publishes serves exactly after a reopen.
#[test]
fn conversion_starts_at_the_threshold_and_stays_exact_between_units() {
    let seed = 0x0a17_0c10_0001_u64;
    println!("seed {seed:#x}");
    let mut rng = Rng(seed);
    let store = TestStore::new();
    let mut serving = open(&store, THRESHOLD, seed).unwrap();
    let mut model = Model::new();
    let (mut started, mut seals_while_converting, mut converting_units) = (false, 0, 0);
    for round in 0..400_u64 {
        let context = format!("seed {seed:#x}, round {round}");
        let batch = mutations(&mut rng, |id| id);
        let boundary = serving.database().sequence();
        for outcome in serving.apply_requests(vec![request(round, boundary, batch.clone())]) {
            outcome.unwrap();
        }
        apply(&mut model, &batch);
        for unit in 0..3 {
            let before = serving.clustering();
            let seals = serving.counters().seal_starts;
            serving.maintenance_step().unwrap();
            let after = serving.clustering();
            match (&before, &after) {
                (ClusteringState::None, ClusteringState::Converting(_)) => {
                    assert!(
                        serving.database().sealed_live_rows() >= THRESHOLD,
                        "{context}: started below the threshold"
                    );
                    started = true;
                }
                (ClusteringState::None, ClusteringState::None) => {}
                (ClusteringState::Converting(_), ClusteringState::Converting(_)) => {
                    converting_units += 1;
                    seals_while_converting += serving.counters().seal_starts - seals;
                }
                (ClusteringState::Converting(_), ClusteringState::Clustered { epoch: 1 }) => {}
                (ClusteringState::Clustered { .. }, ClusteringState::Clustered { epoch: 1 }) => {}
                other => panic!("{context}: unexpected transition {other:?}"),
            }
            check_serving(
                &mut serving,
                &model,
                &mut rng,
                &format!("{context}, unit {unit}"),
            );
        }
        if matches!(serving.clustering(), ClusteringState::Clustered { .. }) && round > 150 {
            break;
        }
    }
    drain(&mut serving);
    assert!(started, "seed {seed:#x}: never started");
    assert_eq!(
        serving.clustering(),
        ClusteringState::Clustered { epoch: 1 }
    );
    assert!(
        seals_while_converting > 0 && converting_units > 20,
        "seed {seed:#x}: {seals_while_converting} seals in {converting_units} converting units"
    );
    let counters = serving.counters();
    assert_eq!((counters.conversion_starts, counters.conversions), (1, 1));
    assert_eq!(counters.conversion_failures, 0);
    check_serving(&mut serving, &model, &mut rng, "after conversion");
    serving.close().unwrap();
    assert_eq!(store.count("sgcentroid-"), 1);
    assert_eq!(store.count("sgcluster-"), 1);
    let db = SegmentedDatabase::open(store.clone(), config())
        .unwrap()
        .with_cluster_probes(usize::MAX);
    assert_eq!(db.clustered_epoch(), Some(1));
    check_database(&db, &model, &mut rng, "reopened");
}

/// Concurrent writers and readers through the admission queue while idle
/// maintenance converts the namespace: every query result equals the
/// exact top-10 of the acknowledged state at its sequence.
#[test]
fn admission_queries_equal_exact_search_before_during_and_after_conversion() {
    let seed = 0x0a17_0c10_0002_u64;
    println!("seed {seed:#x}");
    let store = TestStore::new().with_plan(Plan {
        range_delay: Some(Duration::from_millis(1)),
        ..Plan::default()
    });
    let serving = open(&store, THRESHOLD, seed).unwrap();
    let initial = serving.database().sequence();
    let service = Service::start(
        serving,
        Limits {
            commands: 64,
            bytes: 4 * 1024 * 1024,
            read_priority: None,
            queries: 4,
        },
    )
    .unwrap();
    let client = service.client();
    let writes = Arc::new(Mutex::new(Vec::<(u64, Vec<Mutation>)>::new()));
    let results = Arc::new(Mutex::new(
        Vec::<(u64, Vec<f32>, Vec<(u64, u64)>, bool)>::new(),
    ));
    let done = Arc::new(AtomicBool::new(false));
    let state = |client: &glider::admission::Client<SegmentedServing<TestStore>>| {
        let metrics = client.metrics().unwrap().wait().unwrap().value;
        metrics
            .samples
            .iter()
            .find(|(name, _)| *name == "glider_clustered_state")
            .map(|&(_, value)| value)
            .unwrap()
    };
    std::thread::scope(|scope| {
        for writer in 0..2_u64 {
            let (client, writes) = (client.clone(), writes.clone());
            scope.spawn(move || {
                let mut rng = Rng(seed ^ (writer + 1));
                for _ in 0..70 {
                    let batch = mutations(&mut rng, |id| id * 2 + writer);
                    let id = client.observe(0).unwrap().wait().unwrap().value.request_id;
                    let outcome = client
                        .write(Request {
                            id,
                            conditions: Vec::new(),
                            mutations: batch.clone(),
                        })
                        .unwrap()
                        .wait()
                        .unwrap()
                        .value;
                    writes.lock().unwrap().push((outcome.sequence, batch));
                }
            });
        }
        for reader in 0..3_u64 {
            let (client, results, done) = (client.clone(), results.clone(), done.clone());
            scope.spawn(move || {
                let mut rng = Rng(seed ^ (reader + 100));
                while !done.load(Ordering::Acquire) {
                    let query = rng.vector();
                    let before = state(&client);
                    let result = client
                        .query(query.clone(), 10, Vec::new())
                        .unwrap()
                        .wait()
                        .unwrap()
                        .value;
                    let during = before == 1 && state(&client) == 1;
                    results.lock().unwrap().push((
                        result.sequence,
                        query,
                        bits(&result.neighbors),
                        during,
                    ));
                }
            });
        }
        // Wait for the writers' acknowledgements and the published view.
        let deadline = Instant::now() + Duration::from_secs(120);
        while writes.lock().unwrap().len() < 140 || state(&client) != 2 {
            assert!(
                Instant::now() < deadline,
                "seed {seed:#x}: conversion did not finish"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        done.store(true, Ordering::Release);
    });
    let mut writes = std::mem::take(&mut *writes.lock().unwrap());
    writes.sort_by_key(|(sequence, _)| *sequence);
    let mut results = std::mem::take(&mut *results.lock().unwrap());
    results.sort_by_key(|(sequence, ..)| *sequence);
    let (mut model, mut next, mut during) = (Model::new(), 0, 0);
    for (sequence, query, found, converting) in &results {
        while next < writes.len() && writes[next].0 <= *sequence {
            apply(&mut model, &writes[next].1);
            next += 1;
        }
        assert!(*sequence >= initial);
        assert_eq!(
            found,
            &oracle(&model, query, 10),
            "seed {seed:#x}: query at sequence {sequence}"
        );
        during += usize::from(*converting);
    }
    let first = results.first().map(|result| result.0);
    assert!(
        during > 0,
        "seed {seed:#x}: no query ran during the conversion"
    );
    assert!(
        first < Some(writes[writes.len() / 2].0),
        "seed {seed:#x}: no early query"
    );
    let final_metrics = client.metrics().unwrap().wait().unwrap().value;
    let sample = |name: &str| {
        final_metrics
            .samples
            .iter()
            .find(|(sample, _)| *sample == name)
            .map(|&(_, value)| value)
            .unwrap()
    };
    assert_eq!(sample("glider_conversions_total"), 1);
    assert_eq!(sample("glider_clustered_epoch"), 1);
    assert_eq!(client.status().maintenance_errors, 0);
    service.shutdown(Shutdown::Drain).unwrap();
}

/// The fixed workload of the crash matrix: writes interleaved with two
/// maintenance units each, so seals and cleanup run between conversion steps,
/// then idle maintenance to completion. Stops at the first error and
/// returns the acknowledged model, its sequence, an uncertain write and
/// the seals started while a conversion was staged.
fn crash_run(
    serving: &mut SegmentedServing<TestStore>,
    seed: u64,
) -> (Model, u64, Option<Vec<Mutation>>, u64) {
    let mut rng = Rng(seed);
    let mut model = Model::new();
    let mut seals = 0;
    for round in 0..60_u64 {
        let batch = mutations(&mut rng, |id| id % 400);
        let boundary = serving.database().sequence();
        let outcome = serving
            .apply_requests(vec![request(round, boundary, batch.clone())])
            .pop()
            .unwrap();
        if outcome.is_err() {
            return (model, boundary, Some(batch), seals);
        }
        apply(&mut model, &batch);
        for _ in 0..2 {
            let converting = matches!(serving.clustering(), ClusteringState::Converting(_));
            let before = serving.counters().seal_starts;
            if serving.maintenance_step().is_err() {
                return (model, serving.database().sequence(), None, seals);
            }
            if converting {
                seals += serving.counters().seal_starts - before;
            }
        }
    }
    loop {
        match serving.maintenance_step() {
            Ok(true) => {}
            Ok(false) => break,
            Err(_) => break,
        }
    }
    (model, serving.database().sequence(), None, seals)
}

/// Every create and remove of the workload (logs, seals between conversion
/// steps, the centroid object, posting packs, catalog, conversion root and
/// cleanup) fails before landing or after: reopening keeps the
/// acknowledged state, the previous root serves until the conversion's
/// root, and a new serving handle converts again and removes the orphans.
#[test]
fn every_incremental_conversion_failure_recovers_and_converts_again() {
    let seed = 0x0a17_0c10_0003_u64;
    println!("seed {seed:#x}");
    let threshold = 150;
    let clean = TestStore::new();
    let mut serving = open(&clean, threshold, seed).unwrap();
    let (model, _, _, seals) = crash_run(&mut serving, seed);
    let counters = serving.counters();
    check_serving(&mut serving, &model, &mut Rng(seed), "clean run");
    assert!(seals > 0, "no seal between conversion steps: {counters:?}");
    assert_eq!(
        serving.clustering(),
        ClusteringState::Clustered { epoch: 1 }
    );
    assert_eq!(counters.conversions, 1, "{counters:?}");
    serving.close().unwrap();
    let operations = clean.plan.lock().unwrap().operations;
    assert!(operations > 100, "workload too small: {operations}");
    let stride: usize = std::env::var("GLIDER_CRASH_STRIDE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let mut rng = Rng(seed ^ 0xc4ec);
    for at in (0..operations).step_by(stride.max(1)) {
        for after in [false, true] {
            let context = format!("seed {seed:#x}, operation {at} of {operations}, after {after}");
            let store = TestStore::new().with_plan(Plan {
                fail_at: Some((at, after)),
                ..Plan::default()
            });
            let (mut acknowledged, sequence, uncertain, _) = match open(&store, threshold, seed) {
                Ok(mut serving) => crash_run(&mut serving, seed),
                Err(_) => (Model::new(), 0, None, 0),
            };
            assert!(
                store.plan.lock().unwrap().fired,
                "{context}: fault not reached"
            );
            let store = store.with_plan(Plan::default());
            let db = SegmentedDatabase::open(store.clone(), config())
                .unwrap()
                .with_cluster_probes(usize::MAX);
            if let Some(batch) = uncertain {
                if db.sequence() > sequence {
                    apply(&mut acknowledged, &batch);
                }
            }
            check_database(&db, &acknowledged, &mut rng, &context);
            drop(db);
            // A new owner resumes: any interrupted conversion starts over
            // and its orphans are removed.
            let mut serving = open(&store, threshold, seed).unwrap();
            drain(&mut serving);
            let live = serving.database().sealed_live_rows();
            if live >= threshold {
                assert!(
                    matches!(serving.clustering(), ClusteringState::Clustered { .. }),
                    "{context}: {:?}",
                    serving.clustering()
                );
            } else {
                assert_eq!(serving.clustering(), ClusteringState::None, "{context}");
            }
            check_serving(&mut serving, &acknowledged, &mut rng, &context);
            serving.close().unwrap();
            let views = usize::from(live >= threshold);
            assert_eq!(store.count("sgcentroid-"), views, "{context}");
            assert_eq!(store.count("sgcluster-"), views, "{context}");
            let db = SegmentedDatabase::open(store.clone(), config())
                .unwrap()
                .with_cluster_probes(usize::MAX);
            check_database(
                &db,
                &acknowledged,
                &mut rng,
                &format!("{context}, reopened"),
            );
        }
    }
}

/// A handle stopped mid-conversion (after it staged posting packs) leaves
/// the per-seal root serving; the next owner starts a new attempt, which
/// publishes, and cleanup removes the first attempt's orphans.
#[test]
fn restart_mid_conversion_starts_over_and_removes_orphans() {
    let seed = 0x0a17_0c10_0004_u64;
    println!("seed {seed:#x}");
    let mut rng = Rng(seed);
    let store = TestStore::new();
    let mut serving = open(&store, THRESHOLD, seed).unwrap();
    let mut model = Model::new();
    let mut round = 0;
    while serving.database().sealed_live_rows() < THRESHOLD {
        let batch = mutations(&mut rng, |id| id);
        let boundary = serving.database().sequence();
        serving
            .apply_requests(vec![request(round, boundary, batch.clone())])
            .pop()
            .unwrap()
            .unwrap();
        apply(&mut model, &batch);
        round += 1;
        while serving.clustering() == ClusteringState::None && serving.maintenance_step().unwrap() {
        }
    }
    // Run until the attempt has created posting packs, then stop without
    // closing, as a crash would.
    loop {
        if let ClusteringState::Converting(progress) = serving.clustering() {
            if progress.posting_packs >= 2 {
                break;
            }
        }
        assert!(serving.maintenance_step().unwrap(), "seed {seed:#x}");
    }
    assert_eq!(store.count("sgcentroid-"), 1);
    let staged_packs = store.count("sgpack-");
    drop(serving);
    let db = SegmentedDatabase::open(store.clone(), config()).unwrap();
    assert_eq!(db.clustered_epoch(), None, "the old root still serves");
    check_database(
        &db.with_cluster_probes(usize::MAX),
        &model,
        &mut rng,
        &format!("seed {seed:#x}: reopened mid-conversion"),
    );
    let mut serving = open(&store, THRESHOLD, seed).unwrap();
    drain(&mut serving);
    assert_eq!(
        serving.clustering(),
        ClusteringState::Clustered { epoch: 1 }
    );
    assert_eq!(serving.counters().conversion_starts, 1);
    check_serving(&mut serving, &model, &mut rng, "converted after restart");
    serving.close().unwrap();
    assert_eq!(store.count("sgcentroid-"), 1);
    assert_eq!(store.count("sgcluster-"), 1);
    assert!(store.count("sgpack-") > 0 && staged_packs > 0);
}

/// A failed range read keeps the conversion staged: the unit reports the
/// error without poisoning, and the next unit retries the same read.
#[test]
fn failed_conversion_read_is_retried_without_restarting() {
    let seed = 0x0a17_0c10_0005_u64;
    println!("seed {seed:#x}");
    let mut rng = Rng(seed);
    let store = TestStore::new();
    let mut serving = open(&store, THRESHOLD, seed).unwrap();
    let mut model = Model::new();
    let mut round = 0;
    while serving.clustering() == ClusteringState::None {
        let batch = mutations(&mut rng, |id| id);
        let boundary = serving.database().sequence();
        serving
            .apply_requests(vec![request(round, boundary, batch.clone())])
            .pop()
            .unwrap()
            .unwrap();
        apply(&mut model, &batch);
        round += 1;
        while serving.clustering() == ClusteringState::None && serving.maintenance_step().unwrap() {
        }
    }
    serving.close().unwrap();
    // Reopen with the next conversion read failing.
    let store = store.with_plan(Plan::default());
    let mut serving = open(&store, THRESHOLD, seed).unwrap();
    loop {
        serving.maintenance_step().unwrap();
        if let ClusteringState::Converting(progress) = serving.clustering() {
            if progress.phase == "sample" {
                break;
            }
        }
    }
    let reads = store.plan.lock().unwrap().range_reads;
    store.plan.lock().unwrap().fail_range_read = Some(reads);
    let before = serving.clustering();
    assert!(serving.maintenance_step().is_err(), "seed {seed:#x}");
    assert!(store.plan.lock().unwrap().fired);
    assert!(!serving.database().is_poisoned());
    assert_eq!(
        serving.clustering(),
        before,
        "seed {seed:#x}: state changed"
    );
    drain(&mut serving);
    let counters = serving.counters();
    assert_eq!(
        serving.clustering(),
        ClusteringState::Clustered { epoch: 1 }
    );
    assert_eq!(
        (counters.conversion_starts, counters.conversion_failures),
        (1, 0)
    );
    check_serving(&mut serving, &model, &mut rng, &format!("seed {seed:#x}"));
    serving.close().unwrap();
}

/// `auto_cluster_rows = 0` never converts, however many rows are sealed;
/// an explicit conversion still works.
#[test]
fn zero_threshold_disables_automatic_conversion() {
    let seed = 0x0a17_0c10_0006_u64;
    println!("seed {seed:#x}");
    let mut rng = Rng(seed);
    let store = TestStore::new();
    let mut serving = open(&store, 0, seed).unwrap();
    let mut model = Model::new();
    for round in 0..60 {
        let batch = mutations(&mut rng, |id| id);
        let boundary = serving.database().sequence();
        serving
            .apply_requests(vec![request(round, boundary, batch.clone())])
            .pop()
            .unwrap()
            .unwrap();
        apply(&mut model, &batch);
        drain(&mut serving);
    }
    assert!(serving.database().sealed_live_rows() >= 2 * THRESHOLD);
    assert_eq!(serving.clustering(), ClusteringState::None);
    assert_eq!(serving.counters().conversion_starts, 0);
    assert_eq!(store.count("sgcentroid-"), 0);
    check_serving(&mut serving, &model, &mut rng, &format!("seed {seed:#x}"));
    let summary = serving
        .convert_clustered(ConvertOptions {
            centroids: Some(4),
            seed,
            gather_bytes: 8 * 1024,
        })
        .unwrap();
    assert_eq!(summary.epoch, 1);
    assert_eq!(
        serving.clustering(),
        ClusteringState::Clustered { epoch: 1 }
    );
    check_serving(
        &mut serving,
        &model,
        &mut rng,
        &format!("seed {seed:#x}, explicit"),
    );
    serving.close().unwrap();
}
