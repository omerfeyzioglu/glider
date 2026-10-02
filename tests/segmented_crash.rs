//! Crash-point matrix: fail every object create and remove of a fixed
//! workload, before it lands or after (response lost), then reopen and check
//! the acknowledged state against a model and exact search. A second
//! workload converts the namespace to a clustered view (M37) twice, so every
//! staged posting pack, centroid, catalog and root create is a crash point.
//! A third converts early and then writes group-commit requests through
//! clustered seals and posting merges; it also delays DELETEs until the run
//! ends and checks a backup restored from each recovered namespace.

use glider::{
    retry::{Request, RequestId},
    segmented::{
        ConvertOptions, ReadBudget, SegmentedDatabase, SegmentedOptions, SegmentedServing,
        SegmentedServingOptions,
    },
    store::ObjectStore,
    Config, Error, Metric, Mutation, Result,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

const DIMENSIONS: usize = 8;

fn config() -> Config {
    Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    }
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    Before,
    After,
    /// A remove that reports success but lands only when the run ends.
    Delayed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Plain,
    /// Convert midway, then rebuild the view as a new epoch.
    Convert,
    /// Convert early, then group commits, clustered seals and merges.
    Clustered,
}

#[derive(Default)]
struct Plan {
    operations: usize,
    /// Whether each counted operation was a remove.
    removes: Vec<bool>,
    fail_at: Option<(usize, Fault)>,
    fired: bool,
    delayed: Vec<String>,
}

struct FaultStore<S> {
    inner: S,
    plan: Arc<Mutex<Plan>>,
}

impl<S> FaultStore<S> {
    /// Count one mutating operation and report the fault to inject, if any.
    fn next(&self, remove: bool) -> Option<Fault> {
        let mut plan = self.plan.lock().unwrap();
        let index = plan.operations;
        plan.operations += 1;
        plan.removes.push(remove);
        match plan.fail_at {
            Some((at, fault)) if at == index => {
                plan.fired = true;
                Some(fault)
            }
            _ => None,
        }
    }
}

fn injected() -> Error {
    Error::Io(std::io::Error::other("injected storage failure"))
}

impl<S: ObjectStore> ObjectStore for FaultStore<S> {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.inner.get_range(key, offset, length, payload_len)
    }
    fn list(&self) -> Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        match self.next(false) {
            Some(Fault::Before | Fault::Delayed) => Err(injected()),
            Some(Fault::After) => {
                self.inner.create(key, value)?;
                Err(injected())
            }
            None => self.inner.create(key, value),
        }
    }
    fn remove(&self, key: &str) -> Result<()> {
        match self.next(true) {
            Some(Fault::Before) => Err(injected()),
            Some(Fault::After) => {
                self.inner.remove(key)?;
                Err(injected())
            }
            Some(Fault::Delayed) => {
                self.plan.lock().unwrap().delayed.push(key.into());
                Ok(())
            }
            None => self.inner.remove(key),
        }
    }
    fn remove_many(&self, keys: &[String]) -> Result<()> {
        for key in keys {
            self.remove(key)?;
        }
        Ok(())
    }
}

fn open<S: ObjectStore>(
    inner: S,
    plan: &Arc<Mutex<Plan>>,
) -> Result<SegmentedDatabase<FaultStore<S>>> {
    Ok(SegmentedDatabase::open(
        FaultStore {
            inner,
            plan: plan.clone(),
        },
        config(),
    )?
    .with_reclaim_min_garbage(1)
    .with_cluster_probes(usize::MAX))
}

/// A small view: several gather passes and posting packs for this workload.
fn conversion() -> ConvertOptions {
    ConvertOptions {
        centroids: Some(3),
        seed: 11,
        gather_bytes: 16 * 1024,
    }
}

fn vector(seed: u64) -> Vec<f32> {
    (0..DIMENSIONS as u64)
        .map(|axis| ((seed * 2_654_435_761 + axis * 40_503) % 1_000) as f32 / 10.)
        .collect()
}

/// The fixed workload: overwrite-heavy batches with deletes, then every kind
/// of maintenance. Returns the request batches in order.
fn batches() -> Vec<Vec<Mutation>> {
    (0..24_u64)
        .map(|batch| {
            (0..60_u64)
                .map(|n| {
                    let id = (batch * 37 + n * 11) % 400;
                    if (batch + n) % 9 == 0 {
                        Mutation::Delete { id }
                    } else {
                        Mutation::Put {
                            id,
                            vector: vector(batch * 1_000 + n),
                            metadata: BTreeMap::new(),
                        }
                    }
                })
                .collect()
        })
        .collect()
}

type Model = BTreeMap<u64, Option<Vec<f32>>>;

thread_local! {
    /// Merge rounds the workload published on this test thread.
    static MERGES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn apply(model: &mut Model, mutations: &[Mutation]) {
    for mutation in mutations {
        match mutation {
            Mutation::Put { id, vector, .. } => model.insert(*id, Some(vector.clone())),
            Mutation::Delete { id } => model.insert(*id, None),
        };
    }
}

/// The requests of batch `index`: one, or two sharing a group-commit log in
/// the clustered workload.
fn requests(index: usize, boundary: u64, mutations: &[Mutation], mode: Mode) -> Vec<Request> {
    let parts: Vec<&[Mutation]> = if mode == Mode::Clustered {
        let (first, second) = mutations.split_at(mutations.len() / 2);
        vec![first, second]
    } else {
        vec![mutations]
    };
    parts
        .into_iter()
        .enumerate()
        .map(|(part, mutations)| Request {
            id: RequestId {
                boundary,
                nonce: (index as u128 * 2 + part as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations: mutations.to_vec(),
        })
        .collect()
}

/// Run writes and maintenance until the first error. Returns the model of
/// acknowledged state, the sequence it corresponds to and the requests whose
/// outcome is uncertain, if the failure hit a write.
fn run<S: ObjectStore>(
    db: &mut SegmentedDatabase<FaultStore<S>>,
    mode: Mode,
) -> (Model, u64, Option<Vec<Request>>) {
    let mut model = Model::new();
    for (index, mutations) in batches().into_iter().enumerate() {
        let group = requests(index, db.sequence(), &mutations, mode);
        if db
            .apply_requests(group.clone())
            .iter()
            .any(|result| result.is_err())
        {
            return (model, db.sequence(), Some(group));
        }
        apply(&mut model, &mutations);
        let maintenance: Result<()> = (|| {
            let seal = match mode {
                Mode::Clustered => index % 2 == 1,
                _ => index % 5 == 4,
            };
            if seal {
                db.seal_delta()?;
                while db.merge_postings()?.is_some() {
                    MERGES.with(|merges| merges.set(merges.get() + 1));
                }
                while db.consolidate_runs_step()? {}
            }
            if index % 8 == 7 {
                if db.start_prune()? {
                    while db.prune_step()? {}
                }
                while db.reclaim_pack_step()? {}
                while db.cleanup_step(3)? > 0 {}
            }
            let convert = match mode {
                Mode::Plain => false,
                Mode::Convert => index == 11 || index == 23,
                Mode::Clustered => index == 1,
            };
            if convert {
                db.convert_clustered(conversion())?;
                while db.cleanup_step(3)? > 0 {}
            }
            Ok(())
        })();
        if maintenance.is_err() {
            return (model, db.sequence(), None);
        }
    }
    (model, db.sequence(), None)
}

fn check<S: ObjectStore>(db: &SegmentedDatabase<S>, model: &Model, context: &str) {
    for (&id, expected) in model {
        let found = db.get(id).unwrap().map(|document| document.vector);
        assert_eq!(&found, expected, "{context}: id {id}");
    }
    for seed in 0..4 {
        let query = vector(seed * 7_919);
        let mut expected: Vec<_> = model
            .iter()
            .filter_map(|(&id, vector)| {
                let vector = vector.as_ref()?;
                let distance: f64 = query
                    .iter()
                    .zip(vector)
                    .map(|(a, b)| (f64::from(*a) - f64::from(*b)).powi(2))
                    .sum();
                Some((distance.to_bits(), id))
            })
            .collect();
        expected.sort_unstable();
        expected.truncate(10);
        let found: Vec<_> = db
            .search_exact(&query, 10, &[])
            .unwrap()
            .into_iter()
            .map(|neighbor| (neighbor.distance.to_bits(), neighbor.id))
            .collect();
        assert_eq!(found, expected, "{context}: exact query {seed}");
        // Every routable block within budget: equal to exact search.
        let budget = ReadBudget {
            blocks: db.block_count().max(1),
            requests: usize::MAX,
            bytes: usize::MAX,
            local_blocks: 0,
        };
        assert!(db.clustered_view_error().is_none(), "{context}");
        let selective: Vec<_> = db
            .search_selective_within(&query, 10, budget, &[])
            .unwrap()
            .into_iter()
            .map(|neighbor| (neighbor.distance.to_bits(), neighbor.id))
            .collect();
        assert_eq!(selective, expected, "{context}: selective query {seed}");
    }
}

/// Run the matrix for every `stride`-th mutating operation (both fault
/// modes). `store(case)` opens a fresh, empty namespace for each case name.
fn matrix<S: ObjectStore>(store: impl Fn(&str) -> S, stride: usize, mode: Mode) {
    let convert = mode != Mode::Plain;
    let plan = Arc::new(Mutex::new(Plan::default()));
    let (model, ..) = run(&mut open(store("clean"), &plan).unwrap(), mode);
    let operations = plan.lock().unwrap().operations;
    let removes = plan.lock().unwrap().removes.clone();
    assert!(
        operations > 50,
        "workload too small: {operations} operations"
    );
    let clean = open(store("clean"), &plan).unwrap();
    let epochs = match mode {
        Mode::Plain => None,
        Mode::Convert => Some(2),
        Mode::Clustered => Some(1),
    };
    assert_eq!(clean.clustered_epoch(), epochs);
    if mode == Mode::Clustered {
        let layout = clean.clustered_layout().unwrap();
        assert!(layout.canonical_extents > 0, "{layout:?}");
        assert!(MERGES.with(std::cell::Cell::get) > 0, "no merge round ran");
    }
    check(&clean, &model, "clean run");
    drop(clean);
    for at in (0..operations).step_by(stride.max(1)) {
        let faults: &[Fault] = if mode == Mode::Clustered && removes[at] {
            &[Fault::Before, Fault::After, Fault::Delayed]
        } else {
            &[Fault::Before, Fault::After]
        };
        for &fault in faults {
            let context = format!("operation {at} of {operations}, {fault:?}");
            let case = format!("case-{at}-{fault:?}");
            let plan = Arc::new(Mutex::new(Plan {
                fail_at: Some((at, fault)),
                ..Plan::default()
            }));
            // A fault may hit namespace creation inside the first open;
            // then nothing was acknowledged and the next open must succeed.
            let (mut acknowledged, sequence, uncertain) = match open(store(&case), &plan) {
                Ok(mut db) => run(&mut db, mode),
                Err(_) => (Model::new(), 0, None),
            };
            assert!(plan.lock().unwrap().fired, "{context}: fault not reached");
            // Delayed DELETEs land after everything the run did.
            let late = std::mem::take(&mut plan.lock().unwrap().delayed);
            for key in late {
                store(&case).remove(&key).unwrap();
            }
            let mut db = open(store(&case), &plan).unwrap();
            // An uncertain group is all-or-nothing, visible by its sequences
            // and its retry receipts; if it is absent, a retry with the same
            // request IDs commits it.
            if let Some(group) = uncertain {
                let lookups: Vec<_> = group
                    .iter()
                    .map(|request| db.lookup_request(request.id).unwrap())
                    .collect();
                let mutations: Vec<Mutation> = group
                    .iter()
                    .flat_map(|request| request.mutations.clone())
                    .collect();
                if db.sequence() == sequence + group.len() as u64 {
                    assert!(
                        lookups
                            .iter()
                            .all(|lookup| matches!(lookup, glider::retry::Lookup::Retained(_))),
                        "{context}"
                    );
                } else {
                    assert_eq!(db.sequence(), sequence, "{context}");
                    assert!(
                        lookups
                            .iter()
                            .all(|lookup| *lookup == glider::retry::Lookup::Unknown),
                        "{context}"
                    );
                    for result in db.apply_requests(group.clone()) {
                        result.unwrap();
                    }
                }
                apply(&mut acknowledged, &mutations);
                // Retrying again publishes nothing.
                let committed = db.sequence();
                for result in db.apply_requests(group) {
                    result.unwrap();
                }
                assert_eq!(db.sequence(), committed, "{context}");
            } else {
                assert_eq!(db.sequence(), sequence, "{context}");
            }
            check(&db, &acknowledged, &context);
            // Maintenance resumes and reclaims after reopen without faults.
            db.seal_delta().unwrap();
            while db.merge_postings().unwrap().is_some() {}
            while db.consolidate_runs_step().unwrap() {}
            if db.start_prune().unwrap() {
                while db.prune_step().unwrap() {}
            }
            while db.reclaim_pack_step().unwrap() {}
            while db.cleanup_step(16).unwrap() > 0 {}
            if mode == Mode::Clustered && db.clustered_epoch().is_some() {
                // Seals and merges continue on the recovered view.
                let layout = db.clustered_layout().unwrap();
                assert_eq!(layout.uncovered_packs, 0, "{context}: {layout:?}");
                assert!(layout.max_small_extents <= 3, "{context}: {layout:?}");
            } else if convert && acknowledged.values().any(Option::is_some) {
                let epoch = db.clustered_epoch().unwrap_or(0);
                assert_eq!(db.convert_clustered(conversion()).unwrap().epoch, epoch + 1);
                while db.cleanup_step(16).unwrap() > 0 {}
            } else if convert {
                // Nothing sealed and live: there is nothing to convert.
                assert!(matches!(
                    db.convert_clustered(conversion()),
                    Err(Error::Invalid(_))
                ));
            }
            check(&db, &acknowledged, &format!("{context}, after maintenance"));
            drop(db);
            if convert {
                // Cleanup removed every staged orphan and replaced view.
                let keys = store(&case).list().unwrap();
                let views = usize::from(acknowledged.values().any(Option::is_some));
                for prefix in ["sgcentroid-", "sgcluster-"] {
                    let count = keys.iter().filter(|key| key.starts_with(prefix)).count();
                    assert_eq!(count, views, "{context}: {prefix}");
                }
            }
            let db = open(store(&case), &plan).unwrap();
            check(&db, &acknowledged, &format!("{context}, reopened"));
            drop(db);
            if mode == Mode::Clustered {
                // A backup of the recovered namespace restores the same view.
                let temp = tempfile::tempdir().unwrap();
                let mut serving = SegmentedServing::open(
                    store(&case),
                    config(),
                    SegmentedOptions::default(),
                    SegmentedServingOptions {
                        cache: None,
                        ..SegmentedServingOptions::m21(temp.path().to_path_buf())
                    },
                )
                .unwrap();
                let destination = MemoryStore::default();
                serving.backup_to(destination.clone()).unwrap();
                serving.close().unwrap();
                let restored = SegmentedDatabase::open(destination, config())
                    .unwrap()
                    .with_cluster_probes(usize::MAX);
                check(&restored, &acknowledged, &format!("{context}, restored"));
            }
        }
    }
}

/// Cases cover every `stride`-th operation; `GLIDER_CRASH_STRIDE` overrides
/// the default (every operation in memory, a sample on MinIO).
fn stride(default: usize) -> usize {
    std::env::var("GLIDER_CRASH_STRIDE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// Complete-object store in memory: create publishes all bytes at once and
/// never replaces a key; listing is complete. Cases share it across reopens.
#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

impl ObjectStore for MemoryStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.0.lock().unwrap().keys().cloned().collect())
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut objects = self.0.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

/// The engine's crash semantics over the object contract; the local
/// filesystem backend's own publication protocol has separate tests.
#[test]
fn every_create_and_remove_failure_recovers_acknowledged_state() {
    let stores = Mutex::new(BTreeMap::<String, MemoryStore>::new());
    matrix(
        |case| {
            stores
                .lock()
                .unwrap()
                .entry(case.into())
                .or_default()
                .clone()
        },
        stride(1),
        Mode::Plain,
    );
}

/// The same matrix over a workload that converts to a clustered view and
/// later rebuilds it: interrupted conversions leave the previous root
/// serving and only orphans behind, which cleanup removes.
#[test]
fn every_conversion_create_and_remove_failure_recovers_acknowledged_state() {
    let stores = Mutex::new(BTreeMap::<String, MemoryStore>::new());
    matrix(
        |case| {
            stores
                .lock()
                .unwrap()
                .entry(case.into())
                .or_default()
                .clone()
        },
        stride(1),
        Mode::Convert,
    );
}

/// The clustered workload: every create and remove of a conversion, the
/// clustered seals (packs, index, catalog, root), the merge rounds (packs,
/// catalog, root) and cleanup fails before or after landing, and every
/// remove is also delayed until the run ends. Recovery keeps acknowledged
/// state, group outcomes are all-or-nothing and retryable, maintenance and
/// merges resume, and a backup of the result restores it.
#[test]
fn every_clustered_seal_and_merge_failure_recovers_acknowledged_state() {
    let stores = Mutex::new(BTreeMap::<String, MemoryStore>::new());
    matrix(
        |case| {
            stores
                .lock()
                .unwrap()
                .entry(case.into())
                .or_default()
                .clone()
        },
        stride(1),
        Mode::Clustered,
    );
}

/// The same matrix against S3-compatible storage (run by `tools/test_s3.py`
/// with a disposable MinIO; skipped otherwise).
#[cfg(feature = "s3")]
#[test]
#[ignore]
fn minio_every_sampled_failure_recovers_acknowledged_state() {
    use glider::store::s3::{AmazonS3Builder, S3Store};
    let mut random = [0_u8; 8];
    getrandom::getrandom(&mut random).unwrap();
    let prefix = format!("crash-{}", u64::from_le_bytes(random));
    matrix(
        |case| {
            S3Store::open(
                AmazonS3Builder::new()
                    .with_bucket_name(std::env::var("GLIDER_S3_BUCKET").unwrap())
                    .with_region("us-east-1")
                    .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID").unwrap())
                    .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY").unwrap())
                    .with_endpoint(std::env::var("GLIDER_S3_ENDPOINT").unwrap())
                    .with_allow_http(true),
                &format!("{prefix}-{case}"),
            )
            .unwrap()
        },
        stride(7),
        Mode::Plain,
    );
}
