//! Crash-point matrix: fail every object create and remove of a fixed
//! workload, before it lands or after (response lost), then reopen and check
//! the acknowledged state against a model and exact search.

use glider::{
    retry::{Request, RequestId},
    segmented::SegmentedDatabase,
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
}

#[derive(Default)]
struct Plan {
    operations: usize,
    fail_at: Option<(usize, Fault)>,
    fired: bool,
}

struct FaultStore<S> {
    inner: S,
    plan: Arc<Mutex<Plan>>,
}

impl<S> FaultStore<S> {
    /// Count one mutating operation and report the fault to inject, if any.
    fn next(&self) -> Option<Fault> {
        let mut plan = self.plan.lock().unwrap();
        let index = plan.operations;
        plan.operations += 1;
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
        match self.next() {
            Some(Fault::Before) => Err(injected()),
            Some(Fault::After) => {
                self.inner.create(key, value)?;
                Err(injected())
            }
            None => self.inner.create(key, value),
        }
    }
    fn remove(&self, key: &str) -> Result<()> {
        match self.next() {
            Some(Fault::Before) => Err(injected()),
            Some(Fault::After) => {
                self.inner.remove(key)?;
                Err(injected())
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
    .with_reclaim_min_garbage(1))
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

fn apply(model: &mut Model, mutations: &[Mutation]) {
    for mutation in mutations {
        match mutation {
            Mutation::Put { id, vector, .. } => model.insert(*id, Some(vector.clone())),
            Mutation::Delete { id } => model.insert(*id, None),
        };
    }
}

/// Run writes and maintenance until the first error. Returns the model of
/// acknowledged state, the sequence it corresponds to and the batch whose
/// outcome is uncertain, if the failure hit a write.
fn run<S: ObjectStore>(
    db: &mut SegmentedDatabase<FaultStore<S>>,
) -> (Model, u64, Option<Vec<Mutation>>) {
    let mut model = Model::new();
    for (index, mutations) in batches().into_iter().enumerate() {
        let request = Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: (index as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations: mutations.clone(),
        };
        if db.apply_request(request).is_err() {
            return (model, db.sequence(), Some(mutations));
        }
        apply(&mut model, &mutations);
        let maintenance: Result<()> = (|| {
            if index % 5 == 4 {
                db.seal_delta()?;
                while db.consolidate_runs_step()? {}
            }
            if index % 8 == 7 {
                if db.start_prune()? {
                    while db.prune_step()? {}
                }
                while db.reclaim_pack_step()? {}
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

fn check<S: ObjectStore>(db: &SegmentedDatabase<FaultStore<S>>, model: &Model, context: &str) {
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
        let selective: Vec<_> = db
            .search_selective(&query, 10, db.block_count().max(1), &[])
            .unwrap()
            .into_iter()
            .map(|neighbor| (neighbor.distance.to_bits(), neighbor.id))
            .collect();
        assert_eq!(selective, expected, "{context}: selective query {seed}");
    }
}

/// Run the matrix for every `stride`-th mutating operation (both fault
/// modes). `store(case)` opens a fresh, empty namespace for each case name.
fn matrix<S: ObjectStore>(store: impl Fn(&str) -> S, stride: usize) {
    let plan = Arc::new(Mutex::new(Plan::default()));
    let (model, ..) = run(&mut open(store("clean"), &plan).unwrap());
    let operations = plan.lock().unwrap().operations;
    assert!(
        operations > 50,
        "workload too small: {operations} operations"
    );
    check(&open(store("clean"), &plan).unwrap(), &model, "clean run");
    for at in (0..operations).step_by(stride.max(1)) {
        for fault in [Fault::Before, Fault::After] {
            let context = format!("operation {at} of {operations}, {fault:?}");
            let case = format!("case-{at}-{fault:?}");
            let plan = Arc::new(Mutex::new(Plan {
                fail_at: Some((at, fault)),
                ..Plan::default()
            }));
            // A fault may hit namespace creation inside the first open;
            // then nothing was acknowledged and the next open must succeed.
            let (mut acknowledged, sequence, uncertain) = match open(store(&case), &plan) {
                Ok(mut db) => run(&mut db),
                Err(_) => (Model::new(), 0, None),
            };
            assert!(plan.lock().unwrap().fired, "{context}: fault not reached");
            let mut db = open(store(&case), &plan).unwrap();
            // An uncertain batch is all-or-nothing, visible by its sequence.
            if let Some(mutations) = uncertain {
                if db.sequence() == sequence + 1 {
                    apply(&mut acknowledged, &mutations);
                } else {
                    assert_eq!(db.sequence(), sequence, "{context}");
                }
            } else {
                assert_eq!(db.sequence(), sequence, "{context}");
            }
            check(&db, &acknowledged, &context);
            // Maintenance resumes and reclaims after reopen without faults.
            db.seal_delta().unwrap();
            while db.consolidate_runs_step().unwrap() {}
            if db.start_prune().unwrap() {
                while db.prune_step().unwrap() {}
            }
            while db.reclaim_pack_step().unwrap() {}
            while db.cleanup_step(16).unwrap() > 0 {}
            check(&db, &acknowledged, &format!("{context}, after maintenance"));
            drop(db);
            let db = open(store(&case), &plan).unwrap();
            check(&db, &acknowledged, &format!("{context}, reopened"));
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
    );
}
