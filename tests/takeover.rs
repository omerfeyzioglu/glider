//! Automatic writer takeover: a renewed lease paces it, fencing makes it
//! safe. A taken-over writer, dead or merely paused, can never publish again,
//! and the new writer starts from every write the old one acknowledged.

use glider::{
    lease::{is_lease_key, Lease},
    retry::{Request, RequestId},
    segmented::{SegmentedDatabase, SegmentedOptions},
    store::ObjectStore,
    Config, Error, Metric, Mutation, Result,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier, Mutex},
    time::{Duration, Instant},
};

const DIMENSIONS: usize = 4;

fn config() -> Config {
    Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    }
}

/// Complete-object store in memory with an exclusive conditional create,
/// shared by every handle ("process") opened on it.
#[derive(Clone, Default)]
struct Memory(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

impl Memory {
    fn copy(&self) -> Self {
        Self(Arc::new(Mutex::new(self.0.lock().unwrap().clone())))
    }
    fn keys(&self) -> Vec<String> {
        self.0.lock().unwrap().keys().cloned().collect()
    }
}

impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.keys())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let mut objects = self.0.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

fn vector(seed: u64) -> Vec<f32> {
    (0..DIMENSIONS as u64)
        .map(|axis| ((seed * 2_654_435_761 + axis * 40_503) % 1_000) as f32 / 10.)
        .collect()
}

fn puts(ids: impl IntoIterator<Item = u64>, seed: u64) -> Vec<Mutation> {
    ids.into_iter()
        .map(|id| Mutation::Put {
            id,
            vector: vector(seed * 1_000 + id),
            metadata: BTreeMap::new(),
        })
        .collect()
}

fn write<S: ObjectStore>(
    db: &mut SegmentedDatabase<S>,
    nonce: u128,
    mutations: Vec<Mutation>,
) -> Result<u64> {
    db.apply_request(Request {
        id: RequestId {
            boundary: db.sequence(),
            nonce: nonce.to_le_bytes(),
        },
        conditions: Vec::new(),
        mutations,
    })
    .map(|outcome| outcome.sequence)
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

/// Every modeled document and the exact top 10 of a few queries.
fn check<S: ObjectStore>(db: &SegmentedDatabase<S>, model: &Model, context: &str) {
    for (&id, expected) in model {
        let found = db.get(id).unwrap().map(|document| document.vector);
        assert_eq!(&found, expected, "{context}: id {id}");
    }
    for seed in 0..3 {
        let query = vector(seed * 7_919);
        let mut expected: Vec<_> = model
            .iter()
            .filter_map(|(&id, vector)| {
                let distance: f64 = query
                    .iter()
                    .zip(vector.as_ref()?)
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
    }
}

fn opener(store: &Memory) -> impl Fn() -> Result<Memory> + Send + 'static {
    let store = store.clone();
    move || Ok(store.clone())
}

/// A taken-over writer with sealed runs and an unsealed tail, and its model.
fn populated(store: &Memory) -> (SegmentedDatabase<Memory>, Model) {
    fill(SegmentedDatabase::take_over(store.clone(), config()).unwrap())
}

fn fill<S: ObjectStore>(mut db: SegmentedDatabase<S>) -> (SegmentedDatabase<S>, Model) {
    let mut model = Model::new();
    for batch in 0..6_u64 {
        let mutations = puts((batch * 7..batch * 7 + 20).map(|id| id % 60), batch);
        write(&mut db, u128::from(batch), mutations.clone()).unwrap();
        apply(&mut model, &mutations);
        if batch == 3 {
            db.seal_delta().unwrap();
        }
    }
    (db, model)
}

#[test]
fn killed_writer_is_taken_over_on_the_same_namespace() {
    let store = Memory::default();
    let lease = Lease::acquire(opener(&store), Duration::from_millis(150)).unwrap();
    let (old, mut model) = populated(&store);
    let acknowledged = old.sequence();
    // Killed: neither the handle nor the lease is closed or released.
    drop((old, lease));

    let started = Instant::now();
    let lease = Lease::acquire(opener(&store), Duration::from_millis(150)).unwrap();
    assert!(started.elapsed() >= Duration::from_millis(150));
    let mut db = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    assert_eq!(db.epoch(), acknowledged + 1);
    assert_eq!(db.sequence(), acknowledged + 1);
    check(&db, &model, "after takeover");
    let mutations = puts(100..110, 9);
    assert_eq!(
        write(&mut db, 99, mutations.clone()).unwrap(),
        acknowledged + 2
    );
    apply(&mut model, &mutations);
    drop(db);
    lease.release().unwrap();

    let started = Instant::now();
    let lease = Lease::acquire(opener(&store), Duration::from_secs(60)).unwrap();
    assert!(started.elapsed() < Duration::from_secs(30), "released");
    let db = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    assert_eq!(db.epoch(), acknowledged + 3);
    check(&db, &model, "after a released restart");
    lease.release().unwrap();
    // At most the release marker remains.
    assert_eq!(
        store.keys().iter().filter(|key| is_lease_key(key)).count(),
        1
    );
}

#[test]
fn paused_writer_write_is_fenced_and_not_committed() {
    let store = Memory::default();
    let (mut old, mut model) = populated(&store);
    let mut new = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    check(&new, &model, "takeover of a paused writer");
    // The paused writer resumes: its next log key holds the takeover record.
    let late = puts(500..505, 1);
    assert!(matches!(
        write(&mut old, 7_000, late.clone()),
        Err(Error::Busy(_))
    ));
    assert!(old.is_poisoned());
    assert!(matches!(old.seal_delta(), Err(Error::RecoveryRequired)));
    let mutations = puts(200..205, 2);
    write(&mut new, 7_001, mutations.clone()).unwrap();
    apply(&mut model, &mutations);
    check(&new, &model, "new writer after the fenced write");
    let reopened = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    check(&reopened, &model, "reopened");
    assert!(
        reopened.get(500).unwrap().is_none(),
        "fenced write is absent"
    );
}

#[test]
fn paused_writer_seal_is_fenced_and_its_late_cleanup_is_harmless() {
    let store = Memory::default();
    let (mut old, mut model) = populated(&store);
    // Leave obsolete objects for a late cleanup, then stage a seal whose
    // root the paused writer will try to publish after the takeover.
    old.seal_delta().unwrap();
    let mutations = puts(300..340, 3);
    write(&mut old, 8_000, mutations.clone()).unwrap();
    apply(&mut model, &mutations);
    old.start_seal().unwrap();
    assert!(old.seal_step().unwrap());

    let mut new = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    check(&new, &model, "takeover during a staged seal");
    // Late deletes remove only objects no later root needs.
    while old.cleanup_step(4).unwrap() > 0 {}
    check(&new, &model, "after the paused writer's cleanup");
    let error = loop {
        match old.seal_step() {
            Ok(true) => {}
            Ok(false) => panic!("the fenced seal published its root"),
            Err(error) => break error,
        }
    };
    assert!(matches!(error, Error::Exists(_)), "{error}");
    assert!(old.is_poisoned());
    // The new writer keeps serving and maintaining the namespace.
    let mutations = puts(0..30, 4);
    write(&mut new, 8_001, mutations.clone()).unwrap();
    apply(&mut model, &mutations);
    new.seal_delta().unwrap();
    while new.consolidate_runs_step().unwrap() {}
    while new.cleanup_step(16).unwrap() > 0 {}
    check(&new, &model, "after new maintenance");
    let reopened = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    check(&reopened, &model, "reopened");
}

/// Runs `hook` once, just before the wrapped handle's first create.
struct Hooked {
    inner: Memory,
    hook: Option<Box<dyn FnOnce()>>,
}

impl ObjectStore for Hooked {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn list(&self) -> Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if let Some(hook) = self.hook.take() {
            hook();
        }
        self.inner.create(key, value)
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

#[test]
fn racing_takeovers_leave_exactly_one_writer_that_can_publish() {
    let store = Memory::default();
    let (mut first, mut model) = populated(&store);
    // The second taker lists, then a third takes over before the second
    // publishes its fence: the second's fence conflicts.
    let third = Arc::new(Mutex::new(None));
    let hooked = Hooked {
        inner: store.clone(),
        hook: Some(Box::new({
            let (store, third) = (store.clone(), third.clone());
            move || {
                *third.lock().unwrap() =
                    Some(SegmentedDatabase::take_over(store, config()).unwrap());
            }
        })),
    };
    assert!(matches!(
        SegmentedDatabase::take_over(hooked, config()),
        Err(Error::Exists(_))
    ));
    let mut third = third.lock().unwrap().take().unwrap();
    // Retrying with a fresh handle succeeds and fences the third.
    let mut second = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    assert!(second.epoch() > third.epoch());
    for (db, nonce) in [(&mut first, 1), (&mut third, 2)] {
        assert!(matches!(
            write(db, nonce, puts(900..901, 9)),
            Err(Error::Busy(_))
        ));
    }
    let mutations = puts(800..810, 8);
    write(&mut second, 3, mutations.clone()).unwrap();
    apply(&mut model, &mutations);
    let reopened = SegmentedDatabase::take_over(store, config()).unwrap();
    check(&reopened, &model, "after racing takeovers");
    assert!(reopened.get(900).unwrap().is_none());
}

/// Several processes contend at once; exactly one acquires the lease. The
/// winner renews, so a contender that looks later sees a live holder.
fn contend(store: &Memory, contenders: usize) -> usize {
    let barrier = Arc::new(Barrier::new(contenders));
    let threads: Vec<_> = (0..contenders)
        .map(|_| {
            let (store, barrier) = (store.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                match Lease::acquire(opener(&store), Duration::from_millis(300)) {
                    Ok(lease) => Some(lease.keep(|| {}).unwrap()),
                    Err(Error::Busy(_)) => None,
                    Err(error) => panic!("unexpected lease error: {error}"),
                }
            })
        })
        .collect();
    let keepers: Vec<_> = threads
        .into_iter()
        .filter_map(|thread| thread.join().unwrap())
        .collect();
    let winners = keepers.len();
    for keeper in keepers {
        keeper.release().unwrap();
    }
    winners
}

#[test]
fn concurrent_takeover_attempts_have_exactly_one_lease_winner() {
    let store = Memory::default();
    assert_eq!(contend(&store, 4), 1, "fresh namespace");
    // A dead holder's lease: every contender waits it out, one wins.
    let dead = Lease::acquire(opener(&store), Duration::from_millis(100)).unwrap();
    drop(dead);
    assert_eq!(contend(&store, 4), 1, "expired lease");
}

#[test]
fn renewed_lease_blocks_takeover_and_release_hands_over_at_once() {
    let store = Memory::default();
    let holder = Lease::acquire(opener(&store), Duration::from_millis(300))
        .unwrap()
        .keep(|| {})
        .unwrap();
    assert!(matches!(
        Lease::acquire(opener(&store), Duration::from_millis(300)),
        Err(Error::Busy(_))
    ));
    assert!(!holder.is_deposed());
    assert_eq!(holder.renewal_errors(), 0);
    holder.release().unwrap();
    let started = Instant::now();
    Lease::acquire(opener(&store), Duration::from_millis(300)).unwrap();
    assert!(started.elapsed() < Duration::from_millis(300));
}

#[test]
fn deposed_holder_learns_it_on_renewal() {
    let store = Memory::default();
    let mut holder = Lease::acquire(opener(&store), Duration::from_millis(50)).unwrap();
    // The holder pauses beyond its lease; another process takes it.
    let taker = Lease::acquire(opener(&store), Duration::from_millis(50)).unwrap();
    assert!(taker.number() > holder.number());
    assert!(matches!(holder.renew(), Err(Error::Busy(_))));
    let (deposed, notified) = std::sync::mpsc::channel();
    let keeper = holder.keep(move || deposed.send(()).unwrap()).unwrap();
    notified.recv_timeout(Duration::from_secs(10)).unwrap();
    assert!(keeper.is_deposed());
    assert!(matches!(keeper.release(), Err(Error::Busy(_))));
    taker.release().unwrap();
}

#[test]
fn deposed_holder_publishing_into_a_removed_number_learns_it_is_deposed() {
    let store = Memory::default();
    let mut holder = Lease::acquire(opener(&store), Duration::from_millis(20)).unwrap();
    let first = holder.number();
    let mut taker = Lease::acquire(opener(&store), Duration::from_millis(20)).unwrap();
    // The taker renews twice, removing every number below its own, so the
    // paused holder's next number is free again.
    taker.renew().unwrap();
    taker.renew().unwrap();
    assert!(!store.keys().contains(&format!("sglease-{:020}", first + 1)));
    assert!(matches!(holder.renew(), Err(Error::Busy(_))));
    // The taker removes the deposed holder's stray number on renewal.
    taker.renew().unwrap();
    assert_eq!(
        store.keys().iter().filter(|key| is_lease_key(key)).count(),
        1
    );
    taker.release().unwrap();
}

/// Several processes take over at once, each retrying a conflicted attempt
/// like a server start, then each tries one write: exactly one commits, and
/// it is the only one a later takeover sees.
fn race<S: ObjectStore + Send + 'static>(
    open: impl Fn() -> S + Clone + Send + 'static,
    round: u64,
) {
    let (db, model) = fill(SegmentedDatabase::take_over(open(), config()).unwrap());
    drop(db);
    let contenders = 4;
    let barrier = Arc::new(Barrier::new(contenders));
    let threads: Vec<_> = (0..contenders as u64)
        .map(|contender| {
            let (open, barrier) = (open.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                let mut db = (0..16)
                    .find_map(|_| match SegmentedDatabase::take_over(open(), config()) {
                        Err(Error::Exists(_)) => None,
                        other => Some(other.unwrap()),
                    })
                    .expect("a takeover attempt without conflict");
                // Every takeover finishes before anyone writes.
                barrier.wait();
                let mutations = puts([1_000 + contender], round);
                write(&mut db, contender.into(), mutations.clone())
                    .ok()
                    .map(|_| mutations)
            })
        })
        .collect();
    let committed: Vec<_> = threads
        .into_iter()
        .filter_map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(committed.len(), 1, "round {round}");
    let mut expected = model;
    apply(&mut expected, &committed[0]);
    let db = SegmentedDatabase::take_over(open(), config()).unwrap();
    check(&db, &expected, &format!("round {round}"));
    for id in 1_000..1_000 + contenders as u64 {
        assert_eq!(
            db.get(id).unwrap().is_some(),
            expected.contains_key(&id),
            "round {round}: id {id}"
        );
    }
}

#[test]
fn concurrent_takeovers_leave_exactly_one_writer_that_commits() {
    for round in 0..40 {
        let store = Memory::default();
        race(move || store.clone(), round);
    }
}

/// A killed writer's lease is waited out and a paused writer is fenced on a
/// real S3-compatible store (run by `tools/test_s3.py` with a disposable
/// MinIO; skipped otherwise).
#[cfg(feature = "s3")]
#[test]
#[ignore = "requires isolated MinIO; run tools/test_s3.py"]
fn minio_takeover_fences_paused_writers_and_admits_one_racer() {
    use glider::store::s3::{AmazonS3Builder, S3Store};
    let mut random = [0_u8; 8];
    getrandom::getrandom(&mut random).unwrap();
    let prefix = format!("takeover-{}", u64::from_le_bytes(random));
    let open = |namespace: String| {
        move || {
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
        }
    };
    let store = open(format!("{prefix}-paused"));
    let lease_store = store.clone();
    let lease_open = move || Ok(lease_store());
    let lease = Lease::acquire(lease_open.clone(), Duration::from_millis(300)).unwrap();
    let (mut old, mut model) = fill(SegmentedDatabase::take_over(store(), config()).unwrap());
    // The old process stops renewing but is only paused, not dead.
    drop(lease);
    let lease = Lease::acquire(lease_open, Duration::from_millis(300)).unwrap();
    let mut new = SegmentedDatabase::take_over(store(), config()).unwrap();
    check(&new, &model, "after takeover on S3");
    assert!(matches!(
        write(&mut old, 7_000, puts(500..505, 1)),
        Err(Error::Busy(_))
    ));
    assert!(old.seal_delta().is_err());
    let mutations = puts(200..205, 2);
    write(&mut new, 7_001, mutations.clone()).unwrap();
    apply(&mut model, &mutations);
    new.seal_delta().unwrap();
    while new.cleanup_step(16).unwrap() > 0 {}
    drop(new);
    lease.release().unwrap();
    let db = SegmentedDatabase::take_over(store(), config()).unwrap();
    check(&db, &model, "reopened on S3");
    assert!(db.get(500).unwrap().is_none());
    for round in 0..3 {
        race(open(format!("{prefix}-race-{round}")), round);
    }
}

#[test]
fn rejected_configuration_takes_nothing_over() {
    let store = Memory::default();
    let (mut db, _) = populated(&store);
    let before = store.keys();
    let options = SegmentedOptions {
        resident_filter: Some(("color".into(), "red".into())),
        routed_keys: Vec::new(),
    };
    assert!(matches!(
        SegmentedDatabase::take_over_with_options(store.clone(), config(), options),
        Err(Error::Invalid(_))
    ));
    let wrong = Config {
        dimensions: DIMENSIONS + 1,
        ..config()
    };
    assert!(SegmentedDatabase::take_over(store.clone(), wrong).is_err());
    assert_eq!(store.keys(), before, "no fence was published");
    write(&mut db, 1, puts(0..1, 1)).unwrap();
}

#[test]
fn takeover_ignores_legacy_claims_and_old_namespaces_open() {
    let store = Memory::default();
    // A namespace written before takeover existed: no takeover records,
    // version 1 roots and the claim objects of the earlier protocol.
    let (db, model) = fill(SegmentedDatabase::open(store.clone(), config()).unwrap());
    let sequence = db.sequence();
    assert_eq!(db.epoch(), 0);
    drop(db);
    // Control objects of the earlier claim protocol, left by a killed owner.
    store
        .clone()
        .create("owner-root-v1", br#"{"version":1}"#)
        .unwrap();
    store
        .clone()
        .create(
            "owner-v1-0123456789abcdef0123456789abcdef",
            br#"{"version":1,"token":"0123456789abcdef0123456789abcdef"}"#,
        )
        .unwrap();
    let lease = Lease::acquire(opener(&store), Duration::from_millis(50)).unwrap();
    let mut db = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    assert_eq!(db.epoch(), sequence + 1);
    check(&db, &model, "legacy namespace");
    db.seal_delta().unwrap();
    while db.cleanup_step(16).unwrap() > 0 {}
    drop(db);
    let db = SegmentedDatabase::take_over(store.clone(), config()).unwrap();
    check(&db, &model, "legacy namespace after a fenced seal");
    assert!(store.keys().iter().any(|key| key == "owner-root-v1"));
    assert!(store.keys().iter().any(|key| is_lease_key(key)));
    lease.release().unwrap();
}

/// Fault injection on each create and remove: before it lands or after
/// (response lost). Shared by every handle opened from one plan.
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

#[derive(Clone)]
struct Faulty {
    inner: Memory,
    plan: Arc<Mutex<Plan>>,
}

impl Faulty {
    fn mutate(&mut self, operation: impl FnOnce(&mut Memory) -> Result<()>) -> Result<()> {
        let fault = {
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
        };
        let injected = || Error::Io(std::io::Error::other("injected storage failure"));
        match fault {
            Some(Fault::Before) => Err(injected()),
            Some(Fault::After) => {
                operation(&mut self.inner)?;
                Err(injected())
            }
            None => operation(&mut self.inner),
        }
    }
}

impl ObjectStore for Faulty {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn list(&self) -> Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.mutate(|inner| inner.create(key, value))
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.mutate(|inner| inner.remove(key))
    }
}

const LEASE: Duration = Duration::from_millis(1);

/// One restart: acquire the lease, take over, write one batch, release.
/// Returns whether the batch was acknowledged.
fn restart(store: &Faulty, nonce: u128, batch: &[Mutation]) -> Result<bool> {
    let opened = store.clone();
    let lease = Lease::acquire(move || Ok(opened.clone()), LEASE)?;
    let mut db = SegmentedDatabase::take_over(store.clone(), config())?;
    let acknowledged = write(&mut db, nonce, batch.to_vec()).is_ok();
    drop(db);
    lease.release()?;
    Ok(acknowledged)
}

/// Crash-point matrix of the takeover protocol itself, in the style of
/// `segmented_crash.rs`: every create and remove of a restart fails before
/// or after landing; a further restart must recover every acknowledged
/// write, keep an uncertain batch all-or-nothing, and the paused original
/// writer must stay fenced. Runs on a killed writer's namespace and on an
/// empty prefix, whose first takeover also creates the namespace.
#[test]
fn every_takeover_failure_recovers_and_keeps_the_old_writer_fenced() {
    let base = Memory::default();
    let lease = Lease::acquire(opener(&base), LEASE).unwrap();
    let (old, model) = populated(&base);
    drop((old, lease)); // killed
    crash_matrix(&base, &model, true);
    crash_matrix(&Memory::default(), &Model::new(), false);
}

fn crash_matrix(base: &Memory, model: &Model, paused_writer: bool) {
    let batch = puts(1_000..1_010, 10);
    let follow = puts(2_000..2_005, 20);
    let clean = Arc::new(Mutex::new(Plan::default()));
    let store = Faulty {
        inner: base.copy(),
        plan: clean.clone(),
    };
    assert!(restart(&store, 1, &batch).unwrap());
    let operations = clean.lock().unwrap().operations;
    assert!(
        operations >= 6,
        "restart has {operations} mutating operations"
    );

    for at in 0..operations {
        for fault in [Fault::Before, Fault::After] {
            let context = format!("operation {at} of {operations}, {fault:?}");
            let memory = base.copy();
            // The original writer, paused before any of this happened.
            let mut paused =
                paused_writer.then(|| SegmentedDatabase::open(memory.clone(), config()).unwrap());
            let plan = Arc::new(Mutex::new(Plan {
                fail_at: Some((at, fault)),
                ..Plan::default()
            }));
            let store = Faulty {
                inner: memory.clone(),
                plan: plan.clone(),
            };
            let acknowledged = matches!(restart(&store, 1, &batch), Ok(true));
            assert!(plan.lock().unwrap().fired, "{context}: fault not reached");

            // Recovery: an unfaulted restart on the same namespace.
            let lease = Lease::acquire(opener(&memory), LEASE).unwrap();
            let mut db = SegmentedDatabase::take_over(memory.clone(), config()).unwrap();
            let mut expected = model.clone();
            let applied = db.get(1_000).unwrap().is_some();
            assert!(
                applied || !acknowledged,
                "{context}: acknowledged batch lost"
            );
            if applied {
                apply(&mut expected, &batch);
            }
            check(&db, &expected, &context);
            if let Some(paused) = paused.as_mut() {
                assert!(
                    write(paused, 3, puts(3_000..3_001, 30)).is_err(),
                    "{context}: paused writer published"
                );
                assert!(paused.seal_delta().is_err(), "{context}: paused seal");
            }
            write(&mut db, 2, follow.clone()).unwrap();
            apply(&mut expected, &follow);
            db.seal_delta().unwrap();
            while db.cleanup_step(16).unwrap() > 0 {}
            drop(db);
            lease.release().unwrap();
            let db = SegmentedDatabase::take_over(memory, config()).unwrap();
            check(&db, &expected, &format!("{context}, reopened"));
            assert!(db.get(3_000).unwrap().is_none(), "{context}");
        }
    }
}

/// xorshift64*: reproducible from the printed seed.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Random interleavings of takeovers, writes and staged maintenance by
/// every writer that ever opened the namespace. Only the newest writer may
/// publish; deposed writers' writes fail and their late steps never damage
/// the newest writer's state. `GLIDER_TAKEOVER_SEED` reruns one seed.
#[test]
fn random_interleavings_never_let_a_deposed_writer_publish() {
    let seeds: Vec<u64> = match std::env::var("GLIDER_TAKEOVER_SEED") {
        Ok(seed) => vec![seed.parse().expect("GLIDER_TAKEOVER_SEED is a u64")],
        Err(_) => (1..=12).collect(),
    };
    for seed in seeds {
        // Captured output is shown if the run fails.
        println!("takeover interleaving seed {seed}");
        interleave(seed);
    }
}

fn interleave(seed: u64) {
    let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let store = Memory::default();
    let mut writers = vec![SegmentedDatabase::take_over(store.clone(), config()).unwrap()];
    let mut model = Model::new();
    let mut nonce = 0_u128;
    for step in 0..300 {
        let context = format!("seed {seed} step {step}");
        let current = writers.len() - 1;
        let chosen = if random.below(3) == 0 {
            random.below(writers.len() as u64) as usize
        } else {
            current
        };
        let db = &mut writers[chosen];
        nonce += 1;
        match random.below(10) {
            0 => {
                let db = SegmentedDatabase::take_over(store.clone(), config())
                    .unwrap_or_else(|error| panic!("{context}: takeover failed: {error}"));
                check(&db, &model, &format!("{context}: takeover"));
                writers.push(db);
            }
            1..=5 => {
                if chosen == current && db.tail_objects() >= 60 {
                    while db.seal_step().unwrap() {}
                    db.seal_delta().unwrap();
                }
                let mutations: Vec<_> = (0..1 + random.below(8))
                    .map(|_| {
                        let id = random.below(80);
                        if random.below(5) == 0 {
                            Mutation::Delete { id }
                        } else {
                            Mutation::Put {
                                id,
                                vector: vector(random.below(1 << 20)),
                                metadata: BTreeMap::new(),
                            }
                        }
                    })
                    .collect();
                let result = write(db, nonce, mutations.clone());
                if chosen == current {
                    result.unwrap_or_else(|error| panic!("{context}: write failed: {error}"));
                    apply(&mut model, &mutations);
                } else {
                    assert!(
                        result.is_err(),
                        "{context}: deposed writer {chosen} published"
                    );
                }
            }
            6 | 7 => {
                // One staged seal step, starting a seal if none is staged.
                let result = db.seal_step().and_then(|worked| match worked {
                    true => Ok(()),
                    false => db.start_seal(),
                });
                if chosen == current {
                    result.unwrap_or_else(|error| panic!("{context}: seal failed: {error}"));
                }
            }
            8 => {
                let result = (|| {
                    while db.seal_step()? {}
                    while db.consolidate_runs_step()? {}
                    if db.start_prune()? {
                        while db.prune_step()? {}
                    }
                    while db.reclaim_pack_step()? {}
                    Ok::<_, Error>(())
                })();
                if chosen == current {
                    result.unwrap_or_else(|error| panic!("{context}: maintenance: {error}"));
                }
            }
            _ => {
                let result = db.cleanup_step(1 + random.below(4) as usize);
                if chosen == current {
                    result.unwrap_or_else(|error| panic!("{context}: cleanup failed: {error}"));
                }
            }
        }
        if chosen != current {
            check(
                &writers[current],
                &model,
                &format!("{context}: after deposed writer {chosen}"),
            );
        }
    }
    let db = SegmentedDatabase::take_over(store, config()).unwrap();
    check(&db, &model, &format!("seed {seed}: final takeover"));
}
