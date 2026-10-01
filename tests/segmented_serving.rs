use glider::{
    admission::{Client, Engine, Limits, QueryResult, Service, Shutdown},
    retry::{Request, RequestId},
    segmented::{
        ReadBudget, SegmentedDatabase, SegmentedOptions, SegmentedServing, SegmentedServingOptions,
    },
    store::{LocalStore, ObjectStore},
    Config, Metric, Mutation,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Barrier, Mutex,
    },
    time::{Duration, Instant},
};

fn config() -> Config {
    Config {
        dimensions: 4,
        metric: Metric::SquaredEuclidean,
    }
}

fn options() -> SegmentedOptions {
    SegmentedOptions {
        resident_filter: Some(("cohort".into(), "one-percent".into())),
        routed_keys: Vec::new(),
    }
}

fn serving(cache: &Path) -> SegmentedServingOptions {
    SegmentedServingOptions {
        seal_tail_objects: 4,
        read_budget: ReadBudget::uniform(1_000),
        cleanup_objects: 4,
        max_index_bytes: 1024 * 1024,
        cache: Some((cache.to_path_buf(), 64 * 1024, 1024 * 1024)),
        query_threads: 3,
    }
}

fn vector(id: u64, generation: u64) -> Vec<f32> {
    let x = (id * 7 + generation * 137) % 1000;
    vec![x as f32, (x % 17) as f32, (x % 5) as f32, generation as f32]
}

fn ids(results: &[glider::Neighbor]) -> Vec<u64> {
    results.iter().map(|neighbor| neighbor.id).collect()
}

#[test]
fn service_interleaves_maintenance_and_backup_restores_the_committed_view() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cache = temp.path().join("cache");
    let db = SegmentedServing::open(
        LocalStore::open(&path).unwrap(),
        config(),
        options(),
        serving(&cache),
    )
    .unwrap();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let mut sequence = 0;
    for round in 0..80_u64 {
        let mutations = (0..50)
            .map(|n| {
                let id = (round * 50 + n) % 1_500;
                if n == 49 {
                    return Mutation::Delete { id };
                }
                Mutation::Put {
                    id,
                    vector: vector(id, round),
                    metadata: if id % 100 == 0 {
                        BTreeMap::from([("cohort".into(), "one-percent".into())])
                    } else {
                        BTreeMap::new()
                    },
                }
            })
            .collect();
        let outcome = client
            .write(Request {
                id: RequestId {
                    boundary: sequence,
                    nonce: u128::from(round).to_le_bytes(),
                },
                conditions: Vec::new(),
                mutations,
            })
            .unwrap()
            .wait()
            .unwrap();
        sequence = outcome.value.sequence;
        let result = client
            .query(vector(round, 0), 10, Vec::new())
            .unwrap()
            .wait()
            .unwrap();
        assert!(!result.value.neighbors.is_empty());
        // Undeclared filters are post-filtered: no record has `other`.
        assert!(client
            .query(vector(round, 0), 10, vec![("other".into(), "x".into())])
            .unwrap()
            .wait()
            .unwrap()
            .value
            .neighbors
            .is_empty());
    }
    assert_eq!(client.status().maintenance_errors, 0);
    service.shutdown(Shutdown::Drain).unwrap();

    let mut db = SegmentedServing::open(
        LocalStore::open(&path).unwrap(),
        config(),
        options(),
        serving(&cache),
    )
    .unwrap();
    assert!(db.database().run_count() > 0);
    assert_eq!(db.database().sequence(), sequence);
    assert_eq!(db.database().sketch_rebuilds(), 0);
    while db.maintenance_step().unwrap() {}
    let queries: Vec<_> = (0..20).map(|n| vector(n * 31, 3)).collect();
    for query in &queries {
        let exact = db.database().search_exact(query, 10, &[]).unwrap();
        assert_eq!(
            db.database()
                .search_selective(query, 10, 1_000, &[])
                .unwrap(),
            exact
        );
        assert_eq!(
            db.database()
                .search_selective(query, 10, 1, &[("cohort", "one-percent")])
                .unwrap(),
            db.database()
                .search_exact(query, 10, &[("cohort", "one-percent")])
                .unwrap()
        );
    }
    let backup = temp.path().join("backup");
    db.backup_to(LocalStore::open(&backup).unwrap()).unwrap();
    assert!(db.backup_to(LocalStore::open(&backup).unwrap()).is_err());
    let restored = SegmentedDatabase::open_with_options(
        LocalStore::open(&backup).unwrap(),
        config(),
        options(),
    )
    .unwrap();
    assert_eq!(restored.sequence(), sequence);
    assert_eq!(restored.sketch_rebuilds(), 0);
    for query in &queries {
        assert_eq!(
            ids(&restored.search_exact(query, 10, &[]).unwrap()),
            ids(&db.database().search_exact(query, 10, &[]).unwrap())
        );
    }
    let before: Vec<_> = queries
        .iter()
        .map(|query| db.database().search_selective(query, 10, 8, &[]).unwrap())
        .collect();
    Engine::close(db).unwrap();

    // Corrupt every NVMe cache file: selective reads reject and refetch them.
    let mut corrupted = 0;
    for entry in std::fs::read_dir(cache.join("glider-block-cache-v1")).unwrap() {
        let path = entry.unwrap().path();
        let mut bytes = std::fs::read(&path).unwrap();
        if let Some(byte) = bytes.last_mut() {
            *byte ^= 0xff;
            std::fs::write(&path, bytes).unwrap();
            corrupted += 1;
        }
    }
    assert!(corrupted > 0);
    let db = SegmentedServing::open(
        LocalStore::open(&path).unwrap(),
        config(),
        options(),
        serving(&cache),
    )
    .unwrap();
    for (query, expected) in queries.iter().zip(&before) {
        assert_eq!(
            &db.database().search_selective(query, 10, 8, &[]).unwrap(),
            expected
        );
    }
    assert!(
        db.database()
            .cache_stats()
            .unwrap()
            .unwrap()
            .corrupt_entries
            > 0
    );
    Engine::close(db).unwrap();
}

/// A write at the hard tail bound must finish a seal that idle maintenance
/// started (on slow storage the tail can outgrow it) instead of failing.
#[test]
fn write_at_tail_bound_finishes_an_idle_seal_in_progress() {
    let temp = tempfile::tempdir().unwrap();
    let mut db = SegmentedServing::open(
        LocalStore::open(temp.path().join("db")).unwrap(),
        config(),
        options(),
        serving(&temp.path().join("cache")),
    )
    .unwrap();
    let write = |db: &mut SegmentedServing<LocalStore>, round: u64| {
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: u128::from(round).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: round,
                vector: vector(round, 1),
                metadata: BTreeMap::new(),
            }],
        })
    };
    for round in 0..4 {
        write(&mut db, round).unwrap();
    }
    // One idle unit plans a seal of these four logs without finishing it.
    assert!(db.maintenance_step().unwrap());
    assert_eq!(db.last_unit(), "seal_plan");
    for round in 4..80 {
        write(&mut db, round).unwrap();
    }
    assert_eq!(db.counters().forced_seals, 2);
    assert!(db.database().tail_objects() < 64);
    let exact = db.database().search_exact(&vector(40, 1), 3, &[]).unwrap();
    assert_eq!(ids(&exact)[0], 40);
    db.close().unwrap();
}

const ROWS: u64 = 100;
const ORIGIN: [f32; 4] = [0.; 4];

/// Write `[id, generation, 0, 0]` for each ID. A row's squared distance from
/// the origin is `id² + generation²`, so an origin query reveals the
/// generation of every row it returned.
fn generation_request(boundary: u64, generation: u64, ids: impl Iterator<Item = u64>) -> Request {
    Request {
        id: RequestId {
            boundary,
            nonce: u128::from(generation).to_le_bytes(),
        },
        conditions: Vec::new(),
        mutations: ids
            .map(|id| Mutation::Put {
                id,
                vector: vec![id as f32, generation as f32, 0., 0.],
                metadata: BTreeMap::new(),
            })
            .collect(),
    }
}

/// The generation of each returned row, by ID.
fn generations(result: &QueryResult) -> BTreeMap<u64, u64> {
    result
        .neighbors
        .iter()
        .map(|neighbor| {
            let square = neighbor.distance - (neighbor.id * neighbor.id) as f64;
            let generation = square.sqrt().round() as u64;
            assert_eq!(
                (generation * generation) as f64,
                square,
                "sequence {}",
                result.sequence
            );
            (neighbor.id, generation)
        })
        .collect()
}

/// Every row at one generation.
fn uniform(generation: u64) -> BTreeMap<u64, u64> {
    (0..ROWS).map(|id| (id, generation)).collect()
}

fn query_all<E: Engine>(client: &Client<E>) -> QueryResult {
    client
        .query(ORIGIN.to_vec(), ROWS as usize, Vec::new())
        .unwrap()
        .wait()
        .unwrap()
        .value
}

/// Sealed pack objects in a local namespace.
fn packs(path: &Path) -> BTreeSet<String> {
    std::fs::read_dir(path)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().into_string().unwrap();
            let pack = name.strip_suffix("-seal")?;
            pack.starts_with("sgpack-").then(|| pack.to_owned())
        })
        .collect()
}

/// Seeded concurrent workload: writers put or delete random IDs while
/// readers query every row and idle maintenance seals, consolidates and
/// removes objects; without a block cache every query reads packs from the
/// store. Every result must equal the acknowledged state at its reported
/// sequence, a reader's sequences never decrease, and a read submitted
/// after an acknowledgement observes that write.
#[test]
fn seeded_concurrent_reads_see_one_acknowledged_state_and_every_prior_write() {
    const SEED: u64 = 0x5eed_0034;
    const WRITERS: u64 = 2;
    const WRITES: u64 = 30;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut options = serving(&temp.path().join("cache"));
    options.seal_tail_objects = 2;
    options.cache = None;
    // At most one live block per row, so routing reads every live block
    // and each query is exact.
    options.read_budget = ReadBudget::uniform(ROWS as usize);
    let db = SegmentedServing::open(
        LocalStore::open(&path).unwrap(),
        config(),
        SegmentedOptions::default(),
        options.clone(),
    )
    .unwrap();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let (client, done) = (client.clone(), done.clone());
            std::thread::spawn(move || {
                let mut observed = Vec::new();
                while !done.load(Ordering::Acquire) {
                    let result = query_all(&client);
                    let last = observed.last().map_or(0, |&(sequence, _)| sequence);
                    assert!(
                        result.sequence >= last,
                        "seed {SEED:#x}: sequence {} after {last}",
                        result.sequence
                    );
                    observed.push((result.sequence, generations(&result)));
                }
                observed
            })
        })
        .collect();
    // Writers submit together each round, so their writes may share a group
    // commit; idle maintenance runs while they read.
    let round = Arc::new(Barrier::new(WRITERS as usize));
    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let (client, round_start) = (client.clone(), round.clone());
            std::thread::spawn(move || {
                let mut state = SEED ^ (writer + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
                let mut next = move || {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state
                };
                let (mut boundary, mut acknowledged, mut reads, mut gets) =
                    (0, Vec::new(), Vec::new(), Vec::new());
                for round in 0..WRITES {
                    round_start.wait();
                    let generation = writer * WRITES + round + 1;
                    let ids: BTreeSet<_> = (0..1 + next() % 6).map(|_| next() % ROWS).collect();
                    let mut request = generation_request(boundary, generation, ids.into_iter());
                    for mutation in &mut request.mutations {
                        if next() % 5 == 0 {
                            let Mutation::Put { id, .. } = *mutation else {
                                unreachable!()
                            };
                            *mutation = Mutation::Delete { id };
                        }
                    }
                    let mutations = request.mutations.clone();
                    let outcome = client.write(request).unwrap().wait().unwrap().value;
                    assert!(outcome.conflict.is_none(), "seed {SEED:#x}");
                    boundary = outcome.sequence;
                    // Read-your-writes: both reads are submitted after the
                    // acknowledgement.
                    let result = query_all(&client);
                    reads.push((outcome.sequence, result.sequence, generations(&result)));
                    let (Mutation::Put { id, .. } | Mutation::Delete { id }) = mutations[0];
                    let document = client.get(id).unwrap().wait().unwrap().value;
                    gets.push((outcome.sequence, id, document.map(|d| d.vector[1] as u64)));
                    acknowledged.push((outcome.sequence, mutations));
                }
                (acknowledged, reads, gets)
            })
        })
        .collect();
    let (mut acknowledged, mut after_writes, mut gets) = (BTreeMap::new(), Vec::new(), Vec::new());
    for writer in writers {
        let (writes, reads, documents) = writer.join().unwrap();
        acknowledged.extend(writes);
        after_writes.extend(reads);
        gets.extend(documents);
    }
    // With the writers done, idle maintenance seals the rest of the tail and
    // removes obsolete objects while the readers continue.
    let sample = |name| {
        let metrics = client.metrics().unwrap().wait().unwrap().value;
        metrics
            .samples
            .into_iter()
            .find(|(sample, _)| *sample == name)
            .unwrap()
            .1
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while sample("glider_segmented_removed_objects_total") == 0 {
        assert!(Instant::now() < deadline, "seed {SEED:#x}");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        sample("glider_segmented_seal_starts_total") > 0,
        "seed {SEED:#x}"
    );
    assert_eq!(client.status().maintenance_errors, 0, "seed {SEED:#x}");
    done.store(true, Ordering::Release);
    let mut observed = Vec::new();
    for reader in readers {
        let results = reader.join().unwrap();
        assert!(!results.is_empty(), "seed {SEED:#x}");
        observed.extend(results);
    }
    // The acknowledged state at every sequence.
    assert!(
        acknowledged.keys().copied().eq(1..=WRITERS * WRITES),
        "seed {SEED:#x}"
    );
    let mut states = vec![BTreeMap::new()];
    for mutations in acknowledged.values() {
        let mut state = states.last().unwrap().clone();
        for mutation in mutations {
            match *mutation {
                Mutation::Put { id, ref vector, .. } => state.insert(id, vector[1] as u64),
                Mutation::Delete { id } => state.remove(&id),
            };
        }
        states.push(state);
    }
    for (sequence, rows) in &observed {
        assert_eq!(
            rows, &states[*sequence as usize],
            "seed {SEED:#x}: sequence {sequence}"
        );
    }
    for (written, sequence, rows) in &after_writes {
        assert!(
            sequence >= written,
            "seed {SEED:#x}: read {sequence} after write {written}"
        );
        assert_eq!(
            rows, &states[*sequence as usize],
            "seed {SEED:#x}: sequence {sequence}"
        );
    }
    for &(written, id, document) in &gets {
        assert!(
            states[written as usize..]
                .iter()
                .any(|state| state.get(&id).copied() == document),
            "seed {SEED:#x}: ID {id} read {document:?} after write {written}"
        );
    }
    service.shutdown(Shutdown::Drain).unwrap();
    // Recovery reproduces the last acknowledged state.
    let reopened = SegmentedServing::open(
        LocalStore::open(&path).unwrap(),
        config(),
        SegmentedOptions::default(),
        options,
    )
    .unwrap();
    let last = states.last().unwrap();
    for id in 0..ROWS {
        let document = reopened.database().get(id).unwrap();
        assert_eq!(
            document.map(|d| d.vector[1] as u64),
            last.get(&id).copied(),
            "seed {SEED:#x}: ID {id}"
        );
    }
    reopened.close().unwrap();
}

/// A snapshot answers from the state it was taken at after later writes and
/// maintenance; packs its root references survive cleanup until it is dropped.
/// A query blocked inside a store read holds up neither a write nor the
/// maintenance that retires the packs it reads.
#[test]
fn snapshot_keeps_its_state_and_packs_while_writes_and_maintenance_proceed() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut options = serving(&temp.path().join("cache"));
    options.seal_tail_objects = 1;
    // Without a block cache every block read reaches the store.
    options.cache = None;
    let store = Gated::new(LocalStore::open(&path).unwrap());
    let mut db = SegmentedServing::open(
        store.clone(),
        config(),
        SegmentedOptions::default(),
        options,
    )
    .unwrap();
    let first = db
        .apply_request(generation_request(0, 0, 0..ROWS))
        .unwrap()
        .sequence;
    while db.maintenance_step().unwrap() {}
    let old = db.snapshot().unwrap();
    let before = old.query(&ORIGIN, ROWS as usize, &[]).unwrap();
    assert_eq!(before.sequence, first);
    assert_eq!(generations(&before), uniform(0));
    assert!(before.remote_reads > 0);
    let sealed = packs(&path);
    let (entered, release) = store.reads.arm("sgpack-");
    let blocked = {
        let old = old.clone();
        std::thread::spawn(move || old.query(&ORIGIN, ROWS as usize, &[]).unwrap())
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    let second = db
        .apply_request(generation_request(first, 1, 0..ROWS))
        .unwrap()
        .sequence;
    while db.maintenance_step().unwrap() {}
    // Every generation-0 pack is obsolete now, but `old` still reads them.
    assert!(packs(&path).is_superset(&sealed));
    release.send(()).unwrap();
    let after = blocked.join().unwrap();
    assert_eq!(
        (after.sequence, after.neighbors, after.remote_reads),
        (before.sequence, before.neighbors, before.remote_reads)
    );
    assert_eq!(old.get(7).unwrap().unwrap().vector, [7., 0., 0., 0.]);
    let current = db.snapshot().unwrap();
    let latest = current.query(&ORIGIN, ROWS as usize, &[]).unwrap();
    assert_eq!(latest.sequence, second);
    assert_eq!(generations(&latest), uniform(1));
    drop((old, current));
    while db.maintenance_step().unwrap() {}
    assert!(packs(&path).is_disjoint(&sealed));
    db.close().unwrap();
}

/// Signals entry into a gated store call, then waits for release.
type Waiter = (&'static str, mpsc::SyncSender<()>, mpsc::Receiver<()>);

/// Blocks the next store call on a key with the armed prefix.
#[derive(Clone, Default)]
struct Gate(Arc<Mutex<Option<Waiter>>>);

impl Gate {
    /// Returns the entry signal and the release sender.
    fn arm(&self, prefix: &'static str) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (signal, entered) = mpsc::sync_channel(1);
        let (release, wait) = mpsc::sync_channel(1);
        *self.0.lock().unwrap() = Some((prefix, signal, wait));
        (entered, release)
    }

    fn pass(&self, key: &str) {
        let waiter = {
            let mut armed = self.0.lock().unwrap();
            match &*armed {
                Some((prefix, ..)) if key.starts_with(prefix) => armed.take(),
                _ => None,
            }
        };
        if let Some((_, entered, release)) = waiter {
            entered.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }
}

/// A store whose next matching create or read blocks until released.
struct Gated<S> {
    inner: Arc<S>,
    creates: Gate,
    reads: Gate,
}

impl<S> Clone for Gated<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            creates: self.creates.clone(),
            reads: self.reads.clone(),
        }
    }
}

impl<S> Gated<S> {
    fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
            creates: Gate::default(),
            reads: Gate::default(),
        }
    }
}

impl<S: ObjectStore> ObjectStore for Gated<S> {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        self.reads.pass(key);
        self.inner.get(key)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&self, key: &str, value: &[u8]) -> glider::Result<()> {
        self.creates.pass(key);
        self.inner.create(key, value)
    }
    fn remove(&self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)
    }
}

/// A query reading packs from the store does not wait for a write's log
/// PUT or for maintenance's pack PUT; it sees the last acknowledged state,
/// and each write once acknowledged.
#[test]
fn queries_do_not_wait_for_log_or_pack_publication() {
    let temp = tempfile::tempdir().unwrap();
    let mut options = serving(&temp.path().join("cache"));
    options.seal_tail_objects = 1;
    options.cache = None;
    let store = Gated::new(LocalStore::open(temp.path().join("db")).unwrap());
    let mut db = SegmentedServing::open(
        store.clone(),
        config(),
        SegmentedOptions::default(),
        options,
    )
    .unwrap();
    let first = db
        .apply_request(generation_request(0, 0, 0..ROWS))
        .unwrap()
        .sequence;
    while db.maintenance_step().unwrap() {}
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let full = |client: &Client<_>| {
        let result = query_all(client);
        (result.sequence, generations(&result))
    };
    let (entered, release) = store.creates.arm("sglog-");
    let pending = client.write(generation_request(first, 1, 0..ROWS)).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // Every row is sealed, so this query reads packs from the store.
    let sealed = query_all(&client);
    assert!(sealed.remote_reads > 0);
    assert_eq!((sealed.sequence, generations(&sealed)), (first, uniform(0)));
    // The committer is inside the log create, so it cannot reach the seal.
    let (entered, release_pack) = store.creates.arm("sgpack-");
    release.send(()).unwrap();
    let second = pending.wait().unwrap().value.sequence;
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(full(&client), (second, uniform(1)));
    release_pack.send(()).unwrap();
    service.shutdown(Shutdown::Drain).unwrap();
}
