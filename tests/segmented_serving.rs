use glider::{
    admission::{Limits, QueryResult, Service, Shutdown},
    retry::{Request, RequestId},
    segmented::{SegmentedDatabase, SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::{LocalStore, ObjectStore},
    Config, Metric, Mutation,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
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
        read_budget: glider::segmented::ReadBudget::uniform(1_000),
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
    glider::admission::Engine::close(db).unwrap();

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
    glider::admission::Engine::close(db).unwrap();
}

/// A write at the hard tail bound must finish a seal that idle maintenance
/// started (on slow storage the tail can outgrow it) instead of failing.
#[test]
fn write_at_tail_bound_finishes_an_idle_seal_in_progress() {
    use glider::admission::Engine;
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
/// the origin is `id² + generation²`, so a full origin query reveals the
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

/// The generation of each row, by ID, from a full origin query.
fn generations(result: &QueryResult) -> Vec<u64> {
    let sequence = result.sequence;
    let mut rows: Vec<_> = result
        .neighbors
        .iter()
        .map(|neighbor| {
            let square = neighbor.distance - (neighbor.id * neighbor.id) as f64;
            let generation = square.sqrt().round() as u64;
            assert_eq!(
                (generation * generation) as f64,
                square,
                "sequence {sequence}"
            );
            (neighbor.id, generation)
        })
        .collect();
    rows.sort_unstable();
    assert!(
        rows.iter().map(|&(id, _)| id).eq(0..ROWS),
        "sequence {sequence}"
    );
    rows.into_iter().map(|(_, generation)| generation).collect()
}

fn query_all<E: glider::admission::Engine>(client: &glider::admission::Client<E>) -> QueryResult {
    client
        .query(ORIGIN.to_vec(), ROWS as usize, Vec::new())
        .unwrap()
        .wait()
        .unwrap()
        .value
}

/// Generation 0 writes every row and acknowledges at sequence 1; generation
/// g >= 1 overwrites the IDs congruent to g modulo 4 at sequence g + 1.
fn rotating_ids(generation: u64) -> impl Iterator<Item = u64> {
    (0..ROWS).filter(move |id| generation == 0 || id % 4 == generation % 4)
}

/// The generation of each row at an acknowledged sequence.
fn rotating_state(sequence: u64) -> Vec<u64> {
    (0..ROWS)
        .map(|id| {
            (1..sequence)
                .rev()
                .find(|generation| id % 4 == generation % 4)
                .unwrap_or(0)
        })
        .collect()
}

/// Queries run on reader threads while one writer overwrites a quarter of
/// the rows per request and idle maintenance seals, compacts and switches
/// roots. Every result must equal the acknowledged state at its reported
/// sequence, sequences never decrease per reader, and a read submitted
/// after an acknowledgement observes that write.
#[test]
fn concurrent_queries_see_one_acknowledged_state_and_every_prior_write() {
    let temp = tempfile::tempdir().unwrap();
    let mut options = serving(&temp.path().join("cache"));
    options.seal_tail_objects = 4;
    let db = SegmentedServing::open(
        LocalStore::open(temp.path().join("db")).unwrap(),
        config(),
        SegmentedOptions::default(),
        options,
    )
    .unwrap();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let checked = |client: &glider::admission::Client<_>| {
        let result = query_all(client);
        assert_eq!(
            generations(&result),
            rotating_state(result.sequence),
            "sequence {}",
            result.sequence
        );
        result.sequence
    };
    let mut sequence = 0;
    for generation in 0..=2 {
        sequence = client
            .write(generation_request(
                sequence,
                generation,
                rotating_ids(generation),
            ))
            .unwrap()
            .wait()
            .unwrap()
            .value
            .sequence;
    }
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let (client, done) = (client.clone(), done.clone());
            std::thread::spawn(move || {
                let (mut last, mut count) = (0, 0);
                while !done.load(Ordering::Acquire) {
                    let observed = checked(&client);
                    assert!(observed >= last, "sequence {observed} after {last}");
                    (last, count) = (observed, count + 1);
                }
                count
            })
        })
        .collect();
    for generation in 3..=60 {
        sequence = client
            .write(generation_request(
                sequence,
                generation,
                rotating_ids(generation),
            ))
            .unwrap()
            .wait()
            .unwrap()
            .value
            .sequence;
        assert_eq!(sequence, generation + 1);
        assert!(checked(&client) >= sequence, "generation {generation}");
        let id = generation % 4;
        let document = client.get(id).unwrap().wait().unwrap().value.unwrap();
        assert!(
            document.vector[1] >= generation as f32,
            "generation {generation}"
        );
    }
    done.store(true, Ordering::Release);
    for reader in readers {
        assert!(reader.join().unwrap() > 0);
    }
    let metrics = client.metrics().unwrap().wait().unwrap().value;
    let sample = |name| {
        metrics
            .samples
            .iter()
            .find(|(sample, _)| *sample == name)
            .unwrap()
            .1
    };
    assert!(sample("glider_segmented_seal_starts_total") > 0);
    assert!(sample("glider_cache_remote_fetches_total") > 0);
    assert_eq!(client.status().maintenance_errors, 0);
    service.shutdown(Shutdown::Drain).unwrap();
}

/// A snapshot answers from the state it was taken at after later writes and
/// maintenance; packs its root references survive cleanup until it is dropped.
#[test]
fn snapshot_keeps_its_state_and_packs_across_later_commits() {
    use glider::admission::Engine;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut options = serving(&temp.path().join("cache"));
    options.seal_tail_objects = 1;
    // Without a block cache every block read reaches the store.
    options.cache = None;
    let mut db = SegmentedServing::open(
        LocalStore::open(&path).unwrap(),
        config(),
        SegmentedOptions::default(),
        options,
    )
    .unwrap();
    let packs = || {
        std::fs::read_dir(&path)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                let name = name.to_str().unwrap();
                name.starts_with("sgpack-") && name.ends_with("-seal")
            })
            .count()
    };
    let first = db
        .apply_request(generation_request(0, 0, 0..ROWS))
        .unwrap()
        .sequence;
    while db.maintenance_step().unwrap() {}
    let old = db.snapshot().unwrap();
    let before = old.query(&ORIGIN, ROWS as usize, &[]).unwrap();
    assert_eq!(before.sequence, first);
    assert_eq!(generations(&before), [0; ROWS as usize]);
    assert!(before.remote_reads > 0);
    let second = db
        .apply_request(generation_request(first, 1, 0..ROWS))
        .unwrap()
        .sequence;
    while db.maintenance_step().unwrap() {}
    // Every generation-0 pack is obsolete now, but `old` still reads them.
    let pinned = packs();
    let after = old.query(&ORIGIN, ROWS as usize, &[]).unwrap();
    assert_eq!(
        (after.sequence, after.neighbors, after.remote_reads),
        (before.sequence, before.neighbors, before.remote_reads)
    );
    assert_eq!(old.get(7).unwrap().unwrap().vector, [7., 0., 0., 0.]);
    let current = db.snapshot().unwrap();
    let latest = current.query(&ORIGIN, ROWS as usize, &[]).unwrap();
    assert_eq!(latest.sequence, second);
    assert_eq!(generations(&latest), [1; ROWS as usize]);
    drop((old, current));
    while db.maintenance_step().unwrap() {}
    assert!(packs() < pinned, "{} packs left of {pinned}", packs());
    db.close().unwrap();
}

/// Signals entry into a log create, then waits for release.
type Gate = (mpsc::SyncSender<()>, mpsc::Receiver<()>);

/// In-memory store whose next log create blocks until released.
#[derive(Clone, Default)]
struct Gated {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    gate: Arc<Mutex<Option<Gate>>>,
}

impl Gated {
    fn arm(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<()>) {
        let (signal, entered) = mpsc::sync_channel(1);
        let (release, wait) = mpsc::sync_channel(1);
        *self.gate.lock().unwrap() = Some((signal, wait));
        (entered, release)
    }
}

impl ObjectStore for Gated {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        if key.starts_with("sglog-") {
            if let Some((entered, release)) = self.gate.lock().unwrap().take() {
                entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Err(glider::Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

/// A query does not wait for a write whose log PUT is in progress; it sees
/// the last acknowledged state, and the write once acknowledged.
#[test]
fn queries_run_while_a_write_is_publishing() {
    let store = Gated::default();
    let temp = tempfile::tempdir().unwrap();
    let mut options = serving(&temp.path().join("cache"));
    options.cache = None;
    let db = SegmentedServing::open(
        store.clone(),
        config(),
        SegmentedOptions::default(),
        options,
    )
    .unwrap();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let full = |client: &glider::admission::Client<_>| {
        let result = query_all(client);
        (result.sequence, generations(&result))
    };
    let first = client
        .write(generation_request(0, 0, 0..ROWS))
        .unwrap()
        .wait()
        .unwrap()
        .value
        .sequence;
    let (entered, release) = store.arm();
    let pending = client.write(generation_request(first, 1, 0..ROWS)).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(full(&client), (first, vec![0; ROWS as usize]));
    release.send(()).unwrap();
    let second = pending.wait().unwrap().value.sequence;
    assert_eq!(full(&client), (second, vec![1; ROWS as usize]));
    service.shutdown(Shutdown::Drain).unwrap();
}
