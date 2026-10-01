use glider::{
    retry::{Request, RequestId},
    segmented::{QueryOptions, ReadBudget, SegmentedDatabase, SegmentedOptions},
    store::{LocalStore, ObjectStore},
    Config, Error, Metric, Mutation, Result,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

const DIMENSIONS: usize = 32;

fn config() -> Config {
    Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    }
}

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
    fn vector(&mut self) -> Vec<f32> {
        (0..DIMENSIONS)
            .map(|_| self.below(1000) as f32 / 10.)
            .collect()
    }
}

/// Test store hook: optionally fail one create (before or after it lands) and
/// flip one byte of range reads that start at a pack's first byte.
#[derive(Clone, Default)]
struct Hooks {
    fail_create: Option<(String, bool)>,
    tamper: Vec<(String, usize)>,
}

struct HookStore {
    inner: LocalStore,
    hooks: Arc<Mutex<Hooks>>,
}

impl ObjectStore for HookStore {
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
        let mut bytes = self.inner.get_range(key, offset, length, payload_len)?;
        if offset == 0 {
            for (pack, at) in &self.hooks.lock().unwrap().tamper {
                if let Some(bytes) = bytes.as_mut().filter(|_| pack == key) {
                    bytes[*at] ^= 0xff;
                }
            }
        }
        Ok(bytes)
    }
    fn list(&self) -> Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let fail = {
            let mut hooks = self.hooks.lock().unwrap();
            match &hooks.fail_create {
                Some((prefix, after)) if key.starts_with(prefix.as_str()) => {
                    let after = *after;
                    hooks.fail_create = None;
                    Some(after)
                }
                _ => None,
            }
        };
        match fail {
            Some(true) => {
                self.inner.create(key, value)?;
                Err(Error::Io(std::io::Error::other("lost create response")))
            }
            Some(false) => Err(Error::Io(std::io::Error::other("create failed"))),
            None => self.inner.create(key, value),
        }
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

fn open(path: &Path, hooks: &Arc<Mutex<Hooks>>) -> Result<SegmentedDatabase<HookStore>> {
    SegmentedDatabase::open_with_options(
        HookStore {
            inner: LocalStore::open(path)?,
            hooks: hooks.clone(),
        },
        config(),
        options(),
    )
    .map(|db| db.with_query_threads(3).with_reclaim_min_garbage(1))
}

fn ids(results: Vec<glider::Neighbor>) -> Vec<(u64, u64)> {
    results
        .into_iter()
        .map(|neighbor| (neighbor.id, neighbor.distance.to_bits()))
        .collect()
}

#[test]
fn query_fields_follow_tail_overwrite_seal_and_consolidation() {
    let temp = tempfile::tempdir().unwrap();
    let hooks = Arc::new(Mutex::new(Hooks::default()));
    let mut db = open(&temp.path().join("db"), &hooks).unwrap();
    let fields = QueryOptions {
        include_metadata: true,
        include_vector: true,
    };
    let vector = |n: f32| vec![n; DIMENSIONS];
    for batch in 0..3_u64 {
        let mutations = (0..10_u64)
            .map(|row| Mutation::Put {
                id: batch * 10 + row,
                vector: vector((batch * 10 + row) as f32),
                metadata: BTreeMap::from([
                    ("tag".into(), "hot".into()),
                    ("route".into(), "yes".into()),
                    ("version".into(), batch.to_string()),
                ]),
            })
            .collect();
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: batch.to_le_bytes().repeat(2).try_into().unwrap(),
            },
            conditions: Vec::new(),
            mutations,
        })
        .unwrap();
        db.seal_delta().unwrap();
    }
    while db.consolidate_runs_step().unwrap() {}
    assert_eq!(db.run_count(), 1);
    db.apply_request(Request {
        id: RequestId {
            boundary: db.sequence(),
            nonce: [9; 16],
        },
        conditions: Vec::new(),
        mutations: vec![Mutation::Put {
            id: 0,
            vector: vector(0.),
            metadata: BTreeMap::from([
                ("tag".into(), "hot".into()),
                ("route".into(), "yes".into()),
                ("version".into(), "new-tail".into()),
            ]),
        }],
    })
    .unwrap();
    let budget = ReadBudget::uniform(db.block_count().max(1));
    for filter in [vec![], vec![("route", "yes")], vec![("tag", "hot")]] {
        for hits in [
            db.search_exact_with_options(&vector(0.), 30, &filter, fields)
                .unwrap(),
            db.search_selective_within_options(&vector(0.), 30, budget, &filter, fields)
                .unwrap(),
        ] {
            assert_eq!(hits.len(), 30);
            for hit in hits {
                let document = db.get(hit.id).unwrap().unwrap();
                assert_eq!(
                    hit.metadata.as_ref(),
                    Some(&document.metadata),
                    "id {}",
                    hit.id
                );
                assert_eq!(hit.vector.as_ref(), Some(&document.vector), "id {}", hit.id);
                if hit.id == 0 {
                    assert_eq!(hit.metadata.unwrap()["version"], "new-tail");
                }
            }
        }
    }
}

/// Reading every block must reproduce exact search, and the resident filter is
/// always exact; a small block budget must still validate selected blocks.
fn check(db: &SegmentedDatabase<HookStore>, rng: &mut Rng, seed: u64, step: usize) {
    for _ in 0..3 {
        let query = rng.vector();
        let exact = db.search_exact(&query, 10, &[]).unwrap();
        let routed = db
            .search_selective(&query, 10, db.block_count().max(1), &[])
            .unwrap();
        assert_eq!(ids(routed), ids(exact), "seed {seed} step {step}");
        let filtered = ids(db.search_exact(&query, 10, &[("tag", "hot")]).unwrap());
        let resident = db
            .search_selective(&query, 10, 1, &[("tag", "hot")])
            .unwrap();
        assert_eq!(ids(resident), filtered.clone(), "seed {seed} step {step}");
        // A predicate other than the declared one is post-filtered over the
        // routed blocks; reading every block makes it exact.
        let general = [("tag", "hot"), ("tag", "hot")];
        let post = db
            .search_selective(&query, 10, db.block_count().max(1), &general)
            .unwrap();
        assert_eq!(ids(post), filtered, "seed {seed} step {step}");
        let routed_filter = [("route", "yes")];
        let exact_routed = ids(db.search_exact(&query, 10, &routed_filter).unwrap());
        let selective_routed = ids(db
            .search_selective(&query, 10, db.block_count().max(1), &routed_filter)
            .unwrap());
        assert_eq!(selective_routed, exact_routed, "seed {seed} step {step}");
        let partial = db.search_selective(&query, 10, 2, &[]).unwrap();
        assert!(
            partial.len() <= 10 && partial.windows(2).all(|p| p[0].distance <= p[1].distance),
            "seed {seed} step {step}"
        );
    }
}

#[test]
fn persisted_sketches_follow_seal_maintenance_reopen_and_loss() {
    let seed = std::env::var("GLIDER_TEST_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0x5eed_2024_u64);
    eprintln!("GLIDER_TEST_SEED={seed}");
    let mut rng = Rng(seed | 1);
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let hooks = Arc::new(Mutex::new(Hooks::default()));
    let mut db = open(&path, &hooks).unwrap();
    let mut boundary = 0;
    let mut exercised = [0_usize; 3];
    for step in 0..160 {
        let mutations: Vec<_> = (0..1 + rng.below(100))
            .map(|_| {
                let id = rng.below(1_500);
                if rng.below(5) == 0 {
                    Mutation::Delete { id }
                } else {
                    let mut metadata = BTreeMap::new();
                    if rng.below(20) == 0 {
                        metadata.insert("tag".into(), "hot".into());
                    }
                    if rng.below(3) == 0 {
                        metadata.insert("route".into(), "yes".into());
                    }
                    Mutation::Put {
                        id,
                        vector: rng.vector(),
                        metadata,
                    }
                }
            })
            .collect();
        let request = Request {
            id: RequestId {
                boundary,
                nonce: (step as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations,
        };
        match db.apply_request(request) {
            Ok(outcome) => boundary = outcome.sequence,
            Err(Error::MaintenanceRequired) => db.seal_delta().unwrap(),
            Err(error) => panic!("seed {seed} step {step}: {error}"),
        }
        match rng.below(8) {
            0 | 1 => db.seal_delta().unwrap(),
            2 => {
                while db.consolidate_runs_step().unwrap() {
                    exercised[0] += 1;
                }
            }
            3 => {
                if db.start_prune().unwrap() {
                    exercised[1] += 1;
                    while db.prune_step().unwrap() {}
                }
            }
            4 => {
                while db.reclaim_pack_step().unwrap() {
                    exercised[2] += 1;
                }
            }
            5 => {
                db.cleanup_step(64).unwrap();
            }
            6 if step % 2 == 0 => while db.compact_sketch(1) {},
            6 if step % 3 == 0 => {
                drop(db);
                db = open(&path, &hooks).unwrap();
                assert_eq!(db.sketch_rebuilds(), 0, "seed {seed} step {step}");
            }
            _ => {}
        }
        if step % 8 == 0 {
            check(&db, &mut rng, seed, step);
        }
    }
    assert!(
        exercised.iter().all(|&count| count > 0),
        "seed {seed} {exercised:?}"
    );
    db.seal_delta().unwrap();
    while db.cleanup_step(64).unwrap() > 0 {}
    check(&db, &mut rng, seed, usize::MAX);
    let query = rng.vector();
    let before = ids(db.search_selective(&query, 10, 8, &[]).unwrap());
    drop(db);

    // A corrupt frame header or sketch body changes readiness work, not answers.
    let mut packs: Vec<_> = LocalStore::open(&path)
        .unwrap()
        .list()
        .unwrap()
        .into_iter()
        .filter(|key| key.starts_with("sgpack-"))
        .collect();
    packs.sort();
    assert!(packs.len() >= 2, "seed {seed}");
    hooks.lock().unwrap().tamper = vec![(packs[0].clone(), 0), (packs[1].clone(), 60)];
    let db = open(&path, &hooks).unwrap();
    assert_eq!(db.sketch_rebuilds(), 2, "seed {seed}");
    assert_eq!(
        ids(db.search_selective(&query, 10, 8, &[]).unwrap()),
        before
    );
    check(&db, &mut rng, seed, usize::MAX);
}

#[test]
fn interrupted_pack_publication_leaves_prior_root_and_cleans_orphans() {
    for after in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("db");
        let hooks = Arc::new(Mutex::new(Hooks::default()));
        let mut db = open(&path, &hooks).unwrap();
        let mut rng = Rng(7);
        let request = |boundary: u64, nonce: u8, rng: &mut Rng| Request {
            id: RequestId {
                boundary,
                nonce: [nonce; 16],
            },
            conditions: Vec::new(),
            mutations: (0..100)
                .map(|id| Mutation::Put {
                    id: id + 100 * u64::from(nonce),
                    vector: rng.vector(),
                    metadata: BTreeMap::from([("tag".into(), "hot".into())]),
                })
                .collect(),
        };
        db.apply_request(request(0, 0, &mut rng)).unwrap();
        db.seal_delta().unwrap();
        db.apply_request(request(1, 1, &mut rng)).unwrap();
        hooks.lock().unwrap().fail_create = Some(("sgpack-".into(), after));
        assert!(db.seal_delta().is_err());
        assert!(matches!(
            db.apply_request(request(2, 2, &mut rng)),
            Err(Error::RecoveryRequired)
        ));
        drop(db);
        let mut db = open(&path, &hooks).unwrap();
        assert_eq!(db.run_count(), 1);
        assert_eq!(db.sketch_rebuilds(), 0);
        check(&db, &mut rng, 7, 0);
        db.seal_delta().unwrap();
        assert_eq!(db.run_count(), 2);
        while db.cleanup_step(16).unwrap() > 0 {}
        check(&db, &mut rng, 7, 1);
        drop(db);
        let keys = LocalStore::open(&path).unwrap().list().unwrap();
        let packs = keys.iter().filter(|k| k.starts_with("sgpack-")).count();
        assert_eq!(packs, 2, "after={after} keys={keys:?}");
    }
}

#[test]
fn namespace_options_are_declared_once() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    drop(
        SegmentedDatabase::open_with_options(LocalStore::open(&path).unwrap(), config(), options())
            .unwrap(),
    );
    let metadata = LocalStore::open(&path)
        .unwrap()
        .get("metadata")
        .unwrap()
        .unwrap();
    assert!(std::str::from_utf8(&metadata)
        .unwrap()
        .contains("\"version\":4"));
    assert!(matches!(SegmentedDatabase::open_with_options(
        LocalStore::open(&path).unwrap(), config(),
        SegmentedOptions { resident_filter: options().resident_filter, routed_keys: vec!["other".into()] }
    ), Err(Error::Invalid(message)) if message.contains("routed_keys")));
    assert!(SegmentedDatabase::open(LocalStore::open(&path).unwrap(), config()).is_err());
    let plain = temp.path().join("plain");
    let db = SegmentedDatabase::open(LocalStore::open(&plain).unwrap(), config()).unwrap();
    // Without a declared resident predicate the filter is post-filtered.
    assert!(db
        .search_selective(&[0.; DIMENSIONS], 1, 1, &[("tag", "hot")])
        .unwrap()
        .is_empty());
    drop(db);
    assert!(SegmentedDatabase::open_with_options(
        LocalStore::open(&plain).unwrap(),
        config(),
        options()
    )
    .is_err());
    for routed_keys in [
        vec!["".into()],
        vec!["b".into(), "a".into()],
        vec!["a".into(), "a".into()],
        vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()],
    ] {
        let invalid = temp.path().join("invalid");
        assert!(matches!(
            SegmentedDatabase::open_with_options(
                LocalStore::open(&invalid).unwrap(),
                config(),
                SegmentedOptions {
                    resident_filter: None,
                    routed_keys
                }
            ),
            Err(Error::Invalid(_))
        ));
    }
}

#[test]
fn routed_dictionary_overflow_keeps_matches_after_reopen() {
    let seed = 0x30_254_u64;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("overflow");
    let open = || {
        SegmentedDatabase::open_with_options(
            LocalStore::open(&path).unwrap(),
            config(),
            SegmentedOptions {
                resident_filter: None,
                routed_keys: vec!["group".into(), "route".into()],
            },
        )
    };
    let mut db = open().unwrap();
    for batch in 0..3_u64 {
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: batch.to_le_bytes().repeat(2).try_into().unwrap(),
            },
            conditions: Vec::new(),
            mutations: (batch * 100..batch * 100 + 100)
                .map(|id| Mutation::Put {
                    id,
                    vector: vec![id as f32; DIMENSIONS],
                    metadata: BTreeMap::from([
                        (
                            "group".into(),
                            if id % 2 == 0 { "even" } else { "odd" }.into(),
                        ),
                        ("route".into(), format!("{id:03}")),
                    ]),
                })
                .collect(),
        })
        .unwrap();
    }
    db.seal_delta().unwrap();
    for id in [0, 253, 254, 299] {
        let query = vec![id as f32; DIMENSIONS];
        let value = format!("{id:03}");
        let filter = [("route", value.as_str())];
        assert_eq!(
            ids(db
                .search_selective(&query, 10, db.block_count(), &filter)
                .unwrap()),
            ids(db.search_exact(&query, 10, &filter).unwrap()),
            "seed {seed} id {id}"
        );
    }
    drop(db);
    let db = open().unwrap();
    assert_eq!(db.sketch_rebuilds(), 0, "seed {seed}");
    let query = vec![299.; DIMENSIONS];
    assert_eq!(
        ids(db
            .search_selective(&query, 10, db.block_count(), &[("route", "299")])
            .unwrap()),
        ids(db.search_exact(&query, 10, &[("route", "299")]).unwrap()),
        "seed {seed}"
    );
    for filter in [
        vec![("group", "odd"), ("route", "299")],
        vec![("group", "even"), ("route", "299")],
        vec![("route", "missing")],
    ] {
        assert_eq!(
            ids(db
                .search_selective(&query, 10, db.block_count(), &filter)
                .unwrap()),
            ids(db.search_exact(&query, 10, &filter).unwrap()),
            "seed {seed} filter {filter:?}"
        );
    }
}

#[test]
fn writes_between_seal_steps_keep_sealed_and_newer_versions() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let hooks = Arc::new(Mutex::new(Hooks::default()));
    let mut db = open(&path, &hooks).unwrap();
    let mut rng = Rng(99);
    let mut model = BTreeMap::new();
    let mut nonce = 0_u128;
    let mut write = |db: &mut SegmentedDatabase<HookStore>,
                     ids: Vec<u64>,
                     rng: &mut Rng,
                     model: &mut BTreeMap<u64, Vec<f32>>| {
        nonce += 1;
        let mutations = ids
            .into_iter()
            .map(|id| {
                let vector = rng.vector();
                model.insert(id, vector.clone());
                Mutation::Put {
                    id,
                    vector,
                    metadata: BTreeMap::new(),
                }
            })
            .collect();
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: nonce.to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations,
        })
        .unwrap();
    };
    for batch in 0..30_u64 {
        write(
            &mut db,
            (batch * 100..batch * 100 + 100).collect(),
            &mut rng,
            &mut model,
        );
    }
    db.start_seal().unwrap();
    let mut steps = 0;
    // Overwrite sealed IDs after every staged step, including twice per ID.
    while db.seal_step().unwrap() {
        steps += 1;
        let ids = (0..50)
            .map(|_| rng.below(3_000))
            .collect::<std::collections::BTreeSet<_>>();
        write(&mut db, ids.into_iter().collect(), &mut rng, &mut model);
    }
    assert!(steps >= 4, "multi-pack seal expected, got {steps} steps");
    let check = |db: &SegmentedDatabase<HookStore>, rng: &mut Rng| {
        for _ in 0..5 {
            let query = rng.vector();
            let mut expected: Vec<_> = model
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
            expected.sort_unstable();
            let found: Vec<_> = db
                .search_exact(&query, 10, &[])
                .unwrap()
                .into_iter()
                .map(|n| (n.distance.to_bits(), n.id))
                .collect();
            assert_eq!(found, expected[..10]);
            let swapped: Vec<_> = found.iter().map(|&(bits, id)| (id, bits)).collect();
            assert_eq!(
                ids(db
                    .search_selective(&query, 10, db.block_count(), &[])
                    .unwrap()),
                swapped
            );
        }
    };
    check(&db, &mut rng);
    for (&id, vector) in &model {
        assert_eq!(&db.get(id).unwrap().unwrap().vector, vector);
    }
    drop(db);
    let db = open(&path, &hooks).unwrap();
    check(&db, &mut rng);
}
