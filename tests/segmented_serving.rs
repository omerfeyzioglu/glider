use glider::{
    admission::{Client, Engine, Limits, QueryResult, Service, Shutdown},
    retry::{Request, RequestId},
    segmented::{
        QueryOptions, ReadBudget, SegmentedDatabase, SegmentedOptions, SegmentedServing,
        SegmentedServingOptions,
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
        warm_unit_bytes: 64 * 1024,
        cluster_probes: 16,
        auto_cluster_rows: 0,
        auto_cluster: glider::segmented::ConvertOptions::default(),
        auto_recluster_factor: 0,
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
fn index_admission_rejects_before_commit_and_keeps_group_retry_decisions() {
    let temp = tempfile::tempdir().unwrap();
    let mut limits = serving(&temp.path().join("cache"));
    limits.cache = None;
    limits.max_index_bytes = 4096;
    let mut engine = SegmentedServing::open(
        LocalStore::open(temp.path()).unwrap(),
        config(),
        options(),
        limits,
    )
    .unwrap();
    let make = |nonce: u8, mutations| Request {
        id: RequestId {
            boundary: 1,
            nonce: [nonce; 16],
        },
        conditions: vec![],
        mutations,
    };
    let first = make(
        1,
        vec![Mutation::Put {
            id: 1,
            vector: vector(1, 0),
            metadata: BTreeMap::new(),
        }],
    );
    let rejected = make(
        2,
        (10..110)
            .map(|id| Mutation::Put {
                id,
                vector: vector(id, 0),
                metadata: BTreeMap::new(),
            })
            .collect(),
    );
    let mut conditional = rejected.clone();
    conditional.id.nonce = [3; 16];
    conditional.conditions = vec![glider::retry::Revision { id: 1, boundary: 1 }];
    let delete = make(4, vec![Mutation::Delete { id: 1 }]);
    let results = engine.apply_requests(vec![
        first.clone(),
        rejected.clone(),
        first.clone(),
        conditional,
        delete,
    ]);
    assert_eq!(results[0].as_ref().unwrap(), results[2].as_ref().unwrap());
    assert!(matches!(
        results[1],
        Err(glider::Error::CapacityExceeded(_))
    ));
    assert_eq!(
        results[4].as_ref().unwrap().sequence,
        results[0].as_ref().unwrap().sequence + 2
    );
    assert!(results[3].as_ref().unwrap().conflict.is_some());
    assert_eq!(
        engine.lookup_request(rejected.id).unwrap(),
        glider::retry::Lookup::Unknown
    );
    assert!(!engine.recovery_required());
    assert!(engine.database().get(1).unwrap().is_none());
    assert!(engine.database().get(10).unwrap().is_none());
    engine.close().unwrap();
    let db = SegmentedDatabase::open_with_options(
        LocalStore::open(temp.path()).unwrap(),
        config(),
        options(),
    )
    .unwrap();
    assert_eq!(
        db.lookup_request(first.id).unwrap(),
        glider::retry::Lookup::Retained(*results[0].as_ref().unwrap())
    );
    assert_eq!(
        db.lookup_request(rejected.id).unwrap(),
        glider::retry::Lookup::Unknown
    );
    assert!(db.search_exact(&vector(0, 0), 100, &[]).unwrap().is_empty());
}

#[test]
fn oversized_recovery_allows_reads_retries_and_deletes_then_restores_capacity() {
    let temp = tempfile::tempdir().unwrap();
    let cfg = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let mut db = SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), cfg).unwrap();
    let request = Request {
        id: RequestId {
            boundary: 0,
            nonce: [1; 16],
        },
        conditions: vec![],
        mutations: (0..100)
            .map(|id| Mutation::Put {
                id,
                vector: vec![id as f32; 128],
                metadata: BTreeMap::new(),
            })
            .collect(),
    };
    let ack = db.apply_request(request.clone()).unwrap();
    db.seal_delta().unwrap();
    assert!(db.selective_index_bytes() > 8192);
    drop(db);
    let mut limits = serving(&temp.path().join("cache"));
    limits.cache = None;
    limits.max_index_bytes = 8192;
    limits.seal_tail_objects = 32; // pressure must seal even below the normal threshold
    let mut engine = SegmentedServing::open(
        LocalStore::open(temp.path()).unwrap(),
        cfg,
        SegmentedOptions::default(),
        limits.clone(),
    )
    .unwrap();
    assert_eq!(engine.apply_request(request).unwrap(), ack);
    assert_eq!(
        engine
            .database()
            .search_exact(&vec![0.; 128], 100, &[])
            .unwrap()
            .len(),
        100
    );
    let rejected = Request {
        id: RequestId {
            boundary: engine.sequence(),
            nonce: [2; 16],
        },
        conditions: vec![],
        mutations: vec![Mutation::Put {
            id: 101,
            vector: vec![1.; 128],
            metadata: BTreeMap::new(),
        }],
    };
    let before = engine.sequence();
    assert!(matches!(
        engine.apply_request(rejected),
        Err(glider::Error::CapacityExceeded(_))
    ));
    assert_eq!(engine.sequence(), before);
    engine
        .apply_request(Request {
            id: RequestId {
                boundary: engine.sequence(),
                nonce: [3; 16],
            },
            conditions: vec![],
            mutations: (0..100).map(|id| Mutation::Delete { id }).collect(),
        })
        .unwrap();
    for step in 0..1000 {
        if !engine.maintenance_step().unwrap() {
            break;
        }
        assert!(step < 999, "maintenance failed to settle");
    }
    assert!(engine.database().selective_index_bytes() < 8192);
    engine
        .apply_request(Request {
            id: RequestId {
                boundary: engine.sequence(),
                nonce: [4; 16],
            },
            conditions: vec![],
            mutations: vec![Mutation::Put {
                id: 101,
                vector: vec![1.; 128],
                metadata: BTreeMap::new(),
            }],
        })
        .unwrap();
    engine.close().unwrap();
    let reopened = SegmentedServing::open(
        LocalStore::open(temp.path()).unwrap(),
        cfg,
        SegmentedOptions::default(),
        limits,
    )
    .unwrap();
    assert!(reopened.database().get(101).unwrap().is_some());
    assert!(reopened.database().get(1).unwrap().is_none());
}

#[test]
fn resident_fields_charge_only_final_hit_block_reads() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut db =
        SegmentedDatabase::open_with_options(LocalStore::open(&path).unwrap(), config(), options())
            .unwrap();
    db.apply_request(Request {
        id: RequestId {
            boundary: 0,
            nonce: [1; 16],
        },
        conditions: Vec::new(),
        mutations: (0..5)
            .map(|id| Mutation::Put {
                id,
                vector: vector(id, 0),
                metadata: BTreeMap::from([("cohort".into(), "one-percent".into())]),
            })
            .collect(),
    })
    .unwrap();
    db.seal_delta().unwrap();
    drop(db);
    // Idle warm-up runs beside queries and could cache the hit's block
    // before the query reads it; this test counts the query's own reads.
    let mut cold = serving(&temp.path().join("cache"));
    cold.warm_unit_bytes = 0;
    let service = Service::start(
        SegmentedServing::open(LocalStore::open(&path).unwrap(), config(), options(), cold)
            .unwrap(),
        Limits::default(),
    )
    .unwrap();
    let client = service.client();
    let filter = vec![("cohort".into(), "one-percent".into())];
    let plain = client
        .query(vector(0, 0), 1, filter.clone())
        .unwrap()
        .wait()
        .unwrap()
        .value;
    assert_eq!(plain.remote_reads, 0);
    let with_fields = client
        .query_with_options(
            vector(0, 0),
            1,
            filter,
            QueryOptions {
                include_metadata: true,
                include_vector: true,
            },
        )
        .unwrap()
        .wait()
        .unwrap()
        .value;
    assert_eq!(with_fields.remote_reads, 1);
    assert_eq!(with_fields.hits.len(), 1);
    assert_eq!(
        with_fields.hits[0].metadata.as_ref().unwrap()["cohort"],
        "one-percent"
    );
    service.shutdown(Shutdown::Drain).unwrap();

    let mut no_cache = serving(&temp.path().join("unused-cache"));
    no_cache.cache = None;
    let service = Service::start(
        SegmentedServing::open(
            LocalStore::open(&path).unwrap(),
            config(),
            options(),
            no_cache,
        )
        .unwrap(),
        Limits::default(),
    )
    .unwrap();
    let result = service
        .client()
        .query_with_options(
            vector(0, 0),
            1,
            vec![("cohort".into(), "one-percent".into())],
            QueryOptions {
                include_metadata: true,
                include_vector: false,
            },
        )
        .unwrap()
        .wait()
        .unwrap()
        .value;
    assert_eq!(result.remote_reads, 1);
    service.shutdown(Shutdown::Drain).unwrap();
}

#[test]
fn index_reservation_keeps_versions_displaced_by_a_staged_seal() {
    let temp = tempfile::tempdir().unwrap();
    let mut limits = serving(&temp.path().join("cache"));
    limits.cache = None;
    limits.seal_tail_objects = 1;
    limits.max_index_bytes = 4096;
    let mut engine = SegmentedServing::open(
        LocalStore::open(temp.path()).unwrap(),
        config(),
        SegmentedOptions::default(),
        limits,
    )
    .unwrap();
    let put = |id| Mutation::Put {
        id,
        vector: vector(id, 0),
        metadata: BTreeMap::new(),
    };
    let make = |boundary, nonce, mutations| Request {
        id: RequestId {
            boundary,
            nonce: [nonce; 16],
        },
        conditions: vec![],
        mutations,
    };
    engine
        .apply_request(make(engine.sequence(), 1, vec![put(1), put(2)]))
        .unwrap();
    engine.maintenance_step().unwrap(); // freeze both versions before publishing a pack
    assert_eq!(engine.last_unit(), "seal_plan");
    engine
        .apply_request(make(engine.sequence(), 2, vec![Mutation::Delete { id: 1 }]))
        .unwrap();
    assert!(matches!(
        engine.apply_request(make(engine.sequence(), 3, vec![put(3)])),
        Err(glider::Error::CapacityExceeded(_))
    ));
    for step in 0..1000 {
        if !engine.maintenance_step().unwrap() {
            break;
        }
        assert!(step < 999);
    }
    engine
        .apply_request(make(engine.sequence(), 4, vec![put(3)]))
        .unwrap();
    assert!(engine.database().get(1).unwrap().is_none());
    assert!(engine.database().get(2).unwrap().is_some());
    assert!(engine.database().get(3).unwrap().is_some());
}

#[test]
fn retained_retry_does_not_schedule_pressure_maintenance() {
    let temp = tempfile::tempdir().unwrap();
    let mut limits = serving(&temp.path().join("cache"));
    limits.cache = None;
    limits.max_index_bytes = 4096;
    limits.seal_tail_objects = 32;
    let mut engine = SegmentedServing::open(
        LocalStore::open(temp.path()).unwrap(),
        config(),
        SegmentedOptions::default(),
        limits,
    )
    .unwrap();
    let make = |nonce, ids: Vec<u64>| Request {
        id: RequestId {
            boundary: 1,
            nonce: [nonce; 16],
        },
        conditions: vec![],
        mutations: ids
            .into_iter()
            .map(|id| Mutation::Put {
                id,
                vector: vector(id, 0),
                metadata: BTreeMap::new(),
            })
            .collect(),
    };
    let first = make(1, vec![1, 2]);
    let ack = engine.apply_request(first.clone()).unwrap();
    assert!(matches!(
        engine.apply_request(make(2, vec![3])),
        Err(glider::Error::CapacityExceeded(_))
    ));
    for _ in 0..3 {
        assert_eq!(engine.apply_request(first.clone()).unwrap(), ack);
        assert_eq!(
            engine.apply_requests(vec![first.clone()])[0]
                .as_ref()
                .unwrap(),
            &ack
        );
    }
    assert_eq!(engine.counters().seal_starts, 0);
    assert_eq!(engine.maintenance_time(), Duration::ZERO);
}

/// Keep a successor queued at every write boundary, so idle maintenance
/// cannot make the conservative tail reservations fit by accident.
#[test]
fn sustained_write_queue_releases_index_reservations_without_idle_time() {
    use glider::retry::{Lookup, Outcome, Revision};
    struct Gated {
        inner: SegmentedServing<LocalStore>,
        entered: mpsc::SyncSender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Engine for Gated {
        fn close(self) -> glider::Result<()> {
            self.inner.close()
        }
        fn idle_step(&mut self) -> glider::Result<bool> {
            self.inner.idle_step()
        }
        fn config(&self) -> Config {
            self.inner.config()
        }
        fn sequence(&self) -> u64 {
            self.inner.sequence()
        }
        fn recovery_required(&self) -> bool {
            self.inner.recovery_required()
        }
        fn maintenance_time(&self) -> Duration {
            self.inner.maintenance_time()
        }
        fn apply_request(&mut self, request: Request) -> glider::Result<Outcome> {
            self.entered.send(()).unwrap();
            self.release.recv_timeout(Duration::from_secs(10)).unwrap();
            self.inner.apply_request(request)
        }
        fn revision(&self, id: u64) -> Revision {
            self.inner.revision(id)
        }
        fn request_id(&self) -> glider::Result<RequestId> {
            self.inner.request_id()
        }
        fn lookup_request(&self, id: RequestId) -> glider::Result<Lookup> {
            self.inner.lookup_request(id)
        }
        fn get(&self, id: u64) -> glider::Result<Option<glider::streaming::OwnedDocument>> {
            self.inner.get(id)
        }
        fn query(
            &mut self,
            query: &[f32],
            k: usize,
            filter: &[(&str, &str)],
        ) -> glider::Result<Vec<glider::Neighbor>> {
            self.inner.query(query, k, filter)
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let mut limits = serving(&temp.path().join("cache"));
    limits.cache = None;
    limits.max_index_bytes = 4096;
    limits.seal_tail_objects = 32;
    let mut engine = SegmentedServing::open(
        LocalStore::open(temp.path()).unwrap(),
        config(),
        SegmentedOptions::default(),
        limits,
    )
    .unwrap();
    let request = |nonce: u8, ids: Vec<u64>| Request {
        id: RequestId {
            boundary: 1,
            nonce: [nonce; 16],
        },
        conditions: vec![],
        mutations: ids
            .into_iter()
            .map(|id| Mutation::Put {
                id,
                vector: vector(id, 0),
                metadata: BTreeMap::new(),
            })
            .collect(),
    };
    engine.apply_request(request(1, vec![1, 2])).unwrap();
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::channel();
    let service = Service::start(
        Gated {
            inner: engine,
            entered: entered_tx,
            release: release_rx,
        },
        Limits::default(),
    )
    .unwrap();
    let client = service.client();
    let mut ticket = client.write(request(2, vec![3])).unwrap();
    let mut accepted = false;
    for nonce in 3..=18 {
        entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let next = client.write(request(nonce, vec![3])).unwrap();
        release_tx.send(()).unwrap();
        match ticket.wait() {
            Ok(_) => accepted = true,
            Err(glider::admission::Error::Database(glider::Error::CapacityExceeded(_))) => {}
            result => panic!("unexpected pressure result: {result:?}"),
        }
        ticket = next;
    }
    entered_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    release_tx.send(()).unwrap();
    accepted |= ticket.wait().is_ok();
    service.shutdown(Shutdown::Drain).unwrap();
    assert!(
        accepted,
        "queued writes permanently starved capacity recovery"
    );
    let db = SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config()).unwrap();
    for id in 1..=3 {
        assert!(db.get(id).unwrap().is_some());
    }
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
    // The reopen took over: its takeover record is the next sequence.
    assert_eq!(db.database().sequence(), sequence + 1);
    assert_eq!(db.database().epoch(), sequence + 1);
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
    assert_eq!(restored.sequence(), sequence + 1);
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
    // The acknowledged state at every sequence. Sequence 1 is the opening
    // takeover record, which changes no document.
    assert!(
        acknowledged.keys().copied().eq(2..=WRITERS * WRITES + 1),
        "seed {SEED:#x}"
    );
    let mut states = vec![BTreeMap::new(), BTreeMap::new()];
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
    let before = old
        .query(&ORIGIN, ROWS as usize, &[], QueryOptions::default())
        .unwrap();
    assert_eq!(before.sequence, first);
    assert_eq!(generations(&before), uniform(0));
    assert!(before.remote_reads > 0);
    let sealed = packs(&path);
    let (entered, release) = store.reads.arm("sgpack-");
    let blocked = {
        let old = old.clone();
        std::thread::spawn(move || {
            old.query(&ORIGIN, ROWS as usize, &[], QueryOptions::default())
                .unwrap()
        })
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
    let latest = current
        .query(&ORIGIN, ROWS as usize, &[], QueryOptions::default())
        .unwrap();
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

/// Idle warm-up holds no cache lock during its range read, so a query that
/// looks up and admits cached blocks completes while the warm-up read waits.
#[test]
fn queries_do_not_wait_for_a_warm_up_read() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cache = temp.path().join("cache");
    {
        let mut db = SegmentedServing::open(
            LocalStore::open(&path).unwrap(),
            config(),
            options(),
            SegmentedServingOptions {
                cache: None,
                ..serving(&cache)
            },
        )
        .unwrap();
        for round in 0..30_u64 {
            db.apply_request(Request {
                id: RequestId {
                    boundary: db.sequence(),
                    nonce: u128::from(round).to_le_bytes(),
                },
                conditions: Vec::new(),
                mutations: (round * 100..(round + 1) * 100)
                    .map(|id| Mutation::Put {
                        id,
                        vector: point(id),
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
            while db.maintenance_step().unwrap() {}
        }
        while db.maintenance_step().unwrap() {}
        db.close().unwrap();
    }
    let store = Gated::new(LocalStore::open(&path).unwrap());
    let mut db =
        SegmentedServing::open(store.clone(), config(), options(), serving(&cache)).unwrap();
    // Run maintenance through the first warm-up unit; later packs stay cold.
    while db.maintenance_step().unwrap() && db.last_unit() != "warm" {}
    assert_eq!(db.last_unit(), "warm");
    let (entered, release) = store.reads.arm("sgpack-");
    let service = Service::start(db, Limits::default()).unwrap();
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // The committer is inside the next warm-up range read.
    let (done, finished) = mpsc::channel();
    let client = service.client();
    std::thread::spawn(move || {
        let result = client
            .query(point(5_000_000), 10, Vec::new())
            .unwrap()
            .wait();
        done.send(result.map(|timed| timed.value.neighbors.len()))
            .unwrap();
    });
    let found = finished.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(found.unwrap(), 10);
    release.send(()).unwrap();
    let client = service.client();
    let warm_fetches = || {
        client
            .metrics()
            .unwrap()
            .wait()
            .unwrap()
            .value
            .samples
            .into_iter()
            .find(|(name, _)| *name == "glider_cache_warm_fetches_total")
            .unwrap()
            .1
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while warm_fetches() < 2 {
        assert!(
            Instant::now() < deadline,
            "the released warm-up read finished"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    service.shutdown(Shutdown::Drain).unwrap();
}

/// Deterministic, well spread 4-dimensional point for `id`.
fn point(id: u64) -> Vec<f32> {
    let mut state = id.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x5851_f42d_4c95_7f2d;
    (0..4)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % 10_000) as f32 / 100.
        })
        .collect()
}

/// A namespace of `rows` documents sealed 300 at a time into separate packs,
/// plus a short unsealed tail.
fn sealed(path: &Path, rows: u64) -> SegmentedDatabase<LocalStore> {
    let mut db =
        SegmentedDatabase::open_with_options(LocalStore::open(path).unwrap(), config(), options())
            .unwrap();
    let write = |db: &mut SegmentedDatabase<LocalStore>, ids: std::ops::Range<u64>| {
        let request = Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: u128::from(ids.start).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations: ids
                .map(|id| Mutation::Put {
                    id,
                    vector: point(id),
                    metadata: BTreeMap::new(),
                })
                .collect(),
        };
        db.apply_request(request).unwrap();
    };
    for start in (0..rows).step_by(300) {
        for batch in (start..(start + 300).min(rows)).step_by(100) {
            write(&mut db, batch..(batch + 100).min(rows));
        }
        db.seal_delta().unwrap();
    }
    write(&mut db, rows..rows + 5);
    db
}

fn reopen(path: &Path) -> SegmentedDatabase<LocalStore> {
    SegmentedDatabase::open_with_options(LocalStore::open(path).unwrap(), config(), options())
        .unwrap()
        .with_query_threads(2)
}

fn stats<S: glider::store::ObjectStore>(
    db: &SegmentedDatabase<S>,
) -> glider::segmented::CacheStats {
    db.cache_stats().unwrap().unwrap()
}

#[test]
fn remote_limits_apply_only_to_uncached_blocks() {
    use glider::segmented::ReadBudget;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let blocks = sealed(&path, 3_000).block_count();
    assert!(blocks >= 12, "{blocks} blocks");
    let queries: Vec<_> = (0..12).map(|n| point(1_000_000 + n)).collect();
    let budget = ReadBudget {
        blocks: 4,
        requests: 2,
        bytes: 32 * 1024,
        local_blocks: 6,
    };
    let cold_budget = ReadBudget {
        local_blocks: 0,
        ..budget
    };

    // An empty cache (nothing is ever admitted) reads exactly what a handle
    // without a cache reads, within the remote limits.
    let uncached = reopen(&path);
    let empty = reopen(&path)
        .with_block_cache(temp.path().join("empty"), 0, 0)
        .unwrap();
    let mut cold = Vec::new();
    for query in &queries {
        let before = stats(&empty);
        let found = empty
            .search_selective_within(query, 10, budget, &[])
            .unwrap();
        let after = stats(&empty);
        assert!(after.remote_fetches - before.remote_fetches <= 2);
        assert!(after.remote_payload_bytes - before.remote_payload_bytes <= 32 * 1024);
        assert_eq!(
            found,
            uncached
                .search_selective_within(query, 10, budget, &[])
                .unwrap()
        );
        assert_eq!(
            found,
            uncached
                .search_selective_within(query, 10, cold_budget, &[])
                .unwrap()
        );
        cold.push(found);
    }

    // Warm the whole namespace: cached blocks are read locally, so no query
    // issues a remote request, and `local_blocks == 0` keeps the cold choice.
    let warm = reopen(&path)
        .with_block_cache(temp.path().join("warm"), 0, 64 * 1024 * 1024)
        .unwrap();
    while warm.warm_cache_step(8 * 1024).unwrap() {}
    let warmed = stats(&warm);
    assert!(warmed.warm_complete);
    assert_eq!(warmed.warm_bytes, warmed.namespace_bytes);
    assert_eq!(warmed.remote_fetches, 0);
    for (query, cold) in queries.iter().zip(&cold) {
        assert_eq!(
            &warm
                .search_selective_within(query, 10, cold_budget, &[])
                .unwrap(),
            cold
        );
        let before = stats(&warm);
        let found = warm
            .search_selective_within(query, 10, budget, &[])
            .unwrap();
        let after = stats(&warm);
        assert_eq!(after.nvme_hits - before.nvme_hits, 6);
        assert_eq!(found.len(), 10);
        // A remote byte limit too small for any block reads nothing cold;
        // warm, the six best cached candidates are still reranked.
        let starved = ReadBudget { bytes: 1, ..budget };
        assert!(uncached
            .search_selective_within(query, 10, starved, &[])
            .unwrap()
            .iter()
            .all(|neighbor| neighbor.id >= 3_000));
        let before = stats(&warm);
        assert_eq!(
            warm.search_selective_within(query, 10, starved, &[])
                .unwrap()
                .len(),
            10
        );
        assert_eq!(stats(&warm).nvme_hits - before.nvme_hits, 6);
        // With a local limit covering every block, warm search is exact.
        let all = ReadBudget {
            local_blocks: blocks,
            ..starved
        };
        assert_eq!(
            warm.search_selective_within(query, 10, all, &[]).unwrap(),
            warm.search_exact(query, 10, &[]).unwrap()
        );
    }
    assert_eq!(stats(&warm).remote_fetches, 0);

    // A partially warm cache still bounds remote reads per query.
    let partial = reopen(&path)
        .with_block_cache(temp.path().join("partial"), 0, warmed.namespace_bytes / 3)
        .unwrap();
    while partial.warm_cache_step(8 * 1024).unwrap() {}
    assert!(stats(&partial).warm_bytes < warmed.namespace_bytes);
    for query in &queries {
        let before = stats(&partial);
        partial
            .search_selective_within(query, 10, budget, &[])
            .unwrap();
        let after = stats(&partial);
        assert!(after.remote_fetches - before.remote_fetches <= 2);
        assert!(after.remote_payload_bytes - before.remote_payload_bytes <= 32 * 1024);
    }
}

#[test]
fn warm_up_fills_the_cache_within_its_bound_and_stops() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut db = sealed(&path, 3_000);
    let namespace = {
        let probe = reopen(&path)
            .with_block_cache(temp.path().join("probe"), 0, usize::MAX)
            .unwrap();
        assert!(probe.warm_cache_step(8 * 1024).unwrap());
        stats(&probe).namespace_bytes
    };

    // Half the namespace fits: warm-up fills free space, never more than the
    // limit, then stops without further reads.
    let limit = namespace / 2;
    let half = reopen(&path)
        .with_block_cache(temp.path().join("half"), 0, limit)
        .unwrap();
    let mut steps = 0;
    while half.warm_cache_step(8 * 1024).unwrap() {
        steps += 1;
        assert!(steps < 1_000, "warm-up did not stop");
        assert!(stats(&half).nvme_bytes <= limit);
    }
    let stopped = stats(&half);
    assert!(stopped.warm_complete);
    assert!(stopped.warm_bytes < namespace && stopped.nvme_bytes > 0);
    assert!(!half.warm_cache_step(8 * 1024).unwrap());
    assert_eq!(stats(&half).warm_fetches, stopped.warm_fetches);

    // The whole namespace fits: each unit reads at most one bounded range,
    // and the pass ends with every block cached.
    let directory = temp.path().join("full");
    {
        let full = reopen(&path)
            .with_block_cache(&directory, 0, namespace)
            .unwrap();
        let mut before = stats(&full);
        while full.warm_cache_step(8 * 1024).unwrap() {
            let after = stats(&full);
            assert!(after.warm_fetches - before.warm_fetches <= 1);
            assert!(after.warm_payload_bytes - before.warm_payload_bytes <= 128 * 1024);
            before = after;
        }
        let done = stats(&full);
        assert!(done.warm_complete && done.warm_bytes == namespace);
        assert!(done.nvme_bytes <= namespace);
    }

    // After a restart the cache is already warm: no reads. A new seal
    // publishes a root whose new pack the next pass fetches.
    let fetched = {
        let restarted = reopen(&path)
            .with_block_cache(&directory, 0, 2 * namespace)
            .unwrap();
        while restarted.warm_cache_step(8 * 1024).unwrap() {}
        assert_eq!(stats(&restarted).warm_fetches, 0);
        assert_eq!(stats(&restarted).warm_bytes, namespace);
        stats(&restarted).nvme_bytes
    };
    db.seal_delta().unwrap();
    let db = db.with_block_cache(&directory, 0, 2 * namespace).unwrap();
    while db.warm_cache_step(8 * 1024).unwrap() {}
    let grown = stats(&db);
    assert!(grown.warm_fetches > 0 && grown.nvme_bytes > fetched);
    assert!(grown.warm_complete && grown.warm_bytes == grown.namespace_bytes);
}

/// Idle maintenance warms the namespace after open; losing the warm cache
/// only returns queries to the cold read choice, and warm-up refills it.
#[test]
fn idle_maintenance_warms_the_cache_and_survives_its_loss() {
    use glider::admission::Engine;
    use glider::segmented::ReadBudget;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cache = temp.path().join("cache");
    let blocks = {
        let mut db = SegmentedServing::open(
            LocalStore::open(&path).unwrap(),
            config(),
            options(),
            SegmentedServingOptions {
                cache: None,
                ..serving(&cache)
            },
        )
        .unwrap();
        for round in 0..30_u64 {
            db.apply_request(Request {
                id: RequestId {
                    boundary: db.sequence(),
                    nonce: u128::from(round).to_le_bytes(),
                },
                conditions: Vec::new(),
                mutations: (round * 100..(round + 1) * 100)
                    .map(|id| Mutation::Put {
                        id,
                        vector: point(id),
                        metadata: BTreeMap::new(),
                    })
                    .collect(),
            })
            .unwrap();
            while db.maintenance_step().unwrap() {}
        }
        while db.maintenance_step().unwrap() {}
        let blocks = db.database().block_count();
        db.close().unwrap();
        blocks
    };
    let options = SegmentedServingOptions {
        read_budget: ReadBudget {
            blocks: 2,
            requests: 1,
            bytes: 64 * 1024,
            local_blocks: blocks,
        },
        cache: Some((cache.clone(), 0, 16 * 1024 * 1024)),
        ..serving(&cache)
    };
    let cold_budget = ReadBudget {
        local_blocks: 0,
        ..options.read_budget
    };
    let open = || {
        SegmentedServing::open(
            LocalStore::open(&path).unwrap(),
            config(),
            self::options(),
            options.clone(),
        )
        .unwrap()
    };
    let queries: Vec<_> = (0..10).map(|n| point(2_000_000 + n)).collect();
    let mut db = open();
    while db.maintenance_step().unwrap() {}
    assert!(db.counters().warm_steps > 0);
    let warmed = stats(db.database());
    assert!(warmed.warm_complete && warmed.warm_bytes == warmed.namespace_bytes);
    let metrics = db.metrics().unwrap().samples;
    assert!(metrics.contains(&("glider_cache_warm_complete", 1)));
    assert!(metrics.contains(&("glider_cache_warm_bytes", warmed.warm_bytes as u64)));
    let mut cold = Vec::new();
    for query in &queries {
        assert_eq!(
            db.query(query, 10, &[]).unwrap(),
            db.database().search_exact(query, 10, &[]).unwrap()
        );
        cold.push(
            db.database()
                .search_selective_within(query, 10, cold_budget, &[])
                .unwrap(),
        );
    }
    assert_eq!(stats(db.database()).remote_fetches, 0);
    Engine::close(db).unwrap();

    // Total loss: queries succeed with the cold choice, then warm-up refills.
    std::fs::remove_dir_all(&cache).unwrap();
    let mut db = open();
    for (query, cold) in queries.iter().zip(&cold) {
        assert_eq!(
            &db.database()
                .search_selective_within(query, 10, cold_budget, &[])
                .unwrap(),
            cold
        );
        assert_eq!(db.query(query, 10, &[]).unwrap().len(), 10);
    }
    while db.maintenance_step().unwrap() {}
    for query in &queries {
        assert_eq!(
            db.query(query, 10, &[]).unwrap(),
            db.database().search_exact(query, 10, &[]).unwrap()
        );
    }

    // Partial loss under a warm handle: a query that finds an entry missing
    // falls back to remote reads, and the next pass refetches what was lost.
    // Idle maintenance after the reopen may have sealed the takeover record
    // into a new root, so the cold choice is taken again for this root; a
    // zero local limit makes it independent of the cache.
    let cold: Vec<_> = queries
        .iter()
        .map(|query| {
            db.database()
                .search_selective_within(query, 10, cold_budget, &[])
                .unwrap()
        })
        .collect();
    let files: Vec<_> = std::fs::read_dir(cache.join("glider-block-cache-v1"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    for file in files.iter().step_by(2) {
        std::fs::remove_file(file).unwrap();
    }
    for (query, cold) in queries.iter().zip(&cold) {
        assert_eq!(
            &db.database()
                .search_selective_within(query, 10, cold_budget, &[])
                .unwrap(),
            cold
        );
        db.query(query, 10, &[]).unwrap();
    }
    assert!(stats(db.database()).cache_io_errors > 0);
    while db.maintenance_step().unwrap() {}
    let refilled = stats(db.database());
    assert!(refilled.warm_fetches > 0 && refilled.warm_bytes == refilled.namespace_bytes);
    for query in &queries {
        assert_eq!(
            db.query(query, 10, &[]).unwrap(),
            db.database().search_exact(query, 10, &[]).unwrap()
        );
    }
    Engine::close(db).unwrap();
}
