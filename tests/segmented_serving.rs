use glider::{
    admission::{Limits, Service, Shutdown},
    retry::{Request, RequestId},
    segmented::{
        QueryOptions, SegmentedDatabase, SegmentedOptions, SegmentedServing,
        SegmentedServingOptions,
    },
    store::LocalStore,
    Config, Metric, Mutation,
};
use std::{collections::BTreeMap, path::Path};

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
    let service = Service::start(
        SegmentedServing::open(
            LocalStore::open(&path).unwrap(),
            config(),
            options(),
            serving(&temp.path().join("cache")),
        )
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
