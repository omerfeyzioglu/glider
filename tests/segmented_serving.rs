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
        warm_unit_bytes: 64 * 1024,
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
            db.maintenance_step().unwrap();
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
