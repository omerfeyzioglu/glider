use glider::{
    store::{LocalStore, ObjectStore},
    streaming::StreamingDatabase,
    Config, Database, Error, Metric, Mutation, Result,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

#[derive(Clone, Default)]
struct Memory {
    objects: Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
    gets: Rc<Cell<usize>>,
    get_bytes: Rc<Cell<usize>>,
}
impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.gets.set(self.gets.get() + 1);
        let value = self.objects.borrow().get(key).cloned();
        self.get_bytes
            .set(self.get_bytes.get() + value.as_ref().map_or(0, Vec::len));
        Ok(value)
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.objects.borrow().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if self.objects.borrow().contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        self.objects.borrow_mut().insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.objects.borrow_mut().remove(key);
        Ok(())
    }
}

#[test]
fn m8_filter_distribution_forces_nearly_full_chunk_scan() {
    let store = Memory::default();
    let config = Config {
        dimensions: 64,
        metric: Metric::SquaredEuclidean,
    };
    let mut db = Database::open(store.clone(), config).unwrap();
    let mut state = 42_u64;
    for batch_start in (0..2000).step_by(100) {
        let mut batch = Vec::new();
        for id in batch_start..batch_start + 100 {
            let vector = (0..64)
                .map(|_| {
                    state = state.wrapping_add(0x9e3779b97f4a7c15);
                    let mut z = state;
                    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
                    z ^= z >> 31;
                    ((z >> 40) as u32 as f32) * (1.0 / 8_388_608.0) - 1.0
                })
                .collect();
            let metadata = if id % 100 == 0 {
                BTreeMap::from([("selected".into(), "true".into())])
            } else {
                BTreeMap::new()
            };
            batch.push(Mutation::Put {
                id: id as u64,
                vector,
                metadata,
            });
        }
        db.apply_batch(batch).unwrap();
    }
    db.compact_chunked(131_072).unwrap();
    let chunks: Vec<_> = store
        .objects
        .borrow()
        .iter()
        .filter(|(key, _)| key.starts_with("compactedchunk-"))
        .map(|(_, bytes)| {
            let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
            let rows = value["documents"].as_array().unwrap();
            rows.iter()
                .filter(|row| row[1]["metadata"]["selected"] == "true")
                .count()
        })
        .collect();
    assert_eq!(chunks.iter().sum::<usize>(), 20);
    assert!(chunks.iter().filter(|&&count| count > 0).count() > 4);
    let query = vec![0.0; 64];
    let expected = db
        .search_filtered(&query, 10, &[("selected", "true")])
        .unwrap();
    drop(db);
    let reader = StreamingDatabase::open(store.clone(), config).unwrap();
    let before_gets = store.gets.get();
    let before_bytes = store.get_bytes.get();
    assert_eq!(
        reader
            .search_filtered(&query, 10, &[("selected", "true")])
            .unwrap(),
        expected
    );
    assert_eq!(store.gets.get() - before_gets, chunks.len());
    assert!(store.get_bytes.get() - before_bytes > 1_000_000);
    eprintln!(
        "M11 baseline: {} chunks, {} matching chunks, {} GET bytes",
        chunks.len(),
        chunks.iter().filter(|&&count| count > 0).count(),
        store.get_bytes.get() - before_bytes
    );
    let reader =
        StreamingDatabase::open_with_filter(store.clone(), config, "selected", "true", 64).unwrap();
    let before_gets = store.gets.get();
    let before_bytes = store.get_bytes.get();
    assert_eq!(
        reader
            .search_filtered(&query, 10, &[("selected", "true")])
            .unwrap(),
        expected
    );
    assert_eq!(store.gets.get() - before_gets, 0);
    assert_eq!(store.get_bytes.get() - before_bytes, 0);
}

#[test]
fn resident_filter_posting_applies_tail_and_rejects_missing_base_on_reopen() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    for (id, group) in [(1, "red"), (2, "red"), (3, "blue")] {
        db.put_with_metadata(id, vec![id as f32, 0.], metadata(group))
            .unwrap();
    }
    db.compact_chunked(256).unwrap();
    db.put_with_metadata(1, vec![0., 0.], metadata("blue"))
        .unwrap();
    db.delete(2).unwrap();
    db.put_with_metadata(4, vec![3., 0.], metadata("red"))
        .unwrap();
    db.put_with_metadata(3, vec![3., 0.], metadata("red"))
        .unwrap();
    let query = [0., 0.];
    let expected = db.search_filtered(&query, 10, &[("group", "red")]).unwrap();
    let expected_blue = db
        .search_filtered(&query, 10, &[("group", "blue")])
        .unwrap();
    assert_eq!(
        expected,
        vec![
            glider::Neighbor {
                id: 3,
                distance: 9.
            },
            glider::Neighbor {
                id: 4,
                distance: 9.
            },
        ]
    );
    assert_eq!(
        expected_blue,
        vec![glider::Neighbor {
            id: 1,
            distance: 0.
        }]
    );
    drop(db);

    let reader =
        StreamingDatabase::open_with_filter(store.clone(), cfg, "group", "red", 64).unwrap();
    let before_gets = store.gets.get();
    assert_eq!(
        reader
            .search_filtered(&query, 10, &[("group", "red")])
            .unwrap(),
        expected
    );
    assert_eq!(store.gets.get(), before_gets);
    assert_eq!(
        reader
            .search_filtered(&query, 10, &[("group", "blue")])
            .unwrap(),
        expected_blue
    );
    assert!(store.gets.get() > before_gets); // Unindexed predicates stay exact.
    drop(reader);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.compact_chunked(256).unwrap();
    drop(db);
    let reader =
        StreamingDatabase::open_with_filter(store.clone(), cfg, "group", "red", 64).unwrap();
    let before_gets = store.gets.get();
    assert_eq!(
        reader
            .search_filtered(&query, 10, &[("group", "red")])
            .unwrap(),
        expected
    );
    assert_eq!(store.gets.get(), before_gets);
    drop(reader);
    assert!(matches!(
        StreamingDatabase::open_with_filter(store.clone(), cfg, "group", "red", 1),
        Err(Error::Invalid(_))
    ));
    let chunk = store
        .objects
        .borrow()
        .keys()
        .find(|key| key.starts_with("compactedchunk-"))
        .unwrap()
        .clone();
    store.objects.borrow_mut().remove(&chunk);
    assert!(StreamingDatabase::open_with_filter(store, cfg, "group", "red", 64).is_err());
}

#[test]
fn posting_budget_applies_to_selected_snapshot_not_older_compaction() {
    let store = Memory::default();
    let cfg = config(Metric::Manhattan);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    for id in 0..3 {
        db.put_with_metadata(id, vec![id as f32, 0.], metadata("red"))
            .unwrap();
    }
    db.compact_chunked(256).unwrap();
    db.delete(0).unwrap();
    db.delete(1).unwrap();
    db.checkpoint_chunked(256).unwrap();
    drop(db);
    let reader = StreamingDatabase::open_with_filter(store, cfg, "group", "red", 1).unwrap();
    assert_eq!(
        reader
            .search_filtered(&[0., 0.], 5, &[("group", "red")])
            .unwrap(),
        vec![glider::Neighbor {
            id: 2,
            distance: 2.
        }]
    );
}
fn config(metric: Metric) -> Config {
    Config {
        dimensions: 2,
        metric,
    }
}
fn metadata(group: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("group".into(), group.into())])
}

#[test]
fn streamed_exact_and_filtered_results_match_recovered_database() {
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let store = Memory::default();
        let cfg = config(metric);
        let mut db = Database::open(store.clone(), cfg).unwrap();
        for id in 0..41 {
            db.put_with_metadata(
                id,
                vec![(id % 7) as f32, (id / 7) as f32],
                metadata(if id % 3 == 0 { "red" } else { "blue" }),
            )
            .unwrap();
        }
        db.compact_chunked(280).unwrap();
        db.apply_batch(vec![
            Mutation::Put {
                id: 3,
                vector: vec![100., 100.],
                metadata: metadata("green"),
            },
            Mutation::Delete { id: 4 },
            Mutation::Put {
                id: 99,
                vector: vec![1., 2.],
                metadata: metadata("red"),
            },
            Mutation::Put {
                id: 3,
                vector: vec![0., 0.],
                metadata: metadata("red"),
            },
        ])
        .unwrap();
        db.delete(5).unwrap();
        db.put(5, vec![2., 2.]).unwrap();
        let expected: Vec<_> = [[0., 0.], [1., 2.], [9., 9.]]
            .into_iter()
            .flat_map(|query| {
                [0, 1, 7, 50].into_iter().flat_map(move |k| {
                    [vec![], vec![("group", "red")], vec![("group", "missing")]]
                        .into_iter()
                        .map(move |filter| (query, k, filter))
                })
            })
            .map(|(query, k, filter)| {
                let result = db.search_filtered(&query, k, &filter).unwrap();
                (query, k, filter, result)
            })
            .collect();
        drop(db);

        let reader = StreamingDatabase::open(store.clone(), cfg).unwrap();
        let indexed =
            StreamingDatabase::open_with_filter(store.clone(), cfg, "group", "red", 41).unwrap();
        assert_eq!(reader.sequence(), 44);
        assert_eq!(reader.config(), cfg);
        for (query, k, filter, result) in expected {
            assert_eq!(
                reader.search_filtered(&query, k, &filter).unwrap(),
                result,
                "metric={metric:?} query={query:?} k={k} filter={filter:?}"
            );
            assert_eq!(
                indexed.search_filtered(&query, k, &filter).unwrap(),
                result,
                "posting metric={metric:?} query={query:?} k={k} filter={filter:?}"
            );
        }
        assert_eq!(
            reader.get_with_metadata(3).unwrap().unwrap().vector,
            vec![0., 0.]
        );
        assert_eq!(
            reader.get_with_metadata(3).unwrap().unwrap().metadata,
            metadata("red")
        );
        assert_eq!(reader.get_with_metadata(4).unwrap(), None);
        assert_eq!(
            reader.get_with_metadata(5).unwrap().unwrap().metadata,
            BTreeMap::new()
        );
        assert_eq!(
            reader.get_with_metadata(99).unwrap().unwrap().vector,
            vec![1., 2.]
        );
        assert_eq!(reader.get_with_metadata(999).unwrap(), None);
    }
}

#[test]
fn reader_validates_compaction_root_and_newer_segment_then_replays_tail() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.put(1, vec![1., 1.]).unwrap();
    db.compact_chunked(240).unwrap();
    db.put(2, vec![2., 2.]).unwrap();
    db.checkpoint_chunked(240).unwrap();
    db.put(3, vec![3., 3.]).unwrap();
    let expected = db.search(&[0., 0.], 3).unwrap();
    drop(db);
    let reader = StreamingDatabase::open(store.clone(), cfg).unwrap();
    assert_eq!(reader.search(&[0., 0.], 3).unwrap(), expected);
    drop(reader);

    let key = store
        .objects
        .borrow()
        .keys()
        .find(|k| k.starts_with("compactedchunk-"))
        .unwrap()
        .clone();
    store.objects.borrow_mut().remove(&key);
    assert!(matches!(
        StreamingDatabase::open(store, cfg),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn base_lookup_reads_one_chunk_and_tail_lookup_reads_none() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    for id in 0..20 {
        db.put(id, vec![id as f32, 0.]).unwrap();
    }
    db.compact_chunked(240).unwrap();
    db.put(30, vec![30., 0.]).unwrap();
    drop(db);
    let reader = StreamingDatabase::open(store.clone(), cfg).unwrap();
    let before = store.gets.get();
    assert_eq!(
        reader.get_with_metadata(30).unwrap().unwrap().vector,
        vec![30., 0.]
    );
    assert_eq!(store.gets.get(), before);
    assert_eq!(
        reader.get_with_metadata(7).unwrap().unwrap().vector,
        vec![7., 0.]
    );
    assert_eq!(store.gets.get(), before + 1);
    assert!(reader.get_with_metadata(999).unwrap().is_none());
    assert_eq!(store.gets.get(), before + 1);
    assert!(reader.search(&[0.], 0).is_err());
}

#[test]
fn reader_requires_chunked_snapshot_and_does_not_initialize_storage() {
    let cfg = config(Metric::SquaredEuclidean);
    let empty = Memory::default();
    assert!(matches!(
        StreamingDatabase::open(empty.clone(), cfg),
        Err(Error::Invalid(_))
    ));
    assert!(empty.objects.borrow().is_empty());
    let mut db = Database::open(empty.clone(), cfg).unwrap();
    db.put(1, vec![1., 1.]).unwrap();
    db.compact().unwrap();
    drop(db);
    assert!(matches!(
        StreamingDatabase::open(empty.clone(), cfg),
        Err(Error::Invalid(_))
    ));
    let mut db = Database::open(empty.clone(), cfg).unwrap();
    db.put(2, vec![2., 2.]).unwrap();
    db.compact_chunked(240).unwrap();
    drop(db);
    assert!(StreamingDatabase::open(empty, cfg).is_ok());
}

#[test]
fn query_detects_chunk_loss_after_open() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.put(1, vec![1., 1.]).unwrap();
    db.compact_chunked(240).unwrap();
    drop(db);
    let reader = StreamingDatabase::open(store.clone(), cfg).unwrap();
    let key = store
        .objects
        .borrow()
        .keys()
        .find(|k| k.starts_with("compactedchunk-"))
        .unwrap()
        .clone();
    store.objects.borrow_mut().remove(&key);
    assert!(matches!(
        reader.search(&[0., 0.], 1),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn local_reader_reopens_latest_tail_and_filters_exactly() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cfg = config(Metric::Manhattan);
    let mut db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    db.put_with_metadata(1, vec![1., 0.], metadata("red"))
        .unwrap();
    db.put_with_metadata(2, vec![0., 1.], metadata("blue"))
        .unwrap();
    db.compact_chunked(240).unwrap();
    db.put_with_metadata(3, vec![0., 0.], metadata("red"))
        .unwrap();
    let expected = db
        .search_filtered(&[0., 0.], 3, &[("group", "red")])
        .unwrap();
    drop(db);
    let reader = StreamingDatabase::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    assert_eq!(
        reader
            .search_filtered(&[0., 0.], 3, &[("group", "red")])
            .unwrap(),
        expected
    );
}

#[test]
fn empty_chunked_root_and_later_mutations_are_searchable() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.compact_chunked(256).unwrap();
    db.put_with_metadata(7, vec![1., 2.], metadata("red"))
        .unwrap();
    let expected = db
        .search_filtered(&[0., 0.], 3, &[("group", "red")])
        .unwrap();
    drop(db);
    let reader = StreamingDatabase::open(store.clone(), cfg).unwrap();
    let before = store.gets.get();
    assert_eq!(
        reader
            .search_filtered(&[0., 0.], 3, &[("group", "red")])
            .unwrap(),
        expected
    );
    assert_eq!(store.gets.get(), before);
}

#[test]
fn malformed_tail_fails_streaming_recovery() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.put(1, vec![1., 1.]).unwrap();
    db.compact_chunked(240).unwrap();
    db.put(2, vec![2., 2.]).unwrap();
    drop(db);
    store.objects.borrow_mut().insert(
        "mutation-00000000000000000002".into(),
        br#"{"version":2,"sequence":2,"mutation":{"type":"put","id":2,"vector":[1.0],"metadata":{}}}"#.to_vec(),
    );
    assert!(matches!(
        StreamingDatabase::open(store, cfg),
        Err(Error::Corrupt(_))
    ));
}
