use glider::{
    ivf::IvfConfig,
    store::{LocalStore, ObjectStore},
    Config, Database, Error, Metric, Result,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

#[derive(Clone, Default)]
struct Memory(Rc<RefCell<BTreeMap<String, Vec<u8>>>>);

impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.0.borrow().get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.0.borrow().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if self.0.borrow().contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        self.0.borrow_mut().insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.0.borrow_mut().remove(key);
        Ok(())
    }
}

struct Ambiguous {
    memory: Memory,
    lose_ack: Rc<Cell<bool>>,
}
impl ObjectStore for Ambiguous {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.memory.get(key)
    }
    fn list(&self) -> Result<Vec<String>> {
        self.memory.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.memory.create(key, value)?;
        if self.lose_ack.replace(false) {
            Err(Error::RecoveryRequired)
        } else {
            Ok(())
        }
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.memory.remove(key)
    }
}

fn config(metric: Metric) -> Config {
    Config {
        dimensions: 2,
        metric,
    }
}

fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|&(k, v)| (k.into(), v.into())).collect()
}

fn index(db: &mut Database<impl ObjectStore>, partitions: usize) {
    db.build_ivf(IvfConfig {
        partitions,
        iterations: 4,
        seed: 13,
    })
    .unwrap();
}

#[test]
fn exact_and_ivf_filtering_agree_with_full_probe_for_both_metrics() {
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let mut db = Database::open(Memory::default(), config(metric)).unwrap();
        for id in 0..37 {
            let label = if id % 3 == 0 { "red" } else { "blue" };
            db.put_with_metadata(
                id,
                vec![(id % 9) as f32, (id % 5) as f32],
                fields(&[
                    ("label", label),
                    ("group", if id % 2 == 0 { "a" } else { "b" }),
                ]),
            )
            .unwrap();
        }
        db.put(100, vec![0., 0.]).unwrap();
        for partitions in [1, 7, 100] {
            index(&mut db, partitions);
            for query in [[0., 0.], [4., 3.]] {
                for filter in [
                    vec![],
                    vec![("label", "red")],
                    vec![("label", "red"), ("group", "a")],
                    vec![("missing", "x")],
                    vec![("label", "red"), ("label", "blue")],
                ] {
                    for k in [0, 1, 5, 50] {
                        let exact = db.search_filtered(&query, k, &filter).unwrap();
                        let full = db
                            .search_ivf_filtered(&query, k, partitions, &filter)
                            .unwrap();
                        assert_eq!(
                            full.neighbors, exact,
                            "metric={metric:?} partitions={partitions} filter={filter:?} k={k}"
                        );
                        let partial = db.search_ivf_filtered(&query, k, 1, &filter).unwrap();
                        let all = db.search_filtered(&query, usize::MAX, &filter).unwrap();
                        let positions: Vec<_> = partial
                            .neighbors
                            .iter()
                            .map(|n| all.iter().position(|e| e == n).unwrap())
                            .collect();
                        assert!(positions.windows(2).all(|p| p[0] < p[1]));
                        assert!(partial.vector_distances <= full.vector_distances);
                    }
                }
            }
        }
        assert_eq!(db.get_metadata(100), Some(&fields(&[])));
        assert!(db
            .search_filtered(&[0., 0.], 50, &[("label", "red")])
            .unwrap()
            .iter()
            .all(|n| n.id != 100));
        db.put_with_metadata(100, vec![0., 0.], fields(&[("label", "red")]))
            .unwrap();
        assert!(db.search_ivf_filtered(&[0., 0.], 1, 1, &[]).is_err());
        assert!(db
            .search_filtered(&[0., 0.], 50, &[("label", "red")])
            .unwrap()
            .iter()
            .any(|n| n.id == 100));
        db.delete(100).unwrap();
        assert!(db.get_metadata(100).is_none());
    }
}

#[test]
fn adaptive_filtered_ivf_fills_k_and_never_loses_exact_recall() {
    const SEED: u64 = 42;
    const PARTITIONS: usize = 8;
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let mut db = Database::open(Memory::default(), config(metric)).unwrap();
        for id in 0..128 {
            let metadata = if id % 16 == 0 {
                fields(&[
                    ("selected", "yes"),
                    ("group", if id % 32 == 0 { "a" } else { "b" }),
                ])
            } else {
                fields(&[])
            };
            db.put_with_metadata(id, vec![id as f32, 0.], metadata)
                .unwrap();
        }
        db.build_ivf(IvfConfig {
            partitions: PARTITIONS,
            iterations: 5,
            seed: SEED,
        })
        .unwrap();
        for query in [[0., 0.], [63., 0.], [127., 0.]] {
            for filter in [
                vec![("selected", "yes")],
                vec![("selected", "yes"), ("group", "a")],
                vec![("selected", "missing")],
            ] {
                let all = db.search_filtered(&query, usize::MAX, &filter).unwrap();
                for k in [0, 1, 5, 20] {
                    let exact = db.search_filtered(&query, k, &filter).unwrap();
                    let fixed = db.search_ivf_filtered(&query, k, 1, &filter).unwrap();
                    let adaptive = db
                        .search_ivf_filtered_adaptive(&query, k, 1, &filter)
                        .unwrap();
                    assert_eq!(
                        adaptive.neighbors.len(),
                        k.min(all.len()),
                        "seed={SEED} metric={metric:?} query={query:?} filter={filter:?} k={k}"
                    );
                    assert!(adaptive.partitions_probed >= fixed.partitions_probed);
                    assert!(adaptive.partitions_probed <= PARTITIONS);
                    assert_eq!(fixed.partitions_probed, usize::from(k > 0));
                    assert!(adaptive.vector_distances >= fixed.vector_distances);
                    let recall_at_k = |hits: &[glider::Neighbor]| {
                        if exact.is_empty() {
                            1.0
                        } else {
                            hits.iter().filter(|hit| exact.contains(hit)).count() as f64
                                / exact.len() as f64
                        }
                    };
                    assert!(
                        recall_at_k(&adaptive.neighbors) >= recall_at_k(&fixed.neighbors),
                        "seed={SEED} metric={metric:?} query={query:?} filter={filter:?} k={k}"
                    );
                    let positions: Vec<_> = adaptive
                        .neighbors
                        .iter()
                        .map(|hit| all.iter().position(|candidate| candidate == hit).unwrap())
                        .collect();
                    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
                    if adaptive.partitions_probed == PARTITIONS {
                        assert_eq!(adaptive.neighbors, exact);
                    }
                    let repeat = db
                        .search_ivf_filtered_adaptive(&query, k, 1, &filter)
                        .unwrap();
                    assert_eq!(repeat.neighbors, adaptive.neighbors);
                    assert_eq!(repeat.partitions_probed, adaptive.partitions_probed);
                }
            }
        }
        let query = [0., 0.];
        let filter = &[("selected", "yes")];
        let one = db
            .search_ivf_filtered_adaptive(&query, 1, 1, filter)
            .unwrap();
        assert_eq!(one.partitions_probed, 1);
        assert_eq!(one.neighbors.len(), 1);
        let minimum_three = db
            .search_ivf_filtered_adaptive(&query, 1, 3, filter)
            .unwrap();
        assert_eq!(minimum_three.partitions_probed, 3);
        let fixed = db.search_ivf_filtered(&query, 5, 1, filter).unwrap();
        let adaptive = db
            .search_ivf_filtered_adaptive(&query, 5, 1, filter)
            .unwrap();
        assert!(fixed.neighbors.len() < 5, "seed={SEED} metric={metric:?}");
        assert_eq!(adaptive.neighbors.len(), 5);
        assert!(adaptive.partitions_probed > fixed.partitions_probed);
        let exact = db.search_filtered(&query, 5, filter).unwrap();
        let recall_count =
            |hits: &[glider::Neighbor]| hits.iter().filter(|hit| exact.contains(hit)).count();
        assert!(recall_count(&adaptive.neighbors) > recall_count(&fixed.neighbors));
        let full = db
            .search_ivf_filtered_adaptive(&query, 5, PARTITIONS + 1, filter)
            .unwrap();
        assert_eq!(full.partitions_probed, PARTITIONS);
        assert_eq!(
            full.neighbors,
            db.search_filtered(&query, 5, filter).unwrap()
        );
        assert!(db
            .search_ivf_filtered_adaptive(&query, 5, 0, filter)
            .is_err());
    }
}

#[test]
fn metadata_survives_snapshots_compaction_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    db.put_with_metadata(1, vec![1., 0.], fields(&[("team", "red")]))
        .unwrap();
    db.put_with_metadata(2, vec![0., 1.], fields(&[("team", "blue")]))
        .unwrap();
    db.checkpoint().unwrap();
    db.put_with_metadata(1, vec![0., 0.], fields(&[("team", "blue")]))
        .unwrap();
    db.compact().unwrap();
    db.put_with_metadata(3, vec![2., 0.], fields(&[("team", "red")]))
        .unwrap();
    drop(db);
    let mut db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    assert_eq!(db.get_metadata(1), Some(&fields(&[("team", "blue")])));
    assert_eq!(
        db.search_filtered(&[0., 0.], 5, &[("team", "red")])
            .unwrap()
            .iter()
            .map(|n| n.id)
            .collect::<Vec<_>>(),
        vec![3]
    );
    index(&mut db, 3);
    assert_eq!(
        db.search_ivf_filtered(&[0., 0.], 5, 3, &[("team", "blue")])
            .unwrap()
            .neighbors,
        db.search_filtered(&[0., 0.], 5, &[("team", "blue")])
            .unwrap()
    );
}

#[test]
fn uncertain_metadata_put_keeps_acknowledged_reads_until_recovery() {
    let memory = Memory::default();
    let lose_ack = Rc::new(Cell::new(false));
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(
        Ambiguous {
            memory: memory.clone(),
            lose_ack: lose_ack.clone(),
        },
        cfg,
    )
    .unwrap();
    db.put_with_metadata(1, vec![1., 0.], fields(&[("team", "old")]))
        .unwrap();
    index(&mut db, 1);
    lose_ack.set(true);
    assert!(db
        .put_with_metadata(1, vec![0., 0.], fields(&[("team", "new")]))
        .is_err());
    assert_eq!(db.get(1), Some([1., 0.].as_slice()));
    assert_eq!(db.get_metadata(1), Some(&fields(&[("team", "old")])));
    assert_eq!(
        db.search_ivf_filtered(&[0., 0.], 1, 1, &[("team", "old")])
            .unwrap()
            .neighbors
            .len(),
        1
    );
    assert!(matches!(db.delete(1), Err(Error::RecoveryRequired)));
    drop(db);
    let db = Database::open(memory, cfg).unwrap();
    assert_eq!(db.get(1), Some([0., 0.].as_slice()));
    assert_eq!(db.get_metadata(1), Some(&fields(&[("team", "new")])));
}

#[test]
fn version_one_records_and_snapshots_upgrade_without_losing_state() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    store.0.borrow_mut().insert(
        "metadata".into(),
        serde_json::to_vec(&serde_json::json!({"version":1,"config":cfg})).unwrap(),
    );
    store.0.borrow_mut().insert(
        "mutation-00000000000000000001".into(),
        br#"{"version":1,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1.0,2.0]}}"#
            .to_vec(),
    );
    store.0.borrow_mut().insert(
        "segment-00000000000000000001".into(),
        serde_json::to_vec(
            &serde_json::json!({"version":1,"sequence":1,"config":cfg,"documents":[[1,[1.0,2.0]]]}),
        )
        .unwrap(),
    );
    let mut db = Database::open(store.clone(), cfg).unwrap();
    assert_eq!(db.get_metadata(1), Some(&fields(&[])));
    db.put_with_metadata(2, vec![2., 2.], fields(&[("kind", "new")]))
        .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(store.clone(), cfg).unwrap();
    assert_eq!(db.get_metadata(1), Some(&fields(&[])));
    assert_eq!(db.get_metadata(2), Some(&fields(&[("kind", "new")])));
    let objects = store.0.borrow();
    let record: serde_json::Value =
        serde_json::from_slice(&objects["mutation-00000000000000000002"]).unwrap();
    let segment: serde_json::Value =
        serde_json::from_slice(&objects["segment-00000000000000000002"]).unwrap();
    assert_eq!(record["version"], 2);
    assert_eq!(segment["version"], 2);
}

#[test]
fn malformed_versioned_metadata_fails_recovery() {
    let cfg = config(Metric::SquaredEuclidean);
    for record in [
        r#"{"version":1,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1,2],"metadata":{}}}"#,
        r#"{"version":2,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1,2]}}"#,
        r#"{"version":2,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1,2],"metadata":{"x":2}}}"#,
        r#"{"version":2,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1,2],"metadata":{"x":"a","x":"b"}}}"#,
        r#"{"version":3,"sequence":1,"mutation":{"type":"delete","id":1}}"#,
    ] {
        let store = Memory::default();
        store.0.borrow_mut().insert(
            "metadata".into(),
            serde_json::to_vec(&serde_json::json!({"version":1,"config":cfg})).unwrap(),
        );
        store.0.borrow_mut().insert(
            "mutation-00000000000000000001".into(),
            record.as_bytes().to_vec(),
        );
        assert!(Database::open(store, cfg).is_err(), "accepted {record}");
    }
    let store = Memory::default();
    store.0.borrow_mut().insert(
        "metadata".into(),
        serde_json::to_vec(&serde_json::json!({"version":1,"config":cfg})).unwrap(),
    );
    store.0.borrow_mut().insert("segment-00000000000000000000".into(), serde_json::to_vec(&serde_json::json!({"version":2,"sequence":0,"config":cfg,"documents":[[1,{"vector":[1,2],"metadata":{"x":2}}]]})).unwrap());
    assert!(Database::open(store, cfg).is_err());
}
