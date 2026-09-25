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
struct Memory {
    objects: Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
    fault: Rc<Cell<u8>>,
}
impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.borrow().get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.objects.borrow().keys().cloned().collect())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.objects.borrow_mut().remove(key);
        Ok(())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let fault = self.fault.replace(0);
        if fault != 1 && fault != 3 {
            if self.objects.borrow().contains_key(key) {
                return Err(Error::Exists(key.into()));
            }
            self.objects.borrow_mut().insert(key.into(), value.to_vec());
        }
        match fault {
            1 | 2 => Err(Error::RecoveryRequired),
            3 | 4 => panic!("injected publication panic"),
            _ => Ok(()),
        }
    }
}
fn options(partitions: usize) -> IvfConfig {
    IvfConfig {
        partitions,
        iterations: 5,
        seed: 42,
    }
}
fn config(metric: Metric) -> Config {
    Config {
        dimensions: 2,
        metric,
    }
}

#[test]
fn full_probe_matches_exact_and_partial_probe_is_ordered_and_deterministic() {
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let mut db = Database::open(Memory::default(), config(metric)).unwrap();
        for id in (0..97).rev() {
            let point = match id % 7 {
                0 => vec![f32::MAX, -f32::MAX],
                1 => vec![-0., f32::from_bits(1)],
                _ => vec![(id % 13) as f32, -((id % 11) as f32)],
            };
            db.put(id, point).unwrap();
        }
        for partitions in [1, 7, 120] {
            db.build_ivf(options(partitions)).unwrap();
            for query in [[0., 0.], [4., -3.], [f32::MAX, f32::MAX]] {
                for k in [0, 1, 10, 97, usize::MAX] {
                    let exact = db.search(&query, k).unwrap();
                    assert_eq!(
                        db.search_ivf(&query, k, partitions).unwrap().neighbors,
                        exact,
                        "seed=42 metric={metric:?}"
                    );
                    let mut previous_recall = 0;
                    for probes in [1, 3, partitions.max(3)] {
                        let result = db.search_ivf(&query, k, probes).unwrap();
                        assert!(result.vector_distances <= 97);
                        let all = db.search(&query, usize::MAX).unwrap();
                        let positions: Vec<_> = result
                            .neighbors
                            .iter()
                            .map(|n| all.iter().position(|e| e == n).unwrap())
                            .collect();
                        assert!(positions.windows(2).all(|p| p[0] < p[1]));
                        let recall = result
                            .neighbors
                            .iter()
                            .filter(|n| exact.contains(n))
                            .count();
                        assert!(recall >= previous_recall);
                        previous_recall = recall;
                    }
                }
            }
            let before = db.search_ivf(&[0., 0.], 10, 1).unwrap();
            db.build_ivf(options(partitions)).unwrap();
            let after = db.search_ivf(&[0., 0.], 10, 1).unwrap();
            assert_eq!(before.neighbors, after.neighbors);
            assert_eq!(before.vector_distances, after.vector_distances);
        }
    }
}

#[test]
fn validation_empty_identical_and_invalidation() {
    let mut db = Database::open(Memory::default(), config(Metric::Manhattan)).unwrap();
    assert!(db.search_ivf(&[0., 0.], 1, 1).is_err());
    assert!(db.build_ivf(options(0)).is_err());
    assert!(db
        .build_ivf(IvfConfig {
            iterations: 0,
            ..options(1)
        })
        .is_err());
    db.build_ivf(options(5)).unwrap();
    assert!(db
        .search_ivf(&[0., 0.], 10, 1)
        .unwrap()
        .neighbors
        .is_empty());
    assert!(db.search_ivf(&[0.], 0, 1).is_err());
    assert!(db.search_ivf(&[f32::NAN, 0.], 0, 1).is_err());
    assert!(db.search_ivf(&[0., 0.], 1, 0).is_err());
    for id in 0..9 {
        db.put(id, vec![1., 1.]).unwrap();
    }
    db.build_ivf(options(9)).unwrap();
    assert_eq!(
        db.search_ivf(&[1., 1.], 20, 1).unwrap().neighbors,
        db.search(&[1., 1.], 20).unwrap()
    );
    assert!(db.put(0, vec![f32::INFINITY, 0.]).is_err());
    assert!(db.search_ivf(&[0., 0.], 1, 1).is_ok());
    db.put(0, vec![0., 0.]).unwrap();
    assert!(db.search_ivf(&[0., 0.], 1, 1).is_err());
    db.build_ivf(options(2)).unwrap();
    db.delete(0).unwrap();
    assert!(db.search_ivf(&[0., 0.], 1, 1).is_err());
    db.build_ivf(options(2)).unwrap();
    assert!(db
        .search_ivf(&[0., 0.], 20, 2)
        .unwrap()
        .neighbors
        .iter()
        .all(|n| n.id != 0));
}

#[test]
fn uncertain_writes_and_panics_keep_last_acknowledged_index_then_rebuild_on_recovery() {
    for fault in 1..=4 {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config(Metric::SquaredEuclidean)).unwrap();
        db.put(1, vec![1., 1.]).unwrap();
        db.build_ivf(options(2)).unwrap();
        let before = db.search(&[0., 0.], 10).unwrap();
        store.fault.set(fault);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.put(2, vec![0., 0.])));
        assert_eq!(db.search_ivf(&[0., 0.], 10, 2).unwrap().neighbors, before);
        assert_eq!(
            db.search_ivf_filtered_adaptive(&[0., 0.], 10, 1, &[])
                .unwrap()
                .neighbors,
            before
        );
        assert!(matches!(db.delete(1), Err(Error::RecoveryRequired)));
        drop(db);
        let mut db = Database::open(store, config(Metric::SquaredEuclidean)).unwrap();
        assert!(db.search_ivf(&[0., 0.], 10, 2).is_err());
        assert!(db
            .search_ivf_filtered_adaptive(&[0., 0.], 10, 1, &[])
            .is_err());
        db.build_ivf(options(2)).unwrap();
        assert_eq!(
            db.search_ivf(&[0., 0.], 10, 2).unwrap().neighbors,
            db.search(&[0., 0.], 10).unwrap()
        );
        assert_eq!(db.get(2).is_some(), fault == 2 || fault == 4);
    }
}

#[test]
fn checkpoint_compaction_and_local_reopen_preserve_index_source() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.put(2, vec![2., 1.]).unwrap();
    db.build_ivf(options(2)).unwrap();
    db.checkpoint().unwrap();
    db.compact().unwrap();
    let before = db.search_ivf(&[0., 0.], 2, 2).unwrap().neighbors;
    drop(db);
    let mut db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    assert!(db.search_ivf(&[0., 0.], 2, 2).is_err());
    db.build_ivf(options(2)).unwrap();
    assert_eq!(db.search_ivf(&[0., 0.], 2, 2).unwrap().neighbors, before);
}
