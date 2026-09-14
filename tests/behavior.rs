use glider::{
    store::{LocalStore, ObjectStore},
    Config, Database, Error, Metric, Result,
};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};
fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
#[test]
fn mutations_search_and_repeated_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut db = Database::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert!(db.search(&[0., 0.], 10).unwrap().is_empty());
    db.put(9, vec![1., 0.]).unwrap();
    db.put(2, vec![0., 1.]).unwrap();
    db.put(4, vec![2., 2.]).unwrap();
    assert_eq!(
        db.search(&[0., 0.], 2)
            .unwrap()
            .iter()
            .map(|n| n.id)
            .collect::<Vec<_>>(),
        vec![2, 9]
    );
    db.put(4, vec![0., 0.]).unwrap();
    db.delete(2).unwrap();
    db.delete(99).unwrap();
    let expected = db.search(&[0., 0.], usize::MAX).unwrap();
    assert_eq!(
        expected
            .iter()
            .map(|n| (n.id, n.distance))
            .collect::<Vec<_>>(),
        vec![(4, 0.), (9, 1.)]
    );
    drop(db);
    for _ in 0..3 {
        let db = Database::open(LocalStore::open(&path).unwrap(), config()).unwrap();
        assert_eq!(db.search(&[0., 0.], 99).unwrap(), expected);
        assert_eq!(db.get(2), None);
        assert_eq!(db.get(4), Some([0., 0.].as_slice()));
    }
    let mut db = Database::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    db.delete(4).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    assert_eq!(db.get(4), None);
}
#[test]
fn validation_and_metrics() {
    let store = Memory::default();
    assert!(Database::open(
        store.clone(),
        Config {
            dimensions: 0,
            ..config()
        }
    )
    .is_err());
    let mut db = Database::open(store.clone(), config()).unwrap();
    for vector in [
        vec![],
        vec![1.],
        vec![1., 2., 3.],
        vec![f32::NAN, 0.],
        vec![f32::INFINITY, 0.],
        vec![f32::NEG_INFINITY, 0.],
    ] {
        assert!(db.put(1, vector.clone()).is_err());
        assert!(db.search(&vector, 0).is_err());
    }
    assert_eq!(store.0.borrow().objects.len(), 1);
    db.put(u64::MAX, vec![f32::MAX, -f32::MAX]).unwrap();
    assert!(db.search(&[-f32::MAX, f32::MAX], 1).unwrap()[0]
        .distance
        .is_finite());
    assert!(db.search(&[0., 0.], 0).unwrap().is_empty());
    drop(db);
    assert!(Database::open(
        store.clone(),
        Config {
            dimensions: 3,
            ..config()
        }
    )
    .is_err());
    assert!(Database::open(
        store,
        Config {
            metric: Metric::Manhattan,
            ..config()
        }
    )
    .is_err());
    for (metric, ids, distances) in [
        (Metric::SquaredEuclidean, vec![2, 1], vec![18., 25.]),
        (Metric::Manhattan, vec![1, 2], vec![5., 6.]),
    ] {
        let mut db = Database::open(Memory::default(), Config { metric, ..config() }).unwrap();
        db.put(1, vec![5., 0.]).unwrap();
        db.put(2, vec![3., 3.]).unwrap();
        let hits = db.search(&[0., 0.], 10).unwrap();
        assert_eq!(hits.iter().map(|n| n.id).collect::<Vec<_>>(), ids);
        assert_eq!(
            hits.iter().map(|n| n.distance).collect::<Vec<_>>(),
            distances
        );
    }
}
#[derive(Default)]
struct State {
    objects: BTreeMap<String, Vec<u8>>,
    fail: Option<bool>,
    panic: Option<bool>,
}
#[derive(Default, Clone)]
struct Memory(Rc<RefCell<State>>);
impl ObjectStore for Memory {
    fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.0.borrow().objects.get(k).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.0.borrow().objects.keys().rev().cloned().collect())
    }
    fn create(&mut self, k: &str, v: &[u8]) -> Result<()> {
        let mut s = self.0.borrow_mut();
        let failure = s.fail.take();
        let panic = s.panic.take();
        assert_ne!(panic, Some(false), "injected panic before publication");
        if failure != Some(false) {
            if s.objects.contains_key(k) {
                return Err(Error::Exists(k.into()));
            }
            s.objects.insert(k.into(), v.into());
        }
        assert_ne!(panic, Some(true), "injected panic after publication");
        if failure.is_some() {
            return Err(std::io::Error::other("injected uncertain write").into());
        }
        Ok(())
    }
}
#[test]
fn ambiguous_writes_require_recovery() {
    for committed in [false, true] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        store.0.borrow_mut().fail = Some(committed);
        assert!(db.delete(1).is_err());
        assert_eq!(db.get(1), Some([1., 2.].as_slice()));
        assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
        assert!(matches!(
            db.put(2, vec![0., 0.]),
            Err(Error::RecoveryRequired)
        ));
        drop(db);
        let mut db = Database::open(store.clone(), config()).unwrap();
        assert_eq!(db.get(1).is_none(), committed);
        db.put(2, vec![0., 0.]).unwrap();
        drop(db);
        let db = Database::open(store, config()).unwrap();
        assert_eq!(db.get(1).is_none(), committed);
        assert_eq!(db.get(2), Some([0., 0.].as_slice()));
    }
    for committed in [false, true] {
        let store = Memory::default();
        store.0.borrow_mut().fail = Some(committed);
        assert!(Database::open(store.clone(), config()).is_err());
        assert!(Database::open(store, config()).is_ok());
    }
}
#[test]
fn recovery_rejects_invalid_authoritative_objects() {
    for bad in [
        r#"{"version":2,"sequence":1,"mutation":{"type":"delete","id":1}}"#,
        r#"{"version":1,"sequence":2,"mutation":{"type":"delete","id":1}}"#,
        r#"{"version":1,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1]}}"#,
        "broken",
    ] {
        let store = Memory::default();
        drop(Database::open(store.clone(), config()).unwrap());
        store.0.borrow_mut().objects.insert(
            "mutation-00000000000000000001".into(),
            bad.as_bytes().into(),
        );
        assert!(matches!(
            Database::open(store, config()),
            Err(Error::Corrupt(_))
        ));
    }
    for removed in ["metadata", "mutation-00000000000000000001"] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config()).unwrap();
        db.delete(1).unwrap();
        db.delete(2).unwrap();
        drop(db);
        store.0.borrow_mut().objects.remove(removed);
        assert!(matches!(
            Database::open(store, config()),
            Err(Error::Corrupt(_))
        ));
    }
}

#[test]
fn exact_search_matches_integer_grid_oracle_for_every_k() {
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let mut db = Database::open(Memory::default(), Config { metric, ..config() }).unwrap();
        let points: Vec<_> = (-3_i32..=3)
            .flat_map(|x| (-3_i32..=3).map(move |y| (x, y)))
            .collect();
        for (id, &(x, y)) in points.iter().enumerate().rev() {
            db.put(id as u64, vec![x as f32, y as f32]).unwrap();
        }
        for qx in -2_i32..=2 {
            for qy in -2_i32..=2 {
                let mut oracle: Vec<_> = points
                    .iter()
                    .enumerate()
                    .map(|(id, &(x, y))| {
                        let dx = (x - qx).abs();
                        let dy = (y - qy).abs();
                        let distance = match metric {
                            Metric::SquaredEuclidean => dx * dx + dy * dy,
                            Metric::Manhattan => dx + dy,
                        };
                        (distance, id as u64)
                    })
                    .collect();
                oracle.sort();
                for k in 0..=points.len() + 1 {
                    let actual = db.search(&[qx as f32, qy as f32], k).unwrap();
                    let expected: Vec<_> = oracle
                        .iter()
                        .take(k)
                        .map(|&(d, id)| (id, f64::from(d)))
                        .collect();
                    assert_eq!(
                        actual
                            .iter()
                            .map(|n| (n.id, n.distance))
                            .collect::<Vec<_>>(),
                        expected,
                        "metric={metric:?}, query=({qx},{qy}), k={k}"
                    );
                }
            }
        }
    }
}

#[test]
fn uncertain_put_and_float_roundtrip() {
    for committed in [false, true] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config()).unwrap();
        store.0.borrow_mut().fail = Some(committed);
        assert!(db.put(7, vec![0.1, f32::MIN_POSITIVE]).is_err());
        assert_eq!(db.get(7), None);
        drop(db);
        let db = Database::open(store, config()).unwrap();
        assert_eq!(db.get(7).is_some(), committed);
    }
    let values = [
        0.,
        -0.,
        f32::from_bits(1),
        f32::MIN_POSITIVE,
        f32::MAX,
        -f32::MAX,
        0.1,
        f32::from_bits(0x3f800001),
    ];
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    for (id, &v) in values.iter().enumerate() {
        db.put(id as u64, vec![v, v]).unwrap();
    }
    drop(db);
    let db = Database::open(store, config()).unwrap();
    for (id, &v) in values.iter().enumerate() {
        assert_eq!(db.get(id as u64).unwrap()[0].to_bits(), v.to_bits());
    }
}

#[test]
fn backend_panics_keep_handle_poisoned_before_and_after_publication() {
    for committed in [false, true] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        store.0.borrow_mut().panic = Some(committed);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.delete(1)));
        assert!(result.is_err());
        assert_eq!(db.get(1), Some([1., 2.].as_slice()));
        assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
        assert!(matches!(
            db.put(2, vec![2., 3.]),
            Err(Error::RecoveryRequired)
        ));
        assert!(matches!(db.delete(1), Err(Error::RecoveryRequired)));
        drop(db);
        let mut db = Database::open(store.clone(), config()).unwrap();
        assert_eq!(db.get(1).is_none(), committed);
        db.put(2, vec![2., 3.]).unwrap();
        drop(db);
        let db = Database::open(store.clone(), config()).unwrap();
        assert_eq!(db.get(1).is_none(), committed);
        assert_eq!(db.get(2), Some([2., 3.].as_slice()));
        assert_eq!(
            store.0.borrow().objects.len(),
            if committed { 4 } else { 3 }
        );
    }
}
