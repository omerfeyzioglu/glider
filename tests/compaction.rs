use glider::{store::ObjectStore, Config, Database, Error, Metric, Result};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
fn object(prefix: &str, sequence: u64) -> String {
    format!("{prefix}-{sequence:020}")
}
#[derive(Clone, Default)]
struct Memory(Rc<RefCell<State>>);
#[derive(Default)]
struct State {
    objects: BTreeMap<String, Vec<u8>>,
    fault: Option<(usize, bool, bool)>, // successful operations before failure, after effect, panic
    trace: Vec<String>,
}
impl State {
    fn event(&mut self, name: String, effect: impl FnOnce(&mut Self)) -> Result<()> {
        self.trace.push(name);
        if let Some((remaining, after, panic)) = self.fault {
            if remaining == 0 {
                self.fault = None;
                if after {
                    effect(self);
                }
                assert!(!panic, "injected compaction storage panic");
                return Err(std::io::Error::other("injected compaction error").into());
            }
            self.fault = Some((remaining - 1, after, panic));
        }
        effect(self);
        Ok(())
    }
}
impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.0.borrow().objects.get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        let mut s = self.0.borrow_mut();
        s.event("list".into(), |_| {})?;
        Ok(s.objects.keys().rev().cloned().collect())
    }
    fn create(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        let mut s = self.0.borrow_mut();
        if s.objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        s.event(format!("create:{key}"), |s| {
            s.objects.insert(key.into(), bytes.into());
        })
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        self.0.borrow_mut().event(format!("remove:{key}"), |s| {
            s.objects.remove(key);
        })
    }
}
fn populated() -> (Memory, Database<Memory>) {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.checkpoint().unwrap();
    db.compact().unwrap();
    db.put(1, vec![3., 4.]).unwrap();
    db.put(2, vec![5., 6.]).unwrap();
    db.checkpoint().unwrap();
    db.delete(1).unwrap();
    db.checkpoint().unwrap();
    db.delete(99).unwrap();
    (store, db)
}
fn assert_state(db: &Database<Memory>) {
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([5., 6.].as_slice()));
    assert_eq!(db.get(99), None);
    assert_eq!(db.search(&[5., 6.], 10).unwrap().len(), 1);
}
#[test]
fn multiple_snapshots_and_tail_consolidate_to_one_durable_root() {
    let (store, mut db) = populated();
    let expected = db.search(&[2., 2.], 10).unwrap();
    db.compact().unwrap();
    assert_state(&db);
    assert_eq!(
        store.0.borrow().objects.keys().cloned().collect::<Vec<_>>(),
        vec![object("compacted", 5), "metadata".into()]
    );
    store.0.borrow_mut().trace.clear();
    db.compact().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(store.0.borrow().trace, vec!["list"]);
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(db.search(&[2., 2.], 10).unwrap(), expected);
    db.put(1, vec![7., 8.]).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(db.get(1), Some([7., 8.].as_slice()));
    db.delete(2).unwrap();
    db.compact().unwrap();
    drop(db);
    let db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(db.get(2), None);
    assert_eq!(db.get(1), Some([7., 8.].as_slice()));
    assert_eq!(store.0.borrow().objects.len(), 2);
    assert!(store
        .0
        .borrow()
        .objects
        .contains_key(&object("compacted", 7)));
}
#[test]
fn every_publication_listing_and_removal_failure_is_recoverable() {
    let (baseline_store, mut baseline) = populated();
    baseline_store.0.borrow_mut().trace.clear();
    baseline.compact().unwrap();
    let count = baseline_store.0.borrow().trace.len();
    for failure in 0..count {
        for after in [false, true] {
            for panic in [false, true] {
                let (store, mut db) = populated();
                store.0.borrow_mut().fault = Some((failure, after, panic));
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.compact()));
                if panic {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().is_err());
                }
                assert!(
                    store.0.borrow().fault.is_none(),
                    "fault={failure}/{after}/{panic}"
                );
                assert_state(&db);
                assert!(matches!(
                    db.put(3, vec![3., 3.]),
                    Err(Error::RecoveryRequired)
                ));
                assert!(matches!(db.delete(2), Err(Error::RecoveryRequired)));
                assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
                assert!(matches!(db.compact(), Err(Error::RecoveryRequired)));
                drop(db);
                let mut db = Database::open(store.clone(), config()).unwrap();
                assert_state(&db);
                // Retry maintenance after recovery; its result must survive later writes.
                db.compact().unwrap();
                assert_eq!(store.0.borrow().objects.len(), 2);
                db.put(3, vec![3., 3.]).unwrap();
                drop(db);
                let db = Database::open(store.clone(), config()).unwrap();
                assert_eq!(db.get(1), None);
                assert_eq!(db.get(3), Some([3., 3.].as_slice()));
                assert!(store
                    .0
                    .borrow()
                    .objects
                    .contains_key(&object("mutation", 6)));
            }
        }
    }
}

#[test]
fn only_valid_compaction_roots_allow_gaps_and_tail_gaps_still_fail() {
    for damage in [
        "missing-root",
        "bad-root",
        "bad-version",
        "tail-gap",
        "unknown-key",
    ] {
        let (store, mut db) = populated();
        db.compact().unwrap();
        db.put(3, vec![3., 3.]).unwrap();
        db.checkpoint().unwrap();
        db.put(4, vec![4., 4.]).unwrap();
        drop(db);
        let root = object("compacted", 5);
        let mut s = store.0.borrow_mut();
        match damage {
            "missing-root" => {
                s.objects.remove(&root);
            }
            "bad-root" => {
                s.objects.insert(root, b"broken".to_vec());
            }
            "bad-version" => {
                let mut value: serde_json::Value =
                    serde_json::from_slice(&s.objects[&root]).unwrap();
                value["version"] = serde_json::json!(2);
                s.objects.insert(root, serde_json::to_vec(&value).unwrap());
            }
            "tail-gap" => {
                s.objects.remove(&object("mutation", 6));
            }
            _ => {
                s.objects.insert("compacted-5".into(), vec![]);
            }
        }
        drop(s);
        assert!(
            matches!(Database::open(store, config()), Err(Error::Corrupt(_))),
            "{damage}"
        );
    }
}
#[test]
fn empty_state_and_zero_sequence_compaction_are_valid() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    db.compact().unwrap();
    db.compact().unwrap();
    db.checkpoint().unwrap();
    assert_eq!(store.0.borrow().objects.len(), 2);
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    db.put(1, vec![-0., f32::from_bits(1)]).unwrap();
    db.compact().unwrap();
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(db.get(1).unwrap()[0].to_bits(), (-0_f32).to_bits());
    assert_eq!(db.get(1).unwrap()[1].to_bits(), 1);
    db.delete(1).unwrap();
    db.compact().unwrap();
    drop(db);
    let db = Database::open(store, config()).unwrap();
    assert!(db.search(&[0., 0.], 10).unwrap().is_empty());
}
#[test]
fn reproducible_compaction_history_matches_full_replay() {
    const SEED: u64 = 193753;
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let cfg = Config { metric, ..config() };
        let compacted = Memory::default();
        let log = Memory::default();
        let mut db = Database::open(compacted.clone(), cfg).unwrap();
        let mut oracle = Database::open(log.clone(), cfg).unwrap();
        let mut rng = SEED;
        for step in 0..250 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = (rng >> 32) % 20;
            if rng.is_multiple_of(3) {
                db.delete(id).unwrap();
                oracle.delete(id).unwrap();
            } else {
                let v = vec![(rng % 100) as f32, step as f32];
                db.put(id, v.clone()).unwrap();
                oracle.put(id, v).unwrap();
            }
            if step % 9 == 0 {
                db.checkpoint().unwrap();
            }
            if step % 17 == 0 {
                db.compact().unwrap();
                drop(db);
                drop(oracle);
                db = Database::open(compacted.clone(), cfg).unwrap();
                oracle = Database::open(log.clone(), cfg).unwrap();
                assert_eq!(compacted.0.borrow().objects.len(), 2);
            }
            for k in [0, 1, 7, 30] {
                assert_eq!(
                    db.search(&[5., 7.], k).unwrap(),
                    oracle.search(&[5., 7.], k).unwrap(),
                    "seed={SEED}, step={step}, metric={metric:?}"
                );
            }
        }
    }
}

#[test]
fn invalid_cleanup_plan_never_removes_preexisting_history() {
    for invalid in [
        "unknown".to_owned(),
        object("mutation", 0),
        object("segment", 6),
    ] {
        let (store, mut db) = populated();
        store.0.borrow_mut().objects.insert(invalid.clone(), vec![]);
        let before = store.0.borrow().objects.clone();
        assert!(matches!(db.compact(), Err(Error::Corrupt(_))));
        assert!(matches!(db.delete(2), Err(Error::RecoveryRequired)));
        for (key, value) in before {
            assert_eq!(store.0.borrow().objects.get(&key), Some(&value));
        }
        drop(db);
        store.0.borrow_mut().objects.remove(&invalid);
        let mut db = Database::open(store.clone(), config()).unwrap();
        assert_state(&db);
        db.compact().unwrap();
        assert_eq!(store.0.borrow().objects.len(), 2);
    }
}

#[test]
fn external_loss_of_an_unwitnessed_compaction_root_is_not_detectable() {
    // Media loss is outside the durability contract. With no later key to
    // witness a gap, metadata alone cannot distinguish this loss from emptiness.
    let (store, mut db) = populated();
    db.compact().unwrap();
    drop(db);
    store.0.borrow_mut().objects.remove(&object("compacted", 5));
    let db = Database::open(store, config()).unwrap();
    assert!(db.search(&[0., 0.], 10).unwrap().is_empty());
}
