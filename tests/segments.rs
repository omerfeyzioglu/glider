use glider::{store::ObjectStore, Config, Database, Error, Metric, Result};
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
#[derive(Default, Clone)]
struct Memory(Rc<RefCell<State>>);
#[derive(Default)]
struct State {
    objects: BTreeMap<String, Vec<u8>>,
    gets: Vec<String>,
    fault: Option<(bool, bool)>, // publish, panic
    missing_get: Option<String>,
    duplicate_key: Option<String>,
}
impl ObjectStore for Memory {
    fn remove(&mut self, key: &str) -> Result<()> {
        self.0.borrow_mut().objects.remove(key);
        Ok(())
    }
    fn list(&self) -> Result<Vec<String>> {
        let state = self.0.borrow();
        let mut keys: Vec<_> = state.objects.keys().rev().cloned().collect();
        if let Some(key) = &state.duplicate_key {
            keys.push(key.clone());
        }
        Ok(keys)
    }
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let mut state = self.0.borrow_mut();
        state.gets.push(key.into());
        if state.missing_get.as_deref() == Some(key) {
            return Ok(None);
        }
        Ok(state.objects.get(key).cloned())
    }
    fn create(&mut self, key: &str, bytes: &[u8]) -> Result<()> {
        let mut state = self.0.borrow_mut();
        let fault = state.fault.take();
        if fault.is_none_or(|(publish, _)| publish) {
            if state.objects.contains_key(key) {
                return Err(Error::Exists(key.into()));
            }
            state.objects.insert(key.into(), bytes.into());
        }
        if let Some((_, panic)) = fault {
            assert!(!panic, "injected publication panic");
            return Err(std::io::Error::other("injected publication failure").into());
        }
        Ok(())
    }
}
fn segment(sequence: u64) -> String {
    format!("segment-{sequence:020}")
}
fn mutation(sequence: u64) -> String {
    format!("mutation-{sequence:020}")
}

#[test]
fn checkpoint_recovery_reads_only_snapshot_and_tail_and_preserves_sequence() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    db.checkpoint().unwrap(); // empty snapshot at sequence zero
    db.checkpoint().unwrap(); // idempotent; no second create
    for i in 0..50 {
        db.put(i % 3, vec![i as f32, 1.]).unwrap();
    }
    db.delete(1).unwrap();
    db.checkpoint().unwrap();
    db.put(2, vec![99., 2.]).unwrap();
    let expected = db.search(&[0., 0.], 99).unwrap();
    drop(db);
    store.0.borrow_mut().gets.clear();
    let mut db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(
        store.0.borrow().gets,
        vec!["metadata".into(), segment(51), mutation(52)]
    );
    assert_eq!(db.search(&[0., 0.], 99).unwrap(), expected);
    assert_eq!(db.get(1), None);
    db.put(1, vec![1., 2.]).unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    db.checkpoint().unwrap();
    db.delete(2).unwrap();
    drop(db);
    let db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(db.get(1), Some([1., 2.].as_slice()));
    assert_eq!(db.get(2), None);
    assert!(store.0.borrow().objects.contains_key(&mutation(54)));
    assert_eq!(store.0.borrow().objects.len(), 1 + 54 + 3);
}

#[test]
fn publication_errors_and_panics_require_recovery_with_or_without_a_segment() {
    for prior in [false, true] {
        for publish in [false, true] {
            for panic in [false, true] {
                let store = Memory::default();
                let mut db = Database::open(store.clone(), config()).unwrap();
                db.put(1, vec![1., 2.]).unwrap();
                if prior {
                    db.checkpoint().unwrap();
                }
                db.delete(1).unwrap();
                db.put(2, vec![3., 4.]).unwrap();
                store.0.borrow_mut().fault = Some((publish, panic));
                let outcome =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.checkpoint()));
                if panic {
                    assert!(outcome.is_err());
                } else {
                    assert!(outcome.unwrap().is_err());
                }
                assert_eq!(db.get(1), None);
                assert_eq!(db.get(2), Some([3., 4.].as_slice()));
                assert!(matches!(
                    db.put(3, vec![5., 6.]),
                    Err(Error::RecoveryRequired)
                ));
                assert!(matches!(db.delete(2), Err(Error::RecoveryRequired)));
                assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
                drop(db);
                assert_eq!(store.0.borrow().objects.contains_key(&segment(3)), publish);
                let mut db = Database::open(store.clone(), config()).unwrap();
                assert_eq!(db.get(1), None);
                assert_eq!(db.get(2), Some([3., 4.].as_slice()));
                db.checkpoint().unwrap(); // retry absent snapshot or no-op if complete
                db.put(3, vec![5., 6.]).unwrap();
                drop(db);
                let db = Database::open(store.clone(), config()).unwrap();
                assert_eq!(db.get(3), Some([5., 6.].as_slice()));
                assert!(store.0.borrow().objects.contains_key(&mutation(4)));
            }
        }
    }
}

fn checkpointed_store() -> Memory {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.checkpoint().unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    db.checkpoint().unwrap();
    store
}
#[test]
fn invalid_latest_segment_is_not_hidden_by_fallback_to_an_older_snapshot_or_log() {
    let invalid = [
        json!({"version":3,"sequence":2,"config":config(),"documents":[]}),
        json!({"version":1,"sequence":1,"config":config(),"documents":[]}),
        json!({"version":1,"sequence":2,"config":{"dimensions":1,"metric":"manhattan"},"documents":[]}),
        json!({"version":1,"sequence":2,"config":config(),"documents":[[1,[1.,2.]],[1,[3.,4.]]]}),
        json!({"version":1,"sequence":2,"config":config(),"documents":[[2,[1.,2.]],[1,[3.,4.]]]}),
        json!({"version":1,"sequence":2,"config":config(),"documents":[[1,[1.]]]}),
        json!({"version":1,"sequence":2,"config":config(),"documents":[[1,[null,2.]]]}),
        json!({"version":1,"sequence":2,"config":config(),"documents":[],"unknown":0}),
        Value::Null,
    ];
    for value in invalid {
        let store = checkpointed_store();
        store
            .0
            .borrow_mut()
            .objects
            .insert(segment(2), serde_json::to_vec(&value).unwrap());
        assert!(
            matches!(Database::open(store, config()), Err(Error::Corrupt(_))),
            "{value}"
        );
    }
    let store = checkpointed_store();
    store.0.borrow_mut().missing_get = Some(segment(2));
    assert!(matches!(
        Database::open(store, config()),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn segments_do_not_hide_missing_log_keys_or_unexpected_objects() {
    for missing in ["metadata".to_string(), mutation(1), mutation(2)] {
        let store = checkpointed_store();
        store.0.borrow_mut().objects.remove(&missing);
        assert!(Database::open(store, config()).is_err(), "{missing}");
    }
    for invalid in [
        "segment-2",
        "segment-+0000000000000000002",
        "segment-00000000000000000003",
        "segment-99999999999999999999",
        "unknown",
    ] {
        let store = checkpointed_store();
        store
            .0
            .borrow_mut()
            .objects
            .insert(invalid.into(), b"{}".to_vec());
        assert!(
            matches!(Database::open(store, config()), Err(Error::Corrupt(_))),
            "{invalid}"
        );
    }
    for key in ["metadata".into(), segment(2), mutation(1)] {
        let store = checkpointed_store();
        store.0.borrow_mut().duplicate_key = Some(key);
        assert!(matches!(
            Database::open(store, config()),
            Err(Error::Corrupt(_))
        ));
    }
    // Covered payloads are not re-read by the engine. The complete snapshot is
    // authoritative for this prefix; backends may still validate stored envelopes.
    let store = checkpointed_store();
    store
        .0
        .borrow_mut()
        .objects
        .insert(mutation(1), b"obsolete payload".to_vec());
    assert!(Database::open(store.clone(), config()).is_ok());
    store
        .0
        .borrow_mut()
        .objects
        .insert(mutation(3), b"corrupt tail".to_vec());
    assert!(matches!(
        Database::open(store, config()),
        Err(Error::Corrupt(_))
    ));
}

#[test]
fn seeded_mixed_history_matches_full_replay_for_both_metrics() {
    const SEED: u64 = 0x73e9_0142;
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let cfg = Config { metric, ..config() };
        let log = Memory::default();
        let snapshots = Memory::default();
        let mut oracle = Database::open(log.clone(), cfg).unwrap();
        let mut db = Database::open(snapshots.clone(), cfg).unwrap();
        let mut rng = SEED;
        for step in 0..300 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let id = (rng >> 32) % 20;
            if rng.is_multiple_of(4) {
                oracle.delete(id).unwrap();
                db.delete(id).unwrap();
            } else {
                let vector = vec![(rng % 31) as f32, step as f32];
                oracle.put(id, vector.clone()).unwrap();
                db.put(id, vector).unwrap();
            }
            if step % 17 == 0 {
                db.checkpoint().unwrap();
                drop(db);
                drop(oracle);
                db = Database::open(snapshots.clone(), cfg).unwrap();
                oracle = Database::open(log.clone(), cfg).unwrap();
            }
            for id in 0..20 {
                assert_eq!(
                    db.get(id),
                    oracle.get(id),
                    "seed={SEED}, step={step}, metric={metric:?}"
                );
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
fn snapshot_roundtrips_f32_bits_and_empty_deleted_state() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    let values = [
        0.,
        -0.,
        f32::from_bits(1),
        f32::MIN_POSITIVE,
        f32::MAX,
        -f32::MAX,
        0.1,
    ];
    for (i, &v) in values.iter().enumerate() {
        db.put(i as u64, vec![v, v]).unwrap();
    }
    db.checkpoint().unwrap();
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    for (i, &v) in values.iter().enumerate() {
        assert_eq!(db.get(i as u64).unwrap()[0].to_bits(), v.to_bits());
        db.delete(i as u64).unwrap();
    }
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(store, config()).unwrap();
    assert!(db.search(&[0., 0.], 99).unwrap().is_empty());
}

#[test]
fn uncertain_tail_after_checkpoint_cannot_be_checkpointed_away() {
    for published in [false, true] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config()).unwrap();
        db.put(1, vec![1., 2.]).unwrap();
        db.checkpoint().unwrap();
        store.0.borrow_mut().fault = Some((published, false));
        assert!(db.put(2, vec![3., 4.]).is_err());
        assert!(matches!(db.checkpoint(), Err(Error::RecoveryRequired)));
        assert_eq!(db.get(2), None);
        drop(db);
        let mut db = Database::open(store.clone(), config()).unwrap();
        assert_eq!(db.get(2).is_some(), published);
        db.delete(1).unwrap();
        db.checkpoint().unwrap();
        drop(db);
        let db = Database::open(store.clone(), config()).unwrap();
        assert_eq!(db.get(1), None);
        assert_eq!(db.get(2).is_some(), published);
        let sequence = if published { 3 } else { 2 };
        assert!(store.0.borrow().objects.contains_key(&mutation(sequence)));
        assert!(store.0.borrow().objects.contains_key(&segment(sequence)));
    }
}
