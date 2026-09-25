use glider::{
    store::{LocalStore, ObjectStore},
    Config, Database, Error, Metric, Result,
};
use sha2::{Digest, Sha256};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

#[derive(Clone, Default)]
struct Memory {
    objects: Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
    creates: Rc<Cell<usize>>,
    fail_create: Rc<Cell<Option<(usize, bool)>>>,
    fail_remove: Rc<Cell<Option<bool>>>,
}
impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.borrow().get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.objects.borrow().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        let attempt = self.creates.get() + 1;
        self.creates.set(attempt);
        let fail = self.fail_create.get() == Some((attempt, false));
        if fail {
            self.fail_create.set(None);
            return Err(Error::RecoveryRequired);
        }
        if self.objects.borrow().contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        self.objects.borrow_mut().insert(key.into(), value.to_vec());
        if self.fail_create.get() == Some((attempt, true)) {
            self.fail_create.set(None);
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        if self.fail_remove.get() == Some(false) {
            self.fail_remove.set(None);
            return Err(Error::RecoveryRequired);
        }
        self.objects.borrow_mut().remove(key);
        if self.fail_remove.get() == Some(true) {
            self.fail_remove.set(None);
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
}
fn config(metric: Metric) -> Config {
    Config {
        dimensions: 2,
        metric,
    }
}
fn populate(db: &mut Database<impl ObjectStore>) {
    for id in 0..13 {
        db.put_with_metadata(
            id,
            vec![(id % 5) as f32, (id / 5) as f32],
            BTreeMap::from([("group".into(), (id % 3).to_string())]),
        )
        .unwrap();
    }
}
fn snapshot(db: &Database<impl ObjectStore>) -> Vec<(u64, Vec<f32>, BTreeMap<String, String>)> {
    (0..13)
        .filter_map(|id| {
            db.get(id)
                .map(|vector| (id, vector.to_vec(), db.get_metadata(id).unwrap().clone()))
        })
        .collect()
}
fn chunk_keys(store: &Memory) -> Vec<String> {
    store
        .objects
        .borrow()
        .keys()
        .filter(|key| key.starts_with("segmentchunk-") || key.starts_with("compactedchunk-"))
        .cloned()
        .collect()
}

#[test]
fn chunked_checkpoint_and_compaction_roundtrip_with_bounded_objects() {
    for metric in [Metric::SquaredEuclidean, Metric::Manhattan] {
        let store = Memory::default();
        let cfg = config(metric);
        let mut db = Database::open(store.clone(), cfg).unwrap();
        populate(&mut db);
        let expected = snapshot(&db);
        let exact = db.search(&[2., 1.], 13).unwrap();
        db.checkpoint_chunked(240).unwrap();
        let keys = chunk_keys(&store);
        assert!(keys.len() > 1);
        for key in &keys {
            assert!(store.objects.borrow()[key].len() <= 240);
        }
        drop(db);

        let mut db = Database::open(store.clone(), cfg).unwrap();
        assert_eq!(snapshot(&db), expected);
        assert_eq!(db.search(&[2., 1.], 13).unwrap(), exact);
        db.put(2, vec![99., 99.]).unwrap();
        db.compact_chunked(240).unwrap();
        assert!(chunk_keys(&store)
            .iter()
            .all(|key| key.starts_with("compactedchunk-")));
        assert_eq!(
            store
                .objects
                .borrow()
                .keys()
                .filter(|k| k.starts_with("mutation-"))
                .count(),
            0
        );
        drop(db);

        let mut db = Database::open(store.clone(), cfg).unwrap();
        assert_eq!(db.get(2), Some([99., 99.].as_slice()));
        db.compact().unwrap(); // Repeated cleanup accepts an existing v3 manifest.
        db.put(14, vec![0., 0.]).unwrap();
        db.checkpoint().unwrap(); // A newer legacy snapshot remains readable.
        drop(db);
        let db = Database::open(store, cfg).unwrap();
        assert_eq!(db.get(14), Some([0., 0.].as_slice()));
    }
}

#[test]
fn failed_chunk_or_manifest_publication_recovers_without_partial_state() {
    for after in [false, true] {
        for offset in [1, 2, 3] {
            let store = Memory::default();
            let cfg = config(Metric::SquaredEuclidean);
            let mut db = Database::open(store.clone(), cfg).unwrap();
            populate(&mut db);
            let expected = snapshot(&db);
            // Limit yields multiple chunks; the first two attempts are chunks.
            store
                .fail_create
                .set(Some((store.creates.get() + offset, after)));
            assert!(db.checkpoint_chunked(240).is_err());
            assert_eq!(snapshot(&db), expected);
            assert!(matches!(
                db.put(99, vec![0., 0.]),
                Err(Error::RecoveryRequired)
            ));
            drop(db);

            let mut db = Database::open(store.clone(), cfg).unwrap();
            assert_eq!(snapshot(&db), expected);
            db.checkpoint_chunked(240).unwrap();
            db.put(99, vec![0., 0.]).unwrap();
            db.compact_chunked(240).unwrap();
            drop(db);
            let db = Database::open(store, cfg).unwrap();
            assert_eq!(db.get(99), Some([0., 0.].as_slice()));
        }
    }
}

#[test]
fn chunked_compaction_publication_and_cleanup_failures_resume_safely() {
    let cfg = config(Metric::SquaredEuclidean);
    let baseline = Memory::default();
    let mut db = Database::open(baseline.clone(), cfg).unwrap();
    populate(&mut db);
    db.compact_chunked(240).unwrap();
    let chunks = chunk_keys(&baseline).len();
    assert!(chunks > 1);

    for after in [false, true] {
        for offset in [1, chunks, chunks + 1] {
            let store = Memory::default();
            let mut db = Database::open(store.clone(), cfg).unwrap();
            populate(&mut db);
            let expected = snapshot(&db);
            store
                .fail_create
                .set(Some((store.creates.get() + offset, after)));
            assert!(db.compact_chunked(240).is_err());
            assert_eq!(snapshot(&db), expected);
            assert!(matches!(db.delete(0), Err(Error::RecoveryRequired)));
            drop(db);
            let mut db = Database::open(store.clone(), cfg).unwrap();
            assert_eq!(snapshot(&db), expected);
            db.compact_chunked(240).unwrap();
            assert_eq!(
                store
                    .objects
                    .borrow()
                    .keys()
                    .filter(|k| k.starts_with("mutation-"))
                    .count(),
                0
            );
            drop(db);
            assert_eq!(snapshot(&Database::open(store, cfg).unwrap()), expected);
        }
    }
    for after in [false, true] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), cfg).unwrap();
        populate(&mut db);
        let expected = snapshot(&db);
        store.fail_remove.set(Some(after));
        assert!(db.compact_chunked(240).is_err());
        drop(db);
        let mut db = Database::open(store.clone(), cfg).unwrap();
        assert_eq!(snapshot(&db), expected);
        db.compact_chunked(240).unwrap();
        assert_eq!(
            store
                .objects
                .borrow()
                .keys()
                .filter(|k| k.starts_with("mutation-"))
                .count(),
            0
        );
    }
}

#[test]
fn different_chunk_limit_after_failed_publication_uses_distinct_keys() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    populate(&mut db);
    store
        .fail_create
        .set(Some((store.creates.get() + 2, false)));
    assert!(db.checkpoint_chunked(240).is_err());
    drop(db);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.checkpoint_chunked(320).unwrap();
    assert!(chunk_keys(&store)
        .iter()
        .any(|key| key.contains("00000000000000000240")));
    assert!(chunk_keys(&store)
        .iter()
        .any(|key| key.contains("00000000000000000320")));
    db.compact_chunked(320).unwrap();
    assert!(chunk_keys(&store)
        .iter()
        .all(|key| key.starts_with("compactedchunk-")));
}

#[test]
fn missing_and_corrupt_referenced_chunks_fail_recovery_without_fallback() {
    for corrupt in [false, true] {
        let store = Memory::default();
        let cfg = config(Metric::SquaredEuclidean);
        let mut db = Database::open(store.clone(), cfg).unwrap();
        populate(&mut db);
        db.checkpoint_chunked(240).unwrap();
        drop(db);
        let first = chunk_keys(&store)[0].clone();
        if corrupt {
            store.objects.borrow_mut().get_mut(&first).unwrap()[0] ^= 1;
        } else {
            store.objects.borrow_mut().remove(&first);
        }
        assert!(matches!(Database::open(store, cfg), Err(Error::Corrupt(_))));
    }
}

#[test]
fn manifest_digest_cannot_hide_duplicate_ids_or_bad_vectors() {
    for bad_vector in [false, true] {
        let store = Memory::default();
        let cfg = config(Metric::SquaredEuclidean);
        let mut db = Database::open(store.clone(), cfg).unwrap();
        populate(&mut db);
        db.checkpoint_chunked(500).unwrap();
        drop(db);
        let first = chunk_keys(&store)[0].clone();
        let mut chunk: serde_json::Value =
            serde_json::from_slice(&store.objects.borrow()[&first]).unwrap();
        if bad_vector {
            chunk["documents"][0][1]["vector"] = serde_json::json!([1.0]);
        } else {
            let id = chunk["documents"][0][0].clone();
            chunk["documents"][1][0] = id;
        }
        let bytes = serde_json::to_vec(&chunk).unwrap();
        store.objects.borrow_mut().insert(first, bytes.clone());
        let manifest_key = "segment-00000000000000000013";
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&store.objects.borrow()[manifest_key]).unwrap();
        manifest["chunks"][0]["sha256"] =
            serde_json::json!(format!("{:x}", Sha256::digest(&bytes)));
        store
            .objects
            .borrow_mut()
            .insert(manifest_key.into(), serde_json::to_vec(&manifest).unwrap());
        assert!(matches!(Database::open(store, cfg), Err(Error::Corrupt(_))));
    }
}

#[test]
fn local_chunked_snapshot_survives_restart_and_reclaims_old_history() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    populate(&mut db);
    db.checkpoint_chunked(240).unwrap();
    db.put(13, vec![7., 8.]).unwrap();
    db.compact_chunked(240).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(&path).unwrap(), cfg).unwrap();
    assert_eq!(db.get(13), Some([7., 8.].as_slice()));
    assert_eq!(db.search(&[7., 8.], 1).unwrap()[0].id, 13);
}

#[test]
fn oversized_single_document_is_rejected_before_publication() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.put_with_metadata(
        1,
        vec![1., 2.],
        BTreeMap::from([("long".into(), "x".repeat(500))]),
    )
    .unwrap();
    let before = store.creates.get();
    assert!(matches!(db.checkpoint_chunked(200), Err(Error::Invalid(_))));
    assert_eq!(store.creates.get(), before);
    assert!(chunk_keys(&store).is_empty());
    db.checkpoint_chunked(1024).unwrap();
    drop(db);
    assert_eq!(
        Database::open(store, cfg).unwrap().get(1),
        Some([1., 2.].as_slice())
    );
}

#[test]
fn empty_chunked_compaction_has_no_data_chunks() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    db.compact_chunked(256).unwrap();
    assert!(chunk_keys(&store).is_empty());
    drop(db);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    assert!(db.search(&[0., 0.], 1).unwrap().is_empty());
    db.put(1, vec![1., 1.]).unwrap();
    db.compact_chunked(256).unwrap();
    drop(db);
    assert_eq!(
        Database::open(store, cfg).unwrap().get(1),
        Some([1., 1.].as_slice())
    );
}

#[test]
fn legacy_compaction_reclaims_chunked_checkpoint_objects() {
    let store = Memory::default();
    let cfg = config(Metric::SquaredEuclidean);
    let mut db = Database::open(store.clone(), cfg).unwrap();
    populate(&mut db);
    let expected = snapshot(&db);
    db.checkpoint_chunked(240).unwrap();
    assert!(!chunk_keys(&store).is_empty());
    db.compact().unwrap();
    assert!(chunk_keys(&store).is_empty());
    drop(db);
    assert_eq!(snapshot(&Database::open(store, cfg).unwrap()), expected);
}
