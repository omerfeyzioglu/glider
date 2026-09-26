use glider::{
    ownership::claims,
    recovery::stage_isolated_namespace,
    serving::{SearchMode, ServingOptions, SingleMachine},
    store::ObjectStore,
    Config, Database, Error, Metric, Mutation,
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
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.objects.borrow().get(key).cloned())
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.objects.borrow().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        let f = self.fault.get();
        if f == 1 && key.starts_with("mutation-") {
            self.fault.set(0);
            return Err(Error::RecoveryRequired);
        }
        if self.objects.borrow().contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        self.objects.borrow_mut().insert(key.into(), value.to_vec());
        if (f == 2 && key.starts_with("mutation-"))
            || (f == 3 && key.starts_with("compacted-"))
            || f == 5
        {
            self.fault.set(0);
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.objects.borrow_mut().remove(key);
        if self.fault.get() == 4 {
            self.fault.set(0);
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
}
fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
fn options() -> ServingOptions {
    let mut o = ServingOptions::m8();
    o.max_documents = 3;
    o.maintenance.soft_tail_objects = 2;
    o.maintenance.hard_tail_objects = 3;
    o
}
fn put(id: u64) -> Mutation {
    Mutation::Put {
        id,
        vector: vec![id as f32, 0.],
        metadata: BTreeMap::new(),
    }
}
fn open(store: Memory) -> SingleMachine<Memory> {
    SingleMachine::open(store, config(), options()).unwrap()
}
#[test]
fn capacity_validation_precedes_io_and_maintenance_runs_before_next_batch() {
    let store = Memory::default();
    let mut db = open(store.clone());
    db.apply_batch(vec![put(1)]).unwrap();
    db.apply_batch(vec![put(2)]).unwrap();
    let before = store.list().unwrap();
    assert!(db.apply_batch(vec![put(3), put(4)]).is_err());
    assert!(db.apply_batch(vec![]).is_err());
    assert!(db
        .apply_batch(vec![Mutation::Put {
            id: 1,
            vector: vec![f32::NAN, 0.],
            metadata: BTreeMap::new()
        }])
        .is_err());
    assert!(db
        .apply_batch(vec![Mutation::Put {
            id: 1,
            vector: vec![0., 0.],
            metadata: BTreeMap::from([("large".into(), "x".repeat(4096))])
        }])
        .is_err());
    assert_eq!(store.list().unwrap(), before);
    db.apply_batch(vec![put(3)]).unwrap();
    assert_eq!(db.status().maintenance_runs, 1);
    assert_eq!(db.status().maintenance.tail_objects, 1);
    assert!(db
        .query(&[0., 0.], 1, &[], SearchMode::Approximate)
        .is_err());
    assert!(db.query(&[0., 0.], 4, &[], SearchMode::Exact).is_err());
    db.close().unwrap();
    let db = open(store);
    assert_eq!(db.status().documents, 3);
    assert_eq!(db.get(3).unwrap().0, &[3., 0.]);
    db.close().unwrap();
}
#[test]
fn failed_scheduled_maintenance_never_publishes_submitted_batch() {
    for fault in [3, 4] {
        let source = Memory::default();
        let mut db = open(source.clone());
        db.apply_batch(vec![put(1)]).unwrap();
        db.apply_batch(vec![put(2)]).unwrap();
        source.fault.set(fault);
        assert!(db.apply_batch(vec![put(3)]).is_err());
        assert_eq!(db.status().maintenance.sequence, 2);
        assert!(db.get(3).is_none());
        assert!(db.status().recovery_required);
        assert_eq!(db.status().storage_errors, 1);
        assert!(matches!(db.close(), Err(Error::RecoveryRequired)));
        assert_eq!(claims(&source).unwrap().len(), 1);
        let target = Memory::default();
        stage_isolated_namespace(&source, target.clone(), config()).unwrap();
        let mut db = open(target);
        db.maintain().unwrap();
        assert_eq!(db.status().documents, 2);
        assert!(db.get(3).is_none());
        db.apply_batch(vec![put(3)]).unwrap();
        db.close().unwrap();
    }
}
#[test]
fn uncertain_batch_is_atomic_and_requires_isolated_takeover() {
    for fault in [1, 2] {
        let source = Memory::default();
        let mut db = open(source.clone());
        db.apply_batch(vec![put(1)]).unwrap();
        source.fault.set(fault);
        assert!(db
            .apply_batch(vec![Mutation::Delete { id: 1 }, put(2)])
            .is_err());
        assert!(db.get(1).is_some());
        assert!(db.get(2).is_none());
        assert!(db.backup_to(Memory::default()).is_err());
        assert!(db.close().is_err());
        let target = Memory::default();
        stage_isolated_namespace(&source, target.clone(), config()).unwrap();
        let db = open(target);
        assert_eq!(db.get(1).is_some(), fault == 1);
        assert_eq!(db.get(2).is_some(), fault == 2);
        db.close().unwrap();
    }
}
#[test]
fn interrupted_backup_is_unpromoted_and_source_stays_writable() {
    let source = Memory::default();
    let mut db = open(source);
    db.apply_batch(vec![put(1), put(2)]).unwrap();
    let bad = Memory::default();
    bad.fault.set(5);
    assert!(db.backup_to(bad.clone()).is_err());
    assert_eq!(db.status().backup_errors, 1);
    assert!(!db.status().recovery_required);
    assert!(bad.get("metadata").unwrap().is_none());
    assert!(Database::open(bad, config()).is_err());
    db.apply_batch(vec![put(3)]).unwrap();
    let backup = Memory::default();
    db.backup_to(backup.clone()).unwrap();
    db.apply_batch(vec![Mutation::Delete { id: 1 }]).unwrap();
    let restored = Memory::default();
    stage_isolated_namespace(&backup, restored.clone(), config()).unwrap();
    let copy = open(restored);
    assert_eq!(copy.status().documents, 3);
    assert!(copy.get(1).is_some());
    assert!(db.get(1).is_none());
    copy.close().unwrap();
    db.close().unwrap();
}
#[test]
fn legacy_v1_v2_migrate_through_chunked_backup_and_restore() {
    let mut source = Memory::default();
    source
        .create(
            "metadata",
            br#"{"version":1,"config":{"dimensions":2,"metric":"squared_euclidean"}}"#,
        )
        .unwrap();
    source
        .create(
            "mutation-00000000000000000001",
            br#"{"version":1,"sequence":1,"mutation":{"type":"put","id":1,"vector":[1,0]}}"#,
        )
        .unwrap();
    source.create("mutation-00000000000000000002",br#"{"version":2,"sequence":2,"mutation":{"type":"put","id":2,"vector":[2,0],"metadata":{"selected":"true"}}}"#).unwrap();
    let mut chunked = options();
    chunked.chunk_bytes = Some(131_072);
    let mut db = SingleMachine::open(source.clone(), config(), chunked).unwrap();
    let backup = Memory::default();
    db.backup_to(backup.clone()).unwrap();
    let root = source
        .get("compacted-00000000000000000002")
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&root).unwrap()["version"],
        3
    );
    db.close().unwrap();
    let restored = Memory::default();
    stage_isolated_namespace(&backup, restored.clone(), config()).unwrap();
    let db = open(restored);
    assert!(db.get(1).unwrap().1.is_empty());
    assert_eq!(
        db.query(&[0., 0.], 3, &[("selected", "true")], SearchMode::Exact)
            .unwrap()[0]
            .id,
        2
    );
    db.close().unwrap();
}
#[test]
fn rejected_open_releases_its_clean_claim() {
    let source = Memory::default();
    let mut db = open(source.clone());
    db.apply_batch(vec![put(1), put(2)]).unwrap();
    db.close().unwrap();
    let mut small = options();
    small.max_documents = 1;
    assert!(SingleMachine::open(source.clone(), config(), small).is_err());
    assert!(claims(&source).unwrap().is_empty());
    open(source).close().unwrap();
}
