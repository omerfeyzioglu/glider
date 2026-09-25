use glider::{
    ownership::{claims, clear_stale_claim, OwnedDatabase},
    recovery::stage_isolated_namespace,
    store::ObjectStore,
    Config, Database, Error, Metric,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
};

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}

#[derive(Clone, Default)]
struct Memory {
    objects: Rc<RefCell<BTreeMap<String, Vec<u8>>>>,
}

impl ObjectStore for Memory {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.objects.borrow().get(key).cloned())
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.objects.borrow().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        let mut objects = self.objects.borrow_mut();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        Ok(())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.objects.borrow_mut().remove(key);
        Ok(())
    }
}

struct TimedOutPut {
    inner: Memory,
    armed: Rc<Cell<bool>>,
    pending: Rc<RefCell<Option<(String, Vec<u8>)>>>,
}

impl ObjectStore for TimedOutPut {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        if self.armed.get() && key.starts_with("mutation-") {
            *self.pending.borrow_mut() = Some((key.into(), value.to_vec()));
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "deferred PUT").into());
        }
        self.inner.create(key, value)
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)
    }
}

#[test]
fn late_old_put_cannot_change_a_staged_new_prefix() {
    let mut old = Memory::default();
    let new = Memory::default();
    let armed = Rc::new(Cell::new(false));
    let pending = Rc::new(RefCell::new(None));
    let mut db = OwnedDatabase::open(
        TimedOutPut {
            inner: old.clone(),
            armed: armed.clone(),
            pending: pending.clone(),
        },
        config(),
    )
    .unwrap();
    db.put(1, vec![1., 0.]).unwrap();
    armed.set(true);
    assert!(matches!(db.put(2, vec![2., 0.]), Err(Error::Io(_))));
    assert_eq!(db.get(2), None);
    drop(db); // Simulate a dead writer with one request still in flight.

    stage_isolated_namespace(&old, new.clone(), config()).unwrap();
    let mut new_owner = OwnedDatabase::open(new.clone(), config()).unwrap();
    assert_eq!(new_owner.get(1), Some([1., 0.].as_slice()));
    assert_eq!(new_owner.get(2), None);

    // This deliberately demonstrates why clearing the old claim and reopening
    // its prefix is unsafe while a timed-out request can still complete.
    let owner = claims(&old).unwrap();
    clear_stale_claim(&mut old, &owner[0]).unwrap();
    let same_prefix_view = OwnedDatabase::open(old.clone(), config()).unwrap();
    assert_eq!(same_prefix_view.get(2), None);
    let (key, bytes) = pending.borrow_mut().take().unwrap();
    old.create(&key, &bytes).unwrap();
    assert_eq!(same_prefix_view.get(2), None); // Silent stale view.
    same_prefix_view.close().unwrap();
    let old_reopened = OwnedDatabase::open(old, config()).unwrap();
    assert_eq!(old_reopened.get(2), Some([2., 0.].as_slice()));
    old_reopened.close().unwrap();

    assert_eq!(new_owner.get(2), None);
    new_owner.put(3, vec![3., 0.]).unwrap();
    new_owner.close().unwrap();
    let db = OwnedDatabase::open(new, config()).unwrap();
    assert_eq!(db.get(1), Some([1., 0.].as_slice()));
    assert_eq!(db.get(2), None);
    assert_eq!(db.get(3), Some([3., 0.].as_slice()));
    db.close().unwrap();
}

struct FailAfterOne {
    inner: Memory,
    writes: usize,
}

impl ObjectStore for FailAfterOne {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        if self.writes == 1 {
            return Err(std::io::Error::other("interrupted staging").into());
        }
        self.writes += 1;
        self.inner.create(key, value)
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)
    }
}

#[test]
fn interrupted_copy_has_no_metadata_and_is_never_promoted() {
    let source = Memory::default();
    let mut db = Database::open(source.clone(), config()).unwrap();
    db.put(1, vec![1., 0.]).unwrap();
    db.put(2, vec![2., 0.]).unwrap();
    drop(db);

    let failed = Memory::default();
    assert!(stage_isolated_namespace(
        &source,
        FailAfterOne {
            inner: failed.clone(),
            writes: 0,
        },
        config()
    )
    .is_err());
    assert_eq!(failed.get("metadata").unwrap(), None);
    assert!(matches!(
        Database::open(failed, config()),
        Err(Error::Corrupt(_))
    ));

    let destination = Memory::default();
    stage_isolated_namespace(&source, destination.clone(), config()).unwrap();
    let restored = OwnedDatabase::open(destination, config()).unwrap();
    assert_eq!(restored.get(2), Some([2., 0.].as_slice()));
    restored.close().unwrap();
}
