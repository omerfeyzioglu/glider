use glider::{
    ownership::{claims, clear_stale_claim, OwnedDatabase},
    store::{LocalStore, ObjectStore},
    Config, Database, Error, Metric,
};
use std::{
    collections::BTreeMap,
    process::Command,
    sync::{Arc, Barrier, Mutex},
};

fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}

#[test]
fn claimed_namespace_rejects_second_and_raw_writers() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut first = OwnedDatabase::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    first.put(1, vec![1., 2.]).unwrap();
    assert!(matches!(
        OwnedDatabase::open(LocalStore::open(&path).unwrap(), config()),
        Err(Error::Busy(_))
    ));
    assert!(matches!(
        Database::open(LocalStore::open(&path).unwrap(), config()),
        Err(Error::Corrupt(_))
    ));
    assert_eq!(first.search(&[1., 2.], 1).unwrap()[0].id, 1);
    first.put(2, vec![3., 4.]).unwrap();
    first.compact_chunked(256).unwrap();
    assert_eq!(claims(&LocalStore::open(&path).unwrap()).unwrap().len(), 1);
    first.close().unwrap();

    let mut reopened = OwnedDatabase::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    assert_eq!(reopened.get(1), Some([1., 2.].as_slice()));
    assert_eq!(reopened.get(2), Some([3., 4.].as_slice()));
    reopened.put(3, vec![5., 6.]).unwrap();
    reopened.close().unwrap();
    assert!(claims(&LocalStore::open(&path).unwrap())
        .unwrap()
        .is_empty());
}

#[test]
fn owner_crash_child() {
    let Some(path) = std::env::var_os("GLIDER_OWNER_CRASH_PATH") else {
        return;
    };
    let mut db = OwnedDatabase::open(LocalStore::open(path).unwrap(), config()).unwrap();
    db.put(7, vec![7., 8.]).unwrap();
    std::process::exit(73);
}

#[test]
fn process_exit_preserves_claim_until_verified_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "owner_crash_child"])
        .env("GLIDER_OWNER_CRASH_PATH", &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    assert!(matches!(
        OwnedDatabase::open(LocalStore::open(&path).unwrap(), config()),
        Err(Error::Busy(_))
    ));
    let mut store = LocalStore::open(&path).unwrap();
    let owner = claims(&store).unwrap();
    assert_eq!(owner.len(), 1);
    clear_stale_claim(&mut store, &owner[0]).unwrap();
    let db = OwnedDatabase::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    assert_eq!(db.get(7), Some([7., 8.].as_slice()));
    db.close().unwrap();
}

#[derive(Clone, Default)]
struct Memory {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    after_claim_create: Option<Arc<Barrier>>,
    lose_claim_ack: bool,
    lose_release_ack: bool,
}

impl ObjectStore for Memory {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        drop(objects);
        if key.starts_with("owner-v1-") {
            if let Some(barrier) = &self.after_claim_create {
                barrier.wait();
            }
            if self.lose_claim_ack {
                return Err(std::io::Error::other("lost claim acknowledgement").into());
            }
        }
        Ok(())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.objects.lock().unwrap().remove(key);
        if key.starts_with("owner-v1-") && self.lose_release_ack {
            return Err(std::io::Error::other("lost release acknowledgement").into());
        }
        Ok(())
    }
}

#[test]
fn simultaneous_claims_cannot_both_become_writers() {
    let store = Memory::default();
    OwnedDatabase::open(store.clone(), config())
        .unwrap()
        .close()
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let after_claim = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let mut contender = store.clone();
            contender.after_claim_create = Some(barrier.clone());
            let after_claim = after_claim.clone();
            std::thread::spawn(move || {
                let result = OwnedDatabase::open(contender, config());
                after_claim.wait();
                match result {
                    Ok(db) => {
                        db.close().unwrap();
                        true
                    }
                    Err(Error::Busy(_)) => false,
                    Err(error) => panic!("unexpected claim error: {error}"),
                }
            })
        })
        .collect();
    assert!(
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap() as usize)
            .sum::<usize>()
            <= 1
    );
    assert!(claims(&store).unwrap().is_empty());
    OwnedDatabase::open(store, config())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn lost_claim_ack_requires_manual_reopen_and_cleanup() {
    let store = Memory::default();
    let mut fault = store.clone();
    fault.lose_claim_ack = true;
    assert!(matches!(
        OwnedDatabase::open(fault, config()),
        Err(Error::Io(_))
    ));
    let owner = claims(&store).unwrap();
    assert_eq!(owner.len(), 1);
    assert!(matches!(
        OwnedDatabase::open(store.clone(), config()),
        Err(Error::Busy(_))
    ));
    clear_stale_claim(&mut store.clone(), &owner[0]).unwrap();
    OwnedDatabase::open(store, config())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn corrupt_stale_claim_is_detected_and_can_be_explicitly_cleared() {
    let mut store = Memory::default();
    let db = OwnedDatabase::open(store.clone(), config()).unwrap();
    let owner = claims(&store).unwrap().pop().unwrap();
    drop(db);
    store
        .objects
        .lock()
        .unwrap()
        .insert(owner.clone(), b"corrupt".to_vec());
    assert!(matches!(claims(&store), Err(Error::Corrupt(_))));
    assert!(matches!(
        OwnedDatabase::open(store.clone(), config()),
        Err(Error::Busy(_))
    ));
    clear_stale_claim(&mut store, &owner).unwrap();
    OwnedDatabase::open(store, config())
        .unwrap()
        .close()
        .unwrap();
}

#[test]
fn lost_release_ack_requires_fresh_inspection() {
    let store = Memory::default();
    let mut fault = store.clone();
    fault.lose_release_ack = true;
    let mut db = OwnedDatabase::open(fault, config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    assert!(matches!(db.close(), Err(Error::Io(_))));
    assert!(claims(&store).unwrap().is_empty());
    let db = OwnedDatabase::open(store, config()).unwrap();
    assert_eq!(db.get(1), Some([1., 2.].as_slice()));
    db.close().unwrap();
}
