use super::*;
use crate::{Config, Database, Metric};
use std::{cell::RefCell, rc::Rc};

#[derive(Default)]
pub(super) struct Fault {
    target: Option<(String, usize)>,
    trace: Vec<String>,
    panic: bool,
}
impl Fault {
    fn arm(&mut self, point: &str, occurrence: usize) {
        self.target = Some((point.into(), occurrence));
        self.trace.clear();
        self.panic = false;
    }
    pub(super) fn hit(&mut self, point: &str) -> std::io::Result<()> {
        self.trace.push(point.into());
        if let Some((target, remaining)) = &mut self.target {
            if target == point {
                if *remaining == 0 {
                    self.target = None;
                    assert!(!self.panic, "injected panic at {point}");
                    return Err(std::io::Error::other(format!("injected at {point}")));
                }
                *remaining -= 1;
            }
        }
        Ok(())
    }
    fn fired(&self) {
        assert!(
            self.target.is_none(),
            "fault not reached: {:?}",
            self.target
        );
    }
}
fn config() -> Config {
    Config {
        dimensions: 1,
        metric: Metric::SquaredEuclidean,
    }
}
fn open(root: &Path, fault: &Rc<RefCell<Fault>>) -> Result<LocalStore> {
    LocalStore::open_inner(root, fault.clone())
}
fn publication_points() -> Vec<String> {
    let mut points = Vec::new();
    for operation in [
        "body-create",
        "body-write",
        "body-sync",
        "body-directory-sync",
        "seal-create",
        "seal-write",
        "seal-sync",
        "seal-directory-sync",
    ] {
        points.push(format!("{operation}-before"));
        if operation.ends_with("-write") {
            points.push(format!("{operation}-partial"));
        }
        points.push(format!("{operation}-after"));
    }
    points
}
fn assert_prefix(db: &Database<LocalStore>) {
    assert_eq!(db.get(1), Some([1.].as_slice()));
    assert_eq!(db.get(2), Some([2.].as_slice()));
}
fn finish(root: &Path, tail_present: bool) {
    let mut db = Database::open(LocalStore::open(root).unwrap(), config()).unwrap();
    assert_prefix(&db);
    assert_eq!(db.get(3), tail_present.then_some([3.].as_slice()));
    let recovered_sequence = if tail_present { 3 } else { 2 };
    assert_eq!(db.sequence, recovered_sequence);
    db.put(4, vec![4.]).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(root).unwrap(), config()).unwrap();
    assert_prefix(&db);
    assert_eq!(db.get(3), tail_present.then_some([3.].as_slice()));
    assert_eq!(db.get(4), Some([4.].as_slice()));
    assert_eq!(db.sequence, recovered_sequence + 1);
}

#[test]
fn publication_failures_preserve_prefix_and_recover_sequence() {
    for point in publication_points() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("db");
        let fault = Rc::new(RefCell::new(Fault::default()));
        let mut db = Database::open(open(&root, &fault).unwrap(), config()).unwrap();
        db.put(1, vec![1.]).unwrap();
        db.put(2, vec![2.]).unwrap();
        fault.borrow_mut().arm(&point, 0);
        assert!(db.put(3, vec![3.]).is_err(), "{point}");
        fault.borrow().fired();
        assert_prefix(&db);
        assert_eq!(db.get(3), None);
        assert!(
            matches!(db.put(4, vec![4.]), Err(Error::RecoveryRequired)),
            "{point}"
        );
        assert!(
            matches!(db.store.get("metadata"), Err(Error::RecoveryRequired)),
            "{point}"
        );
        assert!(
            matches!(db.store.list(), Err(Error::RecoveryRequired)),
            "{point}"
        );
        assert!(
            matches!(db.store.create("other", b"x"), Err(Error::RecoveryRequired)),
            "{point}"
        );
        drop(db);
        // These are process-visible interruption states, not a simulation of power
        // loss: a full seal remains visible even if its synchronization was skipped.
        let tail_present = matches!(
            point.as_str(),
            "seal-write-after"
                | "seal-sync-before"
                | "seal-sync-after"
                | "seal-directory-sync-before"
                | "seal-directory-sync-after"
        );
        finish(&root, tail_present);
    }
}

#[test]
fn failed_metadata_handle_cannot_bypass_stabilization() {
    for point in publication_points() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("db");
        let fault = Rc::new(RefCell::new(Fault::default()));
        let mut store = open(&root, &fault).unwrap();
        fault.borrow_mut().arm(&point, 0);
        let metadata = crate::encode(&crate::Metadata {
            version: 1,
            config: config(),
        })
        .unwrap();
        assert!(store.create("metadata", &metadata).is_err(), "{point}");
        fault.borrow().fired();
        assert!(
            matches!(
                Database::open(store, config()),
                Err(Error::RecoveryRequired)
            ),
            "{point}"
        );
        let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
        db.put(1, vec![1.]).unwrap();
        drop(db);
        let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
        assert_eq!(db.get(1), Some([1.].as_slice()));
        assert_eq!(db.sequence, 1);
    }
}

#[test]
fn recovery_sync_failures_never_return_a_usable_store() {
    for operation in [
        "open-directory-sync",
        "open-parent-sync",
        "recover-body-sync",
        "recover-seal-sync",
        "recover-directory-sync",
    ] {
        let occurrences = if operation == "recover-body-sync" || operation == "recover-seal-sync" {
            4
        } else {
            1
        };
        for occurrence in 0..occurrences {
            for side in ["before", "after"] {
                let temp = tempfile::tempdir().unwrap();
                let root = temp.path().join("db");
                let fault = Rc::new(RefCell::new(Fault::default()));
                let mut db = Database::open(open(&root, &fault).unwrap(), config()).unwrap();
                db.put(1, vec![1.]).unwrap();
                db.put(2, vec![2.]).unwrap();
                fault.borrow_mut().arm("seal-write-after", 0);
                assert!(db.put(3, vec![3.]).is_err());
                drop(db);
                // Fail recovery twice at the same boundary, including late in the
                // recovered prefix. No partial success may escape either attempt.
                for _ in 0..2 {
                    fault
                        .borrow_mut()
                        .arm(&format!("{operation}-{side}"), occurrence);
                    assert!(
                        open(&root, &fault).is_err(),
                        "{operation}-{side}/{occurrence}"
                    );
                    fault.borrow().fired();
                }
                finish(&root, true);
            }
        }
    }
}

#[test]
fn interrupted_reclamation_can_be_interrupted_again() {
    for point in publication_points() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("db");
        let fault = Rc::new(RefCell::new(Fault::default()));
        let mut db = Database::open(open(&root, &fault).unwrap(), config()).unwrap();
        db.put(1, vec![1.]).unwrap();
        db.put(2, vec![2.]).unwrap();
        fault.borrow_mut().arm("seal-write-partial", 0);
        assert!(db.put(99, vec![99.]).is_err());
        drop(db);
        let mut db = Database::open(open(&root, &fault).unwrap(), config()).unwrap();
        assert_eq!(db.get(99), None);
        fault.borrow_mut().arm(&point, 0);
        assert!(db.put(3, vec![3.]).is_err(), "{point}");
        fault.borrow().fired();
        drop(db);
        let tail_present = matches!(
            point.as_str(),
            "seal-write-after"
                | "seal-sync-before"
                | "seal-sync-after"
                | "seal-directory-sync-before"
                | "seal-directory-sync-after"
        );
        finish(&root, tail_present);
        let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
        assert_eq!(db.get(99), None);
    }
}

#[test]
fn publication_and_recovery_barriers_remain_ordered() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let fault = Rc::new(RefCell::new(Fault::default()));
    let mut store = open(&root, &fault).unwrap();
    fault.borrow_mut().trace.clear();
    store.create("object", b"value").unwrap();
    assert_eq!(fault.borrow().trace, publication_points());
    drop(store);
    fault.borrow_mut().trace.clear();
    drop(open(&root, &fault).unwrap());
    let expected: Vec<_> = [
        "open-directory-sync",
        "open-parent-sync",
        "recover-body-sync",
        "recover-seal-sync",
        "recover-directory-sync",
    ]
    .iter()
    .flat_map(|op| [format!("{op}-before"), format!("{op}-after")])
    .collect();
    assert_eq!(fault.borrow().trace, expected);
}

#[test]
fn local_publication_panic_also_blocks_recovery_on_the_same_handle() {
    for point in ["body-create-before", "seal-write-after"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("db");
        let fault = Rc::new(RefCell::new(Fault::default()));
        let mut store = open(&root, &fault).unwrap();
        fault.borrow_mut().arm(point, 0);
        fault.borrow_mut().panic = true;
        let metadata = crate::encode(&crate::Metadata {
            version: 1,
            config: config(),
        })
        .unwrap();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || store.create("metadata", &metadata)
        ))
        .is_err());
        fault.borrow().fired();
        assert!(matches!(
            store.get("metadata"),
            Err(Error::RecoveryRequired)
        ));
        assert!(matches!(
            store.create("other", b"x"),
            Err(Error::RecoveryRequired)
        ));
        assert!(matches!(
            Database::open(store, config()),
            Err(Error::RecoveryRequired)
        ));
        let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
        db.put(1, vec![1.]).unwrap();
        drop(db);
        let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
        assert_eq!(db.get(1), Some([1.].as_slice()));
    }
}
