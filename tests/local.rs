use glider::{
    store::{LocalStore, ObjectStore},
    Config, Database, Metric,
};
use std::{fs, process::Command};
fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
#[test]
fn local_object_publication_and_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let mut store = LocalStore::open(&root).unwrap();
    store.create("object", b"a value VTSEALED").unwrap();
    assert!(store.create("object", b"replacement").is_err());
    assert_eq!(store.get("object").unwrap().unwrap(), b"a value VTSEALED");
    let body = fs::read(root.join("object-body")).unwrap();
    let seal = fs::read(root.join("object-seal")).unwrap();
    // Every interrupted body/marker prefix is unpublished; payload bytes cannot
    // accidentally be mistaken for a commit marker.
    for n in 0..body.len() {
        fs::write(root.join("pending-body"), &body[..n]).unwrap();
        assert_eq!(store.get("pending").unwrap(), None);
        assert_eq!(store.list().unwrap(), vec!["object"]);
    }
    fs::write(root.join("pending-body"), &body).unwrap();
    for n in 0..seal.len() {
        fs::write(root.join("pending-seal"), &seal[..n]).unwrap();
        assert_eq!(store.get("pending").unwrap(), None);
    }
    store.create("pending", b"new value").unwrap();
    assert_eq!(store.get("pending").unwrap().unwrap(), b"new value");
    let mut damaged = body.clone();
    damaged[16] ^= 1;
    fs::write(root.join("object-body"), damaged).unwrap();
    assert!(store.get("object").is_err());
    assert!(store.list().is_err());
    fs::write(root.join("object-body"), &body[..body.len() - 1]).unwrap();
    assert!(store.get("object").is_err());
    fs::remove_file(root.join("object-body")).unwrap();
    assert!(store.get("object").is_err());
    assert!(store.get("../escape").is_err());
}
#[test]
fn crash_child() {
    let Some(path) = std::env::var_os("GLIDER_CRASH_TEST_PATH") else {
        return;
    };
    let mut db = Database::open(LocalStore::open(path).unwrap(), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    db.put(1, vec![5., 6.]).unwrap();
    db.delete(2).unwrap();
    // Exit without running Database/LocalStore destructors.
    std::process::exit(73);
}
#[test]
fn process_exit_and_restart() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("GLIDER_CRASH_TEST_PATH", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    // Simulate an additional write interrupted before publication.
    fs::write(root.join("mutation-00000000000000000005-body"), b"partial").unwrap();
    fs::write(root.join("mutation-00000000000000000005-seal"), b"VT").unwrap();
    let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), Some([5., 6.].as_slice()));
    assert_eq!(db.get(2), None);
    assert_eq!(db.search(&[5., 6.], 10).unwrap()[0].distance, 0.);
    db.put(3, vec![7., 8.]).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(3), Some([7., 8.].as_slice()));
}

#[test]
fn missing_or_short_seal_hides_only_an_undetectable_tail() {
    // External damage, not a supported crash outcome for acknowledged data.
    // All short seals are treated as unpublished, including arbitrary garbage.
    for length in std::iter::once(None).chain((0..8).map(Some)) {
        for internal in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("db");
            let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
            db.put(1, vec![1., 2.]).unwrap();
            db.delete(1).unwrap();
            if internal {
                db.put(2, vec![3., 4.]).unwrap();
            }
            drop(db);
            let seal = root.join("mutation-00000000000000000002-seal");
            match length {
                None => fs::remove_file(seal).unwrap(),
                Some(n) => fs::write(seal, vec![b'X'; n]).unwrap(),
            }
            let recovered = Database::open(LocalStore::open(&root).unwrap(), config());
            if internal {
                assert!(matches!(recovered, Err(glider::Error::Corrupt(_))));
            } else {
                let mut db = recovered.unwrap();
                // The lost tail delete resurrects the old value. Sequence 2 is
                // reusable because no later object witnesses the missing delete.
                assert_eq!(db.get(1), Some([1., 2.].as_slice()));
                db.put(2, vec![3., 4.]).unwrap();
                drop(db);
                let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
                assert_eq!(db.get(1), Some([1., 2.].as_slice()));
                assert_eq!(db.get(2), Some([3., 4.].as_slice()));
            }
        }
    }
}
