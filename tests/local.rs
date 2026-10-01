use glider::{
    store::{LocalStore, ObjectStore},
    Config, Database, Metric, Mutation,
};
use std::{fs, process::Command};
fn config() -> Config {
    Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    }
}
/// Handles sharing a directory, as a lease keeper and a database or two
/// processes during takeover do: each key is created exactly once, every
/// published object stays intact, and concurrent reopens reclaim no
/// in-progress publication.
#[test]
fn shared_directory_handles_create_each_key_exactly_once() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("shared");
    LocalStore::open(&root).unwrap();
    let writers = 4;
    let keys = 40;
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reopener = {
        let (root, done) = (root.clone(), done.clone());
        std::thread::spawn(move || {
            let mut reopens = 0;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                LocalStore::open(&root).unwrap();
                reopens += 1;
            }
            reopens
        })
    };
    let threads: Vec<_> = (0..writers)
        .map(|writer| {
            let root = root.clone();
            std::thread::spawn(move || {
                let mut store = LocalStore::open(&root).unwrap();
                let mut won = Vec::new();
                for key in 0..keys {
                    let payload = format!("writer {writer} key {key}").repeat(64);
                    match store.create(&format!("key-{key}"), payload.as_bytes()) {
                        Ok(()) => won.push(key),
                        Err(glider::Error::Exists(_)) => {}
                        Err(error) => panic!("writer {writer} key {key}: {error}"),
                    }
                }
                (writer, won)
            })
        })
        .collect();
    let mut winners = std::collections::BTreeMap::new();
    for thread in threads {
        let (writer, won) = thread.join().unwrap();
        for key in won {
            assert!(
                winners.insert(key, writer).is_none(),
                "key {key} created twice"
            );
        }
    }
    done.store(true, std::sync::atomic::Ordering::Release);
    assert!(reopener.join().unwrap() > 0);
    assert_eq!(winners.len(), keys);
    let store = LocalStore::open(&root).unwrap();
    assert_eq!(store.list().unwrap().len(), keys);
    for (key, writer) in winners {
        let payload = format!("writer {writer} key {key}").repeat(64);
        assert_eq!(
            store.get(&format!("key-{key}")).unwrap().unwrap(),
            payload.as_bytes()
        );
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
fn batch_crash_child() {
    let Some(path) = std::env::var_os("GLIDER_BATCH_CRASH_PATH") else {
        return;
    };
    let mut db = Database::open(LocalStore::open(path).unwrap(), config()).unwrap();
    db.apply_batch(vec![
        Mutation::Put {
            id: 1,
            vector: vec![1., 2.],
            metadata: Default::default(),
        },
        Mutation::Put {
            id: 2,
            vector: vec![3., 4.],
            metadata: Default::default(),
        },
        Mutation::Delete { id: 1 },
    ])
    .unwrap();
    std::process::exit(73);
}

#[test]
fn acknowledged_batch_survives_process_exit_without_destructors() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "batch_crash_child"])
        .env("GLIDER_BATCH_CRASH_PATH", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    db.put(3, vec![5., 6.]).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    assert_eq!(db.get(3), Some([5., 6.].as_slice()));
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

#[test]
fn segment_crash_child() {
    let Some(path) = std::env::var_os("GLIDER_SEGMENT_CRASH_PATH") else {
        return;
    };
    let mut db = Database::open(LocalStore::open(path).unwrap(), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.checkpoint().unwrap();
    db.delete(1).unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    std::process::exit(73);
}
#[test]
fn segment_and_tail_survive_process_exit_without_destructors() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "segment_crash_child"])
        .env("GLIDER_SEGMENT_CRASH_PATH", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    db.put(3, vec![5., 6.]).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(3), Some([5., 6.].as_slice()));
}

#[test]
fn compacted_crash_child() {
    let Some(root) = std::env::var_os("GLIDER_COMPACTION_CRASH_PATH") else {
        return;
    };
    let mut db = Database::open(LocalStore::open(root).unwrap(), config()).unwrap();
    db.put(1, vec![1., 2.]).unwrap();
    db.checkpoint().unwrap();
    db.put(2, vec![3., 4.]).unwrap();
    db.delete(1).unwrap();
    db.compact().unwrap();
    db.put(3, vec![5., 6.]).unwrap();
    std::process::exit(73);
}
#[test]
fn compacted_snapshot_and_tail_survive_process_exit() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("db");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "compacted_crash_child"])
        .env("GLIDER_COMPACTION_CRASH_PATH", &root)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let mut db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(2), Some([3., 4.].as_slice()));
    assert_eq!(db.get(3), Some([5., 6.].as_slice()));
    db.compact().unwrap();
    db.put(4, vec![7., 8.]).unwrap();
    drop(db);
    let db = Database::open(LocalStore::open(&root).unwrap(), config()).unwrap();
    assert_eq!(db.get(1), None);
    assert_eq!(db.get(4), Some([7., 8.].as_slice()));
}
