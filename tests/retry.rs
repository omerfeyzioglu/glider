use glider::{
    ownership::OwnedDatabase,
    recovery::stage_isolated_namespace,
    retry::{Conflict, Lookup, Request, RequestId, RETENTION_COMMITS},
    serving::{ServingOptions, SingleMachine},
    store::{LocalStore, ObjectStore},
    Config, Database, Error, Metric, Mutation,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    objects: BTreeMap<String, Vec<u8>>,
    fault: Option<(String, bool, bool)>, // prefix, publish, panic
}
#[derive(Clone, Default)]
struct Memory(Arc<Mutex<State>>);
impl ObjectStore for Memory {
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.0.lock().unwrap().objects.keys().cloned().collect())
    }
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().objects.get(key).cloned())
    }
    fn create(&self, key: &str, bytes: &[u8]) -> glider::Result<()> {
        let mut s = self.0.lock().unwrap();
        let fault = if s.fault.as_ref().is_some_and(|f| key.starts_with(&f.0)) {
            s.fault.take()
        } else {
            None
        };
        if fault.as_ref().is_none_or(|f| f.1) {
            if s.objects.contains_key(key) {
                return Err(Error::Exists(key.into()));
            }
            s.objects.insert(key.into(), bytes.to_vec());
        }
        drop(s);
        if let Some((_, _, panic)) = fault {
            assert!(!panic, "injected publication panic");
            return Err(std::io::Error::other("injected acknowledgement loss").into());
        }
        Ok(())
    }
    fn remove(&self, key: &str) -> glider::Result<()> {
        let mut s = self.0.lock().unwrap();
        s.objects.remove(key);
        if s.fault.as_ref().is_some_and(|f| f.0 == "remove") {
            s.fault = None;
            return Err(std::io::Error::other("lost remove acknowledgement").into());
        }
        Ok(())
    }
}
fn config() -> Config {
    Config {
        dimensions: 1,
        metric: Metric::SquaredEuclidean,
    }
}
fn put(id: u64, value: f32) -> Mutation {
    Mutation::Put {
        id,
        vector: vec![value],
        metadata: BTreeMap::new(),
    }
}
fn request<S: ObjectStore>(db: &Database<S>, nonce: u8, mutations: Vec<Mutation>) -> Request {
    Request {
        id: RequestId {
            boundary: db.maintenance_status().sequence,
            nonce: [nonce; 16],
        },
        conditions: vec![],
        mutations,
    }
}

#[test]
fn retained_retries_do_not_overwrite_newer_state_and_payload_reuse_conflicts() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    let r = request(&db, 1, vec![put(1, 1.)]);
    let outcome = db.apply_request(r.clone()).unwrap();
    db.put(1, vec![9.]).unwrap();
    let before = store.list().unwrap();
    assert_eq!(db.apply_request(r.clone()).unwrap(), outcome);
    assert_eq!(store.list().unwrap(), before);
    assert_eq!(db.get(1), Some([9.].as_slice()));
    let mut changed = r.clone();
    changed.mutations = vec![put(1, 2.)];
    assert!(matches!(
        db.apply_request(changed),
        Err(Error::RequestConflict)
    ));
    let mut changed = r.clone();
    changed.conditions = vec![glider::retry::Revision { id: 1, boundary: 0 }];
    assert!(matches!(
        db.apply_request(changed),
        Err(Error::RequestConflict)
    ));
    assert_eq!(db.lookup_request(r.id).unwrap(), Lookup::Retained(outcome));
}

#[test]
fn conditional_conflicts_are_atomic_durable_and_survive_delete_reinsert() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    let absent = db.revision(1);
    db.put(2, vec![2.]).unwrap(); // unrelated writes do not conflict
    let mut r = request(&db, 1, vec![put(1, 1.)]);
    r.conditions = vec![absent];
    assert!(db.apply_request(r).unwrap().conflict.is_none());
    let old = db.revision(1);
    db.delete(1).unwrap();
    db.put(1, vec![1.]).unwrap();
    let mut r = request(&db, 2, vec![put(1, 8.), put(2, 8.)]);
    r.conditions = vec![old];
    let outcome = db.apply_request(r.clone()).unwrap();
    assert_eq!(outcome.conflict, Some(Conflict::StaleRevision));
    assert_eq!(db.get(1), Some([1.].as_slice()));
    assert_eq!(db.get(2), Some([2.].as_slice()));
    db.compact().unwrap();
    drop(db);
    let mut db = Database::open(store, config()).unwrap();
    assert_eq!(db.apply_request(r).unwrap(), outcome);
    let absent = db.revision(3);
    db.put(3, vec![3.]).unwrap();
    db.delete(3).unwrap();
    let mut r = request(&db, 3, vec![put(3, 4.)]);
    r.conditions = vec![absent];
    assert_eq!(
        db.apply_request(r).unwrap().conflict,
        Some(Conflict::StaleRevision)
    );
}

#[test]
fn uncertain_publications_never_separate_mutations_from_retry_metadata() {
    for publish in [false, true] {
        for panic in [false, true] {
            let old = Memory::default();
            let new = Memory::default();
            let mut db = OwnedDatabase::open(old.clone(), config()).unwrap();
            let r = request(&db, 1, vec![put(1, 1.), put(2, 2.)]);
            old.0.lock().unwrap().fault = Some(("mutation-".into(), publish, panic));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                db.apply_request(r.clone())
            }));
            assert!(result.is_err() || result.unwrap().is_err());
            assert!(db.get(1).is_none());
            assert!(matches!(
                db.lookup_request(r.id),
                Err(Error::RecoveryRequired)
            ));
            assert!(matches!(
                db.apply_request(r.clone()),
                Err(Error::RecoveryRequired)
            ));
            drop(db);
            stage_isolated_namespace(&old, new.clone(), config()).unwrap();
            let mut db = OwnedDatabase::open(new, config()).unwrap();
            assert_eq!(
                matches!(db.lookup_request(r.id).unwrap(), Lookup::Retained(_)),
                publish
            );
            assert_eq!(db.get(1).is_some(), publish);
            let outcome = db.apply_request(r.clone()).unwrap();
            assert_eq!(outcome.sequence, 1);
            assert_eq!(db.apply_request(r).unwrap(), outcome);
            assert_eq!(db.get(2), Some([2.].as_slice()));
            db.close().unwrap();
        }
    }
}

#[test]
fn snapshots_cleanup_and_streaming_preserve_the_same_decision_boundary() {
    for chunked in [false, true] {
        for phase in ["none", "compactedchunk-", "compacted-", "remove"] {
            if !chunked && phase == "compactedchunk-" {
                continue;
            }
            let store = Memory::default();
            let mut db = Database::open(store.clone(), config()).unwrap();
            let r = request(&db, 1, vec![put(1, 1.)]);
            let outcome = db.apply_request(r.clone()).unwrap();
            let revision = db.revision(1);
            db.checkpoint().unwrap();
            db.put(2, vec![2.]).unwrap();
            if phase != "none" {
                store.0.lock().unwrap().fault = Some((phase.into(), true, false));
            }
            let result = if chunked {
                db.compact_chunked(200)
            } else {
                db.compact()
            };
            assert_eq!(result.is_ok(), phase == "none");
            drop(db);
            let mut db = Database::open(store.clone(), config()).unwrap();
            assert_eq!(db.apply_request(r.clone()).unwrap(), outcome);
            if chunked {
                db.compact_chunked(200).unwrap();
            } else {
                db.compact().unwrap();
            }
            let mut next = request(&db, 2, vec![put(1, 5.)]);
            next.conditions = vec![revision];
            assert!(db.apply_request(next).unwrap().conflict.is_none());
            if chunked {
                drop(db);
                let frozen = glider::streaming::StreamingDatabase::open(store, config()).unwrap();
                assert_eq!(
                    frozen.get_with_metadata(1).unwrap().unwrap().vector,
                    vec![5.]
                );
            }
        }
    }
}

#[test]
fn retention_and_revision_expiration_are_bounded_and_explicit() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    let old = db.revision(999);
    let first = request(&db, 1, vec![put(1, 1.)]);
    let outcome = db.apply_request(first.clone()).unwrap();
    for i in 1..RETENTION_COMMITS {
        let r = request(&db, 1, vec![put(i + 1, i as f32)]);
        db.apply_request(r).unwrap();
    }
    assert_eq!(
        db.lookup_request(first.id).unwrap(),
        Lookup::Retained(outcome)
    );
    let unknown = RequestId {
        nonce: [2; 16],
        ..first.id
    };
    assert_eq!(db.lookup_request(unknown).unwrap(), Lookup::Expired);
    db.put(999, vec![1.]).unwrap();
    assert_eq!(db.lookup_request(first.id).unwrap(), Lookup::Expired);
    assert!(matches!(
        db.apply_request(first),
        Err(Error::RequestExpired)
    ));
    let mut r = request(&db, 3, vec![put(999, 9.)]);
    r.conditions = vec![old];
    assert_eq!(
        db.apply_request(r).unwrap().conflict,
        Some(Conflict::ExpiredRevision)
    );
    db.compact().unwrap();
    drop(db);
    let db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(db.lookup_request(unknown).unwrap(), Lookup::Expired);
    let key = store
        .list()
        .unwrap()
        .into_iter()
        .find(|k| k.starts_with("compacted-"))
        .unwrap();
    let state: serde_json::Value =
        serde_json::from_slice(&store.get(&key).unwrap().unwrap()).unwrap();
    assert!(state["retry"]["receipts"].as_array().unwrap().len() <= 128);
    assert!(state["retry"]["changed"].as_array().unwrap().len() <= 12800);
}

#[test]
fn legacy_large_batch_expires_observations_without_unbounded_tombstones() {
    let mut db = Database::open(Memory::default(), config()).unwrap();
    let observed = db.revision(1);
    db.apply_batch((0..12801).map(|id| Mutation::Delete { id }).collect())
        .unwrap();
    assert_eq!(db.revision_floor(), 1);
    let mut r = request(&db, 1, vec![put(1, 1.)]);
    r.conditions = vec![observed];
    assert_eq!(
        db.apply_request(r).unwrap().conflict,
        Some(Conflict::ExpiredRevision)
    );
}

#[test]
fn concurrent_duplicates_share_one_durable_outcome() {
    let db = Database::open(Memory::default(), config()).unwrap();
    let r = request(&db, 1, vec![put(1, 1.)]);
    let shared = Arc::new(Mutex::new(db));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (db, r, barrier) = (shared.clone(), r.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                db.lock().unwrap().apply_request(r).unwrap()
            })
        })
        .collect();
    for thread in threads {
        assert_eq!(thread.join().unwrap().sequence, 1);
    }
    assert_eq!(shared.lock().unwrap().maintenance_status().sequence, 1);
}

#[test]
fn backup_restore_knows_only_its_selected_boundary_and_retained_receipts() {
    let source = Memory::default();
    let backup = Memory::default();
    let restored = Memory::default();
    let mut db = SingleMachine::open(source, config(), ServingOptions::m8()).unwrap();
    let r = Request {
        id: db.request_id().unwrap(),
        conditions: vec![],
        mutations: vec![put(1, 1.)],
    };
    let outcome = db.apply_request(r.clone()).unwrap();
    db.backup_to(backup.clone()).unwrap();
    let later = Request {
        id: db.request_id().unwrap(),
        conditions: vec![],
        mutations: vec![put(2, 2.)],
    };
    db.apply_request(later.clone()).unwrap();
    let ahead = db.request_id().unwrap();
    stage_isolated_namespace(&backup, restored.clone(), config()).unwrap();
    let mut copy = SingleMachine::open(restored, config(), ServingOptions::m8()).unwrap();
    assert_eq!(copy.apply_request(r).unwrap(), outcome);
    assert_eq!(copy.lookup_request(later.id).unwrap(), Lookup::Unknown);
    assert_eq!(copy.lookup_request(ahead).unwrap(), Lookup::Ahead);
    assert!(copy.get(2).is_none());
    copy.close().unwrap();
    db.close().unwrap();
}

#[test]
fn malformed_retry_metadata_fails_recovery_without_snapshot_fallback() {
    for field in ["missing", "digest", "duplicate", "floor"] {
        let store = Memory::default();
        let mut db = Database::open(store.clone(), config()).unwrap();
        let r = request(&db, 1, vec![put(1, 1.)]);
        db.apply_request(r).unwrap();
        db.compact().unwrap();
        drop(db);
        let key = "compacted-00000000000000000001";
        let mut value: serde_json::Value =
            serde_json::from_slice(&store.get(key).unwrap().unwrap()).unwrap();
        match field {
            "missing" => {
                value.as_object_mut().unwrap().remove("retry");
            }
            "digest" => value["retry"]["receipts"][0]["digest"] = "bad".into(),
            "duplicate" => {
                let r = value["retry"]["receipts"][0].clone();
                value["retry"]["receipts"].as_array_mut().unwrap().push(r);
            }
            _ => value["retry"]["revision_floor"] = 2.into(),
        }
        store
            .0
            .lock()
            .unwrap()
            .objects
            .insert(key.into(), serde_json::to_vec(&value).unwrap());
        assert!(
            matches!(Database::open(store, config()), Err(Error::Corrupt(_))),
            "{field}"
        );
    }
}

struct ExitStore {
    inner: LocalStore,
    publish: bool,
}
impl ObjectStore for ExitStore {
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }
    fn remove(&self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)
    }
    fn create(&self, key: &str, bytes: &[u8]) -> glider::Result<()> {
        if key.starts_with("mutation-") {
            if self.publish {
                self.inner.create(key, bytes)?;
            }
            std::process::exit(73);
        }
        self.inner.create(key, bytes)
    }
}
#[test]
#[ignore = "child process only"]
fn retry_crash_child() {
    let path = std::env::var("GLIDER_RETRY_CRASH_PATH").unwrap();
    let mut db = OwnedDatabase::open(
        ExitStore {
            inner: LocalStore::open(path).unwrap(),
            publish: std::env::var("GLIDER_RETRY_PUBLISH").unwrap() == "true",
        },
        config(),
    )
    .unwrap();
    let r = request(&db, 1, vec![put(1, 1.), put(2, 2.)]);
    db.apply_request(r).unwrap();
    panic!("crash injection did not run");
}
#[test]
fn process_exit_before_or_after_publication_recovers_one_atomic_retry_decision() {
    for publish in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "retry_crash_child"])
            .env("GLIDER_RETRY_CRASH_PATH", &source)
            .env("GLIDER_RETRY_PUBLISH", publish.to_string())
            .output()
            .unwrap();
        assert_eq!(child.status.code(), Some(73));
        let dest = temp.path().join("dest");
        stage_isolated_namespace(
            &LocalStore::open(source).unwrap(),
            LocalStore::open(&dest).unwrap(),
            config(),
        )
        .unwrap();
        let mut db = OwnedDatabase::open(LocalStore::open(dest).unwrap(), config()).unwrap();
        let r = Request {
            id: RequestId {
                boundary: 0,
                nonce: [1; 16],
            },
            conditions: vec![],
            mutations: vec![put(1, 1.), put(2, 2.)],
        };
        assert_eq!(db.get(1).is_some(), publish);
        assert_eq!(
            matches!(db.lookup_request(r.id).unwrap(), Lookup::Retained(_)),
            publish
        );
        assert_eq!(db.apply_request(r).unwrap().sequence, 1);
        db.close().unwrap();
    }
}

#[test]
fn invalid_requests_are_rejected_before_io_and_missing_decisions_are_corruption() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    let base = request(&db, 1, vec![put(1, 1.)]);
    let mut cases = Vec::new();
    let mut r = base.clone();
    r.mutations.clear();
    cases.push(r);
    let mut r = base.clone();
    r.mutations = (0..101).map(|id| put(id, 1.)).collect();
    cases.push(r);
    let mut r = base.clone();
    r.mutations = vec![put(1, f32::NAN)];
    cases.push(r);
    let mut r = base.clone();
    r.id.boundary = 1;
    cases.push(r);
    let mut r = base.clone();
    r.conditions = vec![db.revision(1), db.revision(1)];
    cases.push(r);
    let mut r = base.clone();
    r.conditions = vec![glider::retry::Revision { id: 1, boundary: 1 }];
    cases.push(r);
    let mut r = base.clone();
    r.mutations = vec![Mutation::Put {
        id: 1,
        vector: vec![1.],
        metadata: BTreeMap::from([("large".into(), "x".repeat(glider::retry::MAX_REQUEST_BYTES))]),
    }];
    cases.push(r);
    let before = store.list().unwrap();
    for r in cases {
        assert!(matches!(db.apply_request(r), Err(Error::Invalid(_))));
    }
    assert_eq!(store.list().unwrap(), before);
    db.apply_request(base).unwrap();
    drop(db);
    let key = "mutation-00000000000000000001";
    let original: serde_json::Value =
        serde_json::from_slice(&store.get(key).unwrap().unwrap()).unwrap();
    for kind in ["request", "outcome", "conflict", "false-conflict"] {
        let mut value = original.clone();
        match kind {
            "conflict" => {
                value["outcome"].as_object_mut().unwrap().remove("conflict");
            }
            "false-conflict" => value["outcome"]["conflict"] = "stale_revision".into(),
            _ => {
                value.as_object_mut().unwrap().remove(kind);
            }
        }
        store
            .0
            .lock()
            .unwrap()
            .objects
            .insert(key.into(), serde_json::to_vec(&value).unwrap());
        assert!(
            matches!(
                Database::open(store.clone(), config()),
                Err(Error::Corrupt(_))
            ),
            "{kind}"
        );
    }
}

#[test]
fn lost_conflict_acknowledgement_and_full_retention_window_remain_resolvable() {
    let store = Memory::default();
    let mut db = Database::open(store.clone(), config()).unwrap();
    let old = db.revision(1);
    db.delete(1).unwrap();
    let mut r = request(&db, 1, vec![put(1, 1.)]);
    r.conditions = vec![old];
    store.0.lock().unwrap().fault = Some(("mutation-".into(), true, false));
    assert!(db.apply_request(r.clone()).is_err());
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    assert_eq!(
        db.apply_request(r).unwrap().conflict,
        Some(Conflict::StaleRevision)
    );
    for batch in 0..128 {
        let r = request(
            &db,
            2,
            (0..100)
                .map(|n| Mutation::Delete {
                    id: 100 + batch * 100 + n,
                })
                .collect(),
        );
        db.apply_request(r).unwrap();
    }
    db.compact_chunked(200).unwrap();
    drop(db);
    let mut db = Database::open(store.clone(), config()).unwrap();
    let key = format!("compacted-{:020}", db.maintenance_status().sequence);
    let value: serde_json::Value =
        serde_json::from_slice(&store.get(&key).unwrap().unwrap()).unwrap();
    assert_eq!(value["retry"]["receipts"].as_array().unwrap().len(), 128);
    assert_eq!(value["retry"]["changed"].as_array().unwrap().len(), 12800);
    assert!(serde_json::to_vec(&value["retry"]).unwrap().len() < 1024 * 1024);
    // Pruning happens before the next batch and keeps recent observations valid.
    let observed = db.revision(99999);
    let mut next = request(&db, 3, vec![put(99999, 1.)]);
    next.conditions = vec![observed];
    assert!(db.apply_request(next).unwrap().conflict.is_none());
}
