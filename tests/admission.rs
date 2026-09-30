use glider::{
    admission::{Error as AdmissionError, Limits, Service, Shutdown},
    ownership::claims,
    recovery::stage_isolated_namespace,
    retry::{Lookup, Request, RequestId},
    serving::{ServingOptions, SingleMachine},
    store::ObjectStore,
    Config, Error, Metric, Mutation,
};
use std::{
    collections::BTreeMap,
    sync::{mpsc, Arc, Mutex},
    time::Duration,
};
#[derive(Clone, Copy)]
enum Cut {
    Pass,
    Before,
    After,
    PanicBefore,
    PanicAfter,
}
struct Gate {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<Cut>,
}
#[derive(Default)]
struct State {
    objects: BTreeMap<String, Vec<u8>>,
    gate: Option<Gate>,
}
#[derive(Clone, Default)]
struct Store(Arc<Mutex<State>>);
impl Store {
    fn arm(&self) -> (mpsc::Receiver<()>, mpsc::SyncSender<Cut>) {
        let (signal, entered) = mpsc::sync_channel(1);
        let (release, wait) = mpsc::sync_channel(1);
        self.0.lock().unwrap().gate = Some(Gate {
            entered: signal,
            release: wait,
        });
        (entered, release)
    }
}
impl ObjectStore for Store {
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.0.lock().unwrap().objects.keys().cloned().collect())
    }
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().objects.get(key).cloned())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.0.lock().unwrap().objects.remove(key);
        Ok(())
    }
    fn create(&mut self, key: &str, bytes: &[u8]) -> glider::Result<()> {
        let gate = if key.starts_with("mutation-") {
            self.0.lock().unwrap().gate.take()
        } else {
            None
        };
        let cut = if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.release.recv_timeout(Duration::from_secs(5)).unwrap()
        } else {
            Cut::Pass
        };
        if matches!(cut, Cut::Pass | Cut::After | Cut::PanicAfter) {
            let mut state = self.0.lock().unwrap();
            if state.objects.contains_key(key) {
                return Err(Error::Exists(key.into()));
            }
            state.objects.insert(key.into(), bytes.to_vec());
        }
        match cut {
            Cut::Pass => Ok(()),
            Cut::Before | Cut::After => Err(std::io::Error::other("uncertain PUT").into()),
            _ => panic!("injected worker/backend panic"),
        }
    }
}
fn config() -> Config {
    Config {
        dimensions: 1,
        metric: Metric::SquaredEuclidean,
    }
}
fn open(store: Store) -> SingleMachine<Store> {
    SingleMachine::open(store, config(), ServingOptions::m8()).unwrap()
}
fn request(nonce: u8) -> Request {
    Request {
        id: RequestId {
            boundary: 0,
            nonce: [nonce; 16],
        },
        conditions: vec![],
        mutations: vec![Mutation::Put {
            id: u64::from(nonce),
            vector: vec![f32::from(nonce)],
            metadata: BTreeMap::new(),
        }],
    }
}
fn entered(gate: &mpsc::Receiver<()>) {
    gate.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn count_and_byte_limits_include_active_work_and_release_before_reply() {
    for byte_limit in [false, true] {
        let store = Store::default();
        let db = open(store.clone());
        let one = request(1);
        let two = request(2);
        let bytes =
            serde_json::to_vec(&one).unwrap().len() + serde_json::to_vec(&two).unwrap().len() - 1;
        let limits = if byte_limit {
            Limits {
                commands: 8,
                bytes,
                ..Limits::default()
            }
        } else {
            Limits {
                commands: 1,
                bytes: 1_000_000,
                ..Limits::default()
            }
        };
        let (gate, release) = store.arm();
        let service = Service::start(db, limits).unwrap();
        let client = service.client();
        let first = client.write(one.clone()).unwrap();
        entered(&gate);
        assert!(matches!(
            client.write(two.clone()),
            Err(AdmissionError::Overloaded)
        ));
        assert_eq!(client.status().commands, 1);
        assert_eq!(
            client.status().bytes,
            serde_json::to_vec(&one).unwrap().len()
        );
        assert!(!first.cancel()); // The backend has begun; cancellation cannot undo it.
        release.send(Cut::Pass).unwrap();
        assert_eq!(first.wait().unwrap().value.sequence, 1);
        assert_eq!(client.status().commands, 0);
        assert_eq!(client.write(two).unwrap().wait().unwrap().value.sequence, 2);
        service.shutdown(Shutdown::Drain).unwrap();
        assert!(claims(&store).unwrap().is_empty());
    }
}

#[test]
fn queued_cancellation_and_ticket_drop_prevent_publication() {
    let store = Store::default();
    let db = open(store.clone());
    let (gate, release) = store.arm();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let first = client.write(request(1)).unwrap();
    entered(&gate);
    let cancelled = client.write(request(2)).unwrap();
    assert!(cancelled.cancel());
    drop(client.write(request(3)).unwrap());
    let fourth = client.write(request(4)).unwrap();
    release.send(Cut::Pass).unwrap();
    first.wait().unwrap();
    assert!(matches!(cancelled.wait(), Err(AdmissionError::Cancelled)));
    assert_eq!(fourth.wait().unwrap().value.sequence, 2);
    service.shutdown(Shutdown::Drain).unwrap();
    let db = open(store);
    assert!(db.get(2).is_none() && db.get(3).is_none());
    assert!(db.get(1).is_some() && db.get(4).is_some());
    db.close().unwrap();
}

#[test]
fn fifo_reads_observe_one_committed_batch_and_concurrent_duplicates_share_outcome() {
    let store = Store::default();
    let db = open(store.clone());
    let (gate, release) = store.arm();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let first = client.write(request(1)).unwrap();
    entered(&gate);
    let mut batch = request(2);
    batch.mutations.push(Mutation::Delete { id: 1 });
    let second = client.write(batch.clone()).unwrap();
    let duplicate = client.clone().write(batch.clone()).unwrap();
    let query = client.query(vec![0.], 10, vec![]).unwrap();
    let lookup = client.lookup(batch.id).unwrap();
    release.send(Cut::Pass).unwrap();
    first.wait().unwrap();
    let outcome = second.wait().unwrap().value;
    assert_eq!(duplicate.wait().unwrap().value, outcome);
    let query = query.wait().unwrap().value;
    assert_eq!(query.sequence, outcome.sequence);
    assert_eq!(
        query.neighbors.iter().map(|n| n.id).collect::<Vec<_>>(),
        vec![2]
    );
    assert_eq!(lookup.wait().unwrap().value, Lookup::Retained(outcome));
    let observed = client.observe(2).unwrap().wait().unwrap().value;
    assert_eq!(observed.revision.boundary, 2);
    assert_eq!(observed.request_id.boundary, 2);
    service.shutdown(Shutdown::Drain).unwrap();
}

#[test]
fn shutdown_closes_admission_and_drains_or_cancels_queued_work() {
    for cancel in [false, true] {
        let store = Store::default();
        let db = open(store.clone());
        let (gate, release) = store.arm();
        let service = Service::start(db, Limits::default()).unwrap();
        let client = service.client();
        let active = client.write(request(1)).unwrap();
        entered(&gate);
        let queued = client.write(request(2)).unwrap();
        let mode = if cancel {
            Shutdown::CancelQueued
        } else {
            Shutdown::Drain
        };
        service.begin_shutdown(mode);
        let worker = std::thread::spawn(move || service.shutdown(mode));
        assert!(matches!(client.observe(9), Err(AdmissionError::Closed)));
        release.send(Cut::Pass).unwrap();
        active.wait().unwrap();
        let outcome = queued.wait();
        if cancel {
            assert!(matches!(outcome, Err(AdmissionError::Cancelled)));
        } else {
            assert_eq!(outcome.unwrap().value.sequence, 2);
        }
        worker.join().unwrap().unwrap();
        assert_eq!(client.status().commands, 0);
        assert_eq!(client.status().bytes, 0);
        assert!(claims(&store).unwrap().is_empty());
        let db = open(store);
        assert_eq!(db.get(2).is_some(), !cancel);
        db.close().unwrap();
    }
}

#[test]
fn worker_failure_stops_admission_and_all_outcomes_resolve_after_isolated_recovery() {
    for cut in [Cut::Before, Cut::After, Cut::PanicBefore, Cut::PanicAfter] {
        let store = Store::default();
        let db = open(store.clone());
        let (gate, release) = store.arm();
        let service = Service::start(db, Limits::default()).unwrap();
        let client = service.client();
        let first = client.write(request(1)).unwrap();
        entered(&gate);
        let queued = client.write(request(2)).unwrap();
        release.send(cut).unwrap();
        assert!(first.wait().is_err());
        assert!(matches!(queued.wait(), Err(AdmissionError::WorkerFailed)));
        assert!(service.shutdown(Shutdown::Drain).is_err());
        assert!(client.status().failed);
        assert_eq!(client.status().commands, 0);
        assert!(matches!(
            client.write(request(3)),
            Err(AdmissionError::WorkerFailed)
        ));
        assert_eq!(claims(&store).unwrap().len(), 1);
        let destination = Store::default();
        stage_isolated_namespace(&store, destination.clone(), config()).unwrap();
        let mut db = open(destination);
        assert_eq!(
            matches!(
                db.lookup_request(request(1).id).unwrap(),
                Lookup::Retained(_)
            ),
            matches!(cut, Cut::After | Cut::PanicAfter)
        );
        assert_eq!(db.lookup_request(request(2).id).unwrap(), Lookup::Unknown);
        assert_eq!(db.apply_request(request(1)).unwrap().sequence, 1);
        assert_eq!(db.apply_request(request(2)).unwrap().sequence, 2);
        db.close().unwrap();
    }
}

#[test]
fn dropping_service_stops_admission_but_does_not_cancel_active_publication() {
    let store = Store::default();
    let db = open(store.clone());
    let (gate, release) = store.arm();
    let service = Service::start(db, Limits::default()).unwrap();
    let client = service.client();
    let active = client.write(request(1)).unwrap();
    entered(&gate);
    let queued = client.write(request(2)).unwrap();
    drop(service);
    assert!(matches!(client.observe(3), Err(AdmissionError::Closed)));
    release.send(Cut::Pass).unwrap();
    assert_eq!(active.wait().unwrap().value.sequence, 1);
    assert!(matches!(queued.wait(), Err(AdmissionError::Cancelled)));
}

#[test]
fn read_priority_runs_queued_queries_before_unaged_writes() {
    for (priority, expected) in [(None, 2), (Some(Duration::from_secs(60)), 1)] {
        let store = Store::default();
        let service = Service::start(
            open(store.clone()),
            Limits {
                read_priority: priority,
                ..Limits::default()
            },
        )
        .unwrap();
        let client = service.client();
        let (gate, release) = store.arm();
        let first = client.write(request(1)).unwrap();
        entered(&gate);
        // The worker is inside the first PUT; queue a write, then a query.
        let second = client.write(request(2)).unwrap();
        let query = client.query(vec![0.], 1, vec![]).unwrap();
        release.send(Cut::Pass).unwrap();
        assert_eq!(first.wait().unwrap().value.sequence, 1);
        assert_eq!(query.wait().unwrap().value.sequence, expected);
        assert_eq!(second.wait().unwrap().value.sequence, 2);
        service.shutdown(Shutdown::Drain).unwrap();
    }
}
