//! Reproducible, object-store-local admission workload for M31 latency investigation.
//! Usage: cargo run --offline --release --example m31_latency_probe --
//! ROWS SECONDS [THREADS [grouped|legacy [QUERIES]]]
use glider::{
    admission::{Engine, Limits, Service, Shutdown, Snapshot},
    retry::{Lookup, Outcome, Request, RequestId, Revision},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::ObjectStore,
    Config, Error, Metric, Mutation, Neighbor, Result,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier, Mutex,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Default)]
struct MemoryStore(Arc<Mutex<BTreeMap<String, Vec<u8>>>>, Arc<AtomicU64>);

impl ObjectStore for MemoryStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.0.lock().unwrap().keys().cloned().collect())
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut objects = self.0.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), value.to_vec());
        if key.starts_with("sglog-") {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

/// The old acceptance wrapper inherited Engine's per-request group fallback.
/// `legacy` reproduces that behavior; the other mode forwards group commit.
struct Wrapped {
    inner: SegmentedServing<MemoryStore>,
    legacy: bool,
}

impl Engine for Wrapped {
    fn config(&self) -> Config {
        self.inner.config()
    }
    fn sequence(&self) -> u64 {
        self.inner.sequence()
    }
    fn recovery_required(&self) -> bool {
        self.inner.recovery_required()
    }
    fn maintenance_time(&self) -> Duration {
        self.inner.maintenance_time()
    }
    fn apply_request(&mut self, request: Request) -> Result<Outcome> {
        self.inner.apply_request(request)
    }
    fn apply_requests(&mut self, requests: Vec<Request>) -> Vec<Result<Outcome>> {
        if self.legacy {
            requests
                .into_iter()
                .map(|request| self.inner.apply_request(request))
                .collect()
        } else {
            self.inner.apply_requests(requests)
        }
    }
    fn revision(&self, id: u64) -> Revision {
        self.inner.revision(id)
    }
    fn request_id(&self) -> Result<RequestId> {
        self.inner.request_id()
    }
    fn lookup_request(&self, id: RequestId) -> Result<Lookup> {
        self.inner.lookup_request(id)
    }
    fn get(&self, id: u64) -> Result<Option<glider::streaming::OwnedDocument>> {
        self.inner.get(id)
    }
    fn query(&mut self, query: &[f32], k: usize, filter: &[(&str, &str)]) -> Result<Vec<Neighbor>> {
        self.inner.query(query, k, filter)
    }
    fn idle_step(&mut self) -> Result<bool> {
        self.inner.idle_step()
    }
    fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
        self.inner.snapshot()
    }
    fn close(self) -> Result<()> {
        self.inner.close()
    }
}

fn vector(id: u64, generation: u64) -> Vec<f32> {
    let mut state = id.wrapping_add(generation.wrapping_mul(137)) ^ 0x9e37_79b9_7f4a_7c15;
    (0..128)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 255) as f32
        })
        .collect()
}

fn request(boundary: u64, nonce: u128, start: u64, generation: u64) -> Request {
    let mutations = (start..start + 100)
        .map(|id| {
            let mut metadata = BTreeMap::new();
            if id % 100 == 0 {
                metadata.insert("cohort".to_owned(), "one-percent".to_owned());
            }
            Mutation::Put {
                id,
                vector: vector(id, generation),
                metadata,
            }
        })
        .collect();
    Request {
        id: RequestId {
            boundary,
            nonce: nonce.to_le_bytes(),
        },
        conditions: Vec::new(),
        mutations,
    }
}

fn percentile(values: &[Duration], p: usize) -> f64 {
    let mut times: Vec<_> = values.iter().map(|d| d.as_secs_f64() * 1000.).collect();
    times.sort_by(f64::total_cmp);
    times[(times.len() - 1) * p / 100]
}

fn wait_until(at: Instant) {
    let now = Instant::now();
    if now < at {
        std::thread::sleep(at - now);
    }
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let rows: u64 = args.next().ok_or("ROWS required")?.parse()?;
    let seconds: u64 = args.next().ok_or("SECONDS required")?.parse()?;
    let threads: usize = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(8);
    let mode = args.next().unwrap_or_else(|| "grouped".into());
    assert!(mode == "grouped" || mode == "legacy");
    let queries: usize = args
        .next()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(Limits::default().queries);
    assert!(rows >= 100 && rows.is_multiple_of(100) && seconds > 0);
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let segmented = SegmentedOptions {
        resident_filter: Some(("cohort".into(), "one-percent".into())),
        routed_keys: Vec::new(),
    };
    let mut options = SegmentedServingOptions::m31("unused-cache".into());
    options.cache = None;
    options.query_threads = threads;
    let store = MemoryStore::default();
    let log_count = store.1.clone();
    let mut db = SegmentedServing::open(store, config, segmented, options)?;
    let load_start = Instant::now();
    for batch in 0..rows / 100 {
        db.apply_request(request(db.sequence(), batch as u128, batch * 100, 0))
            .map_err(|error| format!("load write batch {batch}: {error}"))?;
        if db.database().tail_objects() >= 32 {
            while db.maintenance_step().map_err(|error| {
                format!(
                    "load maintenance batch {batch} unit {}: {error}",
                    db.last_unit()
                )
            })? {}
        }
    }
    while db.maintenance_step()? {}
    println!(
        "loaded {rows} rows in {:.2} s",
        load_start.elapsed().as_secs_f64()
    );
    let logs_before = log_count.load(Ordering::Relaxed);
    let initial_sequence = db.sequence();
    let service = Service::start(
        Wrapped {
            inner: db,
            legacy: mode == "legacy",
        },
        Limits {
            read_priority: Some(Duration::from_millis(50)),
            queries,
            ..Limits::default()
        },
    )?;
    let client = service.client();
    let barrier = Arc::new(Barrier::new(9));
    let began = Instant::now() + Duration::from_millis(100);
    let mut workers = Vec::new();
    for writer in 0..4_u64 {
        let (client, barrier) = (client.clone(), barrier.clone());
        workers.push(std::thread::spawn(move || {
            let mut data = Vec::new();
            barrier.wait();
            for round in 0..seconds {
                wait_until(began + Duration::from_secs(round));
                let start = ((writer * seconds + round) * 7919 % (rows / 100)) * 100;
                let submitted = Instant::now();
                let result = client
                    .write(request(
                        initial_sequence,
                        (1_000_000 + writer * seconds + round) as u128,
                        start,
                        round + 1,
                    ))
                    .unwrap()
                    .wait()
                    .unwrap();
                data.push((submitted.elapsed(), result.queue_wait, result.execution));
            }
            data
        }));
    }
    let mut readers = Vec::new();
    for reader in 0..4_u64 {
        let (client, barrier) = (client.clone(), barrier.clone());
        readers.push(std::thread::spawn(move || {
            let mut data = Vec::new();
            barrier.wait();
            for tick in 0..seconds * 10 {
                wait_until(began + Duration::from_millis(tick * 100));
                let filtered = tick % 2 == 0;
                let filter = if filtered {
                    vec![("cohort".into(), "one-percent".into())]
                } else {
                    Vec::new()
                };
                let submitted = Instant::now();
                let result = client
                    .query(vector(reader * 10_000 + tick, 0), 10, filter)
                    .unwrap()
                    .wait()
                    .unwrap();
                data.push((
                    filtered,
                    submitted.elapsed(),
                    result.queue_wait,
                    result.execution,
                ));
            }
            data
        }));
    }
    barrier.wait();
    let writes: Vec<_> = workers
        .into_iter()
        .flat_map(|w| w.join().unwrap())
        .collect();
    let reads: Vec<_> = readers
        .into_iter()
        .flat_map(|r| r.join().unwrap())
        .collect();
    let write_exec: Vec<_> = writes.iter().map(|w| w.2).collect();
    let write_queue: Vec<_> = writes.iter().map(|w| w.1).collect();
    let query_exec: Vec<_> = reads.iter().filter(|r| !r.0).map(|r| r.3).collect();
    let query_queue: Vec<_> = reads.iter().map(|r| r.2).collect();
    let warm: Vec<_> = reads.iter().filter(|r| !r.0).map(|r| r.1).collect();
    let idle = client.idle_maintenance_samples();
    println!(
        "write execution p50/p95 {:.2}/{:.2} ms, queue p95 {:.2} ms",
        percentile(&write_exec, 50),
        percentile(&write_exec, 95),
        percentile(&write_queue, 95)
    );
    println!(
        "write requests {}, log objects {}",
        writes.len(),
        log_count.load(Ordering::Relaxed) - logs_before
    );
    println!("unfiltered query execution p50/p95 {:.2}/{:.2} ms, queue p95 {:.2} ms, end-to-end p95 {:.2} ms", percentile(&query_exec, 50), percentile(&query_exec, 95), percentile(&query_queue, 95), percentile(&warm, 95));
    if !idle.is_empty() {
        println!(
            "idle maintenance units {}, p95 {:.2} ms",
            idle.len(),
            percentile(&idle, 95)
        );
    }
    service.shutdown(Shutdown::Drain)?;
    Ok(())
}
