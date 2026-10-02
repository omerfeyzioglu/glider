//! Reproducible, object-store-local admission workload for M31 latency investigation.
//! Usage: cargo run --offline --release --example m31_latency_probe --
//! ROWS SECONDS [THREADS [grouped|legacy [QUERIES]]]
//! Set M31_PROBE_SNAPSHOT=1 for a held-view seal, M31_PROBE_REOPEN=1 for a
//! fresh open after load, M31_PROBE_LOCAL=1 for a temporary LocalStore, or M31_PROBE_CACHE=1 and
//! M31_PROBE_WARM_STEP=1 for one warm-up unit. The optional memory-probe
//! feature reports live and peak requested Rust heap bytes; leave it off for
//! latency measurements.
use glider::{
    admission::{Engine, Limits, Service, Shutdown, Snapshot},
    retry::{Lookup, Outcome, Request, RequestId, Revision},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::{LocalStore, ObjectStore},
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

#[cfg(feature = "memory-probe")]
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::AtomicUsize,
};

#[cfg(feature = "memory-probe")]
struct MeteredAllocator;

#[cfg(feature = "memory-probe")]
static HEAP_BYTES: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "memory-probe")]
static PEAK_HEAP_BYTES: AtomicUsize = AtomicUsize::new(0);
#[cfg(feature = "memory-probe")]
static TOTAL_HEAP_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "memory-probe")]
#[global_allocator]
static ALLOCATOR: MeteredAllocator = MeteredAllocator;

#[cfg(feature = "memory-probe")]
unsafe impl GlobalAlloc for MeteredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            TOTAL_HEAP_ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
            let current = HEAP_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK_HEAP_BYTES.fetch_max(current, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HEAP_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(ptr, layout, new_size) };
        if !replacement.is_null() {
            if new_size >= layout.size() {
                let added = new_size - layout.size();
                TOTAL_HEAP_ALLOCATED.fetch_add(added, Ordering::Relaxed);
                let current = HEAP_BYTES.fetch_add(added, Ordering::Relaxed) + added;
                PEAK_HEAP_BYTES.fetch_max(current, Ordering::Relaxed);
            } else {
                HEAP_BYTES.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
            }
        }
        replacement
    }
}

#[cfg(feature = "memory-probe")]
fn heap_mib() -> f64 {
    HEAP_BYTES.load(Ordering::Relaxed) as f64 / 1048576.
}

#[cfg(feature = "memory-probe")]
fn peak_heap_mib() -> f64 {
    PEAK_HEAP_BYTES.load(Ordering::Relaxed) as f64 / 1048576.
}

#[cfg(feature = "memory-probe")]
fn reset_heap_peak() {
    PEAK_HEAP_BYTES.store(HEAP_BYTES.load(Ordering::Relaxed), Ordering::Relaxed);
}

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

#[derive(Clone)]
enum ProbeStore {
    Memory(MemoryStore),
    Local(Arc<LocalStore>, Arc<AtomicU64>),
}

impl ProbeStore {
    fn log_count(&self) -> Arc<AtomicU64> {
        match self {
            Self::Memory(store) => store.1.clone(),
            Self::Local(_, count) => count.clone(),
        }
    }
}

impl ObjectStore for ProbeStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Memory(store) => store.get(key),
            Self::Local(store, _) => store.get(key),
        }
    }

    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        expected_payload_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Memory(store) => store.get_range(key, offset, length, expected_payload_len),
            Self::Local(store, _) => store.get_range(key, offset, length, expected_payload_len),
        }
    }

    fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Memory(store) => store.get_many(keys),
            Self::Local(store, _) => store.get_many(keys),
        }
    }

    fn get_ranges(&self, ranges: &[(&str, usize, usize, usize)]) -> Result<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Memory(store) => store.get_ranges(ranges),
            Self::Local(store, _) => store.get_ranges(ranges),
        }
    }

    fn list(&self) -> Result<Vec<String>> {
        match self {
            Self::Memory(store) => store.list(),
            Self::Local(store, _) => store.list(),
        }
    }

    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        match self {
            Self::Memory(store) => store.create(key, value),
            Self::Local(store, count) => {
                store.create(key, value)?;
                if key.starts_with("sglog-") {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
        }
    }

    fn remove(&self, key: &str) -> Result<()> {
        match self {
            Self::Memory(store) => store.remove(key),
            Self::Local(store, _) => store.remove(key),
        }
    }
}

/// The old acceptance wrapper inherited Engine's per-request group fallback.
/// `legacy` reproduces that behavior; the other mode forwards group commit.
struct Wrapped {
    inner: SegmentedServing<ProbeStore>,
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

#[cfg(target_os = "macos")]
fn rss_mib() -> f64 {
    let mut info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::zeroed();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    #[allow(deprecated)]
    let status = unsafe {
        libc::task_info(
            libc::mach_task_self(),
            libc::MACH_TASK_BASIC_INFO,
            info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    assert_eq!(status, 0);
    unsafe { info.assume_init() }.resident_size as f64 / 1048576.
}

#[cfg(not(target_os = "macos"))]
fn rss_mib() -> f64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("proc status");
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .expect("VmRSS")
        .split_whitespace()
        .next()
        .expect("RSS KiB")
        .parse()
        .expect("RSS integer");
    kib as f64 / 1024.
}

fn peak_rss_mib() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let peak = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        peak as f64 / 1048576.
    } else {
        peak as f64 / 1024.
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
    let warm_step = std::env::var_os("M31_PROBE_WARM_STEP").is_some();
    if warm_step {
        options.warm_unit_bytes = 0;
    }
    let cache_path = if std::env::var_os("M31_PROBE_CACHE").is_some() {
        let path = std::env::temp_dir().join(format!("glider-m31-probe-{}", std::process::id()));
        options.cache = Some((path.clone(), 0, 256 * 1024 * 1024));
        Some(path)
    } else {
        None
    };
    options.query_threads = threads;
    let local_dir = if std::env::var_os("M31_PROBE_LOCAL").is_some() {
        Some(tempfile::tempdir_in("target")?)
    } else {
        None
    };
    let store = match &local_dir {
        Some(directory) => ProbeStore::Local(
            Arc::new(LocalStore::open(directory.path().join("namespace"))?),
            Arc::new(AtomicU64::new(0)),
        ),
        None => ProbeStore::Memory(MemoryStore::default()),
    };
    let reopen_store = store.clone();
    let reopen_options = options.clone();
    let reopen_segmented = segmented.clone();
    let log_count = store.log_count();
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
    println!(
        "loaded RSS current/peak {:.2}/{:.2} MiB, sketch charged {:.2} MiB",
        rss_mib(),
        peak_rss_mib(),
        db.database().selective_index_bytes() as f64 / 1048576.
    );
    #[cfg(feature = "memory-probe")]
    println!(
        "loaded heap {:.2} MiB, total allocated {:.2} MiB",
        heap_mib(),
        TOTAL_HEAP_ALLOCATED.load(Ordering::Relaxed) as f64 / 1048576.
    );
    if let Ok(reopens) = std::env::var("M31_PROBE_REOPEN") {
        let reopens: usize = reopens.parse()?;
        assert!(reopens > 0);
        db.close()?;
        for attempt in 0..reopens {
            let started = Instant::now();
            let reopened = SegmentedServing::open(
                reopen_store.clone(),
                config,
                reopen_segmented.clone(),
                reopen_options.clone(),
            )?;
            println!(
                "reopen {} in {:.3} s, RSS current/peak {:.2}/{:.2} MiB, sketch charged {:.2} MiB",
                attempt + 1,
                started.elapsed().as_secs_f64(),
                rss_mib(),
                peak_rss_mib(),
                reopened.database().selective_index_bytes() as f64 / 1048576.
            );
            reopened.close()?;
        }
        #[cfg(feature = "memory-probe")]
        println!(
            "reopened heap current/peak {:.2}/{:.2} MiB",
            heap_mib(),
            peak_heap_mib()
        );
        return Ok(());
    }
    #[cfg(feature = "memory-probe")]
    {
        reset_heap_peak();
    }
    if warm_step {
        println!("warm baseline RSS {:.2} MiB", rss_mib());
        assert!(db.database().warm_cache_step(256 * 1024)?);
        println!(
            "after one 256-KiB warm unit RSS current/peak {:.2}/{:.2} MiB",
            rss_mib(),
            peak_rss_mib()
        );
        #[cfg(feature = "memory-probe")]
        println!(
            "warm heap current/peak {:.2}/{:.2} MiB",
            heap_mib(),
            peak_heap_mib()
        );
        if let Some(path) = cache_path {
            std::fs::remove_dir_all(path)?;
        }
        return Ok(());
    }
    if std::env::var_os("M31_PROBE_SNAPSHOT").is_some() {
        let held = db.snapshot().expect("segmented snapshot");
        println!("snapshot baseline RSS {:.2} MiB", rss_mib());
        for batch in 0..32 {
            db.apply_request(request(
                db.sequence(),
                (2_000_000 + batch) as u128,
                batch * 100,
                1,
            ))?;
        }
        println!(
            "after 32 writes RSS current/peak {:.2}/{:.2} MiB",
            rss_mib(),
            peak_rss_mib()
        );
        #[cfg(feature = "memory-probe")]
        println!(
            "writes heap current/peak {:.2}/{:.2} MiB",
            heap_mib(),
            peak_heap_mib()
        );
        while db.database().tail_objects() > 0 {
            db.maintenance_step()?;
        }
        println!(
            "after root publication RSS current/peak {:.2}/{:.2} MiB",
            rss_mib(),
            peak_rss_mib()
        );
        #[cfg(feature = "memory-probe")]
        println!(
            "root heap current/peak {:.2}/{:.2} MiB",
            heap_mib(),
            peak_heap_mib()
        );
        drop(held);
        println!(
            "after reader release RSS current/peak {:.2}/{:.2} MiB",
            rss_mib(),
            peak_rss_mib()
        );
        return Ok(());
    }
    let logs_before = log_count.load(Ordering::Relaxed);
    let initial_sequence = db.sequence();
    let latest_sequence = Arc::new(AtomicU64::new(initial_sequence));
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
        let (client, barrier, latest_sequence) =
            (client.clone(), barrier.clone(), latest_sequence.clone());
        workers.push(std::thread::spawn(move || {
            let mut data = Vec::new();
            barrier.wait();
            for round in 0..seconds {
                wait_until(began + Duration::from_secs(round));
                let start = ((writer * seconds + round) * 7919 % (rows / 100)) * 100;
                let submitted = Instant::now();
                let result = client
                    .write(request(
                        latest_sequence.load(Ordering::Acquire),
                        (1_000_000 + writer * seconds + round) as u128,
                        start,
                        round + 1,
                    ))
                    .unwrap()
                    .wait()
                    .unwrap();
                latest_sequence.fetch_max(result.value.sequence, Ordering::Release);
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
    println!(
        "serving RSS current/peak {:.2}/{:.2} MiB",
        rss_mib(),
        peak_rss_mib()
    );
    #[cfg(feature = "memory-probe")]
    println!(
        "serving heap current/peak {:.2}/{:.2} MiB, total allocated {:.2} MiB",
        heap_mib(),
        peak_heap_mib(),
        TOTAL_HEAP_ALLOCATED.load(Ordering::Relaxed) as f64 / 1048576.
    );
    service.shutdown(Shutdown::Drain)?;
    if let Some(path) = cache_path {
        std::fs::remove_dir_all(path)?;
    }
    Ok(())
}
