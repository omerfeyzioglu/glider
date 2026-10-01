//! M22–M24 single-machine acceptance on the declared M21 250,000-row envelope.
//! `load` builds the namespace, `serve` measures fresh readiness, static
//! quality and independent read/write traffic, and `verify` checks restart
//! state, update-wave quality, cache loss and backup in fresh processes.
use glider::{
    admission::{Engine, Limits, Service, Shutdown},
    retry::{Request, RequestId},
    segmented::{
        ReadBudget, SegmentedDatabase, SegmentedOptions, SegmentedServing, SegmentedServingOptions,
    },
    store::{
        s3::{AmazonS3Builder, ReadLimits, RequestCounts, S3Store},
        ObjectStore,
    },
    Config, Metric, Mutation, Neighbor,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BinaryHeap},
    env, fs,
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier, Mutex,
    },
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Rows in the namespace: the M21 250,000 unless `GLIDER_M24_ROWS` selects
/// another scale (a multiple of 400), such as the M31 1,000,000.
fn rows() -> u64 {
    env::var("GLIDER_M24_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(250_000)
}
const DIMENSIONS: usize = 128;
const FILTER: (&str, &str) = ("cohort", "one-percent");

fn config() -> Config {
    Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    }
}

fn options() -> SegmentedOptions {
    SegmentedOptions {
        resident_filter: Some((FILTER.0.into(), FILTER.1.into())),
    }
}

fn metadata(id: u64) -> BTreeMap<String, String> {
    if id.is_multiple_of(100) {
        BTreeMap::from([(FILTER.0.into(), FILTER.1.into())])
    } else {
        BTreeMap::new()
    }
}

/// Counts payload bytes returned by GET and range GET, and PUT/DELETE
/// requests by object kind (the key prefix before the first '-').
struct Counted {
    inner: S3Store,
    bytes: Arc<AtomicU64>,
    kinds: Arc<Mutex<BTreeMap<String, (u64, u64)>>>,
}

impl Counted {
    fn count(&self, key: &str, delete: bool) {
        let kind = key.split('-').next().unwrap_or(key).to_owned();
        let mut kinds = self.kinds.lock().unwrap();
        let entry = kinds.entry(kind).or_default();
        if delete {
            entry.1 += 1;
        } else {
            entry.0 += 1;
        }
    }
}

impl ObjectStore for Counted {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let value = self.inner.get(key)?;
        if let Some(bytes) = &value {
            self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(value)
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload_len: usize,
    ) -> glider::Result<Option<Vec<u8>>> {
        let value = self.inner.get_range(key, offset, length, payload_len)?;
        if let Some(bytes) = &value {
            self.bytes.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(value)
    }
    fn get_many(&self, keys: &[String]) -> glider::Result<Vec<Option<Vec<u8>>>> {
        let values = self.inner.get_many(keys)?;
        let total: usize = values.iter().flatten().map(Vec::len).sum();
        self.bytes.fetch_add(total as u64, Ordering::Relaxed);
        Ok(values)
    }
    fn get_ranges(
        &self,
        ranges: &[(&str, usize, usize, usize)],
    ) -> glider::Result<Vec<Option<Vec<u8>>>> {
        let values = self.inner.get_ranges(ranges)?;
        let total: usize = values.iter().flatten().map(Vec::len).sum();
        self.bytes.fetch_add(total as u64, Ordering::Relaxed);
        Ok(values)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        self.count(key, false);
        self.inner.create(key, value)
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.count(key, true);
        self.inner.remove(key)
    }
    fn remove_many(&mut self, keys: &[String]) -> glider::Result<()> {
        for key in keys {
            self.count(key, true);
        }
        self.inner.remove_many(keys)
    }
}

struct Opened {
    store: Counted,
    metrics: glider::store::s3::RequestMetrics,
    bytes: Arc<AtomicU64>,
    kinds: Arc<Mutex<BTreeMap<String, (u64, u64)>>>,
}

fn store(namespace: &str) -> Result<Opened> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(env::var("GLIDER_S3_BUCKET")?)
        .with_region(env::var("GLIDER_S3_REGION").unwrap_or_else(|_| "us-east-1".into()))
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?);
    if let Ok(token) = env::var("AWS_SESSION_TOKEN") {
        builder = builder.with_token(token);
    }
    if let Ok(endpoint) = env::var("GLIDER_S3_ENDPOINT") {
        builder = builder
            .with_allow_http(endpoint.starts_with("http://"))
            .with_endpoint(endpoint);
    }
    let inner = S3Store::open(builder, namespace)?.with_read_limits(ReadLimits {
        objects: 20_000,
        object_bytes: 16 * 1024 * 1024,
        namespace_bytes: 1024 * 1024 * 1024,
    })?;
    let bytes = Arc::new(AtomicU64::new(0));
    let kinds = Arc::new(Mutex::new(BTreeMap::new()));
    Ok(Opened {
        metrics: inner.metrics(),
        store: Counted {
            inner,
            bytes: bytes.clone(),
            kinds: kinds.clone(),
        },
        bytes,
        kinds,
    })
}

fn counts(before: &RequestCounts, after: &RequestCounts) -> Value {
    json!({"get":after.get-before.get,"list":after.list-before.list,"put":after.put-before.put,
        "delete":after.delete-before.delete,"other":after.other-before.other,
        "request_body_bytes":after.request_body_bytes-before.request_body_bytes,
        "http_errors":after.http_errors-before.http_errors,
        "transport_errors":after.transport_errors-before.transport_errors})
}

/// Reads fvecs rows on demand so the serving process never holds the corpus.
struct Rows(fs::File);
impl Rows {
    fn open(path: &str) -> Result<Self> {
        Ok(Self(fs::File::open(path)?))
    }
    fn row(&self, index: u64) -> Result<Vec<f32>> {
        let mut bytes = [0_u8; 4 + DIMENSIONS * 4];
        self.0
            .read_exact_at(&mut bytes, index * (4 + DIMENSIONS as u64 * 4))?;
        if u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize != DIMENSIONS {
            return Err("invalid fvecs dimension".into());
        }
        Ok(bytes[4..]
            .as_chunks::<4>()
            .0
            .iter()
            .map(|part| f32::from_le_bytes(*part))
            .collect())
    }
}

/// Overwrite generation g of an ID uses base row (id + 137g) mod rows.
fn vector(base: &Rows, id: u64, generation: u64) -> Result<Vec<f32>> {
    base.row((id + 137 * generation) % rows())
}

/// Writer c, round r overwrites 100 IDs in its own quarter with generation r+1.
fn batch_ids(client: u64, round: u64) -> impl Iterator<Item = u64> {
    let slot = round % (rows() / 4 / 100);
    (0..100).map(move |n| client * (rows() / 4) + slot * 100 + n)
}

fn rss() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let value = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        value
    } else {
        value * 1024
    }
}

/// macOS lifetime peak physical footprint (dirty plus compressed memory),
/// which excludes shared library pages and freed pages RSS may still count.
fn peak_footprint() -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        let mut info = std::mem::MaybeUninit::<libc::rusage_info_v4>::zeroed();
        let status = unsafe {
            libc::proc_pid_rusage(
                std::process::id() as i32,
                libc::RUSAGE_INFO_V4,
                info.as_mut_ptr().cast(),
            )
        };
        (status == 0).then(|| unsafe { info.assume_init() }.ri_lifetime_max_phys_footprint)
    }
    #[cfg(not(target_os = "macos"))]
    None
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.
}

fn stats(mut values: Vec<f64>) -> Value {
    if values.is_empty() {
        return json!({"count":0});
    }
    values.sort_by(f64::total_cmp);
    let at = |p: usize| values[(values.len() * p).div_ceil(100).saturating_sub(1)];
    json!({"count":values.len(),"mean":values.iter().sum::<f64>()/values.len() as f64,
        "p50":at(50),"p95":at(95),"p99":at(99),"max":values[values.len()-1]})
}

fn serving_options(cache: Option<PathBuf>) -> SegmentedServingOptions {
    let mut options = if rows() > 250_000 {
        SegmentedServingOptions::m31(PathBuf::new())
    } else {
        SegmentedServingOptions::m21(PathBuf::new())
    };
    options.cache = cache.map(|directory| (directory, 0, 256 * 1024 * 1024));
    options
}

struct Handles {
    metrics: glider::store::s3::RequestMetrics,
    bytes: Arc<AtomicU64>,
    kinds: Arc<Mutex<BTreeMap<String, (u64, u64)>>>,
}

fn open_serving(
    namespace: &str,
    cache: Option<PathBuf>,
) -> Result<(SegmentedServing<Counted>, Handles, Value)> {
    let Opened {
        store,
        metrics,
        bytes,
        kinds,
    } = store(namespace)?;
    let before = metrics.snapshot();
    let started = Instant::now();
    let db = SegmentedServing::open(store, config(), options(), serving_options(cache))?;
    let open_ms = ms(started.elapsed());
    let after = metrics.snapshot();
    let observation = json!({"ms":open_ms,"http":counts(&before,&after),
        "get_payload_bytes":bytes.load(Ordering::Relaxed),
        "sketch_charged_bytes":db.database().selective_index_bytes(),
        "sketch_rebuilds":db.database().sketch_rebuilds(),
        "runs":db.database().run_count(),"blocks":db.database().block_count(),
        "tail_objects":db.database().tail_objects(),"sequence":db.database().sequence()});
    Ok((
        db,
        Handles {
            metrics,
            bytes,
            kinds,
        },
        observation,
    ))
}

fn request(boundary: u64, nonce: [u8; 16], mutations: Vec<Mutation>) -> Request {
    Request {
        id: RequestId { boundary, nonce },
        conditions: Vec::new(),
        mutations,
    }
}

fn load(args: &[String]) -> Result<Value> {
    let [base, namespace] = args else {
        return Err("usage: load BASE.fvecs NAMESPACE".into());
    };
    let base = Rows::open(base)?;
    let (mut db, handles, open) = open_serving(namespace, None)?;
    let before = handles.metrics.snapshot();
    let started = Instant::now();
    let mut sequence = 0;
    let mut longest_step = Duration::ZERO;
    let mut units = BTreeMap::new();
    for batch in 0..rows() / 100 {
        let mutations = (batch * 100..(batch + 1) * 100)
            .map(|id| {
                Ok(Mutation::Put {
                    id,
                    vector: vector(&base, id, 0)?,
                    metadata: metadata(id),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut nonce = [0xff; 16];
        nonce[..8].copy_from_slice(&batch.to_le_bytes());
        sequence = db
            .apply_request(request(sequence, nonce, mutations))?
            .sequence;
        if db.database().tail_objects() >= 32 {
            loop {
                let step = Instant::now();
                let peak_before = peak_footprint().unwrap_or(0);
                let worked = db.maintenance_step()?;
                let elapsed = step.elapsed();
                let raised = peak_footprint().unwrap_or(0) - peak_before;
                let entry = units.entry(db.last_unit()).or_insert((0_u64, 0_f64, 0_u64));
                entry.0 += 1;
                entry.1 = entry.1.max(ms(elapsed));
                entry.2 += raised;
                if !worked {
                    break;
                }
                longest_step = longest_step.max(elapsed);
            }
        }
    }
    let after = handles.metrics.snapshot();
    let result = json!({"rows":rows(),"open":open,"load_ms":ms(started.elapsed()),
        "http":counts(&before,&after),"longest_maintenance_step_ms":ms(longest_step),
        "runs":db.database().run_count(),"blocks":db.database().block_count(),
        "tail_objects":db.database().tail_objects(),"sequence":sequence,
        "sketch_charged_bytes":db.database().selective_index_bytes(),
        "counters":db.counters(),"maintenance_units":units,"peak_rss_bytes":rss()});
    db.close()?;
    Ok(result)
}

fn oracle_ids(oracle: &Value, key: &str, index: usize) -> Result<Vec<u64>> {
    oracle[key][index]
        .as_array()
        .ok_or("missing oracle row")?
        .iter()
        .map(|id| id.as_u64().ok_or_else(|| "invalid oracle ID".into()))
        .collect()
}

/// Per command or unit kind: count, slowest milliseconds, peak bytes raised.
type Profile = Arc<Mutex<BTreeMap<&'static str, (u64, f64, u64)>>>;

/// Delegating engine that attributes peak-footprint increases and time to
/// commands and maintenance-unit kinds.
struct Profiled {
    inner: SegmentedServing<Counted>,
    profile: Profile,
}

impl Engine for Profiled {
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
    fn apply_request(&mut self, request: Request) -> glider::Result<glider::retry::Outcome> {
        let peak = peak_footprint().unwrap_or(0);
        let started = Instant::now();
        let result = self.inner.apply_request(request);
        if let Err(error) = &result {
            eprintln!("write failed: {error}");
        }
        let (elapsed, raised) = (
            ms(started.elapsed()),
            peak_footprint().unwrap_or(0).saturating_sub(peak),
        );
        let mut profile = self.profile.lock().unwrap();
        let entry = profile.entry("write").or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(elapsed);
        entry.2 += raised;
        result
    }
    fn revision(&self, id: u64) -> glider::retry::Revision {
        self.inner.revision(id)
    }
    fn request_id(&self) -> glider::Result<RequestId> {
        self.inner.request_id()
    }
    fn lookup_request(&self, id: RequestId) -> glider::Result<glider::retry::Lookup> {
        self.inner.lookup_request(id)
    }
    fn get(&self, id: u64) -> glider::Result<Option<glider::streaming::OwnedDocument>> {
        self.inner.get(id)
    }
    fn query(
        &mut self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> glider::Result<Vec<Neighbor>> {
        let kind = if filter.is_empty() {
            "query"
        } else {
            "filtered_query"
        };
        let peak = peak_footprint().unwrap_or(0);
        let started = Instant::now();
        let result = self.inner.query(query, k, filter);
        let (elapsed, raised) = (
            ms(started.elapsed()),
            peak_footprint().unwrap_or(0).saturating_sub(peak),
        );
        let mut profile = self.profile.lock().unwrap();
        let entry = profile.entry(kind).or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(elapsed);
        entry.2 += raised;
        result
    }
    fn idle_step(&mut self) -> glider::Result<bool> {
        let peak = peak_footprint().unwrap_or(0);
        let started = Instant::now();
        let result = self.inner.idle_step();
        if let Err(error) = &result {
            eprintln!(
                "maintenance unit {} failed: {error}",
                self.inner.last_unit()
            );
        }
        let (elapsed, raised) = (
            ms(started.elapsed()),
            peak_footprint().unwrap_or(0).saturating_sub(peak),
        );
        let mut profile = self.profile.lock().unwrap();
        let entry = profile.entry(self.inner.last_unit()).or_default();
        entry.0 += 1;
        entry.1 = entry.1.max(elapsed);
        entry.2 += raised;
        result
    }
    fn remote_reads(&self) -> (u64, u64) {
        self.inner.remote_reads()
    }
    fn close(self) -> glider::Result<()> {
        self.inner.close()
    }
}

struct Write {
    e2e: f64,
    queue: f64,
    execution: f64,
    maintenance: f64,
    late: f64,
}

struct QueryEvent {
    filtered: bool,
    e2e: f64,
    queue: f64,
    execution: f64,
    reads: u64,
    bytes: u64,
    late: f64,
    short: bool,
}

fn wait_until(due: Instant) -> f64 {
    let now = Instant::now();
    if now < due {
        std::thread::sleep(due - now);
        0.
    } else {
        ms(now - due)
    }
}

fn static_pass(
    db: &mut SegmentedServing<Counted>,
    queries: &Rows,
    oracle: &Value,
) -> Result<Value> {
    let mut classes = Vec::new();
    for pass in ["first_empty_cache", "second_warm"] {
        let mut result = serde_json::Map::new();
        for (key, filter) in [("unfiltered", Vec::new()), ("filtered", vec![FILTER])] {
            let oracle_key = format!("{key}_exact_ids");
            let (mut hits, mut short, mut per_query) = (Vec::new(), 0, Vec::new());
            let (mut latency, mut reads, mut bytes) = (Vec::new(), Vec::new(), Vec::new());
            let (mut cold, mut warm) = (Vec::new(), Vec::new());
            for index in 0..200 {
                let query = queries.row(index as u64)?;
                let (reads_before, bytes_before) = db.remote_reads();
                let started = Instant::now();
                let found = db.query(&query, 10, &filter)?;
                let elapsed = ms(started.elapsed());
                let (reads_after, bytes_after) = db.remote_reads();
                let truth = oracle_ids(oracle, &oracle_key, index)?;
                let hit = found.iter().filter(|n| truth.contains(&n.id)).count();
                hits.push(hit);
                per_query.push(hit as f64 / 10.);
                short += usize::from(found.len() < truth.len());
                latency.push(elapsed);
                reads.push((reads_after - reads_before) as f64);
                bytes.push((bytes_after - bytes_before) as f64);
                if reads_after > reads_before {
                    cold.push(elapsed);
                } else {
                    warm.push(elapsed);
                }
            }
            per_query.sort_by(f64::total_cmp);
            result.insert(key.into(), json!({
                "mean_recall_at_10":hits.iter().sum::<usize>() as f64/2000.,
                "fifth_percentile_recall_at_10":per_query[9],
                "short_results":short,"latency_ms":stats(latency),
                "remote_reads":stats(reads),"remote_payload_bytes":stats(bytes),
                "queries_with_remote_reads_ms":stats(cold),"queries_without_remote_reads_ms":stats(warm)}));
        }
        result.insert("pass".into(), pass.into());
        classes.push(Value::Object(result));
    }
    Ok(json!(classes))
}

fn serve(args: &[String]) -> Result<Value> {
    let [queries, base, namespace, cache, oracle, rounds] = args else {
        return Err("usage: serve QUERY BASE NAMESPACE CACHE_DIR ORACLE ROUNDS".into());
    };
    let queries = Rows::open(queries)?;
    let base = Arc::new(Rows::open(base)?);
    // "-" skips the static-quality pass, whose oracle covers only 250,000 rows.
    let oracle: Option<Value> = if oracle == "-" {
        None
    } else {
        Some(serde_json::from_slice(&fs::read(oracle)?)?)
    };
    let rounds: u64 = rounds.parse()?;
    let rss_before_open = rss();
    let (mut db, handles, open) = open_serving(namespace, Some(PathBuf::from(cache)))?;
    let rss_after_open = rss();
    let static_quality = match &oracle {
        Some(oracle) => static_pass(&mut db, &queries, oracle)?,
        None => Value::Null,
    };
    let rss_after_static = rss();
    let cache_after_static = db.database().cache_stats()?;
    drop(oracle);
    let initial_sequence = db.sequence();
    let profile = Arc::new(Mutex::new(BTreeMap::new()));
    let service = Service::start(
        Profiled {
            inner: db,
            profile: profile.clone(),
        },
        Limits {
            read_priority: Some(Duration::from_millis(50)),
            ..Limits::default()
        },
    )?;
    let client = service.client();
    let before = handles.metrics.snapshot();
    let before_bytes = handles.bytes.load(Ordering::Relaxed);
    let kinds_before = handles.kinds.lock().unwrap().clone();
    let boundary = Arc::new(AtomicU64::new(initial_sequence));
    let acknowledged = Arc::new(Mutex::new(Vec::new()));
    let barrier = Arc::new(Barrier::new(9));
    let began = Instant::now() + Duration::from_millis(200);
    let mut writers = Vec::new();
    for writer in 0..4_u64 {
        let (client, boundary, acknowledged, barrier, base) = (
            client.clone(),
            boundary.clone(),
            acknowledged.clone(),
            barrier.clone(),
            base.clone(),
        );
        writers.push(std::thread::spawn(
            move || -> Result<(Vec<Write>, u64, u64)> {
                let (mut events, mut overloaded, mut skipped) = (Vec::new(), 0, 0);
                barrier.wait();
                for round in 0..rounds {
                    let late = wait_until(began + Duration::from_secs(round));
                    if late > 1000. {
                        skipped += 1;
                        continue;
                    }
                    let mutations = batch_ids(writer, round)
                        .map(|id| {
                            Ok(Mutation::Put {
                                id,
                                vector: vector(&base, id, round + 1)?,
                                metadata: metadata(id),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let mut nonce = [0; 16];
                    nonce[..8].copy_from_slice(&writer.to_le_bytes());
                    nonce[8..].copy_from_slice(&round.to_le_bytes());
                    let started = Instant::now();
                    let ticket = match client.write(request(
                        boundary.load(Ordering::Acquire),
                        nonce,
                        mutations,
                    )) {
                        Ok(ticket) => ticket,
                        Err(glider::admission::Error::Overloaded) => {
                            overloaded += 1;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let result = ticket.wait()?;
                    if result.value.conflict.is_some() {
                        return Err("unexpected conditional conflict".into());
                    }
                    boundary.fetch_max(result.value.sequence, Ordering::AcqRel);
                    acknowledged.lock().unwrap().push((writer, round));
                    events.push(Write {
                        e2e: ms(started.elapsed()),
                        queue: ms(result.queue_wait),
                        execution: ms(result.execution),
                        maintenance: ms(result.maintenance),
                        late,
                    });
                }
                Ok((events, overloaded, skipped))
            },
        ));
    }
    let queries = Arc::new(queries);
    let mut readers = Vec::new();
    for reader in 0..4_u64 {
        let (client, barrier, queries) = (client.clone(), barrier.clone(), queries.clone());
        readers.push(std::thread::spawn(
            move || -> Result<(Vec<QueryEvent>, u64, u64)> {
                let (mut events, mut overloaded, mut skipped) = (Vec::new(), 0, 0);
                barrier.wait();
                for tick in 0..rounds * 10 {
                    let late = wait_until(began + Duration::from_millis(tick * 100));
                    if late > 100. {
                        skipped += 1;
                        continue;
                    }
                    let index = reader * rounds * 10 + tick;
                    let filtered = index.is_multiple_of(2);
                    let query = queries.row(200 + index % 9_800)?;
                    let filter = if filtered {
                        vec![(FILTER.0.to_owned(), FILTER.1.to_owned())]
                    } else {
                        Vec::new()
                    };
                    let started = Instant::now();
                    let ticket = match client.query(query, 10, filter) {
                        Ok(ticket) => ticket,
                        Err(glider::admission::Error::Overloaded) => {
                            overloaded += 1;
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let result = ticket.wait()?;
                    events.push(QueryEvent {
                        filtered,
                        e2e: ms(started.elapsed()),
                        queue: ms(result.queue_wait),
                        execution: ms(result.execution),
                        reads: result.value.remote_reads,
                        bytes: result.value.remote_bytes,
                        late,
                        short: result.value.neighbors.len() < 10,
                    });
                }
                Ok((events, overloaded, skipped))
            },
        ));
    }
    barrier.wait();
    // Running peak RSS once per second shows which phase raises it.
    let sampler_stop = Arc::new(AtomicU64::new(0));
    let sampler = {
        let stop = sampler_stop.clone();
        std::thread::spawn(move || {
            let mut series = Vec::new();
            while stop.load(Ordering::Acquire) == 0 {
                std::thread::sleep(Duration::from_secs(1));
                series.push(rss());
            }
            series
        })
    };
    let (mut writes, mut queries_done) = (Vec::new(), Vec::new());
    let mut losses = [0_u64; 4];
    for writer in writers {
        let (events, overloaded, skipped) = writer.join().map_err(|_| "writer panic")??;
        writes.extend(events);
        losses[0] += overloaded;
        losses[1] += skipped;
    }
    for reader in readers {
        let (events, overloaded, skipped) = reader.join().map_err(|_| "reader panic")??;
        queries_done.extend(events);
        losses[2] += overloaded;
        losses[3] += skipped;
    }
    let elapsed = began.elapsed().as_secs_f64();
    sampler_stop.store(1, Ordering::Release);
    let rss_series = sampler.join().map_err(|_| "sampler panic")?;
    let idle = client.idle_maintenance_samples();
    let status = client.status();
    let after = handles.metrics.snapshot();
    let downloaded = handles.bytes.load(Ordering::Relaxed) - before_bytes;
    let kinds: BTreeMap<_, _> = handles
        .kinds
        .lock()
        .unwrap()
        .iter()
        .map(|(kind, &(puts, deletes))| {
            let (p, d) = kinds_before.get(kind).copied().unwrap_or((0, 0));
            (kind.clone(), json!({"put":puts - p,"delete":deletes - d}))
        })
        .collect();
    let peak_rss_bytes = rss();
    service.shutdown(Shutdown::Drain)?;
    let classes = |filtered: Option<bool>, remote: Option<bool>, field: fn(&QueryEvent) -> f64| {
        stats(
            queries_done
                .iter()
                .filter(|q| filtered.is_none_or(|f| q.filtered == f))
                .filter(|q| remote.is_none_or(|r| (q.reads > 0) == r))
                .map(field)
                .collect(),
        )
    };
    let mut acknowledged = acknowledged.lock().unwrap().clone();
    acknowledged.sort_unstable();
    Ok(
        json!({"rounds":rounds,"open":open,"static_quality":static_quality,
        "offered":{"write_batches":rounds*4,"logical_mutations":rounds*400,"queries":rounds*40},
        "acknowledged":{"write_batches":writes.len(),"queries":queries_done.len()},
        "overloaded":{"writes":losses[0],"queries":losses[2]},
        "late_slots_skipped":{"writes":losses[1],"queries":losses[3]},
        "elapsed_seconds":elapsed,
        "write_ms":stats(writes.iter().map(|w|w.e2e).collect()),
        "write_queue_ms":stats(writes.iter().map(|w|w.queue).collect()),
        "write_execution_ms":stats(writes.iter().map(|w|w.execution).collect()),
        "write_forced_maintenance_ms":stats(writes.iter().filter(|w|w.maintenance>0.).map(|w|w.maintenance).collect()),
        "write_arrival_lateness_ms":stats(writes.iter().map(|w|w.late).collect()),
        "query_arrival_lateness_ms":stats(queries_done.iter().map(|q|q.late).collect()),
        "query_ms":classes(None,None,|q|q.e2e),
        "query_queue_ms":classes(None,None,|q|q.queue),
        "unfiltered_query_ms":classes(Some(false),None,|q|q.e2e),
        "unfiltered_query_execution_ms":classes(Some(false),None,|q|q.execution),
        "filtered_query_ms":classes(Some(true),None,|q|q.e2e),
        "unfiltered_warm_query_ms":classes(Some(false),Some(false),|q|q.e2e),
        "unfiltered_cold_query_ms":classes(Some(false),Some(true),|q|q.e2e),
        "query_remote_reads":classes(None,None,|q|q.reads as f64),
        "query_remote_payload_bytes":classes(None,None,|q|q.bytes as f64),
        "short_results":queries_done.iter().filter(|q|q.short).count(),
        "idle_maintenance_ms":stats(idle.iter().map(|d|ms(*d)).collect()),
        "maintenance_errors":status.maintenance_errors,
        "http":counts(&before,&after),"requests_by_kind":kinds,"get_payload_bytes":downloaded,
        "peak_rss_bytes":peak_rss_bytes,"peak_physical_footprint_bytes":peak_footprint(),
        "engine_profile":profile.lock().unwrap().iter().map(|(kind, (count, max_ms, raised))| {
            (kind.to_string(), json!({"count":count,"max_ms":max_ms,"peak_footprint_raised_bytes":raised}))
        }).collect::<BTreeMap<_, _>>(),
        "cache":cache_after_static,
        "peak_rss_per_second":rss_series,
        "peak_rss_stages":{"before_open":rss_before_open,"after_open":rss_after_open,
            "after_static_quality":rss_after_static},
        "acknowledged_batches":acknowledged}),
    )
}

struct TopK {
    query: Vec<f32>,
    filtered: bool,
    heap: BinaryHeap<(u64, u64)>,
}

fn verify(args: &[String]) -> Result<Value> {
    let [queries, base, namespace, cache, serve_report, backup] = args else {
        return Err(
            "usage: verify QUERY BASE NAMESPACE CACHE_DIR SERVE.json BACKUP_NAMESPACE".into(),
        );
    };
    let queries = Rows::open(queries)?;
    let base = Rows::open(base)?;
    let report: Value = serde_json::from_slice(&fs::read(serve_report)?)?;
    let mut generation = BTreeMap::new();
    for pair in report["acknowledged_batches"]
        .as_array()
        .ok_or("missing batches")?
    {
        let (writer, round) = (pair[0].as_u64().unwrap(), pair[1].as_u64().unwrap());
        for id in batch_ids(writer, round) {
            let previous = generation.insert(id, round + 1).unwrap_or(0);
            if previous > round + 1 {
                return Err("overwrite rounds are not monotonic".into());
            }
        }
    }
    let cache = PathBuf::from(cache);
    let (db, _handles, reopen) = open_serving(namespace, Some(cache.clone()))?;
    // One authenticated scan checks every acknowledged value and computes
    // the exact oracle for 100 held-out queries of each class.
    let mut oracles: Vec<TopK> = (0..200)
        .map(|index| {
            Ok(TopK {
                query: queries.row(9_000 + index / 2)?,
                filtered: index % 2 == 1,
                heap: BinaryHeap::new(),
            })
        })
        .collect::<Result<_>>()?;
    let started = Instant::now();
    let mut seen = 0_u64;
    let mut mismatches = 0_u64;
    db.database().scan_live(|id, vector, found| {
        seen += 1;
        let expected = self::vector(&base, id, generation.get(&id).copied().unwrap_or(0))
            .map_err(|error| glider::Error::Invalid(error.to_string()))?;
        if vector != expected.as_slice() || *found != metadata(id) {
            mismatches += 1;
        }
        for oracle in &mut oracles {
            if oracle.filtered && !id.is_multiple_of(100) {
                continue;
            }
            let distance: f64 = oracle
                .query
                .iter()
                .zip(vector)
                .map(|(a, b)| {
                    let d = f64::from(*a) - f64::from(*b);
                    d * d
                })
                .sum();
            let entry = (distance.to_bits(), id);
            if oracle.heap.len() < 10 {
                oracle.heap.push(entry);
            } else if entry < *oracle.heap.peek().unwrap() {
                oracle.heap.pop();
                oracle.heap.push(entry);
            }
        }
        Ok(())
    })?;
    let scan_ms = ms(started.elapsed());
    let serving_budget = serving_options(None).read_budget;
    let mut budgets = BTreeMap::new();
    for (label, budget) in [
        ("uniform_8", ReadBudget::uniform(8)),
        ("uniform_10", ReadBudget::uniform(10)),
        ("uniform_12", ReadBudget::uniform(12)),
        ("serving", serving_budget),
        (
            "serving_16",
            ReadBudget {
                blocks: 16,
                ..serving_budget
            },
        ),
        (
            "serving_24",
            ReadBudget {
                blocks: 24,
                ..serving_budget
            },
        ),
    ] {
        let mut recall = [0_usize; 2];
        for oracle in &oracles {
            let truth: Vec<u64> = oracle.heap.iter().map(|&(_, id)| id).collect();
            if oracle.filtered {
                continue;
            }
            let found = db
                .database()
                .search_selective_within(&oracle.query, 10, budget, &[])?;
            recall[0] += found.iter().filter(|n| truth.contains(&n.id)).count();
            recall[1] += 10;
        }
        budgets.insert(label, recall[0] as f64 / recall[1] as f64);
    }
    // Best possible coverage by any 8 committed blocks (tail rows count as
    // found): separates layout locality from routing quality.
    let mut ceiling = [0_usize; 2];
    for oracle in oracles.iter().filter(|oracle| !oracle.filtered) {
        let mut per_block = BTreeMap::<(usize, usize), usize>::new();
        let mut tail = 0;
        for &(_, id) in &oracle.heap {
            match db.database().current_block_of(id) {
                Some(block) => *per_block.entry(block).or_default() += 1,
                None => tail += 1,
            }
        }
        let mut counts: Vec<_> = per_block.into_values().collect();
        counts.sort_unstable_by(|a, b| b.cmp(a));
        ceiling[0] += tail + counts.iter().take(8).sum::<usize>();
        ceiling[1] += oracle.heap.len();
    }
    budgets.insert(
        "oracle_best_8_blocks",
        ceiling[0] as f64 / ceiling[1] as f64,
    );
    let mut quality = BTreeMap::<&str, (Vec<f64>, usize)>::new();
    let mut first_results = Vec::new();
    for oracle in &oracles {
        let truth: Vec<u64> = oracle.heap.iter().map(|&(_, id)| id).collect();
        let filter = if oracle.filtered {
            vec![FILTER]
        } else {
            Vec::new()
        };
        let found =
            db.database()
                .search_selective_within(&oracle.query, 10, serving_budget, &filter)?;
        if first_results.len() < 20 {
            first_results.push(found.iter().map(|n| n.id).collect::<Vec<_>>());
        }
        let entry = quality
            .entry(if oracle.filtered {
                "filtered"
            } else {
                "unfiltered"
            })
            .or_default();
        entry
            .0
            .push(found.iter().filter(|n| truth.contains(&n.id)).count() as f64 / 10.);
        entry.1 += usize::from(found.len() < truth.len());
    }
    let quality: BTreeMap<_, _> = quality
        .into_iter()
        .map(|(key, (mut recalls, short))| {
            let mean = recalls.iter().sum::<f64>() / recalls.len() as f64;
            recalls.sort_by(f64::total_cmp);
            (key, json!({"queries":recalls.len(),"mean_recall_at_10":mean,
                "fifth_percentile_recall_at_10":recalls[recalls.len()*5/100-1],"short_results":short}))
        })
        .collect();
    db.close()?;

    fs::remove_dir_all(cache.join("glider-block-cache-v1")).ok();
    let (db, _handles, loss_open) = open_serving(namespace, Some(cache.clone()))?;
    let mut loss_equal = true;
    for (oracle, expected) in oracles.iter().zip(&first_results) {
        let filter = if oracle.filtered {
            vec![FILTER]
        } else {
            Vec::new()
        };
        let found: Vec<_> = db
            .database()
            .search_selective_within(&oracle.query, 10, serving_budget, &filter)?
            .iter()
            .map(|n: &Neighbor| n.id)
            .collect();
        loss_equal &= &found == expected;
    }
    let mut db = db;
    // "-" skips the backup copy, e.g. for a bandwidth-bounded remote check.
    let mut backup_report = json!(null);
    if backup != "-" {
        let Opened {
            store: destination,
            metrics,
            ..
        } = store(backup)?;
        let before = metrics.snapshot();
        let started = Instant::now();
        db.backup_to(destination)?;
        let backup_ms = ms(started.elapsed());
        let backup_http = counts(&before, &metrics.snapshot());
        let restored =
            SegmentedDatabase::open_with_options(store(backup)?.store, config(), options())?;
        let mut backup_equal = restored.sequence() == db.sequence();
        for (oracle, expected) in oracles.iter().zip(&first_results) {
            let filter = if oracle.filtered {
                vec![FILTER]
            } else {
                Vec::new()
            };
            let found: Vec<_> = restored
                .search_selective_within(&oracle.query, 10, serving_budget, &filter)?
                .iter()
                .map(|n| n.id)
                .collect();
            backup_equal &= &found == expected;
        }
        backup_report = json!({"backup_ms":backup_ms,"backup_http":backup_http,
            "backup_restored_equal":backup_equal,
            "restored_sketch_rebuilds":restored.sketch_rebuilds()});
    }
    db.close()?;
    Ok(
        json!({"reopen":reopen,"scan_ms":scan_ms,"live_documents":seen,
        "expected_documents":rows(),"value_mismatches":mismatches,
        "overwritten_ids":generation.len(),"update_wave_quality":quality,
        "unfiltered_mean_recall_by_block_budget":budgets,
        "cache_loss_open":loss_open,"cache_loss_results_equal":loss_equal,
        "backup":backup_report}),
    )
}

/// Exact top-10 IDs of the first 200 queries over the unmodified corpus for
/// the static-quality pass: an f64 scan with ties by ID, like `verify`.
fn oracle(args: &[String]) -> Result<Value> {
    let [queries, base] = args else {
        return Err("usage: oracle QUERY BASE".into());
    };
    let (queries, base) = (Rows::open(queries)?, Rows::open(base)?);
    let queries: Vec<Vec<f32>> = (0..200).map(|i| queries.row(i)).collect::<Result<_>>()?;
    let started = Instant::now();
    let threads = std::thread::available_parallelism()?.get();
    type Heaps = Vec<[BinaryHeap<(u64, u64)>; 2]>;
    let partial: Vec<Heaps> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|thread| {
                let (base, queries) = (&base, &queries);
                scope.spawn(move || -> Result<Heaps> {
                    let mut heaps: Heaps = vec![Default::default(); queries.len()];
                    for id in (thread as u64..rows()).step_by(threads) {
                        let vector = base.row(id)?;
                        for (query, heaps) in queries.iter().zip(&mut heaps) {
                            let distance: f64 = query
                                .iter()
                                .zip(&vector)
                                .map(|(a, b)| {
                                    let d = f64::from(*a) - f64::from(*b);
                                    d * d
                                })
                                .sum();
                            let entry = (distance.to_bits(), id);
                            let classes = 1 + usize::from(id.is_multiple_of(100));
                            for heap in &mut heaps[..classes] {
                                if heap.len() < 10 {
                                    heap.push(entry);
                                } else if entry < *heap.peek().unwrap() {
                                    heap.pop();
                                    heap.push(entry);
                                }
                            }
                        }
                    }
                    Ok(heaps)
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Result<_>>()
    })?;
    let ids = |class: usize| -> Vec<Vec<u64>> {
        (0..queries.len())
            .map(|query| {
                let mut entries: Vec<_> = partial
                    .iter()
                    .flat_map(|heaps| heaps[query][class].iter().copied())
                    .collect();
                entries.sort_unstable();
                entries.into_iter().take(10).map(|(_, id)| id).collect()
            })
            .collect()
    };
    Ok(json!({"rows":rows(),"oracle_ms":ms(started.elapsed()),
        "unfiltered_exact_ids":ids(0),"filtered_exact_ids":ids(1)}))
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let result = match args.get(1).map(String::as_str) {
        Some("load") => load(&args[2..])?,
        Some("serve") => serve(&args[2..])?,
        Some("verify") => verify(&args[2..])?,
        Some("oracle") => oracle(&args[2..])?,
        _ => return Err("usage: m24_acceptance load|serve|verify|oracle ...".into()),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
