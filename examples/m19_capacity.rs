//! Bounded SIFT capacity study; run through tools/m19_benchmark.py.
use glider::{
    admission::{Client, Limits, QueryResult, Service, Shutdown, Timed},
    retry::{Outcome, Request, RequestId},
    serving::{SearchMode, ServingOptions, SingleMachine},
    store::{
        s3::{AmazonS3Builder, ReadLimits, S3Store},
        ObjectStore,
    },
    Config, Metric, Mutation, Neighbor,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    env, fs,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Db = SingleMachine<Observed>;
static ROWS: OnceLock<u64> = OnceLock::new();
static DATA: OnceLock<Vec<Vec<f32>>> = OnceLock::new();
static QUERIES: OnceLock<Vec<Vec<f32>>> = OnceLock::new();
fn rows() -> u64 {
    *ROWS.get().unwrap()
}
fn options() -> ServingOptions {
    ServingOptions {
        max_documents: rows() as usize,
        ..ServingOptions::m8()
    }
}
fn read_vectors(path: &str, expected: usize) -> Result<Vec<Vec<f32>>> {
    let raw = fs::read(path)?;
    if raw.len() != expected * 516 {
        return Err("unexpected fvecs size".into());
    }
    raw.as_chunks::<516>()
        .0
        .iter()
        .map(|row| {
            if u32::from_le_bytes(row[..4].try_into().unwrap()) != 128 {
                return Err("invalid fvecs dimension".into());
            }
            let v: Vec<_> = row[4..]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect();
            if v.iter().any(|f| !f.is_finite()) {
                return Err("nonfinite fvecs".into());
            }
            Ok(v)
        })
        .collect()
}
#[derive(Default)]
struct Observation {
    operations: Vec<Value>,
    lose_next_put: bool,
}
struct Observed {
    inner: S3Store,
    seen: Arc<Mutex<Observation>>,
}
impl Observed {
    fn note(&self, kind: &str, key: &str, bytes: usize, start: Instant) {
        self.seen
            .lock()
            .unwrap()
            .operations
            .push(json!({"kind":kind,"key":key,"bytes":bytes,"ms":ms(start.elapsed())}));
    }
}
impl ObjectStore for Observed {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let start = Instant::now();
        let result = self.inner.get(key);
        self.note(
            "get",
            key,
            result
                .as_ref()
                .ok()
                .and_then(|v| v.as_ref())
                .map_or(0, Vec::len),
            start,
        );
        result
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        let start = Instant::now();
        let result = self.inner.list();
        self.note("list", "", 0, start);
        result
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        let start = Instant::now();
        let result = self.inner.create(key, value);
        self.note("put", key, value.len(), start);
        if result.is_ok() && std::mem::take(&mut self.seen.lock().unwrap().lose_next_put) {
            return Err(glider::Error::Io(std::io::Error::other(
                "injected successful PUT response loss",
            )));
        }
        result
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        let start = Instant::now();
        let result = self.inner.remove(key);
        self.note("delete", key, 0, start);
        result
    }
    fn remove_many(&mut self, keys: &[String]) -> glider::Result<()> {
        let start = Instant::now();
        let result = self.inner.remove_many(keys);
        self.note("remove_many", &keys.join(","), 0, start);
        result
    }
}

const CONFIG: Config = Config {
    dimensions: 128,
    metric: Metric::SquaredEuclidean,
};
fn store(mode: &str) -> Result<Observed> {
    let endpoint = env::var("GLIDER_S3_ENDPOINT")?;
    if !endpoint.starts_with("http://127.0.0.1:") {
        return Err("M19 measurement requires disposable loopback MinIO".into());
    }
    Ok(Observed {
        inner: S3Store::open(
            AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_bucket_name(env::var("GLIDER_S3_BUCKET")?)
                .with_region("us-east-1")
                .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
                .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?)
                .with_allow_http(true)
                .with_client_options(
                    object_store::ClientOptions::new()
                        .with_allow_http(true)
                        .with_timeout(Duration::from_secs(10)),
                ),
            &format!("m19/{}/{mode}", env::var("GLIDER_M19_CASE")?),
        )?
        .with_read_limits(ReadLimits {
            objects: 128,
            object_bytes: 16 * 1024 * 1024,
            namespace_bytes: 32 * 1024 * 1024,
        })?,
        seen: Arc::default(),
    })
}
fn vector(id: u64, generation: u64) -> Vec<f32> {
    if id >= 1_000_000 {
        QUERIES.get().unwrap()[(id % 100) as usize].clone()
    } else {
        DATA.get().unwrap()[((id + generation * 137) % 10000) as usize].clone()
    }
}
fn put(id: u64, generation: u64) -> Mutation {
    Mutation::Put {
        id,
        vector: vector(id, generation),
        metadata: tags(id),
    }
}
fn tags(id: u64) -> BTreeMap<String, String> {
    if id.is_multiple_of(100) {
        BTreeMap::from([("selected".into(), "true".into())])
    } else {
        BTreeMap::new()
    }
}
fn batch(client: u64, round: u64) -> Vec<Mutation> {
    (0..100)
        .map(|n| {
            put(
                client * (rows() / 4) + round % (rows() / 4 / 100) * 100 + n,
                round + 1,
            )
        })
        .collect()
}
#[derive(Clone)]
struct Target(Client<Observed>);
impl Target {
    fn write(&self, request: Request) -> Result<Timed<Outcome>> {
        Ok(self.0.write(request)?.wait()?)
    }
    fn query(&self, q: Vec<f32>, filtered: bool) -> Result<Timed<QueryResult>> {
        Ok(self
            .0
            .query(
                q,
                10,
                if filtered {
                    vec![("selected".into(), "true".into())]
                } else {
                    vec![]
                },
            )?
            .wait()?)
    }
}
struct Write {
    client: u64,
    round: u64,
    result: Timed<Outcome>,
    e2e: f64,
    late: f64,
}
struct Query {
    id: u64,
    filtered: bool,
    result: Timed<QueryResult>,
    e2e: f64,
    late: f64,
}
fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.
}
fn wait_until(due: Instant, paced: bool) -> f64 {
    if !paced {
        return 0.;
    }
    if let Some(wait) = due.checked_duration_since(Instant::now()) {
        std::thread::sleep(wait);
    }
    ms(due.elapsed())
}
fn stats(mut samples: Vec<f64>) -> Value {
    let raw = samples.clone();
    samples.sort_by(f64::total_cmp);
    if samples.is_empty() {
        return json!({"count":0,"p95":0.,"max":0.,"raw":raw});
    }
    let n = samples.len();
    json!({"count":n,"p50":samples[(n-1)/2],"p95":samples[(n*95).div_ceil(100)-1],"max":samples[n-1],"raw":raw})
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
fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 7 {
        return Err(
            "usage: m19_capacity prepare|serve|verify ROWS ROUNDS paced|smoke DATA REPORT".into(),
        );
    }
    let row_count: u64 = args[2].parse()?;
    let rounds: u64 = args[3].parse()?;
    if ![2000, 5000, 10000].contains(&row_count) || !(1..=50).contains(&rounds) {
        return Err("outside fixed study bounds".into());
    }
    ROWS.set(row_count).unwrap();
    DATA.set(read_vectors(
        &format!("{}/siftsmall_base.fvecs", args[5]),
        10000,
    )?)
    .unwrap();
    QUERIES
        .set(read_vectors(
            &format!("{}/siftsmall_query.fvecs", args[5]),
            100,
        )?)
        .unwrap();
    let paced = args[4] == "paced";
    let mut rss_phases = BTreeMap::from([("inputs", rss())]);
    let raw = store("source")?;
    let metrics = raw.inner.metrics();
    let observed = raw.seen.clone();
    let start = Instant::now();
    let mut db = Db::open(raw, CONFIG, options())?;
    let cold_open_ms = ms(start.elapsed());
    rss_phases.insert("cold_open", rss());
    let cold_http = metrics.snapshot();
    let cold_operations = std::mem::take(&mut observed.lock().unwrap().operations);
    if args[1] == "prepare" {
        for start in (0..rows()).step_by(100) {
            db.apply_batch((start..start + 100).map(|id| put(id, 0)).collect())?;
        }
        db.maintain()?;
        db.close()?;
        fs::write(
            &args[6],
            serde_json::to_vec_pretty(
                &json!({"version":1,"phase":"prepare","rows":rows(),"peak_rss_bytes":rss(),"elapsed_ms":ms(start.elapsed()),"operations":observed.lock().unwrap().operations}),
            )?,
        )?;
        return Ok(());
    }
    if args[1] == "verify" {
        return verify(db, &args[6], observed, cold_open_ms, cold_operations);
    }
    if args[1] != "serve" {
        return Err("invalid phase".into());
    }
    for id in 0..rows() {
        let (v, m) = db.get(id).unwrap();
        assert_eq!(v, vector(id, 0));
        assert_eq!(*m, tags(id));
    }
    // Capacity rejection must occur before any maintenance or mutation I/O.
    let before = metrics.snapshot();
    assert!(db.apply_batch(vec![put(rows(), 0)]).is_err());
    assert_eq!(metrics.snapshot(), before);
    let initial_sequence = db.status().maintenance.sequence;
    let mode = "worker";
    let service = Service::start(db, Limits::default())?;
    let target = Target(service.client());
    let before = metrics.snapshot();
    let boundary = Arc::new(AtomicU64::new(initial_sequence));
    let barrier = Arc::new(Barrier::new(5));
    let began = Instant::now() + Duration::from_millis(100);
    let mut threads = Vec::new();
    for client in 0..4 {
        let (target, boundary, barrier) = (target.clone(), boundary.clone(), barrier.clone());
        threads.push(std::thread::spawn(
            move || -> Result<(Vec<Write>, Vec<Query>)> {
                let mut writes = Vec::new();
                let mut queries = Vec::new();
                barrier.wait();
                for round in 0..rounds {
                    let due = began + Duration::from_secs(round);
                    let late = wait_until(due, paced);
                    let mut nonce = [0; 16];
                    nonce[..8].copy_from_slice(&u64::to_le_bytes(client));
                    nonce[8..].copy_from_slice(&round.to_le_bytes());
                    let request = Request {
                        id: RequestId {
                            boundary: boundary.load(Ordering::Acquire),
                            nonce,
                        },
                        conditions: vec![],
                        mutations: batch(client, round),
                    };
                    let start = Instant::now();
                    let result = target.write(request)?;
                    let e2e = ms(start.elapsed());
                    assert!(
                        result.value.conflict.is_none(),
                        "dataset=SIFTsmall client={client} round={round}"
                    );
                    boundary.fetch_max(result.value.sequence, Ordering::Release);
                    writes.push(Write {
                        client,
                        round,
                        result,
                        e2e,
                        late,
                    });
                    for tick in 0..10 {
                        let late = wait_until(due + Duration::from_millis(tick * 100), paced);
                        let id = client * rounds * 10 + round * 10 + tick;
                        let filtered = id.is_multiple_of(2);
                        let q = vector(1_000_000 + id, 0);
                        let start = Instant::now();
                        let result = target.query(q, filtered)?;
                        let e2e = ms(start.elapsed());
                        queries.push(Query {
                            id,
                            filtered,
                            result,
                            e2e,
                            late,
                        });
                    }
                }
                Ok((writes, queries))
            },
        ));
    }
    barrier.wait();
    let mut writes = Vec::new();
    let mut queries = Vec::new();
    for thread in threads {
        let (w, q) = thread.join().map_err(|_| "client panic")??;
        writes.extend(w);
        queries.extend(q);
    }
    let elapsed = if paced {
        began.elapsed().as_secs_f64()
    } else {
        (began - Duration::from_millis(100)).elapsed().as_secs_f64()
    };
    drop(target);
    service.shutdown(Shutdown::Drain)?;
    rss_phases.insert("serving", rss());
    let after = metrics.snapshot();
    writes.sort_by_key(|w| w.result.value.sequence);
    queries.sort_by_key(|q| (q.result.value.sequence, q.id));
    let mut model: BTreeMap<_, _> = (0..rows()).map(|id| (id, vector(id, 0))).collect();
    let mut applied = 0;
    for query in &queries {
        while applied < writes.len()
            && writes[applied].result.value.sequence <= query.result.value.sequence
        {
            for mutation in batch(writes[applied].client, writes[applied].round) {
                if let Mutation::Put { id, vector, .. } = mutation {
                    model.insert(id, vector);
                }
            }
            applied += 1;
        }
        let q = vector(1_000_000 + query.id, 0);
        let mut expected: Vec<_> = model
            .iter()
            .filter(|(id, _)| !query.filtered || id.is_multiple_of(100))
            .map(|(&id, v)| Neighbor {
                id,
                distance: v
                    .iter()
                    .zip(&q)
                    .map(|(&a, &b)| (f64::from(a) - f64::from(b)).powi(2))
                    .sum(),
            })
            .collect();
        expected.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        expected.truncate(10);
        assert_eq!(
            query.result.value.neighbors, expected,
            "dataset=SIFTsmall query={} boundary={}",
            query.id, query.result.value.sequence
        );
    }
    for w in &writes[applied..] {
        for mutation in batch(w.client, w.round) {
            if let Mutation::Put { id, vector, .. } = mutation {
                model.insert(id, vector);
            }
        }
    }
    rss_phases.insert("oracle", rss());
    let recovered_store = store("source")?;
    let recovery_metrics = recovered_store.inner.metrics();
    let start = Instant::now();
    let recovered = Db::open(recovered_store, CONFIG, options())?;
    let recovery_ms = ms(start.elapsed());
    assert_eq!(
        recovered.status().maintenance.sequence,
        initial_sequence + rounds * 4
    );
    for (id, vector) in model {
        let (actual, metadata) = recovered.get(id).unwrap();
        assert_eq!(actual, vector, "dataset=SIFTsmall recovered id={id}");
        assert_eq!(*metadata, tags(id));
    }
    recovered.close()?;
    rss_phases.insert("warm_reopen", rss());
    let write_ms = stats(writes.iter().map(|w| w.e2e).collect());
    let query_ms = stats(queries.iter().map(|q| q.e2e).collect());
    let queue_ms = stats(
        writes
            .iter()
            .map(|w| ms(w.result.queue_wait))
            .chain(queries.iter().map(|q| ms(q.result.queue_wait)))
            .collect(),
    );
    let commit_ms = stats(writes.iter().map(|w| ms(w.result.execution)).collect());
    let maintenance_ms = stats(
        writes
            .iter()
            .filter(|w| !w.result.maintenance.is_zero())
            .map(|w| ms(w.result.maintenance))
            .collect(),
    );
    let throughput = rounds as f64 * 400. / elapsed;
    let filtered_ms = stats(
        queries
            .iter()
            .filter(|q| q.filtered)
            .map(|q| q.e2e)
            .collect(),
    );
    let unfiltered_ms = stats(
        queries
            .iter()
            .filter(|q| !q.filtered)
            .map(|q| q.e2e)
            .collect(),
    );
    let write_queue_ms = stats(writes.iter().map(|w| ms(w.result.queue_wait)).collect());
    let query_queue_ms = stats(queries.iter().map(|q| ms(q.result.queue_wait)).collect());
    let passed = write_ms["p95"].as_f64().unwrap() <= 150.
        && filtered_ms["p95"].as_f64().unwrap() <= 50.
        && unfiltered_ms["p95"].as_f64().unwrap() <= 50.
        && write_queue_ms["p95"].as_f64().unwrap() <= 75.
        && query_queue_ms["p95"].as_f64().unwrap() <= 75.
        && commit_ms["p95"].as_f64().unwrap() <= 100.
        && maintenance_ms["p95"].as_f64().unwrap() <= 100.
        && throughput >= 350.
        && rss() <= 64 * 1024 * 1024
        && cold_open_ms <= 1000.
        && cold_http.get <= 64
        && cold_http.list <= 4
        && cold_operations
            .iter()
            .filter(|o| o["kind"] == "get")
            .map(|o| o["bytes"].as_u64().unwrap())
            .sum::<u64>()
            <= 16 * 1024 * 1024
        && (after.put - before.put) <= rounds * 5
        && (after.delete - before.delete) <= rounds * 5 + 2
        && (after.list - before.list) <= rounds
        && (after.request_body_bytes - before.request_body_bytes) <= rounds * 2 * 1024 * 1024;
    fs::write(
        std::path::Path::new(&args[6]).with_file_name("profile.json"),
        serde_json::to_vec_pretty(&json!({
            "cold_open_ms":cold_open_ms,"cold_operations":cold_operations,"cold_gets":cold_http.get,"cold_lists":cold_http.list,"rss_phases":rss_phases,
            "operations":observed.lock().unwrap().operations
        }))?,
    )?;
    fs::write(
        &args[6],
        serde_json::to_vec_pretty(
            &json!({"version":1,"mode":mode,"backend":"minio","rows":rows(),"dimensions":128,"seed":42,
                "generator":"siftsmall-prefix-rotate137-v1","rounds":rounds,"paced":paced,"oracle_checks":queries.len(),"recovery_passed":true,
                "limits":{"commands":8,"encoded_bytes":327680,"write_p95_ms":150,"query_p95_ms":50,"queue_p95_ms":75,"commit_p95_ms":100,"maintenance_p95_ms":100,"min_mutations_per_second":350,"rss_bytes":67108864},
                "performance_accepted":paced && passed,"elapsed_seconds":elapsed,"logical_mutations_per_second":throughput,"peak_rss_bytes":rss(),
                "write_ms":write_ms,"query_ms":query_ms,"filtered_query_ms":stats(queries.iter().filter(|q|q.filtered).map(|q|q.e2e).collect()),
                "unfiltered_query_ms":stats(queries.iter().filter(|q|!q.filtered).map(|q|q.e2e).collect()),"queue_ms":queue_ms,"write_queue_ms":write_queue_ms,"query_queue_ms":query_queue_ms,"commit_ms":commit_ms,"maintenance_ms":maintenance_ms,
                "arrival_lateness_ms":stats(writes.iter().map(|w|w.late).chain(queries.iter().map(|q|q.late)).collect()),
                "write_order":writes.iter().map(|w|json!({"sequence":w.result.value.sequence,"client":w.client,"round":w.round})).collect::<Vec<_>>(),
                "recovery_ms":recovery_ms,"recovery_gets":recovery_metrics.snapshot().get,"recovery_lists":recovery_metrics.snapshot().list,
                "http":{"get":after.get-before.get,"list":after.list-before.list,"put":after.put-before.put,"delete":after.delete-before.delete,
                    "request_body_bytes":after.request_body_bytes-before.request_body_bytes,"http_errors":after.http_errors-before.http_errors,"transport_errors":after.transport_errors-before.transport_errors}
            }),
        )?,
    )?;
    println!(
        "{mode}: {} queries checked; write p95={} ms query p95={} ms accepted={}",
        queries.len(),
        write_ms["p95"],
        query_ms["p95"],
        paced && passed
    );
    Ok(())
}

fn check(db: &Db, model: &BTreeMap<u64, Vec<f32>>) {
    assert_eq!(db.status().documents, model.len());
    for (&id, v) in model {
        let (actual, m) = db.get(id).unwrap();
        assert_eq!(actual, v);
        assert_eq!(*m, tags(id));
    }
    for id in 0..10 {
        let q = vector(1_000_000 + id, 0);
        for filtered in [false, true] {
            let mut expected: Vec<_> = model
                .iter()
                .filter(|(id, _)| !filtered || id.is_multiple_of(100))
                .map(|(&id, v)| Neighbor {
                    id,
                    distance: v
                        .iter()
                        .zip(&q)
                        .map(|(&a, &b)| (f64::from(a) - f64::from(b)).powi(2))
                        .sum(),
                })
                .collect();
            expected.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
            expected.truncate(10);
            let f = if filtered {
                vec![("selected", "true")]
            } else {
                vec![]
            };
            assert_eq!(db.query(&q, 10, &f, SearchMode::Exact).unwrap(), expected);
        }
    }
}
fn verify(
    mut db: Db,
    report: &str,
    observed: Arc<Mutex<Observation>>,
    cold_ms: f64,
    cold_operations: Vec<Value>,
) -> Result<()> {
    let serving: Value = serde_json::from_slice(&fs::read(report)?)?;
    let mut model: BTreeMap<_, _> = (0..rows()).map(|id| (id, vector(id, 0))).collect();
    for w in serving["write_order"]
        .as_array()
        .ok_or("missing write order")?
    {
        for m in batch(w["client"].as_u64().unwrap(), w["round"].as_u64().unwrap()) {
            if let Mutation::Put { id, vector, .. } = m {
                model.insert(id, vector);
            }
        }
    }
    check(&db, &model);
    let start = Instant::now();
    let backup = store("backup")?;
    let backup_seen = backup.seen.clone();
    db.backup_to(backup)?;
    let backup_ms = ms(start.elapsed());
    db.close()?;
    let source = store("backup")?;
    let source_seen = source.seen.clone();
    let destination = store("restore")?;
    let restore_seen = destination.seen.clone();
    let start = Instant::now();
    glider::recovery::stage_isolated_namespace(&source, destination, CONFIG)?;
    drop(source);
    let destination = store("restore")?;
    let restored_seen = destination.seen.clone();
    let mut restored = Db::open(destination, CONFIG, options())?;
    let restore_ms = ms(start.elapsed());
    check(&restored, &model);
    let mut operations = observed.lock().unwrap().operations.clone();
    for seen in [&backup_seen, &source_seen, &restore_seen, &restored_seen] {
        operations.extend(seen.lock().unwrap().operations.clone());
    }
    let get_bytes: u64 = operations
        .iter()
        .filter(|o| o["kind"] == "get")
        .map(|o| o["bytes"].as_u64().unwrap())
        .sum();
    let put_bytes: u64 = operations
        .iter()
        .filter(|o| o["kind"] == "put")
        .map(|o| o["bytes"].as_u64().unwrap())
        .sum();
    let count = |kind: &str| operations.iter().filter(|o| o["kind"] == kind).count();
    let accepted = backup_ms + restore_ms <= 2500.
        && get_bytes <= 64 * 1024 * 1024
        && put_bytes <= 32 * 1024 * 1024
        && count("get") <= 32
        && count("put") <= 16
        && count("list") <= 12
        && cold_ms <= 1000.
        && rss() <= 64 * 1024 * 1024;
    // Failed backup publication must not poison the source or promote a partial destination.
    let failed_backup = store("failed-backup")?;
    failed_backup.seen.lock().unwrap().lose_next_put = true;
    assert!(restored.backup_to(failed_backup).is_err());
    assert!(glider::Database::open(store("failed-backup")?, CONFIG).is_err());
    assert!(!restored.status().recovery_required);
    check(&restored, &model);
    // Lose the response AFTER MinIO published a whole retry record at this size.
    let request = Request {
        id: restored.request_id()?,
        conditions: vec![],
        mutations: vec![put(0, 999)],
    };
    restored_seen.lock().unwrap().lose_next_put = true;
    assert!(restored.apply_request(request.clone()).is_err());
    assert!(restored.status().recovery_required);
    drop(restored);
    glider::recovery::stage_isolated_namespace(&store("restore")?, store("takeover")?, CONFIG)?;
    let mut taken = Db::open(store("takeover")?, CONFIG, options())?;
    let outcome = match taken.lookup_request(request.id)? {
        glider::retry::Lookup::Retained(outcome) => outcome,
        _ => return Err("lost receipt after takeover".into()),
    };
    assert_eq!(taken.apply_request(request)?, outcome);
    model.insert(0, vector(0, 999));
    check(&taken, &model);
    taken.close()?;
    let mut oversized = store("source")?;
    oversized.inner = oversized.inner.with_read_limits(ReadLimits {
        objects: 128,
        object_bytes: 4096,
        namespace_bytes: 8192,
    })?;
    let metrics = oversized.inner.metrics();
    let error = glider::Database::open(oversized, CONFIG)
        .err()
        .ok_or("oversized open succeeded")?;
    assert!(error.to_string().contains("S3 read limit"));
    assert_eq!(metrics.snapshot().get, 0);
    let path = std::path::Path::new(report).with_file_name("verify.json");
    let accepted = accepted && rss() <= 64 * 1024 * 1024;
    fs::write(
        path,
        serde_json::to_vec_pretty(
            &json!({"version":1,"phase":"verify","rows":rows(),"cold_open_ms":cold_ms,"cold_operations":cold_operations,
        "backup_ms":backup_ms,"restore_ms":restore_ms,"backup_restore_get_bytes":get_bytes,"backup_restore_put_bytes":put_bytes,"operations":operations,
        "backup_restore_accepted":accepted,"state_oracle_passed":true,"lost_ack_takeover_passed":true,"failed_backup_passed":true,"oversized_open_rejected_before_get":true,"peak_rss_bytes":rss()}),
        )?,
    )?;
    println!("M19 final recovery/backup/failure checks passed; resource acceptance={accepted}");
    Ok(())
}
