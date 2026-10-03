//! One bounded MinIO comparison phase; run through tools/m16_benchmark.py.
use glider::{
    admission::{Client, Limits, QueryResult, Service, Shutdown, Timed},
    retry::{Outcome, Request, RequestId},
    serving::{SearchMode, ServingOptions, SingleMachine},
    store::s3::{AmazonS3Builder, S3Store},
    Config, Metric, Mutation, Neighbor,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    env, fs,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Barrier, Mutex,
    },
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Db = SingleMachine<S3Store>;
const CONFIG: Config = Config {
    dimensions: 64,
    metric: Metric::SquaredEuclidean,
};
fn store(mode: &str) -> Result<S3Store> {
    let endpoint = env::var("GLIDER_S3_ENDPOINT")?;
    if !endpoint.starts_with("http://127.0.0.1:") {
        return Err("M16 measurement requires disposable loopback MinIO".into());
    }
    Ok(S3Store::open(
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
        &format!("m16/{mode}"),
    )?)
}
fn vector(id: u64, generation: u64) -> Vec<f32> {
    let mut state =
        42 ^ id.wrapping_mul(0xa0761d6478bd642f) ^ generation.wrapping_mul(0xe7037ed1a0b428db);
    (0..64)
        .map(|_| {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^= z >> 31;
            (z >> 40) as u32 as f32 / 8_388_608. - 1.
        })
        .collect()
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
        .map(|n| put(client * 500 + round % 5 * 100 + n, round + 1))
        .collect()
}
#[derive(Clone)]
enum Target {
    Mutex(Arc<Mutex<Db>>),
    Worker(Client<glider::serving::SingleMachine<S3Store>>),
}
impl Target {
    fn write(&self, request: Request) -> Result<Timed<Outcome>> {
        match self {
            Self::Worker(client) => Ok(client.write(request)?.wait()?),
            Self::Mutex(db) => {
                let start = Instant::now();
                let mut db = db.lock().unwrap();
                let queue_wait = start.elapsed();
                let before = db.status().maintenance_time;
                let start = Instant::now();
                let value = db.apply_request(request)?;
                let elapsed = start.elapsed();
                let maintenance = db.status().maintenance_time - before;
                Ok(Timed {
                    value,
                    queue_wait,
                    execution: elapsed.saturating_sub(maintenance),
                    maintenance,
                })
            }
        }
    }
    fn query(&self, q: Vec<f32>, filtered: bool) -> Result<Timed<QueryResult>> {
        match self {
            Self::Worker(client) => Ok(client
                .query(
                    q,
                    10,
                    if filtered {
                        vec![("selected".into(), "true".into())]
                    } else {
                        vec![]
                    },
                )?
                .wait()?),
            Self::Mutex(db) => {
                let start = Instant::now();
                let db = db.lock().unwrap();
                let queue_wait = start.elapsed();
                let start = Instant::now();
                let filter = if filtered {
                    vec![("selected", "true")]
                } else {
                    vec![]
                };
                let neighbors = db.query(&q, 10, &filter, SearchMode::Exact)?;
                Ok(Timed {
                    value: QueryResult {
                        sequence: db.status().maintenance.sequence,
                        neighbors,
                        hits: Vec::new(),
                        remote_reads: 0,
                        remote_bytes: 0,
                        mode: glider::admission::QueryMode::ExactScan,
                    },
                    queue_wait,
                    execution: start.elapsed(),
                    maintenance: Duration::ZERO,
                })
            }
        }
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
    if args.len() != 5 {
        return Err("usage: m16_concurrency mutex|worker ROUNDS paced|smoke REPORT".into());
    }
    let mode = &args[1];
    let rounds: u64 = args[2].parse()?;
    let paced = args[3] == "paced";
    if !["mutex", "worker"].contains(&mode.as_str()) || !(1..=50).contains(&rounds) {
        return Err("invalid bounded mode/rounds".into());
    }
    let raw = store(mode)?;
    let metrics = raw.metrics();
    let mut db = Db::open(raw, CONFIG, ServingOptions::m8())?;
    for start in (0..2000).step_by(100) {
        db.apply_batch((start..start + 100).map(|id| put(id, 0)).collect())?;
    }
    db.maintain()?;
    let initial_sequence = db.status().maintenance.sequence;
    let mut service = None;
    let target = if mode == "mutex" {
        Target::Mutex(Arc::new(Mutex::new(db)))
    } else {
        let worker = Service::start(db, Limits::default())?;
        let client = worker.client();
        service = Some(worker);
        Target::Worker(client)
    };
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
                        "seed=42 client={client} round={round}"
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
    match target {
        Target::Mutex(db) => Arc::try_unwrap(db)
            .map_err(|_| "baseline still shared")?
            .into_inner()
            .unwrap()
            .close()?,
        Target::Worker(_) => service.take().unwrap().shutdown(Shutdown::Drain)?,
    }
    let after = metrics.snapshot();
    writes.sort_by_key(|w| w.result.value.sequence);
    queries.sort_by_key(|q| (q.result.value.sequence, q.id));
    let mut model: BTreeMap<_, _> = (0..2000).map(|id| (id, vector(id, 0))).collect();
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
            "seed=42 query={} boundary={}",
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
    let recovered_store = store(mode)?;
    let recovery_metrics = recovered_store.metrics();
    let start = Instant::now();
    let recovered = Db::open(recovered_store, CONFIG, ServingOptions::m8())?;
    let recovery_ms = ms(start.elapsed());
    assert_eq!(
        recovered.status().maintenance.sequence,
        initial_sequence + rounds * 4
    );
    for (id, vector) in model {
        let (actual, metadata) = recovered.get(id).unwrap();
        assert_eq!(actual, vector, "seed=42 recovered id={id}");
        assert_eq!(*metadata, tags(id));
    }
    recovered.close()?;
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
        && recovery_ms <= 500.
        && recovery_metrics.snapshot().get <= 64
        && recovery_metrics.snapshot().list <= 4;
    fs::write(
        &args[4],
        serde_json::to_vec_pretty(
            &json!({"version":1,"mode":mode,"backend":"minio","rows":2000,"dimensions":64,"seed":42,
                "generator":"splitmix64-id-generation-v1","rounds":rounds,"paced":paced,"oracle_checks":queries.len(),"recovery_passed":true,
                "limits":{"commands":8,"encoded_bytes":327680,"write_p95_ms":150,"query_p95_ms":50,"queue_p95_ms":75,"commit_p95_ms":100,"maintenance_p95_ms":100,"min_mutations_per_second":350,"rss_bytes":67108864},
                "performance_accepted":paced && rounds == 50 && passed,"elapsed_seconds":elapsed,"logical_mutations_per_second":throughput,"peak_rss_bytes":rss(),
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
        paced && rounds == 50 && passed
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn declared_four_client_burst_fits_the_admission_byte_budget() {
        let largest_burst = (0..50)
            .map(|round| {
                (0..4)
                    .map(|client| {
                        let request = Request {
                            id: RequestId {
                                boundary: 220,
                                nonce: [255; 16],
                            },
                            conditions: vec![],
                            mutations: batch(client, round),
                        };
                        serde_json::to_vec(&request).unwrap().len()
                    })
                    .sum::<usize>()
            })
            .max()
            .unwrap();
        assert!(
            largest_burst <= Limits::default().bytes,
            "fixed-workload burst={largest_burst} bytes, limit={}",
            Limits::default().bytes
        );
    }
}
