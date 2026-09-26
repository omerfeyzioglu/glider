//! One restartable epoch of the M8 serving soak. The supervisor owns MinIO.
use glider::{
    recovery::stage_isolated_namespace,
    serving::{SearchMode, ServingOptions, SingleMachine},
    store::{
        s3::{AmazonS3Builder, S3Store},
        ObjectStore,
    },
    Config, Metric, Mutation, Neighbor,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    env,
    rc::Rc,
    time::{Duration, Instant},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Data = BTreeMap<u64, Vec<f32>>;
#[derive(Clone, Copy, Default)]
struct Counts {
    mutation_bytes: u64,
    maintenance_bytes: u64,
    gets: u64,
    lists: u64,
}
struct Observed {
    inner: S3Store,
    counts: Rc<Cell<Counts>>,
    fail_remove: Rc<Cell<bool>>,
}
impl ObjectStore for Observed {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let mut c = self.counts.get();
        c.gets += 1;
        self.counts.set(c);
        self.inner.get(key)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        let mut c = self.counts.get();
        c.lists += 1;
        self.counts.set(c);
        self.inner.list()
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        self.inner.create(key, value)?;
        let mut c = self.counts.get();
        if key.starts_with("mutation-") {
            c.mutation_bytes += value.len() as u64;
        }
        if key.starts_with("compacted") {
            c.maintenance_bytes += value.len() as u64;
        }
        self.counts.set(c);
        Ok(())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)?;
        if self.fail_remove.replace(false) {
            return Err(std::io::Error::other("injected lost cleanup acknowledgement").into());
        }
        Ok(())
    }
}
fn store(prefix: &str) -> Result<S3Store> {
    Ok(S3Store::open(
        AmazonS3Builder::new()
            .with_endpoint(env::var("GLIDER_S3_ENDPOINT")?)
            .with_bucket_name(env::var("GLIDER_S3_BUCKET")?)
            .with_region("us-east-1")
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            }),
        prefix,
    )?)
}
fn vector(state: &mut u64) -> Vec<f32> {
    (0..64)
        .map(|_| {
            *state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            z ^= z >> 31;
            ((z >> 40) as u32 as f32) * (1.0 / 8_388_608.0) - 1.0
        })
        .collect()
}
fn metadata(id: u64) -> BTreeMap<String, String> {
    if id.is_multiple_of(100) {
        BTreeMap::from([("selected".into(), "true".into())])
    } else {
        BTreeMap::new()
    }
}
fn put(id: u64, v: Vec<f32>) -> Mutation {
    Mutation::Put {
        id,
        vector: v,
        metadata: metadata(id),
    }
}
fn updates(data: &Data, cycle: u64) -> Vec<Mutation> {
    let ids: Vec<_> = data.keys().copied().collect();
    let mut rng = 42 ^ cycle.wrapping_mul(0xa0761d6478bd642f);
    let mut batch: Vec<_> = ids[..200]
        .iter()
        .map(|&id| put(id, vector(&mut rng)))
        .collect();
    batch.extend(ids[200..300].iter().map(|&id| Mutation::Delete { id }));
    batch.extend((2000 + cycle * 100..2100 + cycle * 100).map(|id| put(id, vector(&mut rng))));
    batch
}
fn apply_model(data: &mut Data, batch: &[Mutation]) {
    for m in batch {
        match m {
            Mutation::Put { id, vector, .. } => {
                data.insert(*id, vector.clone());
            }
            Mutation::Delete { id } => {
                data.remove(id);
            }
        }
    }
}
fn oracle(data: &Data, q: &[f32], filtered: bool) -> Vec<Neighbor> {
    let mut all: Vec<_> = data
        .iter()
        .filter(|(id, _)| !filtered || id.is_multiple_of(100))
        .map(|(&id, v)| Neighbor {
            id,
            distance: v
                .iter()
                .zip(q)
                .map(|(&a, &b)| {
                    let d = f64::from(a) - f64::from(b);
                    d * d
                })
                .sum(),
        })
        .collect();
    all.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
    all.truncate(10);
    all
}
fn check(db: &SingleMachine<Observed>, data: &Data) -> Result<()> {
    for (&id, vector) in data {
        let (actual, tags) = db.get(id).ok_or("missing acknowledged document")?;
        assert_eq!(actual, vector, "seed=42 recovered vector id={id}");
        assert_eq!(*tags, metadata(id), "seed=42 recovered metadata id={id}");
    }
    // k=all verifies every ID, distance and metadata after recovery, not just top-k.
    let q = vec![0.; 64];
    for filtered in [false, true] {
        let filter = if filtered {
            vec![("selected", "true")]
        } else {
            vec![]
        };
        let actual = db.query(&q, 2000, &filter, SearchMode::Exact)?;
        let expected: Vec<_> = data
            .iter()
            .filter(|(id, _)| !filtered || id.is_multiple_of(100))
            .map(|(&id, v)| Neighbor {
                id,
                distance: v.iter().map(|&x| f64::from(x) * f64::from(x)).sum(),
            })
            .collect();
        let map: BTreeMap<_, _> = actual.iter().map(|n| (n.id, n.distance)).collect();
        assert_eq!(map.len(), expected.len(), "seed=42 recovery row count");
        for n in expected {
            assert_eq!(
                map.get(&n.id),
                Some(&n.distance),
                "seed=42 recovery id={}",
                n.id
            );
        }
    }
    Ok(())
}
fn rss() -> u64 {
    let mut u = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, u.as_mut_ptr()) },
        0
    );
    let v = unsafe { u.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        v
    } else {
        v * 1024
    }
}
fn timings(mut values: Vec<u64>) -> serde_json::Value {
    let raw = values.clone();
    values.sort_unstable();
    let n = values.len();
    json!({"count":n,"p50_ns":values[(n-1)/2],"p95_ns":values[(n-1)*95/100],"p99_ns":values[(n-1)*99/100],"max_ns":values[n-1],"raw_ns":raw})
}
fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    let start_cycle: u64 = args[1].parse()?;
    let cycles: u64 = args[2].parse()?;
    assert!(cycles > 0);
    let mut prefix = env::var("GLIDER_S3_NAMESPACE")?;
    let config = Config {
        dimensions: 64,
        metric: Metric::SquaredEuclidean,
    };
    let counts = Rc::new(Cell::new(Counts::default()));
    let fault = Rc::new(Cell::new(false));
    let opens = Rc::new(RefCell::new(Vec::new()));
    let open = |prefix: &str| -> Result<SingleMachine<Observed>> {
        let inner = store(prefix)?;
        let http = inner.metrics();
        let before = counts.get();
        let t = Instant::now();
        let db = SingleMachine::open(
            Observed {
                inner,
                counts: counts.clone(),
                fail_remove: fault.clone(),
            },
            config,
            ServingOptions::m8(),
        )?;
        let used = counts.get();
        let h = http.snapshot();
        opens.borrow_mut().push(json!({"ns":t.elapsed().as_nanos(),"gets":used.gets-before.gets,"lists":used.lists-before.lists,"http_gets":h.get,"http_lists":h.list}));
        Ok(db)
    };
    let mut data = Data::new();
    let mut rng = 42;
    for id in 0..2000 {
        data.insert(id, vector(&mut rng));
    }
    for cycle in 0..start_cycle {
        let batch = updates(&data, cycle);
        apply_model(&mut data, &batch);
    }
    let mut db = open(&prefix)?;
    if start_cycle == 0 {
        for chunk in data.iter().collect::<Vec<_>>().chunks(100) {
            db.apply_batch(chunk.iter().map(|(&id, v)| put(id, v.to_vec())).collect())?;
        }
    }
    check(&db, &data)?;
    assert!(db
        .query(&vec![0.; 64], 10, &[], SearchMode::Approximate)
        .is_err());
    let start = Instant::now();
    let mut query_ns = [Vec::new(), Vec::new()];
    let mut batch_ns = Vec::new();
    let mut events = Vec::new();
    let mut max_objects = 0;
    for cycle in start_cycle..start_cycle + cycles {
        let cycle_start = Instant::now();
        let mut rng = 42 ^ 0xd1b54a32d192ed03 ^ cycle.wrapping_mul(0x9e3779b97f4a7c15);
        for i in 0..100 {
            let q = vector(&mut rng);
            let filtered = i % 10 < 4;
            let filter = if filtered {
                vec![("selected", "true")]
            } else {
                vec![]
            };
            let expected = oracle(&data, &q, filtered);
            let t = Instant::now();
            let actual = db.query(&q, 10, &filter, SearchMode::Exact)?;
            query_ns[usize::from(filtered)].push(t.elapsed().as_nanos() as u64);
            assert_eq!(actual, expected, "seed=42 cycle={cycle} query={i}");
        }
        let mut batch = updates(&data, cycle).into_iter();
        loop {
            let chunk: Vec<_> = batch.by_ref().take(100).collect();
            if chunk.is_empty() {
                break;
            }
            let next: Vec<_> = chunk
                .iter()
                .map(|m| serde_json::to_vec(m).unwrap())
                .collect();
            let t = Instant::now();
            db.apply_batch(chunk)?;
            batch_ns.push(t.elapsed().as_nanos() as u64);
            let applied: Vec<Mutation> = next
                .iter()
                .map(|b| serde_json::from_slice(b).unwrap())
                .collect();
            apply_model(&mut data, &applied);
        }
        if cycle == 30 {
            fault.set(true);
            assert!(db.maintain().is_err());
            assert!(db.status().recovery_required);
            assert_eq!(db.status().storage_errors, 1);
            check(&db, &data)?;
            drop(db);
            let target = format!("{prefix}-takeover");
            let t = Instant::now();
            stage_isolated_namespace(&store(&prefix)?, store(&target)?, config)?;
            db = open(&target)?;
            check(&db, &data)?;
            events
                .push(json!({"event":"interrupted_cleanup_takeover","ns":t.elapsed().as_nanos()}));
            prefix = target;
            db.maintain()?;
        }
        if cycle == 60 {
            let backup = format!("{prefix}-backup");
            db.backup_to(store(&backup)?)?;
            let restored = format!("{prefix}-restored");
            let t = Instant::now();
            stage_isolated_namespace(&store(&backup)?, store(&restored)?, config)?;
            let next = open(&restored)?;
            check(&next, &data)?;
            events.push(json!({"event":"backup_restore","ns":t.elapsed().as_nanos()}));
            db.close()?;
            db = next;
            prefix = restored;
        }
        let status = db.status();
        max_objects = max_objects.max(status.maintenance.visible_objects);
        assert!(max_objects <= 128);
        assert!(status.maintenance.tail_objects <= 24);
        assert!(rss() <= 64 * 1024 * 1024, "M8 RSS exceeded");
        if cycle % 30 == 0 {
            eprintln!(
                "cycle={cycle} sequence={} objects={} rss_mib={:.1}",
                status.maintenance.sequence,
                status.maintenance.visible_objects,
                rss() as f64 / 1048576.
            );
        }
        if env::var_os("GLIDER_SOAK_UNPACED").is_none() {
            std::thread::sleep(Duration::from_secs(1).saturating_sub(cycle_start.elapsed()));
        }
    }
    check(&db, &data)?;
    let sequence = db.status().maintenance.sequence;
    db.close()?;
    let c = counts.get();
    let mut hash = Sha256::new();
    for file in [
        "Cargo.toml",
        "Cargo.lock",
        "src/serving.rs",
        "src/lib.rs",
        "examples/m13_soak.rs",
    ] {
        hash.update(std::fs::read(file)?);
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema_version":1,"seed":42,"start_cycle":start_cycle,"cycles":cycles,"active_prefix":prefix,"sequence":sequence,
        "elapsed_seconds":start.elapsed().as_secs_f64(),"query_mix":"60% unfiltered exact, 40% filtered exact; ANN disabled by M12",
        "unfiltered":timings(std::mem::take(&mut query_ns[0])),"filtered":timings(std::mem::take(&mut query_ns[1])),"batch":timings(batch_ns),
        "mutation_payload_bytes":c.mutation_bytes,"maintenance_payload_bytes":c.maintenance_bytes,"max_visible_engine_objects":max_objects,
        "peak_process_rss_bytes":rss(),"opens":*opens.borrow(),"events":events,"source_sha256":format!("{:x}",hash.finalize())})
        )?
    );
    Ok(())
}
