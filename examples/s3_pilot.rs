//! Small provider probe; use tools/s3_pilot.py for Free-plan checks and cleanup.
#[path = "support/pilot_transport.rs"]
mod transport;
use glider::{
    recovery::stage_isolated_namespace,
    serving::{SearchMode, ServingOptions, SingleMachine},
    store::{
        s3::{AmazonS3Builder, S3Store},
        ObjectStore,
    },
    Config, Metric, Mutation, Neighbor,
};
use object_store::ClientOptions;
use serde_json::json;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    env, fs,
    time::{Duration, Instant},
};
use transport::Budget;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type Rows = BTreeMap<u64, Vec<f32>>;
const NAMES: [&str; 5] = ["objects", "live", "takeover", "backup", "restored"];
const CONFIG: Config = Config {
    dimensions: 64,
    metric: Metric::SquaredEuclidean,
};

struct Probe {
    budget: Budget,
    root: String,
    operations: RefCell<Vec<serde_json::Value>>,
}
impl Probe {
    fn timed<T>(&self, operation: &str, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let before = self.budget.snapshot();
        let started = Instant::now();
        let result = f();
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.;
        let after = self.budget.snapshot();
        self.operations.borrow_mut().push(json!({
            "operation": operation, "elapsed_ms": elapsed_ms, "succeeded": result.is_ok(),
            "requests": after.requests - before.requests,
            "request_bytes": after.request_body_bytes - before.request_body_bytes,
            "response_bytes": after.response_body_bytes - before.response_body_bytes,
        }));
        result
    }
    fn batch(&self, db: &mut SingleMachine<S3Store>, mutations: Vec<Mutation>) -> Result<()> {
        let before = db.status().maintenance_time;
        let result = self.timed("batch", || Ok(db.apply_batch(mutations)?));
        self.operations.borrow_mut().last_mut().unwrap()["maintenance_ms"] =
            json!((db.status().maintenance_time - before).as_secs_f64() * 1000.);
        result
    }
    fn store(&self, name: &str) -> Result<S3Store> {
        assert!(NAMES.contains(&name));
        let endpoint = env::var("GLIDER_S3_ENDPOINT")?;
        let local = endpoint.starts_with("http://127.0.0.1:");
        if !local && !endpoint.starts_with("https://") {
            return Err("HTTPS required".into());
        }
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(env::var("GLIDER_S3_BUCKET")?)
            .with_region(env::var("GLIDER_S3_REGION")?)
            .with_endpoint(endpoint)
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?)
            .with_allow_http(local)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(local)
                    .with_timeout(Duration::from_secs(10)),
            );
        if let Ok(token) = env::var("AWS_SESSION_TOKEN") {
            builder = builder.with_token(token);
        }
        Ok(S3Store::with_connector(
            builder,
            &format!("{}/{name}", self.root),
            self.budget.clone(),
        )?)
    }
    fn open(&self, name: &str) -> Result<SingleMachine<S3Store>> {
        Ok(SingleMachine::open(
            self.store(name)?,
            CONFIG,
            ServingOptions::m8(),
        )?)
    }
}
fn vector(id: u64) -> Vec<f32> {
    (0..64)
        .map(|d| ((id * 8191 + d * 127 + 42) % 65536) as f32 / 65536.)
        .collect()
}
fn tags(id: u64) -> BTreeMap<String, String> {
    BTreeMap::from([(
        "selected".into(),
        if id.is_multiple_of(100) { "yes" } else { "no" }.into(),
    )])
}
fn put(id: u64, vector: Vec<f32>) -> Mutation {
    Mutation::Put {
        id,
        vector,
        metadata: tags(id),
    }
}
fn initial() -> Rows {
    (0..2000).map(|id| (id, vector(id))).collect()
}
fn check(probe: &Probe, db: &SingleMachine<S3Store>, rows: &Rows) -> Result<()> {
    assert_eq!(db.status().documents, rows.len());
    for (&id, v) in rows {
        let (actual, metadata) = db.get(id).ok_or("missing row")?;
        assert_eq!(actual, v, "seed=42 id={id}");
        assert_eq!(*metadata, tags(id));
    }
    for q in [vector(123), vector(9123)] {
        for filtered in [false, true] {
            let mut expected: Vec<_> = rows
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
            let filter = if filtered {
                vec![("selected", "yes")]
            } else {
                vec![]
            };
            let actual = probe.timed(if filtered { "filtered_query" } else { "query" }, || {
                Ok(db.query(&q, 10, &filter, SearchMode::Exact)?)
            })?;
            assert_eq!(actual, expected, "seed=42");
        }
    }
    Ok(())
}
fn write(probe: &Probe, marker: &str) -> Result<()> {
    for name in NAMES {
        if !probe.store(name)?.list()?.is_empty() {
            return Err("pilot prefix must be empty; cleanup not authorized".into());
        }
    }
    // Local control file only: the supervisor may now clean these fresh test prefixes.
    fs::write(marker, &probe.root)?;
    let mut objects = probe.store("objects")?;
    objects.create("immutable", b"original")?;
    assert!(matches!(
        objects.create("immutable", b"replacement"),
        Err(glider::Error::Exists(_))
    ));
    drop(objects);
    assert_eq!(
        probe.store("objects")?.get("immutable")?,
        Some(b"original".to_vec())
    );
    assert_eq!(probe.store("objects")?.list()?, vec!["immutable"]);
    let mut db = probe.timed("initialize", || probe.open("live"))?;
    let rows = initial();
    for chunk in rows.iter().collect::<Vec<_>>().chunks(100) {
        probe.batch(
            &mut db,
            chunk.iter().map(|(&id, v)| put(id, v.to_vec())).collect(),
        )?;
    }
    check(probe, &db, &rows)?;
    db.close()?;
    Ok(())
}
fn recover(probe: &Probe) -> Result<()> {
    // A new OS process reconstructs the first phase's acknowledged state.
    let mut rows = initial();
    let mut db = probe.timed("reopen", || probe.open("live"))?;
    check(probe, &db, &rows)?;
    probe.batch(
        &mut db,
        vec![put(0, vector(8000)), Mutation::Delete { id: 1 }],
    )?;
    rows.insert(0, vector(8000));
    rows.remove(&1);
    assert!(db.get(1).is_none());
    check(probe, &db, &rows)?;
    probe.budget.lose_next_mutation_response();
    assert!(probe.batch(&mut db, vec![put(2, vector(9000))]).is_err());
    assert!(db.status().recovery_required);
    check(probe, &db, &rows)?; // Failed acknowledgement cannot change this handle's view.
    drop(db);
    probe.timed("isolated_takeover", || {
        Ok(stage_isolated_namespace(
            &probe.store("live")?,
            probe.store("takeover")?,
            CONFIG,
        )?)
    })?;
    rows.insert(2, vector(9000)); // Fault was injected after the server completed the PUT.
    let mut db = probe.timed("open_takeover", || probe.open("takeover"))?;
    check(probe, &db, &rows)?;
    probe.timed("backup", || Ok(db.backup_to(probe.store("backup")?)?))?;
    probe.timed("restore", || {
        Ok(stage_isolated_namespace(
            &probe.store("backup")?,
            probe.store("restored")?,
            CONFIG,
        )?)
    })?;
    let restored = probe.timed("open_restored", || probe.open("restored"))?;
    check(probe, &restored, &rows)?;
    restored.close()?;
    db.close()?;
    Ok(())
}
fn cleanup(probe: &Probe) -> Result<()> {
    for name in NAMES {
        let mut store = probe.store(name)?;
        for key in store.list()? {
            store.remove(&key)?;
        }
        assert!(store.list()?.is_empty());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 {
        return Err(
            "usage: s3_pilot write|recover|cleanup REPORT.json LOCAL_OWNERSHIP_MARKER".into(),
        );
    }
    let root = env::var("GLIDER_S3_NAMESPACE")?;
    let token = root
        .strip_prefix("glider-pilot/")
        .ok_or("invalid pilot prefix")?;
    if token.len() != 32 || !token.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("invalid pilot token".into());
    }
    let (requests, mib, seconds) = match args[1].as_str() {
        "write" => (2000, 25, 150),
        "recover" => (6000, 65, 300),
        "cleanup" => (2000, 10, 60),
        _ => return Err("unknown phase".into()),
    };
    if args[1] != "write" && fs::read_to_string(&args[3])? != root {
        return Err("missing matching ownership marker".into());
    }
    let probe = Probe {
        budget: Budget::new(requests, mib * 1024 * 1024, seconds),
        root,
        operations: RefCell::new(Vec::new()),
    };
    let start = Instant::now();
    let result = match args[1].as_str() {
        "write" => write(&probe, &args[3]),
        "recover" => recover(&probe),
        _ => cleanup(&probe),
    };
    fs::write(
        &args[2],
        serde_json::to_vec_pretty(&json!({
            "version":2, "measurement_protocol":"pilot-client-timing-v1",
            "phase":args[1], "passed":result.is_ok(), "prefix":probe.root,
            "http_timings":probe.budget.timings(), "operations":probe.operations,
            "counts":probe.budget.snapshot(), "elapsed_seconds":start.elapsed().as_secs_f64(),
            "limits":{"requests":requests,"payload_bytes":mib*1024*1024,"seconds":seconds},
            "rows":2000,"dimensions":64,"seed":42,"generator":"mod65536-v1"
        }))?,
    )?;
    result
}
