//! Targeted M8 filtered streaming query measurement; setup and oracle are untimed.
use glider::{
    store::ObjectStore, streaming::StreamingDatabase, Config, Database, Metric, Mutation, Neighbor,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{cell::Cell, collections::BTreeMap, env, process::Command, rc::Rc, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[path = "../benches/support/backend.rs"]
#[allow(dead_code)] // This probe uses the shared namespace setup but not every metric helper.
mod backend;
use backend::{LocalNamespace, Namespace};

#[derive(Clone, Copy, Default)]
struct Counts {
    gets: u64,
    bytes: u64,
}
struct Counted<S> {
    inner: S,
    counts: Rc<Cell<Counts>>,
}
impl<S: ObjectStore> ObjectStore for Counted<S> {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let value = self.inner.get(key)?;
        let mut counts = self.counts.get();
        counts.gets += 1;
        counts.bytes += value.as_ref().map_or(0, |bytes| bytes.len() as u64);
        self.counts.set(counts);
        Ok(value)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&mut self, key: &str, bytes: &[u8]) -> glider::Result<()> {
        self.inner.create(key, bytes)
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)
    }
}

fn vector(state: &mut u64, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
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

fn measure<S: ObjectStore>(
    reader: &StreamingDatabase<Counted<S>>,
    counts: &Rc<Cell<Counts>>,
    queries: &[Vec<f32>],
    expected: &[Vec<Neighbor>],
    filter: &[(&str, &str)],
) -> Result<Value> {
    counts.set(Counts::default());
    let mut samples = Vec::with_capacity(queries.len());
    for (query, answer) in queries.iter().zip(expected) {
        let start = Instant::now();
        let actual = reader.search_filtered(query, 10, filter)?;
        samples.push(start.elapsed().as_nanos() as u64);
        assert_eq!(&actual, answer);
    }
    let raw_samples = samples.clone();
    samples.sort_unstable();
    let used = counts.get();
    Ok(json!({
        "p50_ns": samples[(samples.len() - 1) / 2],
        "p95_ns": samples[(samples.len() - 1) * 95 / 100],
        "p99_ns": samples[(samples.len() - 1) * 99 / 100],
        "get_calls": used.gets,
        "logical_bytes_read": used.bytes,
        "sample_count": samples.len(),
        "raw_sample_ns": raw_samples,
    }))
}

fn command(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default()
}

fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    let bytes = usage.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        bytes
    } else {
        bytes * 1024
    }
}

fn run<N: Namespace>(namespace: N) -> Result<()> {
    let config = Config {
        dimensions: 64,
        metric: Metric::SquaredEuclidean,
    };
    let counts = Rc::new(Cell::new(Counts::default()));
    let mut db = Database::open(
        Counted {
            inner: namespace.open()?,
            counts: counts.clone(),
        },
        config,
    )?;
    let mut state = 42_u64;
    for start in (0..2000).step_by(100) {
        let mut mutations = Vec::with_capacity(100);
        for id in start..start + 100 {
            let metadata = if id % 100 == 0 {
                BTreeMap::from([("selected".into(), "true".into())])
            } else {
                BTreeMap::new()
            };
            mutations.push(Mutation::Put {
                id: id as u64,
                vector: vector(&mut state, 64),
                metadata,
            });
        }
        db.apply_batch(mutations)?;
    }
    db.compact_chunked(131_072)?;
    let mut query_state = 42_u64 ^ 0xd1b54a32d192ed03;
    let queries: Vec<_> = (0..1000).map(|_| vector(&mut query_state, 64)).collect();
    let filtered: Vec<_> = queries
        .iter()
        .map(|query| db.search_filtered(query, 10, &[("selected", "true")]))
        .collect::<glider::Result<_>>()?;
    let unfiltered: Vec<_> = queries
        .iter()
        .map(|query| db.search(query, 10))
        .collect::<glider::Result<_>>()?;
    drop(db);

    let plain = StreamingDatabase::open(
        Counted {
            inner: namespace.open()?,
            counts: counts.clone(),
        },
        config,
    )?;
    let posting = StreamingDatabase::open_with_filter(
        Counted {
            inner: namespace.open()?,
            counts: counts.clone(),
        },
        config,
        "selected",
        "true",
        64,
    )?;
    // The remote full-scan baseline has a deterministic request count. A short
    // sample establishes its I/O cost without transferring several GiB just to
    // reconfirm that 12 GETs happen on every query.
    let baseline_queries = if N::NAME == "s3" && env::var_os("GLIDER_M11_FULL").is_none() {
        24
    } else {
        queries.len()
    };
    let before_filtered = measure(
        &plain,
        &counts,
        &queries[..baseline_queries],
        &filtered[..baseline_queries],
        &[("selected", "true")],
    )?;
    let after_filtered = measure(
        &posting,
        &counts,
        &queries,
        &filtered,
        &[("selected", "true")],
    )?;
    let before_unfiltered = measure(
        &plain,
        &counts,
        &queries[..baseline_queries],
        &unfiltered[..baseline_queries],
        &[],
    )?;
    let after_unfiltered = measure(
        &posting,
        &counts,
        &queries[..baseline_queries],
        &unfiltered[..baseline_queries],
        &[],
    )?;
    let mut source_hash = Sha256::new();
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "src/lib.rs",
        "src/streaming.rs",
        "src/store/s3.rs",
        "examples/m11_filter_probe.rs",
    ] {
        source_hash.update(path.as_bytes());
        source_hash.update(std::fs::read(path)?);
    }
    let report = json!({
        "backend": N::NAME,
        "git_revision": command("git", &["rev-parse", "HEAD"]),
        "git_status": command("git", &["status", "--short"]),
        "source_sha256": format!("{:x}", source_hash.finalize()),
        "environment": {
            "rustc": command("rustc", &["--version"]),
            "os": command("uname", &["-srvm"]),
            "cpu": if cfg!(target_os = "macos") { command("sysctl", &["-n", "machdep.cpu.brand_string"]) } else { command("uname", &["-m"]) },
            "service": env::var("GLIDER_S3_SERVICE_LABEL").ok(),
        },
        "dataset_seed": 42,
        "query_seed_xor": "d1b54a32d192ed03",
        "rows": 2000,
        "dimensions": 64,
        "queries": 1000,
        "k": 10,
        "selected_every": 100,
        "chunk_bytes": 131072,
        "before_filtered": before_filtered,
        "after_filtered": after_filtered,
        "before_unfiltered": before_unfiltered,
        "after_unfiltered": after_unfiltered,
        "peak_process_rss_bytes": peak_rss_bytes(),
        "inventory": namespace.inventory()?,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("local") => run(LocalNamespace::new(std::path::Path::new("target"), "m11")?),
        #[cfg(feature = "s3")]
        Some("s3") => run(backend::S3Namespace::new(
            backend::S3Config::from_env()?,
            "m11",
        )?),
        _ => Err(
            "usage: cargo run --release --example m11_filter_probe [--features s3] -- local|s3"
                .into(),
        ),
    }
}
