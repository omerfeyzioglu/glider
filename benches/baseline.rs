//! Explicit wall-clock scenarios; setup, validation, reporting and cleanup are untimed.
use glider::{
    store::{LocalStore, ObjectStore},
    Config, Database, Metric,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    env, fs,
    hint::black_box,
    io::{self, Write},
    path::{Path, PathBuf},
    process::Command,
    rc::Rc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Serialize)]
struct Options {
    scenario: String,
    rows: usize,
    dimensions: usize,
    mutations: usize,
    operations: usize,
    queries: usize,
    samples: usize,
    k: usize,
    seed: u64,
    root: PathBuf,
    label: String,
}
impl Options {
    fn parse() -> Result<Option<Self>> {
        let mut o = Self {
            scenario: "all".into(),
            rows: 1000,
            dimensions: 32,
            mutations: 5000,
            operations: 200,
            queries: 100,
            samples: 5,
            k: 10,
            seed: 42,
            root: env::temp_dir(),
            label: "unspecified".into(),
        };
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--bench" {
                continue;
            } // Cargo supplies this to custom harnesses.
            if arg == "--help" {
                eprintln!(
                    "glider baseline: cargo bench --locked --bench baseline -- [options]\n\
                    --scenario all|search|commit|recovery (all)\n\
                    --rows N (1000; search size / recovery live IDs)\n\
                    --dimensions D (32) --mutations N (5000; recovery total puts, >= rows)\n\
                    --operations N (200; commits per insert/overwrite/delete phase)\n\
                    --queries N (100) --samples N (5; search batches / warm reopens)\n\
                    --k N (10) --seed N (42) --root EXISTING_DIRECTORY (OS temp directory)\n\
                    --label TEXT (filesystem/device/power/load notes; unspecified)\n\
                    Output: one JSON document on stdout. Run without --help to measure."
                );
                return Ok(None);
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--scenario" => o.scenario = value,
                "--rows" => o.rows = value.parse()?,
                "--dimensions" => o.dimensions = value.parse()?,
                "--mutations" => o.mutations = value.parse()?,
                "--operations" => o.operations = value.parse()?,
                "--queries" => o.queries = value.parse()?,
                "--samples" => o.samples = value.parse()?,
                "--k" => o.k = value.parse()?,
                "--seed" => o.seed = value.parse()?,
                "--root" => o.root = PathBuf::from(value),
                "--label" => o.label = value,
                _ => return Err(format!("unknown option: {arg}").into()),
            }
        }
        if !matches!(
            o.scenario.as_str(),
            "all" | "search" | "commit" | "recovery"
        ) {
            return Err("invalid scenario".into());
        }
        if [
            o.rows,
            o.dimensions,
            o.operations,
            o.queries,
            o.samples,
            o.k,
        ]
        .contains(&0)
        {
            return Err(
                "rows, dimensions, operations, queries, samples and k must be positive".into(),
            );
        }
        if matches!(o.scenario.as_str(), "all" | "recovery") && o.mutations < o.rows {
            return Err("recovery mutations must be >= rows".into());
        }
        o.root = o.root.canonicalize()?;
        if !o.root.is_dir() {
            return Err("root must be an existing directory".into());
        }
        Ok(Some(o))
    }
    fn config(&self) -> Config {
        Config {
            dimensions: self.dimensions,
            metric: Metric::SquaredEuclidean,
        }
    }
}

// SplitMix64-v1, high 24 bits mapped exactly to f32 in [-1, 1). Separate
// streams keep queries unchanged when dataset cardinality changes.
fn vectors(seed: u64, count: usize, dimensions: usize) -> Vec<Vec<f32>> {
    let mut state = seed;
    (0..count)
        .map(|_| {
            (0..dimensions)
                .map(|_| {
                    state = state.wrapping_add(0x9e3779b97f4a7c15);
                    let mut z = state;
                    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
                    z ^= z >> 31;
                    ((z >> 40) as u32 as f32) * (1.0 / 8_388_608.0) - 1.0
                })
                .collect()
        })
        .collect()
}
fn fingerprint(data: &[Vec<f32>]) -> String {
    let mut hash = Sha256::new();
    for vector in data {
        for value in vector {
            hash.update(value.to_bits().to_le_bytes());
        }
    }
    format!("{:x}", hash.finalize())
}

#[derive(Clone, Copy, Default, Serialize)]
struct Counts {
    get_calls: u64,
    list_calls: u64,
    create_calls: u64,
    get_payload_bytes: u64,
    create_payload_bytes: u64,
}
struct Counted<S> {
    inner: S,
    counts: Rc<Cell<Counts>>,
}
impl<S: ObjectStore> ObjectStore for Counted<S> {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let result = self.inner.get(key)?;
        let mut counts = self.counts.get();
        counts.get_calls += 1;
        counts.get_payload_bytes += result.as_ref().map_or(0, |v| v.len() as u64);
        self.counts.set(counts);
        Ok(result)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        let result = self.inner.list()?;
        let mut counts = self.counts.get();
        counts.list_calls += 1;
        self.counts.set(counts);
        Ok(result)
    }
    fn create(&mut self, key: &str, value: &[u8]) -> glider::Result<()> {
        self.inner.create(key, value)?;
        let mut counts = self.counts.get();
        counts.create_calls += 1;
        counts.create_payload_bytes += value.len() as u64;
        self.counts.set(counts);
        Ok(())
    }
}
type Db = Database<Counted<LocalStore>>;
fn open(root: &Path, config: Config, counts: &Rc<Cell<Counts>>) -> Result<Db> {
    Ok(Database::open(
        Counted {
            inner: LocalStore::open(root)?,
            counts: counts.clone(),
        },
        config,
    )?)
}
fn inventory(root: &Path) -> Result<Value> {
    // Separate from timers and counted calls. File lengths are not device I/O or
    // allocated disk blocks. Query the store rather than inferring logical objects.
    let objects = LocalStore::open(root)?.list()?.len();
    let mut files = 0_u64;
    let mut file_bytes = 0_u64;
    for entry in fs::read_dir(root)? {
        let metadata = entry?.metadata()?;
        if !metadata.is_file() {
            return Err("unexpected non-file in benchmark namespace".into());
        }
        files += 1;
        file_bytes += metadata.len();
    }
    Ok(
        json!({"logical_objects": objects, "physical_files": files, "file_length_bytes": file_bytes}),
    )
}
fn timing(samples: &[f64], operations_per_sample: usize) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let sum: f64 = samples.iter().sum();
    let p50 = sorted[(sorted.len() - 1) / 2];
    json!({
        "raw_sample_ns": samples, "sample_count": samples.len(),
        "operations_per_sample": operations_per_sample,
        "min_sample_ns": sorted[0], "p50_sample_ns": p50,
        "p95_sample_ns": sorted[(sorted.len() * 95).div_ceil(100) - 1],
        "max_sample_ns": sorted[sorted.len() - 1],
        "mean_ns_per_operation": sum / samples.len() as f64 / operations_per_sample as f64,
        "p50_amortized_ns_per_operation": p50 / operations_per_sample as f64,
        "operations_per_timed_second": samples.len() as f64 * operations_per_sample as f64 * 1e9 / sum,
    })
}
fn ns(start: Instant) -> f64 {
    start.elapsed().as_nanos() as f64
}

fn search(o: &Options) -> Result<Value> {
    let temp = tempfile::Builder::new()
        .prefix("glider-search-")
        .tempdir_in(&o.root)?;
    let root = temp.path().join("db");
    let counts = Rc::new(Cell::new(Counts::default()));
    let mut db = open(&root, o.config(), &counts)?;
    let data = vectors(o.seed, o.rows, o.dimensions);
    let queries = vectors(o.seed ^ 0xd1b54a32d192ed03, o.queries, o.dimensions);
    let data_hash = fingerprint(&data);
    let query_hash = fingerprint(&queries);
    for (id, vector) in data.into_iter().enumerate() {
        db.put(id as u64, vector)?;
    }
    let build_counts = counts.get();
    // One full query pass warms the engine and saves a reusable exact oracle.
    let oracle: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| {
            db.search(q, o.k)
                .map(|hits| hits.into_iter().map(|hit| hit.id).collect())
        })
        .collect::<glider::Result<_>>()?;
    counts.set(Counts::default());
    let mut samples = Vec::with_capacity(o.samples);
    for _ in 0..o.samples {
        let start = Instant::now();
        for query in &queries {
            black_box(db.search(black_box(query), o.k)?);
        }
        samples.push(ns(start));
    }
    let measured = counts.get();
    drop(db);
    Ok(
        json!({"scenario": "search", "backend": "local", "cache": "warm in-memory query pass",
        "live_documents": o.rows, "mutation_history": o.rows, "warmup_queries": o.queries,
        "dataset_sha256": data_hash, "query_sha256": query_hash, "exact_neighbor_ids": oracle,
        "build_store_calls": build_counts,
        "timing": timing(&samples, o.queries), "measured_store_calls": measured,
        "inventory": inventory(&root)?}),
    )
}

fn commit(o: &Options) -> Result<Value> {
    let temp = tempfile::Builder::new()
        .prefix("glider-commit-")
        .tempdir_in(&o.root)?;
    let root = temp.path().join("db");
    let counts = Rc::new(Cell::new(Counts::default()));
    let mut db = open(&root, o.config(), &counts)?;
    let mut phases = Vec::new();
    for (phase_index, phase) in ["insert", "overwrite", "delete"].iter().enumerate() {
        let data = if *phase == "delete" {
            Vec::new()
        } else {
            vectors(
                o.seed.wrapping_add(phase_index as u64),
                o.operations,
                o.dimensions,
            )
        };
        let data_hash = fingerprint(&data);
        let mut values = data.into_iter();
        let mut samples = Vec::with_capacity(o.operations);
        counts.set(Counts::default());
        for id in 0..o.operations {
            let vector = values.next(); // Ownership preparation is outside timing.
            let start = Instant::now();
            match vector {
                Some(v) => db.put(id as u64, v)?,
                None => db.delete(id as u64)?,
            }
            samples.push(ns(start));
        }
        let measured = counts.get();
        assert_eq!(db.get(0).is_some(), *phase != "delete");
        phases.push(json!({"phase": phase, "dataset_sha256": data_hash,
            "live_documents_after": if *phase == "delete" { 0 } else { o.operations },
            "mutation_history_after": (phase_index + 1) * o.operations,
            "timing": timing(&samples, 1), "measured_store_calls": measured}));
    }
    drop(db);
    Ok(
        json!({"scenario": "commit", "backend": "local", "warmup_operations": 0,
        "phases": phases, "inventory": inventory(&root)?}),
    )
}

fn recovery(o: &Options) -> Result<Value> {
    let temp = tempfile::Builder::new()
        .prefix("glider-recovery-")
        .tempdir_in(&o.root)?;
    let root = temp.path().join("db");
    let counts = Rc::new(Cell::new(Counts::default()));
    let mut db = open(&root, o.config(), &counts)?;
    let data = vectors(o.seed, o.mutations, o.dimensions);
    let data_hash = fingerprint(&data);
    // Round-robin overwrite history: live size is fixed independently of history.
    let mut expected = vec![Vec::new(); o.rows];
    for (i, vector) in data.into_iter().enumerate() {
        expected[i % o.rows] = vector.clone();
        db.put((i % o.rows) as u64, vector)?;
    }
    let build_counts = counts.get();
    drop(db);
    let footprint = inventory(&root)?;
    drop(open(&root, o.config(), &counts)?); // Explicit untimed warm recovery.
    let mut store_samples = Vec::with_capacity(o.samples);
    let mut replay_samples = Vec::with_capacity(o.samples);
    let mut total_samples = Vec::with_capacity(o.samples);
    let mut calls = Vec::with_capacity(o.samples);
    for _ in 0..o.samples {
        counts.set(Counts::default());
        let start = Instant::now();
        let store = LocalStore::open(&root)?;
        let store_ns = ns(start);
        let counted = Counted {
            inner: store,
            counts: counts.clone(),
        };
        let replay_start = Instant::now();
        let db = Database::open(counted, o.config())?;
        let replay_ns = ns(replay_start);
        let total_ns = ns(start);
        store_samples.push(store_ns);
        replay_samples.push(replay_ns);
        total_samples.push(total_ns);
        calls.push(counts.get());
        for (id, value) in expected.iter().enumerate() {
            assert_eq!(db.get(id as u64), Some(value.as_slice()));
        }
        drop(db); // Destruction excluded from recovery time.
    }
    Ok(
        json!({"scenario": "recovery", "backend": "local", "cache": "warm OS cache; no eviction",
        "live_documents": o.rows, "mutation_history": o.mutations, "history": "round-robin puts",
        "dataset_sha256": data_hash, "warmup_reopens": 1, "build_store_calls": build_counts,
        "local_store_open": timing(&store_samples, 1), "database_replay": timing(&replay_samples, 1),
        "total_open": timing(&total_samples, 1), "measured_store_calls_per_sample": calls,
        "inventory": footprint}),
    )
}

fn command(program: &str, args: &[&str]) -> Option<String> {
    let result = Command::new(program)
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()?;
    result
        .status
        .success()
        .then(|| String::from_utf8_lossy(&result.stdout).trim().to_owned())
}
fn source_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(root.join(directory))? {
        let entry = entry?;
        let path = directory.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            source_files(root, &path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}
fn context(o: &Options) -> Result<Value> {
    let source = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut hash = Sha256::new();
    let mut files = vec![PathBuf::from("Cargo.toml"), PathBuf::from("Cargo.lock")];
    source_files(source, Path::new("src"), &mut files)?;
    source_files(source, Path::new("benches"), &mut files)?;
    files.sort();
    for name in files {
        let bytes = fs::read(source.join(&name))?;
        hash.update(name.to_string_lossy().as_bytes());
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(bytes);
    }
    let cpu = if cfg!(target_os = "macos") {
        command("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        fs::read_to_string("/proc/cpuinfo").ok().and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name"))
                .map(str::to_owned)
        })
    };
    let memory = if cfg!(target_os = "macos") {
        command("sysctl", &["-n", "hw.memsize"])
    } else {
        fs::read_to_string("/proc/meminfo").ok().and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .map(str::to_owned)
        })
    };
    Ok(json!({
        "unix_seconds": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        "git_revision": command("git", &["rev-parse", "HEAD"]),
        "git_status": command("git", &["status", "--short"]),
        "source_sha256": format!("{:x}", hash.finalize()),
        "rustc": command("rustc", &["--version", "--verbose"]),
        "cargo": command("cargo", &["--version"]), "os": command("uname", &["-srvm"]),
        "architecture": env::consts::ARCH, "cpu": cpu, "memory": memory,
        "logical_parallelism": std::thread::available_parallelism()?.get(),
        "filesystem_mount": command("df", &["-Pk", &o.root.to_string_lossy()]),
        "rustflags": env::var("RUSTFLAGS").ok(), "cargo_encoded_rustflags": env::var("CARGO_ENCODED_RUSTFLAGS").ok(),
        "profile": "bench",
        "profile_environment": env::vars().filter(|(key, _)| key.starts_with("CARGO_PROFILE_")).collect::<std::collections::BTreeMap<_, _>>(),
        "debug_assertions": cfg!(debug_assertions), "argv": env::args().collect::<Vec<_>>()
    }))
}
fn main() -> Result<()> {
    let Some(options) = Options::parse()? else {
        return Ok(());
    };
    if cfg!(debug_assertions) {
        return Err("run with cargo bench (optimized bench profile)".into());
    }
    let environment = context(&options)?;
    let mut results = Vec::new();
    for scenario in ["search", "commit", "recovery"] {
        if options.scenario == "all" || options.scenario == scenario {
            eprintln!(
                "Measuring {scenario}: seed={}, dimensions={}",
                options.seed, options.dimensions
            );
            results.push(match scenario {
                "search" => search(&options)?,
                "commit" => commit(&options)?,
                _ => recovery(&options)?,
            });
        }
    }
    let report = json!({"schema_version": 1, "generator": "splitmix64-high24-uniform-f32-v1",
        "metric": "squared_euclidean",
        "counter_scope": "Engine-to-store calls and payload bytes only; excludes backend-internal I/O and inventory",
        "footprint_scope": "Logical objects, physical files, and summed file lengths; not allocated blocks or device bytes written",
        "config": options, "environment": environment, "results": results});
    serde_json::to_writer_pretty(io::stdout().lock(), &report)?;
    writeln!(io::stdout())?;
    Ok(())
}
