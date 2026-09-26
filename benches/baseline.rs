//! Explicit wall-clock scenarios; setup, validation, reporting and cleanup are untimed.
use glider::{store::ObjectStore, Config, Database, Mutation};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    collections::BTreeMap,
    env, fs,
    hint::black_box,
    io::{self, Write},
    path::{Path, PathBuf},
    process::Command,
    rc::Rc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[path = "support/metrics.rs"]
mod metrics;
use metrics::{resources, timing, usage_delta, Usage};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[path = "support/options.rs"]
mod options;
use options::{Backend, Options};
#[path = "support/backend.rs"]
mod backend;
use backend::{LocalNamespace, Namespace};
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
    #[serde(skip_serializing_if = "zero_count")]
    remove_calls: u64,
    get_payload_bytes: u64,
    create_payload_bytes: u64,
    #[serde(skip_serializing_if = "zero_count")]
    get_wall_ns: u64,
    #[serde(skip_serializing_if = "zero_count")]
    list_wall_ns: u64,
}
fn zero_count(value: &u64) -> bool {
    *value == 0
}
struct Counted<S> {
    inner: S,
    counts: Rc<Cell<Counts>>,
    timed_reads: bool,
}
impl<S: ObjectStore> ObjectStore for Counted<S> {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let started = self.timed_reads.then(Instant::now);
        let result = self.inner.get(key)?;
        let mut counts = self.counts.get();
        counts.get_calls += 1;
        if let Some(started) = started {
            counts.get_wall_ns += started.elapsed().as_nanos() as u64;
        }
        counts.get_payload_bytes += result.as_ref().map_or(0, |v| v.len() as u64);
        self.counts.set(counts);
        Ok(result)
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        let started = self.timed_reads.then(Instant::now);
        let result = self.inner.list()?;
        let mut counts = self.counts.get();
        counts.list_calls += 1;
        if let Some(started) = started {
            counts.list_wall_ns += started.elapsed().as_nanos() as u64;
        }
        self.counts.set(counts);
        Ok(result)
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.inner.remove(key)?;
        let mut counts = self.counts.get();
        counts.remove_calls += 1;
        self.counts.set(counts);
        Ok(())
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
type Db<N> = Database<Counted<<N as Namespace>::Store>>;
fn open<N: Namespace>(
    namespace: &N,
    config: Config,
    counts: &Rc<Cell<Counts>>,
    timed_reads: bool,
) -> Result<(Db<N>, backend::Observer)> {
    let store = namespace.open()?;
    let observer = N::observe(&store);
    Ok((
        Database::open(
            Counted {
                inner: store,
                counts: counts.clone(),
                timed_reads,
            },
            config,
        )?,
        observer,
    ))
}
fn ns(start: Instant) -> f64 {
    start.elapsed().as_nanos() as f64
}

fn search<N: Namespace>(o: &Options, namespace: &N) -> Result<Value> {
    let counts = Rc::new(Cell::new(Counts::default()));
    let (mut db, observer) = open(namespace, o.config(), &counts, o.feature == "m10")?;
    let mut data = vectors(o.seed, o.rows, o.dimensions);
    let mut queries = vectors(o.seed ^ 0xd1b54a32d192ed03, o.queries, o.dimensions);
    if o.distribution == "clustered" {
        let centers = vectors(o.seed ^ 0xa0761d6478bd642f, 16, o.dimensions);
        for points in [&mut data, &mut queries] {
            for (i, point) in points.iter_mut().enumerate() {
                for (v, center) in point.iter_mut().zip(&centers[i % centers.len()]) {
                    *v = *center + *v * 0.1;
                }
            }
        }
    }
    let data_hash = fingerprint(&data);
    let query_hash = fingerprint(&queries);
    let mut selected_ids = Sha256::new();
    for (id, vector) in data.into_iter().enumerate() {
        if o.filter_every > 0 && id % o.filter_every == 0 {
            selected_ids.update((id as u64).to_le_bytes());
            db.put_with_metadata(
                id as u64,
                vector,
                BTreeMap::from([("selected".into(), "true".into())]),
            )?;
        } else {
            db.put(id as u64, vector)?;
        }
    }
    let build_counts = counts.get();
    let build_http = observer.snapshot();
    // One full query pass warms the engine and saves a reusable exact oracle.
    let oracle: Vec<Vec<u64>> = queries
        .iter()
        .map(|q| {
            if o.filter_every > 0 {
                db.search_filtered(q, o.k, &[("selected", "true")])
            } else {
                db.search(q, o.k)
            }
            .map(|hits| hits.into_iter().map(|hit| hit.id).collect())
        })
        .collect::<glider::Result<_>>()?;
    let ann = if o.ivf_partitions > 0 {
        let start = Instant::now();
        db.build_ivf(glider::ivf::IvfConfig {
            partitions: o.ivf_partitions,
            iterations: o.ivf_iterations,
            seed: o.seed,
        })?;
        let build_ns = ns(start);
        let mut answers = Vec::new();
        let mut evaluations = Vec::new();
        let mut recalls = Vec::new();
        for (query, exact) in queries.iter().zip(&oracle) {
            let hits = if o.filter_every > 0 {
                db.search_ivf_filtered(query, o.k, o.ivf_probes, &[("selected", "true")])?
            } else {
                db.search_ivf(query, o.k, o.ivf_probes)?
            };
            let ids: Vec<_> = hits.neighbors.iter().map(|h| h.id).collect();
            if o.ivf_probes >= o.ivf_partitions.min(o.rows) && ids != *exact {
                return Err("full-probe IVF differs from exact oracle".into());
            }
            recalls.push(
                ids.iter().filter(|id| exact.contains(id)).count() as f64 / exact.len() as f64,
            );
            evaluations.push(
                json!({"centroid": hits.centroid_distances, "vector": hits.vector_distances}),
            );
            answers.push(ids);
        }
        Some(
            json!({"algorithm": "ivf-flat-v1", "build_ns": build_ns, "returned_neighbor_ids": answers,
            "recall_at_k": recalls, "distance_evaluations": evaluations}),
        )
    } else {
        None
    };
    if o.profile_seconds > 0 {
        // Profilers attach only after durable setup and the oracle warmup.
        // Diagnostic reports must not be used as unprofiled latency baselines.
        eprintln!("PROFILE_READY {}", std::process::id());
        let start = Instant::now();
        while start.elapsed().as_secs_f64() < o.profile_seconds as f64 {
            for query in &queries {
                if o.filter_every > 0 {
                    black_box(db.search_filtered(
                        black_box(query),
                        o.k,
                        &[("selected", "true")],
                    )?);
                } else {
                    black_box(db.search(black_box(query), o.k)?);
                }
            }
        }
    }
    counts.set(Counts::default());
    let http_before = observer.snapshot();
    let mut samples = Vec::with_capacity(o.samples);
    let mut query_samples = Vec::with_capacity(
        o.samples
            .checked_mul(o.queries)
            .ok_or("query count overflow")?,
    );
    let cpu_start = Usage::capture();
    for _ in 0..o.samples {
        let start = Instant::now();
        for query in &queries {
            let query_start = Instant::now();
            if o.ivf_partitions > 0 {
                if o.filter_every > 0 {
                    black_box(db.search_ivf_filtered(
                        black_box(query),
                        o.k,
                        o.ivf_probes,
                        &[("selected", "true")],
                    )?);
                } else {
                    black_box(db.search_ivf(black_box(query), o.k, o.ivf_probes)?);
                }
            } else {
                if o.filter_every > 0 {
                    black_box(db.search_filtered(
                        black_box(query),
                        o.k,
                        &[("selected", "true")],
                    )?);
                } else {
                    black_box(db.search(black_box(query), o.k)?);
                }
            }
            query_samples.push(ns(query_start));
        }
        samples.push(ns(start));
    }
    let cpu = resources(
        vec![usage_delta(cpu_start, Usage::capture())],
        "query loops including timer and loop overhead",
    );
    let measured = counts.get();
    let measured_http = observer.delta(http_before);
    drop(db);
    let mut result = json!({"scenario": "search", "backend": N::NAME, "cache": "warm in-memory query pass",
        "live_documents": o.rows, "mutation_history": o.rows, "warmup_queries": o.queries,
        "dataset_seed": o.seed, "query_seed": o.seed ^ 0xd1b54a32d192ed03_u64,
        "dataset_sha256": data_hash, "query_sha256": query_hash, "exact_neighbor_ids": oracle,
        "query_latency": timing(&query_samples, 1), "resources": cpu,
        "build_store_calls": build_counts,
        "timing": timing(&samples, o.queries), "measured_store_calls": measured,
        "inventory": namespace.inventory()?});
    if let Some(ann) = ann {
        result["ann"] = ann;
    }
    if o.filter_every > 0 {
        result["filter"] = json!({
            "predicate": {"selected": "true"},
            "eligible_documents": o.rows.div_ceil(o.filter_every),
            "selected_ids_sha256": format!("{:x}", selected_ids.finalize()),
        });
    }
    backend::attach_http(&mut result, "build_http_requests", build_http);
    backend::attach_http(&mut result, "measured_http_requests", measured_http);
    namespace.annotate(&mut result);
    Ok(result)
}

fn commit<N: Namespace>(o: &Options, namespace: &N) -> Result<Value> {
    let counts = Rc::new(Cell::new(Counts::default()));
    let (mut db, observer) = open(namespace, o.config(), &counts, o.feature == "m10")?;
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
        let http_before = observer.snapshot();
        let cpu_start = Usage::capture();
        for id in 0..o.operations {
            let vector = values.next(); // Ownership preparation is outside timing.
            let start = Instant::now();
            match vector {
                Some(v) => db.put(id as u64, v)?,
                None => db.delete(id as u64)?,
            }
            samples.push(ns(start));
        }
        let cpu = resources(
            vec![usage_delta(cpu_start, Usage::capture())],
            "commit loop including timer and loop overhead",
        );
        let measured = counts.get();
        assert_eq!(db.get(0).is_some(), *phase != "delete");
        let measured_http = observer.delta(http_before);
        let mut result = json!({"phase": phase,
            "dataset_seed": if *phase == "delete" { None } else { Some(o.seed.wrapping_add(phase_index as u64)) },
            "query_seed": null, "query_sha256": null, "dataset_sha256": data_hash, "resources": cpu,
            "live_documents_after": if *phase == "delete" { 0 } else { o.operations },
            "mutation_history_after": (phase_index + 1) * o.operations,
            "timing": timing(&samples, 1), "measured_store_calls": measured});
        backend::attach_http(&mut result, "measured_http_requests", measured_http);
        phases.push(result);
    }
    drop(db);
    let mut result = json!({"scenario": "commit", "backend": N::NAME, "warmup_operations": 0,
        "phases": phases, "inventory": namespace.inventory()?});
    namespace.annotate(&mut result);
    Ok(result)
}

fn recovery<N: Namespace>(o: &Options, namespace: &N) -> Result<Value> {
    let counts = Rc::new(Cell::new(Counts::default()));
    let (mut db, observer) = open(namespace, o.config(), &counts, o.feature == "m10")?;
    let data = vectors(o.seed, o.mutations, o.dimensions);
    let data_hash = fingerprint(&data);
    // Round-robin overwrite history: live size is fixed independently of history.
    let mut expected = vec![Vec::new(); o.rows];
    let mut checkpoint = None;
    let mut compaction = None;
    let mut mutation_bytes = 0_u64;
    for (batch_index, batch) in data.chunks(o.batch_size).enumerate() {
        let start_index = batch_index * o.batch_size;
        let end_index = start_index + batch.len();
        let mut mutations = Vec::with_capacity(batch.len());
        for (offset, vector) in batch.iter().enumerate() {
            let id = (start_index + offset) % o.rows;
            expected[id] = vector.clone();
            mutations.push(Mutation::Put {
                id: id as u64,
                vector: vector.clone(),
                metadata: BTreeMap::new(),
            });
        }
        let before_mutation = counts.get().create_payload_bytes;
        if o.batch_size == 1 {
            let Mutation::Put { id, vector, .. } = mutations.pop().unwrap() else {
                unreachable!()
            };
            db.put(id, vector)?;
        } else {
            db.apply_batch(mutations)?;
        }
        mutation_bytes += counts.get().create_payload_bytes - before_mutation;
        if o.checkpoint_at == end_index {
            let before = counts.get();
            let before_http = observer.snapshot();
            let start = Instant::now();
            db.checkpoint()?;
            let elapsed = ns(start);
            let mut result = json!({"sequence": batch_index + 1, "latency_ns": elapsed,
                "logical_bytes_written": counts.get().create_payload_bytes - before.create_payload_bytes,
                "creates": counts.get().create_calls - before.create_calls});
            if o.batch_size > 1 {
                result["logical_mutations"] = json!(end_index);
            }
            backend::attach_http(&mut result, "http_requests", observer.delta(before_http));
            checkpoint = Some(result);
        }
        if o.compact_at == end_index {
            let before = counts.get();
            let before_http = observer.snapshot();
            let start = Instant::now();
            if o.chunk_bytes == 0 {
                db.compact()?;
            } else {
                db.compact_chunked(o.chunk_bytes)?;
            }
            let elapsed = ns(start);
            let read = counts.get().get_payload_bytes - before.get_payload_bytes;
            let written = counts.get().create_payload_bytes - before.create_payload_bytes;
            let mut result = json!({"sequence": batch_index + 1, "latency_ns": elapsed,
                "logical_bytes_read": read, "logical_bytes_written": written,
                "creates": counts.get().create_calls - before.create_calls,
                "removes": counts.get().remove_calls - before.remove_calls,
                "lists": counts.get().list_calls - before.list_calls,
                "input_mutation_payload_bytes": mutation_bytes,
                "additional_read_amplification": read as f64 / mutation_bytes as f64,
                "additional_write_amplification": written as f64 / mutation_bytes as f64});
            if o.batch_size > 1 {
                result["logical_mutations"] = json!(end_index);
            }
            backend::attach_http(&mut result, "http_requests", observer.delta(before_http));
            compaction = Some(result);
        }
    }
    let build_counts = counts.get();
    let build_http = observer.snapshot();
    drop(db);
    let footprint = namespace.inventory()?;
    drop(open(namespace, o.config(), &counts, o.feature == "m10")?); // Explicit untimed warm recovery.
    let mut store_samples = Vec::with_capacity(o.samples);
    let mut replay_samples = Vec::with_capacity(o.samples);
    let mut total_samples = Vec::with_capacity(o.samples);
    let mut calls = Vec::with_capacity(o.samples);
    let mut cpu_samples = Vec::with_capacity(o.samples);
    let mut http_samples = Vec::new();
    for _ in 0..o.samples {
        counts.set(Counts::default());
        let cpu_start = Usage::capture();
        let start = Instant::now();
        let store = namespace.open()?;
        let store_ns = ns(start);
        let observer = N::observe(&store);
        let counted = Counted {
            inner: store,
            counts: counts.clone(),
            timed_reads: o.feature == "m10",
        };
        let replay_start = Instant::now();
        let db = Database::open(counted, o.config())?;
        let replay_ns = ns(replay_start);
        let total_ns = ns(start);
        cpu_samples.push(usage_delta(cpu_start, Usage::capture()));
        store_samples.push(store_ns);
        replay_samples.push(replay_ns);
        total_samples.push(total_ns);
        calls.push(counts.get());
        if let Some(http) = observer.snapshot() {
            http_samples.push(http);
        }
        for (id, value) in expected.iter().enumerate() {
            assert_eq!(db.get(id as u64), Some(value.as_slice()));
        }
        drop(db); // Destruction excluded from recovery time.
    }
    let mut result = json!({"scenario": "recovery", "backend": N::NAME, "cache": N::RECOVERY_CACHE,
        "live_documents": o.rows, "mutation_history": o.mutations, "history": "round-robin puts",
        "dataset_seed": o.seed, "query_seed": null, "query_sha256": null,
        "resources": resources(cpu_samples, "sum of open windows excluding validation and destruction"),
        "dataset_sha256": data_hash, "warmup_reopens": 1, "build_store_calls": build_counts,
        "local_store_open": timing(&store_samples, 1), "database_replay": timing(&replay_samples, 1),
        "total_open": timing(&total_samples, 1), "measured_store_calls_per_sample": calls,
        "inventory": footprint});
    if o.feature == "m10" {
        result["durable_mutation_objects"] = json!(o.mutations.div_ceil(o.batch_size));
    }
    if let Some(compaction) = compaction {
        result["compaction"] = compaction;
    }
    if let Some(checkpoint) = checkpoint {
        result["checkpoint"] = checkpoint;
    }
    if N::NAME == "s3" {
        let open = result
            .as_object_mut()
            .unwrap()
            .remove("local_store_open")
            .unwrap();
        result["store_open"] = open;
        result["http_requests_per_sample"] = json!(http_samples);
    }
    backend::attach_http(&mut result, "build_http_requests", build_http);
    namespace.annotate(&mut result);
    Ok(result)
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
fn run_scenario<N: Namespace>(o: &Options, scenario: &str, namespace: &N) -> Result<Value> {
    match scenario {
        "search" => search(o, namespace),
        "commit" => commit(o, namespace),
        _ => recovery(o, namespace),
    }
}
fn main() -> Result<()> {
    let Some(options) = Options::parse()? else {
        return Ok(());
    };
    if cfg!(debug_assertions) {
        return Err("run with cargo bench (optimized bench profile)".into());
    }
    #[cfg(feature = "s3")]
    let s3_config = if options.backend == Backend::S3 {
        Some(backend::S3Config::from_env()?)
    } else {
        None
    };
    #[allow(unused_mut)]
    let mut environment = context(&options)?;
    #[cfg(feature = "s3")]
    if let Some(config) = &s3_config {
        environment["s3"] = config.metadata();
    }
    let mut results = Vec::new();
    for scenario in ["search", "commit", "recovery"] {
        if options.scenario == "all" || options.scenario == scenario {
            eprintln!(
                "Measuring {scenario}: seed={}, dimensions={}",
                options.seed, options.dimensions
            );
            results.push(match options.backend {
                Backend::Local => run_scenario(
                    &options,
                    scenario,
                    &LocalNamespace::new(&options.root, scenario)?,
                )?,
                Backend::S3 => {
                    #[cfg(feature = "s3")]
                    {
                        run_scenario(
                            &options,
                            scenario,
                            &backend::S3Namespace::new(
                                s3_config.as_ref().unwrap().clone(),
                                scenario,
                            )?,
                        )?
                    }
                    #[cfg(not(feature = "s3"))]
                    {
                        return Err("S3 benchmarks require --features s3".into());
                    }
                }
            });
        }
    }
    let mut report = json!({"schema_version": if options.backend == Backend::Local { 2 } else { 3 },
        "feature": options.feature, "phase": options.phase, "comparison_group": options.comparison_group,
        "git_revision": environment["git_revision"],
        "measurement_protocol": if options.backend == Backend::Local { "local-v2-individual-query-timers-rusage" } else { "s3-v1-individual-query-timers-rusage" }, "generator": "splitmix64-high24-uniform-f32-v1",
        "metric": "squared_euclidean",
        "counter_scope": "Engine-to-store calls and payload bytes only; excludes backend-internal I/O and inventory",
        "footprint_scope": "Logical objects, physical files, and summed file lengths; not allocated blocks or device bytes written",
        "config": options, "environment": environment, "results": results});
    if options.distribution == "clustered" {
        report["generator"] = json!("splitmix64-16-centers-plus-uniform-noise-0.1-f32-v1");
    }
    if options.backend == Backend::S3 {
        report["http_counter_scope"] = json!("HTTP client attempts during the measured workload; includes every LIST page and request-body envelope bytes; excludes inventory and setup. Transport errors exclude later response-body consumption errors.");
        report["footprint_scope"] = json!("Native object count and summed object lengths from an untimed listing; client/server physical file footprint unavailable");
    }
    serde_json::to_writer_pretty(io::stdout().lock(), &report)?;
    writeln!(io::stdout())?;
    Ok(())
}
