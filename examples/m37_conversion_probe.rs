//! M37 stage 3: recall of the real clustered layout after conversion.
//!
//! Loads an fvecs prefix through the segmented engine exactly as the M31
//! load does (100-row requests, M21 one-percent metadata, a seal every 32
//! log objects, run consolidation), measures the per-seal layout at the M31
//! budget, converts the namespace with `convert_clustered`, and measures
//! cold clustered queries with the persisted five-bit posting sketches for
//! several probe counts and request/byte caps. The store is in memory and
//! counts range GETs and bytes; recall does not depend on the backend, and
//! no cache is attached, so every query is a cold plan. Exact top-10 (f64,
//! ID ties) is computed from the corpus.
//!
//! Usage: `m37_conversion_probe BASE.fvecs QUERY.fvecs ROWS [CENTROIDS]`.
//! Prints one JSON object.
use glider::{
    retry::{Request, RequestId},
    segmented::{ConvertOptions, ReadBudget, SegmentedDatabase, SegmentedOptions},
    store::ObjectStore,
    Config, Error, Metric, Mutation,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap},
    env, fs,
    io::{BufReader, Read},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const DIMENSIONS: usize = 128;
const QUERY_COUNT: usize = 200;
const K: usize = 10;
const FILTER: (&str, &str) = ("cohort", "one-percent");

/// Complete-object store in memory counting range GETs and their bytes.
#[derive(Clone, Default)]
struct Memory {
    objects: Arc<Mutex<BTreeMap<String, Arc<Vec<u8>>>>>,
    ranges: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl Memory {
    fn take_reads(&self) -> (u64, u64) {
        (
            self.ranges.swap(0, Ordering::SeqCst),
            self.bytes.swap(0, Ordering::SeqCst),
        )
    }
    fn payload(&self, prefix: &str) -> (usize, u64) {
        let objects = self.objects.lock().unwrap();
        let matching = objects.iter().filter(|(key, _)| key.starts_with(prefix));
        matching.fold((0, 0), |(count, bytes), (_, value)| {
            (count + 1, bytes + value.len() as u64)
        })
    }
}

impl ObjectStore for Memory {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        let object = self.objects.lock().unwrap().get(key).cloned();
        Ok(object.map(|bytes| bytes.to_vec()))
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload_len: usize,
    ) -> glider::Result<Option<Vec<u8>>> {
        let object = self.objects.lock().unwrap().get(key).cloned();
        let Some(bytes) = object else {
            return Ok(None);
        };
        if bytes.len() != payload_len || offset + length > bytes.len() {
            return Err(Error::Corrupt(format!("range outside {key}")));
        }
        self.ranges.fetch_add(1, Ordering::SeqCst);
        self.bytes.fetch_add(length as u64, Ordering::SeqCst);
        Ok(Some(bytes[offset..offset + length].to_vec()))
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
    fn create(&self, key: &str, value: &[u8]) -> glider::Result<()> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), Arc::new(value.to_vec()));
        Ok(())
    }
    fn remove(&self, key: &str) -> glider::Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

fn read_rows(path: &str, rows: usize, hash: &mut Sha256) -> Result<Vec<f32>> {
    let mut reader = BufReader::new(fs::File::open(path)?);
    let mut values = Vec::with_capacity(rows * DIMENSIONS);
    let mut bytes = [0_u8; 4 + DIMENSIONS * 4];
    for _ in 0..rows {
        reader.read_exact(&mut bytes)?;
        if u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize != DIMENSIONS {
            return Err("invalid fvecs dimension".into());
        }
        hash.update(bytes);
        values.extend(
            bytes[4..]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|part| f32::from_le_bytes(*part)),
        );
    }
    Ok(values)
}

fn distance(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&x, &y)| (f64::from(x) - f64::from(y)).powi(2))
        .sum()
}

/// Maps `f` over `0..items` on all cores, preserving order.
fn parallel<T: Send>(items: usize, f: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
    let chunk = items.div_ceil(threads).max(1);
    std::thread::scope(|scope| {
        let workers: Vec<_> = (0..items)
            .step_by(chunk)
            .map(|start| {
                let f = &f;
                scope.spawn(move || {
                    (start..(start + chunk).min(items))
                        .map(f)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect()
    })
}

/// Recall statistics of one setting over all queries.
fn measure(
    db: &SegmentedDatabase<Memory>,
    store: &Memory,
    queries: &[&[f32]],
    truth: &[Vec<u64>],
    budget: ReadBudget,
) -> Result<Value> {
    let mut recalls = Vec::new();
    let (mut short, mut requests, mut bytes, mut max_requests, mut max_bytes) = (0, 0, 0, 0, 0);
    let started = Instant::now();
    store.take_reads();
    for (query, truth) in queries.iter().zip(truth) {
        let found = db.search_selective_within(query, K, budget, &[])?;
        let (count, length) = store.take_reads();
        (requests, bytes) = (requests + count, bytes + length);
        (max_requests, max_bytes) = (max_requests.max(count), max_bytes.max(length));
        short += usize::from(found.len() < K);
        let truth: BTreeSet<_> = truth.iter().collect();
        let hits = found.iter().filter(|hit| truth.contains(&hit.id)).count();
        recalls.push(hits as f64 / K as f64);
    }
    let elapsed = started.elapsed().as_secs_f64() * 1e3;
    let count = queries.len() as f64;
    let mean = recalls.iter().sum::<f64>() / count;
    recalls.sort_by(f64::total_cmp);
    Ok(json!({
        "requests": budget.requests,
        "bytes_cap": budget.bytes,
        "mean_recall_at_10": (mean * 1e4).round() / 1e4,
        // Fifth percentile: the tenth lowest of 200 queries.
        "p5_recall_at_10": recalls[(recalls.len() * 5).div_ceil(100) - 1],
        "short_results": short,
        "mean_range_requests": requests as f64 / count,
        "max_range_requests": max_requests,
        "mean_range_bytes": bytes as f64 / count,
        "max_range_bytes": max_bytes,
        "mean_query_ms": elapsed / count,
    }))
}

fn run(args: &[String]) -> Result<Value> {
    let [base, query_path, rows, rest @ ..] = args else {
        return Err("usage: m37_conversion_probe BASE QUERY ROWS [CENTROIDS]".into());
    };
    let rows: usize = rows.parse()?;
    let centroids = rest.first().map(|value| value.parse()).transpose()?;
    let mut base_hash = Sha256::new();
    let corpus = read_rows(base, rows, &mut base_hash)?;
    let mut query_hash = Sha256::new();
    let query_values = read_rows(query_path, QUERY_COUNT, &mut query_hash)?;
    let row = |id: usize| &corpus[id * DIMENSIONS..(id + 1) * DIMENSIONS];
    let queries: Vec<&[f32]> = query_values.chunks(DIMENSIONS).collect();

    let started = Instant::now();
    let truth: Vec<Vec<u64>> = parallel(queries.len(), |index| {
        let mut heap = BinaryHeap::with_capacity(K + 1);
        for id in 0..rows {
            let entry = (distance(queries[index], row(id)).to_bits(), id as u64);
            if heap.len() < K {
                heap.push(entry);
            } else if entry < *heap.peek().unwrap() {
                heap.pop();
                heap.push(entry);
            }
        }
        heap.into_sorted_vec()
            .into_iter()
            .map(|(_, id)| id)
            .collect()
    });
    let oracle_ms = started.elapsed().as_millis();
    let mut truth_hash = Sha256::new();
    truth_hash.update(serde_json::to_vec(&truth)?);

    let config = Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    };
    let options = SegmentedOptions {
        resident_filter: Some((FILTER.0.into(), FILTER.1.into())),
        routed_keys: Vec::new(),
    };
    let store = Memory::default();
    let mut db = SegmentedDatabase::open_with_options(store.clone(), config, options.clone())?
        .with_query_threads(8);
    let started = Instant::now();
    for (index, start) in (0..rows).step_by(100).enumerate() {
        let mutations = (start..(start + 100).min(rows))
            .map(|id| Mutation::Put {
                id: id as u64,
                vector: row(id).to_vec(),
                metadata: if id % 100 == 0 {
                    BTreeMap::from([(FILTER.0.into(), FILTER.1.into())])
                } else {
                    BTreeMap::new()
                },
            })
            .collect();
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: (index as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations,
        })?;
        if db.tail_objects() >= 32 {
            db.seal_delta()?;
            while db.consolidate_runs_step()? {}
            while db.cleanup_step(64)? > 0 {}
        }
    }
    db.seal_delta()?;
    while db.consolidate_runs_step()? {}
    while db.cleanup_step(64)? > 0 {}
    let load_ms = started.elapsed().as_millis();
    let canonical = store.payload("sgpack-");
    let per_seal_index_bytes = db.selective_index_bytes();
    let m31_budget = ReadBudget {
        blocks: 12,
        requests: 8,
        bytes: 1024 * 1024,
        local_blocks: 0,
    };
    let per_seal = measure(&db, &store, &queries, &truth, m31_budget)?;
    eprintln!("loaded {rows} rows; per-seal {per_seal}");

    let started = Instant::now();
    let summary = db.convert_clustered(ConvertOptions {
        centroids,
        ..ConvertOptions::default()
    })?;
    let conversion_ms = started.elapsed().as_millis();
    eprintln!("converted: {summary:?}");
    let mut results = Vec::new();
    let settings = [
        (4, 8, 1024 * 1024),
        (8, 8, 1024 * 1024),
        (16, 8, 1024 * 1024),
        (32, 8, 1024 * 1024),
        (16, 4, 1024 * 1024),
        (16, 12, 1024 * 1024),
        (16, 8, 512 * 1024),
        (16, 8, 256 * 1024),
    ];
    for (probes, requests, bytes) in settings {
        db.set_cluster_probes(probes);
        let budget = ReadBudget {
            blocks: 12,
            requests,
            bytes,
            local_blocks: 0,
        };
        let mut result = measure(&db, &store, &queries, &truth, budget)?;
        result["probes"] = json!(probes);
        eprintln!("{result}");
        results.push(result);
    }
    let all = store.payload("sgpack-");
    Ok(json!({
        "probe": "m37_conversion_probe",
        "base_prefix_sha256": format!("{:x}", base_hash.finalize()),
        "query_prefix_sha256": format!("{:x}", query_hash.finalize()),
        "exact_top10_sha256": format!("{:x}", truth_hash.finalize()),
        "rows": rows,
        "queries": queries.len(),
        "k": K,
        "metric": "squared_euclidean",
        "oracle_ms": oracle_ms,
        "load_ms": load_ms,
        "canonical_packs": canonical.0,
        "canonical_pack_bytes": canonical.1,
        "per_seal_index_bytes": per_seal_index_bytes,
        "per_seal_m31_budget": per_seal,
        "conversion": summary,
        "conversion_ms": conversion_ms,
        "posting_pack_bytes": all.1 - canonical.1,
        "clustered_index_bytes": db.selective_index_bytes(),
        "sketch_rebuilds": db.sketch_rebuilds(),
        "results": results,
        "threads": std::thread::available_parallelism()?.get(),
        "os": env::consts::OS,
        "arch": env::consts::ARCH,
    }))
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    println!("{}", serde_json::to_string_pretty(&run(&args)?)?);
    Ok(())
}
