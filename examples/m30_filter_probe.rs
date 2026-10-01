//! M30 filtered selective-search quality on the M21 SIFT1M layout.
use glider::{
    admission::Engine,
    retry::{Request, RequestId},
    segmented::{ReadBudget, SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::LocalStore,
    Config, Metric, Mutation,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BinaryHeap},
    env, fs,
    io::{BufReader, Read},
    path::Path,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const DIMENSIONS: usize = 128;
const QUERY_COUNT: usize = 200;
const K: usize = 10;
const PREDICATES: [(&str, &str, u64, u64); 5] = [
    ("cohort", "one-percent", 100, 0),
    ("half", "yes", 2, 0),
    ("tenth", "yes", 10, 0),
    ("pct", "yes", 100, 1),
    ("permille", "yes", 1000, 7),
];

fn row(reader: &mut impl Read, hash: Option<&mut Sha256>) -> Result<Vec<f32>> {
    let mut bytes = [0_u8; 4 + DIMENSIONS * 4];
    reader.read_exact(&mut bytes)?;
    if u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize != DIMENSIONS {
        return Err("invalid SIFT1M fvecs dimensions".into());
    }
    if let Some(hash) = hash {
        hash.update(bytes);
    }
    Ok(bytes[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|part| f32::from_le_bytes(*part))
        .collect())
}

fn metadata(id: u64) -> BTreeMap<String, String> {
    PREDICATES
        .iter()
        .filter(|(_, _, modulus, remainder)| id % modulus == *remainder)
        .map(|(key, value, _, _)| ((*key).into(), (*value).into()))
        .collect()
}

fn exact(base: &[Vec<f32>], query: &[f32]) -> [Vec<u64>; PREDICATES.len()] {
    let mut heaps: [BinaryHeap<(u64, u64)>; PREDICATES.len()] =
        std::array::from_fn(|_| BinaryHeap::new());
    for (id, vector) in base.iter().enumerate() {
        let matches: [bool; PREDICATES.len()] = std::array::from_fn(|index| {
            let (_, _, modulus, remainder) = PREDICATES[index];
            id as u64 % modulus == remainder
        });
        if !matches.contains(&true) {
            continue;
        }
        let distance: f64 = vector
            .iter()
            .zip(query)
            .map(|(&v, &q)| {
                let difference = f64::from(v) - f64::from(q);
                difference * difference
            })
            .sum();
        let entry = (distance.to_bits(), id as u64);
        for (matches, heap) in matches.into_iter().zip(&mut heaps) {
            if matches {
                if heap.len() < K {
                    heap.push(entry);
                } else if entry < *heap.peek().unwrap() {
                    heap.pop();
                    heap.push(entry);
                }
            }
        }
    }
    heaps.map(|heap| heap.into_iter().map(|(_, id)| id).collect())
}

#[derive(Default)]
struct Quality {
    recall: Vec<f64>,
    result_count: usize,
    short: usize,
}

impl Quality {
    fn observe(&mut self, found: &[u64], truth: &[u64]) {
        self.recall
            .push(found.iter().filter(|id| truth.contains(id)).count() as f64 / K as f64);
        self.result_count += found.len();
        self.short += usize::from(found.len() < K);
    }

    fn report(mut self) -> Value {
        self.recall.sort_by(f64::total_cmp);
        let queries = self.recall.len();
        json!({
            "queries": queries,
            "mean_recall_at_10": self.recall.iter().sum::<f64>() / queries as f64,
            "fifth_percentile_recall_at_10": self.recall[(queries * 5).div_ceil(100) - 1],
            "mean_result_count": self.result_count as f64 / queries as f64,
            "short_result_fraction": self.short as f64 / queries as f64,
        })
    }
}

fn run(args: &[String]) -> Result<Value> {
    let [base_path, query_path, rows_arg, output] = args else {
        return Err("usage: m30_filter_probe BASE.fvecs QUERY.fvecs ROWS OUT_DIR".into());
    };
    let rows: usize = rows_arg.parse()?;
    if rows < 10 || !rows.is_multiple_of(100) {
        return Err("ROWS must be at least 10 and a multiple of 100".into());
    }
    let output = Path::new(output);
    fs::create_dir_all(output)?;
    let namespace = output.join("namespace");
    if namespace.exists() {
        return Err("OUT_DIR/namespace already exists; use a fresh output directory".into());
    }
    let mut base_file = BufReader::new(fs::File::open(base_path)?);
    let mut query_file = BufReader::new(fs::File::open(query_path)?);
    let queries: Vec<_> = (0..QUERY_COUNT)
        .map(|_| row(&mut query_file, None))
        .collect::<Result<_>>()?;
    let config = Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    };
    let mut serving = SegmentedServingOptions::m21(output.join("cache"));
    serving.cache = None;
    let serving_budget = serving.read_budget;
    let budgets = [
        ("serving", serving_budget),
        (
            "2x",
            ReadBudget {
                blocks: serving_budget.blocks * 2,
                requests: serving_budget.requests * 2,
                bytes: serving_budget.bytes * 2,
            },
        ),
        (
            "4x",
            ReadBudget {
                blocks: serving_budget.blocks * 4,
                requests: serving_budget.requests * 4,
                bytes: serving_budget.bytes * 4,
            },
        ),
    ];
    let mut db = SegmentedServing::open(
        LocalStore::open(&namespace)?,
        config,
        SegmentedOptions {
            resident_filter: Some(("cohort".into(), "one-percent".into())),
        },
        serving,
    )?;
    let mut hash = Sha256::new();
    let mut base = Vec::with_capacity(rows);
    let mut sequence = 0;
    for batch in 0..rows / 100 {
        let mutations = (batch * 100..(batch + 1) * 100)
            .map(|id| {
                let vector = row(&mut base_file, Some(&mut hash))?;
                base.push(vector.clone());
                Ok(Mutation::Put {
                    id: id as u64,
                    vector,
                    metadata: metadata(id as u64),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut nonce = [0xff; 16];
        nonce[..8].copy_from_slice(&(batch as u64).to_le_bytes());
        sequence = db
            .apply_request(Request {
                id: RequestId {
                    boundary: sequence,
                    nonce,
                },
                conditions: Vec::new(),
                mutations,
            })?
            .sequence;
        if db.database().tail_objects() >= 32 {
            while db.maintenance_step()? {}
        }
        if (batch + 1) % 250 == 0 {
            eprintln!("loaded {} rows", (batch + 1) * 100);
        }
    }
    while db.maintenance_step()? {}
    let layout = json!({
        "runs": db.database().run_count(),
        "blocks": db.database().block_count(),
        "tail_objects": db.database().tail_objects(),
        "sequence": sequence,
    });
    let mut quality: BTreeMap<&str, BTreeMap<&str, Quality>> = BTreeMap::new();
    for (index, query) in queries.iter().enumerate() {
        let truth = exact(&base, query);
        for (predicate_index, &(key, value, _, _)) in PREDICATES.iter().enumerate() {
            for &(budget_name, budget) in &budgets {
                let found =
                    db.database()
                        .search_selective_within(query, K, budget, &[(key, value)])?;
                let ids: Vec<_> = found.iter().map(|neighbor| neighbor.id).collect();
                quality
                    .entry(key)
                    .or_default()
                    .entry(budget_name)
                    .or_default()
                    .observe(&ids, &truth[predicate_index]);
            }
        }
        if (index + 1) % 25 == 0 {
            eprintln!("measured {} queries", index + 1);
        }
    }
    db.close()?;
    let quality: BTreeMap<_, _> = quality
        .into_iter()
        .map(|(predicate, budgets)| {
            (
                predicate,
                budgets
                    .into_iter()
                    .map(|(name, value)| (name, value.report()))
                    .collect::<BTreeMap<_, _>>(),
            )
        })
        .collect();
    Ok(json!({
        "base_prefix_sha256": format!("{:x}", hash.finalize()),
        "base_prefix_bytes": rows * (4 + DIMENSIONS * 4),
        "rows": rows,
        "queries": QUERY_COUNT,
        "k": K,
        "metric": "squared_euclidean",
        "store": "LocalStore",
        "request_batch_rows": 100,
        "resident_filter": ["cohort", "one-percent"],
        "budgets": budgets.iter().map(|(name, budget)| (*name, json!({"blocks":budget.blocks,"requests":budget.requests,"bytes":budget.bytes}))).collect::<BTreeMap<_, _>>(),
        "predicates": PREDICATES.iter().map(|&(key, value, modulus, remainder)| (key, json!({"value":value,"id_modulus":modulus,"id_remainder":remainder,"matching_rows":(0..rows as u64).filter(|id| id % modulus == remainder).count()}))).collect::<BTreeMap<_, _>>(),
        "layout": layout,
        "results": quality,
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    println!("{}", serde_json::to_string(&run(&args)?)?);
    Ok(())
}
