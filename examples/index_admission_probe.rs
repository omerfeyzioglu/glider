//! Local 10k-row 128/768-dimensional index-admission regression probe.
//! Synthetic SplitMix64 seed 42, batches of 100, 50 independent queries; no cache,
//! no automatic conversion. Reports actual timings and exact-oracle recall.
use glider::{
    admission::Engine,
    retry::{Request, RequestId},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::LocalStore,
    Config, Metric, Mutation,
};
use std::{collections::BTreeMap, path::PathBuf, time::Instant};
fn random(state: &mut u64) -> f32 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^= z >> 31;
    ((z >> 40) as f32) / (1u32 << 24) as f32 * 2.0 - 1.0
}
fn percentile(xs: &[f64], p: f64) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(f64::total_cmp);
    v[((v.len() as f64 * p).ceil() as usize).saturating_sub(1)]
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let dim: usize = args[1].parse().unwrap();
    let base = PathBuf::from(&args[2]);
    assert!(!base.exists(), "use a fresh output directory");
    std::fs::create_dir_all(&base).unwrap();
    let rows = 10000;
    let cfg = Config {
        dimensions: dim,
        metric: Metric::SquaredEuclidean,
    };
    let mut opts = SegmentedServingOptions::m31(base.join("cache"));
    opts.cache = None;
    opts.seal_tail_objects = 8;
    opts.auto_cluster_rows = 0;
    opts.auto_recluster_factor = 0;
    opts.warm_unit_bytes = 0;
    let mut engine = SegmentedServing::open(
        LocalStore::open(base.join("db")).unwrap(),
        cfg,
        SegmentedOptions::default(),
        opts.clone(),
    )
    .unwrap();
    let mut state = 42;
    let mut writes = vec![];
    let mut maintenance = 0.0;
    for batch in 0..rows / 100 {
        let mutations = (batch * 100..(batch + 1) * 100)
            .map(|id| Mutation::Put {
                id: id as u64,
                vector: (0..dim).map(|_| random(&mut state)).collect(),
                metadata: BTreeMap::new(),
            })
            .collect();
        let req = Request {
            id: RequestId {
                boundary: engine.sequence(),
                nonce: (batch as u128 + 1).to_le_bytes(),
            },
            conditions: vec![],
            mutations,
        };
        let t = Instant::now();
        engine.apply_request(req).unwrap();
        writes.push(t.elapsed().as_secs_f64() * 1000.0);
        let t = Instant::now();
        while engine.maintenance_step().unwrap() {}
        maintenance += t.elapsed().as_secs_f64() * 1000.0;
    }
    let bytes = engine.database().selective_index_bytes();
    let mut queries = vec![];
    let mut recall = 0.0;
    let mut hash = 0xcbf29ce484222325u64;
    let mut qstate = 42 ^ 0xd1b54a32d192ed03;
    let mut qs = vec![];
    for _ in 0..50 {
        let query: Vec<_> = (0..dim).map(|_| random(&mut qstate)).collect();
        let exact = engine.database().search_exact(&query, 10, &[]).unwrap();
        let t = Instant::now();
        let found = engine
            .database()
            .search_selective_within(&query, 10, opts.read_budget, &[])
            .unwrap();
        queries.push(t.elapsed().as_secs_f64() * 1000.0);
        recall += found
            .iter()
            .filter(|n| exact.iter().any(|e| e.id == n.id))
            .count() as f64
            / 10.0;
        for hit in &found {
            hash = (hash ^ hit.id).wrapping_mul(0x100000001b3);
        }
        qs.push(query);
    }
    engine.close().unwrap();
    let t = Instant::now();
    let reopened = SegmentedServing::open(
        LocalStore::open(base.join("db")).unwrap(),
        cfg,
        SegmentedOptions::default(),
        opts,
    )
    .unwrap();
    let reopen = t.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(
        reopened
            .database()
            .search_exact(&qs[0], rows, &[])
            .unwrap()
            .len(),
        rows
    );
    reopened.close().unwrap();
    println!("{{\"dimensions\":{dim},\"rows\":{rows},\"seed\":42,\"batch\":100,\"queries\":50,\"index_bytes\":{bytes},\"write_p50_ms\":{},\"write_p95_ms\":{},\"query_p50_ms\":{},\"query_p95_ms\":{},\"maintenance_ms\":{maintenance},\"reopen_ms\":{reopen},\"recall_at_10\":{},\"result_hash\":\"{hash:016x}\"}}",percentile(&writes,0.50),percentile(&writes,0.95),percentile(&queries,0.50),percentile(&queries,0.95),recall/50.0);
}
