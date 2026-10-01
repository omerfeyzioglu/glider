//! Times 100-row overwrite requests through the serving engine.
//! Usage: cargo run --offline --release --example write_probe -- ROWS
use glider::{
    admission::Engine,
    retry::{Request, RequestId},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::LocalStore,
    Config, Metric, Mutation,
};
use std::{collections::BTreeMap, time::Instant};

fn main() {
    let rows: u64 = std::env::args().nth(1).unwrap().parse().unwrap();
    assert!(rows >= 100 && rows.is_multiple_of(100));
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let options = SegmentedOptions {
        resident_filter: Some(("cohort".into(), "one-percent".into())),
    };
    let mut serving = SegmentedServingOptions::m21(dir.path().join("cache"));
    serving.cache = None;
    serving.max_index_bytes = usize::MAX / 2;
    let mut db = SegmentedServing::open(
        LocalStore::open(dir.path().join("db")).unwrap(),
        config,
        options,
        serving,
    )
    .unwrap();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut batch = |db: &mut SegmentedServing<LocalStore>, ids: Vec<u64>, nonce: u64| {
        let mutations = ids
            .into_iter()
            .map(|id| {
                let mut metadata = BTreeMap::new();
                if id % 100 == 0 {
                    metadata.insert("cohort".to_owned(), "one-percent".to_owned());
                }
                Mutation::Put {
                    id,
                    vector: (0..128).map(|_| (next() % 256) as f32).collect(),
                    metadata,
                }
            })
            .collect();
        let request = Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: (nonce as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations,
        };
        let started = Instant::now();
        db.apply_request(request).unwrap();
        let elapsed = started.elapsed().as_secs_f64() * 1e3;
        if db.database().tail_objects() >= 32 {
            while db.maintenance_step().unwrap() {}
        }
        elapsed
    };
    for b in 0..rows / 100 {
        batch(&mut db, (b * 100..b * 100 + 100).collect(), b);
    }
    while db.maintenance_step().unwrap() {}
    let mut times: Vec<f64> = (0..400)
        .map(|n| {
            let start = (n * 7919 % (rows / 100)) * 100;
            batch(&mut db, (start..start + 100).collect(), 1_000_000 + n)
        })
        .collect();
    times.sort_by(f64::total_cmp);
    println!(
        "rows {rows} write ms p50 {:.2} p95 {:.2} max {:.2}",
        times[200], times[380], times[399]
    );
}
