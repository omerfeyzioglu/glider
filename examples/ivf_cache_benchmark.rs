//! Targeted local IVF training versus persisted-cache load measurement.
//! cargo run --release --example ivf_cache_benchmark
use glider::{ivf::IvfConfig, store::LocalStore, Config, Database, Metric, Mutation};
use std::{collections::BTreeMap, time::Instant};

fn main() -> glider::Result<()> {
    const ROWS: u64 = 512;
    const DIMENSIONS: usize = 64;
    const SAMPLES: usize = 5;
    let temp = tempfile::tempdir().map_err(glider::Error::Io)?;
    let path = temp.path().join("db");
    let config = Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    };
    let options = IvfConfig {
        partitions: 16,
        iterations: 8,
        seed: 42,
    };
    let mut db = Database::open(LocalStore::open(&path)?, config)?;
    let mut state = 42_u64;
    let mutations = (0..ROWS)
        .map(|id| {
            let vector = (0..DIMENSIONS)
                .map(|_| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    ((state >> 40) as f32) / (1_u32 << 24) as f32
                })
                .collect();
            Mutation::Put {
                id,
                vector,
                metadata: BTreeMap::new(),
            }
        })
        .collect::<Vec<_>>();
    db.apply_batch(mutations)?;
    db.compact()?;
    drop(db);

    let mut build_us = Vec::new();
    for _ in 0..SAMPLES {
        let mut db = Database::open(LocalStore::open(&path)?, config)?;
        let start = Instant::now();
        db.build_ivf(options)?;
        build_us.push(start.elapsed().as_micros());
    }
    let mut db = Database::open(LocalStore::open(&path)?, config)?;
    db.load_or_build_ivf(options)?;
    drop(db);
    let mut load_us = Vec::new();
    for _ in 0..SAMPLES {
        let mut db = Database::open(LocalStore::open(&path)?, config)?;
        let start = Instant::now();
        db.load_or_build_ivf(options)?;
        load_us.push(start.elapsed().as_micros());
        let query = vec![0.5; DIMENSIONS];
        assert_eq!(
            db.search_ivf(&query, ROWS as usize, options.partitions)?
                .neighbors,
            db.search(&query, ROWS as usize)?
        );
    }
    build_us.sort_unstable();
    load_us.sort_unstable();
    println!(
        "local warm IVF; rows={ROWS} dimensions={DIMENSIONS} partitions={} iterations={} seed={} samples={SAMPLES}; build_us={build_us:?} load_us={load_us:?}; medians={}us -> {}us",
        options.partitions, options.iterations, options.seed, build_us[SAMPLES / 2], load_us[SAMPLES / 2]
    );
    Ok(())
}
