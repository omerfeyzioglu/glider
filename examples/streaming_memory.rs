//! Targeted resident-memory comparison for full and streaming exact readers.
//! cargo build --release --example streaming_memory
//! target/release/examples/streaming_memory prepare target/streaming-memory-db
//! /usr/bin/time -l target/release/examples/streaming_memory full target/streaming-memory-db
//! /usr/bin/time -l target/release/examples/streaming_memory streaming target/streaming-memory-db
use glider::{store::LocalStore, streaming::StreamingDatabase, Config, Database, Metric, Mutation};
use std::{collections::BTreeMap, hint::black_box, path::Path, time::Instant};

const ROWS: u64 = 20_000;
const DIMENSIONS: usize = 64;
const SEED: u64 = 42;
const CHUNK_BYTES: usize = 128 * 1024;

fn config() -> Config {
    Config {
        dimensions: DIMENSIONS,
        metric: Metric::SquaredEuclidean,
    }
}

fn main() -> glider::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err(glider::Error::Invalid(
            "usage: streaming_memory prepare|full|streaming PATH".into(),
        ));
    }
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "prepare" => {
            let mut state = SEED;
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
                .collect();
            let mut db = Database::open(LocalStore::open(path)?, config())?;
            db.apply_batch(mutations)?;
            db.compact_chunked(CHUNK_BYTES)?;
            println!("prepared rows={ROWS} dimensions={DIMENSIONS} seed={SEED} chunk_bytes={CHUNK_BYTES}");
        }
        "full" => {
            let start = Instant::now();
            let db = Database::open(LocalStore::open(path)?, config())?;
            let query = vec![0.5; DIMENSIONS];
            let neighbors = black_box(db.search(&query, 10)?);
            println!(
                "mode=full elapsed_ms={} top_id={}",
                start.elapsed().as_millis(),
                neighbors[0].id
            );
        }
        "streaming" => {
            let start = Instant::now();
            let db = StreamingDatabase::open(LocalStore::open(path)?, config())?;
            let query = vec![0.5; DIMENSIONS];
            let neighbors = black_box(db.search(&query, 10)?);
            println!(
                "mode=streaming elapsed_ms={} top_id={}",
                start.elapsed().as_millis(),
                neighbors[0].id
            );
        }
        _ => return Err(glider::Error::Invalid("unknown mode".into())),
    }
    Ok(())
}
