//! Reproducible open probe with emulated first-byte latency and bandwidth.
//! Usage: cargo run --offline --release --example m39_open_probe -- ROWS RTT_MS MIB_PER_S
//! Builds a synthetic clustered namespace before enabling request delays.
use glider::{
    retry::{Request, RequestId},
    segmented::{ConvertOptions, OpenProfile, SegmentedDatabase, SegmentedOptions},
    store::ObjectStore,
    Config, Error, Metric, Mutation, Result,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Default)]
struct Memory {
    objects: Arc<Mutex<BTreeMap<String, Arc<Vec<u8>>>>>,
    reads: Arc<Mutex<BTreeMap<String, (u64, u64)>>>,
    delayed: Arc<AtomicBool>,
    rtt: Duration,
    bytes_per_second: u64,
}

impl Memory {
    fn delay(&self, kind: &str, bytes: usize) {
        if !self.delayed.load(Ordering::Relaxed) {
            return;
        }
        let mut reads = self.reads.lock().unwrap();
        let entry = reads.entry(kind.into()).or_default();
        entry.0 += 1;
        entry.1 += bytes as u64;
        drop(reads);
        let transfer = Duration::from_secs_f64(bytes as f64 / self.bytes_per_second as f64);
        std::thread::sleep(self.rtt + transfer);
    }
    fn value(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.objects.lock().unwrap().get(key).cloned()
    }
}

impl ObjectStore for Memory {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let value = self.value(key);
        self.delay(
            key.split('-').next().unwrap_or(key),
            value.as_ref().map_or(0, |v| v.len()),
        );
        Ok(value.map(|v| v.to_vec()))
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload: usize,
    ) -> Result<Option<Vec<u8>>> {
        let value = self.value(key);
        self.delay("range", length);
        let Some(value) = value else { return Ok(None) };
        if value.len() != payload || offset.checked_add(length).is_none_or(|end| end > payload) {
            return Err(Error::Corrupt(format!("bad range: {key}")));
        }
        Ok(Some(value[offset..offset + length].to_vec()))
    }
    fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut result = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(32) {
            std::thread::scope(|scope| {
                let jobs: Vec<_> = chunk
                    .iter()
                    .map(|key| scope.spawn(|| self.get(key)))
                    .collect();
                for job in jobs {
                    result.push(job.join().unwrap()?);
                }
                Ok::<_, Error>(())
            })?;
        }
        Ok(result)
    }
    fn get_ranges(&self, ranges: &[(&str, usize, usize, usize)]) -> Result<Vec<Option<Vec<u8>>>> {
        let mut result = Vec::with_capacity(ranges.len());
        for chunk in ranges.chunks(32) {
            std::thread::scope(|scope| {
                let jobs: Vec<_> = chunk
                    .iter()
                    .map(|&(key, offset, length, payload)| {
                        scope.spawn(move || self.get_range(key, offset, length, payload))
                    })
                    .collect();
                for job in jobs {
                    result.push(job.join().unwrap()?);
                }
                Ok::<_, Error>(())
            })?;
        }
        Ok(result)
    }
    fn list(&self) -> Result<Vec<String>> {
        let keys: Vec<_> = self.objects.lock().unwrap().keys().cloned().collect();
        self.delay("list", keys.iter().map(String::len).sum());
        Ok(keys)
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut objects = self.objects.lock().unwrap();
        if objects.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        objects.insert(key.into(), Arc::new(value.to_vec()));
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

fn vector(id: u64) -> Vec<f32> {
    let mut state = id ^ 0x9e37_79b9_7f4a_7c15;
    (0..128)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 255) as f32
        })
        .collect()
}

fn millis(time: Duration) -> f64 {
    time.as_secs_f64() * 1000.0
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err("usage: m39_open_probe ROWS RTT_MS MIB_PER_S".into());
    }
    let rows: u64 = args[0].parse()?;
    let rtt_ms: u64 = args[1].parse()?;
    let mib_per_second: u64 = args[2].parse()?;
    assert!(rows > 0 && rows.is_multiple_of(100) && mib_per_second > 0);
    let store = Memory {
        rtt: Duration::from_millis(rtt_ms),
        bytes_per_second: mib_per_second * 1024 * 1024,
        ..Memory::default()
    };
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let options = SegmentedOptions {
        resident_filter: Some(("cohort".into(), "one-percent".into())),
        routed_keys: Vec::new(),
    };
    let mut db = SegmentedDatabase::open_with_options(store.clone(), config, options.clone())?;
    let began = Instant::now();
    for batch in 0..rows / 100 {
        let mutations = (batch * 100..batch * 100 + 100)
            .map(|id| Mutation::Put {
                id,
                vector: vector(id),
                metadata: if id % 100 == 0 {
                    BTreeMap::from([("cohort".into(), "one-percent".into())])
                } else {
                    BTreeMap::new()
                },
            })
            .collect();
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: (batch as u128).to_le_bytes(),
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
    eprintln!("built {rows} rows in {:.1}s", began.elapsed().as_secs_f64());
    db.convert_clustered(ConvertOptions::default())?;
    drop(db);
    store.delayed.store(true, Ordering::Relaxed);
    let mut profile = OpenProfile::default();
    let started = Instant::now();
    let reopened = SegmentedDatabase::open_with_options_profiled(
        store.clone(),
        config,
        options,
        &mut profile,
    )?;
    let elapsed = started.elapsed();
    let reads = store.reads.lock().unwrap().clone();
    let requests: u64 = reads.values().map(|(count, _)| count).sum();
    let bytes: u64 = reads.values().map(|(_, bytes)| bytes).sum();
    println!(
        "{}",
        json!({
            "mode": "uncached",
            "rows": rows, "rtt_ms": rtt_ms, "mib_per_second": mib_per_second,
            "open_ms": millis(elapsed), "requests": requests, "bytes": bytes,
            "reads_by_kind": reads,
            "phases_ms": {
                "list_metadata": millis(profile.list_metadata),
                "root_manifests": millis(profile.root_manifests),
                "run_indexes": millis(profile.run_indexes),
                "run_index_reads": millis(profile.run_index_reads),
                "run_index_cpu": millis(profile.run_indexes.saturating_sub(profile.run_index_reads)),
                "tail_replay": millis(profile.tail_replay),
                "routing": millis(profile.routing),
                "catalog": millis(profile.catalog),
                "catalog_reads": millis(profile.catalog_reads),
                "sketch_frames": millis(profile.sketch_frames),
                "sketch_reads": millis(profile.sketch_reads),
                "sketch_decode": millis(profile.sketch_frames.saturating_sub(profile.sketch_reads)),
                "routing_build": millis(profile.routing.saturating_sub(profile.catalog + profile.sketch_frames)),
                "finish": millis(profile.finish),
            },
            "sketch_rebuilds": reopened.sketch_rebuilds(),
            "index_bytes": reopened.selective_index_bytes(),
        })
    );
    drop(reopened);
    let directory = tempfile::tempdir()?;
    for mode in ["cache_fill", "cache_reopen"] {
        store.reads.lock().unwrap().clear();
        let mut profile = OpenProfile::default();
        let started = Instant::now();
        let reopened = SegmentedDatabase::open_with_options_profiled_cached(
            store.clone(),
            config,
            SegmentedOptions {
                resident_filter: Some(("cohort".into(), "one-percent".into())),
                routed_keys: Vec::new(),
            },
            &mut profile,
            Some((directory.path(), 0, 256 * 1024 * 1024)),
        )?;
        let elapsed = started.elapsed();
        let reads = store.reads.lock().unwrap().clone();
        println!(
            "{}",
            json!({
                "mode": mode, "rows": rows, "open_ms": millis(elapsed),
                "requests": reads.values().map(|(count, _)| count).sum::<u64>(),
                "bytes": reads.values().map(|(_, bytes)| bytes).sum::<u64>(),
                "reads_by_kind": reads,
                "phases_ms": {
                    "list_metadata": millis(profile.list_metadata),
                    "root_manifests": millis(profile.root_manifests),
                    "run_indexes": millis(profile.run_indexes),
                    "run_index_reads": millis(profile.run_index_reads),
                    "tail_replay": millis(profile.tail_replay),
                    "routing": millis(profile.routing),
                    "catalog": millis(profile.catalog),
                    "sketch_frames": millis(profile.sketch_frames),
                    "sketch_reads": millis(profile.sketch_reads),
                    "routing_build": millis(profile.routing.saturating_sub(profile.catalog + profile.sketch_frames)),
                },
                "sketch_rebuilds": reopened.sketch_rebuilds(),
                "index_bytes": reopened.selective_index_bytes(),
                "cache": reopened.cache_stats()?,
            })
        );
    }
    Ok(())
}
