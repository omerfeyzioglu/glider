use glider::{
    retry::{Request, RequestId},
    segmented::{OpenProfile, SegmentedDatabase, SegmentedOptions},
    store::{LocalStore, ObjectStore},
    Config, Metric, Mutation, Result,
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

struct Counted {
    inner: LocalStore,
    remote: Arc<AtomicU64>,
    parallel: bool,
}

impl Counted {
    fn open(path: &Path, remote: Arc<AtomicU64>, parallel: bool) -> Self {
        Self {
            inner: LocalStore::open(path).unwrap(),
            remote,
            parallel,
        }
    }
}

impl ObjectStore for Counted {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        if key.starts_with("sgindex-") {
            self.remote.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get(key)
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload: usize,
    ) -> Result<Option<Vec<u8>>> {
        if key.starts_with("sgpack-") {
            self.remote.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_range(key, offset, length, payload)
    }
    fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        if !self.parallel {
            return keys.iter().map(|key| self.get(key)).collect();
        }
        std::thread::scope(|scope| {
            let jobs: Vec<_> = keys
                .iter()
                .map(|key| scope.spawn(|| self.get(key)))
                .collect();
            jobs.into_iter().map(|job| job.join().unwrap()).collect()
        })
    }
    fn get_ranges(&self, ranges: &[(&str, usize, usize, usize)]) -> Result<Vec<Option<Vec<u8>>>> {
        if !self.parallel {
            return ranges
                .iter()
                .map(|&(key, offset, length, payload)| self.get_range(key, offset, length, payload))
                .collect();
        }
        std::thread::scope(|scope| {
            let jobs: Vec<_> = ranges
                .iter()
                .map(|&(key, offset, length, payload)| {
                    scope.spawn(move || self.get_range(key, offset, length, payload))
                })
                .collect();
            jobs.into_iter().map(|job| job.join().unwrap()).collect()
        })
    }
    fn list(&self) -> Result<Vec<String>> {
        self.inner.list()
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        self.inner.create(key, value)
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.inner.remove(key)
    }
}

fn config() -> Config {
    Config {
        dimensions: 8,
        metric: Metric::SquaredEuclidean,
    }
}
fn options() -> SegmentedOptions {
    SegmentedOptions::default()
}

#[test]
fn cached_open_matches_remote_and_recovers_missing_or_corrupt_entries() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("namespace");
    let cache = temp.path().join("cache");
    let remote = Arc::new(AtomicU64::new(0));
    let mut db = SegmentedDatabase::open_with_options(
        Counted::open(&path, remote.clone(), false),
        config(),
        options(),
    )
    .unwrap();
    for batch in 0..3_u64 {
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: (batch as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations: (0..100)
                .map(|n| Mutation::Put {
                    id: batch * 100 + n,
                    vector: vec![n as f32; 8],
                    metadata: BTreeMap::new(),
                })
                .collect(),
        })
        .unwrap();
        db.seal_delta().unwrap();
    }
    drop(db);
    let plain = SegmentedDatabase::open_with_options(
        Counted::open(&path, remote.clone(), false),
        config(),
        options(),
    )
    .unwrap();
    let expected = plain.search_exact(&[20.0; 8], 10, &[]).unwrap();
    drop(plain);
    remote.store(0, Ordering::Relaxed);
    let open = |parallel| {
        SegmentedDatabase::open_with_options_profiled_cached(
            Counted::open(&path, remote.clone(), parallel),
            config(),
            options(),
            &mut OpenProfile::default(),
            Some((&cache, 0, 16 * 1024 * 1024)),
        )
        .unwrap()
    };
    let filled = open(true);
    assert!(remote.load(Ordering::Relaxed) >= 2);
    let directory = cache.join("glider-block-cache-v1");
    let entries: Vec<_> = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(entries.len() >= 2);
    assert_eq!(filled.search_exact(&[20.0; 8], 10, &[]).unwrap(), expected);
    drop(filled);
    remote.store(0, Ordering::Relaxed);
    let warm = open(true);
    assert_eq!(remote.load(Ordering::Relaxed), 0);
    assert_eq!(warm.search_exact(&[20.0; 8], 10, &[]).unwrap(), expected);
    drop(warm);
    std::fs::remove_file(&entries[0]).unwrap();
    std::fs::write(&entries[1], b"corrupt").unwrap();
    remote.store(0, Ordering::Relaxed);
    let recovered = open(true);
    assert!(remote.load(Ordering::Relaxed) >= 2);
    assert_eq!(
        recovered.search_exact(&[20.0; 8], 10, &[]).unwrap(),
        expected
    );
}
