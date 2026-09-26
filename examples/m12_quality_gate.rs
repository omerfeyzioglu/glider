//! Feasibility gate, not a production persisted ANN reader. Logical layout costs
//! are computed from actual serialized candidate objects; no remote latency claim.
use glider::{ivf::IvfConfig, store::ObjectStore, Config, Database, Error, Metric, Mutation};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
struct CandidateDocument {
    vector: Vec<f32>,
    metadata: BTreeMap<String, String>,
}
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(deny_unknown_fields)]
struct CandidatePartition {
    version: u32,
    sequence: u64,
    config: Config,
    documents: Vec<(u64, CandidateDocument)>,
}
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    time::Instant,
};

#[derive(Serialize)]
struct CandidateBundle<'a> {
    version: u32,
    partitions: &'a [CandidatePartition],
}

#[derive(Clone, Default)]
struct Memory(Rc<RefCell<BTreeMap<String, Vec<u8>>>>);
impl ObjectStore for Memory {
    fn get(&self, key: &str) -> glider::Result<Option<Vec<u8>>> {
        Ok(self.0.borrow().get(key).cloned())
    }
    fn list(&self) -> glider::Result<Vec<String>> {
        Ok(self.0.borrow().keys().cloned().collect())
    }
    fn remove(&mut self, key: &str) -> glider::Result<()> {
        self.0.borrow_mut().remove(key);
        Ok(())
    }
    fn create(&mut self, key: &str, bytes: &[u8]) -> glider::Result<()> {
        let mut map = self.0.borrow_mut();
        if map.contains_key(key) {
            return Err(Error::Exists(key.into()));
        }
        map.insert(key.into(), bytes.to_vec());
        Ok(())
    }
}
fn vectors(seed: u64, count: usize) -> Vec<Vec<f32>> {
    let mut state = seed;
    (0..count)
        .map(|_| {
            (0..64)
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
fn score(a: &[f32], b: &[f32]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&a, &b)| (f64::from(a) - f64::from(b)).powi(2))
        .sum()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut reports = Vec::new();
    for clustered in [false, true] {
        let mut data = vectors(42, 2000);
        let mut queries = vectors(42 ^ 0xd1b54a32d192ed03, 1000);
        if clustered {
            let centers = vectors(42 ^ 0xa0761d6478bd642f, 16);
            for points in [&mut data, &mut queries] {
                for (i, point) in points.iter_mut().enumerate() {
                    for (v, c) in point.iter_mut().zip(&centers[i % 16]) {
                        *v = *c + *v * 0.1;
                    }
                }
            }
        }
        let store = Memory::default();
        let config = Config {
            dimensions: 64,
            metric: Metric::SquaredEuclidean,
        };
        let mut db = Database::open(store.clone(), config)?;
        for start in (0..2000).step_by(100) {
            db.apply_batch(
                (start..start + 100)
                    .map(|id| Mutation::Put {
                        id: id as u64,
                        vector: data[id].clone(),
                        metadata: if id % 100 == 0 {
                            BTreeMap::from([("selected".into(), "true".into())])
                        } else {
                            BTreeMap::new()
                        },
                    })
                    .collect(),
            )?;
        }
        let start = Instant::now();
        db.load_or_build_ivf(IvfConfig {
            partitions: 32,
            iterations: 8,
            seed: 42,
        })?;
        let build_ns = start.elapsed().as_nanos();
        let key = store
            .list()?
            .into_iter()
            .find(|key| key.starts_with("ivf-"))
            .unwrap();
        let cache: Value = serde_json::from_slice(&store.get(&key)?.unwrap())?;
        let centers: Vec<Vec<f32>> = serde_json::from_value(cache["index"]["centers"].clone())?;
        let postings: Vec<Vec<usize>> = serde_json::from_value(cache["index"]["postings"].clone())?;
        // Versioned candidate wire objects contain full vectors + metadata for
        // exact reranking. Serialize and round-trip, but do not publish a new
        // engine format before the quality/resource gate is satisfied.
        let partitions: Vec<_> = postings
            .iter()
            .map(|ids| CandidatePartition {
                version: 1,
                sequence: 20,
                config,
                documents: ids
                    .iter()
                    .map(|id| {
                        (
                            *id as u64,
                            CandidateDocument {
                                vector: data[*id].clone(),
                                metadata: db.get_metadata(*id as u64).unwrap().clone(),
                            },
                        )
                    })
                    .collect(),
            })
            .collect();
        let sizes: Vec<_> = partitions
            .iter()
            .map(|p| {
                let bytes = serde_json::to_vec(p).unwrap();
                assert_eq!(
                    serde_json::from_slice::<CandidatePartition>(&bytes).unwrap(),
                    *p
                );
                bytes.len()
            })
            .collect();
        let bundles: Vec<_> = partitions
            .chunks(4)
            .map(|p| {
                serde_json::to_vec(&CandidateBundle {
                    version: 1,
                    partitions: p,
                })
                .unwrap()
                .len()
            })
            .collect();
        let mut cases = Vec::new();
        for filtered in [false, true] {
            let filter = if filtered {
                vec![("selected", "true")]
            } else {
                vec![]
            };
            let exact: Vec<_> = queries
                .iter()
                .map(|q| db.search_filtered(q, 10, &filter).unwrap())
                .collect();
            for (probes, adaptive) in [(8, false), (16, false), (24, false), (32, false), (8, true)]
            {
                let mut raw = Vec::new();
                for (query, oracle) in queries.iter().zip(&exact) {
                    let result = if adaptive {
                        db.search_ivf_filtered_adaptive(query, 10, probes, &filter)?
                    } else {
                        db.search_ivf_filtered(query, 10, probes, &filter)?
                    };
                    let recall = result
                        .neighbors
                        .iter()
                        .filter(|n| oracle.contains(n))
                        .count() as f64
                        / oracle.len() as f64;
                    if probes == 32 {
                        assert_eq!(result.neighbors, *oracle);
                    }
                    let mut ranked: Vec<_> = centers
                        .iter()
                        .enumerate()
                        .map(|(i, c)| (i, score(query, c)))
                        .collect();
                    ranked.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
                    let selected: Vec<_> = ranked
                        .iter()
                        .take(result.partitions_probed)
                        .map(|x| x.0)
                        .collect();
                    let groups: BTreeSet<_> = selected.iter().map(|p| p / 4).collect();
                    raw.push(json!({"recall":recall,"short":result.neighbors.len()<10,
                        "partition_gets":selected.len(),"partition_bytes":selected.iter().map(|p|sizes[*p]).sum::<usize>(),
                        "bundle_gets":groups.len(),"bundle_bytes":groups.iter().map(|g|bundles[*g]).sum::<usize>()}));
                }
                cases.push(
                    json!({"filtered":filtered,"probes":probes,"adaptive":adaptive,"queries":raw}),
                );
            }
        }
        // Derived cache loss must leave authoritative exact results unchanged.
        let expected = db.search_filtered(&queries[0], 10, &[("selected", "true")])?;
        drop(db);
        let mut clean = store.clone();
        clean.remove(&key)?;
        let mut reopened = Database::open(store, config)?;
        assert_eq!(
            reopened.search_filtered(&queries[0], 10, &[("selected", "true")])?,
            expected
        );
        reopened.build_ivf(IvfConfig {
            partitions: 32,
            iterations: 8,
            seed: 42,
        })?;
        assert_eq!(
            reopened
                .search_ivf_filtered(&queries[0], 10, 32, &[("selected", "true")])?
                .neighbors,
            expected
        );
        reports.push(json!({"distribution":if clustered {"clustered"} else {"uniform"}, "build_and_cache_ns":build_ns,
            "partition_layout_bytes":sizes.iter().sum::<usize>(),"bundle_layout_bytes":bundles.iter().sum::<usize>(),"cases":cases}));
    }
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()?;
    println!(
        "{}",
        serde_json::to_string_pretty(
            &json!({"schema_version":1,"git_revision":String::from_utf8(revision.stdout)?.trim(),
        "source_sha256":format!("{:x}",Sha256::digest(std::fs::read("examples/m12_quality_gate.rs")?)),
        "seed":42,"rows":2000,"dimensions":64,"queries":1000,"k":10,"selected_every":100,
        "method":"resident IVF quality plus serialized candidate layout costs; no remote I/O or latency measurement",
        "reports":reports})
        )?
    );
    Ok(())
}
