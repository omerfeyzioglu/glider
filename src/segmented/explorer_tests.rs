//! Recorded website queries through the real clustered reader and block cache.
use super::*;
use crate::{
    retry::{Request, RequestId},
    segmented::ConvertOptions,
};
use serde_json::{json, Value};
use std::sync::Mutex;

#[derive(Clone, Default)]
struct RecordingStore {
    objects: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    ranges: Arc<Mutex<Vec<(String, usize, usize)>>>,
}
impl ObjectStore for RecordingStore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload: usize,
    ) -> Result<Option<Vec<u8>>> {
        let bytes = self.get(key)?.unwrap();
        assert_eq!(bytes.len(), payload);
        self.ranges
            .lock()
            .unwrap()
            .push((key.into(), offset, length));
        Ok(Some(bytes[offset..offset + length].to_vec()))
    }
    fn list(&self) -> Result<Vec<String>> {
        Ok(self.objects.lock().unwrap().keys().cloned().collect())
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let mut objects = self.objects.lock().unwrap();
        assert!(!objects.contains_key(key));
        objects.insert(key.into(), value.into());
        Ok(())
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

fn write(db: &mut SegmentedDatabase<RecordingStore>, mutations: Vec<Mutation>) {
    let boundary = db.sequence();
    db.apply_request(Request {
        id: RequestId {
            boundary,
            nonce: (boundary as u128 + 1).to_le_bytes(),
        },
        conditions: vec![],
        mutations,
    })
    .unwrap();
}

#[test]
fn website_recordings_match_clustered_engine() {
    const SEED: u64 = 42;
    let input: Value =
        serde_json::from_str(include_str!("../../tests/fixtures/site-search-input.json")).unwrap();
    let vector = |value: &Value| {
        value["vector"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect::<Vec<_>>()
    };
    let documents = input["documents"].as_array().unwrap();
    let queries: Vec<Vec<f32>> = input["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(vector)
        .collect();
    let directory = tempfile::tempdir().unwrap();
    let store = RecordingStore::default();
    let mut db = SegmentedDatabase::open(
        store.clone(),
        Config {
            dimensions: 384,
            metric: Metric::Cosine,
        },
    )
    .unwrap();
    write(
        &mut db,
        documents
            .iter()
            .map(|doc| Mutation::Put {
                id: doc["id"].as_u64().unwrap(),
                vector: vector(doc),
                metadata: BTreeMap::from([
                    ("title".into(), doc["title"].as_str().unwrap().into()),
                    ("category".into(), doc["category"].as_str().unwrap().into()),
                ]),
            })
            .collect(),
    );
    db.seal_delta().unwrap();
    db.convert_clustered(ConvertOptions {
        centroids: Some(4),
        seed: SEED,
        gather_bytes: 4 * 1024 * 1024,
    })
    .unwrap();
    let view = db.view();
    // Stable display IDs: cluster ID then block's first row ID, never random object names.
    let mut blocks = Vec::new();
    for (slot, pack) in view.sketches.packs.iter().enumerate() {
        for index in 0..pack.sketch.blocks.len() {
            if let Some(block) = view.block_ref(slot, index) {
                blocks.push(block.clone());
            }
        }
    }
    blocks.sort_by_key(|b| (b.partition, b.first_id));
    let mut records = Vec::new();
    for (qi, query) in queries.iter().enumerate() {
        let oracle = db.search_exact(query, 3, &[]).unwrap();
        for (effort, probes, requests, bytes) in [
            ("lean", 1, 1, 65536),
            ("balanced", 2, 2, 65536),
            ("wide", 4, 8, 262144),
        ] {
            db.set_cluster_probes(probes);
            let budget = ReadBudget {
                blocks: 8,
                requests,
                bytes,
                local_blocks: 64,
            };
            for tier in ["cold", "ssd", "ram"] {
                if tier != "ram" {
                    db = db
                        .with_block_cache(
                            directory.path().join(format!("{qi}-{effort}-{tier}")),
                            8 * 1024 * 1024,
                            64 * 1024 * 1024,
                        )
                        .unwrap();
                    if tier == "ssd" {
                        while db.warm_cache_step(256 * 1024).unwrap() {}
                    }
                }
                let view = db.view();
                let cluster = view.cluster.as_ref().unwrap();
                let probed = cluster.probe(
                    view.config.metric,
                    &view.config.query(query).unwrap(),
                    probes,
                );
                let ranked = view.route_within(
                    &view.config.query(query).unwrap(),
                    blocks.len() + 64,
                    &[],
                    &probed,
                );
                // Record existing local entries before the query; lookup does not admit bytes.
                let mut local = BTreeMap::new();
                for &(_, slot, index) in &ranked {
                    let reference = view.block_ref(slot, index).unwrap();
                    if let Some((_, source)) = lock_cache(view.cache.as_ref().unwrap())
                        .unwrap()
                        .lookup(reference)
                        .unwrap()
                    {
                        local.insert(
                            reference.sha256.clone(),
                            if source == Source::Ram { "ram" } else { "ssd" },
                        );
                    }
                }
                let before = db.cache_stats().unwrap().unwrap();
                store.ranges.lock().unwrap().clear();
                let (hits, reads) = view
                    .search_selective_within(
                        query,
                        3,
                        budget,
                        &[],
                        QueryOptions {
                            include_metadata: true,
                            include_vector: false,
                        },
                    )
                    .unwrap();
                for hit in &hits {
                    assert_eq!(
                        hit.metadata.as_ref().unwrap()["title"],
                        documents[hit.id as usize]["title"].as_str().unwrap()
                    );
                }
                if effort == "wide" {
                    assert_eq!(
                        hits.iter().map(|h| h.id).collect::<Vec<_>>(),
                        oracle.iter().map(|h| h.id).collect::<Vec<_>>(),
                        "seed={SEED}; full probing must match exact search"
                    );
                }
                let after = db.cache_stats().unwrap().unwrap();
                let ranges = store.ranges.lock().unwrap().clone();
                assert_eq!(reads.requests as usize, ranges.len(), "seed={SEED}");
                assert_eq!(
                    reads.bytes as usize,
                    ranges.iter().map(|r| r.2).sum::<usize>()
                );
                assert!(reads.requests <= requests as u64 && reads.bytes <= bytes as u64);
                if tier != "cold" {
                    assert_eq!(reads.requests, 0);
                }
                let selected: Vec<Value> = blocks
                    .iter()
                    .enumerate()
                    .filter_map(|(id, block)| {
                        let remote = ranges.iter().any(|(key, offset, length)| {
                            *key == block.object
                                && block.offset >= *offset
                                && block.offset + block.length <= offset + length
                        });
                        let source = if remote {
                            Some("object")
                        } else {
                            local.get(&block.sha256).copied()
                        };
                        source.map(|source| json!({"id": id, "source": source}))
                    })
                    .collect();
                assert_eq!(
                    selected.iter().filter(|b| b["source"] == "ssd").count() as u64,
                    after.nvme_hits - before.nvme_hits
                );
                assert_eq!(
                    selected.iter().filter(|b| b["source"] == "ram").count() as u64,
                    after.ram_hits - before.ram_hits
                );
                let matched = hits
                    .iter()
                    .filter(|hit| oracle.iter().any(|n| n.id == hit.id))
                    .count();
                records.push(json!({"query": qi, "effort": effort, "tier": tier,
                        "probed": probed, "selected": selected, "requests": reads.requests, "bytes": reads.bytes,
                        "ram_hits": after.ram_hits - before.ram_hits, "ssd_hits": after.nvme_hits - before.nvme_hits,
                        "recall": matched as f64 / 3., "sequence": db.sequence(),
                        "hits": hits.iter().map(|h| json!({"id": h.id, "distance": h.distance, "exact": oracle.iter().any(|n| n.id == h.id)})).collect::<Vec<_>>(),
                        "oracle": oracle.iter().map(|n| n.id).collect::<Vec<_>>() }));
            }
        }
    }
    let mut public_documents = documents.clone();
    for doc in &mut public_documents {
        doc.as_object_mut().unwrap().remove("vector");
    }
    let data = json!({"version": 2, "seed": SEED, "rows": documents.len(), "dimensions": 384, "metric": "cosine", "centroids": 4,
        "model": input["model"], "model_revision": input["model_revision"], "documents": public_documents,
        "backend": "In-memory ObjectStore; filesystem SSD cache; no network timing",
        "queries": input["queries"].as_array().unwrap().iter().map(|q| q["text"].clone()).collect::<Vec<_>>(),
        "blocks": blocks.iter().enumerate().map(|(id, b)| json!({"id": id, "cluster": b.partition, "rows": b.rows, "bytes": b.length})).collect::<Vec<_>>(), "records": records });
    // Compare serialized bytes: serde_json's default f64 parser can round a
    // shortest decimal to a neighboring float, even when reruns are identical.
    let encoded = format!("{}\n", serde_json::to_string_pretty(&data).unwrap());
    if let Ok(path) = std::env::var("GLIDER_EXPLORER_OUTPUT") {
        std::fs::write(path, encoded).unwrap();
    } else {
        assert!(
            encoded == include_str!("../../site/explorer-data.json"),
            "seed={SEED}; regenerate the website recordings after a reader change"
        );
    }
}
