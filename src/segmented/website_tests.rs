//! Records the website playground's search through the real clustered reader
//! and checks the path the playground illustrates: an acknowledged write is in
//! the object store, a cold query reads it from there, a repeated query is
//! served from the local cache, and a new process with empty caches returns
//! the same results from the object store alone.
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

#[test]
fn website_search_matches_clustered_engine() {
    const SEED: u64 = 42;
    const PROBES: usize = 4;
    const BUDGET: ReadBudget = ReadBudget {
        blocks: 8,
        requests: 8,
        bytes: 262_144,
        local_blocks: 64,
    };
    let config = Config {
        dimensions: 384,
        metric: Metric::Cosine,
    };
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
    let question = &input["queries"][0];
    let query = vector(question);
    let directory = tempfile::tempdir().unwrap();
    let store = RecordingStore::default();

    // Write: one request; once it returns, its log object is in the store.
    let mut db = SegmentedDatabase::open(store.clone(), config).unwrap();
    let objects_before = store.objects.lock().unwrap().len();
    let boundary = db.sequence();
    db.apply_request(Request {
        id: RequestId {
            boundary,
            nonce: (boundary as u128 + 1).to_le_bytes(),
        },
        conditions: vec![],
        mutations: documents
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
    })
    .unwrap();
    assert!(store.objects.lock().unwrap().len() > objects_before);
    db.seal_delta().unwrap();
    db.convert_clustered(ConvertOptions {
        centroids: Some(4),
        seed: SEED,
        gather_bytes: 4 * 1024 * 1024,
    })
    .unwrap();
    let oracle: Vec<u64> = db
        .search_exact(&query, 3, &[])
        .unwrap()
        .iter()
        .map(|n| n.id)
        .collect();

    // Returns the hit IDs and the number of object-store range reads.
    let search = |db: &SegmentedDatabase<RecordingStore>| {
        store.ranges.lock().unwrap().clear();
        let (hits, reads) = db
            .view()
            .search_selective_within(
                &query,
                3,
                BUDGET,
                &[],
                QueryOptions {
                    include_metadata: true,
                    include_vector: false,
                },
            )
            .unwrap();
        assert_eq!(
            reads.requests as usize,
            store.ranges.lock().unwrap().len(),
            "seed={SEED}"
        );
        for hit in &hits {
            assert_eq!(
                hit.metadata.as_ref().unwrap()["title"],
                documents[hit.id as usize]["title"].as_str().unwrap()
            );
        }
        (
            hits.iter().map(|h| h.id).collect::<Vec<_>>(),
            reads.requests,
        )
    };

    // First query: empty caches, so the blocks come from the object store.
    db.set_cluster_probes(PROBES);
    db = db
        .with_block_cache(directory.path().join("first"), 8 << 20, 64 << 20)
        .unwrap();
    let (cold, cold_reads) = search(&db);
    assert_eq!(
        cold, oracle,
        "seed={SEED}; full probing must match exact search"
    );
    assert!((1..=BUDGET.requests as u64).contains(&cold_reads));

    // Same query again: served from the local cache without object-store reads.
    let (warm, warm_reads) = search(&db);
    assert_eq!((warm, warm_reads), (cold.clone(), 0));

    // Crash: the process and its caches are gone. A new process takes over the
    // collection from the object store with an empty cache directory.
    drop(db);
    let mut db = SegmentedDatabase::open(store.clone(), config).unwrap();
    db.set_cluster_probes(PROBES);
    let db = db
        .with_block_cache(directory.path().join("restarted"), 8 << 20, 64 << 20)
        .unwrap();
    let (restarted, restarted_reads) = search(&db);
    assert_eq!(restarted, cold, "seed={SEED}");
    assert!(restarted_reads >= 1);

    let results: Vec<Value> = cold
        .iter()
        .map(|&id| {
            let mut doc = documents[id as usize].clone();
            doc.as_object_mut().unwrap().remove("vector");
            doc
        })
        .collect();
    let data = json!({"version": 3, "seed": SEED, "documents": documents.len(),
        "model": input["model"], "model_revision": input["model_revision"],
        "question": question["text"], "results": results});
    let encoded = format!(
        "// Generated by website_search_matches_clustered_engine; do not edit.\nexport default {};\n",
        serde_json::to_string_pretty(&data).unwrap()
    );
    if let Ok(path) = std::env::var("GLIDER_WEBSITE_OUTPUT") {
        std::fs::write(path, encoded).unwrap();
    } else {
        assert!(
            encoded == include_str!("../../site/search-recording.mjs"),
            "seed={SEED}; regenerate site/search-recording.mjs after a reader change"
        );
    }
}
