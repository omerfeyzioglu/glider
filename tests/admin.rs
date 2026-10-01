#![cfg(feature = "server")]

use glider::{
    admission::Engine,
    ownership::claims,
    retry::{Request, RequestId},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    server::{stage_segmented_namespace, Store},
    store::LocalStore,
    Config, Metric, Mutation,
};
use std::collections::BTreeMap;

#[test]
fn stopped_segmented_owner_stages_to_fresh_namespace_and_releases_claim() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let destination = temp.path().join("destination");
    let config = Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    };
    let options = SegmentedOptions::default();
    let serving = SegmentedServingOptions::m21(temp.path().join("cache"));
    let mut owner = SegmentedServing::open(
        Store::Local(LocalStore::open(&source).unwrap()),
        config,
        options.clone(),
        serving.clone(),
    )
    .unwrap();
    owner
        .apply_request(Request {
            id: RequestId {
                boundary: 0,
                nonce: 1u128.to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations: vec![Mutation::Put {
                id: 7,
                vector: vec![1.0, 2.0],
                metadata: BTreeMap::new(),
            }],
        })
        .unwrap();
    drop(owner); // Simulates a stopped writer after forced exit.
    assert_eq!(
        claims(&LocalStore::open(&source).unwrap()).unwrap().len(),
        1
    );
    let source_store = Store::Local(LocalStore::open(&source).unwrap());
    let staged_store = || Store::Local(LocalStore::open(&destination).unwrap());
    let (sequence, objects, bytes) =
        stage_segmented_namespace(&source_store, staged_store(), config, options.clone()).unwrap();
    assert_eq!(sequence, 1);
    assert!(objects >= 3);
    assert!(bytes > 0);
    assert!(
        stage_segmented_namespace(&source_store, staged_store(), config, options.clone()).is_err()
    );
    let restored = SegmentedServing::open(staged_store(), config, options, serving).unwrap();
    assert_eq!(
        restored.database().get(7).unwrap().unwrap().vector,
        vec![1.0, 2.0]
    );
    restored.close().unwrap();
    assert!(claims(&LocalStore::open(&destination).unwrap())
        .unwrap()
        .is_empty());
}
