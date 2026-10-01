#![cfg(feature = "server")]

use glider::{
    admission::Engine,
    lease::{is_lease_key, Lease},
    retry::{Request, RequestId},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    server::{stage_segmented_namespace, Store},
    store::{LocalStore, ObjectStore},
    Config, Metric, Mutation,
};
use std::{collections::BTreeMap, time::Duration};

#[test]
fn killed_segmented_owner_stages_to_fresh_namespace_without_its_lease() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let destination = temp.path().join("destination");
    let config = Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    };
    let options = SegmentedOptions::default();
    let serving = SegmentedServingOptions::m21(temp.path().join("cache"));
    let lease_path = source.clone();
    let lease = Lease::acquire(
        move || LocalStore::open(&lease_path),
        Duration::from_secs(60),
    )
    .unwrap();
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
    // A killed writer leaves its handle and lease behind.
    drop((owner, lease));
    let listed = LocalStore::open(&source).unwrap().list().unwrap();
    assert_eq!(listed.iter().filter(|key| is_lease_key(key)).count(), 1);
    let source_store = Store::Local(LocalStore::open(&source).unwrap());
    let staged_store = || Store::Local(LocalStore::open(&destination).unwrap());
    let (sequence, objects, bytes) =
        stage_segmented_namespace(&source_store, staged_store(), config, options.clone()).unwrap();
    // Sequence 1 is the first takeover record.
    assert_eq!(sequence, 2);
    assert!(objects >= 3);
    assert!(bytes > 0);
    assert!(
        stage_segmented_namespace(&source_store, staged_store(), config, options.clone()).is_err()
    );
    assert!(!staged_store()
        .list()
        .unwrap()
        .iter()
        .any(|key| is_lease_key(key)));
    let restored = SegmentedServing::open(staged_store(), config, options, serving).unwrap();
    assert_eq!(
        restored.database().get(7).unwrap().unwrap().vector,
        vec![1.0, 2.0]
    );
    // Opening the staged copy is a takeover: one sequence for its record.
    assert_eq!(restored.database().epoch(), sequence + 1);
    assert_eq!(restored.database().sequence(), sequence + 1);
    restored.close().unwrap();
}
