use glider::{
    retry::{Request, RequestId},
    segmented::SegmentedDatabase,
    store::LocalStore,
    Config, Metric, Mutation,
};
use std::collections::BTreeMap;

#[test]
fn diagnostic_location_tracks_published_and_tail_visibility() {
    let temp = tempfile::tempdir().unwrap();
    let config = Config {
        dimensions: 2,
        metric: Metric::SquaredEuclidean,
    };
    let mut db = SegmentedDatabase::open(LocalStore::open(temp.path()).unwrap(), config).unwrap();
    let request = |boundary, nonce, mutation| Request {
        id: RequestId {
            boundary,
            nonce: [nonce; 16],
        },
        conditions: Vec::new(),
        mutations: vec![mutation],
    };
    let put = |vector| Mutation::Put {
        id: 42,
        vector,
        metadata: BTreeMap::new(),
    };

    assert_eq!(db.block_count(), 0);
    assert_eq!(db.current_block_of(42), None);
    db.apply_request(request(0, 1, put(vec![1., 2.]))).unwrap();
    assert_eq!(db.current_block_of(42), None);
    db.seal_delta().unwrap();
    assert_eq!(db.block_count(), 1);
    assert_eq!(db.current_block_of(42), Some((0, 0)));
    assert!(db.block_payload_len(0, 0).is_some_and(|bytes| bytes > 0));
    assert_eq!(db.block_payload_len(0, 1), None);

    db.apply_request(request(1, 2, put(vec![3., 4.]))).unwrap();
    assert_eq!(db.current_block_of(42), None);
    db.seal_delta().unwrap();
    assert!(db.current_block_of(42).is_some());
    db.apply_request(request(2, 3, Mutation::Delete { id: 42 }))
        .unwrap();
    assert_eq!(db.current_block_of(42), None);
    db.seal_delta().unwrap();
    assert_eq!(db.current_block_of(42), None);
    assert!(db.get(42).unwrap().is_none());
}
