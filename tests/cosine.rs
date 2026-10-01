use glider::{
    ivf::IvfConfig,
    store::{LocalStore, ObjectStore},
    streaming::StreamingDatabase,
    Config, Database, Error, Metric, Neighbor,
};
use std::collections::BTreeMap;

const SEED: u64 = 0x026c_051e;

fn config() -> Config {
    Config {
        dimensions: 3,
        metric: Metric::Cosine,
    }
}

fn unit(vector: &[f32]) -> Vec<f32> {
    let norm = vector
        .iter()
        .map(|&x| f64::from(x).powi(2))
        .sum::<f64>()
        .sqrt();
    vector
        .iter()
        .map(|&x| (f64::from(x) / norm) as f32)
        .collect()
}

fn oracle(rows: &BTreeMap<u64, Vec<f32>>, query: &[f32], k: usize) -> Vec<Neighbor> {
    let query = unit(query);
    let mut found: Vec<_> = rows
        .iter()
        .map(|(&id, vector)| {
            let vector = unit(vector);
            let dot = query
                .iter()
                .zip(&vector)
                .map(|(&a, &b)| f64::from(a) * f64::from(b))
                .sum::<f64>();
            Neighbor {
                id,
                distance: 1. - dot,
            }
        })
        .collect();
    found.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
    found.truncate(k);
    found
}

fn rows() -> BTreeMap<u64, Vec<f32>> {
    let mut state = SEED;
    (0..80)
        .map(|id| {
            let vector = (0..3)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state % 2001) as f32 - 1000.
                })
                .collect();
            (id, vector)
        })
        .collect()
}

fn queries() -> [[f32; 3]; 3] {
    [[3., 4., -2.], [-8., 1., 5.], [0.25, -10., 4.]]
}

#[test]
fn resident_cosine_oracle_replay_and_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let mut rows = rows();
    let mut db = Database::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    assert_eq!(
        serde_json::to_string(&Metric::Cosine).unwrap(),
        "\"cosine\""
    );
    assert!(String::from_utf8(
        LocalStore::open(&path)
            .unwrap()
            .get("metadata")
            .unwrap()
            .unwrap()
    )
    .unwrap()
    .contains("\"cosine\""));
    for (&id, vector) in &rows {
        db.put(id, vector.clone()).unwrap();
    }
    assert!(matches!(db.put(100, vec![0.; 3]), Err(Error::Invalid(_))));
    assert!(matches!(db.search(&[0.; 3], 1), Err(Error::Invalid(_))));
    assert_eq!(db.get(0), Some(unit(&rows[&0]).as_slice()), "seed {SEED}");
    db.checkpoint_chunked(4096).unwrap();
    db.put(80, vec![6., -2., 1.]).unwrap();
    let log = LocalStore::open(&path)
        .unwrap()
        .get("mutation-00000000000000000081")
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8(log).unwrap().contains("[6.0,-2.0,1.0]"),
        "seed {SEED}"
    );
    rows.insert(80, vec![6., -2., 1.]);
    drop(db);
    let reader = StreamingDatabase::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    for query in queries() {
        assert_eq!(
            reader.search(&query, 10).unwrap(),
            oracle(&rows, &query, 10),
            "seed {SEED}"
        );
    }
    let mut db = Database::open(LocalStore::open(&path).unwrap(), config()).unwrap();
    assert_eq!(db.config().metric, Metric::Cosine);
    db.build_ivf(IvfConfig {
        partitions: 7,
        iterations: 4,
        seed: SEED,
    })
    .unwrap();
    for query in queries() {
        assert_eq!(
            db.search(&query, 10).unwrap(),
            oracle(&rows, &query, 10),
            "seed {SEED}"
        );
        assert_eq!(
            db.search_ivf(&query, 10, 7).unwrap().neighbors,
            oracle(&rows, &query, 10),
            "seed {SEED}"
        );
    }
    assert!(db.search_filtered(&[0.; 3], 0, &[]).is_err());
    assert!(Database::open(
        LocalStore::open(&path).unwrap(),
        Config {
            metric: Metric::Manhattan,
            ..config()
        }
    )
    .is_err());
}

#[cfg(feature = "experimental-segmented")]
#[test]
fn segmented_cosine_oracle_seal_selective_and_resident() {
    use glider::{
        retry::{Request, RequestId},
        segmented::{SegmentedDatabase, SegmentedOptions},
        Mutation,
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("db");
    let options = SegmentedOptions {
        resident_filter: Some(("tag".into(), "hot".into())),
    };
    let mut rows = rows();
    let open = || {
        SegmentedDatabase::open_with_options(
            LocalStore::open(&path).unwrap(),
            config(),
            options.clone(),
        )
        .unwrap()
    };
    let mut db = open();
    assert!(String::from_utf8(
        LocalStore::open(&path)
            .unwrap()
            .get("metadata")
            .unwrap()
            .unwrap()
    )
    .unwrap()
    .contains("\"cosine\""));
    let mutations = rows
        .iter()
        .map(|(&id, vector)| Mutation::Put {
            id,
            vector: vector.clone(),
            metadata: BTreeMap::from([(
                "tag".into(),
                if id % 3 == 0 { "hot" } else { "cold" }.into(),
            )]),
        })
        .collect();
    db.apply_request(Request {
        id: RequestId {
            boundary: 0,
            nonce: [1; 16],
        },
        conditions: vec![],
        mutations,
    })
    .unwrap();
    assert!(matches!(
        db.apply_request(Request {
            id: RequestId {
                boundary: 1,
                nonce: [2; 16]
            },
            conditions: vec![],
            mutations: vec![Mutation::Put {
                id: 100,
                vector: vec![0.; 3],
                metadata: BTreeMap::new()
            }]
        }),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        db.search_exact(&[0.; 3], 1, &[]),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        db.search_selective(&[0.; 3], 1, 1, &[]),
        Err(Error::Invalid(_))
    ));
    db.start_seal().unwrap();
    while db.seal_step().unwrap() {}
    let check = |db: &SegmentedDatabase<LocalStore>, rows: &BTreeMap<u64, Vec<f32>>| {
        assert_eq!(
            db.get(0).unwrap().unwrap().vector,
            unit(&rows[&0]),
            "seed {SEED}"
        );
        for query in queries() {
            let expected = oracle(rows, &query, 10);
            let hot: BTreeMap<_, _> = rows
                .iter()
                .filter(|(id, _)| **id % 3 == 0 || **id == 80)
                .map(|(&id, vector)| (id, vector.clone()))
                .collect();
            let filtered = oracle(&hot, &query, 10);
            assert_eq!(
                db.search_exact(&query, 10, &[]).unwrap(),
                expected,
                "seed {SEED}"
            );
            assert_eq!(
                db.search_selective(&query, 10, db.block_count().max(1), &[])
                    .unwrap(),
                expected,
                "seed {SEED}"
            );
            assert_eq!(
                db.search_selective(&query, 10, 1, &[("tag", "hot")])
                    .unwrap(),
                filtered,
                "seed {SEED}"
            );
            assert_eq!(
                db.search_exact(&query, 10, &[("tag", "hot")]).unwrap(),
                filtered,
                "seed {SEED}"
            );
        }
    };
    check(&db, &rows);
    let tail_request = Request {
        id: RequestId {
            boundary: 1,
            nonce: [3; 16],
        },
        conditions: vec![],
        mutations: vec![Mutation::Put {
            id: 80,
            vector: vec![6., -2., 1.],
            metadata: BTreeMap::from([("tag".into(), "hot".into())]),
        }],
    };
    db.apply_request(tail_request.clone()).unwrap();
    rows.insert(80, vec![6., -2., 1.]);
    check(&db, &rows);
    drop(db);
    let db = open();
    assert_eq!(db.sketch_rebuilds(), 0, "seed {SEED}");
    check(&db, &rows);
    let mut db = db;
    db.apply_request(tail_request).unwrap();
    assert!(SegmentedDatabase::open_with_options(
        LocalStore::open(&path).unwrap(),
        Config {
            metric: Metric::SquaredEuclidean,
            ..config()
        },
        options
    )
    .is_err());
}
