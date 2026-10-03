use glider::{
    retry::{Request, RequestId},
    segmented::{ReadBudget, SegmentedDatabase, SegmentedOptions},
    store::LocalStore,
    Config, Filter, Metric, Mutation,
};
use serde_json::json;
use std::collections::BTreeMap;

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

#[test]
fn exact_matches_brute_force_and_selective_hits_match() {
    const SEED: u64 = 0x7182_a1b2_c3d4_e5f6;
    let mut seed = SEED;
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("db");
    let mut db = SegmentedDatabase::open_with_options(
        LocalStore::open(&db_path).unwrap(),
        Config {
            dimensions: 2,
            metric: Metric::SquaredEuclidean,
        },
        SegmentedOptions {
            resident_filter: Some(("cohort".into(), "yes".into())),
            routed_keys: vec!["cohort".into(), "color".into()],
        },
    )
    .unwrap();
    for batch in 0..3_u64 {
        let mutations = (batch * 45..batch * 45 + 45)
            .map(|id| {
                let r = next(&mut seed);
                let mut metadata = BTreeMap::new();
                if !r.is_multiple_of(3) {
                    metadata.insert("cohort".into(), "yes".into());
                }
                if !r.is_multiple_of(5) {
                    metadata.insert(
                        "color".into(),
                        if r.is_multiple_of(2) { "red" } else { "blue" }.into(),
                    );
                }
                if !r.is_multiple_of(7) {
                    metadata.insert(
                        "score".into(),
                        if r.is_multiple_of(11) {
                            "NaN".into()
                        } else {
                            (r % 20).to_string()
                        },
                    );
                }
                Mutation::Put {
                    id,
                    vector: vec![(r % 100) as f32, (r % 37) as f32],
                    metadata,
                }
            })
            .collect();
        db.apply_request(Request {
            id: RequestId {
                boundary: db.sequence(),
                nonce: [batch as u8 + 1; 16],
            },
            conditions: vec![],
            mutations,
        })
        .unwrap();
        db.seal_delta().unwrap();
    }
    db.apply_request(Request {
        id: RequestId {
            boundary: db.sequence(),
            nonce: [4; 16],
        },
        conditions: vec![],
        mutations: vec![
            Mutation::Delete { id: 3 },
            Mutation::Put {
                id: 5,
                vector: vec![0., 0.],
                metadata: BTreeMap::from([
                    ("cohort".into(), "yes".into()),
                    ("color".into(), "red".into()),
                    ("score".into(), "7".into()),
                ]),
            },
        ],
    })
    .unwrap();
    let shapes = [
        json!({"cohort":"yes"}),
        json!({"cohort":{"$eq":"yes"},"score":{"$gt":3,"$lte":12}}),
        json!({"$or":[{"color":"red"},{"score":{"$lt":4}}]}),
        json!({"$not":{"color":{"$exists":true}}}),
        json!({"color":{"$ne":"blue","$nin":["green"]}}),
        json!({"$and":[{"cohort":"yes"},{"$or":[{"score":{"$in":["7","8"]}},{"color":"blue"}]}]}),
    ];
    for case in 0..36 {
        let filter = Filter::parse(&shapes[(next(&mut seed) as usize) % shapes.len()]).unwrap();
        let query = [
            (next(&mut seed) % 100) as f32,
            (next(&mut seed) % 37) as f32,
        ];
        let k = (next(&mut seed) % 15 + 1) as usize;
        let mut expected = Vec::new();
        db.scan_live(|id, vector, metadata| {
            if filter.matches(metadata) {
                let distance = query
                    .iter()
                    .zip(vector)
                    .map(|(a, b)| f64::from(*a - *b).powi(2))
                    .sum::<f64>();
                expected.push((id, distance));
            }
            Ok(())
        })
        .unwrap();
        expected.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
        expected.truncate(k);
        let exact = db.search_exact_filter(&query, k, &filter).unwrap();
        assert_eq!(
            exact.iter().map(|n| (n.id, n.distance)).collect::<Vec<_>>(),
            expected,
            "seed={SEED} case={case} filter={filter:?}"
        );
        let approximate = db
            .search_selective_within_filter(&query, k, ReadBudget::uniform(1), &filter)
            .unwrap();
        for hit in approximate {
            let metadata = db.get(hit.id).unwrap().unwrap().metadata;
            assert!(
                filter.matches(&metadata),
                "seed={SEED} case={case} id={} filter={filter:?}",
                hit.id
            );
        }
    }
    let resident =
        Filter::parse(&json!({"$and":[{"cohort":"yes"},{"cohort":{"$eq":"yes"}}]})).unwrap();
    let exact = db.search_exact_filter(&[5., 6.], 100, &resident).unwrap();
    let selective = db
        .search_selective_within_filter(&[5., 6.], 100, ReadBudget::uniform(1), &resident)
        .unwrap();
    assert_eq!(selective, exact, "seed={SEED} resident claim");
}
