//! Targeted SIFT1M segmented load/recovery probe; run via tools/test_s3.py.
use glider::{
    retry::{Request, RequestId},
    segmented::SegmentedDatabase,
    store::s3::{AmazonS3Builder, ReadLimits, S3Store},
    Config, Metric, Mutation, Neighbor,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    env,
    fs::File,
    io::{BufReader, Read},
    time::Instant,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn row(reader: &mut impl Read) -> Result<Vec<f32>> {
    let mut bytes = [0_u8; 516];
    reader.read_exact(&mut bytes)?;
    if u32::from_le_bytes(bytes[..4].try_into().unwrap()) != 128 {
        return Err("invalid fvecs dimension".into());
    }
    Ok(bytes[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|part| f32::from_le_bytes(*part))
        .collect())
}

fn store(namespace: &str) -> Result<S3Store> {
    let builder = AmazonS3Builder::new()
        .with_bucket_name(env::var("GLIDER_S3_BUCKET")?)
        .with_region("us-east-1")
        .with_access_key_id(env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY")?)
        .with_endpoint(env::var("GLIDER_S3_ENDPOINT")?)
        .with_allow_http(true);
    Ok(
        S3Store::open(builder, namespace)?.with_read_limits(ReadLimits {
            objects: 10_000,
            object_bytes: 16 * 1024 * 1024,
            namespace_bytes: 1024 * 1024 * 1024,
        })?,
    )
}

fn rss() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let value = unsafe { usage.assume_init() }.ru_maxrss as u64;
    if cfg!(target_os = "macos") {
        value
    } else {
        value * 1024
    }
}

fn top10(data_path: &str, rows: usize, query: &[f32]) -> Result<Vec<Neighbor>> {
    let mut input = BufReader::new(File::open(data_path)?);
    let mut best = Vec::new();
    for id in 0..rows {
        let vector = row(&mut input)?;
        let distance: f64 = query
            .iter()
            .zip(vector)
            .map(|(&a, b)| {
                let diff = f64::from(a) - f64::from(b);
                diff * diff
            })
            .sum();
        best.push(Neighbor {
            id: id as u64,
            distance,
        });
        best.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
        best.truncate(10);
    }
    Ok(best)
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 5 {
        return Err("usage: m22_capacity BASE.fvecs QUERY.fvecs ROWS NAMESPACE".into());
    }
    let rows: usize = args[3].parse()?;
    if rows == 0 || !rows.is_multiple_of(100) {
        return Err("rows must be a positive multiple of 100".into());
    }
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let mut query_file = File::open(&args[2])?;
    let query = row(&mut query_file)?;
    let mut data = BufReader::new(File::open(&args[1])?);
    let first_store = store(&args[4])?;
    let write_metrics = first_store.metrics();
    let mut db = SegmentedDatabase::open(first_store, config)?;
    let mut samples = BTreeMap::new();
    let mut seal_steps = 0;
    let mut seal_max_ms = 0_f64;
    let mut cleanup_calls = 0;
    let start = Instant::now();
    for batch in 0..rows / 100 {
        let mutations = (0..100)
            .map(|offset| {
                let id = (batch * 100 + offset) as u64;
                let vector = row(&mut data)?;
                if id == 0 || id == (rows / 2) as u64 || id == (rows - 1) as u64 {
                    samples.insert(id, vector.clone());
                }
                let metadata = if id.is_multiple_of(100) {
                    BTreeMap::from([("cohort".into(), "one-percent".into())])
                } else {
                    BTreeMap::new()
                };
                Ok(Mutation::Put {
                    id,
                    vector,
                    metadata,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let outcome = db.apply_request(Request {
            id: RequestId {
                boundary: batch as u64,
                nonce: (batch as u128).to_le_bytes(),
            },
            conditions: Vec::new(),
            mutations,
        })?;
        if outcome.sequence != (batch + 1) as u64 {
            return Err("unexpected sequence".into());
        }
        if (batch + 1) % 64 == 0 || batch + 1 == rows / 100 {
            db.start_seal()?;
            while {
                let step = Instant::now();
                let progressed = db.seal_step()?;
                if progressed {
                    seal_steps += 1;
                    seal_max_ms = seal_max_ms.max(step.elapsed().as_secs_f64() * 1000.);
                }
                progressed
            } {}
            while db.cleanup_step(32)? != 0 {
                cleanup_calls += 1;
            }
        }
    }
    let load_ms = start.elapsed().as_secs_f64() * 1000.;
    let write_counts = write_metrics.snapshot();
    let load_rss = rss();
    drop(db);
    let second_store = store(&args[4])?;
    let reopen_metrics = second_store.metrics();
    let start = Instant::now();
    let reopened = SegmentedDatabase::open(second_store, config)?;
    let reopen_ms = start.elapsed().as_secs_f64() * 1000.;
    for (id, expected) in samples {
        if reopened.get(id)?.as_ref().map(|d| &d.vector) != Some(&expected) {
            return Err(format!("recovered document mismatch: {id}").into());
        }
    }
    let reopen_counts = reopen_metrics.snapshot();
    let start = Instant::now();
    let actual = reopened.search_exact(&query, 10, &[])?;
    let exact_ms = start.elapsed().as_secs_f64() * 1000.;
    let exact_counts = reopen_metrics.snapshot();
    let oracle = top10(&args[1], rows, &query)?;
    if actual != oracle {
        return Err("segmented exact result disagrees with independent oracle".into());
    }
    println!(
        "{}",
        json!({
            "version":1,"rows":rows,"dimensions":128,"backend":"local_minio",
            "load_ms":load_ms,"load_peak_rss_bytes":load_rss,"seal_steps":seal_steps,
            "seal_step_max_ms":seal_max_ms,"cleanup_calls":cleanup_calls,
            "write_put":write_counts.put,"write_delete":write_counts.delete,
            "write_list":write_counts.list,"write_get":write_counts.get,
            "write_uploaded_bytes":write_counts.request_body_bytes,
            "reopen_ms":reopen_ms,"reopen_get":reopen_counts.get,
            "reopen_list":reopen_counts.list,"exact_ms":exact_ms,
            "exact_get":exact_counts.get-reopen_counts.get,
            "exact_top10_ids":actual.iter().map(|n|n.id).collect::<Vec<_>>(),
            "peak_rss_bytes":rss(),"exact_oracle_passed":true,
        })
    );
    Ok(())
}
