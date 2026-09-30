//! Targeted selective-read probe on a previously published M22 namespace.
use glider::{
    segmented::SegmentedDatabase,
    store::s3::{AmazonS3Builder, ReadLimits, S3Store},
    Config, Metric,
};
use serde_json::json;
use std::{env, fs, io::Read, path::Path, time::Instant};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

fn queries(path: &str) -> Result<Vec<Vec<f32>>> {
    let mut file = fs::File::open(path)?;
    (0..200)
        .map(|_| {
            let mut bytes = [0_u8; 516];
            file.read_exact(&mut bytes)?;
            if u32::from_le_bytes(bytes[..4].try_into().unwrap()) != 128 {
                return Err("invalid SIFT1M query dimensions".into());
            }
            Ok(bytes[4..]
                .as_chunks::<4>()
                .0
                .iter()
                .map(|part| f32::from_le_bytes(*part))
                .collect())
        })
        .collect()
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

fn percentile(values: &[f64], percentile: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[(sorted.len() * percentile / 100).saturating_sub(1)]
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 5 {
        return Err("usage: m24_selective QUERY.fvecs NAMESPACE ORACLE.json CACHE_DIR".into());
    }
    let queries = queries(&args[1])?;
    let oracle: serde_json::Value = serde_json::from_slice(&fs::read(&args[3])?)?;
    let expected = oracle["unfiltered_exact_ids"]
        .as_array()
        .ok_or("missing exact oracle IDs")?;
    if expected.len() != queries.len() {
        return Err("wrong exact oracle query count".into());
    }
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let s3 = store(&args[2])?;
    let metrics = s3.metrics();
    let open_start = Instant::now();
    let mut db = SegmentedDatabase::open(s3, config)?;
    let open_ms = open_start.elapsed().as_secs_f64() * 1000.;
    let open_get = metrics.snapshot().get;
    let build_start = Instant::now();
    db.build_selective_index(24 * 1024 * 1024)?;
    let build_ms = build_start.elapsed().as_secs_f64() * 1000.;
    let build_get = metrics.snapshot().get - open_get;
    let sketch_bytes = db.selective_index_bytes().ok_or("sketch not installed")?;
    let build_peak_rss_bytes = rss();
    let db = db.with_block_cache(Path::new(&args[4]), 4 * 1024 * 1024, 256 * 1024 * 1024)?;
    let mut latencies = Vec::with_capacity(queries.len());
    let mut recalls = Vec::with_capacity(queries.len());
    let mut get_counts = Vec::with_capacity(queries.len());
    let mut remote_bytes = Vec::with_capacity(queries.len());
    let mut first_pass_ids = Vec::with_capacity(queries.len());
    let mut short_results = 0;
    let mut first_ids = Vec::new();
    for (index, query) in queries.iter().enumerate() {
        let before_get = metrics.snapshot().get;
        let before_bytes = db.cache_stats()?.unwrap().remote_payload_bytes;
        let start = Instant::now();
        let neighbors = db.search_selective_unfiltered(query, 10, 8)?;
        let elapsed = start.elapsed().as_secs_f64() * 1000.;
        let ids: Vec<_> = neighbors.into_iter().map(|neighbor| neighbor.id).collect();
        first_pass_ids.push(ids.clone());
        if index == 0 {
            first_ids = ids.clone();
        }
        let truth: Vec<u64> = expected[index]
            .as_array()
            .ok_or("invalid exact-oracle row")?
            .iter()
            .map(|id| id.as_u64().ok_or("invalid exact-oracle ID"))
            .collect::<std::result::Result<_, _>>()?;
        let hits = ids.iter().filter(|id| truth.contains(id)).count();
        recalls.push(hits);
        short_results += usize::from(ids.len() < truth.len());
        latencies.push(elapsed);
        get_counts.push(metrics.snapshot().get - before_get);
        remote_bytes.push(db.cache_stats()?.unwrap().remote_payload_bytes - before_bytes);
    }
    let mut ordered_recalls = recalls.clone();
    ordered_recalls.sort_unstable();
    let quality = json!({
        "mean_recall_at_10":recalls.iter().sum::<usize>() as f64/(recalls.len()*10) as f64,
        "fifth_percentile_recall_at_10":ordered_recalls[recalls.len()*5/100-1] as f64/10.,
        "short_results":short_results,
    });
    let mut warm_latencies = Vec::with_capacity(queries.len());
    let before_warm_get = metrics.snapshot().get;
    let before_warm_bytes = db.cache_stats()?.unwrap().remote_payload_bytes;
    for (query, expected_ids) in queries.iter().zip(&first_pass_ids) {
        let start = Instant::now();
        let ids: Vec<_> = db
            .search_selective_unfiltered(query, 10, 8)?
            .into_iter()
            .map(|neighbor| neighbor.id)
            .collect();
        warm_latencies.push(start.elapsed().as_secs_f64() * 1000.);
        if &ids != expected_ids {
            return Err("warm selective result changed".into());
        }
    }
    let warm_get = metrics.snapshot().get - before_warm_get;
    let warm_remote_bytes = db.cache_stats()?.unwrap().remote_payload_bytes - before_warm_bytes;
    let cache = db.cache_stats()?.unwrap();
    drop(db);
    fs::remove_dir_all(Path::new(&args[4]).join("glider-block-cache-v1"))?;
    let loss_store = store(&args[2])?;
    let loss_metrics = loss_store.metrics();
    let loss_start = Instant::now();
    let mut lost = SegmentedDatabase::open(loss_store, config)?;
    let loss_open_ms = loss_start.elapsed().as_secs_f64() * 1000.;
    let loss_start = Instant::now();
    lost.build_selective_index(24 * 1024 * 1024)?;
    let loss_build_ms = loss_start.elapsed().as_secs_f64() * 1000.;
    let lost = lost.with_block_cache(Path::new(&args[4]), 4 * 1024 * 1024, 256 * 1024 * 1024)?;
    let before_loss_get = loss_metrics.snapshot().get;
    let loss_start = Instant::now();
    let loss_ids: Vec<_> = lost
        .search_selective_unfiltered(&queries[0], 10, 8)?
        .into_iter()
        .map(|neighbor| neighbor.id)
        .collect();
    let loss_query_ms = loss_start.elapsed().as_secs_f64() * 1000.;
    if loss_ids != first_ids {
        return Err("cache loss changed selective result".into());
    }
    let loss_query_get = loss_metrics.snapshot().get - before_loss_get;
    let loss_query_remote_payload_bytes = lost.cache_stats()?.unwrap().remote_payload_bytes;
    println!(
        "{}",
        json!({
            "version":1,"rows":250000,"queries":200,"k":10,"metric":"squared_euclidean",
            "backend":"local_minio","open_ms":open_ms,"open_get":open_get,
            "build_ms":build_ms,"build_get":build_get,"sketch_bytes":sketch_bytes,
            "build_peak_rss_bytes":build_peak_rss_bytes,"peak_rss_bytes":rss(),
            "quality":quality,"first_query_ms":latencies[0],"first_query_get":get_counts[0],
            "first_query_remote_payload_bytes":remote_bytes[0],"first_ids":first_ids,
            "query_p50_ms":percentile(&latencies,50),"query_p95_ms":percentile(&latencies,95),
            "query_max_ms":latencies.iter().copied().fold(0_f64,f64::max),
            "query_get_total":get_counts.iter().sum::<u64>(),
            "query_get_max":get_counts.iter().copied().max().unwrap_or(0),
            "query_remote_payload_total":remote_bytes.iter().sum::<u64>(),
            "query_remote_payload_max":remote_bytes.iter().copied().max().unwrap_or(0),
            "warm_query_p50_ms":percentile(&warm_latencies,50),
            "warm_query_p95_ms":percentile(&warm_latencies,95),
            "warm_query_max_ms":warm_latencies.iter().copied().fold(0_f64,f64::max),
            "warm_query_get_total":warm_get,"warm_query_remote_payload_total":warm_remote_bytes,
            "cache_loss_open_ms":loss_open_ms,"cache_loss_build_ms":loss_build_ms,
            "cache_loss_query_ms":loss_query_ms,"cache_loss_query_get":loss_query_get,
            "cache_loss_query_remote_payload_bytes":loss_query_remote_payload_bytes,
            "cache_loss_result_equal":true,
            "cache":cache,
        })
    );
    Ok(())
}
