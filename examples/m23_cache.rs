//! Targeted M23 block-cache probe on an existing segmented SIFT1M namespace.
use glider::{
    segmented::SegmentedDatabase,
    store::s3::{AmazonS3Builder, ReadLimits, S3Store},
    Config, Metric,
};
use serde_json::json;
use std::{env, fs, io::Read, path::Path, process::Command, time::Instant};

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

fn query(path: &str) -> Result<Vec<f32>> {
    let mut file = fs::File::open(path)?;
    let mut bytes = [0_u8; 516];
    file.read_exact(&mut bytes)?;
    if u32::from_le_bytes(bytes[..4].try_into().unwrap()) != 128 {
        return Err("invalid SIFT1M query dimensions".into());
    }
    Ok(bytes[4..]
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| f32::from_le_bytes(*chunk))
        .collect())
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

fn warm_phase(
    query: &[f32],
    namespace: &str,
    cache_dir: &Path,
    config: Config,
    ram_limit: usize,
    nvme_limit: usize,
) -> Result<serde_json::Value> {
    let warm_store = store(namespace)?;
    let warm_metrics = warm_store.metrics();
    let open_start = Instant::now();
    let warm = SegmentedDatabase::open(warm_store, config)?
        .with_block_cache(cache_dir, ram_limit, nvme_limit)?;
    let warm_open_ms = open_start.elapsed().as_secs_f64() * 1000.;
    let warm_open_get = warm_metrics.snapshot().get;
    let start = Instant::now();
    let result = warm.search_exact(query, 10, &[])?;
    let warm_ms = start.elapsed().as_secs_f64() * 1000.;
    let warm_get = warm_metrics.snapshot().get - warm_open_get;
    let warm_cache = warm.cache_stats()?.unwrap();
    if !warm_cache.nvme_available || warm_get != 0 || warm_cache.nvme_hits == 0 {
        return Err("NVMe-warm pass did not serve authenticated cached blocks".into());
    }
    let top_id = result.first().ok_or("empty exact top ten")?.id;
    let before_hot = warm.cache_stats()?.unwrap();
    let first_document = warm.get(top_id)?;
    let after_first_hot = warm.cache_stats()?.unwrap();
    let second_document = warm.get(top_id)?;
    let after_hot = warm.cache_stats()?.unwrap();
    if first_document != second_document || after_hot.ram_hits <= after_first_hot.ram_hits {
        return Err("RAM-hot point read did not preserve the result and hit RAM".into());
    }
    Ok(json!({
        "warm_open_ms":warm_open_ms,"warm_open_get":warm_open_get,
        "nvme_warm_exact_ms":warm_ms,"nvme_warm_exact_get":warm_get,
        "nvme_warm_cache":warm_cache,"warm_peak_rss_bytes":rss(),
        "point_after_warm_first":{"ram_hits":after_first_hot.ram_hits-before_hot.ram_hits,
            "nvme_hits":after_first_hot.nvme_hits-before_hot.nvme_hits,
            "remote_fetches":after_first_hot.remote_fetches-before_hot.remote_fetches},
        "point_ram_hot":{"ram_hits":after_hot.ram_hits-after_first_hot.ram_hits,
            "nvme_hits":after_hot.nvme_hits-after_first_hot.nvme_hits,
            "remote_fetches":after_hot.remote_fetches-after_first_hot.remote_fetches},
        "exact_top10_ids":result.iter().map(|n|n.id).collect::<Vec<_>>(),
        "budget_passed":warm_cache.nvme_bytes<=nvme_limit && warm_cache.ram_bytes<=ram_limit,
    }))
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() != 4 && !(args.len() == 5 && args[4] == "--warm-only") {
        return Err("usage: m23_cache QUERY.fvecs NAMESPACE CACHE_DIRECTORY".into());
    }
    let query = query(&args[1])?;
    let namespace = &args[2];
    let cache_dir = Path::new(&args[3]);
    let config = Config {
        dimensions: 128,
        metric: Metric::SquaredEuclidean,
    };
    let ram_limit = 4 * 1024 * 1024;
    let nvme_limit = 256 * 1024 * 1024;
    if args.len() == 5 {
        println!(
            "{}",
            warm_phase(&query, namespace, cache_dir, config, ram_limit, nvme_limit)?
        );
        return Ok(());
    }
    let first_store = store(namespace)?;
    let first_metrics = first_store.metrics();
    let open_start = Instant::now();
    let first = SegmentedDatabase::open(first_store, config)?
        .with_block_cache(cache_dir, ram_limit, nvme_limit)?;
    let cold_open_ms = open_start.elapsed().as_secs_f64() * 1000.;
    let cold_open_get = first_metrics.snapshot().get;
    let start = Instant::now();
    let expected = first.search_exact(&query, 10, &[])?;
    let cold_ms = start.elapsed().as_secs_f64() * 1000.;
    let cold_get = first_metrics.snapshot().get - cold_open_get;
    let cold_cache = first.cache_stats()?.unwrap();
    drop(first);

    let output = Command::new(env::current_exe()?)
        .arg(&args[1])
        .arg(namespace)
        .arg(&args[3])
        .arg("--warm-only")
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "fresh-process NVMe-warm probe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let warm: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let expected_ids = json!(expected.iter().map(|n| n.id).collect::<Vec<_>>());
    if warm["exact_top10_ids"] != expected_ids || warm["budget_passed"] != true {
        return Err("NVMe-warm result differs from cold exact result".into());
    }

    fs::remove_dir_all(cache_dir.join("glider-block-cache-v1"))?;
    let loss_store = store(namespace)?;
    let loss_metrics = loss_store.metrics();
    let lost = SegmentedDatabase::open(loss_store, config)?
        .with_block_cache(cache_dir, ram_limit, nvme_limit)?;
    let loss_open_get = loss_metrics.snapshot().get;
    let start = Instant::now();
    let lost_result = lost.search_exact(&query, 10, &[])?;
    let loss_ms = start.elapsed().as_secs_f64() * 1000.;
    let loss_get = loss_metrics.snapshot().get - loss_open_get;
    if lost_result != expected {
        return Err("total cache loss changed exact result".into());
    }
    let loss_cache = lost.cache_stats()?.unwrap();
    if loss_get != cold_get || loss_cache.remote_fetches != cold_cache.remote_fetches {
        return Err("total cache loss did not refetch the cold block set".into());
    }
    drop(lost);

    let pressure_dir = cache_dir.join("pressure");
    let pressure_store = store(namespace)?;
    let pressure_metrics = pressure_store.metrics();
    let pressure = SegmentedDatabase::open(pressure_store, config)?.with_block_cache(
        &pressure_dir,
        0,
        256 * 1024,
    )?;
    let pressure_open_get = pressure_metrics.snapshot().get;
    let start = Instant::now();
    let pressure_result = pressure.search_exact(&query, 10, &[])?;
    let pressure_first_ms = start.elapsed().as_secs_f64() * 1000.;
    let pressure_first_get = pressure_metrics.snapshot().get - pressure_open_get;
    let start = Instant::now();
    let pressure_again = pressure.search_exact(&query, 10, &[])?;
    let pressure_second_ms = start.elapsed().as_secs_f64() * 1000.;
    let pressure_second_get =
        pressure_metrics.snapshot().get - pressure_open_get - pressure_first_get;
    if pressure_result != expected || pressure_again != expected {
        return Err("cache pressure changed exact result".into());
    }
    let pressure_cache = pressure.cache_stats()?.unwrap();
    if pressure_first_get != cold_get || pressure_second_get != cold_get {
        return Err("cache pressure did not exercise remote eviction misses".into());
    }
    if cold_cache.nvme_bytes > nvme_limit
        || pressure_cache.nvme_bytes > 256 * 1024
        || cold_cache.ram_bytes > ram_limit
        || pressure_cache.ram_bytes != 0
    {
        return Err("cache occupancy exceeded configured budget".into());
    }
    println!(
        "{}",
        json!({
            "version":1,"rows":250000,"dimensions":128,"backend":"local_minio",
            "ram_budget_bytes":ram_limit,"nvme_budget_bytes":nvme_limit,
            "cold_open_ms":cold_open_ms,"cold_open_get":cold_open_get,
            "cold_exact_ms":cold_ms,"cold_exact_get":cold_get,"cold_cache":cold_cache,
            "warm_open_ms":warm["warm_open_ms"],"warm_open_get":warm["warm_open_get"],
            "nvme_warm_exact_ms":warm["nvme_warm_exact_ms"],
            "nvme_warm_exact_get":warm["nvme_warm_exact_get"],
            "nvme_warm_cache":warm["nvme_warm_cache"],
            "warm_peak_rss_bytes":warm["warm_peak_rss_bytes"],
            "point_after_warm_first":warm["point_after_warm_first"],
            "point_ram_hot":warm["point_ram_hot"],
            "cache_loss_exact_ms":loss_ms,"cache_loss_exact_get":loss_get,
            "cache_loss_cache":loss_cache,
            "pressure_budget_bytes":256*1024,"pressure_first_ms":pressure_first_ms,
            "pressure_first_get":pressure_first_get,"pressure_second_ms":pressure_second_ms,
            "pressure_second_get":pressure_second_get,"pressure_cache":pressure_cache,
            "exact_top10_ids":expected.iter().map(|n|n.id).collect::<Vec<_>>(),
            "exact_results_equal":true,"peak_rss_bytes":rss(),
        })
    );
    Ok(())
}
