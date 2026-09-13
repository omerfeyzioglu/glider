use serde_json::{json, Value};

/// Nearest-rank sample percentiles. Zero duration is measurable, but its
/// throughput is undefined. Missing/invalid samples never become zero metrics.
pub fn timing(samples: &[f64], operations_per_sample: usize) -> Value {
    let valid = !samples.is_empty()
        && operations_per_sample > 0
        && samples.iter().all(|n| n.is_finite() && *n >= 0.0);
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let percentile =
        |percent: usize| valid.then(|| sorted[(sorted.len() * percent).div_ceil(100) - 1]);
    let sum: f64 = samples.iter().sum();
    json!({
        "raw_sample_ns": samples, "sample_count": samples.len(),
        "operations_per_sample": operations_per_sample,
        "min_sample_ns": valid.then(|| sorted[0]),
        "p50_sample_ns": percentile(50), "p95_sample_ns": percentile(95),
        "p99_sample_ns": percentile(99), "max_sample_ns": valid.then(|| sorted[sorted.len()-1]),
        "mean_ns_per_operation": valid.then(|| sum / samples.len() as f64 / operations_per_sample as f64),
        "p50_amortized_ns_per_operation": percentile(50).map(|v| v / operations_per_sample as f64),
        "operations_per_timed_second": (valid && sum > 0.0 && sum.is_finite()).then(|| samples.len() as f64 * operations_per_sample as f64 * 1e9 / sum),
    })
}

#[derive(Clone, Copy)]
pub struct Usage {
    pub user_ns: u64,
    pub system_ns: u64,
    pub max_rss_bytes: u64,
}
impl Usage {
    /// RUSAGE_SELF excludes child processes. Linux reports RSS in KiB; macOS
    /// reports bytes. Unsupported platforms and failed probes return None.
    pub fn capture() -> Option<Self> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            // SAFETY: getrusage initializes this correctly sized output on success.
            if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
                return None;
            }
            // SAFETY: checked the successful initialization above.
            let usage = unsafe { usage.assume_init() };
            let time = |value: libc::timeval| -> Option<u64> {
                u64::try_from(value.tv_sec)
                    .ok()?
                    .checked_mul(1_000_000_000)?
                    .checked_add(u64::try_from(value.tv_usec).ok()?.checked_mul(1000)?)
            };
            let rss = u64::try_from(usage.ru_maxrss).ok()?;
            Some(Self {
                user_ns: time(usage.ru_utime)?,
                system_ns: time(usage.ru_stime)?,
                max_rss_bytes: rss.checked_mul(if cfg!(target_os = "linux") { 1024 } else { 1 })?,
            })
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        None
    }
}

pub fn usage_delta(before: Option<Usage>, after: Option<Usage>) -> Value {
    let delta = before.zip(after).and_then(|(a, b)| {
        Some((
            b.user_ns.checked_sub(a.user_ns)?,
            b.system_ns.checked_sub(a.system_ns)?,
            b.max_rss_bytes,
        ))
    });
    json!({
        "user_cpu_ns": delta.map(|d| d.0),
        "system_cpu_ns": delta.map(|d| d.1),
        // This is deliberately not a subtraction of two high-water marks.
        "process_max_rss_bytes": after.map(|a| a.max_rss_bytes),
    })
}

pub fn resources(samples: Vec<Value>, scope: &str) -> Value {
    let sum = |key: &str| -> Option<u64> {
        if samples.is_empty() {
            return None;
        }
        samples
            .iter()
            .try_fold(0_u64, |acc, s| acc.checked_add(s[key].as_u64()?))
    };
    let rss = samples
        .iter()
        .map(|s| s["process_max_rss_bytes"].as_u64())
        .collect::<Option<Vec<_>>>()
        .and_then(|v| v.into_iter().max());
    json!({"user_cpu_ns": sum("user_cpu_ns"), "system_cpu_ns": sum("system_cpu_ns"),
        "process_max_rss_bytes": rss, "cpu_scope": scope,
        "rss_scope": "process lifetime high-water mark including setup and earlier scenarios",
        "raw_samples": samples})
}

pub fn metadata(feature: &str, phase: &str, group: &str) -> Result<Value, &'static str> {
    if feature.trim().is_empty() || group.trim().is_empty() {
        return Err("feature and comparison group must be nonempty");
    }
    if !matches!(phase, "baseline" | "before" | "after") {
        return Err("phase must be baseline, before, or after");
    }
    Ok(json!({"feature": feature, "phase": phase, "comparison_group": group}))
}
