//! Benchmark-only setup, inventory and request observation. Workloads stay generic.
use super::Result;
use glider::store::{LocalStore, ObjectStore};
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub trait Namespace {
    type Store: ObjectStore;
    const NAME: &'static str;
    const RECOVERY_CACHE: &'static str;
    fn open(&self) -> Result<Self::Store>;
    fn inventory(&self) -> Result<Value>;
    fn observe(store: &Self::Store) -> Observer;
    fn annotate(&self, _: &mut Value) {}
}
pub struct LocalNamespace {
    _temp: tempfile::TempDir,
    root: PathBuf,
}
impl LocalNamespace {
    pub fn new(root: &Path, scenario: &str) -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix(&format!("glider-{scenario}-"))
            .tempdir_in(root)?;
        let root = temp.path().join("db");
        Ok(Self { _temp: temp, root })
    }
}
impl Namespace for LocalNamespace {
    type Store = LocalStore;
    const NAME: &'static str = "local";
    const RECOVERY_CACHE: &'static str = "warm OS cache; no eviction";
    fn open(&self) -> Result<LocalStore> {
        Ok(LocalStore::open(&self.root)?)
    }
    fn observe(_: &LocalStore) -> Observer {
        Observer::Local
    }
    fn inventory(&self) -> Result<Value> {
        // Same untimed LocalStore inventory as the original harness.
        let objects = LocalStore::open(&self.root)?.list()?.len();
        let mut files = 0_u64;
        let mut file_bytes = 0_u64;
        for entry in fs::read_dir(&self.root)? {
            let metadata = entry?.metadata()?;
            if !metadata.is_file() {
                return Err("unexpected non-file in benchmark namespace".into());
            }
            files += 1;
            file_bytes += metadata.len();
        }
        Ok(
            json!({"logical_objects": objects, "physical_files": files, "file_length_bytes": file_bytes}),
        )
    }
}
pub enum Observer {
    Local,
    #[cfg(feature = "s3")]
    S3(glider::store::s3::RequestMetrics),
}
impl Observer {
    pub fn snapshot(&self) -> Option<Value> {
        match self {
            Self::Local => None,
            #[cfg(feature = "s3")]
            Self::S3(metrics) => {
                let c = metrics.snapshot();
                Some(
                    json!({"get": c.get, "list": c.list, "put": c.put, "other": c.other,
                    "request_body_bytes": c.request_body_bytes, "http_errors": c.http_errors,
                    "transport_errors": c.transport_errors}),
                )
            }
        }
    }
    pub fn delta(&self, before: Option<Value>) -> Option<Value> {
        before.zip(self.snapshot()).map(|(a, b)| http_delta(&a, &b))
    }
}
pub fn http_delta(before: &Value, after: &Value) -> Value {
    let mut result = json!({});
    for key in [
        "get",
        "list",
        "put",
        "other",
        "request_body_bytes",
        "http_errors",
        "transport_errors",
    ] {
        result[key] = json!(before[key]
            .as_u64()
            .zip(after[key].as_u64())
            .and_then(|(a, b)| b.checked_sub(a)));
    }
    result
}
pub fn attach_http(result: &mut Value, key: &str, counts: Option<Value>) {
    if let Some(counts) = counts {
        result[key] = counts;
    }
}

#[cfg(feature = "s3")]
pub use s3::{S3Config, S3Namespace};
#[cfg(feature = "s3")]
mod s3 {
    use super::*;
    use futures::TryStreamExt;
    use glider::store::s3::{AmazonS3Builder, S3Store};
    use object_store::{ObjectStore as RemoteStore, RetryConfig};

    // Deliberately neither Debug nor Serialize: credentials never enter reports.
    #[derive(Clone)]
    pub struct S3Config {
        endpoint: String,
        bucket: String,
        region: String,
        namespace: String,
        access_key: String,
        secret_key: String,
        token: Option<String>,
        service_label: Option<String>,
    }
    impl S3Config {
        pub fn from_env() -> Result<Self> {
            Self::read(|key| std::env::var(key).ok())
        }
        pub fn read(mut env: impl FnMut(&str) -> Option<String>) -> Result<Self> {
            let mut required = |name| {
                env(name)
                    .filter(|s| !s.trim().is_empty())
                    .ok_or_else(|| format!("missing environment variable: {name}"))
            };
            let endpoint = required("GLIDER_S3_ENDPOINT")?;
            let bucket = required("GLIDER_S3_BUCKET")?;
            let region = required("GLIDER_S3_REGION")?;
            let namespace = required("GLIDER_S3_NAMESPACE")?;
            let access_key = required("AWS_ACCESS_KEY_ID")?;
            let secret_key = required("AWS_SECRET_ACCESS_KEY")?;
            // Reject credential-bearing URL components rather than accidentally
            // persisting userinfo, signed queries or fragments into the archive.
            let host = endpoint
                .strip_prefix("https://")
                .or_else(|| endpoint.strip_prefix("http://"));
            if host.is_none_or(|h| h.is_empty() || h.starts_with('/'))
                || endpoint.contains(['@', '?', '#'])
                || endpoint.chars().any(char::is_whitespace)
            {
                return Err("GLIDER_S3_ENDPOINT must be an HTTP(S) endpoint without userinfo, query or fragment".into());
            }
            if namespace.split('/').any(|p| {
                p.is_empty()
                    || !p
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            }) {
                return Err(
                    "GLIDER_S3_NAMESPACE must contain nonempty alphanumeric, '-' or '_' components"
                        .into(),
                );
            }
            Ok(Self {
                endpoint,
                bucket,
                region,
                namespace,
                access_key,
                secret_key,
                token: env("AWS_SESSION_TOKEN").filter(|s| !s.is_empty()),
                service_label: env("GLIDER_S3_SERVICE_LABEL"),
            })
        }
        fn builder(&self) -> AmazonS3Builder {
            let mut builder = AmazonS3Builder::new()
                .with_endpoint(&self.endpoint)
                .with_bucket_name(&self.bucket)
                .with_region(&self.region)
                .with_access_key_id(&self.access_key)
                .with_secret_access_key(&self.secret_key)
                .with_allow_http(self.endpoint.starts_with("http://"))
                .with_virtual_hosted_style_request(false)
                .with_retry(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                });
            if let Some(token) = &self.token {
                builder = builder.with_token(token);
            }
            builder
        }
        pub fn metadata(&self) -> Value {
            json!({"endpoint": self.endpoint, "bucket": self.bucket, "region": self.region,
                "namespace_prefix": self.namespace, "service_label": self.service_label,
                "addressing": "path", "automatic_retries": 0, "request_timeout_seconds": 30})
        }
    }
    pub struct S3Namespace {
        config: S3Config,
        namespace: String,
        _unique: tempfile::TempDir,
    }
    impl S3Namespace {
        pub fn new(config: S3Config, scenario: &str) -> Result<Self> {
            // Local scratch is only a collision-resistant run identifier. S3
            // persistence/recovery never uses it. It contains no object data.
            let unique = tempfile::Builder::new()
                .prefix(&format!("glider-{scenario}-"))
                .tempdir()?;
            let suffix = unique.path().file_name().unwrap().to_str().unwrap();
            let namespace = format!("{}/{suffix}", config.namespace);
            let value = Self {
                config,
                namespace,
                _unique: unique,
            };
            if !value.open()?.list()?.is_empty() {
                return Err("benchmark namespace must be empty".into());
            }
            Ok(value)
        }
    }
    impl Namespace for S3Namespace {
        type Store = S3Store;
        const NAME: &'static str = "s3";
        const RECOVERY_CACHE: &'static str =
            "remote reopen after one warmup; server cache uncontrolled";
        fn open(&self) -> Result<S3Store> {
            Ok(S3Store::open(self.config.builder(), &self.namespace)?)
        }
        fn observe(store: &S3Store) -> Observer {
            Observer::S3(store.metrics())
        }
        fn annotate(&self, result: &mut Value) {
            result["namespace"] = json!(self.namespace);
        }
        fn inventory(&self) -> Result<Value> {
            // Native object metadata, not inferred envelope sizes or client files.
            // A separate client keeps these untimed LISTs out of workload counters.
            let store = self.config.builder().build()?;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let prefix = object_store::path::Path::from(self.namespace.as_str());
            let objects: Vec<_> = rt.block_on(store.list(Some(&prefix)).try_collect())?;
            let bytes = objects
                .iter()
                .try_fold(0_u64, |sum, o| sum.checked_add(o.size))
                .ok_or("object inventory size overflow")?;
            Ok(
                json!({"logical_objects": objects.len(), "object_length_bytes": bytes,
                "physical_files": null, "file_length_bytes": null}),
            )
        }
    }
}
