use crate::{
    admission::Limits,
    segmented::{SegmentedOptions, SegmentedServingOptions},
    store::{
        s3::{AmazonS3Builder, S3Store},
        LocalStore, ObjectStore,
    },
    Config, Error, Metric,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// The namespace's backing store: a local directory for development, or an
/// S3-compatible bucket.
pub enum Store {
    Local(LocalStore),
    S3(S3Store),
}

macro_rules! each_store {
    ($self:expr, $store:ident => $body:expr) => {
        match $self {
            Store::Local($store) => $body,
            Store::S3($store) => $body,
        }
    };
}

impl ObjectStore for Store {
    fn get(&self, key: &str) -> crate::Result<Option<Vec<u8>>> {
        each_store!(self, store => store.get(key))
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        payload_len: usize,
    ) -> crate::Result<Option<Vec<u8>>> {
        each_store!(self, store => store.get_range(key, offset, length, payload_len))
    }
    fn get_many(&self, keys: &[String]) -> crate::Result<Vec<Option<Vec<u8>>>> {
        each_store!(self, store => store.get_many(keys))
    }
    fn get_ranges(
        &self,
        ranges: &[(&str, usize, usize, usize)],
    ) -> crate::Result<Vec<Option<Vec<u8>>>> {
        each_store!(self, store => store.get_ranges(ranges))
    }
    fn list(&self) -> crate::Result<Vec<String>> {
        each_store!(self, store => store.list())
    }
    fn create(&self, key: &str, value: &[u8]) -> crate::Result<()> {
        each_store!(self, store => store.create(key, value))
    }
    fn remove(&self, key: &str) -> crate::Result<()> {
        each_store!(self, store => store.remove(key))
    }
    fn remove_many(&self, keys: &[String]) -> crate::Result<()> {
        each_store!(self, store => store.remove_many(keys))
    }
}

/// Everything the server needs; see [`ServerConfig::from_env`].
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub store: StoreConfig,
    pub collection: Config,
    pub options: SegmentedOptions,
    pub serving: SegmentedServingOptions,
    pub limits: Limits,
    /// Required `Authorization: Bearer` token, if set.
    pub token: Option<String>,
    /// Writer lease duration: a restart after a crash waits at most this
    /// long before taking over. It never affects correctness.
    pub lease: Duration,
}

#[derive(Clone)]
pub enum StoreConfig {
    Local(PathBuf),
    S3 {
        bucket: String,
        namespace: String,
        region: String,
        endpoint: Option<String>,
    },
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn required(name: &str) -> crate::Result<String> {
    env(name).ok_or_else(|| Error::Invalid(format!("set {name}")))
}

impl ServerConfig {
    /// Read the configuration from environment variables:
    ///
    /// - `GLIDER_LISTEN` (default `127.0.0.1:8080`), `GLIDER_API_TOKEN`
    /// - `GLIDER_DIMENSIONS`, `GLIDER_METRIC` (`squared_euclidean`,
    ///   `manhattan` or `cosine`), optional `GLIDER_RESIDENT_FILTER=key=value`,
    ///   `GLIDER_ROUTED_KEYS=key1,key2` (at most four)
    /// - storage: `GLIDER_DATA_DIR` for a local directory, or
    ///   `GLIDER_S3_BUCKET`, `GLIDER_S3_NAMESPACE`, `GLIDER_S3_REGION`
    ///   (default `us-east-1`), optional `GLIDER_S3_ENDPOINT` and the usual
    ///   `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`
    /// - `GLIDER_CACHE_DIR` (default `glider-cache`), `GLIDER_CACHE_BYTES`
    ///   (NVMe cache, default 256 MiB, which idle warm-up fills with the
    ///   namespace), `GLIDER_LOCAL_BLOCKS` (cached blocks a query may rerank
    ///   locally beyond its remote budget, default 24; 0 makes results
    ///   independent of cache contents)
    /// - `GLIDER_LEASE_SECONDS` (default 10): writer lease duration
    /// - `GLIDER_AUTO_CLUSTER_ROWS` (default 250,000): live sealed rows at
    ///   which a namespace without a clustered view converts to one as idle
    ///   maintenance; 0 disables
    pub fn from_env() -> crate::Result<Self> {
        let invalid = |name: &str| Error::Invalid(format!("invalid {name}"));
        let dimensions = required("GLIDER_DIMENSIONS")?
            .parse()
            .map_err(|_| invalid("GLIDER_DIMENSIONS"))?;
        let metric = match env("GLIDER_METRIC")
            .as_deref()
            .unwrap_or("squared_euclidean")
        {
            "squared_euclidean" => Metric::SquaredEuclidean,
            "manhattan" => Metric::Manhattan,
            "cosine" => Metric::Cosine,
            _ => return Err(invalid("GLIDER_METRIC")),
        };
        let resident_filter = env("GLIDER_RESIDENT_FILTER")
            .map(|value| {
                value
                    .split_once('=')
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    .ok_or_else(|| invalid("GLIDER_RESIDENT_FILTER"))
            })
            .transpose()?;
        let mut routed_keys: Vec<String> = env("GLIDER_ROUTED_KEYS")
            .map(|value| value.split(',').map(|key| key.trim().to_owned()).collect())
            .unwrap_or_default();
        routed_keys.sort();
        routed_keys.dedup();
        let store = match env("GLIDER_DATA_DIR") {
            Some(directory) => StoreConfig::Local(directory.into()),
            None => StoreConfig::S3 {
                bucket: required("GLIDER_S3_BUCKET")?,
                namespace: required("GLIDER_S3_NAMESPACE")?,
                region: env("GLIDER_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
                endpoint: env("GLIDER_S3_ENDPOINT"),
            },
        };
        let mut serving = SegmentedServingOptions::m31(
            env("GLIDER_CACHE_DIR")
                .unwrap_or("glider-cache".into())
                .into(),
        );
        if let Some(bytes) = env("GLIDER_CACHE_BYTES") {
            let bytes = bytes.parse().map_err(|_| invalid("GLIDER_CACHE_BYTES"))?;
            if let Some(cache) = serving.cache.as_mut() {
                cache.2 = bytes;
            }
        }
        if let Some(blocks) = env("GLIDER_LOCAL_BLOCKS") {
            serving.read_budget.local_blocks =
                blocks.parse().map_err(|_| invalid("GLIDER_LOCAL_BLOCKS"))?;
        }
        if let Some(rows) = env("GLIDER_AUTO_CLUSTER_ROWS") {
            serving.auto_cluster_rows = rows
                .parse()
                .map_err(|_| invalid("GLIDER_AUTO_CLUSTER_ROWS"))?;
        }
        Ok(Self {
            listen: env("GLIDER_LISTEN")
                .unwrap_or_else(|| "127.0.0.1:8080".into())
                .parse()
                .map_err(|_| invalid("GLIDER_LISTEN"))?,
            store,
            collection: Config { dimensions, metric },
            options: SegmentedOptions {
                resident_filter,
                routed_keys,
            },
            serving,
            limits: Limits {
                read_priority: Some(std::time::Duration::from_millis(50)),
                ..Limits::default()
            },
            token: env("GLIDER_API_TOKEN"),
            lease: match env("GLIDER_LEASE_SECONDS") {
                Some(seconds) => seconds
                    .parse()
                    .ok()
                    .and_then(|seconds| Duration::try_from_secs_f64(seconds).ok())
                    .ok_or_else(|| invalid("GLIDER_LEASE_SECONDS"))?,
                None => Duration::from_secs(10),
            },
        })
    }

    /// Open the configured object-store namespace without claiming it.
    pub fn open_store(&self) -> crate::Result<Store> {
        self.store.open()
    }
}

impl StoreConfig {
    /// Open a local directory or S3 prefix without claiming it.
    pub fn open(&self) -> crate::Result<Store> {
        Ok(match self {
            StoreConfig::Local(directory) => {
                std::fs::create_dir_all(directory)?;
                Store::Local(LocalStore::open(directory)?)
            }
            StoreConfig::S3 {
                bucket,
                namespace,
                region,
                endpoint,
            } => {
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region);
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_allow_http(endpoint.starts_with("http://"))
                        .with_endpoint(endpoint);
                }
                Store::S3(S3Store::open(builder, namespace)?)
            }
        })
    }
}
