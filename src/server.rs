//! HTTP/JSON service for one segmented collection.
//!
//! One process owns one namespace through `SegmentedServing` behind the
//! bounded admission queue. Writes are acknowledged only after durable
//! publication; every write carries a request ID (supplied by the client for
//! safe retries, otherwise issued by the server and returned).
use crate::segmented::QueryOptions;
use crate::{
    admission::{self, Client, Limits, Service, Shutdown},
    ownership::is_control_key,
    retry::{Conflict, Lookup, Outcome, Request, RequestId},
    segmented::{SegmentedDatabase, SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::{
        s3::{AmazonS3Builder, S3Store},
        LocalStore, ObjectStore,
    },
    Config, Error, Metric, Mutation,
};
use axum::{
    extract::{Path, Request as HttpRequest, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

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
    fn create(&mut self, key: &str, value: &[u8]) -> crate::Result<()> {
        each_store!(self, store => store.create(key, value))
    }
    fn remove(&mut self, key: &str) -> crate::Result<()> {
        each_store!(self, store => store.remove(key))
    }
    fn remove_many(&mut self, keys: &[String]) -> crate::Result<()> {
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
}

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
        })
    }

    /// Open the configured object-store namespace without claiming it.
    pub fn open_store(&self) -> crate::Result<Store> {
        self.store.open()
    }

    /// Claim and open the collection for serial administrative work.
    pub fn open_engine(&self) -> crate::Result<SegmentedServing<Store>> {
        SegmentedServing::open(
            self.open_store()?,
            self.collection,
            self.options.clone(),
            self.serving.clone(),
        )
    }

    /// Claim the namespace and start the admission worker. Blocking.
    pub fn start(&self) -> crate::Result<Service<SegmentedServing<Store>>> {
        Service::start(self.open_engine()?, self.limits).map_err(|error| match error {
            admission::Error::Database(error) => error,
            other => Error::Invalid(other.to_string()),
        })
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

/// Copy a stopped segmented namespace or backup into a fresh prefix. Metadata
/// publishes last; a failed destination must be discarded. The caller must
/// prove the source writer has stopped before staging a crashed namespace.
pub fn stage_segmented_namespace(
    source: &Store,
    mut destination: Store,
    config: Config,
    options: SegmentedOptions,
) -> crate::Result<(u64, usize, u64)> {
    if !destination.list()?.is_empty() {
        return Err(Error::Invalid("restore destination must be empty".into()));
    }
    let mut keys = source.list()?;
    keys.sort();
    if keys.windows(2).any(|pair| pair[0] == pair[1])
        || keys.iter().filter(|key| *key == "metadata").count() != 1
    {
        return Err(Error::Corrupt(
            "source listing has duplicate keys or no metadata".into(),
        ));
    }
    let mut copied = 0;
    let mut bytes = 0;
    for key in keys.iter().filter(|key| *key != "metadata") {
        if is_control_key(key) {
            continue;
        }
        let payload = source
            .get(key)?
            .ok_or_else(|| Error::Corrupt(format!("listed source object missing: {key}")))?;
        destination.create(key, &payload)?;
        copied += 1;
        bytes += payload.len() as u64;
    }
    let metadata = source
        .get("metadata")?
        .ok_or_else(|| Error::Corrupt("listed source metadata missing".into()))?;
    destination.create("metadata", &metadata)?;
    copied += 1;
    bytes += metadata.len() as u64;
    let restored = SegmentedDatabase::open_with_options(destination, config, options)?;
    Ok((restored.sequence(), copied, bytes))
}

type Engine0 = SegmentedServing<Store>;

#[derive(Clone)]
struct AppState {
    client: Client<Engine0>,
    token: Option<Arc<str>>,
    metrics: Arc<HttpMetrics>,
}

const ENDPOINTS: [&str; 8] = [
    "/healthz",
    "/metrics",
    "/v1/status",
    "/v1/write",
    "/v1/query",
    "/v1/points/{id}",
    "/v1/requests/{boundary}/{nonce}",
    "unmatched",
];
const BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

struct EndpointMetrics {
    requests: [AtomicU64; 5],
    buckets: [AtomicU64; 12],
    latency_micros: AtomicU64,
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_micros: AtomicU64::new(0),
        }
    }
}

struct HttpMetrics {
    endpoints: [EndpointMetrics; ENDPOINTS.len()],
}

impl HttpMetrics {
    fn new() -> Self {
        Self {
            endpoints: std::array::from_fn(|_| EndpointMetrics::new()),
        }
    }
}

fn endpoint(path: &str) -> usize {
    match path {
        "/healthz" => 0,
        "/metrics" => 1,
        "/v1/status" => 2,
        "/v1/write" => 3,
        "/v1/query" => 4,
        path if path.starts_with("/v1/points/") => 5,
        path if path.starts_with("/v1/requests/") => 6,
        _ => 7,
    }
}

async fn record_metrics(
    State(metrics): State<Arc<HttpMetrics>>,
    request: HttpRequest,
    next: Next,
) -> Response {
    let endpoint = endpoint(request.uri().path());
    let start = Instant::now();
    let response = next.run(request).await;
    let elapsed = start.elapsed();
    let counters = &metrics.endpoints[endpoint];
    let class = usize::from(response.status().as_u16() / 100);
    if (1..=5).contains(&class) {
        counters.requests[class - 1].fetch_add(1, Ordering::Relaxed);
    }
    let seconds = elapsed.as_secs_f64();
    for (index, bound) in BUCKETS.iter().enumerate() {
        if seconds <= *bound {
            counters.buckets[index].fetch_add(1, Ordering::Relaxed);
        }
    }
    counters.buckets[BUCKETS.len()].fetch_add(1, Ordering::Relaxed);
    counters.latency_micros.fetch_add(
        elapsed.as_micros().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
    response
}

/// JSON error with a status that tells clients whether to retry.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<admission::Error> for ApiError {
    fn from(error: admission::Error) -> Self {
        let status = match &error {
            admission::Error::Overloaded => StatusCode::TOO_MANY_REQUESTS,
            admission::Error::Database(Error::Invalid(_)) => StatusCode::BAD_REQUEST,
            admission::Error::Database(Error::RequestConflict | Error::RequestExpired) => {
                StatusCode::CONFLICT
            }
            admission::Error::Database(Error::Corrupt(_)) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::SERVICE_UNAVAILABLE,
        };
        ApiError(status, error.to_string())
    }
}

fn bad_request(message: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, message.into())
}

/// Run blocking admission work off the async executor.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| ApiError(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
}

fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(token) = &state.token else {
        return Ok(());
    };
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if supplied == Some(token.as_ref()) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token".into(),
        ))
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
struct RequestIdJson {
    boundary: u64,
    /// 32 lowercase hexadecimal digits.
    nonce: String,
}

impl RequestIdJson {
    fn from_id(id: RequestId) -> Self {
        Self {
            boundary: id.boundary,
            nonce: id.nonce.iter().map(|byte| format!("{byte:02x}")).collect(),
        }
    }

    fn to_id(&self) -> Result<RequestId, ApiError> {
        let invalid = || bad_request("request_id.nonce must be 32 lowercase hex digits");
        if self.nonce.len() != 32 || self.nonce.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(invalid());
        }
        let mut nonce = [0; 16];
        for (index, byte) in nonce.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&self.nonce[index * 2..index * 2 + 2], 16)
                .map_err(|_| invalid())?;
        }
        Ok(RequestId {
            boundary: self.boundary,
            nonce,
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Point {
    id: u64,
    vector: Vec<f32>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteBody {
    #[serde(default)]
    upsert: Vec<Point>,
    #[serde(default)]
    delete: Vec<u64>,
    request_id: Option<RequestIdJson>,
}

fn outcome_json(outcome: Outcome, id: RequestId) -> Value {
    json!({
        "sequence": outcome.sequence,
        "request_id": RequestIdJson::from_id(id),
        "conflict": outcome.conflict.map(|conflict| match conflict {
            Conflict::StaleRevision => "stale_revision",
            Conflict::ExpiredRevision => "expired_revision",
        }),
    })
}

/// `POST /v1/write`: one atomic batch of upserts then deletes.
async fn write(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<WriteBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let mut mutations: Vec<_> = body
        .upsert
        .into_iter()
        .map(|point| Mutation::Put {
            id: point.id,
            vector: point.vector,
            metadata: point.metadata,
        })
        .collect();
    mutations.extend(body.delete.into_iter().map(|id| Mutation::Delete { id }));
    if mutations.is_empty() {
        return Err(bad_request("write needs at least one upsert or delete"));
    }
    let supplied = body
        .request_id
        .as_ref()
        .map(RequestIdJson::to_id)
        .transpose()?;
    let client = state.client.clone();
    blocking(move || {
        let id = match supplied {
            Some(id) => id,
            None => client.observe(0)?.wait()?.value.request_id,
        };
        let outcome = client
            .write(Request {
                id,
                conditions: Vec::new(),
                mutations,
            })?
            .wait()?
            .value;
        Ok(Json(outcome_json(outcome, id)))
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryBody {
    vector: Vec<f32>,
    #[serde(default = "default_k")]
    k: usize,
    #[serde(default)]
    filter: BTreeMap<String, String>,
    #[serde(default)]
    include_metadata: bool,
    #[serde(default)]
    include_vector: bool,
}

fn default_k() -> usize {
    10
}

/// `POST /v1/query`: unfiltered approximate search within the read budget,
/// or exact search of the declared resident filter.
async fn query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<QueryBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if body.k == 0 || body.k > 1_000 {
        return Err(bad_request("k must be between 1 and 1000"));
    }
    let client = state.client.clone();
    blocking(move || {
        let options = QueryOptions {
            include_metadata: body.include_metadata,
            include_vector: body.include_vector,
        };
        let result = client
            .query_with_options(
                body.vector,
                body.k,
                body.filter.into_iter().collect(),
                options,
            )?
            .wait()?
            .value;
        Ok(Json(json!({
            "sequence": result.sequence,
            "results": result
                .hits
                .iter()
                .map(|hit| {
                    let mut result = json!({ "id": hit.id, "distance": hit.distance });
                    if let Some(metadata) = &hit.metadata {
                        result["metadata"] = json!(metadata);
                    }
                    if let Some(vector) = &hit.vector {
                        result["vector"] = json!(vector);
                    }
                    result
                })
                .collect::<Vec<_>>(),
        })))
    })
    .await
}

/// `GET /v1/points/{id}`.
async fn point(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Result<Response, ApiError> {
    authorize(&state, &headers)?;
    let client = state.client.clone();
    blocking(move || {
        Ok(match client.get(id)?.wait()?.value {
            Some(document) => Json(json!({
                "id": id,
                "vector": document.vector,
                "metadata": document.metadata,
            }))
            .into_response(),
            None => ApiError(StatusCode::NOT_FOUND, format!("no point {id}")).into_response(),
        })
    })
    .await
}

/// `GET /v1/requests/{boundary}/{nonce}`: resolve an uncertain write.
async fn request(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((boundary, nonce)): Path<(u64, String)>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let id = RequestIdJson { boundary, nonce }.to_id()?;
    let client = state.client.clone();
    blocking(move || {
        Ok(Json(match client.lookup(id)?.wait()?.value {
            Lookup::Retained(outcome) => {
                json!({ "state": "retained", "outcome": outcome_json(outcome, id) })
            }
            Lookup::Unknown => json!({ "state": "unknown" }),
            Lookup::Expired => json!({ "state": "expired" }),
            Lookup::Ahead => json!({ "state": "ahead" }),
        }))
    })
    .await
}

/// `GET /v1/status`.
async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let client = state.client.clone();
    blocking(move || {
        let engine = client.metrics()?.wait()?.value;
        let queue = client.status();
        Ok(Json(json!({
            "sequence": engine.sequence,
            "queued_commands": queue.commands,
            "queued_bytes": queue.bytes,
            "closed": queue.closed,
            "failed": queue.failed,
            "maintenance_errors": queue.maintenance_errors,
            "cache": cache_status(&engine),
        })))
    })
    .await
}

/// NVMe warm-up state from the engine's cache samples: `disabled` without an
/// NVMe tier, `cold` before the first warm-up unit, `warming` during a pass,
/// `warm` when the tier holds every block of the selected root, and
/// `partial` when a pass ended with part of the root uncached (the limit is
/// below `namespace_bytes`). Queries never depend on it for correctness.
fn cache_status(engine: &admission::EngineMetrics) -> Value {
    let sample = |name: &str| {
        engine
            .samples
            .iter()
            .find(|(sample, _)| *sample == name)
            .map_or(0, |&(_, value)| value)
    };
    let (limit, namespace, warm) = (
        sample("glider_cache_nvme_limit_bytes"),
        sample("glider_cache_namespace_bytes"),
        sample("glider_cache_warm_bytes"),
    );
    let state = match (limit, sample("glider_cache_warm_complete"), namespace) {
        (0, _, _) => "disabled",
        (_, 0, 0) => "cold",
        (_, 0, _) => "warming",
        _ if warm >= namespace => "warm",
        _ => "partial",
    };
    json!({
        "state": state,
        "nvme_bytes": sample("glider_cache_nvme_bytes"),
        "nvme_limit_bytes": limit,
        "namespace_bytes": namespace,
        "warm_bytes": warm,
    })
}

async fn health(State(state): State<AppState>) -> StatusCode {
    let queue = state.client.status();
    if queue.failed || queue.closed {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

fn render_metrics(
    http: &HttpMetrics,
    queue: admission::Status,
    engine: admission::EngineMetrics,
) -> String {
    let mut body = String::new();
    body.push_str("# TYPE glider_http_requests_total counter\n");
    for (name, metrics) in ENDPOINTS.iter().zip(&http.endpoints) {
        for (index, count) in metrics.requests.iter().enumerate() {
            writeln!(
                body,
                "glider_http_requests_total{{endpoint=\"{name}\",status_class=\"{}xx\"}} {}",
                index + 1,
                count.load(Ordering::Relaxed)
            )
            .unwrap();
        }
    }
    body.push_str("# TYPE glider_http_request_duration_seconds histogram\n");
    for (name, metrics) in ENDPOINTS.iter().zip(&http.endpoints) {
        for (index, bound) in BUCKETS.iter().enumerate() {
            writeln!(body, "glider_http_request_duration_seconds_bucket{{endpoint=\"{name}\",le=\"{bound}\"}} {}", metrics.buckets[index].load(Ordering::Relaxed)).unwrap();
        }
        writeln!(
            body,
            "glider_http_request_duration_seconds_bucket{{endpoint=\"{name}\",le=\"+Inf\"}} {}",
            metrics.buckets[BUCKETS.len()].load(Ordering::Relaxed)
        )
        .unwrap();
        writeln!(
            body,
            "glider_http_request_duration_seconds_sum{{endpoint=\"{name}\"}} {}",
            metrics.latency_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
        )
        .unwrap();
        writeln!(
            body,
            "glider_http_request_duration_seconds_count{{endpoint=\"{name}\"}} {}",
            metrics.buckets[BUCKETS.len()].load(Ordering::Relaxed)
        )
        .unwrap();
    }
    for (name, value, kind) in [
        ("glider_admission_commands", queue.commands as u64, "gauge"),
        ("glider_admission_bytes", queue.bytes as u64, "gauge"),
        ("glider_worker_failed", u64::from(queue.failed), "gauge"),
        ("glider_worker_closed", u64::from(queue.closed), "gauge"),
        (
            "glider_maintenance_errors_total",
            queue.maintenance_errors,
            "counter",
        ),
        ("glider_committed_sequence", engine.sequence, "gauge"),
    ] {
        writeln!(body, "# TYPE {name} {kind}\n{name} {value}").unwrap();
    }
    for (name, value) in engine.samples {
        let kind = if name.ends_with("_total") {
            "counter"
        } else {
            "gauge"
        };
        writeln!(body, "# TYPE {name} {kind}\n{name} {value}").unwrap();
    }
    body
}

/// `GET /metrics`: unauthenticated Prometheus text exposition.
async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let client = state.client.clone();
    let http = state.metrics.clone();
    blocking(move || {
        let engine = client.metrics()?.wait()?.value;
        let queue = client.status();
        let body = render_metrics(&http, queue, engine);
        Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response())
    })
    .await
}

/// Routes for one collection served by `client`.
pub fn router(client: Client<Engine0>, token: Option<String>) -> Router {
    let http_metrics = Arc::new(HttpMetrics::new());
    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/status", get(status))
        .route("/v1/write", post(write))
        .route("/v1/query", post(query))
        .route("/v1/points/{id}", get(point))
        .route("/v1/requests/{boundary}/{nonce}", get(request))
        .with_state(AppState {
            client,
            token: token.map(Arc::from),
            metrics: http_metrics.clone(),
        })
        .layer(middleware::from_fn_with_state(http_metrics, record_metrics))
}

/// Serve until SIGINT or SIGTERM, then drain queued work and release the
/// namespace's ownership claim. A worker failure keeps the claim; follow
/// `docs/RECOVERY.md` before reopening.
pub async fn run(config: ServerConfig) -> crate::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    let service = tokio::task::block_in_place(|| config.start())?;
    let app = router(service.client(), config.token.clone());
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let interrupt = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("install SIGTERM handler");
                tokio::select! {
                    _ = interrupt => {},
                    _ = terminate.recv() => {},
                }
            }
            #[cfg(not(unix))]
            let _ = interrupt.await;
        })
        .await?;
    tokio::task::block_in_place(|| service.shutdown(Shutdown::Drain)).map_err(|error| match error {
        admission::Error::Database(error) => error,
        other => Error::Invalid(other.to_string()),
    })
}
