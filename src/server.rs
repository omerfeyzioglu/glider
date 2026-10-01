//! HTTP/JSON service for one segmented collection.
//!
//! One process owns one namespace through `SegmentedServing` behind the
//! bounded admission queue. Writes are acknowledged only after durable
//! publication; every write carries a request ID (supplied by the client for
//! safe retries, otherwise issued by the server and returned).
use crate::{
    admission::{self, Client, Limits, Service, Shutdown},
    retry::{Conflict, Lookup, Outcome, Request, RequestId},
    segmented::{SegmentedOptions, SegmentedServing, SegmentedServingOptions},
    store::{
        s3::{AmazonS3Builder, S3Store},
        LocalStore, ObjectStore,
    },
    Config, Error, Metric, Mutation,
};
use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc};

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
    ///   `manhattan` or `cosine`), optional `GLIDER_RESIDENT_FILTER=key=value`
    /// - storage: `GLIDER_DATA_DIR` for a local directory, or
    ///   `GLIDER_S3_BUCKET`, `GLIDER_S3_NAMESPACE`, `GLIDER_S3_REGION`
    ///   (default `us-east-1`), optional `GLIDER_S3_ENDPOINT` and the usual
    ///   `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`
    /// - `GLIDER_CACHE_DIR` (default `glider-cache`), `GLIDER_CACHE_BYTES`
    ///   (NVMe cache, default 256 MiB)
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
        let store = match env("GLIDER_DATA_DIR") {
            Some(directory) => StoreConfig::Local(directory.into()),
            None => StoreConfig::S3 {
                bucket: required("GLIDER_S3_BUCKET")?,
                namespace: required("GLIDER_S3_NAMESPACE")?,
                region: env("GLIDER_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
                endpoint: env("GLIDER_S3_ENDPOINT"),
            },
        };
        let mut serving = SegmentedServingOptions::m21(
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
        Ok(Self {
            listen: env("GLIDER_LISTEN")
                .unwrap_or_else(|| "127.0.0.1:8080".into())
                .parse()
                .map_err(|_| invalid("GLIDER_LISTEN"))?,
            store,
            collection: Config { dimensions, metric },
            options: SegmentedOptions { resident_filter },
            serving,
            limits: Limits {
                read_priority: Some(std::time::Duration::from_millis(50)),
                ..Limits::default()
            },
            token: env("GLIDER_API_TOKEN"),
        })
    }

    fn open_store(&self) -> crate::Result<Store> {
        Ok(match &self.store {
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

    /// Claim the namespace and start the admission worker. Blocking.
    pub fn start(&self) -> crate::Result<Service<SegmentedServing<Store>>> {
        let engine = SegmentedServing::open(
            self.open_store()?,
            self.collection,
            self.options.clone(),
            self.serving.clone(),
        )?;
        Service::start(engine, self.limits).map_err(|error| match error {
            admission::Error::Database(error) => error,
            other => Error::Invalid(other.to_string()),
        })
    }
}

type Engine0 = SegmentedServing<Store>;

#[derive(Clone)]
struct AppState {
    client: Client<Engine0>,
    token: Option<Arc<str>>,
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
        let result = client
            .query(body.vector, body.k, body.filter.into_iter().collect())?
            .wait()?
            .value;
        Ok(Json(json!({
            "sequence": result.sequence,
            "results": result
                .neighbors
                .iter()
                .map(|neighbor| json!({ "id": neighbor.id, "distance": neighbor.distance }))
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
        let sequence = client.observe(0)?.wait()?.value.revision.boundary;
        let queue = client.status();
        Ok(Json(json!({
            "sequence": sequence,
            "queued_commands": queue.commands,
            "queued_bytes": queue.bytes,
            "closed": queue.closed,
            "failed": queue.failed,
            "maintenance_errors": queue.maintenance_errors,
        })))
    })
    .await
}

async fn health(State(state): State<AppState>) -> StatusCode {
    let queue = state.client.status();
    if queue.failed || queue.closed {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

/// Routes for one collection served by `client`.
pub fn router(client: Client<Engine0>, token: Option<String>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/status", get(status))
        .route("/v1/write", post(write))
        .route("/v1/query", post(query))
        .route("/v1/points/{id}", get(point))
        .route("/v1/requests/{boundary}/{nonce}", get(request))
        .with_state(AppState {
            client,
            token: token.map(Arc::from),
        })
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
