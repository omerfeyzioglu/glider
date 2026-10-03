use super::{
    error::{bad_request, json_errors, ApiError},
    metrics::{cache_status, clustering_status, record_metrics, render_metrics, HttpMetrics},
    Engine0,
};
use crate::{
    admission::Client,
    retry::{Lookup, Outcome, Request, RequestId},
    segmented::QueryOptions,
    Mutation,
};
use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
struct AppState {
    client: Client<Engine0>,
    token: Option<Arc<str>>,
    metrics: Arc<HttpMetrics>,
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

fn outcome_json(outcome: Outcome, id: RequestId) -> Result<Value, ApiError> {
    if outcome.conflict.is_some() {
        return Err(ApiError(
            StatusCode::INTERNAL_SERVER_ERROR,
            "conditional write outcome cannot be represented over HTTP".into(),
        ));
    }
    Ok(json!({
        "sequence": outcome.sequence,
        "request_id": RequestIdJson::from_id(id),
    }))
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
        Ok(Json(outcome_json(outcome, id)?))
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
    #[serde(default)]
    exact: bool,
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
            .query_with_mode(
                body.vector,
                body.k,
                body.filter.into_iter().collect(),
                options,
                body.exact,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchGetBody {
    ids: Vec<u64>,
    #[serde(default = "default_true")]
    include_vector: bool,
    #[serde(default = "default_true")]
    include_metadata: bool,
}

fn default_true() -> bool {
    true
}

async fn batch_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<BatchGetBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if body.ids.is_empty() || body.ids.len() > 1_000 {
        return Err(bad_request("ids must contain 1 to 1000 entries"));
    }
    let client = state.client.clone();
    blocking(move || {
        let ids = body.ids;
        let result = client.batch_get(ids.clone())?.wait()?.value;
        let mut points = Vec::new();
        let mut missing = Vec::new();
        for (id, document) in ids.into_iter().zip(result.points) {
            if let Some(document) = document {
                let mut point = json!({ "id": id });
                if body.include_vector {
                    point["vector"] = json!(document.vector);
                }
                if body.include_metadata {
                    point["metadata"] = json!(document.metadata);
                }
                points.push(point);
            } else {
                missing.push(id);
            }
        }
        Ok(Json(
            json!({ "points": points, "missing": missing, "sequence": result.sequence }),
        ))
    })
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScanBody {
    #[serde(default)]
    filter: BTreeMap<String, String>,
    after: Option<u64>,
    #[serde(default = "default_scan_limit")]
    limit: usize,
    #[serde(default)]
    include_metadata: bool,
}

fn default_scan_limit() -> usize {
    1_000
}

async fn scan(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<ScanBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if body.limit == 0 || body.limit > 10_000 {
        return Err(bad_request("limit must be between 1 and 10000"));
    }
    let client = state.client.clone();
    blocking(move || {
        let result = client
            .scan(
                body.filter.into_iter().collect(),
                body.after,
                body.limit,
                body.include_metadata,
            )?
            .wait()?
            .value;
        let mut response =
            json!({ "next": result.next, "matched": result.matched, "sequence": result.sequence });
        if body.include_metadata {
            response["points"] = json!(result
                .points
                .into_iter()
                .map(|(id, metadata)| json!({ "id": id, "metadata": metadata.unwrap_or_default() }))
                .collect::<Vec<_>>());
        } else {
            response["ids"] = json!(result.points.into_keys().collect::<Vec<_>>());
        }
        Ok(Json(response))
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
                json!({ "state": "retained", "outcome": outcome_json(outcome, id)? })
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
            "clustering": clustering_status(&engine),
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
        .route("/v1/points/get", post(batch_get))
        .route("/v1/scan", post(scan))
        .route("/v1/points/{id}", get(point))
        .route("/v1/requests/{boundary}/{nonce}", get(request))
        .with_state(AppState {
            client,
            token: token.map(Arc::from),
            metrics: http_metrics.clone(),
        })
        .layer(middleware::from_fn(json_errors))
        .layer(middleware::from_fn_with_state(http_metrics, record_metrics))
}
