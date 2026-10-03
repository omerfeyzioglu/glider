use super::{
    catalog::CreateCollection,
    error::{bad_request, json_errors, ApiError},
    metrics::{
        cache_status, clustering_status, record_metrics, render_http_metrics, render_metrics,
        HttpMetrics,
    },
    Engine0, Multi,
};
use crate::{
    admission::Client,
    retry::{Lookup, Outcome, Request, RequestId},
    segmented::QueryOptions,
    Filter, Mutation,
};
use axum::{
    extract::{Extension, FromRef, OriginalUri, Path, Request as HttpRequest, State},
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
    client: Option<Client<Engine0>>,
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
    Extension(client): Extension<Client<Engine0>>,
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
    let client = client.clone();
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
    #[serde(default = "empty_filter")]
    filter: Value,
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

fn empty_filter() -> Value {
    json!({})
}

/// `POST /v1/query`: unfiltered approximate search within the read budget,
/// or exact search of the declared resident filter.
async fn query(
    State(state): State<AppState>,
    Extension(client): Extension<Client<Engine0>>,
    headers: HeaderMap,
    Json(body): Json<QueryBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if body.k == 0 || body.k > 1_000 {
        return Err(bad_request("k must be between 1 and 1000"));
    }
    let filter = Filter::parse(&body.filter).map_err(bad_request)?;
    let client = client.clone();
    blocking(move || {
        let options = QueryOptions {
            include_metadata: body.include_metadata,
            include_vector: body.include_vector,
        };
        let result = client
            .query_with_mode_filter(body.vector, body.k, filter, options, body.exact)?
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
    Extension(client): Extension<Client<Engine0>>,
    headers: HeaderMap,
    Json(body): Json<BatchGetBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if body.ids.is_empty() || body.ids.len() > 1_000 {
        return Err(bad_request("ids must contain 1 to 1000 entries"));
    }
    let client = client.clone();
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
    #[serde(default = "empty_filter")]
    filter: Value,
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
    Extension(client): Extension<Client<Engine0>>,
    headers: HeaderMap,
    Json(body): Json<ScanBody>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    if body.limit == 0 || body.limit > 10_000 {
        return Err(bad_request("limit must be between 1 and 10000"));
    }
    let filter = Filter::parse(&body.filter).map_err(bad_request)?;
    let client = client.clone();
    blocking(move || {
        let result = client
            .scan_filter(filter, body.after, body.limit, body.include_metadata)?
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
    Extension(client): Extension<Client<Engine0>>,
    headers: HeaderMap,
    Path(path): Path<BTreeMap<String, String>>,
) -> Result<Response, ApiError> {
    authorize(&state, &headers)?;
    let id = path
        .get("id")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| bad_request("invalid point id"))?;
    let client = client.clone();
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
    Extension(client): Extension<Client<Engine0>>,
    headers: HeaderMap,
    Path(path): Path<BTreeMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let boundary = path
        .get("boundary")
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| bad_request("invalid request boundary"))?;
    let nonce = path
        .get("nonce")
        .cloned()
        .ok_or_else(|| bad_request("invalid request nonce"))?;
    let id = RequestIdJson { boundary, nonce }.to_id()?;
    let client = client.clone();
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
    Extension(client): Extension<Client<Engine0>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize(&state, &headers)?;
    let client = client.clone();
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
    let Some(client) = &state.client else {
        return StatusCode::OK;
    };
    let queue = client.status();
    if queue.failed || queue.closed {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

/// `GET /metrics`: unauthenticated Prometheus text exposition.
async fn metrics(State(state): State<AppState>) -> Result<Response, ApiError> {
    let Some(client) = state.client.clone() else {
        return Ok((
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            "# glider multi-collection mode\n",
        )
            .into_response());
    };
    let http = state.metrics.clone();
    blocking(move || {
        let engine = client.metrics()?.wait()?.value;
        let queue = client.status();
        let body = render_metrics(&http, queue, engine);
        Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response())
    })
    .await
}

fn data_routes() -> Router<AppState> {
    Router::new()
        .route("/status", get(status))
        .route("/write", post(write))
        .route("/query", post(query))
        .route("/points/get", post(batch_get))
        .route("/scan", post(scan))
        .route("/points/{id}", get(point))
        .route("/requests/{boundary}/{nonce}", get(request))
}

async fn single_client(
    State(state): State<AppState>,
    mut request: HttpRequest,
    next: middleware::Next,
) -> Response {
    request
        .extensions_mut()
        .insert(state.client.as_ref().unwrap().clone());
    next.run(request).await
}

/// Routes for one collection served by `client`.
pub fn router(client: Client<Engine0>, token: Option<String>) -> Router {
    let http_metrics = Arc::new(HttpMetrics::new());
    let state = AppState {
        client: Some(client),
        token: token.map(Arc::from),
        metrics: http_metrics.clone(),
    };
    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(metrics))
        .nest(
            "/v1",
            data_routes().layer(middleware::from_fn_with_state(state.clone(), single_client)),
        )
        .with_state(state)
        .layer(middleware::from_fn(json_errors))
        .layer(middleware::from_fn_with_state(http_metrics, record_metrics))
}

fn multi_error(error: crate::Error) -> ApiError {
    use crate::Error;
    // Busy covers both a collection whose lease another process holds and
    // every open collection being in use; the inner message says which, and
    // both are worth retrying.
    if let Error::Busy(message) = error {
        return ApiError(StatusCode::TOO_MANY_REQUESTS, message);
    }
    let code = match error {
        Error::Invalid(_) => StatusCode::BAD_REQUEST,
        Error::RequestConflict | Error::Exists(_) => StatusCode::CONFLICT,
        Error::Corrupt(_) => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    ApiError(code, error.to_string())
}

#[derive(Clone)]
struct MultiState {
    manager: Multi,
    token: Option<Arc<str>>,
    metrics: Arc<HttpMetrics>,
}
impl FromRef<MultiState> for AppState {
    fn from_ref(state: &MultiState) -> Self {
        Self {
            client: None,
            token: state.token.clone(),
            metrics: state.metrics.clone(),
        }
    }
}
fn authorize_multi(state: &MultiState, headers: &HeaderMap) -> Result<(), ApiError> {
    authorize(
        &AppState {
            client: None,
            token: state.token.clone(),
            metrics: state.metrics.clone(),
        },
        headers,
    )
}
async fn create_collection(
    State(state): State<MultiState>,
    headers: HeaderMap,
    Json(body): Json<CreateCollection>,
) -> Result<Response, ApiError> {
    authorize_multi(&state, &headers)?;
    let (record, created) = state.manager.create(body).await.map_err(multi_error)?;
    Ok((
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(record.description(state.manager.is_open(&record.name).await)),
    )
        .into_response())
}
async fn list_collections(
    State(state): State<MultiState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authorize_multi(&state, &headers)?;
    let records = state.manager.list().await.map_err(multi_error)?;
    let mut descriptions = Vec::new();
    for record in records {
        descriptions.push(record.description(state.manager.is_open(&record.name).await));
    }
    Ok(Json(json!({"collections":descriptions})))
}
async fn get_collection(
    State(state): State<MultiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    authorize_multi(&state, &headers)?;
    let Some(record) = state.manager.get(&name).await.map_err(multi_error)? else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "collection not found".into(),
        ));
    };
    let Some(usage) = state
        .manager
        .use_collection(&name)
        .await
        .map_err(multi_error)?
    else {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            "collection not found".into(),
        ));
    };
    let client = usage.client();
    let status = blocking(move || {
        let engine = client.metrics()?.wait()?.value;
        let queue = client.status();
        Ok::<_, ApiError>(
            json!({"sequence":engine.sequence,"queued_commands":queue.commands,
            "queued_bytes":queue.bytes,"closed":queue.closed,"failed":queue.failed,
            "maintenance_errors":queue.maintenance_errors,"cache":cache_status(&engine),
            "clustering":clustering_status(&engine)}),
        )
    })
    .await?;
    let mut description = record.description(true);
    description
        .as_object_mut()
        .unwrap()
        .insert("status".into(), status);
    Ok(Json(description))
}
async fn delete_collection(
    State(state): State<MultiState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    authorize_multi(&state, &headers)?;
    if state.manager.delete(&name).await.map_err(multi_error)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError(
            StatusCode::NOT_FOUND,
            "collection not found".into(),
        ))
    }
}
async fn multi_client(
    State(state): State<MultiState>,
    OriginalUri(uri): OriginalUri,
    mut request: HttpRequest,
    next: middleware::Next,
) -> Response {
    if let Err(error) = authorize_multi(&state, request.headers()) {
        return error.into_response();
    }
    let Some(name) = uri.path().split('/').nth(3) else {
        return ApiError(StatusCode::BAD_REQUEST, "missing collection name".into()).into_response();
    };
    let usage = match state.manager.use_collection(name).await {
        Ok(Some(usage)) => usage,
        Ok(None) => {
            return ApiError(StatusCode::NOT_FOUND, "collection not found".into()).into_response()
        }
        Err(error) => return multi_error(error).into_response(),
    };
    request.extensions_mut().insert(usage.client());
    let result = next.run(request).await;
    drop(usage);
    result
}
async fn legacy_route() -> ApiError {
    ApiError(
        StatusCode::NOT_FOUND,
        "use /v1/collections/{name}/...".into(),
    )
}

async fn multi_metrics(State(state): State<MultiState>) -> Response {
    let mut body = render_http_metrics(&state.metrics);
    body.push_str(&format!(
        "# TYPE glider_open_collections gauge\nglider_open_collections {}\n",
        state.manager.open_count().await
    ));
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

pub fn multi_router(manager: Multi, token: Option<String>) -> Router {
    let http_metrics = Arc::new(HttpMetrics::new());
    let token = token.map(Arc::from);
    let state = AppState {
        client: None,
        token: token.clone(),
        metrics: http_metrics.clone(),
    };
    let multi_state = MultiState {
        manager,
        token,
        metrics: http_metrics.clone(),
    };
    let collection_routes =
        data_routes()
            .with_state(state.clone())
            .layer(middleware::from_fn_with_state(
                multi_state.clone(),
                multi_client,
            ));
    Router::new()
        .route("/healthz", get(health))
        .route("/metrics", get(multi_metrics))
        .route("/v1/write", post(legacy_route))
        .route("/v1/query", post(legacy_route))
        .route("/v1/points/get", post(legacy_route))
        .route("/v1/scan", post(legacy_route))
        .route("/v1/points/{id}", get(legacy_route))
        .route("/v1/requests/{boundary}/{nonce}", get(legacy_route))
        .route("/v1/status", get(legacy_route))
        .nest("/v1/collections/{name}", collection_routes)
        .route(
            "/v1/collections",
            post(create_collection).get(list_collections),
        )
        .route(
            "/v1/collections/{name}",
            get(get_collection).delete(delete_collection),
        )
        .with_state(multi_state)
        .layer(middleware::from_fn(json_errors))
        .layer(middleware::from_fn_with_state(http_metrics, record_metrics))
}
