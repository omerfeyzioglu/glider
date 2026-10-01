//! Synchronous S3-compatible storage. Use from blocking threads, including
//! `spawn_blocking` when called by an async application.
use super::{decode_envelope, encode_envelope, uncertain, ObjectStore};
use crate::{Error, Result};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
pub use object_store::aws::AmazonS3Builder;
use object_store::{
    aws::{AmazonS3, S3ConditionalPut},
    client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
        ReqwestConnector,
    },
    path::Path,
    ClientOptions, GetOptions, ObjectStore as RemoteStore, ObjectStoreExt, PutMode, PutOptions,
    RetryConfig,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::runtime::{Builder, Runtime};

/// HTTP attempts, including failed requests and every listing page. Bytes are
/// attempted request bodies (including envelopes), not physical storage traffic.
/// Credential-provider requests are included if they use the same connector.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RequestCounts {
    pub get: u64,
    pub list: u64,
    pub put: u64,
    pub delete: u64,
    pub other: u64,
    pub request_body_bytes: u64,
    /// HTTP client call failures; excludes later response-body consumption errors.
    pub transport_errors: u64,
    pub http_errors: u64,
}
/// Clone before giving the store to Database to observe request deltas afterward.
#[derive(Debug, Default, Clone)]
pub struct RequestMetrics(Arc<Mutex<RequestCounts>>);

/// Opt-in bounds on visible namespace inventory and each downloaded envelope.
/// Includes control/obsolete objects and envelope bytes, not decoded engine RAM.
#[derive(Debug, Clone, Copy)]
pub struct ReadLimits {
    pub objects: usize,
    pub object_bytes: usize,
    pub namespace_bytes: u64,
}
impl ReadLimits {
    fn validate(self) -> Result<()> {
        if self.objects == 0 || self.object_bytes == 0 || self.namespace_bytes == 0 {
            return Err(Error::Invalid("S3 read limits must be positive".into()));
        }
        Ok(())
    }
}
fn read_limit(what: &str) -> Error {
    Error::Invalid(format!("S3 read limit exceeded: {what}"))
}
impl RequestMetrics {
    pub fn snapshot(&self) -> RequestCounts {
        *self.0.lock().unwrap()
    }
}
#[derive(Debug)]
struct MeteredConnector<C> {
    inner: C,
    metrics: RequestMetrics,
}
impl<C: HttpConnector> HttpConnector for MeteredConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(MeteredService {
            inner: self.inner.connect(options)?,
            metrics: self.metrics.clone(),
        }))
    }
}
#[derive(Debug)]
struct MeteredService {
    inner: HttpClient,
    metrics: RequestMetrics,
}
#[async_trait]
impl HttpService for MeteredService {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        {
            let mut counts = self.metrics.0.lock().unwrap();
            match request.method().as_str() {
                "PUT" => counts.put += 1,
                "DELETE" => counts.delete += 1,
                "GET"
                    if request
                        .uri()
                        .query()
                        .is_some_and(|q| q.split('&').any(|p| p == "list-type=2")) =>
                {
                    counts.list += 1
                }
                "GET" => counts.get += 1,
                _ => counts.other += 1,
            }
            counts.request_body_bytes += request.body().content_length() as u64;
        }
        let response = self.inner.execute(request).await;
        let mut counts = self.metrics.0.lock().unwrap();
        match &response {
            Err(_) => counts.transport_errors += 1,
            Ok(r) if !r.status().is_success() => counts.http_errors += 1,
            _ => {}
        }
        response
    }
}

/// One exclusive owner per bucket/namespace. Bucket creation and credentials are
/// caller-managed. The server must support strongly consistent complete listings
/// and atomic `If-None-Match: *` PUT; there is no unconditional-write fallback.
/// No filesystem publication or recovery operations are used here.
///
/// Object operations must run on a blocking thread. Like Tokio `block_on`,
/// calling them directly inside an async task panics; use `spawn_blocking`.
pub struct S3Store {
    remote: AmazonS3,
    namespace: Path,
    runtime: Option<Runtime>,
    metrics: RequestMetrics,
    poisoned: AtomicBool,
    read_limits: Option<ReadLimits>,
}
impl S3Store {
    /// Configure endpoint/region/credentials with the builder. This method forces
    /// conditional writes, disables automatic retries and installs request metrics.
    /// It performs no requests; Database::open validates the namespace remotely.
    pub fn open(builder: AmazonS3Builder, namespace: &str) -> Result<Self> {
        Self::with_connector(builder, namespace, ReqwestConnector::default())
    }
    /// Supply a transport for bounded probes or fault injection. Conditional
    /// publication, disabled retries and request metrics are still enforced.
    pub fn with_connector<C: HttpConnector>(
        builder: AmazonS3Builder,
        namespace: &str,
        connector: C,
    ) -> Result<Self> {
        if namespace.split('/').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        }) {
            return Err(Error::Invalid(
                "namespace must contain nonempty alphanumeric, '-' or '_' path components".into(),
            ));
        }
        let metrics = RequestMetrics::default();
        let remote = builder
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_disable_bulk_delete(true)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .with_http_connector(MeteredConnector {
                inner: connector,
                metrics: metrics.clone(),
            })
            .build()
            .map_err(remote_error)?;
        // Keep HTTP connection tasks alive while the synchronous caller is
        // idle, so peer closure and pool expiration are processed promptly.
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        Ok(Self {
            remote,
            namespace: Path::from(namespace),
            runtime: Some(runtime),
            metrics,
            poisoned: AtomicBool::new(false),
            read_limits: None,
        })
    }
    /// Set before opening the engine. Listing rejects an oversized namespace
    /// without returning a partial inventory; GET checks headers and body chunks.
    /// Failed owned opens can leave a claim, as with other recovery failures.
    pub fn with_read_limits(mut self, limits: ReadLimits) -> Result<Self> {
        limits.validate()?;
        self.read_limits = Some(limits);
        Ok(self)
    }
    pub fn metrics(&self) -> RequestMetrics {
        self.metrics.clone()
    }
    fn ready(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
    fn path(&self, key: &str) -> Result<Path> {
        if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
            return Err(Error::Invalid("invalid object key".into()));
        }
        Ok(self.namespace.clone().join(key))
    }
    fn run<T>(&self, future: impl Future<Output = object_store::Result<T>>) -> Result<T> {
        self.runtime
            .as_ref()
            .unwrap()
            .block_on(future)
            .map_err(remote_error)
    }
}
impl Drop for S3Store {
    fn drop(&mut self) {
        // Safe even if a caller moves an idle handle into an async task to drop it.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
fn remote_error(error: object_store::Error) -> Error {
    // The SDK also labels HTTP 409 as AlreadyExists. Only a wrapped conditional
    // rejection proves existence; a conflict remains an uncertain I/O error.
    if let object_store::Error::AlreadyExists { path, source } = &error {
        if matches!(
            source.downcast_ref::<object_store::Error>(),
            Some(
                object_store::Error::Precondition { .. } | object_store::Error::NotModified { .. }
            )
        ) {
            return Error::Exists(path.clone());
        }
    }
    Error::Io(std::io::Error::other(error))
}

impl S3Store {
    /// Concurrent reads issued by one batched call.
    const READ_CONCURRENCY: usize = 16;

    async fn get_async(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.ready()?;
        let path = self.path(key)?;
        {
            let result = match self.remote.get(&path).await {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(remote_error(error)),
            };
            if let Some(limits) = self.read_limits {
                if result.meta.size > limits.object_bytes as u64 {
                    return Err(read_limit("object header bytes"));
                }
                let mut stream = result.into_stream();
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.try_next().await.map_err(remote_error)? {
                    if chunk.len() > limits.object_bytes.saturating_sub(bytes.len()) {
                        return Err(read_limit("object body bytes"));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                decode_envelope(&bytes, key).map(Some)
            } else {
                let bytes = result.bytes().await.map_err(remote_error)?;
                decode_envelope(&bytes, key).map(Some)
            }
        }
    }

    async fn get_range_async(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        expected_payload_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.ready()?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::Invalid("range overflow".into()))?;
        if length == 0 || end > expected_payload_len {
            return Err(Error::Invalid("range outside expected payload".into()));
        }
        let envelope_len = expected_payload_len
            .checked_add(48)
            .ok_or_else(|| Error::Invalid("envelope length overflow".into()))?;
        if self
            .read_limits
            .is_some_and(|limits| envelope_len > limits.object_bytes)
        {
            return Err(read_limit("object header bytes"));
        }
        let start = u64::try_from(offset)
            .ok()
            .and_then(|n| n.checked_add(16))
            .ok_or_else(|| Error::Invalid("range offset overflow".into()))?;
        let end = u64::try_from(end)
            .ok()
            .and_then(|n| n.checked_add(16))
            .ok_or_else(|| Error::Invalid("range end overflow".into()))?;
        let path = self.path(key)?;
        {
            let result = match self
                .remote
                .get_opts(&path, GetOptions::new().with_range(Some(start..end)))
                .await
            {
                Ok(result) => result,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(error) => return Err(remote_error(error)),
            };
            if result.meta.size != envelope_len as u64 {
                return Err(Error::Corrupt(format!("object length mismatch: {key}")));
            }
            let mut stream = result.into_stream();
            let mut bytes = Vec::with_capacity(length);
            while let Some(chunk) = stream.try_next().await.map_err(remote_error)? {
                if chunk.len() > length.saturating_sub(bytes.len()) {
                    return Err(Error::Corrupt(format!("oversized range response: {key}")));
                }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() != length {
                return Err(Error::Corrupt(format!("short range response: {key}")));
            }
            Ok(Some(bytes))
        }
    }
}

impl ObjectStore for S3Store {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.runtime.as_ref().unwrap().block_on(self.get_async(key))
    }
    fn get_range(
        &self,
        key: &str,
        offset: usize,
        length: usize,
        expected_payload_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.runtime
            .as_ref()
            .unwrap()
            .block_on(self.get_range_async(key, offset, length, expected_payload_len))
    }
    fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        self.runtime.as_ref().unwrap().block_on(
            futures::stream::iter(keys.iter().map(|key| self.get_async(key)))
                .buffered(Self::READ_CONCURRENCY)
                .try_collect(),
        )
    }
    fn get_ranges(&self, ranges: &[(&str, usize, usize, usize)]) -> Result<Vec<Option<Vec<u8>>>> {
        self.runtime.as_ref().unwrap().block_on(
            futures::stream::iter(ranges.iter().map(|&(key, offset, length, payload)| {
                self.get_range_async(key, offset, length, payload)
            }))
            .buffered(Self::READ_CONCURRENCY)
            .try_collect(),
        )
    }
    fn list(&self) -> Result<Vec<String>> {
        self.ready()?;
        let prefix = format!("{}/", self.namespace);
        // Consume pages incrementally. Never expose a partial listing, including
        // when a limit or a later page fails. SDK page/transport buffers remain.
        self.runtime.as_ref().unwrap().block_on(async {
            let mut objects = self.remote.list(Some(&self.namespace));
            let mut keys = Vec::new();
            let mut total_bytes = 0_u64;
            while let Some(object) = objects.try_next().await.map_err(remote_error)? {
                if let Some(limits) = self.read_limits {
                    if keys.len() >= limits.objects {
                        return Err(read_limit("object count"));
                    }
                    if object.size > limits.object_bytes as u64 {
                        return Err(read_limit("listed object bytes"));
                    }
                    total_bytes = total_bytes
                        .checked_add(object.size)
                        .ok_or_else(|| read_limit("namespace byte overflow"))?;
                    if total_bytes > limits.namespace_bytes {
                        return Err(read_limit("namespace bytes"));
                    }
                }
                let key = object
                    .location
                    .as_ref()
                    .strip_prefix(&prefix)
                    .ok_or_else(|| Error::Corrupt("S3 listing escaped namespace".into()))?;
                self.path(key)
                    .map_err(|_| Error::Corrupt(format!("unexpected S3 key: {key}")))?;
                keys.push(key.to_owned());
            }
            Ok(keys)
        })
    }
    fn remove(&self, key: &str) -> Result<()> {
        self.ready()?;
        let path = self.path(key)?;
        uncertain(&self.poisoned, || {
            self.run(async {
                match self.remote.delete(&path).await {
                    Err(object_store::Error::NotFound { .. }) => Ok(()),
                    other => other,
                }
            })
        })
    }
    fn remove_many(&self, keys: &[String]) -> Result<()> {
        self.ready()?;
        let paths: Vec<_> = keys
            .iter()
            .map(|key| self.path(key))
            .collect::<Result<_>>()?;
        if paths.is_empty() {
            return Ok(());
        }
        uncertain(&self.poisoned, || {
            // Keep native single-key DELETE semantics and no automatic retries.
            // Finish the bounded batch even on error; every key is already
            // obsolete. A response loss can still leave a late delete, safe by
            // non-reuse.
            let remote = &self.remote;
            let results = self.runtime.as_ref().unwrap().block_on(async {
                futures::stream::iter(paths)
                    .map(|path| async move {
                        match remote.delete(&path).await {
                            Err(object_store::Error::NotFound { .. }) => Ok(()),
                            other => other,
                        }
                    })
                    .buffer_unordered(4)
                    .collect::<Vec<_>>()
                    .await
            });
            for result in results {
                result.map_err(remote_error)?;
            }
            Ok(())
        })
    }
    fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        self.ready()?;
        let path = self.path(key)?;
        let bytes = encode_envelope(value);
        uncertain(&self.poisoned, || {
            self.run(self.remote.put_opts(
                &path,
                bytes.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            ))?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests;
