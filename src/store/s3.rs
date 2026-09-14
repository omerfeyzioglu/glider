//! Synchronous S3-compatible storage. Use from blocking threads, including
//! `spawn_blocking` when called by an async application.
use super::{decode_envelope, encode_envelope, ObjectStore};
use crate::{Error, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
pub use object_store::aws::AmazonS3Builder;
use object_store::{
    aws::{AmazonS3, S3ConditionalPut},
    client::{
        HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService,
        ReqwestConnector,
    },
    path::Path,
    ClientOptions, ObjectStore as RemoteStore, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
};
use std::{
    future::Future,
    sync::{Arc, Mutex},
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
    pub other: u64,
    pub request_body_bytes: u64,
    /// HTTP client call failures; excludes later response-body consumption errors.
    pub transport_errors: u64,
    pub http_errors: u64,
}
/// Clone before giving the store to Database to observe request deltas afterward.
#[derive(Debug, Default, Clone)]
pub struct RequestMetrics(Arc<Mutex<RequestCounts>>);
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
    poisoned: bool,
}
impl S3Store {
    /// Configure endpoint/region/credentials with the builder. This method forces
    /// conditional writes, disables automatic retries and installs request metrics.
    /// It performs no requests; Database::open validates the namespace remotely.
    pub fn open(builder: AmazonS3Builder, namespace: &str) -> Result<Self> {
        Self::with_connector(builder, namespace, ReqwestConnector::default())
    }
    fn with_connector<C: HttpConnector>(
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
        let runtime = Builder::new_current_thread().enable_all().build()?;
        Ok(Self {
            remote,
            namespace: Path::from(namespace),
            runtime: Some(runtime),
            metrics,
            poisoned: false,
        })
    }
    pub fn metrics(&self) -> RequestMetrics {
        self.metrics.clone()
    }
    fn ready(&self) -> Result<()> {
        if self.poisoned {
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

impl ObjectStore for S3Store {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.ready()?;
        let path = self.path(key)?;
        let bytes = self.run(async {
            match self.remote.get(&path).await {
                Ok(result) => result.bytes().await.map(Some),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            }
        })?;
        bytes.map(|b| decode_envelope(&b, key)).transpose()
    }
    fn list(&self) -> Result<Vec<String>> {
        self.ready()?;
        // Exhaust the SDK's paginated stream before exposing any keys to recovery.
        let objects: Vec<_> = self.run(self.remote.list(Some(&self.namespace)).try_collect())?;
        let prefix = format!("{}/", self.namespace);
        let mut keys = Vec::with_capacity(objects.len());
        for object in objects {
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
    }
    fn create(&mut self, key: &str, value: &[u8]) -> Result<()> {
        self.ready()?;
        let path = self.path(key)?;
        let bytes = encode_envelope(value);
        self.poisoned = true;
        self.run(self.remote.put_opts(
            &path,
            bytes.into(),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        ))?;
        self.poisoned = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
