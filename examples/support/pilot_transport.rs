//! Probe-only HTTP budget, shared by every namespace and recovery handle.
use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::{
    client::{
        HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpResponse,
        HttpResponseBody, HttpService, ReqwestConnector,
    },
    ClientOptions,
};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Default, Clone, Serialize)]
pub struct Counts {
    pub requests: u64,
    pub request_body_bytes: u64,
    pub response_body_bytes: u64,
    pub http_errors: u64,
}
/// Client-side time through complete response consumption; no URLs or headers.
#[derive(Debug, Clone, Serialize)]
pub struct HttpTiming {
    operation: &'static str,
    object_kind: &'static str,
    started_ms: f64,
    headers_ms: Option<f64>,
    total_ms: f64,
    status: Option<u16>,
    outcome: &'static str,
    request_bytes: u64,
    response_bytes: u64,
}
fn classify(method: &str, path: &str, query: Option<&str>) -> (&'static str, &'static str) {
    if method == "GET" && query.is_some_and(|q| q.split('&').any(|s| s == "list-type=2")) {
        return ("LIST", "inventory");
    }
    let operation = match method {
        "GET" => "GET",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        _ => "OTHER",
    };
    let key = path.rsplit('/').next().unwrap_or("");
    let kind = if key.starts_with("mutation-") {
        "mutation"
    } else if key.starts_with("compacted-") || key.starts_with("segment-") {
        "snapshot"
    } else if key.starts_with("owner-") {
        "ownership"
    } else if key == "metadata" {
        "metadata"
    } else {
        "other"
    };
    (operation, kind)
}
struct TimedRequest {
    budget: Budget,
    started: Instant,
    sample: HttpTiming,
}
impl Drop for TimedRequest {
    fn drop(&mut self) {
        self.sample.total_ms = self.started.elapsed().as_secs_f64() * 1000.;
        self.budget
            .0
            .lock()
            .unwrap()
            .timings
            .push(self.sample.clone());
    }
}
#[derive(Debug)]
struct State {
    counts: Counts,
    timings: Vec<HttpTiming>,
    started: Instant,
    requests_limit: u64,
    payload_limit: u64,
    seconds_limit: u64,
    lose_mutation_response: bool,
}
#[derive(Debug, Clone)]
pub struct Budget(Arc<Mutex<State>>);
impl Budget {
    pub fn new(requests_limit: u64, payload_limit: u64, seconds_limit: u64) -> Self {
        Self(Arc::new(Mutex::new(State {
            counts: Counts::default(),
            timings: Vec::new(),
            started: Instant::now(),
            requests_limit,
            payload_limit,
            seconds_limit,
            lose_mutation_response: false,
        })))
    }
    pub fn snapshot(&self) -> Counts {
        self.0.lock().unwrap().counts.clone()
    }
    pub fn timings(&self) -> Vec<HttpTiming> {
        self.0.lock().unwrap().timings.clone()
    }
    fn time_request(&self, request: &HttpRequest) -> TimedRequest {
        let (operation, object_kind) = classify(
            request.method().as_str(),
            request.uri().path(),
            request.uri().query(),
        );
        TimedRequest {
            budget: self.clone(),
            started: Instant::now(),
            sample: HttpTiming {
                operation,
                object_kind,
                started_ms: self.0.lock().unwrap().started.elapsed().as_secs_f64() * 1000.,
                headers_ms: None,
                total_ms: 0.,
                status: None,
                outcome: "transport_or_body_error",
                request_bytes: request.body().content_length() as u64,
                response_bytes: 0,
            },
        }
    }
    pub fn lose_next_mutation_response(&self) {
        self.0.lock().unwrap().lose_mutation_response = true;
    }
    fn admit(&self, bytes: u64, mutation: bool) -> Result<bool, HttpError> {
        let mut s = self.0.lock().unwrap();
        if s.started.elapsed() >= Duration::from_secs(s.seconds_limit)
            || s.counts.requests >= s.requests_limit
            || s.counts.request_body_bytes + s.counts.response_body_bytes + bytes > s.payload_limit
        {
            return Err(failure("pilot request/payload/time budget exhausted"));
        }
        s.counts.requests += 1;
        s.counts.request_body_bytes += bytes;
        let lose = mutation && s.lose_mutation_response;
        if lose {
            s.lose_mutation_response = false;
        }
        Ok(lose)
    }
    fn received(&self, bytes: u64) -> Result<(), HttpError> {
        let mut s = self.0.lock().unwrap();
        s.counts.response_body_bytes += bytes;
        if s.counts.request_body_bytes + s.counts.response_body_bytes > s.payload_limit {
            return Err(failure("pilot response payload budget exhausted"));
        }
        Ok(())
    }
}
fn failure(message: &str) -> HttpError {
    HttpError::new(HttpErrorKind::Request, std::io::Error::other(message))
}
#[derive(Debug)]
struct Service {
    inner: HttpClient,
    budget: Budget,
}
impl HttpConnector for Budget {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        Ok(HttpClient::new(Service {
            inner: ReqwestConnector::default().connect(options)?,
            budget: self.clone(),
        }))
    }
}
#[async_trait]
impl HttpService for Service {
    async fn call(&self, request: HttpRequest) -> Result<HttpResponse, HttpError> {
        let mutation = request.method() == "PUT" && request.uri().path().contains("/mutation-");
        let lose = self
            .budget
            .admit(request.body().content_length() as u64, mutation)?;
        let mut timing = self.budget.time_request(&request);
        let response = self.inner.execute(request).await?;
        timing.sample.headers_ms = Some(timing.started.elapsed().as_secs_f64() * 1000.);
        let (parts, body) = response.into_parts();
        timing.sample.status = Some(parts.status.as_u16());
        if !parts.status.is_success() {
            self.budget.0.lock().unwrap().counts.http_errors += 1;
        }
        let mut stream = body.bytes_stream();
        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            timing.sample.response_bytes += chunk.len() as u64;
            self.budget.received(chunk.len() as u64)?;
            payload.extend_from_slice(&chunk);
        }
        if lose && parts.status.is_success() {
            timing.sample.outcome = "injected_response_loss";
            return Err(failure("injected lost mutation acknowledgement"));
        }
        timing.sample.outcome = if parts.status.is_success() {
            "success"
        } else {
            "http_error"
        };
        Ok(HttpResponse::from_parts(
            parts,
            HttpResponseBody::from(payload),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn timing_classification_retains_no_request_identity() {
        assert_eq!(
            classify("GET", "/private-bucket", Some("prefix=secret&list-type=2")),
            ("LIST", "inventory")
        );
        assert_eq!(
            classify("PUT", "/secret/mutation-123", None),
            ("PUT", "mutation")
        );
        assert_eq!(
            classify("GET", "/secret/compacted-123", None),
            ("GET", "snapshot")
        );
        assert_eq!(
            classify("DELETE", "/secret/owner-v1-token", None),
            ("DELETE", "ownership")
        );
    }
    #[derive(Debug)]
    struct ResponseProbe(bool);
    #[async_trait]
    impl HttpService for ResponseProbe {
        async fn call(&self, _: HttpRequest) -> Result<HttpResponse, HttpError> {
            if self.0 {
                return Err(failure("injected connection failure"));
            }
            Ok(HttpResponse::new(HttpResponseBody::from(vec![0; 4])))
        }
    }
    #[test]
    fn failed_http_attempts_preserve_headers_and_received_bytes() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        for fails_before_headers in [false, true] {
            let budget = Budget::new(1, 3, 60);
            let service = Service {
                inner: HttpClient::new(ResponseProbe(fails_before_headers)),
                budget: budget.clone(),
            };
            let request = HttpRequest::new(object_store::client::HttpRequestBody::empty());
            // Either connection failure or the real streamed body budget check fails.
            assert!(runtime.block_on(service.call(request)).is_err());
            let samples = budget.timings();
            assert_eq!(samples.len(), 1);
            let sample = &samples[0];
            assert_eq!(sample.outcome, "transport_or_body_error");
            assert_eq!(sample.headers_ms.is_none(), fails_before_headers);
            assert_eq!(
                sample.status,
                if fails_before_headers {
                    None
                } else {
                    Some(200)
                }
            );
            assert_eq!(
                sample.response_bytes,
                if fails_before_headers { 0 } else { 4 }
            );
            assert_eq!(sample.response_bytes, budget.snapshot().response_body_bytes);
            assert_eq!(budget.snapshot().requests, 1);
        }
    }
    #[test]
    fn all_clones_share_request_limits() {
        let a = Budget::new(3, 100, 60);
        let b = a.clone();
        for _ in 0..3 {
            a.admit(0, false).unwrap();
        }
        assert!(b.admit(0, false).is_err());
        assert_eq!(a.snapshot().requests, 3);
    }
    #[test]
    fn payload_and_time_stop_new_requests() {
        let b = Budget::new(10_000, 90 * 1024 * 1024, 530);
        assert!(b.admit(91 * 1024 * 1024, false).is_err());
        assert_eq!(b.snapshot().requests, 0);
        b.received(90 * 1024 * 1024).unwrap();
        assert!(b.received(1).is_err());
        assert!(b.admit(0, false).is_err());
        let b = Budget::new(10_000, 90 * 1024 * 1024, 530);
        b.0.lock().unwrap().started = Instant::now() - Duration::from_secs(600);
        assert!(b.admit(0, false).is_err());
    }
    #[test]
    fn only_one_mutation_response_is_lost() {
        let b = Budget::new(10_000, 90 * 1024 * 1024, 530);
        b.lose_next_mutation_response();
        assert!(!b.admit(0, false).unwrap());
        assert!(b.admit(0, true).unwrap());
        assert!(!b.admit(0, true).unwrap());
    }
}
