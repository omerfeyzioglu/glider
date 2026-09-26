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
#[derive(Debug)]
struct State {
    counts: Counts,
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
        let response = self.inner.execute(request).await?;
        let (parts, body) = response.into_parts();
        if !parts.status.is_success() {
            self.budget.0.lock().unwrap().counts.http_errors += 1;
        }
        let mut stream = body.bytes_stream();
        let mut payload = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            self.budget.received(chunk.len() as u64)?;
            payload.extend_from_slice(&chunk);
        }
        if lose && parts.status.is_success() {
            return Err(failure("injected lost mutation acknowledgement"));
        }
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
