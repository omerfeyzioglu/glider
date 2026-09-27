//! Bounded FIFO admission; one worker owns all reads, publication and maintenance.
use crate::{
    retry::{Lookup, Outcome, Request, RequestId, Revision},
    serving::{SearchMode, SingleMachine},
    store::ObjectStore,
    Config, Neighbor,
};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU8, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("admission count or encoded-byte limit reached")]
    Overloaded,
    #[error("admission is closed")]
    Closed,
    #[error("cancelled before execution")]
    Cancelled,
    #[error("worker failed; resolve uncertain requests through isolated recovery")]
    WorkerFailed,
    #[error(transparent)]
    Database(#[from] crate::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Includes the command currently executing.
    pub commands: usize,
    /// Sum of encoded payload charges, including active work.
    pub bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            commands: 8,
            bytes: 320 * 1024,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Status {
    pub commands: usize,
    pub bytes: usize,
    pub closed: bool,
    pub failed: bool,
}
#[derive(Debug)]
pub struct Timed<T> {
    pub value: T,
    pub queue_wait: Duration,
    /// Validation/deduplication/execution, excluding scheduled maintenance.
    pub execution: Duration,
    pub maintenance: Duration,
}
#[derive(Debug, Clone, Copy)]
pub struct Observation {
    pub revision: Revision,
    pub request_id: RequestId,
}
#[derive(Debug)]
pub struct QueryResult {
    pub sequence: u64,
    pub neighbors: Vec<Neighbor>,
}
#[derive(Debug, Clone, Copy)]
pub enum Shutdown {
    Drain,
    CancelQueued,
}

const QUEUED: u8 = 0;
const STARTED: u8 = 1;
const CANCELLED: u8 = 2;
const FINISHED: u8 = 3;

/// Dropping a queued ticket cancels it. Once started, work continues and its
/// durable outcome is resolved using its unchanged M15 request ID.
pub struct Ticket<T> {
    receiver: mpsc::Receiver<Result<Timed<T>>>,
    state: Arc<AtomicU8>,
}
impl<T> Ticket<T> {
    /// True proves execution will not start. False does not prove publication.
    pub fn cancel(&self) -> bool {
        self.state
            .compare_exchange(QUEUED, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
    pub fn wait(self) -> Result<Timed<T>> {
        self.receiver.recv().unwrap_or(Err(Error::WorkerFailed))
    }
}
impl<T> Drop for Ticket<T> {
    fn drop(&mut self) {
        self.cancel();
    }
}

type Delivery = Box<dyn FnOnce() + Send>;
trait Execute<S: ObjectStore>: Send {
    fn execute(self: Box<Self>, db: &mut SingleMachine<S>, queue_wait: Duration) -> Delivery;
    fn reject(self: Box<Self>, error: Error) -> Delivery;
}
struct Task<T, F> {
    reply: mpsc::SyncSender<Result<Timed<T>>>,
    operation: F,
}
impl<S, T, F> Execute<S> for Task<T, F>
where
    S: ObjectStore,
    T: Send + 'static,
    F: FnOnce(&mut SingleMachine<S>) -> crate::Result<T> + Send,
{
    fn execute(self: Box<Self>, db: &mut SingleMachine<S>, queue_wait: Duration) -> Delivery {
        let before = db.status().maintenance_time;
        let start = Instant::now();
        let result = (self.operation)(db);
        let elapsed = start.elapsed();
        let maintenance = db.status().maintenance_time - before;
        let result = result
            .map(|value| Timed {
                value,
                queue_wait,
                execution: elapsed.saturating_sub(maintenance),
                maintenance,
            })
            .map_err(Error::Database);
        let reply = self.reply;
        Box::new(move || {
            let _ = reply.send(result);
        })
    }
    fn reject(self: Box<Self>, error: Error) -> Delivery {
        let reply = self.reply;
        Box::new(move || {
            let _ = reply.send(Err(error));
        })
    }
}
struct Job<S: ObjectStore> {
    task: Box<dyn Execute<S>>,
    state: Arc<AtomicU8>,
    queued: Instant,
    bytes: usize,
}
struct Queue<S: ObjectStore> {
    jobs: VecDeque<Job<S>>,
    commands: usize,
    bytes: usize,
    closed: bool,
    failed: bool,
    cancel_queued: bool,
}
struct Shared<S: ObjectStore> {
    queue: Mutex<Queue<S>>,
    ready: Condvar,
    limits: Limits,
}
impl<S: ObjectStore> Shared<S> {
    fn close(&self, cancel: bool) {
        let mut queue = self.queue.lock().unwrap();
        queue.closed = true;
        queue.cancel_queued |= cancel;
        self.ready.notify_all();
    }
    fn fail(&self) {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.closed = true;
        queue.failed = true;
        let pending = std::mem::take(&mut queue.jobs);
        queue.commands = 0;
        queue.bytes = 0;
        drop(queue);
        for job in pending {
            job.state.store(FINISHED, Ordering::Release);
            job.task.reject(Error::WorkerFailed)();
        }
    }
}

/// Clones share the same bounded queue, never another database owner.
pub struct Client<S: ObjectStore> {
    shared: Arc<Shared<S>>,
    config: Config,
}
impl<S: ObjectStore> Clone for Client<S> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            config: self.config,
        }
    }
}
impl<S: ObjectStore + 'static> Client<S> {
    fn submit<T, F, B>(&self, bytes: usize, build: B) -> Result<Ticket<T>>
    where
        T: Send + 'static,
        B: FnOnce() -> crate::Result<F>,
        F: FnOnce(&mut SingleMachine<S>) -> crate::Result<T> + Send + 'static,
    {
        let (reply, receiver) = mpsc::sync_channel(1);
        let state = Arc::new(AtomicU8::new(QUEUED));
        let mut queue = self.shared.queue.lock().unwrap();
        if queue.failed {
            return Err(Error::WorkerFailed);
        }
        if queue.closed {
            return Err(Error::Closed);
        }
        if queue.commands == self.shared.limits.commands
            || bytes > self.shared.limits.bytes.saturating_sub(queue.bytes)
        {
            return Err(Error::Overloaded);
        }
        // Reserve capacity under the queue lock before bounded normalization.
        let operation = build()?;
        queue.commands += 1;
        queue.bytes += bytes;
        queue.jobs.push_back(Job {
            task: Box::new(Task { reply, operation }),
            state: state.clone(),
            queued: Instant::now(),
            bytes,
        });
        self.shared.ready.notify_one();
        Ok(Ticket { receiver, state })
    }
    pub fn status(&self) -> Status {
        let queue = self.shared.queue.lock().unwrap();
        Status {
            commands: queue.commands,
            bytes: queue.bytes,
            closed: queue.closed,
            failed: queue.failed,
        }
    }
    pub fn write(&self, request: Request) -> Result<Ticket<Outcome>> {
        request.validate(self.config)?;
        let charge = crate::encoded_len(&request)?;
        self.submit(charge, move || {
            let bytes = crate::encode(&request)?;
            let request: Request = crate::decode(&bytes)?;
            Ok(move |db: &mut SingleMachine<S>| db.apply_request(request))
        })
    }
    pub fn observe(&self, id: u64) -> Result<Ticket<Observation>> {
        self.submit(16, move || {
            Ok(move |db: &mut SingleMachine<S>| {
                Ok(Observation {
                    revision: db.revision(id),
                    request_id: db.request_id()?,
                })
            })
        })
    }
    pub fn lookup(&self, id: RequestId) -> Result<Ticket<Lookup>> {
        self.submit(24, move || {
            Ok(move |db: &mut SingleMachine<S>| db.lookup_request(id))
        })
    }
    pub fn query(
        &self,
        query: Vec<f32>,
        k: usize,
        filter: Vec<(String, String)>,
    ) -> Result<Ticket<QueryResult>> {
        self.config.vector(&query)?;
        if filter.len() > 100 {
            return Err(crate::Error::Invalid("at most 100 equality predicates".into()).into());
        }
        let charge = crate::encoded_len(&(&query, k, &filter))?;
        if charge > crate::retry::MAX_REQUEST_BYTES {
            return Err(Error::Overloaded);
        }
        self.submit(charge, move || {
            let bytes = crate::encode(&(&query, k, &filter))?;
            let (query, k, filter): (Vec<f32>, usize, Vec<(String, String)>) =
                crate::decode(&bytes)?;
            Ok(move |db: &mut SingleMachine<S>| {
                let borrowed: Vec<_> = filter
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let neighbors = db.query(&query, k, &borrowed, SearchMode::Exact)?;
                Ok(QueryResult {
                    sequence: db.status().maintenance.sequence,
                    neighbors,
                })
            })
        })
    }
}

/// Owns the worker lifecycle. Use explicit shutdown to acknowledge graceful
/// release. Drop closes admission and cancels queued work, but never claims that
/// an active storage operation has stopped or that ownership was released.
pub struct Service<S: ObjectStore> {
    client: Client<S>,
    worker: Option<JoinHandle<Result<()>>>,
}
impl<S: ObjectStore + Send + 'static> Service<S> {
    pub fn start(db: SingleMachine<S>, limits: Limits) -> Result<Self> {
        if db.status().recovery_required {
            return Err(crate::Error::RecoveryRequired.into());
        }
        if limits.commands == 0 || limits.bytes == 0 {
            db.close()?;
            return Err(crate::Error::Invalid("admission limits must be positive".into()).into());
        }
        let config = db.config();
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                jobs: VecDeque::new(),
                commands: 0,
                bytes: 0,
                closed: false,
                failed: false,
                cancel_queued: false,
            }),
            ready: Condvar::new(),
            limits,
        });
        let worker_shared = shared.clone();
        let worker = std::thread::Builder::new()
            .name("glider-committer".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(db, &worker_shared)
                }));
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => {
                        worker_shared.fail();
                        Err(error)
                    }
                    Err(_) => {
                        worker_shared.fail();
                        Err(Error::WorkerFailed)
                    }
                }
            })
            .map_err(crate::Error::Io)?;
        Ok(Self {
            client: Client { shared, config },
            worker: Some(worker),
        })
    }
    pub fn client(&self) -> Client<S> {
        self.client.clone()
    }
    /// Stop new admission synchronously; the worker finishes according to mode.
    /// Explicit shutdown subsequently joins it and acknowledges ownership release.
    pub fn begin_shutdown(&self, mode: Shutdown) {
        self.client
            .shared
            .close(matches!(mode, Shutdown::CancelQueued));
    }
    pub fn shutdown(mut self, mode: Shutdown) -> Result<()> {
        self.client
            .shared
            .close(matches!(mode, Shutdown::CancelQueued));
        self.worker
            .take()
            .expect("worker owned until shutdown")
            .join()
            .unwrap_or(Err(Error::WorkerFailed))
    }
}
impl<S: ObjectStore> Drop for Service<S> {
    fn drop(&mut self) {
        if self.worker.is_some() {
            self.client.shared.close(true);
        }
    }
}
fn run<S: ObjectStore>(mut db: SingleMachine<S>, shared: &Shared<S>) -> Result<()> {
    loop {
        let (job, cancel) = {
            let mut queue = shared.queue.lock().unwrap();
            while queue.jobs.is_empty() && !queue.closed {
                queue = shared.ready.wait(queue).unwrap();
            }
            match queue.jobs.pop_front() {
                Some(job) => (job, queue.cancel_queued),
                None => {
                    drop(queue);
                    return db.close().map_err(Error::Database);
                }
            }
        };
        let deliver = if cancel
            || job
                .state
                .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            job.task.reject(Error::Cancelled)
        } else {
            job.task.execute(&mut db, job.queued.elapsed())
        };
        job.state.store(FINISHED, Ordering::Release);
        {
            let mut queue = shared.queue.lock().unwrap();
            queue.commands -= 1;
            queue.bytes -= job.bytes;
        }
        let failed = db.status().recovery_required;
        if failed {
            shared.fail();
        }
        deliver();
        if failed {
            return Err(Error::WorkerFailed);
        }
    }
}
