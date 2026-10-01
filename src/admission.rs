//! Bounded FIFO admission; one worker owns all reads, publication and maintenance.
use crate::{
    retry::{Lookup, Outcome, Request, RequestId, Revision},
    serving::{SearchMode, SingleMachine},
    store::ObjectStore,
    Config, Neighbor,
};

/// One serialized database owner driven by the admission worker. Reads,
/// publication and maintenance never overlap because only the worker holds it.
pub trait Engine: Send + 'static {
    fn config(&self) -> Config;
    fn sequence(&self) -> u64;
    fn recovery_required(&self) -> bool;
    /// Cumulative synchronous maintenance time charged inside commands.
    fn maintenance_time(&self) -> Duration;
    fn apply_request(&mut self, request: Request) -> crate::Result<Outcome>;
    fn revision(&self, id: u64) -> Revision;
    fn request_id(&self) -> crate::Result<RequestId>;
    fn lookup_request(&self, id: RequestId) -> crate::Result<Lookup>;
    /// Current document for an ID, or `None` if absent or deleted.
    fn get(&self, id: u64) -> crate::Result<Option<crate::streaming::OwnedDocument>>;
    fn query(
        &mut self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> crate::Result<Vec<Neighbor>>;
    /// Run at most one bounded maintenance unit while no command is queued.
    /// Returns whether work was performed.
    fn idle_step(&mut self) -> crate::Result<bool> {
        Ok(false)
    }
    /// Cumulative remote block reads and payload bytes charged to queries.
    fn remote_reads(&self) -> (u64, u64) {
        (0, 0)
    }
    /// Read-only values sampled on the owning worker thread.
    fn metrics(&self) -> crate::Result<EngineMetrics> {
        Ok(EngineMetrics {
            sequence: self.sequence(),
            samples: Vec::new(),
        })
    }
    fn close(self) -> crate::Result<()>;
}

impl<S: ObjectStore + Send + 'static> Engine for SingleMachine<S> {
    fn config(&self) -> Config {
        SingleMachine::config(self)
    }
    fn sequence(&self) -> u64 {
        self.status().maintenance.sequence
    }
    fn recovery_required(&self) -> bool {
        self.status().recovery_required
    }
    fn maintenance_time(&self) -> Duration {
        self.status().maintenance_time
    }
    fn apply_request(&mut self, request: Request) -> crate::Result<Outcome> {
        SingleMachine::apply_request(self, request)
    }
    fn revision(&self, id: u64) -> Revision {
        SingleMachine::revision(self, id)
    }
    fn request_id(&self) -> crate::Result<RequestId> {
        SingleMachine::request_id(self)
    }
    fn lookup_request(&self, id: RequestId) -> crate::Result<Lookup> {
        SingleMachine::lookup_request(self, id)
    }
    fn get(&self, id: u64) -> crate::Result<Option<crate::streaming::OwnedDocument>> {
        Ok(
            SingleMachine::get(self, id).map(|(vector, metadata)| {
                crate::streaming::OwnedDocument {
                    vector: vector.to_vec(),
                    metadata: metadata.clone(),
                }
            }),
        )
    }
    fn query(
        &mut self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
    ) -> crate::Result<Vec<Neighbor>> {
        SingleMachine::query(self, query, k, filter, SearchMode::Exact)
    }
    fn close(self) -> crate::Result<()> {
        SingleMachine::close(self)
    }
}
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
    /// When set, a queued query runs before queued writes, lookups and
    /// observations unless the oldest of those has waited this long. Queued
    /// commands are concurrent, so either order is linearizable; a query
    /// submitted after an acknowledgement always observes that write.
    pub read_priority: Option<Duration>,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            commands: 8,
            bytes: 320 * 1024,
            read_priority: None,
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct Status {
    pub commands: usize,
    pub bytes: usize,
    pub closed: bool,
    pub failed: bool,
    /// Idle maintenance units that failed without poisoning the engine.
    pub maintenance_errors: u64,
}
/// Engine values returned through the admission queue. Sample names are fixed
/// by the engine implementation, never supplied by a client.
#[derive(Debug)]
pub struct EngineMetrics {
    pub sequence: u64,
    pub samples: Vec<(&'static str, u64)>,
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
    /// Remote object reads and payload bytes this query caused.
    pub remote_reads: u64,
    pub remote_bytes: u64,
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
trait Execute<E: Engine>: Send {
    fn execute(self: Box<Self>, db: &mut E, queue_wait: Duration) -> Delivery;
    fn reject(self: Box<Self>, error: Error) -> Delivery;
}
struct Task<T, F> {
    reply: mpsc::SyncSender<Result<Timed<T>>>,
    operation: F,
}
impl<E, T, F> Execute<E> for Task<T, F>
where
    E: Engine,
    T: Send + 'static,
    F: FnOnce(&mut E) -> crate::Result<T> + Send,
{
    fn execute(self: Box<Self>, db: &mut E, queue_wait: Duration) -> Delivery {
        let before = db.maintenance_time();
        let start = Instant::now();
        let result = (self.operation)(db);
        let elapsed = start.elapsed();
        let maintenance = db.maintenance_time() - before;
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
struct Job<E: Engine> {
    read: bool,
    task: Box<dyn Execute<E>>,
    state: Arc<AtomicU8>,
    queued: Instant,
    bytes: usize,
}
struct Queue<E: Engine> {
    jobs: VecDeque<Job<E>>,
    commands: usize,
    bytes: usize,
    closed: bool,
    failed: bool,
    cancel_queued: bool,
    /// Durations of idle maintenance units in microseconds, capped.
    idle_steps: Vec<u32>,
    maintenance_errors: u64,
}
const MAX_IDLE_SAMPLES: usize = 65_536;
struct Shared<E: Engine> {
    queue: Mutex<Queue<E>>,
    ready: Condvar,
    limits: Limits,
}
impl<E: Engine> Shared<E> {
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
pub struct Client<E: Engine> {
    shared: Arc<Shared<E>>,
    config: Config,
}
impl<E: Engine> Clone for Client<E> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            config: self.config,
        }
    }
}
impl<E: Engine> Client<E> {
    fn submit<T, F, B>(&self, read: bool, bytes: usize, build: B) -> Result<Ticket<T>>
    where
        T: Send + 'static,
        B: FnOnce() -> crate::Result<F>,
        F: FnOnce(&mut E) -> crate::Result<T> + Send + 'static,
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
            read,
            task: Box::new(Task { reply, operation }),
            state: state.clone(),
            queued: Instant::now(),
            bytes,
        });
        self.shared.ready.notify_one();
        Ok(Ticket { receiver, state })
    }
    /// Durations of completed idle maintenance units (at most 65,536).
    pub fn idle_maintenance_samples(&self) -> Vec<Duration> {
        let queue = self.shared.queue.lock().unwrap();
        queue
            .idle_steps
            .iter()
            .map(|&micros| Duration::from_micros(u64::from(micros)))
            .collect()
    }
    pub fn status(&self) -> Status {
        let queue = self.shared.queue.lock().unwrap();
        Status {
            commands: queue.commands,
            bytes: queue.bytes,
            closed: queue.closed,
            failed: queue.failed,
            maintenance_errors: queue.maintenance_errors,
        }
    }
    pub fn metrics(&self) -> Result<Ticket<EngineMetrics>> {
        self.submit(true, 16, || Ok(|db: &mut E| db.metrics()))
    }
    pub fn write(&self, request: Request) -> Result<Ticket<Outcome>> {
        request.validate(self.config)?;
        let charge = crate::encoded_len(&request)?;
        self.submit(false, charge, move || {
            let bytes = crate::encode(&request)?;
            let request: Request = crate::decode(&bytes)?;
            Ok(move |db: &mut E| db.apply_request(request))
        })
    }
    pub fn observe(&self, id: u64) -> Result<Ticket<Observation>> {
        self.submit(false, 16, move || {
            Ok(move |db: &mut E| {
                Ok(Observation {
                    revision: db.revision(id),
                    request_id: db.request_id()?,
                })
            })
        })
    }
    pub fn get(&self, id: u64) -> Result<Ticket<Option<crate::streaming::OwnedDocument>>> {
        self.submit(true, 16, move || Ok(move |db: &mut E| db.get(id)))
    }
    pub fn lookup(&self, id: RequestId) -> Result<Ticket<Lookup>> {
        self.submit(false, 24, move || {
            Ok(move |db: &mut E| db.lookup_request(id))
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
        self.submit(true, charge, move || {
            let bytes = crate::encode(&(&query, k, &filter))?;
            let (query, k, filter): (Vec<f32>, usize, Vec<(String, String)>) =
                crate::decode(&bytes)?;
            Ok(move |db: &mut E| {
                let borrowed: Vec<_> = filter
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let (reads, bytes) = db.remote_reads();
                let neighbors = db.query(&query, k, &borrowed)?;
                let (reads_after, bytes_after) = db.remote_reads();
                Ok(QueryResult {
                    sequence: db.sequence(),
                    remote_reads: reads_after - reads,
                    remote_bytes: bytes_after - bytes,
                    neighbors,
                })
            })
        })
    }
}

/// Owns the worker lifecycle. Use explicit shutdown to acknowledge graceful
/// release. Drop closes admission and cancels queued work, but never claims that
/// an active storage operation has stopped or that ownership was released.
pub struct Service<E: Engine> {
    client: Client<E>,
    worker: Option<JoinHandle<Result<()>>>,
}
impl<E: Engine> Service<E> {
    pub fn start(db: E, limits: Limits) -> Result<Self> {
        if db.recovery_required() {
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
                idle_steps: Vec::new(),
                maintenance_errors: 0,
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
    pub fn client(&self) -> Client<E> {
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
impl<E: Engine> Drop for Service<E> {
    fn drop(&mut self) {
        if self.worker.is_some() {
            self.client.shared.close(true);
        }
    }
}
fn run<E: Engine>(mut db: E, shared: &Shared<E>) -> Result<()> {
    let mut idle_work = true;
    loop {
        let (job, cancel) = {
            let mut queue = shared.queue.lock().unwrap();
            while queue.jobs.is_empty() && !queue.closed {
                if idle_work {
                    // Queued commands take precedence; one bounded unit runs
                    // only while the queue is empty and is never preempted.
                    drop(queue);
                    let start = Instant::now();
                    let step = db.idle_step();
                    let elapsed = start.elapsed();
                    queue = shared.queue.lock().unwrap();
                    idle_work = match step {
                        Ok(worked) => worked,
                        Err(error) if db.recovery_required() => {
                            return Err(Error::Database(error));
                        }
                        // A failed read leaves authoritative state unchanged;
                        // retry only after the next command, and report it.
                        Err(_) => {
                            queue.maintenance_errors += 1;
                            false
                        }
                    };
                    if idle_work && queue.idle_steps.len() < MAX_IDLE_SAMPLES {
                        queue
                            .idle_steps
                            .push(u32::try_from(elapsed.as_micros()).unwrap_or(u32::MAX));
                    }
                    continue;
                }
                queue = shared.ready.wait(queue).unwrap();
            }
            let next = match shared.limits.read_priority {
                Some(aging)
                    if queue
                        .jobs
                        .front()
                        .is_some_and(|job| !job.read && job.queued.elapsed() < aging) =>
                {
                    queue.jobs.iter().position(|job| job.read).unwrap_or(0)
                }
                _ => 0,
            };
            match queue.jobs.remove(next) {
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
        idle_work = true;
        let failed = db.recovery_required();
        if failed {
            shared.fail();
        }
        deliver();
        if failed {
            return Err(Error::WorkerFailed);
        }
    }
}
