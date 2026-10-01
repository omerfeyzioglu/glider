//! Bounded FIFO admission. One committer owns publication and maintenance;
//! reads run beside it on published snapshots when the engine provides them.
use crate::{
    retry::{Lookup, Outcome, Request, RequestId, Revision},
    segmented::{QueryHit, QueryOptions},
    serving::{SearchMode, SingleMachine},
    store::ObjectStore,
    streaming::OwnedDocument,
    Config, Neighbor,
};

/// One acknowledged engine state that answers reads beside the committer.
/// It never changes after publication.
pub trait Snapshot: Send + Sync {
    /// The acknowledged sequence this state reflects.
    fn sequence(&self) -> u64;
    /// Current document for an ID, or `None` if absent or deleted.
    fn get(&self, id: u64) -> crate::Result<Option<OwnedDocument>>;
    /// Hits with the fields `options` requests, and the remote reads and
    /// payload bytes this query caused.
    fn query(
        &self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
        options: QueryOptions,
    ) -> crate::Result<QueryResult>;
}

/// One database owner driven by the admission committer. Commands and
/// maintenance never overlap because only the committer holds it; reads
/// overlap them only through published snapshots.
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
    fn query_with_options(
        &mut self,
        query: &[f32],
        k: usize,
        filter: &[(&str, &str)],
        options: QueryOptions,
    ) -> crate::Result<Vec<QueryHit>> {
        self.query(query, k, filter)?
            .into_iter()
            .map(|neighbor| {
                let document = if options.include_metadata || options.include_vector {
                    Some(self.get(neighbor.id)?.ok_or_else(|| {
                        crate::Error::Corrupt("query hit missing from current documents".into())
                    })?)
                } else {
                    None
                };
                Ok(QueryHit {
                    id: neighbor.id,
                    distance: neighbor.distance,
                    metadata: document
                        .as_ref()
                        .and_then(|d| options.include_metadata.then(|| d.metadata.clone())),
                    vector: document.and_then(|d| options.include_vector.then_some(d.vector)),
                })
            })
            .collect()
    }
    /// Run at most one bounded maintenance unit while no command is queued.
    /// Returns whether work was performed.
    fn idle_step(&mut self) -> crate::Result<bool> {
        Ok(false)
    }
    /// Apply queued independent requests together. Engines that support
    /// group commit publish them in one durable object; the default applies
    /// them one by one. Returns one result per request, in order.
    fn apply_requests(&mut self, requests: Vec<Request>) -> Vec<crate::Result<Outcome>> {
        requests
            .into_iter()
            .map(|request| self.apply_request(request))
            .collect()
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
    /// The current acknowledged state for reads beside the committer, or
    /// `None` to run reads on the committer. When the engine at start returns
    /// a snapshot, the committer publishes a new one after every command and
    /// maintenance unit, before delivering any result.
    fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
        None
    }
    /// Called only after every snapshot has been dropped.
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
    fn get(&self, id: u64) -> crate::Result<Option<OwnedDocument>> {
        Ok(
            SingleMachine::get(self, id).map(|(vector, metadata)| OwnedDocument {
                vector: vector.to_vec(),
                metadata: metadata.clone(),
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
    /// For engines that run reads on the committer: when set, a queued query
    /// runs before queued writes, lookups and observations unless the oldest
    /// of those has waited this long. Queued commands are concurrent, so
    /// either order is linearizable; a query submitted after an
    /// acknowledgement always observes that write.
    pub read_priority: Option<Duration>,
    /// For engines that publish snapshots: reads executing at once, each on
    /// its own reader thread.
    pub queries: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            commands: 8,
            bytes: 320 * 1024,
            read_priority: None,
            queries: 4,
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
    pub hits: Vec<QueryHit>,
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
type Reply<T> = mpsc::SyncSender<Result<Timed<T>>>;
fn deliver<T: Send + 'static>(reply: Reply<T>, result: Result<Timed<T>>) -> Delivery {
    Box::new(move || {
        let _ = reply.send(result);
    })
}
/// Queries and document reads stay data so they can run on a snapshot.
enum Read {
    Query {
        query: Vec<f32>,
        k: usize,
        filter: Vec<(String, String)>,
        options: QueryOptions,
        reply: Reply<QueryResult>,
    },
    Get {
        id: u64,
        reply: Reply<Option<OwnedDocument>>,
    },
}
/// Where a read runs: on a published snapshot, or on the committer.
enum Target<'a, E> {
    Snapshot(&'a dyn Snapshot),
    Committer(&'a mut E),
}
impl Read {
    fn execute<E: Engine>(self, target: Target<'_, E>, queue_wait: Duration) -> Delivery {
        fn finish<T: Send + 'static>(
            reply: Reply<T>,
            result: crate::Result<T>,
            queue_wait: Duration,
            start: Instant,
        ) -> Delivery {
            let execution = start.elapsed();
            let result = result.map(|value| Timed {
                value,
                queue_wait,
                execution,
                maintenance: Duration::ZERO,
            });
            deliver(reply, result.map_err(Error::Database))
        }
        let start = Instant::now();
        match self {
            Read::Query {
                query,
                k,
                filter,
                options,
                reply,
            } => {
                let filter: Vec<_> = filter
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                let result = match target {
                    Target::Snapshot(snapshot) => snapshot.query(&query, k, &filter, options),
                    Target::Committer(db) => {
                        let (reads, bytes) = db.remote_reads();
                        db.query_with_options(&query, k, &filter, options)
                            .map(|hits| {
                                let (reads_after, bytes_after) = db.remote_reads();
                                QueryResult {
                                    sequence: db.sequence(),
                                    remote_reads: reads_after - reads,
                                    remote_bytes: bytes_after - bytes,
                                    neighbors: hits.iter().map(QueryHit::neighbor).collect(),
                                    hits,
                                }
                            })
                    }
                };
                finish(reply, result, queue_wait, start)
            }
            Read::Get { id, reply } => {
                let result = match target {
                    Target::Snapshot(snapshot) => snapshot.get(id),
                    Target::Committer(db) => db.get(id),
                };
                finish(reply, result, queue_wait, start)
            }
        }
    }
    fn reject(self, error: Error) -> Delivery {
        match self {
            Read::Query { reply, .. } => deliver(reply, Err(error)),
            Read::Get { reply, .. } => deliver(reply, Err(error)),
        }
    }
}
/// Writes stay data so the worker can group consecutive ones.
enum Work<E: Engine> {
    Task(Box<dyn Execute<E>>),
    Write(Request, Reply<Outcome>),
    Read(Read),
}
impl<E: Engine> Work<E> {
    fn reject(self, error: Error) -> Delivery {
        match self {
            Work::Task(task) => task.reject(error),
            Work::Write(_, reply) => deliver(reply, Err(error)),
            Work::Read(read) => read.reject(error),
        }
    }
}
/// At most this many queued writes, and this many encoded bytes, share one
/// group commit.
const MAX_GROUP_WRITES: usize = 16;
const MAX_GROUP_BYTES: usize = 1024 * 1024;
struct Job<E: Engine> {
    read: bool,
    work: Work<E>,
    state: Arc<AtomicU8>,
    queued: Instant,
    bytes: usize,
}
struct Queue<E: Engine> {
    /// Committer work, including reads when the engine publishes no snapshot.
    jobs: VecDeque<Job<E>>,
    /// Reads for the reader threads, when the engine publishes snapshots.
    reads: VecDeque<Job<E>>,
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
    /// Wakes the committer.
    ready: Condvar,
    /// Wakes reader threads.
    readable: Condvar,
    limits: Limits,
    /// Reader threads: `limits.queries`, or zero when the engine publishes
    /// no snapshot and reads run on the committer.
    readers: usize,
    /// The latest published snapshot; `None` when reads run on the committer.
    /// Only the committer replaces it, always with a newer state.
    snapshot: Mutex<Option<Arc<dyn Snapshot>>>,
}
impl<E: Engine> Shared<E> {
    fn close(&self, cancel: bool) {
        let mut queue = self.queue.lock().unwrap();
        queue.closed = true;
        queue.cancel_queued |= cancel;
        self.ready.notify_all();
        self.readable.notify_all();
    }
    fn fail(&self) {
        let mut queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        queue.closed = true;
        queue.failed = true;
        let mut pending = std::mem::take(&mut queue.jobs);
        pending.append(&mut queue.reads);
        // Charges of active work are released here too; see `release`.
        queue.commands = 0;
        queue.bytes = 0;
        self.ready.notify_all();
        self.readable.notify_all();
        drop(queue);
        for job in pending {
            job.state.store(FINISHED, Ordering::Release);
            job.work.reject(Error::WorkerFailed)();
        }
    }
    fn publish(&self, snapshot: Option<Arc<dyn Snapshot>>) {
        *self.snapshot.lock().unwrap_or_else(|e| e.into_inner()) = snapshot;
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
        self.enqueue(read, bytes, move |reply| {
            let operation = build()?;
            Ok(Work::Task(Box::new(Task { reply, operation })))
        })
    }
    fn enqueue<T, B>(&self, read: bool, bytes: usize, build: B) -> Result<Ticket<T>>
    where
        B: FnOnce(mpsc::SyncSender<Result<Timed<T>>>) -> crate::Result<Work<E>>,
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
        let work = build(reply)?;
        queue.commands += 1;
        queue.bytes += bytes;
        let job = Job {
            read,
            work,
            state: state.clone(),
            queued: Instant::now(),
            bytes,
        };
        if self.shared.readers > 0 && matches!(job.work, Work::Read(_)) {
            queue.reads.push_back(job);
            self.shared.readable.notify_one();
        } else {
            queue.jobs.push_back(job);
            self.shared.ready.notify_one();
        }
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
        self.enqueue(false, charge, move |reply| {
            let bytes = crate::encode(&request)?;
            let request: Request = crate::decode(&bytes)?;
            Ok(Work::Write(request, reply))
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
    pub fn get(&self, id: u64) -> Result<Ticket<Option<OwnedDocument>>> {
        self.enqueue(true, 16, move |reply| {
            Ok(Work::Read(Read::Get { id, reply }))
        })
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
        self.query_with_options(query, k, filter, QueryOptions::default())
    }

    pub fn query_with_options(
        &self,
        query: Vec<f32>,
        k: usize,
        filter: Vec<(String, String)>,
        options: QueryOptions,
    ) -> Result<Ticket<QueryResult>> {
        self.config.vector(&query)?;
        if filter.len() > 100 {
            return Err(crate::Error::Invalid("at most 100 equality predicates".into()).into());
        }
        let charge = crate::encoded_len(&(&query, k, &filter))?;
        if charge > crate::retry::MAX_REQUEST_BYTES {
            return Err(Error::Overloaded);
        }
        self.enqueue(true, charge, move |reply| {
            let bytes = crate::encode(&(&query, k, &filter))?;
            let (query, k, filter) = crate::decode(&bytes)?;
            Ok(Work::Read(Read::Query {
                query,
                k,
                filter,
                options,
                reply,
            }))
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
        if limits.commands == 0 || limits.bytes == 0 || limits.queries == 0 {
            db.close()?;
            return Err(crate::Error::Invalid("admission limits must be positive".into()).into());
        }
        let config = db.config();
        let snapshot = db.snapshot();
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                jobs: VecDeque::new(),
                reads: VecDeque::new(),
                commands: 0,
                bytes: 0,
                closed: false,
                failed: false,
                cancel_queued: false,
                idle_steps: Vec::new(),
                maintenance_errors: 0,
            }),
            ready: Condvar::new(),
            readable: Condvar::new(),
            limits,
            readers: if snapshot.is_some() {
                limits.queries
            } else {
                0
            },
            snapshot: Mutex::new(snapshot),
        });
        let mut readers = Vec::with_capacity(shared.readers);
        for _ in 0..shared.readers {
            let reader_shared = shared.clone();
            let spawned = std::thread::Builder::new()
                .name("glider-reader".into())
                .spawn(move || serve_reads(&reader_shared));
            match spawned {
                Ok(reader) => readers.push(reader),
                Err(error) => {
                    shared.close(true);
                    for reader in readers {
                        let _ = reader.join();
                    }
                    shared.publish(None);
                    db.close()?;
                    return Err(crate::Error::Io(error).into());
                }
            }
        }
        let worker_shared = shared.clone();
        let spawned = std::thread::Builder::new()
            .name("glider-committer".into())
            .spawn(move || {
                let mut db = db;
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run(&mut db, &worker_shared)
                }))
                .unwrap_or(Err(Error::WorkerFailed));
                if result.is_err() {
                    worker_shared.fail();
                }
                // Readers stop once admission is closed and their queue is
                // drained; then no snapshot outlives the engine.
                for reader in readers {
                    let _ = reader.join();
                }
                worker_shared.publish(None);
                result?;
                // A reader that panicked failed the service.
                if worker_shared
                    .queue
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed
                {
                    return Err(Error::WorkerFailed);
                }
                db.close().map_err(Error::Database)
            });
        let worker = match spawned {
            Ok(worker) => worker,
            Err(error) => {
                // Stops the readers; the dropped engine keeps its claim.
                shared.close(true);
                return Err(crate::Error::Io(error).into());
            }
        };
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
/// Return a finished job's admission charge.
fn release<E: Engine>(shared: &Shared<E>, bytes: usize) {
    let mut queue = shared.queue.lock().unwrap();
    // `fail` already returned every charge, including active work.
    if !queue.failed {
        queue.commands -= 1;
        queue.bytes -= bytes;
    }
}

/// Publish the committer's current state for reader threads. Called before
/// any result of the work that produced it is delivered, so a read admitted
/// after an acknowledgement observes that write.
fn publish<E: Engine>(db: &E, shared: &Shared<E>) {
    if shared.readers > 0 {
        shared.publish(db.snapshot());
    }
}

/// Reader thread: run queued reads, each on the latest snapshot when it
/// starts, until admission closes and no read is queued.
fn serve_reads<E: Engine>(shared: &Shared<E>) {
    loop {
        let (job, cancel) = {
            let mut queue = shared.queue.lock().unwrap();
            loop {
                if let Some(job) = queue.reads.pop_front() {
                    break (job, queue.cancel_queued);
                }
                if queue.closed {
                    return;
                }
                queue = shared.readable.wait(queue).unwrap();
            }
        };
        let Work::Read(read) = job.work else {
            unreachable!("reader threads receive only reads");
        };
        if cancel
            || job
                .state
                .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            let delivery = read.reject(Error::Cancelled);
            job.state.store(FINISHED, Ordering::Release);
            release(shared, job.bytes);
            delivery();
            continue;
        }
        let queue_wait = job.queued.elapsed();
        let snapshot = shared
            .snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("snapshots are published while readers run");
        let delivery = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            read.execute::<E>(Target::Snapshot(&*snapshot), queue_wait)
        }));
        drop(snapshot);
        job.state.store(FINISHED, Ordering::Release);
        release(shared, job.bytes);
        match delivery {
            Ok(deliver) => deliver(),
            // The dropped reply reports `WorkerFailed` to this read's ticket.
            Err(_) => {
                shared.fail();
                return;
            }
        }
    }
}

/// The committer: commands, write groups and idle maintenance, one at a
/// time. Returns once admission is closed and its queue is drained.
fn run<E: Engine>(db: &mut E, shared: &Shared<E>) -> Result<()> {
    let mut idle_work = true;
    loop {
        let (jobs, cancel) = {
            let mut queue = shared.queue.lock().unwrap();
            while queue.jobs.is_empty() && !queue.closed {
                if idle_work {
                    // Queued commands take precedence; one bounded unit runs
                    // only while no command is queued and is never preempted.
                    drop(queue);
                    let start = Instant::now();
                    let step = db.idle_step();
                    let elapsed = start.elapsed();
                    if matches!(step, Ok(true)) {
                        publish(db, shared);
                    }
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
            let mut jobs = match queue.jobs.remove(next) {
                Some(job) => vec![job],
                None if queue.failed => return Err(Error::WorkerFailed),
                None => return Ok(()),
            };
            // Group consecutive queued writes for one durable publication.
            if matches!(jobs[0].work, Work::Write(..)) {
                let mut bytes = jobs[0].bytes;
                while jobs.len() < MAX_GROUP_WRITES
                    && queue.jobs.front().is_some_and(|job| {
                        matches!(job.work, Work::Write(..)) && bytes + job.bytes <= MAX_GROUP_BYTES
                    })
                {
                    let job = queue.jobs.pop_front().expect("front checked");
                    bytes += job.bytes;
                    jobs.push(job);
                }
            }
            (jobs, queue.cancel_queued)
        };
        let mut deliveries: Vec<Delivery> = Vec::with_capacity(jobs.len());
        let mut writes = Vec::new();
        let mut started = Vec::with_capacity(jobs.len());
        for job in jobs {
            if cancel
                || job
                    .state
                    .compare_exchange(QUEUED, STARTED, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                deliveries.push(job.work.reject(Error::Cancelled));
                job.state.store(FINISHED, Ordering::Release);
                release(shared, job.bytes);
            } else {
                started.push(job);
            }
        }
        let mut write_jobs = Vec::new();
        for job in started {
            let queue_wait = job.queued.elapsed();
            match job.work {
                Work::Task(task) => {
                    deliveries.push(task.execute(db, queue_wait));
                    job.state.store(FINISHED, Ordering::Release);
                    release(shared, job.bytes);
                }
                Work::Read(read) => {
                    deliveries.push(read.execute(Target::Committer(&mut *db), queue_wait));
                    job.state.store(FINISHED, Ordering::Release);
                    release(shared, job.bytes);
                }
                Work::Write(request, reply) => {
                    writes.push((request, reply, queue_wait));
                    write_jobs.push((job.state, job.bytes));
                }
            }
        }
        if !writes.is_empty() {
            let before = db.maintenance_time();
            let start = Instant::now();
            let (requests, replies): (Vec<_>, Vec<_>) = writes
                .into_iter()
                .map(|(request, reply, wait)| (request, (reply, wait)))
                .unzip();
            let results = db.apply_requests(requests);
            let elapsed = start.elapsed();
            let maintenance = db.maintenance_time() - before;
            // Active writes hold their charge until their results exist.
            for (state, bytes) in write_jobs {
                state.store(FINISHED, Ordering::Release);
                release(shared, bytes);
            }
            for (result, (reply, queue_wait)) in results.into_iter().zip(replies) {
                let result = result
                    .map(|value| Timed {
                        value,
                        queue_wait,
                        execution: elapsed.saturating_sub(maintenance),
                        maintenance,
                    })
                    .map_err(Error::Database);
                deliveries.push(deliver(reply, result));
            }
        }
        idle_work = true;
        let failed = db.recovery_required();
        if failed {
            shared.fail();
        } else {
            publish(db, shared);
        }
        for deliver in deliveries {
            deliver();
        }
        if failed {
            return Err(Error::WorkerFailed);
        }
    }
}
