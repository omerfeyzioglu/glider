use super::{
    catalog::{Catalog, Collection, CreateCollection},
    Engine0, Running, ServerConfig,
};
use crate::{
    admission::{Client, Shutdown},
    Error, Result,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{watch, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};

struct Open {
    client: Client<Engine0>,
    running: Mutex<Option<Running>>,
    generation: String,
    active: AtomicUsize,
    idle: Notify,
    closing: AtomicBool,
    used: AtomicU64,
    last_used: Mutex<Instant>,
    _permit: OwnedSemaphorePermit,
}

pub struct Use {
    open: Arc<Open>,
}
impl Use {
    pub fn client(&self) -> Client<Engine0> {
        self.open.client.clone()
    }
}
impl Drop for Use {
    fn drop(&mut self) {
        if self.open.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.open.idle.notify_waiters();
        }
    }
}

struct Inner {
    config: ServerConfig,
    catalog: Catalog,
    opened: AsyncMutex<HashMap<String, Arc<Open>>>,
    names: AsyncMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    permits: Arc<Semaphore>,
    tick: AtomicU64,
    idle_stop: watch::Sender<bool>,
    idle_task: Mutex<Option<JoinHandle<()>>>,
}
#[derive(Clone)]
pub struct Multi {
    inner: Arc<Inner>,
}

impl Multi {
    pub fn new(config: ServerConfig) -> Result<Self> {
        if !config.multi {
            return Err(Error::Invalid(
                "GLIDER_DIMENSIONS selects single-collection mode".into(),
            ));
        }
        let catalog = Catalog::new(config.store.clone());
        // Open and validate the catalog before serving requests.
        catalog.sweep()?;
        let idle = config.collection_idle;
        let (idle_stop, mut idle_stopped) = watch::channel(false);
        let multi = Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(config.max_open_collections)),
                config,
                catalog,
                opened: AsyncMutex::new(HashMap::new()),
                names: AsyncMutex::new(HashMap::new()),
                tick: AtomicU64::new(0),
                idle_stop,
                idle_task: Mutex::new(None),
            }),
        };
        if !idle.is_zero() {
            let interval = (idle / 4)
                .min(Duration::from_secs(5))
                .max(Duration::from_millis(1));
            let weak = Arc::downgrade(&multi.inner);
            let task = tokio::runtime::Handle::try_current()
                .map_err(|error| Error::Invalid(format!("multi mode requires Tokio: {error}")))?
                .spawn(async move {
                    let mut ticks = tokio::time::interval(interval);
                    ticks.tick().await;
                    loop {
                        tokio::select! {
                            _ = ticks.tick() => {
                                let Some(inner) = weak.upgrade() else { break };
                                Multi { inner }.close_idle(idle).await;
                            }
                            changed = idle_stopped.changed() => {
                                if changed.is_err() || *idle_stopped.borrow() { break }
                            }
                        }
                    }
                });
            *multi.inner.idle_task.lock().unwrap() = Some(task);
        }
        Ok(multi)
    }
    async fn name_lock(&self, name: &str) -> Arc<AsyncMutex<()>> {
        let mut names = self.inner.names.lock().await;
        names
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone()
    }
    pub fn catalog(&self) -> &Catalog {
        &self.inner.catalog
    }
    pub async fn is_open(&self, name: &str) -> bool {
        self.inner.opened.lock().await.contains_key(name)
    }
    pub async fn open_count(&self) -> usize {
        self.inner.opened.lock().await.len()
    }
    pub async fn create(&self, request: CreateCollection) -> Result<(Collection, bool)> {
        let name = request.name.clone();
        let lock = self.name_lock(&name).await;
        let _guard = lock.lock().await;
        let catalog = self.inner.config.store.clone();
        tokio::task::spawn_blocking(move || Catalog::new(catalog).create(request))
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?
    }
    pub async fn list(&self) -> Result<Vec<Collection>> {
        let base = self.inner.config.store.clone();
        tokio::task::spawn_blocking(move || Catalog::new(base).list())
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?
    }
    /// Best-effort reclamation of generations no longer named by the catalog.
    pub async fn sweep(&self) -> Result<()> {
        let base = self.inner.config.store.clone();
        tokio::task::spawn_blocking(move || Catalog::new(base).sweep())
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?
    }
    pub async fn get(&self, name: &str) -> Result<Option<Collection>> {
        let base = self.inner.config.store.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || Catalog::new(base).get(&name))
            .await
            .map_err(|error| Error::Invalid(error.to_string()))?
    }
    fn touch(&self, open: &Open) {
        *open.last_used.lock().unwrap() = Instant::now();
        open.used.store(
            self.inner.tick.fetch_add(1, Ordering::Relaxed) + 1,
            Ordering::Relaxed,
        );
    }
    pub async fn use_collection(&self, name: &str) -> Result<Option<Use>> {
        let lock = self.name_lock(name).await;
        let _guard = lock.lock().await;
        // This process is the only one serving its base prefix, and create,
        // delete and close all hold the name lock, so an open entry is current
        // without re-reading the catalog on every request.
        if let Some(open) = self.inner.opened.lock().await.get(name).cloned() {
            if !open.closing.load(Ordering::Acquire) {
                open.active.fetch_add(1, Ordering::AcqRel);
                self.touch(&open);
                return Ok(Some(Use { open }));
            }
        }
        let Some(record) = self.get(name).await? else {
            return Ok(None);
        };
        let permit = match self.inner.permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                self.evict(name).await?;
                self.inner
                    .permits
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| {
                        Error::Busy("all open collections have requests in flight".into())
                    })?
            }
        };
        let mut config = self.inner.config.clone();
        config.store = self.inner.catalog.data_store(&record);
        config.collection = record.config();
        config.options = record.options();
        config.multi = false;
        config.limits.queries = 2;
        config.serving.query_threads = 2;
        if let Some(cache) = config.serving.cache.as_mut() {
            cache.0 = cache
                .0
                .join(format!("{}-{}", record.name, record.generation));
            cache.2 = (cache.2 / config.max_open_collections).max(16 * 1024 * 1024);
        }
        let running = tokio::task::spawn_blocking(move || config.start())
            .await
            .map_err(|error| Error::Invalid(error.to_string()))??;
        let deposed = running.deposed();
        let open = Arc::new(Open {
            client: running.client(),
            running: Mutex::new(Some(running)),
            generation: record.generation.clone(),
            active: AtomicUsize::new(1),
            idle: Notify::new(),
            closing: AtomicBool::new(false),
            used: AtomicU64::new(0),
            last_used: Mutex::new(Instant::now()),
            _permit: permit,
        });
        self.touch(&open);
        self.inner
            .opened
            .lock()
            .await
            .insert(name.to_owned(), open.clone());
        let manager = self.clone();
        let name = name.to_owned();
        let generation = record.generation;
        tokio::spawn(async move {
            deposed.notified().await;
            let _ = manager.close_if_generation(&name, &generation).await;
        });
        Ok(Some(Use { open }))
    }
    async fn evict(&self, except: &str) -> Result<()> {
        let candidates = {
            let opened = self.inner.opened.lock().await;
            let mut candidates: Vec<_> = opened
                .iter()
                .filter(|(name, open)| {
                    name.as_str() != except && open.active.load(Ordering::Acquire) == 0
                })
                .map(|(name, open)| (open.used.load(Ordering::Relaxed), name.clone()))
                .collect();
            candidates.sort();
            candidates
        };
        for (_, name) in candidates {
            let lock = self.name_lock(&name).await;
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            let open = {
                let mut opened = self.inner.opened.lock().await;
                if opened
                    .get(&name)
                    .is_some_and(|open| open.active.load(Ordering::Acquire) == 0)
                {
                    opened.remove(&name)
                } else {
                    None
                }
            };
            if let Some(open) = open {
                self.close(open).await?;
                return Ok(());
            }
        }
        Err(Error::Busy(
            "all open collections have requests in flight".into(),
        ))
    }
    async fn close_idle(&self, idle: Duration) {
        let names: Vec<_> = self.inner.opened.lock().await.keys().cloned().collect();
        for name in names {
            let lock = self.name_lock(&name).await;
            let Ok(_guard) = lock.try_lock() else {
                continue;
            };
            let open = {
                let mut opened = self.inner.opened.lock().await;
                if opened.get(&name).is_some_and(|open| {
                    open.active.load(Ordering::Acquire) == 0
                        && open.last_used.lock().unwrap().elapsed() >= idle
                }) {
                    opened.remove(&name)
                } else {
                    None
                }
            };
            if let Some(open) = open {
                if let Err(error) = self.close(open).await {
                    eprintln!("idle collection close failed for {name}: {error}");
                }
            }
        }
    }
    async fn close(&self, open: Arc<Open>) -> Result<()> {
        open.closing.store(true, Ordering::Release);
        loop {
            let notified = open.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if open.active.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
        let running = open.running.lock().unwrap().take();
        if let Some(running) = running {
            tokio::task::spawn_blocking(move || running.shutdown(Shutdown::Drain))
                .await
                .map_err(|error| Error::Invalid(error.to_string()))??;
        }
        Ok(())
    }
    async fn close_if_generation(&self, name: &str, generation: &str) -> Result<()> {
        let lock = self.name_lock(name).await;
        let _guard = lock.lock().await;
        let open = {
            let mut opened = self.inner.opened.lock().await;
            if opened
                .get(name)
                .is_some_and(|open| open.generation == generation)
            {
                opened.remove(name)
            } else {
                None
            }
        };
        if let Some(open) = open {
            self.close(open).await?;
        }
        Ok(())
    }
    pub async fn delete(&self, name: &str) -> Result<bool> {
        let lock = self.name_lock(name).await;
        let _guard = lock.lock().await;
        let Some(record) = self.get(name).await? else {
            return Ok(false);
        };
        if let Some(open) = self.inner.opened.lock().await.remove(name) {
            self.close(open).await?;
        }
        let base = self.inner.config.store.clone();
        let cache = self.inner.config.serving.cache.as_ref().map(|cache| {
            cache
                .0
                .join(format!("{}-{}", record.name, record.generation))
        });
        tokio::task::spawn_blocking(move || {
            let catalog = Catalog::new(base);
            catalog.delete(&record)?;
            // The cache is disposable; a leftover directory is never read
            // because a recreated collection gets a new generation.
            if let Some(cache) = cache {
                let _ = std::fs::remove_dir_all(cache);
            }
            // A failed cleanup leaves an invisible orphan for the next sweep.
            let _ = catalog.remove_data(&record);
            let _ = catalog.sweep();
            Ok::<(), Error>(())
        })
        .await
        .map_err(|error| Error::Invalid(error.to_string()))??;
        Ok(true)
    }
    pub async fn shutdown(&self) -> Result<()> {
        self.inner.idle_stop.send_replace(true);
        let task = self.inner.idle_task.lock().unwrap().take();
        if let Some(task) = task {
            task.await
                .map_err(|error| Error::Invalid(error.to_string()))?;
        }
        let names: Vec<_> = self.inner.opened.lock().await.keys().cloned().collect();
        let mut first_error = None;
        for name in names {
            let lock = self.name_lock(&name).await;
            let _guard = lock.lock().await;
            if let Some(open) = self.inner.opened.lock().await.remove(&name) {
                if let Err(error) = self.close(open).await {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
