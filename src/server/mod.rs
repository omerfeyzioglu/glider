//! HTTP/JSON service for one segmented collection.
//!
//! One process owns one namespace through `SegmentedServing` behind the
//! bounded admission queue. Startup acquires the namespace's renewed lease
//! (waiting out a dead writer's lease) and takes over with fencing, so a
//! restart after a crash needs no operator step. Writes are acknowledged
//! only after durable publication; every write carries a request ID
//! (supplied by the client for safe retries, otherwise issued by the server
//! and returned).
mod catalog;
mod config;
mod error;
mod http;
mod metrics;
mod multi;
mod recovery;

pub use catalog::{Catalog, Collection, CreateCollection};
pub use config::{ServerConfig, Store, StoreConfig};
pub use http::{multi_router, multi_router_with_console, router, router_with_console};
pub use multi::Multi;
pub use recovery::stage_segmented_namespace;

use crate::{
    admission::{self, Client, Service, Shutdown},
    lease::{Keeper, Lease},
    segmented::{SegmentedDatabase, SegmentedServing},
    Error,
};
use std::sync::Arc;
use tokio::sync::Notify;

type Engine0 = SegmentedServing<Store>;

impl ServerConfig {
    /// Acquire the writer lease, take over the collection, run serial
    /// administrative work, then release the lease. Blocking; waits out a
    /// lease left by a dead writer and fails busy while one is renewed.
    pub fn with_engine<T>(
        &self,
        work: impl FnOnce(&mut SegmentedServing<Store>) -> crate::Result<T>,
    ) -> crate::Result<T> {
        let keeper = self.acquire_lease(|| {})?;
        let result = self.take_over().and_then(|mut engine| {
            let value = work(&mut engine)?;
            engine.close()?;
            Ok(value)
        });
        let released = keeper.release();
        let value = result?;
        released?;
        Ok(value)
    }

    /// Acquire the writer lease, take over the collection and start the
    /// admission worker with background lease renewal. Blocking.
    pub fn start(&self) -> crate::Result<Running> {
        let deposed = Arc::new(Notify::new());
        let keeper = self.acquire_lease({
            let deposed = deposed.clone();
            move || deposed.notify_one()
        })?;
        let service = self
            .take_over()
            .and_then(|engine| Service::start(engine, self.limits).map_err(database_error));
        match service {
            Ok(service) => Ok(Running {
                service,
                keeper,
                deposed,
            }),
            Err(error) => {
                // The lease only paces takeovers; if this release fails too,
                // it expires after its duration.
                let _ = keeper.release();
                Err(error)
            }
        }
    }

    /// Like [`ServerConfig::with_engine`], but on the database itself: the
    /// handle opens even when its clustered view is unavailable, so a
    /// conversion can rebuild the view. Fails if the work leaves the handle
    /// uncertain.
    pub fn with_database<T>(
        &self,
        work: impl FnOnce(&mut SegmentedDatabase<Store>) -> crate::Result<T>,
    ) -> crate::Result<T> {
        const ATTEMPTS: usize = 4;
        let keeper = self.acquire_lease(|| {})?;
        let mut attempt = 1;
        let opened = loop {
            match SegmentedDatabase::take_over_with_options(
                self.open_store()?,
                self.collection,
                self.options.clone(),
            ) {
                Err(Error::Exists(_)) if attempt < ATTEMPTS => attempt += 1,
                other => break other,
            }
        };
        let result = opened.and_then(|mut db| {
            let value = work(&mut db)?;
            if db.is_poisoned() {
                return Err(Error::RecoveryRequired);
            }
            Ok(value)
        });
        let released = keeper.release();
        let value = result?;
        released?;
        Ok(value)
    }

    fn acquire_lease(
        &self,
        on_deposed: impl FnOnce() + Send + 'static,
    ) -> crate::Result<Keeper<Store>> {
        let store = self.store.clone();
        Lease::acquire(move || store.open(), self.lease)?.keep(on_deposed)
    }

    /// Take over with fencing. A conflict means an earlier writer published
    /// during the takeover; each attempt re-lists through a fresh handle.
    fn take_over(&self) -> crate::Result<SegmentedServing<Store>> {
        const ATTEMPTS: usize = 4;
        let mut attempt = 1;
        loop {
            match SegmentedServing::open(
                self.open_store()?,
                self.collection,
                self.options.clone(),
                self.serving.clone(),
            ) {
                Err(Error::Exists(_)) if attempt < ATTEMPTS => attempt += 1,
                other => return other,
            }
        }
    }
}

fn database_error(error: admission::Error) -> Error {
    match error {
        admission::Error::Database(error) => error,
        other => Error::Invalid(other.to_string()),
    }
}

/// A started server: the admission worker over a taken-over engine, and the
/// background renewal of its writer lease.
pub struct Running {
    service: Service<Engine0>,
    keeper: Keeper<Store>,
    deposed: Arc<Notify>,
}

impl Running {
    pub fn client(&self) -> Client<Engine0> {
        self.service.client()
    }

    /// Notified once if lease renewal finds that another process took over;
    /// this server's writes are then fenced and it should stop.
    pub fn deposed(&self) -> Arc<Notify> {
        self.deposed.clone()
    }

    /// Stop the worker according to `mode`, then release the lease so the
    /// next process takes over without waiting. The lease is released even
    /// after a worker failure: the next takeover fences any late request.
    pub fn shutdown(self, mode: Shutdown) -> crate::Result<()> {
        let stopped = self.service.shutdown(mode).map_err(database_error);
        let released = self.keeper.release();
        stopped?;
        released
    }
}

/// Serve until SIGINT or SIGTERM, then drain queued work and release the
/// writer lease. Stops early, with an error, if another process takes over
/// (only possible after this process failed to renew for a full lease).
pub async fn run(config: ServerConfig) -> crate::Result<()> {
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    if config.multi {
        let setup = config.clone();
        let multi = tokio::task::spawn_blocking(move || Multi::new(setup))
            .await
            .map_err(|error| Error::Invalid(error.to_string()))??;
        let app = multi_router_with_console(multi.clone(), config.token.clone(), config.console);
        let sweeper = multi.clone();
        let sweep_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await; // Startup already swept the base.
            loop {
                interval.tick().await;
                if let Err(error) = sweeper.sweep().await {
                    eprintln!("collection orphan sweep failed: {error}");
                }
            }
        });
        let served = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let interrupt = tokio::signal::ctrl_c();
                #[cfg(unix)]
                {
                    let mut terminate =
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                            .expect("install SIGTERM handler");
                    tokio::select! { _ = interrupt => {}, _ = terminate.recv() => {} }
                }
                #[cfg(not(unix))]
                {
                    let _ = interrupt.await;
                }
            })
            .await;
        sweep_task.abort();
        served?;
        return multi.shutdown().await;
    }
    let running = tokio::task::block_in_place(|| config.start())?;
    let app = router_with_console(running.client(), config.token.clone(), config.console);
    let deposed = running.deposed();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let interrupt = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("install SIGTERM handler");
                tokio::select! {
                    _ = interrupt => {},
                    _ = terminate.recv() => {},
                    _ = deposed.notified() => {},
                }
            }
            #[cfg(not(unix))]
            tokio::select! {
                _ = interrupt => {},
                _ = deposed.notified() => {},
            }
        })
        .await?;
    tokio::task::block_in_place(|| running.shutdown(Shutdown::Drain))
}
