//! Renewed writer lease: decides *when* another process may take over a
//! segmented namespace, never *whether* doing so is safe.
//!
//! The holder publishes numbered immutable `sglease-{n:020}` objects, a new
//! one every third of its declared duration, and removes older ones. A
//! process that wants the namespace observes the newest lease; unless it is
//! a release marker, it waits the holder's declared duration and looks again.
//! If no newer lease appeared, it publishes the next number with a
//! conditional create; exactly one of several concurrent takers can win that
//! number, and a holder that finds a number above its own has been deposed.
//! Waiting uses only the taker's own monotonic sleep, so no clocks are
//! compared. A wrong guess costs availability only: correctness comes from
//! the fences of [`crate::segmented::SegmentedDatabase::take_over`], which
//! hold even when two processes both believe they hold the lease.
use crate::{store::ObjectStore, Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

const PREFIX: &str = "sglease-";
/// Longest accepted lease. It bounds how long a taker waits on a lease
/// declared by another process.
pub const MAX_DURATION: Duration = Duration::from_secs(3600);

/// Lease object payload, version 1.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    /// Random token of the holding process; 32 lowercase hex digits.
    holder: String,
    duration_ms: u64,
    /// A release marker: the holder stopped and a taker need not wait.
    released: bool,
}

fn lease_key(number: u64) -> String {
    format!("{PREFIX}{number:020}")
}

fn number(key: &str) -> Result<u64> {
    key.strip_prefix(PREFIX)
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .filter(|&number| number > 0 && key == lease_key(number))
        .ok_or_else(|| Error::Corrupt(format!("invalid lease key: {key}")))
}

/// Lease objects are control state, never part of a database's contents.
pub fn is_lease_key(key: &str) -> bool {
    key.starts_with(PREFIX)
}

fn token() -> Result<String> {
    let mut nonce = [0_u8; 16];
    getrandom::getrandom(&mut nonce).map_err(|error| {
        Error::Io(std::io::Error::other(format!(
            "OS randomness unavailable: {error}"
        )))
    })?;
    Ok(nonce.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn decode(bytes: &[u8]) -> Result<Record> {
    let record: Record = serde_json::from_slice(bytes)
        .map_err(|error| Error::Corrupt(format!("invalid lease: {error}")))?;
    if record.version != 1
        || record.holder.len() != 32
        || !record
            .holder
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || record.duration_ms == 0
        || record.duration_ms > MAX_DURATION.as_millis() as u64
    {
        return Err(Error::Corrupt("invalid lease record".into()));
    }
    Ok(record)
}

fn deposed() -> Error {
    Error::Busy("deposed: another process took over the lease".into())
}

type Opener<S> = Box<dyn Fn() -> Result<S> + Send>;

/// A held lease. Renew it at least every third of its duration (see
/// [`Lease::keep`]) and release it on graceful shutdown.
pub struct Lease<S> {
    open: Opener<S>,
    /// Discarded after any error, since a backend may reject an uncertain
    /// handle until it is reopened.
    store: Option<S>,
    holder: String,
    number: u64,
    duration: Duration,
}

impl<S: ObjectStore> Lease<S> {
    /// Acquire the namespace's lease, waiting up to the current holder's
    /// declared duration. `open` returns a fresh store handle for the same
    /// namespace. Returns [`Error::Busy`] if the holder renewed while this
    /// call watched, or if another process acquired the lease first.
    pub fn acquire(
        open: impl Fn() -> Result<S> + Send + 'static,
        duration: Duration,
    ) -> Result<Self> {
        if duration < Duration::from_millis(1) || duration > MAX_DURATION {
            return Err(Error::Invalid(
                "lease duration must be between one millisecond and one hour".into(),
            ));
        }
        let mut lease = Self {
            open: Box::new(open),
            store: None,
            holder: token()?,
            number: 0,
            duration,
        };
        let mut newest = lease.observe()?;
        if let Some((number, record)) = &newest {
            let record = record
                .as_ref()
                .ok_or_else(|| Error::Busy("the lease holder is renewing".into()))?;
            if !record.released {
                std::thread::sleep(Duration::from_millis(record.duration_ms));
                let current = lease.observe()?;
                let expired = match &current {
                    Some((current_number, Some(current_record))) => {
                        current_number == number || current_record.released
                    }
                    Some((_, None)) => false,
                    None => true,
                };
                if !expired {
                    return Err(Error::Busy("the lease holder renewed its lease".into()));
                }
                newest = current.or(newest);
            }
        }
        let next = newest
            .map_or(Some(1), |(number, _)| number.checked_add(1))
            .ok_or_else(|| Error::Invalid("lease numbers exhausted".into()))?;
        match lease.publish(next, false) {
            Err(Error::Exists(_)) => {
                return Err(Error::Busy("another process acquired the lease".into()))
            }
            other => other?,
        }
        // Superseded leases are removed by the first renewal or release.
        lease.number = next;
        Ok(lease)
    }

    /// The current lease number; it increases with every renewal.
    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }

    /// Publish the next lease number, confirm that no newer one exists and
    /// remove older ones. [`Error::Busy`] means another process took over
    /// the lease: this holder is deposed and its writes are fenced. Other
    /// errors are uncertain; renewing again resolves them.
    pub fn renew(&mut self) -> Result<()> {
        self.advance(false)?;
        self.settle()
    }

    /// Publish a release marker so the next taker need not wait, then remove
    /// older leases. Call only after this process stopped issuing storage
    /// requests for the namespace. A late request it already issued remains
    /// harmless: the next takeover fences it.
    pub fn release(mut self) -> Result<()> {
        self.advance(true)?;
        self.settle()
    }

    /// Renew in a background thread every third of the duration until the
    /// returned keeper is released or dropped. `on_deposed` runs once if a
    /// renewal finds the lease taken over.
    pub fn keep(self, on_deposed: impl FnOnce() + Send + 'static) -> Result<Keeper<S>>
    where
        S: Send + 'static,
    {
        let (stop, stopped) = mpsc::channel::<()>();
        let deposed = Arc::new(AtomicBool::new(false));
        let errors = Arc::new(AtomicU64::new(0));
        let interval = self.duration / 3;
        let mut lease = self;
        let thread = {
            let deposed = deposed.clone();
            let errors = errors.clone();
            std::thread::Builder::new()
                .name("glider-lease".into())
                .spawn(move || {
                    while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(interval)
                    {
                        match lease.renew() {
                            Ok(()) => {}
                            Err(Error::Busy(_)) => {
                                deposed.store(true, Ordering::Release);
                                on_deposed();
                                break;
                            }
                            // Another process waits a full duration without
                            // a renewal before taking over; retry next time.
                            Err(_) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    lease
                })
                .map_err(Error::Io)?
        };
        Ok(Keeper {
            stop: Some(stop),
            thread: Some(thread),
            deposed,
            errors,
        })
    }

    fn store(&mut self) -> Result<&mut S> {
        if self.store.is_none() {
            self.store = Some((self.open)()?);
        }
        Ok(self.store.as_mut().expect("store opened above"))
    }

    /// Run one storage operation, discarding the handle after an error.
    fn with_store<T>(&mut self, operation: impl FnOnce(&mut S) -> Result<T>) -> Result<T> {
        let result = operation(self.store()?);
        if result.is_err() {
            self.store = None;
        }
        result
    }

    fn listed(&mut self) -> Result<Vec<u64>> {
        self.with_store(|store| store.list())?
            .iter()
            .filter(|key| is_lease_key(key))
            .map(|key| number(key))
            .collect()
    }

    /// The newest lease number and its record; `None` as the record means
    /// it was removed after the listing, i.e. its holder renewed.
    fn observe(&mut self) -> Result<Option<(u64, Option<Record>)>> {
        let Some(newest) = self.listed()?.into_iter().max() else {
            return Ok(None);
        };
        let record = self
            .with_store(|store| store.get(&lease_key(newest)))?
            .map(|bytes| decode(&bytes))
            .transpose()?;
        Ok(Some((newest, record)))
    }

    fn publish(&mut self, number: u64, released: bool) -> Result<()> {
        let bytes = serde_json::to_vec(&Record {
            version: 1,
            holder: self.holder.clone(),
            duration_ms: self.duration.as_millis() as u64,
            released,
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        self.with_store(|store| store.create(&lease_key(number), &bytes))
    }

    /// Publish the next number. An existing object there is ours only if an
    /// earlier attempt's response was lost; then continue after it.
    fn advance(&mut self, released: bool) -> Result<()> {
        loop {
            let next = self
                .number
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("lease numbers exhausted".into()))?;
            let landed = match self.publish(next, released) {
                Ok(()) => released,
                Err(Error::Exists(_)) => {
                    // A holder never removes its newest number, so a removed
                    // one was superseded by a later lease, not ours.
                    let bytes = self
                        .with_store(|store| store.get(&lease_key(next)))?
                        .ok_or_else(deposed)?;
                    let record = decode(&bytes)?;
                    if record.holder != self.holder {
                        return Err(deposed());
                    }
                    record.released
                }
                Err(error) => return Err(error),
            };
            self.number = next;
            if landed == released {
                return Ok(());
            }
        }
    }

    /// After publishing: a number above ours means another process took
    /// over (a resumed holder can publish into the gap a newer holder left
    /// by removing old numbers), so we are deposed. Otherwise remove every
    /// older number, including a deposed holder's.
    fn settle(&mut self) -> Result<()> {
        let listed = self.listed()?;
        if listed.iter().any(|&number| number > self.number) {
            return Err(deposed());
        }
        for number in listed {
            if number < self.number {
                self.with_store(|store| store.remove(&lease_key(number)))?;
            }
        }
        Ok(())
    }
}

/// Background renewal of a [`Lease`]. Dropping it stops renewing without
/// releasing, so the lease expires after its duration.
pub struct Keeper<S> {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<JoinHandle<Lease<S>>>,
    deposed: Arc<AtomicBool>,
    errors: Arc<AtomicU64>,
}

impl<S: ObjectStore> Keeper<S> {
    /// Whether a renewal found the lease taken over by another process.
    pub fn is_deposed(&self) -> bool {
        self.deposed.load(Ordering::Acquire)
    }

    /// Failed renewal attempts, each retried at the next interval.
    pub fn renewal_errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Stop renewing and release the lease; see [`Lease::release`].
    pub fn release(mut self) -> Result<()> {
        drop(self.stop.take());
        let lease = self
            .thread
            .take()
            .expect("keeper owns its thread until release")
            .join()
            .map_err(|_| Error::Io(std::io::Error::other("lease renewal thread panicked")))?;
        if self.is_deposed() {
            return Err(deposed());
        }
        lease.release()
    }
}

impl<S> Drop for Keeper<S> {
    fn drop(&mut self) {
        // Closing the channel ends the renewal loop at its next wakeup.
        drop(self.stop.take());
    }
}
