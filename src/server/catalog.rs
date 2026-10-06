//! Versioned collection catalog. One immutable object is the existence authority.
use super::StoreConfig;
use crate::{store::ObjectStore, Config, Error, Metric, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Collection {
    pub version: u8,
    pub name: String,
    pub dimensions: usize,
    pub metric: Metric,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_filter: Option<(String, String)>,
    #[serde(default)]
    pub routed_keys: Vec<String>,
    pub generation: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCollection {
    pub name: String,
    pub dimensions: usize,
    #[serde(default = "default_metric")]
    pub metric: Metric,
    #[serde(default)]
    pub resident_filter: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub routed_keys: Vec<String>,
}
fn default_metric() -> Metric {
    Metric::SquaredEuclidean
}

fn check_name(name: &str) -> Result<()> {
    if name.len() > 63
        || name.is_empty()
        || !name.as_bytes()[0].is_ascii_lowercase() && !name.as_bytes()[0].is_ascii_digit()
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        // Names are object keys, which allow only ASCII alphanumerics and '-'.
        return Err(Error::Invalid(
            "collection name must match ^[a-z0-9][a-z0-9-]{0,62}$".into(),
        ));
    }
    Ok(())
}

impl CreateCollection {
    fn normalize(mut self) -> Result<Collection> {
        check_name(&self.name)?;
        if self.dimensions == 0 {
            return Err(Error::Invalid("dimensions must be positive".into()));
        }
        if self.routed_keys.len() > 4 || self.routed_keys.iter().any(String::is_empty) {
            return Err(Error::Invalid(
                "routed_keys needs at most four nonempty keys".into(),
            ));
        }
        self.routed_keys.sort();
        self.routed_keys.dedup();
        let resident_filter = self
            .resident_filter
            .map(|filter| {
                if filter.len() != 1 {
                    return Err(Error::Invalid(
                        "resident_filter needs one key/value pair".into(),
                    ));
                }
                Ok(filter.into_iter().next().unwrap())
            })
            .transpose()?;
        Ok(Collection {
            version: 1,
            name: self.name,
            dimensions: self.dimensions,
            metric: self.metric,
            resident_filter,
            routed_keys: self.routed_keys,
            generation: random_generation()?,
        })
    }
}

fn random_generation() -> Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::getrandom(&mut bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

impl Collection {
    pub fn config(&self) -> Config {
        Config {
            dimensions: self.dimensions,
            metric: self.metric,
        }
    }
    pub fn options(&self) -> crate::segmented::SegmentedOptions {
        crate::segmented::SegmentedOptions {
            resident_filter: self.resident_filter.clone(),
            routed_keys: self.routed_keys.clone(),
        }
    }
    pub fn description(&self, open: bool) -> serde_json::Value {
        serde_json::json!({"name":self.name,"dimensions":self.dimensions,"metric":self.metric,
            "resident_filter":self.resident_filter.as_ref().map(|(key,value)| BTreeMap::from([(key.clone(),value.clone())])),
            "routed_keys":self.routed_keys,"open":open})
    }
    fn validate(&self, key: &str) -> Result<()> {
        check_name(&self.name)?;
        if self.name != key
            || self.version != 1
            || self.dimensions == 0
            || self.generation.len() != 32
            || !self
                .generation
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Error::Corrupt(format!("invalid catalog object: {key}")));
        }
        Ok(())
    }
}

pub struct Catalog {
    base: StoreConfig,
}
/// Full state at an immutable per-name sequence. Conditional create of the
/// next slot is the compare-and-append decision, including deletion.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogState {
    version: u8,
    sequence: u64,
    collection: Option<Collection>,
}

fn state_key(sequence: u64) -> String {
    format!("state-{sequence:020}")
}

impl Catalog {
    pub fn new(base: StoreConfig) -> Self {
        Self { base }
    }
    fn store(&self) -> Result<super::Store> {
        self.base.child("catalog").open()
    }
    fn history(&self, name: &str) -> Result<super::Store> {
        self.base.child(&format!("catalog-history/{name}")).open()
    }
    fn read_state(&self, name: &str, initial: Collection) -> Result<(Option<Collection>, u64)> {
        let history = self.history(name)?;
        let mut keys = history.list()?;
        keys.sort();
        for (index, key) in keys.iter().enumerate() {
            if key != &state_key(index as u64 + 1) {
                return Err(Error::Corrupt(format!("invalid catalog history: {name}")));
            }
        }
        let sequence = keys.len() as u64;
        let Some(key) = keys.last() else {
            return Ok((Some(initial), 0));
        };
        let bytes = history
            .get(key)?
            .ok_or_else(|| Error::Corrupt(format!("missing catalog state: {name}")))?;
        let state: CatalogState = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Corrupt(format!("catalog history {name}: {error}")))?;
        if state.version != 1 || state.sequence != sequence {
            return Err(Error::Corrupt(format!("invalid catalog state: {name}")));
        }
        if let Some(value) = &state.collection {
            value.validate(name)?;
        }
        Ok((state.collection, sequence))
    }
    fn observed(&self, name: &str) -> Result<(Option<Collection>, u64)> {
        check_name(name)?;
        let Some(bytes) = self.store()?.get(name)? else {
            return Ok((None, 0));
        };
        let initial: Collection = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Corrupt(format!("catalog {name}: {error}")))?;
        initial.validate(name)?;
        self.read_state(name, initial)
    }
    fn append(&self, name: &str, sequence: u64, collection: Option<Collection>) -> Result<()> {
        let sequence = sequence
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("catalog sequence exhausted".into()))?;
        let bytes = serde_json::to_vec(&CatalogState {
            version: 1,
            sequence,
            collection,
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
        self.history(name)?.create(&state_key(sequence), &bytes)
    }
    pub fn get(&self, name: &str) -> Result<Option<Collection>> {
        self.observed(name).map(|(value, _)| value)
    }
    pub fn list(&self) -> Result<Vec<Collection>> {
        let store = self.store()?;
        let keys = store.list()?;
        // S3's batched reads issue up to 32 GETs concurrently and preserve
        // input order. The local store uses the same interface.
        let values = store.get_many(&keys)?;
        let mut result = Vec::new();
        for (key, bytes) in keys.into_iter().zip(values) {
            let bytes =
                bytes.ok_or_else(|| Error::Corrupt(format!("catalog disappeared: {key}")))?;
            let value: Collection = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Corrupt(format!("catalog {key}: {error}")))?;
            value.validate(&key)?;
            if let Some(current) = self.read_state(&key, value)?.0 {
                result.push(current);
            }
        }
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }
    pub fn create(&self, request: CreateCollection) -> Result<(Collection, bool)> {
        let value = request.normalize()?;
        let (existing, sequence) = self.observed(&value.name)?;
        if let Some(existing) = existing {
            return same_configuration(existing, &value);
        }
        if sequence > 0 {
            return match self.append(&value.name, sequence, Some(value.clone())) {
                Ok(()) => Ok((value, true)),
                Err(Error::Exists(_)) => same_configuration(
                    self.get(&value.name)?.ok_or(Error::RequestConflict)?,
                    &value,
                ),
                Err(error) => Err(error),
            };
        }
        let bytes =
            serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))?;
        match self.store()?.create(&value.name, &bytes) {
            Ok(()) => Ok((value, true)),
            Err(Error::Exists(_)) => {
                let existing = self.get(&value.name)?.ok_or(Error::RecoveryRequired)?;
                same_configuration(existing, &value)
            }
            Err(error) => Err(error),
        }
    }
    /// The next immutable state is the authoritative deletion point.
    pub fn delete(&self, value: &Collection) -> Result<()> {
        let (current, sequence) = self.observed(&value.name)?;
        if current.as_ref() != Some(value) {
            return Err(Error::RequestConflict);
        }
        match self.append(&value.name, sequence, None) {
            Err(Error::Exists(_)) => Err(Error::RequestConflict),
            result => result,
        }
    }
    pub fn data_store(&self, value: &Collection) -> StoreConfig {
        self.base
            .child(&format!("data/{}/{}", value.name, value.generation))
    }
    pub fn remove_data(&self, value: &Collection) -> Result<()> {
        let store = self.data_store(value).open()?;
        store.remove_many(&store.list()?)
    }
    /// Reclaim generations with no matching authoritative catalog record.
    pub fn sweep(&self) -> Result<()> {
        let live = self
            .list()?
            .into_iter()
            .map(|value| (value.name, value.generation))
            .collect::<std::collections::BTreeSet<_>>();
        for (name, generation) in self.base.data_generations()? {
            check_name(&name)?;
            if generation.len() != 32
                || !generation
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(Error::Corrupt(format!(
                    "invalid data generation: {name}/{generation}"
                )));
            }
            if !live.contains(&(name.clone(), generation.clone())) {
                if self
                    .get(&name)?
                    .is_some_and(|value| value.generation == generation)
                {
                    continue;
                }
                let store = self
                    .base
                    .child(&format!("data/{name}/{generation}"))
                    .open()?;
                store.remove_many(&store.list()?)?;
            }
        }
        Ok(())
    }
}

fn same_configuration(existing: Collection, requested: &Collection) -> Result<(Collection, bool)> {
    if existing.dimensions == requested.dimensions
        && existing.metric == requested.metric
        && existing.resident_filter == requested.resident_filter
        && existing.routed_keys == requested.routed_keys
    {
        Ok((existing, false))
    } else {
        Err(Error::RequestConflict)
    }
}
