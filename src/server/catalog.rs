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
impl Catalog {
    pub fn new(base: StoreConfig) -> Self {
        Self { base }
    }
    fn store(&self) -> Result<super::Store> {
        self.base.child("catalog").open()
    }
    pub fn get(&self, name: &str) -> Result<Option<Collection>> {
        check_name(name)?;
        let Some(bytes) = self.store()?.get(name)? else {
            return Ok(None);
        };
        let value: Collection = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Corrupt(format!("catalog {name}: {error}")))?;
        value.validate(name)?;
        Ok(Some(value))
    }
    pub fn list(&self) -> Result<Vec<Collection>> {
        let store = self.store()?;
        let mut result = Vec::new();
        for key in store.list()? {
            let bytes = store
                .get(&key)?
                .ok_or_else(|| Error::Corrupt(format!("catalog disappeared: {key}")))?;
            let value: Collection = serde_json::from_slice(&bytes)
                .map_err(|error| Error::Corrupt(format!("catalog {key}: {error}")))?;
            value.validate(&key)?;
            result.push(value);
        }
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }
    pub fn create(&self, request: CreateCollection) -> Result<(Collection, bool)> {
        let value = request.normalize()?;
        let bytes =
            serde_json::to_vec(&value).map_err(|error| Error::Invalid(error.to_string()))?;
        match self.store()?.create(&value.name, &bytes) {
            Ok(()) => Ok((value, true)),
            Err(Error::Exists(_)) => {
                let existing = self.get(&value.name)?.ok_or(Error::RecoveryRequired)?;
                if existing.dimensions == value.dimensions
                    && existing.metric == value.metric
                    && existing.resident_filter == value.resident_filter
                    && existing.routed_keys == value.routed_keys
                {
                    Ok((existing, false))
                } else {
                    Err(Error::RequestConflict)
                }
            }
            Err(error) => Err(error),
        }
    }
    /// Removing the catalog record is the authoritative deletion point.
    pub fn delete(&self, value: &Collection) -> Result<()> {
        if self.get(&value.name)?.as_ref() != Some(value) {
            return Err(Error::RequestConflict);
        }
        self.store()?.remove(&value.name)
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
