use super::Store;
use crate::{
    lease::is_lease_key,
    ownership::is_control_key,
    segmented::{SegmentedDatabase, SegmentedOptions},
    store::ObjectStore,
    Config, Error,
};

/// Copy a stopped segmented namespace or backup into a fresh prefix. Metadata
/// publishes last; a failed destination must be discarded. The caller must
/// prove the source writer has stopped before staging a crashed namespace.
pub fn stage_segmented_namespace(
    source: &Store,
    destination: Store,
    config: Config,
    options: SegmentedOptions,
) -> crate::Result<(u64, usize, u64)> {
    if !destination.list()?.is_empty() {
        return Err(Error::Invalid("restore destination must be empty".into()));
    }
    let mut keys = source.list()?;
    keys.sort();
    if keys.windows(2).any(|pair| pair[0] == pair[1])
        || keys.iter().filter(|key| *key == "metadata").count() != 1
    {
        return Err(Error::Corrupt(
            "source listing has duplicate keys or no metadata".into(),
        ));
    }
    let mut copied = 0;
    let mut bytes = 0;
    for key in keys.iter().filter(|key| *key != "metadata") {
        if is_control_key(key) || is_lease_key(key) {
            continue;
        }
        let payload = source
            .get(key)?
            .ok_or_else(|| Error::Corrupt(format!("listed source object missing: {key}")))?;
        destination.create(key, &payload)?;
        copied += 1;
        bytes += payload.len() as u64;
    }
    let metadata = source
        .get("metadata")?
        .ok_or_else(|| Error::Corrupt("listed source metadata missing".into()))?;
    destination.create("metadata", &metadata)?;
    copied += 1;
    bytes += metadata.len() as u64;
    let restored = SegmentedDatabase::open_with_options(destination, config, options)?;
    Ok((restored.sequence(), copied, bytes))
}
