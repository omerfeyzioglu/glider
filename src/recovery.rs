//! Stage a frozen namespace listing in a fresh prefix after an uncertain writer
//! crash. Late requests against the old prefix cannot change the staged copy.
use crate::{ownership::is_control_key, store::ObjectStore, Config, Database, Error, Result};

/// Copy complete objects from one stopped writer's prefix into a *new, empty,
/// unadvertised* prefix. The selected listing is frozen before transfer;
/// ownership control objects are not copied. Metadata is published last, so an
/// interruption before that publication leaves data objects without a database
/// root. A successful return has validated the selected root and mutation tail.
///
/// The source process must be stopped. A late in-flight request may still alter
/// the old prefix; it cannot alter the destination. Do not expose the destination
/// to clients until this function succeeds and a new `OwnedDatabase` claim is
/// established there. A failed destination prefix
/// is never reused. This operation does not repair external loss of an
/// acknowledged object in the source.
pub fn stage_isolated_namespace<S: ObjectStore, D: ObjectStore>(
    source: &S,
    mut destination: D,
    config: Config,
) -> Result<()> {
    config.validate()?;
    if !destination.list()?.is_empty() {
        return Err(Error::Invalid(
            "restore destination namespace must be empty".into(),
        ));
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
    for key in keys.iter().filter(|key| *key != "metadata") {
        if is_control_key(key) {
            continue;
        }
        let bytes = source
            .get(key)?
            .ok_or_else(|| Error::Corrupt(format!("listed source object missing: {key}")))?;
        destination.create(key, &bytes)?;
    }
    let metadata = source
        .get("metadata")?
        .ok_or_else(|| Error::Corrupt("listed source metadata missing".into()))?;
    destination.create("metadata", &metadata)?;
    drop(Database::open(destination, config)?);
    Ok(())
}
