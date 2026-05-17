use summdb_core::{
    error::{Result, SummError},
    keys::{manifest_key, tag_key},
    types::ManifestRecord,
};

use crate::StorageEngine;

/// Set `T:{repo}:{tag} → new_digest`, and reconcile the affected manifests'
/// `tags` lists: remove `tag` from the old target's list (if any) and add it
/// to the new target's list.
pub fn set_tag(
    storage: &dyn StorageEngine,
    repo: &str,
    tag: &str,
    new_digest: &str,
) -> Result<()> {
    let key = tag_key(repo, tag);
    let old_digest = match storage.get(&key)? {
        Some(b) => Some(
            String::from_utf8(b).map_err(|e| SummError::InvalidData(e.to_string()))?,
        ),
        None => None,
    };

    if let Some(old) = &old_digest {
        if old != new_digest {
            remove_tag_from_manifest(storage, repo, old, tag)?;
        }
    }

    storage.put(&key, new_digest.as_bytes())?;

    if old_digest.as_deref() != Some(new_digest) {
        add_tag_to_manifest(storage, repo, new_digest, tag)?;
    }

    Ok(())
}

fn add_tag_to_manifest(
    storage: &dyn StorageEngine,
    repo: &str,
    digest: &str,
    tag: &str,
) -> Result<()> {
    let key = manifest_key(repo, digest);
    if storage.get(&key)?.is_none() {
        return Ok(());
    }
    let tag = tag.to_string();
    storage.merge(
        &key,
        Box::new(move |current| {
            let bytes = current.unwrap_or_default();
            let mut record: ManifestRecord = postcard::from_bytes(&bytes)
                .map_err(|e| SummError::InvalidData(e.to_string()))?;
            if !record.tags.contains(&tag) {
                record.tags.push(tag);
            }
            postcard::to_allocvec(&record)
                .map_err(|e| SummError::InvalidData(e.to_string()))
        }),
    )
}

fn remove_tag_from_manifest(
    storage: &dyn StorageEngine,
    repo: &str,
    digest: &str,
    tag: &str,
) -> Result<()> {
    let key = manifest_key(repo, digest);
    if storage.get(&key)?.is_none() {
        return Ok(());
    }
    let tag = tag.to_string();
    storage.merge(
        &key,
        Box::new(move |current| {
            let bytes = current.unwrap_or_default();
            let mut record: ManifestRecord = postcard::from_bytes(&bytes)
                .map_err(|e| SummError::InvalidData(e.to_string()))?;
            record.tags.retain(|t| t != &tag);
            postcard::to_allocvec(&record)
                .map_err(|e| SummError::InvalidData(e.to_string()))
        }),
    )
}
