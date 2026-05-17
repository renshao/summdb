use summdb_core::{
    error::{Result, SummError},
    keys::{manifest_key, tag_key},
    types::{Digest, ManifestRecord, RepoId},
};

use crate::StorageEngine;

/// Set the tag `T:{repo}:{tag} → new_digest`, and reconcile both manifests'
/// `tags` lists: drop `tag` from the previous target (if any) and add it to
/// the new target.
pub fn set_tag(
    storage: &dyn StorageEngine,
    repo: RepoId,
    tag: &str,
    new_digest: &Digest,
) -> Result<()> {
    let key = tag_key(repo, tag);
    let old_digest = match storage.get(&key)? {
        Some(b) => Some(Digest::from_slice(&b).map_err(SummError::InvalidData)?),
        None => None,
    };

    if let Some(old) = &old_digest {
        if old != new_digest {
            remove_tag_from_manifest(storage, repo, old, tag)?;
        }
    }

    storage.put(&key, new_digest.as_bytes())?;

    if old_digest.as_ref() != Some(new_digest) {
        add_tag_to_manifest(storage, repo, new_digest, tag)?;
    }

    Ok(())
}

fn add_tag_to_manifest(
    storage: &dyn StorageEngine,
    repo: RepoId,
    digest: &Digest,
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
    repo: RepoId,
    digest: &Digest,
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
