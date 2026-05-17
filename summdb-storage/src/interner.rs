use std::collections::HashMap;

use parking_lot::RwLock;
use summdb_core::{
    error::{Result, SummError},
    keys::{
        parse_repo_intern_reverse_key, repo_intern_forward_key, repo_intern_reverse_key,
        repo_intern_reverse_prefix,
    },
    types::RepoId,
};

use crate::StorageEngine;

pub struct RepoInterner {
    inner: RwLock<Inner>,
}

struct Inner {
    forward: HashMap<String, RepoId>,
    reverse: Vec<String>,
}

impl RepoInterner {
    /// Build the in-memory maps from the IR: keyspace.
    pub fn load(engine: &dyn StorageEngine) -> Result<Self> {
        let entries = engine.scan_prefix(&repo_intern_reverse_prefix())?;
        let mut by_id: Vec<(RepoId, String)> = entries
            .into_iter()
            .filter_map(|(k, v)| {
                let id = parse_repo_intern_reverse_key(&k)?;
                let repo = String::from_utf8(v).ok()?;
                Some((id, repo))
            })
            .collect();
        by_id.sort_by_key(|(id, _)| *id);

        let mut forward = HashMap::with_capacity(by_id.len());
        let mut reverse = Vec::with_capacity(by_id.len());
        for (id, repo) in by_id {
            while (reverse.len() as RepoId) < id {
                reverse.push(String::new());
            }
            if (reverse.len() as RepoId) == id {
                forward.insert(repo.clone(), id);
                reverse.push(repo);
            }
        }
        Ok(Self {
            inner: RwLock::new(Inner { forward, reverse }),
        })
    }

    pub fn resolve(&self, id: RepoId) -> Option<String> {
        let inner = self.inner.read();
        inner.reverse.get(id as usize).cloned()
    }

    pub fn try_lookup(&self, repo: &str) -> Option<RepoId> {
        let inner = self.inner.read();
        inner.forward.get(repo).copied()
    }

    /// Look up or allocate an id for the given repo. Writes the (forward, reverse)
    /// key pair to the engine before updating the in-memory maps, so a crash never
    /// leaves the cache out of sync with what's persisted.
    pub fn intern(&self, engine: &dyn StorageEngine, repo: &str) -> Result<RepoId> {
        if let Some(id) = self.try_lookup(repo) {
            return Ok(id);
        }
        let mut inner = self.inner.write();
        if let Some(&id) = inner.forward.get(repo) {
            return Ok(id);
        }
        let id = inner.reverse.len() as RepoId;
        if id == RepoId::MAX {
            return Err(SummError::InvalidData("repo interner exhausted".into()));
        }
        engine.put(&repo_intern_forward_key(repo), &id.to_be_bytes())?;
        engine.put(&repo_intern_reverse_key(id), repo.as_bytes())?;
        inner.forward.insert(repo.to_string(), id);
        inner.reverse.push(repo.to_string());
        Ok(id)
    }
}
