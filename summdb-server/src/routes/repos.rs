use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    routing::get,
};
use serde::{Deserialize, Serialize};
use summdb_core::{
    keys::{layer_key, manifest_prefix, tag_prefix},
    types::{Digest, LayerRecord, ManifestRecord},
};

use crate::{error::AppError, state::AppState};

#[derive(Serialize)]
pub struct RepoStats {
    pub repo: String,
    pub manifests: usize,
    pub tags: usize,
    pub size: u64,
}

#[derive(Deserialize)]
pub struct LayersQuery {
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Serialize, Clone)]
pub struct RepoLayerRow {
    pub digest: Digest,
    pub size: u64,
    pub manifest_count: usize,
    pub tags: Vec<String>,
}

#[derive(Serialize)]
pub struct RepoLayersResponse {
    pub by_size: Vec<RepoLayerRow>,
    pub by_manifest_count: Vec<RepoLayerRow>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/repos", get(list_repos))
        .route("/v1/repos/:repo/stats", get(get_stats))
        .route("/v1/repos/:repo/layers", get(get_layers))
}

async fn list_repos(State(state): State<AppState>) -> Json<Vec<String>> {
    let mut repos = state.interner.all_repos();
    repos.sort_unstable();
    Json(repos)
}

async fn get_stats(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Json<RepoStats>, AppError> {
    let repo_id = match state.interner.try_lookup(&repo) {
        Some(id) => id,
        None => {
            return Ok(Json(RepoStats {
                repo,
                manifests: 0,
                tags: 0,
                size: 0,
            }));
        }
    };
    let manifest_entries = state.storage.scan_prefix(&manifest_prefix(repo_id))?;
    let tags = state.storage.scan_prefix(&tag_prefix(repo_id))?.len();

    // Sum top-level manifests only (no parent) to avoid double-counting layers
    // shared between a manifest list and its per-platform children.
    let mut size: u64 = 0;
    for (_, v) in &manifest_entries {
        let record: ManifestRecord = postcard::from_bytes(v)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        if record.parent.is_none() {
            size = size.saturating_add(record.total_layer_size);
        }
    }

    Ok(Json(RepoStats {
        repo,
        manifests: manifest_entries.len(),
        tags,
        size,
    }))
}

async fn get_layers(
    State(state): State<AppState>,
    Path(repo): Path<String>,
    Query(params): Query<LayersQuery>,
) -> Result<Json<RepoLayersResponse>, AppError> {
    let limit = params.limit.unwrap_or(20);
    let repo_id = match state.interner.try_lookup(&repo) {
        Some(id) => id,
        None => {
            return Ok(Json(RepoLayersResponse {
                by_size: vec![],
                by_manifest_count: vec![],
            }));
        }
    };

    // Load every manifest in this repo, keyed by digest for in-memory parent lookups.
    let entries = state.storage.scan_prefix(&manifest_prefix(repo_id))?;
    let mut by_digest: HashMap<Digest, ManifestRecord> = HashMap::with_capacity(entries.len());
    for (_, v) in &entries {
        let record: ManifestRecord = postcard::from_bytes(v)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        by_digest.insert(record.digest, record);
    }

    // For each unique layer in the repo, collect referencing manifests and their tags
    // (falling back to the parent index's tags when a child manifest has none of its own).
    let mut layer_map: HashMap<Digest, (HashSet<Digest>, HashSet<String>)> = HashMap::new();
    for record in by_digest.values() {
        let effective_tags: Vec<String> = if !record.tags.is_empty() {
            record.tags.clone()
        } else if let Some(p) = record.parent.as_ref().filter(|p| p.repo == repo_id) {
            by_digest
                .get(&p.digest)
                .map(|pr| pr.tags.clone())
                .unwrap_or_default()
        } else {
            vec![]
        };
        for layer in &record.layers {
            let entry = layer_map
                .entry(*layer)
                .or_insert_with(|| (HashSet::new(), HashSet::new()));
            entry.0.insert(record.digest);
            for t in &effective_tags {
                entry.1.insert(t.clone());
            }
        }
    }

    // Resolve each unique layer's size (one read per layer).
    let mut rows: Vec<RepoLayerRow> = Vec::with_capacity(layer_map.len());
    for (digest, (mds, tags)) in layer_map {
        let size = match state.storage.get(&layer_key(&digest))? {
            Some(b) => postcard::from_bytes::<LayerRecord>(&b)
                .map_err(|e| AppError::Internal(e.to_string()))?
                .size,
            None => 0,
        };
        let mut tags_vec: Vec<String> = tags.into_iter().collect();
        tags_vec.sort();
        rows.push(RepoLayerRow {
            digest,
            size,
            manifest_count: mds.len(),
            tags: tags_vec,
        });
    }

    let mut by_size = rows.clone();
    by_size.sort_by(|a, b| b.size.cmp(&a.size).then(b.manifest_count.cmp(&a.manifest_count)));
    by_size.truncate(limit);

    let mut by_manifest_count = rows;
    by_manifest_count
        .sort_by(|a, b| b.manifest_count.cmp(&a.manifest_count).then(b.size.cmp(&a.size)));
    by_manifest_count.truncate(limit);

    Ok(Json(RepoLayersResponse {
        by_size,
        by_manifest_count,
    }))
}
