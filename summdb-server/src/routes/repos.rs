use axum::{
    Json, Router,
    extract::{Path, State},
    routing::get,
};
use serde::Serialize;
use summdb_core::{
    keys::{manifest_prefix, tag_prefix},
    types::ManifestRecord,
};

use crate::{error::AppError, state::AppState};

#[derive(Serialize)]
pub struct RepoStats {
    pub repo: String,
    pub manifests: usize,
    pub tags: usize,
    pub size: u64,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/repos", get(list_repos))
        .route("/v1/repos/:repo/stats", get(get_stats))
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
