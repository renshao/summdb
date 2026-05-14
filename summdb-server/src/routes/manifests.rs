use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use serde::Deserialize;
use summdb_core::{
    keys::{manifest_key, manifest_prefix},
    types::{ManifestRecord, Platform},
};

use crate::{error::AppError, state::AppState};

#[derive(Deserialize)]
pub struct PutManifestBody {
    pub media_type: String,
    pub size: u64,
    pub platform: Option<Platform>,
    pub layers: Vec<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/repos/:repo/manifests/:digest",
            get(get_manifest).put(put_manifest),
        )
        .route("/v1/repos/:repo/manifests", get(list_manifests))
}

async fn put_manifest(
    State(state): State<AppState>,
    Path((repo, digest)): Path<(String, String)>,
    Json(body): Json<PutManifestBody>,
) -> Result<StatusCode, AppError> {
    let record = ManifestRecord {
        repo: repo.clone(),
        digest: digest.clone(),
        media_type: body.media_type,
        size: body.size,
        platform: body.platform,
        layers: body.layers,
    };
    let key = manifest_key(&repo, &digest);
    let value = serde_json::to_vec(&record)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    state.storage.put(&key, &value)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_manifest(
    State(state): State<AppState>,
    Path((repo, digest)): Path<(String, String)>,
) -> Result<Json<ManifestRecord>, AppError> {
    let key = manifest_key(&repo, &digest);
    match state.storage.get(&key)? {
        Some(v) => {
            let record: ManifestRecord = serde_json::from_slice(&v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            Ok(Json(record))
        }
        None => Err(AppError::NotFound),
    }
}

async fn list_manifests(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Json<Vec<ManifestRecord>>, AppError> {
    let prefix = manifest_prefix(&repo);
    let entries = state.storage.scan_prefix(&prefix)?;
    let mut manifests = Vec::with_capacity(entries.len());
    for (_, v) in entries {
        let record: ManifestRecord = serde_json::from_slice(&v)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        manifests.push(record);
    }
    Ok(Json(manifests))
}
