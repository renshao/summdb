use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use serde::Deserialize;
use summdb_core::{
    error::SummError,
    keys::{layer_key, manifest_key, manifest_prefix},
    types::{ChildRef, LayerRecord, ManifestRecord, ManifestRef, Platform},
};
use summdb_storage::StorageEngine;

use crate::{error::AppError, state::AppState};

#[derive(Deserialize)]
pub struct LayerInput {
    pub digest: String,
    #[serde(default)]
    pub size: u64,
}

#[derive(Deserialize)]
pub struct PutManifestBody {
    pub media_type: String,
    pub size: u64,
    pub platform: Option<Platform>,
    pub layers: Vec<LayerInput>,
    #[serde(default)]
    pub children: Vec<ChildRef>,
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
    let layer_digests: Vec<String> = body.layers.iter().map(|l| l.digest.clone()).collect();
    let record = ManifestRecord {
        repo: repo.clone(),
        digest: digest.clone(),
        media_type: body.media_type,
        size: body.size,
        platform: body.platform,
        layers: layer_digests,
        children: body.children,
    };
    let key = manifest_key(&repo, &digest);
    let value = postcard::to_allocvec(&record)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    state.storage.put(&key, &value)?;

    for layer in body.layers {
        record_layer_ref(state.storage.as_ref(), &layer.digest, layer.size, &repo, &digest)?;
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn get_manifest(
    State(state): State<AppState>,
    Path((repo, digest)): Path<(String, String)>,
) -> Result<Json<ManifestRecord>, AppError> {
    let key = manifest_key(&repo, &digest);
    match state.storage.get(&key)? {
        Some(v) => {
            let record: ManifestRecord = postcard::from_bytes(&v)
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
        let record: ManifestRecord = postcard::from_bytes(&v)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        manifests.push(record);
    }
    Ok(Json(manifests))
}

pub fn record_layer_ref(
    storage: &dyn StorageEngine,
    layer_digest: &str,
    size: u64,
    repo: &str,
    manifest_digest: &str,
) -> Result<(), AppError> {
    let manifest_ref = ManifestRef {
        repo: repo.to_string(),
        digest: manifest_digest.to_string(),
    };
    storage.merge(
        &layer_key(layer_digest),
        Box::new(move |current| {
            let mut record = current
                .and_then(|b| postcard::from_bytes::<LayerRecord>(&b).ok())
                .unwrap_or(LayerRecord { size, manifests: vec![] });
            if record.size == 0 {
                record.size = size;
            }
            if !record.manifests.iter().any(|m| m == &manifest_ref) {
                record.manifests.push(manifest_ref);
            }
            postcard::to_allocvec(&record).map_err(|e| SummError::InvalidData(e.to_string()))
        }),
    )?;
    Ok(())
}
