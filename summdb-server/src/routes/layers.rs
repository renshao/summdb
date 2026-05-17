use axum::{Json, Router, extract::{Path, State}, routing::get};
use summdb_core::{
    keys::{layer_key, manifest_key},
    types::{Digest, LayerRecord, ManifestRecord},
};

use crate::{
    error::AppError,
    state::AppState,
    views::{layer_to_view, manifest_to_view, LayerRecordView, ManifestView},
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/layers/:digest", get(get_layer))
        .route("/v1/layers/:digest/manifests", get(get_layer_manifests))
}

async fn get_layer(
    State(state): State<AppState>,
    Path(digest_str): Path<String>,
) -> Result<Json<LayerRecordView>, AppError> {
    let digest: Digest = digest_str.parse().map_err(AppError::Internal)?;
    match state.storage.get(&layer_key(&digest))? {
        Some(v) => {
            let record: LayerRecord = postcard::from_bytes(&v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            Ok(Json(layer_to_view(record, &state.interner)))
        }
        None => Err(AppError::NotFound),
    }
}

async fn get_layer_manifests(
    State(state): State<AppState>,
    Path(digest_str): Path<String>,
) -> Result<Json<Vec<ManifestView>>, AppError> {
    let digest: Digest = digest_str.parse().map_err(AppError::Internal)?;
    let layer = match state.storage.get(&layer_key(&digest))? {
        Some(v) => postcard::from_bytes::<LayerRecord>(&v)
            .map_err(|e| AppError::Internal(e.to_string()))?,
        None => return Ok(Json(vec![])),
    };
    let mut manifests = Vec::with_capacity(layer.manifests.len());
    for r in layer.manifests {
        if let Some(v) = state.storage.get(&manifest_key(r.repo, &r.digest))? {
            let record: ManifestRecord = postcard::from_bytes(&v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            manifests.push(manifest_to_view(record, &state.interner));
        }
    }
    Ok(Json(manifests))
}
