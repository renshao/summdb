use axum::{Json, Router, extract::{Path, State}, routing::get};
use summdb_core::{
    keys::{layer_key, manifest_key},
    types::{LayerRecord, ManifestRecord},
};

use crate::{error::AppError, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/layers/:digest", get(get_layer))
        .route("/v1/layers/:digest/manifests", get(get_layer_manifests))
}

async fn get_layer(
    State(state): State<AppState>,
    Path(layer_digest): Path<String>,
) -> Result<Json<LayerRecord>, AppError> {
    match state.storage.get(&layer_key(&layer_digest))? {
        Some(v) => {
            let record: LayerRecord = postcard::from_bytes(&v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            Ok(Json(record))
        }
        None => Err(AppError::NotFound),
    }
}

async fn get_layer_manifests(
    State(state): State<AppState>,
    Path(layer_digest): Path<String>,
) -> Result<Json<Vec<ManifestRecord>>, AppError> {
    let layer = match state.storage.get(&layer_key(&layer_digest))? {
        Some(v) => postcard::from_bytes::<LayerRecord>(&v)
            .map_err(|e| AppError::Internal(e.to_string()))?,
        None => return Ok(Json(vec![])),
    };
    let mut manifests = Vec::with_capacity(layer.manifests.len());
    for r in layer.manifests {
        if let Some(v) = state.storage.get(&manifest_key(&r.repo, &r.digest))? {
            let record: ManifestRecord = postcard::from_bytes(&v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            manifests.push(record);
        }
    }
    Ok(Json(manifests))
}
