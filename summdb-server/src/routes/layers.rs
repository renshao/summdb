use axum::{Json, Router, extract::{Path, State}, routing::get};
use summdb_core::types::ManifestRecord;

use crate::{error::AppError, state::AppState};

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/layers/:digest/manifests", get(get_layer_manifests))
}

async fn get_layer_manifests(
    State(state): State<AppState>,
    Path(layer_digest): Path<String>,
) -> Result<Json<Vec<ManifestRecord>>, AppError> {
    let entries = state.storage.scan_prefix("M:")?;
    let mut manifests = Vec::new();
    for (_, v) in entries {
        let record: ManifestRecord = serde_json::from_slice(&v)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        if record.layers.contains(&layer_digest) {
            manifests.push(record);
        }
    }
    Ok(Json(manifests))
}
