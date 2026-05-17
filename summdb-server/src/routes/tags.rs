use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use summdb_core::keys::{tag_key, tag_prefix};

use crate::{error::AppError, state::AppState};

#[derive(Deserialize)]
pub struct PutTagBody {
    pub digest: String,
}

#[derive(Serialize)]
pub struct TagRecord {
    pub tag: String,
    pub digest: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/repos/:repo/tags/:tag",
            get(get_tag).put(put_tag),
        )
        .route("/v1/repos/:repo/tags", get(list_tags))
}

async fn put_tag(
    State(state): State<AppState>,
    Path((repo, tag)): Path<(String, String)>,
    Json(body): Json<PutTagBody>,
) -> Result<StatusCode, AppError> {
    summdb_storage::ops::set_tag(state.storage.as_ref(), &repo, &tag, &body.digest)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_tag(
    State(state): State<AppState>,
    Path((repo, tag)): Path<(String, String)>,
) -> Result<Json<TagRecord>, AppError> {
    let key = tag_key(&repo, &tag);
    match state.storage.get(&key)? {
        Some(v) => {
            let digest = String::from_utf8(v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            Ok(Json(TagRecord { tag, digest }))
        }
        None => Err(AppError::NotFound),
    }
}

async fn list_tags(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Json<Vec<TagRecord>>, AppError> {
    let prefix = tag_prefix(&repo);
    let entries = state.storage.scan_prefix(&prefix)?;
    let tags = entries
        .into_iter()
        .filter_map(|(k, v)| {
            let tag = k.strip_prefix(&prefix)?.to_string();
            let digest = String::from_utf8(v).ok()?;
            Some(TagRecord { tag, digest })
        })
        .collect();
    Ok(Json(tags))
}
