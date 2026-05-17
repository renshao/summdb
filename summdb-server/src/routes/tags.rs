use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::get,
};
use serde::{Deserialize, Serialize};
use summdb_core::{
    keys::{parse_tag_key_suffix, tag_key, tag_prefix},
    types::Digest,
};

use crate::{error::AppError, state::AppState};

#[derive(Deserialize)]
pub struct PutTagBody {
    pub digest: String,
}

#[derive(Serialize)]
pub struct TagRecord {
    pub tag: String,
    pub digest: Digest,
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
    let digest: Digest = body
        .digest
        .parse()
        .map_err(|e: String| AppError::Internal(e))?;
    let repo_id = state.interner.intern(state.storage.as_ref(), &repo)?;
    summdb_storage::ops::set_tag(state.storage.as_ref(), repo_id, &tag, &digest)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn get_tag(
    State(state): State<AppState>,
    Path((repo, tag)): Path<(String, String)>,
) -> Result<Json<TagRecord>, AppError> {
    let repo_id = match state.interner.try_lookup(&repo) {
        Some(id) => id,
        None => return Err(AppError::NotFound),
    };
    match state.storage.get(&tag_key(repo_id, &tag))? {
        Some(v) => {
            let digest = Digest::from_slice(&v)
                .map_err(AppError::Internal)?;
            Ok(Json(TagRecord { tag, digest }))
        }
        None => Err(AppError::NotFound),
    }
}

async fn list_tags(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Json<Vec<TagRecord>>, AppError> {
    let repo_id = match state.interner.try_lookup(&repo) {
        Some(id) => id,
        None => return Ok(Json(vec![])),
    };
    let entries = state.storage.scan_prefix(&tag_prefix(repo_id))?;
    let mut out = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        let Some(tag) = parse_tag_key_suffix(&k) else { continue };
        let digest = Digest::from_slice(&v).map_err(AppError::Internal)?;
        out.push(TagRecord {
            tag: tag.to_string(),
            digest,
        });
    }
    Ok(Json(out))
}
