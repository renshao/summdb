use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Deserialize;
use summdb_core::{
    error::SummError,
    keys::{layer_key, manifest_body_key, manifest_key, manifest_prefix},
    types::{ChildRef, Digest, LayerRecord, ManifestRecord, ManifestRef, Platform, RepoId},
};
use summdb_storage::StorageEngine;

use crate::{
    error::AppError,
    state::AppState,
    views::{manifest_to_view, ManifestView},
};

const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Deserialize)]
pub struct LayerInput {
    pub digest: String,
    #[serde(default)]
    pub size: u64,
}

#[derive(Deserialize)]
pub struct ChildInput {
    pub digest: String,
    pub platform: Option<Platform>,
}

#[derive(Deserialize)]
pub struct ParentInput {
    pub repo: String,
    pub digest: String,
}

#[derive(Deserialize)]
pub struct PutManifestBody {
    pub media_type: String,
    pub total_layer_size: u64,
    pub platform: Option<Platform>,
    pub layers: Vec<LayerInput>,
    #[serde(default)]
    pub children: Vec<ChildInput>,
    #[serde(default)]
    pub parent: Option<ParentInput>,
    /// Raw manifest JSON. Stored zstd-compressed under the B: key.
    #[serde(default)]
    pub body: Option<String>,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/repos/:repo/manifests/:digest",
            get(get_manifest).put(put_manifest),
        )
        .route(
            "/v1/repos/:repo/manifests/:digest/body",
            get(get_manifest_body),
        )
        .route("/v1/repos/:repo/manifests", get(list_manifests))
}

async fn put_manifest(
    State(state): State<AppState>,
    Path((repo, digest_str)): Path<(String, String)>,
    Json(body): Json<PutManifestBody>,
) -> Result<StatusCode, AppError> {
    let digest: Digest = digest_str.parse().map_err(AppError::Internal)?;
    let repo_id = state.interner.intern(state.storage.as_ref(), &repo)?;

    let mut layer_digests = Vec::with_capacity(body.layers.len());
    for l in &body.layers {
        layer_digests.push(l.digest.parse::<Digest>().map_err(AppError::Internal)?);
    }
    let mut children = Vec::with_capacity(body.children.len());
    for c in body.children {
        children.push(ChildRef {
            digest: c.digest.parse::<Digest>().map_err(AppError::Internal)?,
            platform: c.platform,
        });
    }
    let parent = match body.parent {
        Some(p) => {
            let parent_repo_id = state.interner.intern(state.storage.as_ref(), &p.repo)?;
            Some(ManifestRef {
                repo: parent_repo_id,
                digest: p.digest.parse::<Digest>().map_err(AppError::Internal)?,
            })
        }
        None => None,
    };

    let record = ManifestRecord {
        repo: repo_id,
        digest,
        media_type: body.media_type,
        total_layer_size: body.total_layer_size,
        platform: body.platform,
        layers: layer_digests.clone(),
        children,
        parent,
        tags: vec![],
    };
    let value = postcard::to_allocvec(&record)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    state.storage.put(&manifest_key(repo_id, &digest), &value)?;

    if let Some(raw) = body.body.as_deref() {
        let compressed = zstd::encode_all(raw.as_bytes(), ZSTD_COMPRESSION_LEVEL)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        state
            .storage
            .put(&manifest_body_key(repo_id, &digest), &compressed)?;
    }

    for (layer, input) in layer_digests.iter().zip(&body.layers) {
        record_layer_ref(state.storage.as_ref(), layer, input.size, repo_id, &digest)?;
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn get_manifest_body(
    State(state): State<AppState>,
    Path((repo, digest_str)): Path<(String, String)>,
) -> Result<Response, AppError> {
    let digest: Digest = digest_str.parse().map_err(AppError::Internal)?;
    let repo_id = state.interner.try_lookup(&repo).ok_or(AppError::NotFound)?;
    let compressed = state
        .storage
        .get(&manifest_body_key(repo_id, &digest))?
        .ok_or(AppError::NotFound)?;
    let raw = zstd::decode_all(compressed.as_slice())
        .map_err(|e| AppError::Internal(e.to_string()))?;

    let media_type = state
        .storage
        .get(&manifest_key(repo_id, &digest))?
        .and_then(|v| postcard::from_bytes::<ManifestRecord>(&v).ok())
        .map(|m| m.media_type)
        .unwrap_or_else(|| "application/json".to_string());

    let mut headers = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(&media_type) {
        headers.insert(header::CONTENT_TYPE, v);
    }
    Ok((headers, raw).into_response())
}

async fn get_manifest(
    State(state): State<AppState>,
    Path((repo, digest_str)): Path<(String, String)>,
) -> Result<Json<ManifestView>, AppError> {
    let digest: Digest = digest_str.parse().map_err(AppError::Internal)?;
    let repo_id = match state.interner.try_lookup(&repo) {
        Some(id) => id,
        None => return Err(AppError::NotFound),
    };
    match state.storage.get(&manifest_key(repo_id, &digest))? {
        Some(v) => {
            let record: ManifestRecord = postcard::from_bytes(&v)
                .map_err(|e| AppError::Internal(e.to_string()))?;
            Ok(Json(manifest_to_view(record, &state.interner)))
        }
        None => Err(AppError::NotFound),
    }
}

async fn list_manifests(
    State(state): State<AppState>,
    Path(repo): Path<String>,
) -> Result<Json<Vec<ManifestView>>, AppError> {
    let repo_id = match state.interner.try_lookup(&repo) {
        Some(id) => id,
        None => return Ok(Json(vec![])),
    };
    let entries = state.storage.scan_prefix(&manifest_prefix(repo_id))?;
    let mut out = Vec::with_capacity(entries.len());
    for (_, v) in entries {
        let record: ManifestRecord = postcard::from_bytes(&v)
            .map_err(|e| AppError::Internal(e.to_string()))?;
        out.push(manifest_to_view(record, &state.interner));
    }
    Ok(Json(out))
}

pub fn record_layer_ref(
    storage: &dyn StorageEngine,
    layer_digest: &Digest,
    size: u64,
    repo: RepoId,
    manifest_digest: &Digest,
) -> Result<(), AppError> {
    let manifest_ref = ManifestRef {
        repo,
        digest: *manifest_digest,
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
