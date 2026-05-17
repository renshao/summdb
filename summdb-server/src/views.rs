use serde::Serialize;
use summdb_core::types::{ChildRef, Digest, LayerRecord, ManifestRecord, ManifestRef, Platform};
use summdb_storage::RepoInterner;

#[derive(Serialize)]
pub struct ManifestView {
    pub repo: String,
    pub digest: Digest,
    pub media_type: String,
    pub size: u64,
    pub platform: Option<Platform>,
    pub layers: Vec<Digest>,
    pub children: Vec<ChildRef>,
    pub parent: Option<ManifestRefView>,
    pub tags: Vec<String>,
}

#[derive(Serialize)]
pub struct ManifestRefView {
    pub repo: String,
    pub digest: Digest,
}

#[derive(Serialize)]
pub struct LayerRecordView {
    pub size: u64,
    pub manifests: Vec<ManifestRefView>,
}

pub fn manifest_to_view(m: ManifestRecord, interner: &RepoInterner) -> ManifestView {
    ManifestView {
        repo: interner.resolve(m.repo).unwrap_or_default(),
        digest: m.digest,
        media_type: m.media_type,
        size: m.size,
        platform: m.platform,
        layers: m.layers,
        children: m.children,
        parent: m.parent.map(|p| manifest_ref_to_view(p, interner)),
        tags: m.tags,
    }
}

pub fn manifest_ref_to_view(r: ManifestRef, interner: &RepoInterner) -> ManifestRefView {
    ManifestRefView {
        repo: interner.resolve(r.repo).unwrap_or_default(),
        digest: r.digest,
    }
}

pub fn layer_to_view(l: LayerRecord, interner: &RepoInterner) -> LayerRecordView {
    LayerRecordView {
        size: l.size,
        manifests: l
            .manifests
            .into_iter()
            .map(|r| manifest_ref_to_view(r, interner))
            .collect(),
    }
}
