use crate::types::{Digest, RepoId};

pub const PREFIX_TAG: u8 = b'T';
pub const PREFIX_MANIFEST: u8 = b'M';
pub const PREFIX_MANIFEST_BODY: u8 = b'B';
pub const PREFIX_LAYER: u8 = b'L';
pub const PREFIX_REPO_INTERN_FORWARD: &[u8] = b"RI";
pub const PREFIX_REPO_INTERN_REVERSE: &[u8] = b"IR";

pub fn tag_key(repo: RepoId, tag: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 4 + tag.len());
    k.push(PREFIX_TAG);
    k.extend_from_slice(&repo.to_be_bytes());
    k.extend_from_slice(tag.as_bytes());
    k
}

pub fn manifest_key(repo: RepoId, digest: &Digest) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 4 + 32);
    k.push(PREFIX_MANIFEST);
    k.extend_from_slice(&repo.to_be_bytes());
    k.extend_from_slice(digest.as_bytes());
    k
}

pub fn layer_key(digest: &Digest) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 32);
    k.push(PREFIX_LAYER);
    k.extend_from_slice(digest.as_bytes());
    k
}

pub fn manifest_body_key(repo: RepoId, digest: &Digest) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 4 + 32);
    k.push(PREFIX_MANIFEST_BODY);
    k.extend_from_slice(&repo.to_be_bytes());
    k.extend_from_slice(digest.as_bytes());
    k
}

pub fn tag_prefix(repo: RepoId) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 4);
    k.push(PREFIX_TAG);
    k.extend_from_slice(&repo.to_be_bytes());
    k
}

pub fn manifest_prefix(repo: RepoId) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 4);
    k.push(PREFIX_MANIFEST);
    k.extend_from_slice(&repo.to_be_bytes());
    k
}

pub fn repo_intern_forward_key(repo: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_REPO_INTERN_FORWARD.len() + repo.len());
    k.extend_from_slice(PREFIX_REPO_INTERN_FORWARD);
    k.extend_from_slice(repo.as_bytes());
    k
}

pub fn repo_intern_reverse_key(id: RepoId) -> Vec<u8> {
    let mut k = Vec::with_capacity(PREFIX_REPO_INTERN_REVERSE.len() + 4);
    k.extend_from_slice(PREFIX_REPO_INTERN_REVERSE);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

pub fn repo_intern_reverse_prefix() -> Vec<u8> {
    PREFIX_REPO_INTERN_REVERSE.to_vec()
}

/// Parse a repo id back out of a reverse interner key (`IR<id_be_u32>`).
pub fn parse_repo_intern_reverse_key(k: &[u8]) -> Option<RepoId> {
    if k.len() != PREFIX_REPO_INTERN_REVERSE.len() + 4
        || &k[..PREFIX_REPO_INTERN_REVERSE.len()] != PREFIX_REPO_INTERN_REVERSE
    {
        return None;
    }
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&k[PREFIX_REPO_INTERN_REVERSE.len()..]);
    Some(u32::from_be_bytes(buf))
}

/// Extract the tag string suffix from a tag key (`T<repo_id_be_u32><tag>`).
pub fn parse_tag_key_suffix(k: &[u8]) -> Option<&str> {
    if k.len() < 5 || k[0] != PREFIX_TAG {
        return None;
    }
    std::str::from_utf8(&k[5..]).ok()
}
