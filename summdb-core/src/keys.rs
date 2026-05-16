pub fn tag_key(repo: &str, tag: &str) -> String {
    format!("T:{repo}:{tag}")
}

pub fn manifest_key(repo: &str, digest: &str) -> String {
    format!("M:{repo}:{digest}")
}

pub fn layer_key(digest: &str) -> String {
    format!("L:{digest}")
}

pub fn tag_prefix(repo: &str) -> String {
    format!("T:{repo}:")
}

pub fn manifest_prefix(repo: &str) -> String {
    format!("M:{repo}:")
}
