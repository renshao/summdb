use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::{Context, Result, bail};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, LINK, WWW_AUTHENTICATE};
use serde::Deserialize;

const ACCEPTABLE_MANIFEST_TYPES: &str = concat!(
    "application/vnd.oci.image.index.v1+json,",
    "application/vnd.oci.image.manifest.v1+json,",
    "application/vnd.docker.distribution.manifest.list.v2+json,",
    "application/vnd.docker.distribution.manifest.v2+json"
);

pub struct RegistryClient {
    base: String,
    http: reqwest::Client,
    user: Option<String>,
    pass: Option<String>,
    tokens: Mutex<HashMap<String, String>>,
}

pub struct ManifestResponse {
    pub digest: String,
    pub media_type: String,
    pub body: Vec<u8>,
}

#[derive(Deserialize)]
struct TokenResponse {
    token: Option<String>,
    access_token: Option<String>,
}

struct BearerChallenge {
    realm: String,
    service: Option<String>,
    scope: Option<String>,
}

impl RegistryClient {
    pub fn new(base: String, user: Option<String>, pass: Option<String>) -> Result<Self> {
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::builder().build()?,
            user,
            pass,
            tokens: Mutex::new(HashMap::new()),
        })
    }

    pub async fn list_catalog(&self) -> Result<Vec<String>> {
        let mut path = "/v2/_catalog?n=1000".to_string();
        let mut out = Vec::new();
        loop {
            let resp = self.request(&path, None, "registry:catalog:*").await?;
            let next = parse_link_next(resp.headers().get(LINK));
            let body: serde_json::Value = resp.json().await?;
            if let Some(arr) = body.get("repositories").and_then(|v| v.as_array()) {
                for r in arr {
                    if let Some(s) = r.as_str() {
                        out.push(s.to_string());
                    }
                }
            }
            match next {
                Some(n) => path = n,
                None => break,
            }
        }
        Ok(out)
    }

    pub async fn list_tags(&self, repo: &str) -> Result<Vec<String>> {
        let mut path = format!("/v2/{repo}/tags/list?n=1000");
        let scope = format!("repository:{repo}:pull");
        let mut out = Vec::new();
        loop {
            let resp = self.request(&path, None, &scope).await?;
            let next = parse_link_next(resp.headers().get(LINK));
            let body: serde_json::Value = resp.json().await?;
            if let Some(arr) = body.get("tags").and_then(|v| v.as_array()) {
                for t in arr {
                    if let Some(s) = t.as_str() {
                        out.push(s.to_string());
                    }
                }
            }
            match next {
                Some(n) => path = n,
                None => break,
            }
        }
        Ok(out)
    }

    pub async fn fetch_manifest(&self, repo: &str, reference: &str) -> Result<ManifestResponse> {
        let path = format!("/v2/{repo}/manifests/{reference}");
        let scope = format!("repository:{repo}:pull");
        let resp = self
            .request(&path, Some(ACCEPTABLE_MANIFEST_TYPES), &scope)
            .await?;
        let digest = resp
            .headers()
            .get("docker-content-digest")
            .and_then(|h| h.to_str().ok())
            .map(String::from)
            .context("missing Docker-Content-Digest header")?;
        let media_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .map(|s| s.split(';').next().unwrap_or(s).trim().to_string())
            .unwrap_or_default();
        let body = resp.bytes().await?.to_vec();
        Ok(ManifestResponse { digest, media_type, body })
    }

    async fn request(
        &self,
        path: &str,
        accept: Option<&str>,
        scope: &str,
    ) -> Result<reqwest::Response> {
        let url = if path.starts_with("http") {
            path.to_string()
        } else {
            format!("{}{}", self.base, path)
        };
        let resp = self.send(&url, accept, scope).await?;
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            if !resp.status().is_success() {
                bail!("GET {path} failed: {}", resp.status());
            }
            return Ok(resp);
        }
        let auth = resp
            .headers()
            .get(WWW_AUTHENTICATE)
            .and_then(|h| h.to_str().ok())
            .map(String::from)
            .context("401 with no WWW-Authenticate")?;
        let challenge = parse_bearer_challenge(&auth)
            .context("WWW-Authenticate is not a Bearer challenge")?;
        let token = self.fetch_token(&challenge).await?;
        self.tokens.lock().unwrap().insert(scope.to_string(), token);
        let resp = self.send(&url, accept, scope).await?;
        if !resp.status().is_success() {
            bail!("GET {path} failed after auth: {}", resp.status());
        }
        Ok(resp)
    }

    async fn send(
        &self,
        url: &str,
        accept: Option<&str>,
        scope: &str,
    ) -> Result<reqwest::Response> {
        let mut req = self.http.get(url);
        if let Some(a) = accept {
            req = req.header(ACCEPT, a);
        }
        let token = self.tokens.lock().unwrap().get(scope).cloned();
        if let Some(t) = token {
            req = req.header(AUTHORIZATION, format!("Bearer {t}"));
        }
        Ok(req.send().await?)
    }

    async fn fetch_token(&self, c: &BearerChallenge) -> Result<String> {
        let mut req = self.http.get(&c.realm);
        let mut q: Vec<(&str, String)> = Vec::new();
        if let Some(s) = &c.service {
            q.push(("service", s.clone()));
        }
        if let Some(s) = &c.scope {
            q.push(("scope", s.clone()));
        }
        req = req.query(&q);
        if let (Some(u), Some(p)) = (&self.user, &self.pass) {
            req = req.basic_auth(u, Some(p));
        }
        let resp = req.send().await?.error_for_status()?;
        let tr: TokenResponse = resp.json().await?;
        tr.token
            .or(tr.access_token)
            .context("token endpoint returned neither `token` nor `access_token`")
    }
}

fn parse_bearer_challenge(h: &str) -> Option<BearerChallenge> {
    let rest = h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer "))?;
    let mut realm = None;
    let mut service = None;
    let mut scope = None;
    for part in rest.split(',') {
        let (k, v) = part.trim().split_once('=')?;
        let v = v.trim().trim_matches('"').to_string();
        match k.trim() {
            "realm" => realm = Some(v),
            "service" => service = Some(v),
            "scope" => scope = Some(v),
            _ => {}
        }
    }
    Some(BearerChallenge { realm: realm?, service, scope })
}

fn parse_link_next(h: Option<&reqwest::header::HeaderValue>) -> Option<String> {
    let s = h?.to_str().ok()?;
    for entry in s.split(',') {
        let entry = entry.trim();
        let is_next = entry.contains("rel=\"next\"") || entry.contains("rel=next");
        if !is_next {
            continue;
        }
        let start = entry.find('<')? + 1;
        let end = entry[start..].find('>')? + start;
        return Some(entry[start..end].to_string());
    }
    None
}
