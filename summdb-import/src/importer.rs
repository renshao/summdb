use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Deserialize;
use summdb_core::{
    keys::{manifest_key, tag_key},
    types::{ManifestRecord, Platform},
};
use summdb_storage::StorageEngine;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::client::{ManifestResponse, RegistryClient};

#[derive(Deserialize)]
struct Index {
    manifests: Vec<IndexEntry>,
}

#[derive(Deserialize)]
struct IndexEntry {
    digest: String,
    platform: Option<IndexPlatform>,
}

#[derive(Deserialize)]
struct IndexPlatform {
    os: String,
    architecture: String,
}

#[derive(Deserialize)]
struct ImageManifest {
    layers: Vec<Descriptor>,
}

#[derive(Deserialize)]
struct Descriptor {
    digest: String,
}

pub async fn import(
    client: Arc<RegistryClient>,
    db: Arc<dyn StorageEngine>,
    repo_filter: Option<&str>,
    parallelism: usize,
) -> Result<()> {
    let multi = MultiProgress::new();

    // Discovery phase: figure out which repos and what tags they have.
    let repos_with_tags: Vec<(String, Vec<String>)> = match repo_filter {
        Some(r) => {
            multi.println(format!("listing tags for {r}..."))?;
            let tags = client.list_tags(r).await.context("listing tags")?;
            multi.println(format!("found {} tag(s) in {r}", tags.len()))?;
            vec![(r.to_string(), tags)]
        }
        None => {
            multi.println("listing catalog...")?;
            let repos = client.list_catalog().await.context("listing catalog")?;
            multi.println(format!("found {} repo(s), listing tags...", repos.len()))?;
            let mut all = Vec::with_capacity(repos.len());
            let mut total = 0usize;
            for repo in repos {
                let tags = match client.list_tags(&repo).await {
                    Ok(t) => t,
                    Err(e) => {
                        multi.println(format!("  tags for {repo} failed: {e:#}"))?;
                        Vec::new()
                    }
                };
                total += tags.len();
                all.push((repo, tags));
            }
            multi.println(format!("total: {total} tag(s) across {} repo(s)", all.len()))?;
            all
        }
    };

    // Build worker spinners + bottom progress bar.
    let (workers, overall) = build_progress(&multi, parallelism);

    for (repo, tags) in repos_with_tags {
        if tags.is_empty() {
            continue;
        }
        overall.set_length(tags.len() as u64);
        overall.set_position(0);
        overall.set_message(repo.clone());
        process_repo(&client, &db, &repo, &tags, &workers, &overall, parallelism).await;
    }

    overall.finish_and_clear();
    for w in &workers {
        w.finish_and_clear();
    }
    Ok(())
}

fn build_progress(multi: &MultiProgress, parallelism: usize) -> (Vec<ProgressBar>, ProgressBar) {
    let worker_style = ProgressStyle::with_template("  {prefix:.bold} {spinner:.green} {wide_msg}")
        .unwrap()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏ ");
    let workers: Vec<ProgressBar> = (0..parallelism)
        .map(|i| {
            let pb = multi.add(ProgressBar::new_spinner());
            pb.set_style(worker_style.clone());
            pb.set_prefix(format!("w{}", i + 1));
            pb.set_message("idle");
            pb.enable_steady_tick(Duration::from_millis(100));
            pb
        })
        .collect();
    let overall = multi.add(ProgressBar::new(0));
    overall.set_style(
        ProgressStyle::with_template("  [{bar:30.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    (workers, overall)
}

async fn process_repo(
    client: &Arc<RegistryClient>,
    db: &Arc<dyn StorageEngine>,
    repo: &str,
    tags: &[String],
    workers: &[ProgressBar],
    overall: &ProgressBar,
    parallelism: usize,
) {
    let sem = Arc::new(Semaphore::new(parallelism));
    let bar_pool: Arc<Mutex<Vec<ProgressBar>>> = Arc::new(Mutex::new(workers.to_vec()));
    let mut set: JoinSet<()> = JoinSet::new();

    for tag in tags {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let bar = bar_pool.lock().unwrap().pop().expect("free bar must exist");
        let bar_pool = bar_pool.clone();
        let overall = overall.clone();
        let client = client.clone();
        let db = db.clone();
        let repo = repo.to_string();
        let tag = tag.clone();
        set.spawn(async move {
            bar.set_message(format!("{repo}:{tag}"));
            if let Err(e) = import_tag(&client, db.as_ref(), &repo, &tag, &bar).await {
                let _ = bar.println(format!("error {repo}:{tag}: {e:#}"));
            }
            bar.set_message("idle");
            bar_pool.lock().unwrap().push(bar);
            overall.inc(1);
            drop(permit);
        });
    }
    while set.join_next().await.is_some() {}
}

async fn import_tag(
    client: &RegistryClient,
    db: &dyn StorageEngine,
    repo: &str,
    tag: &str,
    bar: &ProgressBar,
) -> Result<()> {
    bar.set_message(format!("{repo}:{tag} fetching"));
    let mr = client.fetch_manifest(repo, tag).await?;
    let digest = mr.digest.clone();
    bar.set_message(format!("{repo}:{tag} {}", short(&digest)));
    process_manifest(client, db, repo, &mr, None, bar).await?;
    db.put(&tag_key(repo, tag), digest.as_bytes())?;
    Ok(())
}

fn short(digest: &str) -> String {
    digest
        .strip_prefix("sha256:")
        .map(|d| format!("sha256:{}", &d[..d.len().min(12)]))
        .unwrap_or_else(|| digest.to_string())
}

fn process_manifest<'a>(
    client: &'a RegistryClient,
    db: &'a dyn StorageEngine,
    repo: &'a str,
    mr: &'a ManifestResponse,
    platform_hint: Option<Platform>,
    bar: &'a ProgressBar,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
    Box::pin(async move {
        if is_index(&mr.media_type) {
            let idx: Index =
                serde_json::from_slice(&mr.body).context("parsing index manifest")?;
            let record = ManifestRecord {
                repo: repo.to_string(),
                digest: mr.digest.clone(),
                media_type: mr.media_type.clone(),
                size: mr.body.len() as u64,
                platform: None,
                layers: vec![],
            };
            db.put(
                &manifest_key(repo, &mr.digest),
                &serde_json::to_vec(&record)?,
            )?;
            for child in idx.manifests {
                bar.set_message(format!("{repo} child {}", short(&child.digest)));
                let child_mr = match client.fetch_manifest(repo, &child.digest).await {
                    Ok(m) => m,
                    Err(e) => {
                        let _ = bar.println(format!("  child {} failed: {e:#}", child.digest));
                        continue;
                    }
                };
                let plat = child.platform.map(|p| Platform {
                    os: p.os,
                    arch: p.architecture,
                });
                process_manifest(client, db, repo, &child_mr, plat, bar).await?;
            }
        } else {
            let img: ImageManifest =
                serde_json::from_slice(&mr.body).context("parsing image manifest")?;
            let layers = img.layers.into_iter().map(|d| d.digest).collect();
            let record = ManifestRecord {
                repo: repo.to_string(),
                digest: mr.digest.clone(),
                media_type: mr.media_type.clone(),
                size: mr.body.len() as u64,
                platform: platform_hint,
                layers,
            };
            db.put(
                &manifest_key(repo, &mr.digest),
                &serde_json::to_vec(&record)?,
            )?;
        }
        Ok(())
    })
}

fn is_index(mt: &str) -> bool {
    mt == "application/vnd.oci.image.index.v1+json"
        || mt == "application/vnd.docker.distribution.manifest.list.v2+json"
}
