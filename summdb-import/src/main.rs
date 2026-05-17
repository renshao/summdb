mod client;
mod importer;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use summdb_storage::{RepoInterner, StorageEngine};

#[derive(Parser)]
#[command(name = "summdb-import", about = "Import tags and manifests from an OCI registry")]
struct Args {
    /// Registry base URL (e.g. https://ghcr.io, https://registry-1.docker.io)
    #[arg(long)]
    registry: String,

    /// Repo to import; can be specified multiple times.
    /// Use "repo" for all tags or "repo:GLOB" to filter (e.g. library/python:3.*).
    /// If omitted, uses the registry's _catalog endpoint.
    #[arg(long = "repo", value_name = "REPO[:GLOB]")]
    repos: Vec<String>,

    /// Path to the summdb file
    #[arg(long, default_value = "summdb.db")]
    db: String,

    /// Basic auth username (forwarded to token endpoint if challenged)
    #[arg(long)]
    user: Option<String>,

    /// Basic auth password
    #[arg(long)]
    pass: Option<String>,

    /// Number of tags to process in parallel
    #[arg(long, default_value = "5")]
    parallelism: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let specs: Vec<importer::RepoSpec> = args
        .repos
        .iter()
        .map(|s| importer::RepoSpec::parse(s))
        .collect::<anyhow::Result<_>>()
        .context("parsing --repo arguments")?;
    let engine = summdb_storage::RedbEngine::open(&args.db)?;
    let interner = Arc::new(RepoInterner::load(&engine as &dyn StorageEngine)?);
    let db: Arc<dyn StorageEngine> = Arc::new(engine);
    let client = Arc::new(client::RegistryClient::new(args.registry, args.user, args.pass)?);
    importer::import(client, db, interner, &specs, args.parallelism).await
}
