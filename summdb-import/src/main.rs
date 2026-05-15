mod client;
mod importer;

use std::sync::Arc;

use clap::Parser;
use summdb_storage::StorageEngine;

#[derive(Parser)]
#[command(name = "summdb-import", about = "Import tags and manifests from an OCI registry")]
struct Args {
    /// Registry base URL (e.g. https://ghcr.io, https://registry-1.docker.io)
    #[arg(long)]
    registry: String,

    /// Single repo to import. If omitted, uses the registry's _catalog endpoint.
    #[arg(long)]
    repo: Option<String>,

    /// Path to summdb file
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
    let engine = summdb_storage::RedbEngine::open(&args.db)?;
    let db: Arc<dyn StorageEngine> = Arc::new(engine);
    let client = Arc::new(client::RegistryClient::new(args.registry, args.user, args.pass)?);
    importer::import(client, db, args.repo.as_deref(), args.parallelism).await
}
