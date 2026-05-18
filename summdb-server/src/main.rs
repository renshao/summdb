mod error;
mod routes;
mod state;
mod ui;
mod views;

use std::sync::Arc;

use clap::Parser;
use state::AppState;
use summdb_storage::{RedbEngine, RepoInterner, StorageEngine};

#[derive(Parser)]
#[command(name = "summdb-server", about = "summdb HTTP server")]
struct Args {
    /// Path to the summdb file
    #[arg(long, default_value = "summdb.db")]
    db: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let engine = RedbEngine::open(&args.db)
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", args.db));
    let interner = Arc::new(
        RepoInterner::load(&engine as &dyn StorageEngine)
            .unwrap_or_else(|e| panic!("failed to load repo interner: {e}")),
    );
    let state = AppState::new(engine, interner);

    let app = axum::Router::new()
        .merge(routes::repos::router())
        .merge(routes::tags::router())
        .merge(routes::manifests::router())
        .merge(routes::layers::router())
        .merge(ui::router())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:1031")
        .await
        .expect("failed to bind port 1031");
    println!("summdb listening on port 1031");
    axum::serve(listener, app).await.unwrap();
}
