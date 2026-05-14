mod error;
mod routes;
mod state;

use state::AppState;
use summdb_storage::RedbEngine;

#[tokio::main]
async fn main() {
    let engine = RedbEngine::open("summdb.db").expect("failed to open summdb.db");
    let state = AppState::new(engine);

    let app = axum::Router::new()
        .merge(routes::tags::router())
        .merge(routes::manifests::router())
        .merge(routes::layers::router())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:6666")
        .await
        .expect("failed to bind port 6666");
    println!("summdb listening on port 6666");
    axum::serve(listener, app).await.unwrap();
}
