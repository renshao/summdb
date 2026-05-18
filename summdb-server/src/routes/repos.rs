use axum::{Json, Router, extract::State, routing::get};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/repos", get(list_repos))
}

async fn list_repos(State(state): State<AppState>) -> Json<Vec<String>> {
    let mut repos = state.interner.all_repos();
    repos.sort_unstable();
    Json(repos)
}
