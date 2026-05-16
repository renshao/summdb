use axum::{Router, response::Html, routing::get};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/ui", get(ui_handler))
}

async fn ui_handler() -> Html<String> {
    #[cfg(debug_assertions)]
    let html = {
        const PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/ui.html");
        tokio::fs::read_to_string(PATH)
            .await
            .unwrap_or_else(|e| format!("error loading ui.html: {e}"))
    };
    #[cfg(not(debug_assertions))]
    let html = include_str!("ui.html").to_string();

    Html(html)
}
