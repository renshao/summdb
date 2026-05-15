use axum::{Router, response::Html, routing::get};

use crate::state::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/ui", get(ui_handler))
}

async fn ui_handler() -> Html<&'static str> {
    Html(include_str!("ui.html"))
}
