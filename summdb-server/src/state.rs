use std::sync::Arc;

use summdb_storage::StorageEngine;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StorageEngine>,
}

impl AppState {
    pub fn new(engine: impl StorageEngine) -> Self {
        Self {
            storage: Arc::new(engine),
        }
    }
}
