use std::sync::Arc;

use summdb_storage::{RepoInterner, StorageEngine};

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn StorageEngine>,
    pub interner: Arc<RepoInterner>,
}

impl AppState {
    pub fn new(engine: impl StorageEngine, interner: Arc<RepoInterner>) -> Self {
        Self {
            storage: Arc::new(engine),
            interner,
        }
    }
}
