use summdb_core::error::Result;

pub trait StorageEngine: Send + Sync + 'static {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn delete(&self, key: &[u8]) -> Result<()>;
    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn merge(
        &self,
        key: &[u8],
        f: Box<dyn FnOnce(Option<Vec<u8>>) -> Result<Vec<u8>> + Send>,
    ) -> Result<()>;
}
