use std::sync::{Arc, Mutex};

use redb::{Database, ReadableTable, TableDefinition};
use summdb_core::error::{Result, SummError};

use crate::engine::StorageEngine;

const DATA: TableDefinition<&str, &[u8]> = TableDefinition::new("data");

pub struct RedbEngine {
    db: Arc<Mutex<Database>>,
}

impl RedbEngine {
    pub fn open(path: &str) -> Result<Self> {
        let db = Database::create(path).map_err(|e| SummError::Storage(e.to_string()))?;
        // Ensure the table exists before any reads
        let txn = db.begin_write().map_err(|e| SummError::Storage(e.to_string()))?;
        txn.open_table(DATA).map_err(|e| SummError::Storage(e.to_string()))?;
        txn.commit().map_err(|e| SummError::Storage(e.to_string()))?;
        Ok(Self {
            db: Arc::new(Mutex::new(db)),
        })
    }
}

impl StorageEngine for RedbEngine {
    fn put(&self, key: &str, value: &[u8]) -> Result<()> {
        let db = self.db.lock().unwrap();
        let txn = db.begin_write().map_err(|e| SummError::Storage(e.to_string()))?;
        {
            let mut table = txn.open_table(DATA).map_err(|e| SummError::Storage(e.to_string()))?;
            table.insert(key, value).map_err(|e| SummError::Storage(e.to_string()))?;
        }
        txn.commit().map_err(|e| SummError::Storage(e.to_string()))?;
        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let db = self.db.lock().unwrap();
        let txn = db.begin_read().map_err(|e| SummError::Storage(e.to_string()))?;
        let table = txn.open_table(DATA).map_err(|e| SummError::Storage(e.to_string()))?;
        match table.get(key).map_err(|e| SummError::Storage(e.to_string()))? {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    fn delete(&self, key: &str) -> Result<()> {
        let db = self.db.lock().unwrap();
        let txn = db.begin_write().map_err(|e| SummError::Storage(e.to_string()))?;
        {
            let mut table = txn.open_table(DATA).map_err(|e| SummError::Storage(e.to_string()))?;
            table.remove(key).map_err(|e| SummError::Storage(e.to_string()))?;
        }
        txn.commit().map_err(|e| SummError::Storage(e.to_string()))?;
        Ok(())
    }

    fn merge(
        &self,
        key: &str,
        f: Box<dyn FnOnce(Option<Vec<u8>>) -> Result<Vec<u8>> + Send>,
    ) -> Result<()> {
        let db = self.db.lock().unwrap();
        let txn = db.begin_write().map_err(|e| SummError::Storage(e.to_string()))?;
        {
            let mut table = txn.open_table(DATA).map_err(|e| SummError::Storage(e.to_string()))?;
            let current = match table.get(key).map_err(|e| SummError::Storage(e.to_string()))? {
                Some(v) => Some(v.value().to_vec()),
                None => None,
            };
            let new = f(current)?;
            table
                .insert(key, new.as_slice())
                .map_err(|e| SummError::Storage(e.to_string()))?;
        }
        txn.commit().map_err(|e| SummError::Storage(e.to_string()))?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let db = self.db.lock().unwrap();
        let txn = db.begin_read().map_err(|e| SummError::Storage(e.to_string()))?;
        let table = txn.open_table(DATA).map_err(|e| SummError::Storage(e.to_string()))?;
        let mut results = Vec::new();
        for entry in table.range(prefix..).map_err(|e| SummError::Storage(e.to_string()))? {
            let (k, v) = entry.map_err(|e| SummError::Storage(e.to_string()))?;
            if !k.value().starts_with(prefix) {
                break;
            }
            results.push((k.value().to_string(), v.value().to_vec()));
        }
        Ok(results)
    }
}
