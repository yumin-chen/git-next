//! SQLite-based storage backend for local file-based storage
//! 
//! Provides persistent storage using SQLite with transaction support and connection pooling.
//! This backend is suitable for local development and single-user scenarios.

use crate::{Storage, Transaction, StorageError, Result, Reference, ReferenceTarget};
use gitnext_core::{ObjectId, GitObject};
use async_trait::async_trait;
use sqlx::{SqlitePool, Row};
use std::path::Path;

/// SQLite-based storage backend with connection pooling and transaction support
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Create a new SQLite storage backend with the given database path
    pub async fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_url = format!("sqlite:{}", db_path.as_ref().display());
        
        let pool = SqlitePool::connect(&db_url)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to connect to SQLite: {}", e)))?;
        
        let storage = Self { pool };
        storage.initialize_schema().await?;
        
        Ok(storage)
    }
    
    /// Create an in-memory SQLite database (for testing)
    pub async fn new_in_memory() -> Result<Self> {
        let pool = SqlitePool::connect("sqlite::memory:")
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to create in-memory SQLite: {}", e)))?;
        
        let storage = Self { pool };
        storage.initialize_schema().await?;
        
        Ok(storage)
    }
    
    /// Initialize the database schema
    async fn initialize_schema(&self) -> Result<()> {
        // Create objects table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                id BLOB PRIMARY KEY,
                object_type INTEGER NOT NULL,
                data BLOB NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(format!("Failed to create objects table: {}", e)))?;
        
        // Create references table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS refs (
                name TEXT PRIMARY KEY,
                target_type INTEGER NOT NULL,
                target_value BLOB NOT NULL,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(format!("Failed to create references table: {}", e)))?;
        
        // Create indices for performance
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_objects_type ON objects(object_type)")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to create objects index: {}", e)))?;
        
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_refs_target ON refs(target_value)")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to create references index: {}", e)))?;
        
        Ok(())
    }
    
    /// Get the number of stored objects (for testing)
    pub async fn object_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM objects")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to count objects: {}", e)))?;
        
        Ok(row.get("count"))
    }
    
    /// Get the number of stored references (for testing)
    pub async fn reference_count(&self) -> Result<i64> {
        let row = sqlx::query("SELECT COUNT(*) as count FROM refs")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to count references: {}", e)))?;
        
        Ok(row.get("count"))
    }
    
    /// Clear all stored data (for testing)
    pub async fn clear(&self) -> Result<()> {
        sqlx::query("DELETE FROM objects")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to clear objects: {}", e)))?;
        
        sqlx::query("DELETE FROM refs")
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to clear references: {}", e)))?;
        
        Ok(())
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn store_object(&self, id: &ObjectId, object: &GitObject) -> Result<()> {
        // Verify that the object hash matches the provided ID
        let computed_id = object.canonical_hash();
        if computed_id != *id {
            return Err(StorageError::CorruptionDetected {
                id: *id,
                details: format!("Object hash mismatch: expected {}, got {}", id, computed_id),
            });
        }
        
        // Serialize the object
        let data = bincode::serialize(object)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize object: {}", e)))?;
        
        let object_type = object.object_type() as i32;
        let id_bytes = id.as_bytes();
        
        sqlx::query(
            "INSERT OR REPLACE INTO objects (id, object_type, data) VALUES (?, ?, ?)"
        )
        .bind(id_bytes.as_slice())
        .bind(object_type)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(format!("Failed to store object: {}", e)))?;
        
        Ok(())
    }
    
    async fn load_object(&self, id: &ObjectId) -> Result<Option<GitObject>> {
        let id_bytes = id.as_bytes();
        
        let row = sqlx::query("SELECT data FROM objects WHERE id = ?")
            .bind(id_bytes.as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to load object: {}", e)))?;
        
        match row {
            Some(row) => {
                let data: Vec<u8> = row.get("data");
                let object = bincode::deserialize(&data)
                    .map_err(|e| StorageError::Serialization(format!("Failed to deserialize object: {}", e)))?;
                Ok(Some(object))
            }
            None => Ok(None),
        }
    }
    
    async fn list_refs(&self) -> Result<Vec<Reference>> {
        let rows = sqlx::query("SELECT name, target_type, target_value FROM refs ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to list references: {}", e)))?;
        
        let mut references = Vec::new();
        for row in rows {
            let name: String = row.get("name");
            let target_type: i32 = row.get("target_type");
            let target_value: Vec<u8> = row.get("target_value");
            
            let target = match target_type {
                0 => {
                    // Direct reference
                    if target_value.len() != 32 {
                        return Err(StorageError::CorruptionDetected {
                            id: ObjectId::from_canonical_bytes(b"invalid"),
                            details: format!("Invalid ObjectId length: {}", target_value.len()),
                        });
                    }
                    let mut id_bytes = [0u8; 32];
                    id_bytes.copy_from_slice(&target_value);
                    ReferenceTarget::Direct(ObjectId::from_blake3_bytes(id_bytes))
                }
                1 => {
                    // Symbolic reference
                    let target_name = String::from_utf8(target_value)
                        .map_err(|e| StorageError::Serialization(format!("Invalid UTF-8 in symbolic reference: {}", e)))?;
                    ReferenceTarget::Symbolic(target_name)
                }
                _ => {
                    return Err(StorageError::CorruptionDetected {
                        id: ObjectId::from_canonical_bytes(b"invalid"),
                        details: format!("Invalid reference target type: {}", target_type),
                    });
                }
            };
            
            references.push(Reference { name, target });
        }
        
        Ok(references)
    }
    
    async fn update_ref(&self, name: &str, target: &ObjectId) -> Result<()> {
        let target_value = target.as_bytes().as_slice();
        
        sqlx::query(
            "INSERT OR REPLACE INTO refs (name, target_type, target_value) VALUES (?, ?, ?)"
        )
        .bind(name)
        .bind(0i32) // Direct reference type
        .bind(target_value)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(format!("Failed to update reference: {}", e)))?;
        
        Ok(())
    }
    
    async fn delete_ref(&self, name: &str) -> Result<()> {
        let result = sqlx::query("DELETE FROM refs WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to delete reference: {}", e)))?;
        
        if result.rows_affected() == 0 {
            return Err(StorageError::RefNotFound { name: name.to_string() });
        }
        
        Ok(())
    }
    
    async fn transaction(&self) -> Result<Box<dyn Transaction>> {
        Ok(Box::new(SqliteTransaction::new(self.pool.clone())))
    }
}

/// SQLite-based transaction implementation
pub struct SqliteTransaction {
    pool: SqlitePool,
    staged_operations: Vec<StagedOperation>,
    completed: bool,
}

#[derive(Debug, Clone)]
enum StagedOperation {
    StoreObject { id: ObjectId, object: GitObject },
    UpdateRef { name: String, target: ObjectId },
}

impl SqliteTransaction {
    fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            staged_operations: Vec::new(),
            completed: false,
        }
    }
    
    fn ensure_not_completed(&self) -> Result<()> {
        if self.completed {
            return Err(StorageError::TransactionFailed {
                reason: "Transaction already completed".to_string(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Transaction for SqliteTransaction {
    async fn store_object(&mut self, id: &ObjectId, object: &GitObject) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Verify that the object hash matches the provided ID
        let computed_id = object.canonical_hash();
        if computed_id != *id {
            return Err(StorageError::CorruptionDetected {
                id: *id,
                details: format!("Object hash mismatch: expected {}, got {}", id, computed_id),
            });
        }
        
        // Stage the operation
        self.staged_operations.push(StagedOperation::StoreObject {
            id: *id,
            object: object.clone(),
        });
        
        Ok(())
    }
    
    async fn update_ref(&mut self, name: &str, target: &ObjectId) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Stage the operation
        self.staged_operations.push(StagedOperation::UpdateRef {
            name: name.to_string(),
            target: *target,
        });
        
        Ok(())
    }
    
    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Begin a real SQLite transaction
        let mut tx = self.pool.begin()
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to begin transaction: {}", e)))?;
        
        // Execute all staged operations
        for operation in &self.staged_operations {
            match operation {
                StagedOperation::StoreObject { id, object } => {
                    let data = bincode::serialize(object)
                        .map_err(|e| StorageError::Serialization(format!("Failed to serialize object: {}", e)))?;
                    
                    let object_type = object.object_type() as i32;
                    let id_bytes = id.as_bytes();
                    
                    sqlx::query(
                        "INSERT OR REPLACE INTO objects (id, object_type, data) VALUES (?, ?, ?)"
                    )
                    .bind(id_bytes.as_slice())
                    .bind(object_type)
                    .bind(data)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Backend(format!("Failed to store object in transaction: {}", e)))?;
                }
                StagedOperation::UpdateRef { name, target } => {
                    let target_value = target.as_bytes().as_slice();
                    
                    sqlx::query(
                        "INSERT OR REPLACE INTO refs (name, target_type, target_value) VALUES (?, ?, ?)"
                    )
                    .bind(name)
                    .bind(0i32) // Direct reference type
                    .bind(target_value)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| StorageError::Backend(format!("Failed to update reference in transaction: {}", e)))?;
                }
            }
        }
        
        // Commit the transaction
        tx.commit()
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to commit transaction: {}", e)))?;
        
        self.completed = true;
        Ok(())
    }
    
    async fn rollback(mut self: Box<Self>) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Simply discard all staged operations
        self.staged_operations.clear();
        self.completed = true;
        
        Ok(())
    }
}
