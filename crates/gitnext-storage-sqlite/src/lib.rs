//! SQLite-based storage backend for local file-based storage
//!
//! Provides persistent storage using SQLite with transaction support and connection pooling.
//! This backend is suitable for local development and single-user scenarios.

use async_trait::async_trait;
use gitnext_core::{GitObject, ObjectId};
use gitnext_storage::{Reference, ReferenceTarget, Storage, StorageError, Transaction};
use sqlx::{sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::path::Path;

// Re-export Result for convenience in the crate
pub type Result<T> = std::result::Result<T, StorageError>;

/// SQLite-based storage backend with connection pooling and transaction support
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Create a new SQLite storage backend with the given database path
    pub async fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let db_url = format!("sqlite:{}", db_path.as_ref().display());

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect(&db_url)
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
            .map_err(|e| {
                StorageError::Backend(format!("Failed to create in-memory SQLite: {}", e))
            })?;

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
                data BLOB NOT NULL
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
                target_value BLOB NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(format!("Failed to create references table: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn store_object(&self, id: &ObjectId, object: &GitObject) -> Result<()> {
        let computed_id = object.canonical_hash();
        if computed_id != *id {
            return Err(StorageError::CorruptionDetected {
                id: *id,
                details: format!("Object hash mismatch: expected {}, got {}", id, computed_id),
            });
        }

        let data = bincode::serialize(object)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize object: {}", e)))?;

        sqlx::query("INSERT OR REPLACE INTO objects (id, data) VALUES (?, ?)")
            .bind(&id.as_bytes()[..])
            .bind(data)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to store object: {}", e)))?;

        Ok(())
    }

    async fn load_object(&self, id: &ObjectId) -> Result<Option<GitObject>> {
        let row = sqlx::query("SELECT data FROM objects WHERE id = ?")
            .bind(&id.as_bytes()[..])
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to load object: {}", e)))?;

        match row {
            Some(row) => {
                let data: Vec<u8> = row.get("data");
                let object = bincode::deserialize(&data).map_err(|e| {
                    StorageError::Serialization(format!("Failed to deserialize object: {}", e))
                })?;
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
                0 => { // Direct
                    let mut id_bytes = [0u8; 32];
                    id_bytes.copy_from_slice(&target_value);
                    ReferenceTarget::Direct(ObjectId::from_blake3_bytes(id_bytes))
                }
                1 => { // Symbolic
                    let target_name = String::from_utf8(target_value).map_err(|e| {
                        StorageError::Serialization(format!(
                            "Invalid UTF-8 in symbolic reference: {}",
                            e
                        ))
                    })?;
                    ReferenceTarget::Symbolic(target_name)
                }
                _ => {
                    return Err(StorageError::CorruptionDetected {
                        id: ObjectId::from_canonical_bytes(b"invalid"), // Placeholder ID
                        details: format!("Invalid reference target type: {}", target_type),
                    });
                }
            };
            references.push(Reference { name, target });
        }
        Ok(references)
    }

    async fn update_ref(&self, name: &str, target: &ObjectId) -> Result<()> {
        sqlx::query(
            "INSERT OR REPLACE INTO refs (name, target_type, target_value) VALUES (?, ?, ?)",
        )
        .bind(name)
        .bind(0i32) // Direct reference type
        .bind(&target.as_bytes()[..])
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(format!("Failed to update reference: {}", e)))?;
        Ok(())
    }

    async fn transaction(&self) -> Result<Box<dyn Transaction>> {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to begin transaction: {}", e)))?;
        Ok(Box::new(SqliteTransaction { tx: Some(tx) }))
    }
}

/// SQLite-based transaction implementation
pub struct SqliteTransaction {
    tx: Option<sqlx::Transaction<'static, sqlx::Sqlite>>,
}

impl SqliteTransaction {
    fn ensure_active(&self) -> Result<()> {
        if self.tx.is_none() {
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
        self.ensure_active()?;
        let computed_id = object.canonical_hash();
        if computed_id != *id {
            return Err(StorageError::CorruptionDetected {
                id: *id,
                details: format!("Object hash mismatch: expected {}, got {}", id, computed_id),
            });
        }

        let data = bincode::serialize(object)
            .map_err(|e| StorageError::Serialization(format!("Failed to serialize object: {}", e)))?;

        sqlx::query("INSERT OR REPLACE INTO objects (id, data) VALUES (?, ?)")
            .bind(&id.as_bytes()[..])
            .bind(data)
            .execute(&mut **self.tx.as_mut().unwrap())
            .await
            .map_err(|e| {
                StorageError::Backend(format!("Failed to store object in transaction: {}", e))
            })?;
        Ok(())
    }

    async fn update_ref(&mut self, name: &str, target: &ObjectId) -> Result<()> {
        self.ensure_active()?;
        sqlx::query(
            "INSERT OR REPLACE INTO refs (name, target_type, target_value) VALUES (?, ?, ?)",
        )
        .bind(name)
        .bind(0i32) // Direct reference type
        .bind(&target.as_bytes()[..])
        .execute(&mut **self.tx.as_mut().unwrap())
        .await
        .map_err(|e| {
            StorageError::Backend(format!(
                "Failed to update reference in transaction: {}",
                e
            ))
        })?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.ensure_active()?;
        self.tx
            .take()
            .unwrap()
            .commit()
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to commit transaction: {}", e)))
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        self.ensure_active()?;
        self.tx
            .take()
            .unwrap()
            .rollback()
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to rollback transaction: {}", e)))
    }
}
