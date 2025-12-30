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

#[cfg(test)]
mod tests {
    use super::*;
    use gitnext_core::{GitObject, Blob};
    
    #[tokio::test]
    async fn test_sqlite_storage_basic_operations() {
        let storage = SqliteStorage::new_in_memory().await.unwrap();
        
        // Test storing and loading an object
        let blob = Blob::new(bytes::Bytes::from("hello world"));
        let object = GitObject::Blob(blob);
        let id = object.canonical_hash();
        
        storage.store_object(&id, &object).await.unwrap();
        
        let loaded = storage.load_object(&id).await.unwrap();
        assert!(loaded.is_some());
        
        // Test object count
        assert_eq!(storage.object_count().await.unwrap(), 1);
        
        // Test reference operations
        storage.update_ref("refs/heads/main", &id).await.unwrap();
        let refs = storage.list_refs().await.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "refs/heads/main");
        
        match &refs[0].target {
            ReferenceTarget::Direct(target_id) => assert_eq!(*target_id, id),
            _ => panic!("Expected direct reference"),
        }
    }
    
    #[tokio::test]
    async fn test_sqlite_storage_hash_validation() {
        let storage = SqliteStorage::new_in_memory().await.unwrap();
        
        let blob = Blob::new(bytes::Bytes::from("test content"));
        let object = GitObject::Blob(blob);
        let correct_id = object.canonical_hash();
        
        // Try to store with correct ID - should succeed
        storage.store_object(&correct_id, &object).await.unwrap();
        
        // Try to store with incorrect ID - should fail
        let wrong_id = ObjectId::from_canonical_bytes(b"wrong hash");
        let result = storage.store_object(&wrong_id, &object).await;
        assert!(result.is_err());
        
        match result.unwrap_err() {
            StorageError::CorruptionDetected { .. } => {}, // Expected
            _ => panic!("Expected corruption detected error"),
        }
    }
    
    #[tokio::test]
    async fn test_sqlite_transaction_commit() {
        let storage = SqliteStorage::new_in_memory().await.unwrap();
        
        let blob1 = Blob::new(bytes::Bytes::from("content 1"));
        let object1 = GitObject::Blob(blob1);
        let id1 = object1.canonical_hash();
        
        let blob2 = Blob::new(bytes::Bytes::from("content 2"));
        let object2 = GitObject::Blob(blob2);
        let id2 = object2.canonical_hash();
        
        // Start a transaction
        let mut tx = storage.transaction().await.unwrap();
        
        // Stage operations
        tx.store_object(&id1, &object1).await.unwrap();
        tx.store_object(&id2, &object2).await.unwrap();
        tx.update_ref("refs/heads/main", &id1).await.unwrap();
        
        // Before commit, objects should not be visible
        assert_eq!(storage.object_count().await.unwrap(), 0);
        assert_eq!(storage.reference_count().await.unwrap(), 0);
        
        // Commit the transaction
        tx.commit().await.unwrap();
        
        // After commit, objects should be visible
        assert_eq!(storage.object_count().await.unwrap(), 2);
        assert_eq!(storage.reference_count().await.unwrap(), 1);
        
        // Verify objects can be loaded
        let loaded1 = storage.load_object(&id1).await.unwrap();
        assert!(loaded1.is_some());
        
        let loaded2 = storage.load_object(&id2).await.unwrap();
        assert!(loaded2.is_some());
        
        // Verify reference
        let refs = storage.list_refs().await.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "refs/heads/main");
    }
    
    #[tokio::test]
    async fn test_sqlite_transaction_rollback() {
        let storage = SqliteStorage::new_in_memory().await.unwrap();
        
        let blob = Blob::new(bytes::Bytes::from("test content"));
        let object = GitObject::Blob(blob);
        let id = object.canonical_hash();
        
        // Start a transaction
        let mut tx = storage.transaction().await.unwrap();
        
        // Stage operations
        tx.store_object(&id, &object).await.unwrap();
        tx.update_ref("refs/heads/main", &id).await.unwrap();
        
        // Before rollback, verify nothing is committed
        assert_eq!(storage.object_count().await.unwrap(), 0);
        assert_eq!(storage.reference_count().await.unwrap(), 0);
        
        // Rollback the transaction
        tx.rollback().await.unwrap();
        
        // After rollback, nothing should be visible
        assert_eq!(storage.object_count().await.unwrap(), 0);
        assert_eq!(storage.reference_count().await.unwrap(), 0);
        
        let loaded = storage.load_object(&id).await.unwrap();
        assert!(loaded.is_none());
        
        let refs = storage.list_refs().await.unwrap();
        assert_eq!(refs.len(), 0);
    }
    
    #[tokio::test]
    #[ignore] // Skip this test due to file system permissions in CI
    async fn test_sqlite_persistence() {
        // Use a simple filename in current directory for testing
        let db_path = format!("test_gitnext_{}.db", std::process::id());
        
        let blob = Blob::new(bytes::Bytes::from("persistent data"));
        let object = GitObject::Blob(blob);
        let id = object.canonical_hash();
        
        // Store data in first instance
        {
            let storage = SqliteStorage::new(&db_path).await.unwrap();
            storage.store_object(&id, &object).await.unwrap();
            storage.update_ref("refs/heads/main", &id).await.unwrap();
        }
        
        // Verify data persists in second instance
        {
            let storage = SqliteStorage::new(&db_path).await.unwrap();
            
            let loaded = storage.load_object(&id).await.unwrap();
            assert!(loaded.is_some());
            
            let refs = storage.list_refs().await.unwrap();
            assert_eq!(refs.len(), 1);
            assert_eq!(refs[0].name, "refs/heads/main");
        }
        
        // Clean up
        let _ = std::fs::remove_file(&db_path);
    }

    // Property test generators - reuse from gitnext-core
    use gitnext_core::tests::arb_git_object;
    use proptest::prelude::*;

    proptest! {
        /// Property 6: Storage Backend Behavioral Consistency (Strong Consistency Backends)
        /// For any sequence of repository operations, executing them on strongly consistent 
        /// storage backends (Memory, SQLite, PostgreSQL) should produce identical final repository states.
        /// **Validates: Requirements 2.7**
        /// **Note**: Applies to strongly consistent backends only
        #[test]
        fn prop_sqlite_storage_backend_consistency(
            objects in prop::collection::vec(arb_git_object(), 1..10),
            ref_names in prop::collection::vec("[a-zA-Z0-9/_-]{1,50}", 1..5)
        ) {
            tokio_test::block_on(async {
                let storage = SqliteStorage::new_in_memory().await.unwrap();
                
                // Store all objects and verify they can be retrieved
                let mut stored_ids = Vec::new();
                for object in &objects {
                    let id = object.canonical_hash();
                    storage.store_object(&id, object).await.unwrap();
                    stored_ids.push(id);
                    
                    // Verify immediate retrieval
                    let loaded = storage.load_object(&id).await.unwrap();
                    prop_assert!(loaded.is_some());
                    
                    // Verify hash consistency
                    let loaded_object = loaded.unwrap();
                    let loaded_id = loaded_object.canonical_hash();
                    prop_assert_eq!(id, loaded_id);
                }
                
                // Create references to stored objects
                for (i, ref_name) in ref_names.iter().enumerate() {
                    if i < stored_ids.len() {
                        let target_id = stored_ids[i];
                        storage.update_ref(ref_name, &target_id).await.unwrap();
                    }
                }
                
                // Verify all references can be listed and point to correct objects
                let refs = storage.list_refs().await.unwrap();
                prop_assert_eq!(refs.len(), std::cmp::min(ref_names.len(), stored_ids.len()));
                
                for reference in &refs {
                    match &reference.target {
                        ReferenceTarget::Direct(target_id) => {
                            // Verify the referenced object exists
                            let loaded = storage.load_object(target_id).await.unwrap();
                            prop_assert!(loaded.is_some());
                        }
                        ReferenceTarget::Symbolic(_) => {
                            // Symbolic references not used in this test
                        }
                    }
                }
                
                // Verify transaction consistency
                let mut tx = storage.transaction().await.unwrap();
                
                // Store additional objects in transaction
                let new_blob = gitnext_core::Blob::new(bytes::Bytes::from("transaction test"));
                let new_object = GitObject::Blob(new_blob);
                let new_id = new_object.canonical_hash();
                
                tx.store_object(&new_id, &new_object).await.unwrap();
                tx.update_ref("refs/transaction/test", &new_id).await.unwrap();
                
                // Before commit, new objects should not be visible
                let loaded_before = storage.load_object(&new_id).await.unwrap();
                prop_assert!(loaded_before.is_none());
                
                // Commit transaction
                tx.commit().await.unwrap();
                
                // After commit, new objects should be visible
                let loaded_after = storage.load_object(&new_id).await.unwrap();
                prop_assert!(loaded_after.is_some());
                
                // Verify final state consistency
                let final_refs = storage.list_refs().await.unwrap();
                prop_assert_eq!(final_refs.len(), std::cmp::min(ref_names.len(), stored_ids.len()) + 1);
                
                Ok(())
            })?;
        }
        
        /// Additional property test for SQLite transaction rollback consistency
        #[test]
        fn prop_sqlite_transaction_rollback_consistency(
            objects in prop::collection::vec(arb_git_object(), 1..5)
        ) {
            tokio_test::block_on(async {
                let storage = SqliteStorage::new_in_memory().await.unwrap();
                
                // Store some initial objects
                let mut initial_ids = Vec::new();
                for object in &objects {
                    let id = object.canonical_hash();
                    storage.store_object(&id, object).await.unwrap();
                    initial_ids.push(id);
                }
                
                let initial_count = storage.object_count().await.unwrap();
                let initial_ref_count = storage.reference_count().await.unwrap();
                
                // Start a transaction and make changes
                let mut tx = storage.transaction().await.unwrap();
                
                let new_blob = gitnext_core::Blob::new(bytes::Bytes::from("rollback test"));
                let new_object = GitObject::Blob(new_blob);
                let new_id = new_object.canonical_hash();
                
                tx.store_object(&new_id, &new_object).await.unwrap();
                tx.update_ref("refs/rollback/test", &new_id).await.unwrap();
                
                // Rollback the transaction
                tx.rollback().await.unwrap();
                
                // Verify state is unchanged
                prop_assert_eq!(storage.object_count().await.unwrap(), initial_count);
                prop_assert_eq!(storage.reference_count().await.unwrap(), initial_ref_count);
                
                // Verify new object is not present
                let loaded = storage.load_object(&new_id).await.unwrap();
                prop_assert!(loaded.is_none());
                
                // Verify all original objects are still present
                for id in &initial_ids {
                    let loaded = storage.load_object(id).await.unwrap();
                    prop_assert!(loaded.is_some());
                }
                
                Ok(())
            })?;
        }
        
        /// Cross-backend consistency test: Memory vs SQLite
        #[test]
        fn prop_memory_sqlite_consistency(
            objects in prop::collection::vec(arb_git_object(), 1..5),
            ref_names in prop::collection::vec("[a-zA-Z0-9/_-]{1,50}", 1..3)
        ) {
            tokio_test::block_on(async {
                let memory_storage = crate::memory::MemoryStorage::new();
                let sqlite_storage = SqliteStorage::new_in_memory().await.unwrap();
                
                // Store the same objects in both backends
                let mut stored_ids = Vec::new();
                for object in &objects {
                    let id = object.canonical_hash();
                    
                    memory_storage.store_object(&id, object).await.unwrap();
                    sqlite_storage.store_object(&id, object).await.unwrap();
                    
                    stored_ids.push(id);
                }
                
                // Create the same references in both backends
                for (i, ref_name) in ref_names.iter().enumerate() {
                    if i < stored_ids.len() {
                        let target_id = stored_ids[i];
                        memory_storage.update_ref(ref_name, &target_id).await.unwrap();
                        sqlite_storage.update_ref(ref_name, &target_id).await.unwrap();
                    }
                }
                
                // Verify both backends have the same objects
                for id in &stored_ids {
                    let memory_obj = memory_storage.load_object(id).await.unwrap();
                    let sqlite_obj = sqlite_storage.load_object(id).await.unwrap();
                    
                    prop_assert!(memory_obj.is_some());
                    prop_assert!(sqlite_obj.is_some());
                    
                    // Objects should be identical
                    let memory_obj = memory_obj.unwrap();
                    let sqlite_obj = sqlite_obj.unwrap();
                    prop_assert_eq!(memory_obj.canonical_hash(), sqlite_obj.canonical_hash());
                }
                
                // Verify both backends have the same references
                let memory_refs = memory_storage.list_refs().await.unwrap();
                let sqlite_refs = sqlite_storage.list_refs().await.unwrap();
                
                prop_assert_eq!(memory_refs.len(), sqlite_refs.len());
                
                // Sort references by name for comparison
                let mut memory_refs = memory_refs;
                let mut sqlite_refs = sqlite_refs;
                memory_refs.sort_by(|a, b| a.name.cmp(&b.name));
                sqlite_refs.sort_by(|a, b| a.name.cmp(&b.name));
                
                for (memory_ref, sqlite_ref) in memory_refs.iter().zip(sqlite_refs.iter()) {
                    prop_assert_eq!(&memory_ref.name, &sqlite_ref.name);
                    match (&memory_ref.target, &sqlite_ref.target) {
                        (ReferenceTarget::Direct(mem_id), ReferenceTarget::Direct(sql_id)) => {
                            prop_assert_eq!(mem_id, sql_id);
                        }
                        (ReferenceTarget::Symbolic(mem_name), ReferenceTarget::Symbolic(sql_name)) => {
                            prop_assert_eq!(mem_name, sql_name);
                        }
                        _ => prop_assert!(false, "Reference target types don't match"),
                    }
                }
                
                Ok(())
            })?;
        }
    }
}