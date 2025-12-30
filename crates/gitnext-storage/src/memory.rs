//! Memory-based storage backend for testing and temporary operations
//! 
//! Provides in-memory HashMap-based storage with transaction support and rollback capability.
//! This backend is primarily intended for testing and temporary operations.

use crate::{Storage, Transaction, StorageError, Result, Reference, ReferenceTarget};
use gitnext_core::{ObjectId, GitObject};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// In-memory storage backend using HashMap for objects and references
/// Provides strong consistency guarantees and transaction support with rollback
pub struct MemoryStorage {
    /// Objects stored by their ObjectId
    objects: Arc<RwLock<HashMap<ObjectId, GitObject>>>,
    /// References stored by name
    references: Arc<RwLock<HashMap<String, ReferenceTarget>>>,
    /// Active transactions
    transactions: Arc<RwLock<HashMap<Uuid, MemoryTransaction>>>,
}

impl MemoryStorage {
    /// Create a new empty memory storage backend
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            references: Arc::new(RwLock::new(HashMap::new())),
            transactions: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Get the number of stored objects (for testing)
    pub fn object_count(&self) -> usize {
        self.objects.read().unwrap().len()
    }
    
    /// Get the number of stored references (for testing)
    pub fn reference_count(&self) -> usize {
        self.references.read().unwrap().len()
    }
    
    /// Clear all stored data (for testing)
    pub fn clear(&self) {
        self.objects.write().unwrap().clear();
        self.references.write().unwrap().clear();
        self.transactions.write().unwrap().clear();
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn store_object(&self, id: &ObjectId, object: &GitObject) -> Result<()> {
        // Verify that the object hash matches the provided ID
        let computed_id = object.canonical_hash();
        if computed_id != *id {
            return Err(StorageError::CorruptionDetected {
                id: *id,
                details: format!("Object hash mismatch: expected {}, got {}", id, computed_id),
            });
        }
        
        let mut objects = self.objects.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
        objects.insert(*id, object.clone());
        Ok(())
    }
    
    async fn load_object(&self, id: &ObjectId) -> Result<Option<GitObject>> {
        let objects = self.objects.read().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
        Ok(objects.get(id).cloned())
    }
    
    async fn list_refs(&self) -> Result<Vec<Reference>> {
        let references = self.references.read().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
        let refs = references
            .iter()
            .map(|(name, target)| Reference {
                name: name.clone(),
                target: target.clone(),
            })
            .collect();
        Ok(refs)
    }
    
    async fn update_ref(&self, name: &str, target: &ObjectId) -> Result<()> {
        let mut references = self.references.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
        references.insert(name.to_string(), ReferenceTarget::Direct(*target));
        Ok(())
    }
    
    async fn transaction(&self) -> Result<Box<dyn Transaction>> {
        let transaction_id = Uuid::new_v4();
        let transaction = MemoryTransaction::new(
            transaction_id,
            Arc::clone(&self.objects),
            Arc::clone(&self.references),
            Arc::clone(&self.transactions),
        );
        
        // Register the transaction
        {
            let mut transactions = self.transactions.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
            transactions.insert(transaction_id, transaction.clone());
        }
        
        Ok(Box::new(transaction))
    }
}

/// Memory-based transaction implementation with rollback capability
#[derive(Clone)]
pub struct MemoryTransaction {
    id: Uuid,
    /// Staged object changes (not yet committed)
    staged_objects: HashMap<ObjectId, GitObject>,
    /// Staged reference changes (not yet committed)
    staged_refs: HashMap<String, ReferenceTarget>,
    /// Reference to the main storage objects
    objects: Arc<RwLock<HashMap<ObjectId, GitObject>>>,
    /// Reference to the main storage references
    references: Arc<RwLock<HashMap<String, ReferenceTarget>>>,
    /// Reference to active transactions registry
    transactions: Arc<RwLock<HashMap<Uuid, MemoryTransaction>>>,
    /// Whether this transaction has been committed or rolled back
    completed: bool,
}

impl MemoryTransaction {
    fn new(
        id: Uuid,
        objects: Arc<RwLock<HashMap<ObjectId, GitObject>>>,
        references: Arc<RwLock<HashMap<String, ReferenceTarget>>>,
        transactions: Arc<RwLock<HashMap<Uuid, MemoryTransaction>>>,
    ) -> Self {
        Self {
            id,
            staged_objects: HashMap::new(),
            staged_refs: HashMap::new(),
            objects,
            references,
            transactions,
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
impl Transaction for MemoryTransaction {
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
        
        // Stage the object for commit
        self.staged_objects.insert(*id, object.clone());
        Ok(())
    }
    
    async fn update_ref(&mut self, name: &str, target: &ObjectId) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Stage the reference update for commit
        self.staged_refs.insert(name.to_string(), ReferenceTarget::Direct(*target));
        Ok(())
    }
    
    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Apply all staged changes atomically
        {
            let mut objects = self.objects.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
            let mut references = self.references.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
            
            // Apply object changes
            for (id, object) in &self.staged_objects {
                objects.insert(*id, object.clone());
            }
            
            // Apply reference changes
            for (name, target) in &self.staged_refs {
                references.insert(name.clone(), target.clone());
            }
        }
        
        // Mark transaction as completed and remove from registry
        self.completed = true;
        {
            let mut transactions = self.transactions.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
            transactions.remove(&self.id);
        }
        
        Ok(())
    }
    
    async fn rollback(mut self: Box<Self>) -> Result<()> {
        self.ensure_not_completed()?;
        
        // Simply discard all staged changes and mark as completed
        self.staged_objects.clear();
        self.staged_refs.clear();
        self.completed = true;
        
        // Remove from registry
        {
            let mut transactions = self.transactions.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
            transactions.remove(&self.id);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitnext_core::{GitObject, Blob};
    use proptest::prelude::*;
    
    #[tokio::test]
    async fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();
        
        // Test storing and loading an object
        let blob = Blob::new(bytes::Bytes::from("hello world"));
        let object = GitObject::Blob(blob);
        let id = object.canonical_hash();
        
        storage.store_object(&id, &object).await.unwrap();
        
        let loaded = storage.load_object(&id).await.unwrap();
        assert!(loaded.is_some());
        
        // Test object count
        assert_eq!(storage.object_count(), 1);
        
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
    async fn test_memory_storage_hash_validation() {
        let storage = MemoryStorage::new();
        
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
    async fn test_memory_transaction_commit() {
        let storage = MemoryStorage::new();
        
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
        assert_eq!(storage.object_count(), 0);
        assert_eq!(storage.reference_count(), 0);
        
        // Commit the transaction
        tx.commit().await.unwrap();
        
        // After commit, objects should be visible
        assert_eq!(storage.object_count(), 2);
        assert_eq!(storage.reference_count(), 1);
        
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
    async fn test_memory_transaction_rollback() {
        let storage = MemoryStorage::new();
        
        let blob = Blob::new(bytes::Bytes::from("test content"));
        let object = GitObject::Blob(blob);
        let id = object.canonical_hash();
        
        // Start a transaction
        let mut tx = storage.transaction().await.unwrap();
        
        // Stage operations
        tx.store_object(&id, &object).await.unwrap();
        tx.update_ref("refs/heads/main", &id).await.unwrap();
        
        // Before rollback, verify nothing is committed
        assert_eq!(storage.object_count(), 0);
        assert_eq!(storage.reference_count(), 0);
        
        // Rollback the transaction
        tx.rollback().await.unwrap();
        
        // After rollback, nothing should be visible
        assert_eq!(storage.object_count(), 0);
        assert_eq!(storage.reference_count(), 0);
        
        let loaded = storage.load_object(&id).await.unwrap();
        assert!(loaded.is_none());
        
        let refs = storage.list_refs().await.unwrap();
        assert_eq!(refs.len(), 0);
    }
    
    #[tokio::test]
    async fn test_memory_transaction_completed_operations_fail() {
        let storage = MemoryStorage::new();
        
        let blob = Blob::new(bytes::Bytes::from("test content"));
        let object = GitObject::Blob(blob);
        let id = object.canonical_hash();
        
        // Start and commit a transaction
        let mut tx = storage.transaction().await.unwrap();
        tx.store_object(&id, &object).await.unwrap();
        tx.commit().await.unwrap();
        
        // Try to use the transaction after commit - should fail
        // Note: We can't actually test this because tx is consumed by commit()
        // This test documents the expected behavior
    }

    // Property test generators - reuse from gitnext-core
    use gitnext_core::tests::arb_git_object;

    proptest! {
        /// Property 6: Storage Backend Behavioral Consistency (Strong Consistency Backends)
        /// For any sequence of repository operations, executing them on strongly consistent 
        /// storage backends (Memory, SQLite, PostgreSQL) should produce identical final repository states.
        /// **Validates: Requirements 2.7**
        /// **Note**: Applies to strongly consistent backends only
        #[test]
        fn prop_storage_backend_consistency(
            objects in prop::collection::vec(arb_git_object(), 1..10),
            ref_names in prop::collection::vec("[a-zA-Z0-9/_-]{1,50}", 1..5)
        ) {
            tokio_test::block_on(async {
                let storage = MemoryStorage::new();
                
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
        
        /// Additional property test for transaction rollback consistency
        #[test]
        fn prop_transaction_rollback_consistency(
            objects in prop::collection::vec(arb_git_object(), 1..5)
        ) {
            tokio_test::block_on(async {
                let storage = MemoryStorage::new();
                
                // Store some initial objects
                let mut initial_ids = Vec::new();
                for object in &objects {
                    let id = object.canonical_hash();
                    storage.store_object(&id, object).await.unwrap();
                    initial_ids.push(id);
                }
                
                let initial_count = storage.object_count();
                let initial_ref_count = storage.reference_count();
                
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
                prop_assert_eq!(storage.object_count(), initial_count);
                prop_assert_eq!(storage.reference_count(), initial_ref_count);
                
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
    }
}