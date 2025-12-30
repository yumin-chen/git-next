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
    
    async fn delete_ref(&self, name: &str) -> Result<()> {
        let mut references = self.references.write().map_err(|_| StorageError::Backend("Lock poisoned".to_string()))?;
        references.remove(name).ok_or_else(|| StorageError::RefNotFound { name: name.to_string() })?;
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
