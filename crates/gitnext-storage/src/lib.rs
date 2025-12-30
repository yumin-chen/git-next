use async_trait::async_trait;
use gitnext_core::*;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Object not found: {id}")]
    ObjectNotFound { id: ObjectId },

    #[error("Reference not found: {name}")]
    RefNotFound { name: String },

    #[error("Transaction failed: {reason}")]
    TransactionFailed { reason: String },

    #[error("Corruption detected in object {id}: {details}")]
    CorruptionDetected { id: ObjectId, details: String },

    #[error("Storage backend unavailable: {backend}")]
    BackendUnavailable { backend: String },

    #[error("Concurrent modification detected")]
    ConcurrentModification,

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Unified Storage trait for all backend implementations (ADR-002)
/// Provides async operations for storing and retrieving Git objects and references
#[async_trait]
pub trait Storage: Send + Sync {
    /// Store a Git object with its computed ObjectId
    async fn store_object(&self, id: &ObjectId, object: &GitObject) -> Result<()>;
    
    /// Load a Git object by its ObjectId
    async fn load_object(&self, id: &ObjectId) -> Result<Option<GitObject>>;
    
    /// List all references with optional prefix filter
    async fn list_refs(&self) -> Result<Vec<Reference>>;
    
    /// Update a reference to point to a new ObjectId
    async fn update_ref(&self, name: &str, target: &ObjectId) -> Result<()>;
    
    /// Begin a new transaction for atomic operations
    async fn transaction(&self) -> Result<Box<dyn Transaction>>;
}

/// Transaction trait for atomic multi-operation updates
/// Provides rollback capability for operation safety (Requirements 10.2, 10.5)
#[async_trait]
pub trait Transaction: Send {
    /// Store an object within the transaction
    async fn store_object(&mut self, id: &ObjectId, object: &GitObject) -> Result<()>;
    
    /// Update a reference within the transaction
    async fn update_ref(&mut self, name: &str, target: &ObjectId) -> Result<()>;
    
    /// Commit all operations in the transaction atomically
    async fn commit(self: Box<Self>) -> Result<()>;
    
    /// Rollback all operations in the transaction
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Reference types for Git references
#[derive(Debug, Clone)]
pub struct Reference {
    pub name: String,
    pub target: ReferenceTarget,
}

#[derive(Debug, Clone)]
pub enum ReferenceTarget {
    Direct(ObjectId),
    Symbolic(String),
}

/// Error recovery mechanisms for storage operations
pub struct RecoveryManager {
    storage: Arc<dyn Storage>,
}

impl RecoveryManager {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
    
    /// Attempt to recover from a failed transaction
    pub async fn recover_transaction(&self) -> Result<()> {
        // Implementation would check for incomplete transactions and clean them up
        // For now, this is a placeholder for the recovery mechanism
        Ok(())
    }
    
    /// Validate storage consistency
    pub async fn validate_consistency(&self) -> Result<Vec<String>> {
        // Implementation would check for orphaned objects, broken references, etc.
        // Returns list of issues found
        Ok(Vec::new())
    }
}

// Backend implementations
pub mod memory;
pub mod sqlite;

pub use memory::MemoryStorage;
pub use sqlite::SqliteStorage;