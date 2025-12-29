use async_trait::async_trait;
use gitnext_core::*;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Object not found: {0}")]
    ObjectNotFound(Hash),

    #[error("Reference not found: {0}")]
    RefNotFound(String),

    #[error("Object already exists: {0}")]
    ObjectExists(Hash),

    #[error("Reference update conflict: {0}")]
    RefUpdateConflict(String),

    #[error("Transaction conflict")]
    TransactionConflict,

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Core storage backend trait
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Object operations
    async fn write_object(&self, hash: Hash, object: &Object) -> Result<()>;
    async fn read_object(&self, hash: Hash) -> Result<Object>;
    async fn exists_object(&self, hash: Hash) -> Result<bool>;

    /// Batch operations (optimized for bulk writes/reads)
    async fn write_objects(&self, objects: Vec<(Hash, Object)>) -> Result<()> {
        for (hash, obj) in objects {
            self.write_object(hash, &obj).await?;
        }
        Ok(())
    }

    async fn read_objects(&self, hashes: Vec<Hash>) -> Result<Vec<Object>> {
        let mut results = Vec::with_capacity(hashes.len());
        for hash in hashes {
            results.push(self.read_object(hash).await?);
        }
        Ok(results)
    }

    /// Reference operations (atomic CAS)
    async fn read_ref(&self, name: &str) -> Result<Reference>;
    async fn update_ref(&self, name: &str, old: Option<Hash>, new: Hash) -> Result<()>;
    async fn delete_ref(&self, name: &str) -> Result<()>;
    async fn list_refs(&self, prefix: Option<&str>) -> Result<Vec<(String, Hash)>>;

    /// Commit graph operations (for fast queries)
    async fn write_commit_node(&self, hash: Hash, node: &CommitNode) -> Result<()>;
    async fn read_commit_node(&self, hash: Hash) -> Result<CommitNode>;
    async fn query_commits(&self, query: &CommitQuery) -> Result<Vec<Hash>>;

    /// Artifact tracking
    async fn write_artifact_version(&self, version: &ArtifactVersion) -> Result<()>;
    async fn read_artifact_versions(&self, id: ArtifactId) -> Result<Vec<ArtifactVersion>>;
    async fn query_artifacts(&self, query: &ArtifactQuery) -> Result<Vec<ArtifactId>>;

    /// Operation log
    async fn append_operation(&self, op: &Operation) -> Result<()>;
    async fn read_operations(&self, since: Option<OperationId>) -> Result<Vec<Operation>>;

    /// Transaction support
    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>>;

    /// Metadata
    async fn read_metadata(&self) -> Result<RepoMetadata>;
    async fn write_metadata(&self, meta: &RepoMetadata) -> Result<()>;

    /// Garbage collection
    async fn find_unreachable(&self, roots: Vec<Hash>) -> Result<Vec<Hash>>;
    async fn delete_objects(&self, hashes: Vec<Hash>) -> Result<()>;
}

/// Transaction for atomic multi-operation updates
#[async_trait]
pub trait Transaction: Send {
    async fn write_object(&mut self, hash: Hash, object: &Object) -> Result<()>;
    async fn update_ref(&mut self, name: &str, old: Option<Hash>, new: Hash) -> Result<()>;
    async fn commit(self: Box<Self>) -> Result<()>;
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Commit graph node (for fast ancestry queries)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommitNode {
    pub hash: Hash,
    pub parents: Vec<Hash>,
    pub generation: u64,
    pub author_timestamp: i64,
    pub committer_timestamp: i64,
    pub tree: Hash,
}

/// Query builder for commits
#[derive(Debug, Clone, Default)]
pub struct CommitQuery {
    pub since_timestamp: Option<i64>,
    pub until_timestamp: Option<i64>,
    pub author: Option<String>,
    pub message_pattern: Option<String>,
    pub min_generation: Option<u64>,
    pub max_generation: Option<u64>,
    pub limit: Option<usize>,
}

/// Artifact version (stable identity tracking)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactVersion {
    pub artifact_id: ArtifactId,
    pub commit: Hash,
    pub content_hash: Hash,
    pub metadata: ArtifactMetadata,
}

/// Query builder for artifacts
#[derive(Debug, Clone, Default)]
pub struct ArtifactQuery {
    pub artifact_type: Option<ArtifactType>,
    pub name_pattern: Option<String>,
    pub path_pattern: Option<String>,
    pub language: Option<String>,
}

/// Caching layer wrapper
pub struct CachedBackend<B: StorageBackend> {
    inner: B,
    object_cache: Arc<dashmap::DashMap<Hash, Object>>,
    ref_cache: Arc<dashmap::DashMap<String, Reference>>,
}

impl<B: StorageBackend> CachedBackend<B> {
    pub fn new(backend: B, capacity: usize) -> Self {
        Self {
            inner: backend,
            object_cache: Arc::new(dashmap::DashMap::with_capacity(capacity)),
            ref_cache: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub fn invalidate_ref(&self, name: &str) {
        self.ref_cache.remove(name);
    }

    pub fn clear_cache(&self) {
        self.object_cache.clear();
        self.ref_cache.clear();
    }
}

#[async_trait]
impl<B: StorageBackend> StorageBackend for CachedBackend<B> {
    async fn read_object(&self, hash: Hash) -> Result<Object> {
        // Check cache first
        if let Some(obj) = self.object_cache.get(&hash) {
            return Ok(obj.clone());
        }

        // Cache miss: read from backend
        let obj = self.inner.read_object(hash).await?;
        self.object_cache.insert(hash, obj.clone());
        Ok(obj)
    }

    async fn write_object(&self, hash: Hash, object: &Object) -> Result<()> {
        self.inner.write_object(hash, object).await?;
        self.object_cache.insert(hash, object.clone());
        Ok(())
    }

    async fn read_ref(&self, name: &str) -> Result<Reference> {
        if let Some(ref_val) = self.ref_cache.get(name) {
            return Ok(ref_val.clone());
        }

        let ref_val = self.inner.read_ref(name).await?;
        self.ref_cache.insert(name.to_string(), ref_val.clone());
        Ok(ref_val)
    }

    async fn update_ref(&self, name: &str, old: Option<Hash>, new: Hash) -> Result<()> {
        self.inner.update_ref(name, old, new).await?;
        self.invalidate_ref(name);
        Ok(())
    }

    // Delegate other methods...
    async fn exists_object(&self, hash: Hash) -> Result<bool> {
        if self.object_cache.contains_key(&hash) {
            return Ok(true);
        }
        self.inner.exists_object(hash).await
    }

    async fn delete_ref(&self, name: &str) -> Result<()> {
        self.inner.delete_ref(name).await?;
        self.invalidate_ref(name);
        Ok(())
    }

    async fn list_refs(&self, prefix: Option<&str>) -> Result<Vec<(String, Hash)>> {
        self.inner.list_refs(prefix).await
    }

    async fn write_commit_node(&self, hash: Hash, node: &CommitNode) -> Result<()> {
        self.inner.write_commit_node(hash, node).await
    }

    async fn read_commit_node(&self, hash: Hash) -> Result<CommitNode> {
        self.inner.read_commit_node(hash).await
    }

    async fn query_commits(&self, query: &CommitQuery) -> Result<Vec<Hash>> {
        self.inner.query_commits(query).await
    }

    async fn write_artifact_version(&self, version: &ArtifactVersion) -> Result<()> {
        self.inner.write_artifact_version(version).await
    }

    async fn read_artifact_versions(&self, id: ArtifactId) -> Result<Vec<ArtifactVersion>> {
        self.inner.read_artifact_versions(id).await
    }

    async fn query_artifacts(&self, query: &ArtifactQuery) -> Result<Vec<ArtifactId>> {
        self.inner.query_artifacts(query).await
    }

    async fn append_operation(&self, op: &Operation) -> Result<()> {
        self.inner.append_operation(op).await
    }

    async fn read_operations(&self, since: Option<OperationId>) -> Result<Vec<Operation>> {
        self.inner.read_operations(since).await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        self.inner.begin_transaction().await
    }

    async fn read_metadata(&self) -> Result<RepoMetadata> {
        self.inner.read_metadata().await
    }

    async fn write_metadata(&self, meta: &RepoMetadata) -> Result<()> {
        self.inner.write_metadata(meta).await
    }

    async fn find_unreachable(&self, roots: Vec<Hash>) -> Result<Vec<Hash>> {
        self.inner.find_unreachable(roots).await
    }

    async fn delete_objects(&self, hashes: Vec<Hash>) -> Result<()> {
        self.inner.delete_objects(hashes).await
    }
}

/// Parallel batch reader
pub struct BatchReader<B: StorageBackend> {
    backend: Arc<B>,
    concurrency: usize,
}

impl<B: StorageBackend> BatchReader<B> {
    pub fn new(backend: Arc<B>, concurrency: usize) -> Self {
        Self { backend, concurrency }
    }

    pub async fn read_objects(&self, hashes: Vec<Hash>) -> Result<Vec<Object>> {
        use futures::stream::{self, StreamExt};

        stream::iter(hashes)
            .map(|hash| {
                let backend = self.backend.clone();
                async move { backend.read_object(hash).await }
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
    }
}
