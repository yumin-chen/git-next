use gitnext_core::*;
use gitnext_storage::*;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Repository {
    backend: Arc<dyn StorageBackend>,
    metadata: Arc<RwLock<RepoMetadata>>,
    op_log: OperationLog,
}

impl Repository {
    pub async fn open(backend: Arc<dyn StorageBackend>) -> Result<Self> {
        let metadata = backend.read_metadata().await?;

        Ok(Self {
            backend: backend.clone(),
            metadata: Arc::new(RwLock::new(metadata)),
            op_log: OperationLog::new(backend),
        })
    }

    pub async fn init(backend: Arc<dyn StorageBackend>) -> Result<Self> {
        let metadata = RepoMetadata {
            id: RepoId::new(),
            created_at: chrono::Utc::now().timestamp(),
            default_branch: "main".to_string(),
            description: None,
        };

        backend.write_metadata(&metadata).await?;

        let repo = Self::open(backend).await?;
        repo.create_initial_commit().await?;

        Ok(repo)
    }

    async fn create_initial_commit(&self) -> Result<()> {
        let empty_tree = Tree { entries: vec![] };
        let tree_obj = Object::Tree(empty_tree);
        let tree_hash = tree_obj.hash();

        self.backend.write_object(tree_hash, &tree_obj).await?;

        let commit = Commit {
            tree: tree_hash,
            parents: vec![],
            author: Signature {
                name: "System".to_string(),
                email: "system@gitnext".to_string(),
                timestamp: chrono::Utc::now().timestamp(),
                timezone_offset: 0,
            },
            committer: Signature {
                name: "System".to_string(),
                email: "system@gitnext".to_string(),
                timestamp: chrono::Utc::now().timestamp(),
                timezone_offset: 0,
            },
            message: "Initial commit".to_string(),
            generation: 0,
            artifact_changes: vec![],
            signature: None,
        };

        let commit_obj = Object::Commit(commit.clone());
        let commit_hash = commit_obj.hash();

        self.backend.write_object(commit_hash, &commit_obj).await?;

        let node = CommitNode {
            hash: commit_hash,
            parents: vec![],
            generation: 0,
            author_timestamp: commit.author.timestamp,
            committer_timestamp: commit.committer.timestamp,
            tree: tree_hash,
        };
        self.backend.write_commit_node(commit_hash, &node).await?;

        self.backend
            .update_ref("refs/heads/main", None, commit_hash)
            .await?;

        Ok(())
    }

    pub async fn head(&self) -> Result<Hash> {
        match self.backend.read_ref("HEAD").await? {
            Reference::Direct(hash) => Ok(hash),
            Reference::Symbolic(target) => match self.backend.read_ref(&target).await? {
                Reference::Direct(hash) => Ok(hash),
                _ => Err(StorageError::Backend("Invalid HEAD".to_string())),
            },
        }
    }

    pub async fn read_object(&self, hash: Hash) -> Result<Object> {
        self.backend.read_object(hash).await
    }

    pub async fn write_object(&self, object: Object) -> Result<Hash> {
        let hash = object.hash();
        self.backend.write_object(hash, &object).await?;
        Ok(hash)
    }

    pub async fn commit(
        &self,
        tree: Hash,
        parents: Vec<Hash>,
        author: Signature,
        message: String,
        artifact_changes: Vec<ArtifactChange>,
    ) -> Result<Hash> {
        let parent_generations =
            futures::future::try_join_all(parents.iter().map(|p| self.backend.read_commit_node(*p)))
                .await?;

        let generation = parent_generations
            .iter()
            .map(|n| n.generation)
            .max()
            .map(|g| g + 1)
            .unwrap_or(0);

        let commit = Commit {
            tree,
            parents: parents.clone(),
            author: author.clone(),
            committer: author,
            message,
            generation,
            artifact_changes: artifact_changes.clone(),
            signature: None,
        };

        let commit_obj = Object::Commit(commit.clone());
        let commit_hash = commit_obj.hash();

        self.backend
            .write_object(commit_hash, &commit_obj)
            .await?;

        let node = CommitNode {
            hash: commit_hash,
            parents,
            generation,
            author_timestamp: commit.author.timestamp,
            committer_timestamp: commit.committer.timestamp,
            tree,
        };
        self.backend.write_commit_node(commit_hash, &node).await?;

        for change in artifact_changes {
            if let Some(new_content) = change.new_content {
                let version = ArtifactVersion {
                    artifact_id: change.artifact_id,
                    commit: commit_hash,
                    content_hash: new_content,
                    metadata: change.metadata,
                };
                self.backend.write_artifact_version(&version).await?;
            }
        }

        Ok(commit_hash)
    }

    pub async fn update_ref(&self, name: &str, old: Option<Hash>, new: Hash) -> Result<()> {
        let mut op = Operation::new("update_ref".to_string(), vec![name.to_string()]);

        if let Ok(old_hash) = self.resolve_ref(name).await {
            op.before_refs.push((name.to_string(), old_hash));
        }

        self.backend.update_ref(name, old, new).await?;

        op.after_refs.push((name.to_string(), new));
        self.op_log.append(op).await?;

        Ok(())
    }

    pub async fn resolve_ref(&self, name: &str) -> Result<Hash> {
        match self.backend.read_ref(name).await? {
            Reference::Direct(hash) => Ok(hash),
            Reference::Symbolic(target) => Box::pin(self.resolve_ref(&target)).await,
        }
    }

    pub async fn list_refs(&self, prefix: Option<&str>) -> Result<Vec<(String, Hash)>> {
        self.backend.list_refs(prefix).await
    }

    pub fn query(&self) -> QueryBuilder {
        QueryBuilder::new(self.backend.clone())
    }

    pub async fn is_ancestor(&self, ancestor: Hash, descendant: Hash) -> Result<bool> {
        let ancestor_node = self.backend.read_commit_node(ancestor).await?;
        let descendant_node = self.backend.read_commit_node(descendant).await?;

        if ancestor_node.generation >= descendant_node.generation {
            return Ok(false);
        }

        let mut to_check = vec![descendant];
        let mut visited = std::collections::HashSet::new();

        while let Some(current) = to_check.pop() {
            if current == ancestor {
                return Ok(true);
            }

            if visited.contains(&current) {
                continue;
            }
            visited.insert(current);

            let node = self.backend.read_commit_node(current).await?;

            if node.generation < ancestor_node.generation {
                continue;
            }

            to_check.extend(node.parents);
        }

        Ok(false)
    }

    pub async fn merge_base(&self, a: Hash, b: Hash) -> Result<Option<Hash>> {
        use std::collections::{HashMap, HashSet};

        let node_a = self.backend.read_commit_node(a).await?;
        let node_b = self.backend.read_commit_node(b).await?;

        let mut visited_a = HashSet::new();
        let mut visited_b = HashSet::new();
        let mut generations = HashMap::new();

        let mut queue_a = vec![(a, node_a.generation)];
        let mut queue_b = vec![(b, node_b.generation)];

        visited_a.insert(a);
        visited_b.insert(b);
        generations.insert(a, node_a.generation);
        generations.insert(b, node_b.generation);

        while !queue_a.is_empty() || !queue_b.is_empty() {
            if let Some((current, _)) = queue_a.pop() {
                if visited_b.contains(&current) {
                    return Ok(Some(current));
                }

                let node = self.backend.read_commit_node(current).await?;
                for parent in node.parents {
                    if visited_a.insert(parent) {
                        let parent_node = self.backend.read_commit_node(parent).await?;
                        queue_a.push((parent, parent_node.generation));
                        generations.insert(parent, parent_node.generation);
                    }
                }
            }

            if let Some((current, _)) = queue_b.pop() {
                if visited_a.contains(&current) {
                    return Ok(Some(current));
                }

                let node = self.backend.read_commit_node(current).await?;
                for parent in node.parents {
                    if visited_b.insert(parent) {
                        let parent_node = self.backend.read_commit_node(parent).await?;
                        queue_b.push((parent, parent_node.generation));
                        generations.insert(parent, parent_node.generation);
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn undo(&self) -> Result<()> {
        self.op_log.undo(self).await
    }

    pub async fn operations(&self, since: Option<OperationId>) -> Result<Vec<Operation>> {
        self.backend.read_operations(since).await
    }
}

pub struct QueryBuilder {
    backend: Arc<dyn StorageBackend>,
    query: CommitQuery,
}

impl QueryBuilder {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            query: CommitQuery::default(),
        }
    }

    pub fn since(mut self, timestamp: i64) -> Self {
        self.query.since_timestamp = Some(timestamp);
        self
    }

    pub fn until(mut self, timestamp: i64) -> Self {
        self.query.until_timestamp = Some(timestamp);
        self
    }

    pub fn author(mut self, author: String) -> Self {
        self.query.author = Some(author);
        self
    }

    pub fn message(mut self, pattern: String) -> Self {
        self.query.message_pattern = Some(pattern);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    pub async fn execute(self) -> Result<Vec<Hash>> {
        self.backend.query_commits(&self.query).await
    }
}

pub struct OperationLog {
    backend: Arc<dyn StorageBackend>,
}

impl OperationLog {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }

    pub async fn append(&self, op: Operation) -> Result<()> {
        self.backend.append_operation(&op).await
    }

    pub async fn undo(&self, repo: &Repository) -> Result<()> {
        let ops = self.backend.read_operations(None).await?;

        if let Some(last_op) = ops.last() {
            for (name, hash) in &last_op.before_refs {
                repo.backend.update_ref(name, None, *hash).await?;
            }

            let mut undo_op = Operation::new("undo".to_string(), vec![]);
            undo_op.before_refs = last_op.after_refs.clone();
            undo_op.after_refs = last_op.before_refs.clone();
            self.append(undo_op).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitnext_storage_memory::MemoryBackend;

    #[tokio::test]
    async fn test_repository_init() {
        let backend = Arc::new(MemoryBackend::new());
        let repo = Repository::init(backend).await.unwrap();

        let head = repo.head().await.unwrap();
        assert_eq!(head.as_bytes().len(), 32);
    }

    #[tokio::test]
    async fn test_commit_and_query() {
        let backend = Arc::new(MemoryBackend::new());
        let repo = Repository::init(backend).await.unwrap();

        let tree_hash = repo
            .write_object(Object::Tree(Tree { entries: vec![] }))
            .await
            .unwrap();

        let author = Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            timezone_offset: 0,
        };

        let head = repo.head().await.unwrap();
        let commit_hash = repo
            .commit(
                tree_hash,
                vec![head],
                author,
                "Test commit".to_string(),
                vec![],
            )
            .await
            .unwrap();

        let commits = repo.query().limit(10).execute().await.unwrap();
        assert!(commits.len() >= 1);
    }

    #[tokio::test]
    async fn test_is_ancestor() {
        let backend = Arc::new(MemoryBackend::new());
        let repo = Repository::init(backend).await.unwrap();

        let initial = repo.head().await.unwrap();

        let tree_hash = repo
            .write_object(Object::Tree(Tree { entries: vec![] }))
            .await
            .unwrap();
        let author = Signature {
            name: "Test".to_string(),
            email: "test@example.com".to_string(),
            timestamp: chrono::Utc::now().timestamp(),
            timezone_offset: 0,
        };

        let second = repo
            .commit(tree_hash, vec![initial], author, "Second".to_string(), vec![])
            .await
            .unwrap();

        assert!(repo.is_ancestor(initial, second).await.unwrap());
        assert!(!repo.is_ancestor(second, initial).await.unwrap());
    }
}
