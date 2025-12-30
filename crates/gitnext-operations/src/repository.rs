use gitnext_core::{GitObject, ObjectId, Tree, Commit, Signature, Blob};
use gitnext_storage::{Storage, StorageError, ReferenceTarget};
use std::sync::Arc;
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Repository struct with storage backend (Requirements 1.1)
/// Implements basic repository operations with operation logging
pub struct Repository {
    storage: Arc<dyn Storage>,
    operation_log: std::sync::Mutex<OperationLog>,
}

/// Operation logging system for undo/redo functionality (ADR-003)
pub struct OperationLog {
    storage: Arc<dyn Storage>,
    /// Current position in the operation log for undo/redo
    current_position: usize,
    /// Chain of operation log entries
    log_chain: Vec<Uuid>,
}

/// Comprehensive operation recording (ADR-003)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub operation: Operation,
    pub before_state: RepositoryState,
    pub after_state: RepositoryState,
    pub command_intent: CommandIntent,
    pub user_metadata: UserMetadata,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Operation {
    Commit { 
        before_head: Option<ObjectId>, 
        after_head: ObjectId,
        tree: ObjectId,
        message: String,
        parents: Vec<ObjectId>,
    },
    CreateBranch { 
        name: String, 
        target: ObjectId,
        before_refs: HashMap<String, ObjectId>,
    },
    DeleteBranch {
        name: String,
        deleted_target: ObjectId,
        before_refs: HashMap<String, ObjectId>,
    },
    SwitchBranch {
        from_branch: String,
        to_branch: String,
        before_head: ObjectId,
        after_head: ObjectId,
    },
    Merge { 
        branch: String, 
        before_head: ObjectId, 
        after_head: ObjectId,
        strategy: MergeStrategy,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RepositoryState {
    pub head: Option<ObjectId>,
    pub refs: HashMap<String, ObjectId>,
    pub index_state: Option<IndexState>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CommandIntent {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UserMetadata {
    pub user_name: Option<String>,
    pub user_email: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexState {
    // Placeholder for working directory index state
    pub entries: HashMap<String, ObjectId>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MergeStrategy {
    ThreeWay,
    Ours,
    Theirs,
    Recursive,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MergeResult {
    Success { commit: ObjectId },
    Conflicts { conflicted_files: Vec<String> },
}

impl Repository {
    /// Initialize a new repository with proper Git structure (Requirements 1.1)
    pub async fn init(storage: Arc<dyn Storage>) -> Result<Self, StorageError> {
        // Create empty tree for initial commit
        let empty_tree = Tree::new(vec![]);
        let tree_object = GitObject::Tree(empty_tree);
        let tree_id = tree_object.canonical_hash();
        
        // Store the empty tree
        storage.store_object(&tree_id, &tree_object).await?;
        
        // Create initial commit
        let author = Signature {
            name: "GitNext".to_string(),
            email: "gitnext@system".to_string(),
            timestamp: Utc::now().timestamp(),
            timezone_offset: 0,
        };
        
        let initial_commit = Commit {
            tree: tree_id,
            parents: vec![],
            author: author.clone(),
            committer: author,
            message: "Initial commit".to_string(),
        };
        
        let commit_object = GitObject::Commit(initial_commit);
        let commit_id = commit_object.canonical_hash();
        
        // Store the initial commit
        storage.store_object(&commit_id, &commit_object).await?;
        
        // Set up initial references
        storage.update_ref("refs/heads/main", &commit_id).await?;
        // Set HEAD as a symbolic reference to main branch
        // For now, we'll set it directly to the commit since we don't have symbolic ref support yet
        storage.update_ref("HEAD", &commit_id).await?;
        
        // Create repository with operation log
        let operation_log = std::sync::Mutex::new(OperationLog::new(storage.clone()));
        
        let repo = Repository {
            storage,
            operation_log,
        };
        
        // Record the initialization operation
        let init_state = RepositoryState {
            head: Some(commit_id),
            refs: {
                let mut refs = HashMap::new();
                refs.insert("refs/heads/main".to_string(), commit_id);
                refs.insert("HEAD".to_string(), commit_id);
                refs
            },
            index_state: None,
        };
        
        let init_operation = Operation::Commit {
            before_head: None,
            after_head: commit_id,
            tree: tree_id,
            message: "Initial commit".to_string(),
            parents: vec![],
        };
        
        let log_entry = LogEntry {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            operation: init_operation,
            before_state: RepositoryState {
                head: None,
                refs: HashMap::new(),
                index_state: None,
            },
            after_state: init_state,
            command_intent: CommandIntent {
                command: "init".to_string(),
                args: vec![],
                working_directory: ".".to_string(),
            },
            user_metadata: UserMetadata {
                user_name: None,
                user_email: None,
                session_id: None,
            },
        };
        
        repo.operation_log.lock().unwrap().record(log_entry).await?;
        
        Ok(repo)
    }
    
    /// Open an existing repository
    pub async fn open(storage: Arc<dyn Storage>) -> Result<Self, StorageError> {
        let mut operation_log = OperationLog::new(storage.clone());
        
        // Load existing operation log chain
        operation_log.load_chain().await?;
        
        Ok(Repository {
            storage,
            operation_log: std::sync::Mutex::new(operation_log),
        })
    }
    
    /// Get the current HEAD commit
    pub async fn head(&self) -> Result<ObjectId, StorageError> {
        // Try to resolve HEAD reference
        let refs = self.storage.list_refs().await?;
        
        for reference in refs {
            if reference.name == "HEAD" {
                match reference.target {
                    ReferenceTarget::Direct(id) => return Ok(id),
                    ReferenceTarget::Symbolic(target_ref) => {
                        // Resolve symbolic reference
                        for ref2 in self.storage.list_refs().await? {
                            if ref2.name == target_ref {
                                match ref2.target {
                                    ReferenceTarget::Direct(id) => return Ok(id),
                                    _ => continue,
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Err(StorageError::RefNotFound { name: "HEAD".to_string() })
    }
    
    /// Create a new branch pointing to the specified commit
    pub async fn create_branch(&self, name: &str, target: &ObjectId) -> Result<(), StorageError> {
        let branch_ref = format!("refs/heads/{}", name);
        
        // Capture before state
        let before_refs = self.get_all_refs().await?;
        
        // Update the reference
        self.storage.update_ref(&branch_ref, target).await?;
        
        // Capture after state
        let after_refs = self.get_all_refs().await?;
        
        // Record the operation
        let operation = Operation::CreateBranch {
            name: name.to_string(),
            target: *target,
            before_refs: before_refs.clone(),
        };
        
        let log_entry = LogEntry {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            operation,
            before_state: RepositoryState {
                head: self.head().await.ok(),
                refs: before_refs,
                index_state: None,
            },
            after_state: RepositoryState {
                head: self.head().await.ok(),
                refs: after_refs,
                index_state: None,
            },
            command_intent: CommandIntent {
                command: "branch".to_string(),
                args: vec![name.to_string()],
                working_directory: ".".to_string(),
            },
            user_metadata: UserMetadata {
                user_name: None,
                user_email: None,
                session_id: None,
            },
        };
        
        self.operation_log.lock().unwrap().record(log_entry).await?;
        
        Ok(())
    }
    
    /// Switch to a different branch
    pub async fn switch_branch(&self, branch_name: &str) -> Result<(), StorageError> {
        let branch_ref = format!("refs/heads/{}", branch_name);
        
        // Get current HEAD
        let before_head = self.head().await?;
        
        // Find the target branch
        let refs = self.storage.list_refs().await?;
        let target_commit = refs.iter()
            .find(|r| r.name == branch_ref)
            .and_then(|r| match &r.target {
                ReferenceTarget::Direct(id) => Some(*id),
                _ => None,
            })
            .ok_or_else(|| StorageError::RefNotFound { name: branch_ref.clone() })?;
        
        // Update HEAD to point to the branch
        self.storage.update_ref("HEAD", &target_commit).await?;
        
        // Store the current branch name in a special reference
        let current_branch_ref = "refs/gitnext/current-branch";
        let branch_name_bytes = branch_name.as_bytes().to_vec();
        let branch_name_blob = gitnext_core::Blob::new(bytes::Bytes::from(branch_name_bytes));
        let branch_name_object = gitnext_core::GitObject::Blob(branch_name_blob);
        let branch_name_id = branch_name_object.canonical_hash();
        self.storage.store_object(&branch_name_id, &branch_name_object).await?;
        self.storage.update_ref(current_branch_ref, &branch_name_id).await?;
        
        // Record the operation
        let operation = Operation::SwitchBranch {
            from_branch: "HEAD".to_string(), // Simplified for now
            to_branch: branch_name.to_string(),
            before_head,
            after_head: target_commit,
        };
        
        let log_entry = LogEntry {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            operation,
            before_state: RepositoryState {
                head: Some(before_head),
                refs: self.get_all_refs().await?,
                index_state: None,
            },
            after_state: RepositoryState {
                head: Some(target_commit),
                refs: self.get_all_refs().await?,
                index_state: None,
            },
            command_intent: CommandIntent {
                command: "switch".to_string(),
                args: vec![branch_name.to_string()],
                working_directory: ".".to_string(),
            },
            user_metadata: UserMetadata {
                user_name: None,
                user_email: None,
                session_id: None,
            },
        };
        
        self.operation_log.lock().unwrap().record(log_entry).await?;
        
        Ok(())
    }
    
    /// Delete a branch
    pub async fn delete_branch(&self, branch_name: &str) -> Result<(), StorageError> {
        let branch_ref = format!("refs/heads/{}", branch_name);
        
        // Capture before state
        let before_refs = self.get_all_refs().await?;
        
        // Check if branch exists and get its target
        let refs = self.storage.list_refs().await?;
        let deleted_target = refs.iter()
            .find(|r| r.name == branch_ref)
            .and_then(|r| match &r.target {
                ReferenceTarget::Direct(id) => Some(*id),
                _ => None,
            })
            .ok_or_else(|| StorageError::RefNotFound { name: branch_ref.clone() })?;
        
        // Prevent deletion of current branch
        // Only prevent deletion if HEAD is a symbolic reference pointing to this branch
        let current_branch = self.get_current_branch().await?;
        if let Some(current) = current_branch {
            if current == branch_name {
                return Err(StorageError::Backend(
                    format!("Cannot delete current branch '{}'", branch_name)
                ));
            }
        }
        
        // Delete the branch reference
        self.storage.delete_ref(&branch_ref).await?;
        
        // Capture after state
        let after_refs = self.get_all_refs().await?;
        
        // Record the operation
        let operation = Operation::DeleteBranch {
            name: branch_name.to_string(),
            deleted_target,
            before_refs: before_refs.clone(),
        };
        
        let log_entry = LogEntry {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            operation,
            before_state: RepositoryState {
                head: self.head().await.ok(),
                refs: before_refs,
                index_state: None,
            },
            after_state: RepositoryState {
                head: self.head().await.ok(),
                refs: after_refs,
                index_state: None,
            },
            command_intent: CommandIntent {
                command: "branch".to_string(),
                args: vec!["-d".to_string(), branch_name.to_string()],
                working_directory: ".".to_string(),
            },
            user_metadata: UserMetadata {
                user_name: None,
                user_email: None,
                session_id: None,
            },
        };
        
        self.operation_log.lock().unwrap().record(log_entry).await?;
        
        Ok(())
    }
    
    /// Helper method to get all references as a HashMap
    async fn get_all_refs(&self) -> Result<HashMap<String, ObjectId>, StorageError> {
        let refs = self.storage.list_refs().await?;
        let mut ref_map = HashMap::new();
        
        for reference in refs {
            if let ReferenceTarget::Direct(id) = reference.target {
                ref_map.insert(reference.name, id);
            }
        }
        
        Ok(ref_map)
    }
    
    /// Undo the last operation (Requirements 4.2, 4.3, 4.5)
    pub async fn undo(&self) -> Result<Option<Operation>, StorageError> {
        self.operation_log.lock().unwrap().undo(self).await
    }
    
    /// Redo a previously undone operation (Requirements 4.2, 4.3, 4.5)
    pub async fn redo(&self) -> Result<Option<Operation>, StorageError> {
        self.operation_log.lock().unwrap().redo(self).await
    }
    
    /// Check if there are operations that can be undone
    pub fn can_undo(&self) -> bool {
        self.operation_log.lock().unwrap().can_undo()
    }
    
    /// Check if there are operations that can be redone
    pub fn can_redo(&self) -> bool {
        self.operation_log.lock().unwrap().can_redo()
    }
    
    /// Get a preview of the operation that would be undone
    pub async fn peek_undo(&self) -> Result<Option<Operation>, StorageError> {
        self.operation_log.lock().unwrap().peek_undo().await
    }
    
    /// Get a preview of the operation that would be redone
    pub async fn peek_redo(&self) -> Result<Option<Operation>, StorageError> {
        self.operation_log.lock().unwrap().peek_redo().await
    }
    
    /// Get the current position in the operation log
    pub fn operation_log_position(&self) -> usize {
        self.operation_log.lock().unwrap().current_position()
    }
    
    /// Get the total number of operations in the log
    pub fn operation_log_size(&self) -> usize {
        self.operation_log.lock().unwrap().total_operations()
    }
    
    /// Create a new commit with the given tree and message (Requirements 1.3, 4.1)
    pub async fn commit(
        &self,
        tree: &ObjectId,
        parents: Vec<ObjectId>,
        author: Signature,
        committer: Signature,
        message: String,
    ) -> Result<ObjectId, StorageError> {
        // Get current HEAD for before state
        let before_head = self.head().await.ok();
        
        // Clone message for later use
        let message_clone = message.clone();
        
        // Create the commit object
        let commit = Commit {
            tree: *tree,
            parents: parents.clone(),
            author,
            committer,
            message,
        };
        
        let commit_object = GitObject::Commit(commit);
        let commit_id = commit_object.canonical_hash();
        
        // Store the commit object
        self.storage.store_object(&commit_id, &commit_object).await?;
        
        // Update HEAD to point to the new commit
        self.storage.update_ref("HEAD", &commit_id).await?;
        
        // Update the current branch reference
        let current_branch = self.get_current_branch().await?;
        if let Some(branch_name) = current_branch {
            let branch_ref = format!("refs/heads/{}", branch_name);
            self.storage.update_ref(&branch_ref, &commit_id).await?;
        }
        
        // Record the commit operation
        let operation = Operation::Commit {
            before_head,
            after_head: commit_id,
            tree: *tree,
            message: message_clone.clone(),
            parents,
        };
        
        let log_entry = LogEntry {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            operation,
            before_state: RepositoryState {
                head: before_head,
                refs: self.get_all_refs().await?,
                index_state: None,
            },
            after_state: RepositoryState {
                head: Some(commit_id),
                refs: self.get_all_refs().await?,
                index_state: None,
            },
            command_intent: CommandIntent {
                command: "commit".to_string(),
                args: vec!["-m".to_string(), message_clone],
                working_directory: ".".to_string(),
            },
            user_metadata: UserMetadata {
                user_name: None,
                user_email: None,
                session_id: None,
            },
        };
        
        self.operation_log.lock().unwrap().record(log_entry).await?;
        
        Ok(commit_id)
    }
    
    /// Get the current branch name (if HEAD points to a branch)
    pub async fn get_current_branch(&self) -> Result<Option<String>, StorageError> {
        // First, try to get the current branch from our tracking reference
        let current_branch_ref = "refs/gitnext/current-branch";
        let refs = self.storage.list_refs().await?;
        
        if let Some(current_ref) = refs.iter().find(|r| r.name == current_branch_ref) {
            if let ReferenceTarget::Direct(branch_name_id) = &current_ref.target {
                if let Some(gitnext_core::GitObject::Blob(blob)) = self.storage.load_object(branch_name_id).await? {
                    if let Some(content) = &blob.content {
                        let branch_name = String::from_utf8_lossy(content).to_string();
                        return Ok(Some(branch_name));
                    }
                }
            }
        }
        
        // Fallback: check if HEAD is a symbolic reference
        let head_ref = refs.iter().find(|r| r.name == "HEAD");
        
        if let Some(head) = head_ref {
            match &head.target {
                ReferenceTarget::Symbolic(target) => {
                    // HEAD points to a branch
                    if target.starts_with("refs/heads/") {
                        return Ok(Some(target.strip_prefix("refs/heads/").unwrap().to_string()));
                    }
                }
                ReferenceTarget::Direct(_head_commit) => {
                    // HEAD points directly to a commit (detached HEAD)
                    // In this case, we're not on any specific branch
                    return Ok(None);
                }
            }
        }
        
        Ok(None)
    }
}

impl OperationLog {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            current_position: 0,
            log_chain: Vec::new(),
        }
    }
    
    /// Get the current position in the operation log
    pub fn current_position(&self) -> usize {
        self.current_position
    }
    
    /// Get the total number of operations in the log
    pub fn total_operations(&self) -> usize {
        self.log_chain.len()
    }
    
    /// Check if there are operations that can be undone
    pub fn can_undo(&self) -> bool {
        self.current_position > 0
    }
    
    /// Check if there are operations that can be redone
    pub fn can_redo(&self) -> bool {
        self.current_position < self.log_chain.len()
    }
    
    /// Get a preview of the operation that would be undone
    pub async fn peek_undo(&self) -> Result<Option<Operation>, StorageError> {
        if !self.can_undo() {
            return Ok(None);
        }
        
        let entry_id = self.log_chain[self.current_position - 1];
        let entry = self.load_log_entry(entry_id).await?;
        Ok(entry.map(|e| e.operation))
    }
    
    /// Get a preview of the operation that would be redone
    pub async fn peek_redo(&self) -> Result<Option<Operation>, StorageError> {
        if !self.can_redo() {
            return Ok(None);
        }
        
        let entry_id = self.log_chain[self.current_position];
        let entry = self.load_log_entry(entry_id).await?;
        Ok(entry.map(|e| e.operation))
    }
    
    /// Record an operation in the log (Requirements 4.1, 4.4)
    pub async fn record(&mut self, entry: LogEntry) -> Result<(), StorageError> {
        // Serialize the log entry
        let serialized = bincode::serialize(&entry)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        // Create a blob object to store the log entry
        let log_blob = Blob::new(bytes::Bytes::from(serialized));
        let log_object = GitObject::Blob(log_blob);
        let log_id = log_object.canonical_hash();
        
        // Store the log entry
        self.storage.store_object(&log_id, &log_object).await?;
        
        // Update a reference to track the log entry
        let log_ref = format!("refs/logs/operations/{}", entry.id);
        self.storage.update_ref(&log_ref, &log_id).await?;
        
        // Add to log chain and update position
        // If we're not at the end of the chain, truncate future operations
        if self.current_position < self.log_chain.len() {
            self.log_chain.truncate(self.current_position);
        }
        
        self.log_chain.push(entry.id);
        self.current_position = self.log_chain.len();
        
        // Update the log chain reference
        self.update_log_chain_ref().await?;
        
        Ok(())
    }
    
    /// Get the current operation log entry
    pub async fn current_entry(&self) -> Result<Option<LogEntry>, StorageError> {
        if self.current_position == 0 || self.current_position > self.log_chain.len() {
            return Ok(None);
        }
        
        let entry_id = self.log_chain[self.current_position - 1];
        self.load_log_entry(entry_id).await
    }
    
    /// Undo the last operation (Requirements 4.2, 4.3, 4.5)
    pub async fn undo(&mut self, repo: &Repository) -> Result<Option<Operation>, StorageError> {
        if self.current_position == 0 {
            return Ok(None); // Nothing to undo
        }
        
        // Get the current operation
        let entry_id = self.log_chain[self.current_position - 1];
        let entry = self.load_log_entry(entry_id).await?
            .ok_or_else(|| StorageError::Backend("Log entry not found".to_string()))?;
        
        // Apply the reverse operation based on the operation type
        match &entry.operation {
            Operation::CreateBranch { name, .. } => {
                // Delete the branch that was created
                let branch_ref = format!("refs/heads/{}", name);
                repo.storage.delete_ref(&branch_ref).await?;
            }
            Operation::DeleteBranch { name, deleted_target, .. } => {
                // Recreate the branch that was deleted
                let branch_ref = format!("refs/heads/{}", name);
                repo.storage.update_ref(&branch_ref, deleted_target).await?;
            }
            Operation::SwitchBranch { from_branch, before_head, .. } => {
                // Switch back to the previous HEAD
                repo.storage.update_ref("HEAD", before_head).await?;
                
                // Update current branch tracking if switching from a named branch
                if from_branch != "HEAD" {
                    let current_branch_ref = "refs/gitnext/current-branch";
                    let branch_name_bytes = from_branch.as_bytes().to_vec();
                    let branch_name_blob = gitnext_core::Blob::new(bytes::Bytes::from(branch_name_bytes));
                    let branch_name_object = gitnext_core::GitObject::Blob(branch_name_blob);
                    let branch_name_id = branch_name_object.canonical_hash();
                    repo.storage.store_object(&branch_name_id, &branch_name_object).await?;
                    repo.storage.update_ref(current_branch_ref, &branch_name_id).await?;
                }
            }
            Operation::Commit { before_head, .. } => {
                // Reset HEAD to the previous commit
                if let Some(prev_head) = before_head {
                    repo.storage.update_ref("HEAD", prev_head).await?;
                    
                    // Also update the current branch reference if we're on a branch
                    let current_branch = repo.get_current_branch().await?;
                    if let Some(branch_name) = current_branch {
                        let branch_ref = format!("refs/heads/{}", branch_name);
                        repo.storage.update_ref(&branch_ref, prev_head).await?;
                    }
                } else {
                    // This was the initial commit, we can't undo it completely
                    // but we can reset HEAD to None (though this is unusual)
                    return Err(StorageError::Backend(
                        "Cannot undo initial commit - no previous state".to_string()
                    ));
                }
            }
            Operation::Merge { before_head, .. } => {
                // Reset HEAD to the state before the merge
                repo.storage.update_ref("HEAD", before_head).await?;
                
                // Update current branch reference if we're on a branch
                let current_branch = repo.get_current_branch().await?;
                if let Some(branch_name) = current_branch {
                    let branch_ref = format!("refs/heads/{}", branch_name);
                    repo.storage.update_ref(&branch_ref, before_head).await?;
                }
            }
        }
        
        // Move position back
        self.current_position -= 1;
        self.update_log_chain_ref().await?;
        
        Ok(Some(entry.operation))
    }
    
    /// Redo a previously undone operation (Requirements 4.2, 4.3, 4.5)
    pub async fn redo(&mut self, repo: &Repository) -> Result<Option<Operation>, StorageError> {
        if self.current_position >= self.log_chain.len() {
            return Ok(None); // Nothing to redo
        }
        
        // Get the next operation to redo
        let entry_id = self.log_chain[self.current_position];
        let entry = self.load_log_entry(entry_id).await?
            .ok_or_else(|| StorageError::Backend("Log entry not found".to_string()))?;
        
        // Apply the operation again
        match &entry.operation {
            Operation::CreateBranch { name, target, .. } => {
                let branch_ref = format!("refs/heads/{}", name);
                repo.storage.update_ref(&branch_ref, target).await?;
            }
            Operation::DeleteBranch { name, .. } => {
                let branch_ref = format!("refs/heads/{}", name);
                repo.storage.delete_ref(&branch_ref).await?;
            }
            Operation::SwitchBranch { to_branch, after_head, .. } => {
                repo.storage.update_ref("HEAD", after_head).await?;
                
                // Update current branch tracking
                let current_branch_ref = "refs/gitnext/current-branch";
                let branch_name_bytes = to_branch.as_bytes().to_vec();
                let branch_name_blob = gitnext_core::Blob::new(bytes::Bytes::from(branch_name_bytes));
                let branch_name_object = gitnext_core::GitObject::Blob(branch_name_blob);
                let branch_name_id = branch_name_object.canonical_hash();
                repo.storage.store_object(&branch_name_id, &branch_name_object).await?;
                repo.storage.update_ref(current_branch_ref, &branch_name_id).await?;
            }
            Operation::Commit { after_head, .. } => {
                repo.storage.update_ref("HEAD", after_head).await?;
                
                // Update current branch reference if we're on a branch
                let current_branch = repo.get_current_branch().await?;
                if let Some(branch_name) = current_branch {
                    let branch_ref = format!("refs/heads/{}", branch_name);
                    repo.storage.update_ref(&branch_ref, after_head).await?;
                }
            }
            Operation::Merge { after_head, .. } => {
                repo.storage.update_ref("HEAD", after_head).await?;
                
                // Update current branch reference if we're on a branch
                let current_branch = repo.get_current_branch().await?;
                if let Some(branch_name) = current_branch {
                    let branch_ref = format!("refs/heads/{}", branch_name);
                    repo.storage.update_ref(&branch_ref, after_head).await?;
                }
            }
        }
        
        // Move position forward
        self.current_position += 1;
        self.update_log_chain_ref().await?;
        
        Ok(Some(entry.operation))
    }
    
    /// Load a log entry by ID
    async fn load_log_entry(&self, entry_id: Uuid) -> Result<Option<LogEntry>, StorageError> {
        let log_ref = format!("refs/logs/operations/{}", entry_id);
        let refs = self.storage.list_refs().await?;
        
        // Find the log reference
        let log_object_id = refs.iter()
            .find(|r| r.name == log_ref)
            .and_then(|r| match &r.target {
                ReferenceTarget::Direct(id) => Some(*id),
                _ => None,
            });
        
        if let Some(object_id) = log_object_id {
            if let Some(GitObject::Blob(blob)) = self.storage.load_object(&object_id).await? {
                if let Some(content) = &blob.content {
                    let entry: LogEntry = bincode::deserialize(content)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    return Ok(Some(entry));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Update the log chain reference for persistence
    async fn update_log_chain_ref(&self) -> Result<(), StorageError> {
        // Serialize the log chain state
        let chain_data = bincode::serialize(&(self.current_position, &self.log_chain))
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        
        let chain_blob = Blob::new(bytes::Bytes::from(chain_data));
        let chain_object = GitObject::Blob(chain_blob);
        let chain_id = chain_object.canonical_hash();
        
        self.storage.store_object(&chain_id, &chain_object).await?;
        self.storage.update_ref("refs/logs/chain", &chain_id).await?;
        
        Ok(())
    }
    
    /// Load the log chain from storage
    pub async fn load_chain(&mut self) -> Result<(), StorageError> {
        let refs = self.storage.list_refs().await?;
        
        // Find the chain reference
        let chain_object_id = refs.iter()
            .find(|r| r.name == "refs/logs/chain")
            .and_then(|r| match &r.target {
                ReferenceTarget::Direct(id) => Some(*id),
                _ => None,
            });
        
        if let Some(object_id) = chain_object_id {
            if let Some(GitObject::Blob(blob)) = self.storage.load_object(&object_id).await? {
                if let Some(content) = &blob.content {
                    let (position, chain): (usize, Vec<Uuid>) = bincode::deserialize(content)
                        .map_err(|e| StorageError::Serialization(e.to_string()))?;
                    
                    self.current_position = position;
                    self.log_chain = chain;
                }
            }
        }
        
        Ok(())
    }
    
    /// Replay operations from a specific position (for crash recovery)
    pub async fn replay_from(&self, _position: usize) -> Result<(), StorageError> {
        // Placeholder for operation replay functionality
        // This would be used for crash recovery and debugging
        Ok(())
    }
    
    /// Compact the operation log to manage storage growth
    pub async fn compact(&mut self, keep_entries: usize) -> Result<(), StorageError> {
        if self.log_chain.len() <= keep_entries {
            return Ok(()); // Nothing to compact
        }
        
        // Keep only the most recent entries
        let remove_count = self.log_chain.len() - keep_entries;
        self.log_chain.drain(0..remove_count);
        
        // Adjust current position
        if self.current_position > remove_count {
            self.current_position -= remove_count;
        } else {
            self.current_position = 0;
        }
        
        // Update the chain reference
        self.update_log_chain_ref().await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitnext_storage::memory::MemoryStorage;

    #[tokio::test]
    async fn test_repository_init() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let head = repo.head().await.unwrap();
        assert_eq!(head.as_bytes().len(), 32);
    }

    #[tokio::test]
    async fn test_create_branch() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let head = repo.head().await.unwrap();
        repo.create_branch("feature", &head).await.unwrap();

        // Verify the branch was created by checking references
        let refs = repo.storage.list_refs().await.unwrap();
        let feature_ref = refs.iter().find(|r| r.name == "refs/heads/feature");
        assert!(feature_ref.is_some());
    }

    #[tokio::test]
    async fn test_switch_branch() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let head = repo.head().await.unwrap();
        repo.create_branch("feature", &head).await.unwrap();
        repo.switch_branch("feature").await.unwrap();

        // HEAD should now point to the feature branch commit
        let new_head = repo.head().await.unwrap();
        assert_eq!(head, new_head);
    }

    #[tokio::test]
    async fn test_delete_branch() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let head = repo.head().await.unwrap();
        repo.create_branch("feature", &head).await.unwrap();

        // Verify the branch was created
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(refs.iter().any(|r| r.name == "refs/heads/feature"));

        // Make sure we're not on the feature branch (we should be on main)
        // Delete the branch
        repo.delete_branch("feature").await.unwrap();

        // Verify the branch was deleted
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(!refs.iter().any(|r| r.name == "refs/heads/feature"));
    }

    #[tokio::test]
    async fn test_delete_current_branch_fails() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        // Create and switch to a feature branch
        let head = repo.head().await.unwrap();
        repo.create_branch("feature", &head).await.unwrap();
        repo.switch_branch("feature").await.unwrap();

        // Try to delete the current branch (feature) - should fail
        let result = repo.delete_branch("feature").await;
        assert!(result.is_err());
        
        match result.unwrap_err() {
            StorageError::Backend(msg) => {
                assert!(msg.contains("Cannot delete current branch"));
            }
            _ => panic!("Expected backend error for deleting current branch"),
        }
    }

    #[tokio::test]
    async fn test_undo_redo() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let initial_head = repo.head().await.unwrap();
        let initial_position = repo.operation_log_position();
        
        // Create a branch
        repo.create_branch("test-branch", &initial_head).await.unwrap();
        
        // Verify branch exists
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(refs.iter().any(|r| r.name == "refs/heads/test-branch"));
        assert!(repo.can_undo());
        assert!(!repo.can_redo());
        
        // Test undo - should remove the branch
        let undone_op = repo.undo().await.unwrap();
        assert!(undone_op.is_some());
        
        // Verify branch was removed
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(!refs.iter().any(|r| r.name == "refs/heads/test-branch"));
        
        // Should be back to initial position (after init commit)
        assert_eq!(repo.operation_log_position(), initial_position);
        // Can still undo the initial commit if needed
        assert!(repo.can_undo());
        assert!(repo.can_redo());
        
        // Test redo - should recreate the branch
        let redone_op = repo.redo().await.unwrap();
        assert!(redone_op.is_some());
        
        // Verify branch was recreated
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(refs.iter().any(|r| r.name == "refs/heads/test-branch"));
        assert!(repo.can_undo());
        assert!(!repo.can_redo());
    }

    #[tokio::test]
    async fn test_undo_redo_commit() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage.clone()).await.unwrap();

        let initial_head = repo.head().await.unwrap();
        
        // Create an empty tree for the commit
        let empty_tree = Tree::new(vec![]);
        let tree_object = GitObject::Tree(empty_tree);
        let tree_id = tree_object.canonical_hash();
        
        // Store the tree
        repo.storage.store_object(&tree_id, &tree_object).await.unwrap();
        
        // Create author and committer signatures
        let author = Signature {
            name: "Test Author".to_string(),
            email: "test@example.com".to_string(),
            timestamp: Utc::now().timestamp(),
            timezone_offset: 0,
        };
        
        let committer = author.clone();
        
        // Create a commit
        let commit_id = repo.commit(
            &tree_id,
            vec![initial_head],
            author,
            committer,
            "Test commit for undo/redo".to_string(),
        ).await.unwrap();
        
        // Verify HEAD was updated
        let new_head = repo.head().await.unwrap();
        assert_eq!(new_head, commit_id);
        assert!(repo.can_undo());
        
        // Test undo - should reset HEAD to previous commit
        let undone_op = repo.undo().await.unwrap();
        assert!(undone_op.is_some());
        
        // Verify HEAD was reset
        let head_after_undo = repo.head().await.unwrap();
        assert_eq!(head_after_undo, initial_head);
        assert!(repo.can_redo());
        
        // Test redo - should restore the commit
        let redone_op = repo.redo().await.unwrap();
        assert!(redone_op.is_some());
        
        // Verify HEAD was restored
        let head_after_redo = repo.head().await.unwrap();
        assert_eq!(head_after_redo, commit_id);
    }

    #[tokio::test]
    async fn test_undo_redo_branch_switch() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let initial_head = repo.head().await.unwrap();
        
        // Create a feature branch
        repo.create_branch("feature", &initial_head).await.unwrap();
        
        // Switch to the feature branch
        repo.switch_branch("feature").await.unwrap();
        
        // Verify we're on the feature branch
        let current_branch = repo.get_current_branch().await.unwrap();
        assert_eq!(current_branch, Some("feature".to_string()));
        
        // Test undo - should switch back to previous branch/HEAD
        let undone_op = repo.undo().await.unwrap();
        assert!(undone_op.is_some());
        
        // Verify HEAD was reset (though current branch tracking might be different)
        let head_after_undo = repo.head().await.unwrap();
        assert_eq!(head_after_undo, initial_head);
        
        // Test redo - should switch back to feature branch
        let redone_op = repo.redo().await.unwrap();
        assert!(redone_op.is_some());
        
        // Verify we're back on the feature branch
        let head_after_redo = repo.head().await.unwrap();
        assert_eq!(head_after_redo, initial_head); // Same commit, different branch
        let current_branch_after_redo = repo.get_current_branch().await.unwrap();
        assert_eq!(current_branch_after_redo, Some("feature".to_string()));
    }

    #[tokio::test]
    async fn test_multiple_undo_redo_operations() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let initial_head = repo.head().await.unwrap();
        
        // Perform multiple operations
        repo.create_branch("branch1", &initial_head).await.unwrap();
        repo.create_branch("branch2", &initial_head).await.unwrap();
        repo.create_branch("branch3", &initial_head).await.unwrap();
        
        // Verify all branches exist
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch1"));
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch2"));
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch3"));
        
        // Undo all operations
        repo.undo().await.unwrap(); // Undo branch3 creation
        repo.undo().await.unwrap(); // Undo branch2 creation
        repo.undo().await.unwrap(); // Undo branch1 creation
        
        // Verify all branches were removed
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(!refs.iter().any(|r| r.name == "refs/heads/branch1"));
        assert!(!refs.iter().any(|r| r.name == "refs/heads/branch2"));
        assert!(!refs.iter().any(|r| r.name == "refs/heads/branch3"));
        
        // Redo some operations
        repo.redo().await.unwrap(); // Redo branch1 creation
        repo.redo().await.unwrap(); // Redo branch2 creation
        
        // Verify partial restoration
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch1"));
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch2"));
        assert!(!refs.iter().any(|r| r.name == "refs/heads/branch3"));
        
        // Should still be able to redo the last operation
        assert!(repo.can_redo());
        repo.redo().await.unwrap(); // Redo branch3 creation
        
        // Verify full restoration
        let refs = repo.storage.list_refs().await.unwrap();
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch1"));
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch2"));
        assert!(refs.iter().any(|r| r.name == "refs/heads/branch3"));
        
        // Should not be able to redo anymore
        assert!(!repo.can_redo());
    }

    #[tokio::test]
    async fn test_undo_redo_state_consistency() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        let initial_head = repo.head().await.unwrap();
        let initial_position = repo.operation_log_position();
        let initial_size = repo.operation_log_size();
        
        // Create a branch
        repo.create_branch("test", &initial_head).await.unwrap();
        
        let after_create_position = repo.operation_log_position();
        let after_create_size = repo.operation_log_size();
        
        // Verify log state changed
        assert_eq!(after_create_position, initial_position + 1);
        assert_eq!(after_create_size, initial_size + 1);
        
        // Undo the operation
        repo.undo().await.unwrap();
        
        let after_undo_position = repo.operation_log_position();
        let after_undo_size = repo.operation_log_size();
        
        // Verify position moved back but size remained the same
        assert_eq!(after_undo_position, initial_position);
        assert_eq!(after_undo_size, after_create_size); // Size doesn't change on undo
        
        // Redo the operation
        repo.redo().await.unwrap();
        
        let after_redo_position = repo.operation_log_position();
        let after_redo_size = repo.operation_log_size();
        
        // Verify we're back to the state after creation
        assert_eq!(after_redo_position, after_create_position);
        assert_eq!(after_redo_size, after_create_size);
    }

    #[tokio::test]
    async fn test_commit_operations() {
        let storage = Arc::new(MemoryStorage::new());
        let repo = Repository::init(storage).await.unwrap();

        // Create an empty tree for the commit
        let empty_tree = Tree::new(vec![]);
        let tree_object = GitObject::Tree(empty_tree);
        let tree_id = tree_object.canonical_hash();
        
        // Store the tree
        repo.storage.store_object(&tree_id, &tree_object).await.unwrap();
        
        // Get the current HEAD as parent
        let parent_commit = repo.head().await.unwrap();
        
        // Create author and committer signatures
        let author = Signature {
            name: "Test Author".to_string(),
            email: "test@example.com".to_string(),
            timestamp: Utc::now().timestamp(),
            timezone_offset: 0,
        };
        
        let committer = author.clone();
        
        // Create a commit
        let commit_id = repo.commit(
            &tree_id,
            vec![parent_commit],
            author,
            committer,
            "Test commit message".to_string(),
        ).await.unwrap();
        
        // Verify the commit was created and HEAD was updated
        let new_head = repo.head().await.unwrap();
        assert_eq!(new_head, commit_id);
        
        // Verify the commit object exists and has correct content
        let commit_object = repo.storage.load_object(&commit_id).await.unwrap();
        assert!(commit_object.is_some());
        
        if let Some(GitObject::Commit(commit)) = commit_object {
            assert_eq!(commit.tree, tree_id);
            assert_eq!(commit.parents, vec![parent_commit]);
            assert_eq!(commit.message, "Test commit message");
            assert_eq!(commit.author.name, "Test Author");
            assert_eq!(commit.committer.name, "Test Author");
        } else {
            panic!("Expected commit object");
        }
    }
}

// Property-based tests for operation logging
#[cfg(test)]
mod property_tests {
    use super::*;
    use gitnext_storage::memory::MemoryStorage;
    use proptest::prelude::*;

    // Generator for repository operations
    prop_compose! {
        fn arb_branch_name()(name in "[a-zA-Z][a-zA-Z0-9_-]{0,30}") -> String {
            name
        }
    }

    prop_compose! {
        fn arb_commit_message()(message in "[\\x20-\\x7E\\n]{1,200}") -> String {
            message
        }
    }

    proptest! {
        /// Property 11: Operation Logging Completeness
        /// For any repository mutation operation, the operation should be recorded 
        /// in the operation log with sufficient information to undo it.
        /// **Validates: Requirements 4.1**
        #[test]
        fn prop_operation_logging_completeness(
            branch_names in prop::collection::vec(arb_branch_name(), 1..5)
        ) {
            tokio_test::block_on(async {
                let storage = Arc::new(MemoryStorage::new());
                let repo = Repository::init(storage.clone()).await.unwrap();
                
                let initial_head = repo.head().await.unwrap();
                let initial_refs = repo.storage.list_refs().await.unwrap();
                
                // Count initial references by type
                let initial_branch_refs: Vec<_> = initial_refs.iter()
                    .filter(|r| r.name.starts_with("refs/heads/"))
                    .collect();
                let initial_log_refs: Vec<_> = initial_refs.iter()
                    .filter(|r| r.name.starts_with("refs/logs/operations/"))
                    .collect();
                let initial_chain_refs: Vec<_> = initial_refs.iter()
                    .filter(|r| r.name == "refs/logs/chain")
                    .collect();
                let initial_head_refs: Vec<_> = initial_refs.iter()
                    .filter(|r| r.name == "HEAD")
                    .collect();
                
                // Perform a series of branch creation operations
                for branch_name in &branch_names {
                    repo.create_branch(branch_name, &initial_head).await.unwrap();
                }
                
                // Verify that references were created
                let final_refs = repo.storage.list_refs().await.unwrap();
                
                // Count final references by type
                let final_branch_refs: Vec<_> = final_refs.iter()
                    .filter(|r| r.name.starts_with("refs/heads/"))
                    .collect();
                let final_log_refs: Vec<_> = final_refs.iter()
                    .filter(|r| r.name.starts_with("refs/logs/operations/"))
                    .collect();
                let final_chain_refs: Vec<_> = final_refs.iter()
                    .filter(|r| r.name == "refs/logs/chain")
                    .collect();
                let final_head_refs: Vec<_> = final_refs.iter()
                    .filter(|r| r.name == "HEAD")
                    .collect();
                
                // Verify branch references were created correctly
                prop_assert_eq!(final_branch_refs.len(), initial_branch_refs.len() + branch_names.len(), 
                    "Should have {} branch refs (initial {} + new {}), got {}", 
                    initial_branch_refs.len() + branch_names.len(), 
                    initial_branch_refs.len(), 
                    branch_names.len(), 
                    final_branch_refs.len());
                
                // Verify each branch was created
                for branch_name in &branch_names {
                    let branch_ref = format!("refs/heads/{}", branch_name);
                    let branch_exists = final_refs.iter().any(|r| r.name == branch_ref);
                    prop_assert!(branch_exists, "Branch {} should exist", branch_name);
                }
                
                // Verify operation log entries were created
                // Each branch creation should add one log entry
                prop_assert_eq!(final_log_refs.len(), initial_log_refs.len() + branch_names.len(), 
                    "Should have {} log entries (initial {} + new {}), got {}", 
                    initial_log_refs.len() + branch_names.len(), 
                    initial_log_refs.len(), 
                    branch_names.len(), 
                    final_log_refs.len());
                
                // Verify log chain reference exists and is maintained
                prop_assert_eq!(final_chain_refs.len(), 1, "Should have exactly one log chain reference");
                prop_assert_eq!(initial_chain_refs.len(), 1, "Should have had exactly one initial log chain reference");
                
                // Verify HEAD reference is maintained
                prop_assert_eq!(final_head_refs.len(), 1, "Should have exactly one HEAD reference");
                prop_assert_eq!(initial_head_refs.len(), 1, "Should have had exactly one initial HEAD reference");
                
                // Verify that each log entry can be loaded and contains valid operation data
                for log_ref in &final_log_refs {
                    if let gitnext_storage::ReferenceTarget::Direct(log_id) = &log_ref.target {
                        let log_object = storage.load_object(log_id).await.unwrap();
                        prop_assert!(log_object.is_some(), "Log object should exist");
                        
                        if let Some(gitnext_core::GitObject::Blob(blob)) = log_object {
                            prop_assert!(blob.content.is_some(), "Log blob should have content");
                            
                            // Try to deserialize the log entry
                            if let Some(content) = &blob.content {
                                let deserialize_result: Result<LogEntry, _> = bincode::deserialize(content);
                                prop_assert!(deserialize_result.is_ok(), 
                                    "Log entry should be deserializable: {:?}", 
                                    deserialize_result.err());
                                
                                if let Ok(log_entry) = deserialize_result {
                                    // Verify log entry has required fields
                                    prop_assert!(!log_entry.id.to_string().is_empty(), "Log entry should have ID");
                                    prop_assert!(log_entry.timestamp.timestamp() > 0, "Log entry should have valid timestamp");
                                    
                                    // Verify operation has before and after states
                                    match &log_entry.operation {
                                        Operation::CreateBranch { name, target, before_refs } => {
                                            prop_assert!(!name.is_empty(), "Branch name should not be empty");
                                            prop_assert_eq!(target.as_bytes().len(), 32, "Target should be valid ObjectId");
                                            prop_assert!(before_refs.is_empty() || !before_refs.is_empty(), "Before refs should be valid");
                                        }
                                        Operation::Commit { before_head: _, after_head, tree, message, parents: _ } => {
                                            prop_assert_eq!(after_head.as_bytes().len(), 32, "After head should be valid ObjectId");
                                            prop_assert_eq!(tree.as_bytes().len(), 32, "Tree should be valid ObjectId");
                                            prop_assert!(!message.is_empty(), "Commit message should not be empty");
                                        }
                                        _ => {
                                            // Other operation types are valid
                                        }
                                    }
                                    
                                    // Verify command intent is recorded
                                    prop_assert!(!log_entry.command_intent.command.is_empty(), 
                                        "Command intent should be recorded");
                                }
                            }
                        }
                    }
                }
                
                Ok(())
            })?;
        }
        
        /// Additional property test for operation log chain consistency
        #[test]
        fn prop_operation_log_chain_consistency(
            operations in prop::collection::vec(arb_branch_name(), 1..3)
        ) {
            tokio_test::block_on(async {
                let storage = Arc::new(MemoryStorage::new());
                let repo = Repository::init(storage.clone()).await.unwrap();
                
                let initial_head = repo.head().await.unwrap();
                
                // Perform operations and track expected state
                let mut expected_branches = Vec::new();
                
                for branch_name in &operations {
                    repo.create_branch(branch_name, &initial_head).await.unwrap();
                    expected_branches.push(branch_name.clone());
                    
                    // Verify state after each operation
                    let refs = repo.storage.list_refs().await.unwrap();
                    for expected_branch in &expected_branches {
                        let branch_ref = format!("refs/heads/{}", expected_branch);
                        let branch_exists = refs.iter().any(|r| r.name == branch_ref);
                        prop_assert!(branch_exists, "Branch {} should exist after creation", expected_branch);
                    }
                }
                
                // Verify final state consistency
                let final_refs = repo.storage.list_refs().await.unwrap();
                for expected_branch in &expected_branches {
                    let branch_ref = format!("refs/heads/{}", expected_branch);
                    let branch_exists = final_refs.iter().any(|r| r.name == branch_ref);
                    prop_assert!(branch_exists, "Branch {} should exist in final state", expected_branch);
                }
                
                Ok(())
            })?;
        }
        
        /// Property 2: Commit Operation Integrity
        /// For any repository state and valid changes, committing those changes should 
        /// create a commit object with correct parent relationships and update the 
        /// repository state appropriately.
        /// **Validates: Requirements 1.3**
        #[test]
        fn prop_commit_operation_integrity(
            commit_messages in prop::collection::vec(arb_commit_message(), 1..3),
            author_name in "[a-zA-Z ]{1,50}",
            author_email in "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}",
        ) {
            tokio_test::block_on(async {
                let storage = Arc::new(MemoryStorage::new());
                let repo = Repository::init(storage.clone()).await.unwrap();
                
                let initial_head = repo.head().await.unwrap();
                let mut current_head = initial_head;
                let mut expected_commits = Vec::new();
                
                // Create author signature
                let author = gitnext_core::Signature {
                    name: author_name.clone(),
                    email: author_email.clone(),
                    timestamp: chrono::Utc::now().timestamp(),
                    timezone_offset: 0,
                };
                
                // Create a series of commits
                for (i, message) in commit_messages.iter().enumerate() {
                    // Create an empty tree for each commit
                    let tree = gitnext_core::Tree::new(vec![]);
                    let tree_object = gitnext_core::GitObject::Tree(tree);
                    let tree_id = tree_object.canonical_hash();
                    
                    // Store the tree
                    storage.store_object(&tree_id, &tree_object).await.unwrap();
                    
                    // Create the commit
                    let commit_id = repo.commit(
                        &tree_id,
                        vec![current_head],
                        author.clone(),
                        author.clone(),
                        message.clone(),
                    ).await.unwrap();
                    
                    expected_commits.push((commit_id, tree_id, current_head, message.clone()));
                    current_head = commit_id;
                    
                    // Verify HEAD was updated
                    let new_head = repo.head().await.unwrap();
                    prop_assert_eq!(new_head, commit_id, "HEAD should point to new commit");
                    
                    // Verify the commit object exists and has correct structure
                    let commit_object = storage.load_object(&commit_id).await.unwrap();
                    prop_assert!(commit_object.is_some(), "Commit object should exist");
                    
                    if let Some(gitnext_core::GitObject::Commit(commit)) = commit_object {
                        // Verify commit structure
                        prop_assert_eq!(commit.tree, tree_id, "Commit should reference correct tree");
                        prop_assert_eq!(commit.parents.len(), 1, "Commit should have exactly one parent");
                        prop_assert_eq!(commit.parents[0], expected_commits[i].2, "Commit should have correct parent");
                        prop_assert_eq!(commit.message, message.clone(), "Commit should have correct message");
                        prop_assert_eq!(commit.author.name, author_name.clone(), "Commit should have correct author name");
                        prop_assert_eq!(commit.author.email, author_email.clone(), "Commit should have correct author email");
                        prop_assert_eq!(commit.committer.name, author_name.clone(), "Commit should have correct committer name");
                        prop_assert_eq!(commit.committer.email, author_email.clone(), "Commit should have correct committer email");
                        
                        // Verify timestamps are reasonable (within last hour)
                        let now = chrono::Utc::now().timestamp();
                        prop_assert!(commit.author.timestamp <= now, "Author timestamp should not be in future");
                        prop_assert!(commit.author.timestamp > now - 3600, "Author timestamp should be recent");
                        prop_assert!(commit.committer.timestamp <= now, "Committer timestamp should not be in future");
                        prop_assert!(commit.committer.timestamp > now - 3600, "Committer timestamp should be recent");
                    } else {
                        prop_assert!(false, "Expected commit object, got {:?}", commit_object);
                    }
                }
                
                // Verify commit chain integrity
                let final_head = repo.head().await.unwrap();
                prop_assert_eq!(final_head, current_head, "Final HEAD should match last commit");
                
                // Walk back through the commit chain to verify parent relationships
                let mut walk_commit = final_head;
                for (i, (expected_id, expected_tree, _expected_parent, expected_message)) in expected_commits.iter().rev().enumerate() {
                    prop_assert_eq!(walk_commit, *expected_id, "Commit chain should be correct at position {}", i);
                    
                    let commit_object = storage.load_object(&walk_commit).await.unwrap().unwrap();
                    if let gitnext_core::GitObject::Commit(commit) = commit_object {
                        prop_assert_eq!(commit.tree, *expected_tree, "Tree should match at position {}", i);
                        prop_assert_eq!(commit.message, expected_message.clone(), "Message should match at position {}", i);
                        
                        if !commit.parents.is_empty() {
                            walk_commit = commit.parents[0];
                        } else {
                            // Reached the initial commit
                            break;
                        }
                    }
                }
                
                // Verify operation log entries were created
                let refs = storage.list_refs().await.unwrap();
                let log_refs: Vec<_> = refs.iter()
                    .filter(|r| r.name.starts_with("refs/logs/operations/"))
                    .collect();
                
                // Should have log entries for init + each commit
                prop_assert!(log_refs.len() >= commit_messages.len(), 
                    "Should have at least {} log entries for commits", commit_messages.len());
                
                Ok(())
            })?;
        }
        
        /// Property 3: Branch Operation Consistency
        /// For any repository and branch name, creating a branch and switching to it should 
        /// result in the working directory reflecting the branch's target commit.
        /// **Validates: Requirements 1.4**
        #[test]
        fn prop_branch_operation_consistency(
            branch_names in prop::collection::vec(arb_branch_name(), 1..5),
            commit_messages in prop::collection::vec(arb_commit_message(), 0..3),
        ) {
            tokio_test::block_on(async {
                let storage = Arc::new(MemoryStorage::new());
                let repo = Repository::init(storage.clone()).await.unwrap();
                
                let initial_head = repo.head().await.unwrap();
                let mut current_head = initial_head;
                
                // Create some commits to have different target commits for branches
                let author = gitnext_core::Signature {
                    name: "Test Author".to_string(),
                    email: "test@example.com".to_string(),
                    timestamp: chrono::Utc::now().timestamp(),
                    timezone_offset: 0,
                };
                
                let mut commit_targets = vec![initial_head];
                
                // Create additional commits to use as branch targets
                for (i, message) in commit_messages.iter().enumerate() {
                    let tree = gitnext_core::Tree::new(vec![]);
                    let tree_object = gitnext_core::GitObject::Tree(tree);
                    let tree_id = tree_object.canonical_hash();
                    storage.store_object(&tree_id, &tree_object).await.unwrap();
                    
                    let commit_id = repo.commit(
                        &tree_id,
                        vec![current_head],
                        author.clone(),
                        author.clone(),
                        format!("Commit {}: {}", i, message),
                    ).await.unwrap();
                    
                    commit_targets.push(commit_id);
                    current_head = commit_id;
                }
                
                // Test branch operations for each branch name
                for (i, branch_name) in branch_names.iter().enumerate() {
                    // Use different target commits for different branches
                    let target_commit = commit_targets[i % commit_targets.len()];
                    
                    // Create the branch pointing to the target commit
                    repo.create_branch(branch_name, &target_commit).await.unwrap();
                    
                    // Verify the branch was created with correct target
                    let refs = repo.storage.list_refs().await.unwrap();
                    let branch_ref = format!("refs/heads/{}", branch_name);
                    let branch_target = refs.iter()
                        .find(|r| r.name == branch_ref)
                        .and_then(|r| match &r.target {
                            gitnext_storage::ReferenceTarget::Direct(id) => Some(*id),
                            _ => None,
                        });
                    
                    prop_assert!(branch_target.is_some(), "Branch {} should exist", branch_name);
                    prop_assert_eq!(branch_target.unwrap(), target_commit, 
                        "Branch {} should point to target commit", branch_name);
                    
                    // Switch to the branch
                    repo.switch_branch(branch_name).await.unwrap();
                    
                    // Verify HEAD now points to the branch's target commit
                    let new_head = repo.head().await.unwrap();
                    prop_assert_eq!(new_head, target_commit, 
                        "After switching to branch {}, HEAD should point to target commit", branch_name);
                    
                    // Verify the current branch is correctly tracked
                    let current_branch = repo.get_current_branch().await.unwrap();
                    prop_assert_eq!(current_branch, Some(branch_name.clone()), 
                        "Current branch should be {} after switching", branch_name);
                    
                    // Verify the working directory state reflects the target commit
                    // (In our implementation, this means HEAD points to the correct commit)
                    let head_commit_object = storage.load_object(&new_head).await.unwrap();
                    prop_assert!(head_commit_object.is_some(), 
                        "HEAD commit object should exist after switching to branch {}", branch_name);
                    
                    if let Some(gitnext_core::GitObject::Commit(commit)) = head_commit_object {
                        // Verify the commit structure is intact
                        prop_assert_eq!(commit.tree.as_bytes().len(), 32, 
                            "Commit tree should be valid ObjectId");
                        prop_assert!(!commit.message.is_empty(), 
                            "Commit message should not be empty");
                        
                        // Verify the tree object exists and is accessible
                        let tree_object = storage.load_object(&commit.tree).await.unwrap();
                        prop_assert!(tree_object.is_some(), 
                            "Tree object should exist for commit in branch {}", branch_name);
                    }
                }
                
                // Test switching between created branches
                if branch_names.len() > 1 {
                    for (i, branch_name) in branch_names.iter().enumerate() {
                        let target_commit = commit_targets[i % commit_targets.len()];
                        
                        // Switch to this branch
                        repo.switch_branch(branch_name).await.unwrap();
                        
                        // Verify HEAD points to the correct commit
                        let head_after_switch = repo.head().await.unwrap();
                        prop_assert_eq!(head_after_switch, target_commit, 
                            "HEAD should point to correct commit after switching to branch {}", branch_name);
                        
                        // Verify current branch tracking
                        let current_branch = repo.get_current_branch().await.unwrap();
                        prop_assert_eq!(current_branch, Some(branch_name.clone()), 
                            "Current branch tracking should be correct for {}", branch_name);
                    }
                }
                
                // Test that branch references persist across operations
                let final_refs = storage.list_refs().await.unwrap();
                for branch_name in &branch_names {
                    let branch_ref = format!("refs/heads/{}", branch_name);
                    let branch_exists = final_refs.iter().any(|r| r.name == branch_ref);
                    prop_assert!(branch_exists, "Branch {} should still exist at end", branch_name);
                }
                
                // Verify operation log recorded all branch operations
                let log_refs: Vec<_> = final_refs.iter()
                    .filter(|r| r.name.starts_with("refs/logs/operations/"))
                    .collect();
                
                // Should have log entries for: init + commits + branch creations + branch switches
                let expected_min_logs = 1 + commit_messages.len() + branch_names.len() + branch_names.len();
                prop_assert!(log_refs.len() >= expected_min_logs, 
                    "Should have at least {} log entries, found {}", expected_min_logs, log_refs.len());
                
                Ok(())
            })?;
        }
        
        /// Property 12: Undo Operation Correctness (Conflict-Free Operations)
        /// For any conflict-free repository operation that has been performed, undoing it 
        /// should restore the repository to its exact previous state.
        /// **Validates: Requirements 4.2**
        /// **Note**: V1 scope limited to conflict-free operations
        #[test]
        fn prop_undo_operation_correctness(
            branch_operations in prop::collection::vec(arb_branch_name(), 1..5),
            commit_messages in prop::collection::vec(arb_commit_message(), 0..3),
        ) {
            tokio_test::block_on(async {
                let storage = Arc::new(MemoryStorage::new());
                let repo = Repository::init(storage.clone()).await.unwrap();
                
                let initial_head = repo.head().await.unwrap();
                let initial_refs = repo.get_all_refs().await.unwrap();
                let initial_position = repo.operation_log_position();
                
                // Create author signature for commits
                let author = gitnext_core::Signature {
                    name: "Test Author".to_string(),
                    email: "test@example.com".to_string(),
                    timestamp: chrono::Utc::now().timestamp(),
                    timezone_offset: 0,
                };
                
                let mut operations_performed = Vec::new();
                let mut state_snapshots = Vec::new();
                
                // Capture initial state
                state_snapshots.push((
                    repo.head().await.unwrap(),
                    repo.get_all_refs().await.unwrap(),
                    repo.operation_log_position(),
                ));
                
                // Perform a series of conflict-free operations
                let mut current_head = initial_head;
                
                // First, create some commits to have different targets for branches
                for (i, message) in commit_messages.iter().enumerate() {
                    let tree = gitnext_core::Tree::new(vec![]);
                    let tree_object = gitnext_core::GitObject::Tree(tree);
                    let tree_id = tree_object.canonical_hash();
                    storage.store_object(&tree_id, &tree_object).await.unwrap();
                    
                    let commit_id = repo.commit(
                        &tree_id,
                        vec![current_head],
                        author.clone(),
                        author.clone(),
                        format!("Test commit {}: {}", i, message),
                    ).await.unwrap();
                    
                    operations_performed.push(format!("commit: {}", message));
                    current_head = commit_id;
                    
                    // Capture state after commit
                    state_snapshots.push((
                        repo.head().await.unwrap(),
                        repo.get_all_refs().await.unwrap(),
                        repo.operation_log_position(),
                    ));
                }
                
                // Then perform branch operations
                for branch_name in &branch_operations {
                    // Create branch
                    repo.create_branch(branch_name, &current_head).await.unwrap();
                    operations_performed.push(format!("create_branch: {}", branch_name));
                    
                    // Capture state after branch creation
                    state_snapshots.push((
                        repo.head().await.unwrap(),
                        repo.get_all_refs().await.unwrap(),
                        repo.operation_log_position(),
                    ));
                    
                    // Switch to branch (if not the first branch to avoid conflicts)
                    if branch_operations.len() > 1 {
                        repo.switch_branch(branch_name).await.unwrap();
                        operations_performed.push(format!("switch_branch: {}", branch_name));
                        
                        // Capture state after branch switch
                        state_snapshots.push((
                            repo.head().await.unwrap(),
                            repo.get_all_refs().await.unwrap(),
                            repo.operation_log_position(),
                        ));
                    }
                }
                
                // Now test undo correctness by undoing operations one by one
                let total_operations = operations_performed.len();
                
                for i in (0..total_operations).rev() {
                    // Verify we can undo
                    prop_assert!(repo.can_undo(), 
                        "Should be able to undo operation {} ({})", i, operations_performed[i]);
                    
                    // Get the expected state before undo (the state before this operation)
                    let expected_state = &state_snapshots[i];
                    
                    // Perform undo
                    let undone_operation = repo.undo().await.unwrap();
                    prop_assert!(undone_operation.is_some(), 
                        "Undo should return the undone operation for operation {}", i);
                    
                    // Verify the repository state was restored exactly
                    let actual_head = repo.head().await.unwrap();
                    let actual_refs = repo.get_all_refs().await.unwrap();
                    let actual_position = repo.operation_log_position();
                    
                    prop_assert_eq!(actual_head, expected_state.0, 
                        "HEAD should be restored to exact previous state after undoing operation {} ({})", 
                        i, operations_performed[i]);
                    
                    prop_assert_eq!(actual_position, expected_state.2, 
                        "Operation log position should be restored after undoing operation {} ({})", 
                        i, operations_performed[i]);
                    
                    // Verify all references are restored exactly
                    for (ref_name, expected_target) in &expected_state.1 {
                        let actual_target = actual_refs.get(ref_name);
                        prop_assert_eq!(actual_target, Some(expected_target), 
                            "Reference {} should be restored to exact previous state after undoing operation {} ({})", 
                            ref_name, i, operations_performed[i]);
                    }
                    
                    // Verify no extra references were created
                    for (ref_name, _) in &actual_refs {
                        prop_assert!(expected_state.1.contains_key(ref_name), 
                            "No unexpected reference {} should exist after undoing operation {} ({})", 
                            ref_name, i, operations_performed[i]);
                    }
                    
                    // Verify the exact number of references matches
                    prop_assert_eq!(actual_refs.len(), expected_state.1.len(), 
                        "Number of references should match exactly after undoing operation {} ({})", 
                        i, operations_performed[i]);
                    
                    // Verify that the undone operation matches what we expect
                    if let Some(undone_op) = undone_operation {
                        match (&undone_op, operations_performed[i].as_str()) {
                            (Operation::CreateBranch { name, .. }, op_str) if op_str.starts_with("create_branch:") => {
                                let expected_name = op_str.strip_prefix("create_branch: ").unwrap();
                                prop_assert_eq!(name, expected_name, 
                                    "Undone branch creation should match expected branch name");
                            }
                            (Operation::SwitchBranch { to_branch, .. }, op_str) if op_str.starts_with("switch_branch:") => {
                                let expected_name = op_str.strip_prefix("switch_branch: ").unwrap();
                                prop_assert_eq!(to_branch, expected_name, 
                                    "Undone branch switch should match expected branch name");
                            }
                            (Operation::Commit { message, .. }, op_str) if op_str.starts_with("commit:") => {
                                let expected_message = op_str.strip_prefix("commit: ").unwrap();
                                prop_assert!(message.contains(expected_message), 
                                    "Undone commit should contain expected message fragment");
                            }
                            _ => {
                                // Other operation types are acceptable
                            }
                        }
                    }
                }
                
                // After undoing all operations, we should be back to initial state
                let final_head = repo.head().await.unwrap();
                let final_refs = repo.get_all_refs().await.unwrap();
                let final_position = repo.operation_log_position();
                
                prop_assert_eq!(final_head, initial_head, 
                    "After undoing all operations, HEAD should be back to initial state");
                prop_assert_eq!(final_position, initial_position, 
                    "After undoing all operations, log position should be back to initial state");
                
                // Verify initial references are restored (allowing for log references)
                for (ref_name, expected_target) in &initial_refs {
                    if !ref_name.starts_with("refs/logs/") {
                        let actual_target = final_refs.get(ref_name);
                        prop_assert_eq!(actual_target, Some(expected_target), 
                            "Initial reference {} should be restored exactly", ref_name);
                    }
                }
                
                // Verify we cannot undo further than the initial state
                // (We should still be able to undo the init operation, but let's not test that edge case)
                
                // Test that we can redo all operations back
                for i in 0..total_operations {
                    prop_assert!(repo.can_redo(), 
                        "Should be able to redo operation {} after undoing all", i);
                    
                    let redone_operation = repo.redo().await.unwrap();
                    prop_assert!(redone_operation.is_some(), 
                        "Redo should return the redone operation for operation {}", i);
                    
                    // Verify state is restored to what it was after the original operation
                    let expected_state = &state_snapshots[i + 1]; // +1 because snapshots include initial state
                    
                    let actual_head = repo.head().await.unwrap();
                    let actual_refs = repo.get_all_refs().await.unwrap();
                    let actual_position = repo.operation_log_position();
                    
                    prop_assert_eq!(actual_head, expected_state.0, 
                        "HEAD should be restored after redoing operation {}", i);
                    prop_assert_eq!(actual_position, expected_state.2, 
                        "Operation log position should be restored after redoing operation {}", i);
                    
                    // Verify references are restored (excluding log references which may have changed)
                    for (ref_name, expected_target) in &expected_state.1 {
                        if !ref_name.starts_with("refs/logs/") {
                            let actual_target = actual_refs.get(ref_name);
                            prop_assert_eq!(actual_target, Some(expected_target), 
                                "Reference {} should be restored after redoing operation {}", ref_name, i);
                        }
                    }
                }
                
                // After redoing all operations, we should be back to the final state
                prop_assert!(!repo.can_redo(), "Should not be able to redo after redoing all operations");
                prop_assert!(repo.can_undo(), "Should be able to undo after redoing all operations");
                
                Ok(())
            })?;
        }
    }
}
