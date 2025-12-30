//! GitNext Objects - Canonical Git object model with immutable design
//! 
//! This module implements ADR-002 (Canonical Object Model) with immutable structs
//! and comprehensive validation and builder utilities.

use gitnext_core::{ObjectId, ObjectType, GitObject, Blob, Tree, Commit, Tag, Signature, TreeEntry, FileMode};
use thiserror::Error;

/// Object-specific error types
#[derive(Debug, Error)]
pub enum ObjectError {
    #[error("Invalid object type: {0}")]
    InvalidType(String),
    
    #[error("Missing required field: {0}")]
    MissingField(String),
    
    #[error("Invalid signature format: {0}")]
    InvalidSignature(String),
    
    #[error("Invalid tree entry: {0}")]
    InvalidTreeEntry(String),
}

pub type Result<T> = std::result::Result<T, ObjectError>;

/// Extended object operations and utilities
pub trait ObjectOps {
    /// Validate object integrity
    fn validate(&self) -> Result<()>;
    
    /// Get object size in bytes
    fn size(&self) -> u64;
    
    /// Check if object is empty/minimal
    fn is_empty(&self) -> bool;
}

impl ObjectOps for GitObject {
    fn validate(&self) -> Result<()> {
        match self {
            GitObject::Blob(blob) => blob.validate(),
            GitObject::Tree(tree) => tree.validate(),
            GitObject::Commit(commit) => commit.validate(),
            GitObject::Tag(tag) => tag.validate(),
        }
    }
    
    fn size(&self) -> u64 {
        match self {
            GitObject::Blob(blob) => blob.size(),
            GitObject::Tree(tree) => tree.size(),
            GitObject::Commit(commit) => commit.size(),
            GitObject::Tag(tag) => tag.size(),
        }
    }
    
    fn is_empty(&self) -> bool {
        match self {
            GitObject::Blob(blob) => blob.is_empty(),
            GitObject::Tree(tree) => tree.is_empty(),
            GitObject::Commit(commit) => commit.is_empty(),
            GitObject::Tag(tag) => tag.is_empty(),
        }
    }
}

impl ObjectOps for Blob {
    fn validate(&self) -> Result<()> {
        if let Some(content) = &self.content {
            if content.len() as u64 != self.size {
                return Err(ObjectError::InvalidType(
                    format!("Blob size mismatch: expected {}, got {}", self.size, content.len())
                ));
            }
        }
        Ok(())
    }
    
    fn size(&self) -> u64 {
        self.size
    }
    
    fn is_empty(&self) -> bool {
        self.size == 0
    }
}

impl ObjectOps for Tree {
    fn validate(&self) -> Result<()> {
        // Check that entries are sorted (canonical requirement)
        for i in 1..self.entries.len() {
            if self.entries[i-1].name >= self.entries[i].name {
                return Err(ObjectError::InvalidTreeEntry(
                    format!("Tree entries not sorted: '{}' >= '{}'", 
                           self.entries[i-1].name, self.entries[i].name)
                ));
            }
        }
        
        // Validate each entry
        for entry in &self.entries {
            entry.validate()?;
        }
        
        Ok(())
    }
    
    fn size(&self) -> u64 {
        // Approximate size calculation
        self.entries.iter()
            .map(|e| e.name.len() as u64 + 32 + 8) // name + hash + metadata
            .sum()
    }
    
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl ObjectOps for Commit {
    fn validate(&self) -> Result<()> {
        if self.message.is_empty() {
            return Err(ObjectError::MissingField("message".to_string()));
        }
        
        self.author.validate()?;
        self.committer.validate()?;
        
        Ok(())
    }
    
    fn size(&self) -> u64 {
        // Approximate size calculation
        32 + // tree hash
        self.parents.len() as u64 * 32 + // parent hashes
        self.author.size() +
        self.committer.size() +
        self.message.len() as u64
    }
    
    fn is_empty(&self) -> bool {
        self.message.is_empty()
    }
}

impl ObjectOps for Tag {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(ObjectError::MissingField("name".to_string()));
        }
        
        if self.message.is_empty() {
            return Err(ObjectError::MissingField("message".to_string()));
        }
        
        self.tagger.validate()?;
        
        Ok(())
    }
    
    fn size(&self) -> u64 {
        // Approximate size calculation
        32 + // target hash
        self.name.len() as u64 +
        self.tagger.size() +
        self.message.len() as u64
    }
    
    fn is_empty(&self) -> bool {
        self.name.is_empty() || self.message.is_empty()
    }
}

impl ObjectOps for Signature {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(ObjectError::InvalidSignature("Empty name".to_string()));
        }
        
        if self.email.is_empty() {
            return Err(ObjectError::InvalidSignature("Empty email".to_string()));
        }
        
        // Basic email validation
        if !self.email.contains('@') {
            return Err(ObjectError::InvalidSignature(
                format!("Invalid email format: {}", self.email)
            ));
        }
        
        Ok(())
    }
    
    fn size(&self) -> u64 {
        self.name.len() as u64 + self.email.len() as u64 + 16 // timestamp + timezone
    }
    
    fn is_empty(&self) -> bool {
        self.name.is_empty() || self.email.is_empty()
    }
}

impl ObjectOps for TreeEntry {
    fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(ObjectError::InvalidTreeEntry("Empty name".to_string()));
        }
        
        // Check for invalid characters in name
        if self.name.contains('/') || self.name.contains('\0') {
            return Err(ObjectError::InvalidTreeEntry(
                format!("Invalid characters in name: {}", self.name)
            ));
        }
        
        // Validate mode matches entry type
        match (self.mode, self.entry_type) {
            (FileMode::Tree, ObjectType::Tree) => Ok(()),
            (FileMode::Normal | FileMode::Executable | FileMode::Symlink, ObjectType::Blob) => Ok(()),
            _ => Err(ObjectError::InvalidTreeEntry(
                format!("Mode {:?} doesn't match type {:?}", self.mode, self.entry_type)
            )),
        }
    }
    
    fn size(&self) -> u64 {
        self.name.len() as u64 + 32 + 8 // name + hash + metadata
    }
    
    fn is_empty(&self) -> bool {
        self.name.is_empty()
    }
}

/// Object builder utilities for creating valid objects
pub struct BlobBuilder {
    content: Option<bytes::Bytes>,
}

impl BlobBuilder {
    pub fn new() -> Self {
        Self { content: None }
    }
    
    pub fn content(mut self, content: impl Into<bytes::Bytes>) -> Self {
        self.content = Some(content.into());
        self
    }
    
    pub fn build(self) -> Blob {
        let content = self.content.unwrap_or_else(|| bytes::Bytes::new());
        Blob::new(content)
    }
}

impl Default for BlobBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TreeBuilder {
    entries: Vec<TreeEntry>,
}

impl TreeBuilder {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }
    
    pub fn entry(mut self, name: impl Into<String>, mode: FileMode, hash: ObjectId, entry_type: ObjectType) -> Self {
        self.entries.push(TreeEntry {
            name: name.into(),
            mode,
            hash,
            entry_type,
        });
        self
    }
    
    pub fn build(self) -> Tree {
        Tree::new(self.entries)
    }
}

impl Default for TreeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CommitBuilder {
    tree: Option<ObjectId>,
    parents: Vec<ObjectId>,
    author: Option<Signature>,
    committer: Option<Signature>,
    message: String,
}

impl CommitBuilder {
    pub fn new() -> Self {
        Self {
            tree: None,
            parents: Vec::new(),
            author: None,
            committer: None,
            message: String::new(),
        }
    }
    
    pub fn tree(mut self, tree: ObjectId) -> Self {
        self.tree = Some(tree);
        self
    }
    
    pub fn parent(mut self, parent: ObjectId) -> Self {
        self.parents.push(parent);
        self
    }
    
    pub fn author(mut self, author: Signature) -> Self {
        self.author = Some(author);
        self
    }
    
    pub fn committer(mut self, committer: Signature) -> Self {
        self.committer = Some(committer);
        self
    }
    
    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }
    
    pub fn build(self) -> Result<Commit> {
        let tree = self.tree.ok_or_else(|| ObjectError::MissingField("tree".to_string()))?;
        let author = self.author.ok_or_else(|| ObjectError::MissingField("author".to_string()))?;
        let committer = self.committer.ok_or_else(|| ObjectError::MissingField("committer".to_string()))?;
        
        Ok(Commit {
            tree,
            parents: self.parents,
            author,
            committer,
            message: self.message,
        })
    }
}

impl Default for CommitBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_blob_operations() {
        let blob = BlobBuilder::new()
            .content("hello world")
            .build();
        
        assert_eq!(blob.size(), 11);
        assert!(!blob.is_empty());
        assert!(blob.validate().is_ok());
        
        let display = format!("{}", blob);
        assert!(display.contains("size=11"));
        assert!(display.contains("hello world"));
    }
    
    #[test]
    fn test_tree_operations() {
        let hash1 = ObjectId::from_canonical_bytes(b"test1");
        let hash2 = ObjectId::from_canonical_bytes(b"test2");
        
        let tree = TreeBuilder::new()
            .entry("file1.txt", FileMode::Normal, hash1, ObjectType::Blob)
            .entry("file2.txt", FileMode::Executable, hash2, ObjectType::Blob)
            .build();
        
        assert!(!tree.is_empty());
        assert!(tree.validate().is_ok());
        
        let display = format!("{}", tree);
        assert!(display.contains("entries=2"));
        assert!(display.contains("file1.txt"));
        assert!(display.contains("file2.txt"));
    }
    
    #[test]
    fn test_commit_operations() {
        let tree_hash = ObjectId::from_canonical_bytes(b"tree");
        let author = Signature {
            name: "Test Author".to_string(),
            email: "test@example.com".to_string(),
            timestamp: 1234567890,
            timezone_offset: 0,
        };
        
        let commit = CommitBuilder::new()
            .tree(tree_hash)
            .author(author.clone())
            .committer(author)
            .message("Test commit")
            .build()
            .unwrap();
        
        assert!(!commit.is_empty());
        assert!(commit.validate().is_ok());
        
        let display = format!("{}", commit);
        assert!(display.contains("tree"));
        assert!(display.contains("Test Author"));
        assert!(display.contains("Test commit"));
    }
    
    #[test]
    fn test_signature_validation() {
        let valid_sig = Signature {
            name: "Test User".to_string(),
            email: "test@example.com".to_string(),
            timestamp: 1234567890,
            timezone_offset: 0,
        };
        assert!(valid_sig.validate().is_ok());
        
        let invalid_sig = Signature {
            name: "Test User".to_string(),
            email: "invalid-email".to_string(),
            timestamp: 1234567890,
            timezone_offset: 0,
        };
        assert!(invalid_sig.validate().is_err());
    }
    
    #[test]
    fn test_tree_entry_validation() {
        let hash = ObjectId::from_canonical_bytes(b"test");
        
        let valid_entry = TreeEntry {
            name: "valid-name.txt".to_string(),
            mode: FileMode::Normal,
            hash,
            entry_type: ObjectType::Blob,
        };
        assert!(valid_entry.validate().is_ok());
        
        let invalid_entry = TreeEntry {
            name: "invalid/name.txt".to_string(),
            mode: FileMode::Normal,
            hash,
            entry_type: ObjectType::Blob,
        };
        assert!(invalid_entry.validate().is_err());
    }
}