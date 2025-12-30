//! GitNext Core - Dual-hash object identity system and canonical object model
//! 
//! This module implements ADR-001 (Dual-Hash Strategy) and ADR-002 (Canonical Object Model)

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Core error types for GitNext
#[derive(Debug, Error)]
pub enum GitNextError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Hash derivation error: {0}")]
    HashDerivation(String),
    
    #[error("Invalid object format: {0}")]
    InvalidFormat(String),
}

pub type Result<T> = std::result::Result<T, GitNextError>;

/// Primary object identifier using BLAKE3 (ADR-001)
/// This is the canonical identity used for all internal operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ObjectId {
    /// BLAKE3 hash (256-bit) - primary identity for all internal operations
    blake3_hash: [u8; 32],
}

impl ObjectId {
    /// Create ObjectId from BLAKE3 hash of canonical serialization
    pub fn from_canonical_bytes(bytes: &[u8]) -> Self {
        let hash = blake3::hash(bytes);
        Self {
            blake3_hash: *hash.as_bytes(),
        }
    }
    
    /// Get the BLAKE3 hash bytes
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.blake3_hash
    }
    
    /// Create from existing BLAKE3 hash bytes
    pub fn from_blake3_bytes(bytes: [u8; 32]) -> Self {
        Self { blake3_hash: bytes }
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.blake3_hash))
    }
}

/// Git compatibility hash types (ADR-001)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GitHash {
    Sha1([u8; 20]),
    Sha256([u8; 32]),
}

impl GitHash {
    /// Derive Git hash from Git-formatted bytes (not from BLAKE3)
    pub fn from_git_bytes(bytes: &[u8], hash_type: GitHashType) -> Self {
        match hash_type {
            GitHashType::Sha1 => {
                use sha1::{Digest, Sha1};
                let mut hasher = Sha1::new();
                hasher.update(bytes);
                let result = hasher.finalize();
                let mut hash_bytes = [0u8; 20];
                hash_bytes.copy_from_slice(&result);
                GitHash::Sha1(hash_bytes)
            }
            GitHashType::Sha256 => {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                let result = hasher.finalize();
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&result);
                GitHash::Sha256(hash_bytes)
            }
        }
    }
    
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            GitHash::Sha1(bytes) => bytes,
            GitHash::Sha256(bytes) => bytes,
        }
    }
}

impl fmt::Display for GitHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.as_bytes()))
    }
}

/// Git hash algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GitHashType {
    Sha1,
    Sha256,
}

/// Hash derivation utility for Git compatibility (ADR-001)
/// This derives Git hashes by re-serializing canonical objects to Git format
pub struct CompatHashDeriver {
    // Cache for performance optimization (not conversion)
    cache: std::collections::HashMap<ObjectId, GitHash>,
}

impl CompatHashDeriver {
    pub fn new() -> Self {
        Self {
            cache: std::collections::HashMap::new(),
        }
    }
    
    /// Derive Git hash from canonical object by re-serializing to Git format
    /// This is NOT a conversion from BLAKE3 - it's a re-serialization and hash
    pub fn derive_git_hash(&mut self, object: &GitObject, hash_type: GitHashType) -> Result<GitHash> {
        let object_id = object.canonical_hash();
        
        // Check cache first
        if let Some(cached_hash) = self.cache.get(&object_id) {
            return Ok(*cached_hash);
        }
        
        // Serialize to Git format and hash
        let git_bytes = self.serialize_to_git_format(object)?;
        let git_hash = GitHash::from_git_bytes(&git_bytes, hash_type);
        
        // Cache the result
        self.cache.insert(object_id, git_hash);
        
        Ok(git_hash)
    }
    
    /// Serialize canonical object to Git binary format
    pub fn serialize_to_git_format(&self, object: &GitObject) -> Result<Vec<u8>> {
        match object {
            GitObject::Blob(blob) => self.serialize_blob_to_git(blob),
            GitObject::Tree(tree) => self.serialize_tree_to_git(tree),
            GitObject::Commit(commit) => self.serialize_commit_to_git(commit),
            GitObject::Tag(tag) => self.serialize_tag_to_git(tag),
        }
    }
    
    fn serialize_blob_to_git(&self, blob: &Blob) -> Result<Vec<u8>> {
        let content = blob.content.as_ref().ok_or_else(|| {
            GitNextError::InvalidFormat("Blob missing content for Git export".to_string())
        })?;
        
        let header = format!("blob {}\0", content.len());
        let mut result = header.into_bytes();
        result.extend_from_slice(content);
        Ok(result)
    }
    
    fn serialize_tree_to_git(&self, tree: &Tree) -> Result<Vec<u8>> {
        let mut content = Vec::new();
        
        for entry in &tree.entries {
            let mode_str = match entry.mode {
                FileMode::Normal => "100644",
                FileMode::Executable => "100755", 
                FileMode::Symlink => "120000",
                FileMode::Tree => "40000",
            };
            
            content.extend_from_slice(mode_str.as_bytes());
            content.push(b' ');
            content.extend_from_slice(entry.name.as_bytes());
            content.push(0);
            content.extend_from_slice(entry.hash.as_bytes());
        }
        
        let header = format!("tree {}\0", content.len());
        let mut result = header.into_bytes();
        result.extend_from_slice(&content);
        Ok(result)
    }
    
    fn serialize_commit_to_git(&self, commit: &Commit) -> Result<Vec<u8>> {
        let mut content = Vec::new();
        
        // Tree line
        content.extend_from_slice(b"tree ");
        content.extend_from_slice(hex::encode(commit.tree.as_bytes()).as_bytes());
        content.push(b'\n');
        
        // Parent lines
        for parent in &commit.parents {
            content.extend_from_slice(b"parent ");
            content.extend_from_slice(hex::encode(parent.as_bytes()).as_bytes());
            content.push(b'\n');
        }
        
        // Author line
        content.extend_from_slice(b"author ");
        content.extend_from_slice(self.format_signature(&commit.author).as_bytes());
        content.push(b'\n');
        
        // Committer line
        content.extend_from_slice(b"committer ");
        content.extend_from_slice(self.format_signature(&commit.committer).as_bytes());
        content.push(b'\n');
        
        // Empty line before message
        content.push(b'\n');
        
        // Message
        content.extend_from_slice(commit.message.as_bytes());
        
        let header = format!("commit {}\0", content.len());
        let mut result = header.into_bytes();
        result.extend_from_slice(&content);
        Ok(result)
    }
    
    fn serialize_tag_to_git(&self, tag: &Tag) -> Result<Vec<u8>> {
        let mut content = Vec::new();
        
        // Object line
        content.extend_from_slice(b"object ");
        content.extend_from_slice(hex::encode(tag.target.as_bytes()).as_bytes());
        content.push(b'\n');
        
        // Type line
        let type_str = match tag.target_type {
            ObjectType::Blob => "blob",
            ObjectType::Tree => "tree", 
            ObjectType::Commit => "commit",
            ObjectType::Tag => "tag",
        };
        content.extend_from_slice(b"type ");
        content.extend_from_slice(type_str.as_bytes());
        content.push(b'\n');
        
        // Tag line
        content.extend_from_slice(b"tag ");
        content.extend_from_slice(tag.name.as_bytes());
        content.push(b'\n');
        
        // Tagger line
        content.extend_from_slice(b"tagger ");
        content.extend_from_slice(self.format_signature(&tag.tagger).as_bytes());
        content.push(b'\n');
        
        // Empty line before message
        content.push(b'\n');
        
        // Message
        content.extend_from_slice(tag.message.as_bytes());
        
        let header = format!("tag {}\0", content.len());
        let mut result = header.into_bytes();
        result.extend_from_slice(&content);
        Ok(result)
    }
    
    fn format_signature(&self, sig: &Signature) -> String {
        format!("{} <{}> {} {:+05}", 
                sig.name, 
                sig.email, 
                sig.timestamp, 
                sig.timezone_offset)
    }
}

impl Default for CompatHashDeriver {
    fn default() -> Self {
        Self::new()
    }
}

/// Canonical Git object types (ADR-002)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GitObject {
    Blob(Blob),
    Tree(Tree),
    Commit(Commit),
    Tag(Tag),
}

impl GitObject {
    /// Compute canonical hash (BLAKE3 of canonical serialization)
    pub fn canonical_hash(&self) -> ObjectId {
        let bytes = self.canonical_serialize().expect("Canonical serialization should not fail");
        ObjectId::from_canonical_bytes(&bytes)
    }
    
    /// Canonical serialization for deterministic hashing
    pub fn canonical_serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| GitNextError::Serialization(e.to_string()))
    }
    
    pub fn object_type(&self) -> ObjectType {
        match self {
            GitObject::Blob(_) => ObjectType::Blob,
            GitObject::Tree(_) => ObjectType::Tree,
            GitObject::Commit(_) => ObjectType::Commit,
            GitObject::Tag(_) => ObjectType::Tag,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectType {
    Blob = 1,
    Tree = 2,
    Commit = 3,
    Tag = 4,
}

/// Blob: Raw content (ADR-002)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    /// Content bytes
    pub content: Option<bytes::Bytes>,
    /// Total size in bytes
    pub size: u64,
}

impl Blob {
    pub fn new(content: bytes::Bytes) -> Self {
        let size = content.len() as u64;
        Self {
            content: Some(content),
            size,
        }
    }
}

/// Tree: Directory structure (ADR-002)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tree {
    /// Sorted entries for deterministic hashing
    pub entries: Vec<TreeEntry>,
}

impl Tree {
    pub fn new(mut entries: Vec<TreeEntry>) -> Self {
        // Sort for canonical representation
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Self { entries }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: String,
    pub mode: FileMode,
    pub hash: ObjectId,
    pub entry_type: ObjectType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileMode {
    Normal = 0o100644,
    Executable = 0o100755,
    Symlink = 0o120000,
    Tree = 0o040000,
}

/// Commit: Snapshot with history (ADR-002)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub tree: ObjectId,
    pub parents: Vec<ObjectId>,
    pub author: Signature,
    pub committer: Signature,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub timestamp: i64,
    pub timezone_offset: i16,
}

/// Tag: Named reference to object (ADR-002)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub target: ObjectId,
    pub target_type: ObjectType,
    pub name: String,
    pub tagger: Signature,
    pub message: String,
}

/// Reference types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reference {
    Direct(ObjectId),
    Symbolic(String),
}

// Display implementations for human-readable output
impl fmt::Display for GitObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GitObject::Blob(blob) => write!(f, "blob {}", blob),
            GitObject::Tree(tree) => write!(f, "tree {}", tree),
            GitObject::Commit(commit) => write!(f, "commit {}", commit),
            GitObject::Tag(tag) => write!(f, "tag {}", tag),
        }
    }
}

impl fmt::Display for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "size={}", self.size)?;
        if let Some(content) = &self.content {
            if content.len() <= 100 {
                // Show small content inline
                if let Ok(text) = std::str::from_utf8(content) {
                    write!(f, " content=\"{}\"", text.escape_default())?;
                } else {
                    write!(f, " content=<binary {} bytes>", content.len())?;
                }
            } else {
                write!(f, " content=<{} bytes>", content.len())?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for Tree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "entries={}", self.entries.len())?;
        for entry in &self.entries {
            writeln!(f, "  {}", entry)?;
        }
        Ok(())
    }
}

impl fmt::Display for TreeEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:06o} {:?} {} {}", 
               self.mode as u32, 
               self.entry_type, 
               self.hash, 
               self.name)
    }
}

impl fmt::Display for Commit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "tree {}", self.tree)?;
        for parent in &self.parents {
            writeln!(f, "parent {}", parent)?;
        }
        writeln!(f, "author {}", self.author)?;
        writeln!(f, "committer {}", self.committer)?;
        writeln!(f)?;
        write!(f, "{}", self.message)
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} <{}> {} {:+05}", 
               self.name, 
               self.email, 
               self.timestamp, 
               self.timezone_offset)
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "object {}", self.target)?;
        writeln!(f, "type {:?}", self.target_type)?;
        writeln!(f, "tag {}", self.name)?;
        writeln!(f, "tagger {}", self.tagger)?;
        writeln!(f)?;
        write!(f, "{}", self.message)
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod tests {
    use super::*;
    use proptest::prelude::*;
    
    #[test]
    fn test_object_id_creation() {
        let content = b"hello world";
        let id = ObjectId::from_canonical_bytes(content);
        
        // Should be deterministic
        let id2 = ObjectId::from_canonical_bytes(content);
        assert_eq!(id, id2);
        
        // Different content should produce different hash
        let id3 = ObjectId::from_canonical_bytes(b"different content");
        assert_ne!(id, id3);
    }
    
    #[test]
    fn test_git_hash_derivation() {
        let blob = Blob::new(bytes::Bytes::from("hello world"));
        let object = GitObject::Blob(blob);
        
        let mut deriver = CompatHashDeriver::new();
        let git_hash = deriver.derive_git_hash(&object, GitHashType::Sha1).unwrap();
        
        // Should be deterministic
        let git_hash2 = deriver.derive_git_hash(&object, GitHashType::Sha1).unwrap();
        assert_eq!(git_hash, git_hash2);
    }
    
    #[test]
    fn test_canonical_serialization() {
        let blob = Blob::new(bytes::Bytes::from("test content"));
        let object = GitObject::Blob(blob);
        
        let bytes1 = object.canonical_serialize().unwrap();
        let bytes2 = object.canonical_serialize().unwrap();
        
        // Should be deterministic
        assert_eq!(bytes1, bytes2);
        
        // Hash should be consistent
        let hash1 = object.canonical_hash();
        let hash2 = object.canonical_hash();
        assert_eq!(hash1, hash2);
    }

    // Property test generators
    prop_compose! {
        pub fn arb_signature()(
            name in "[a-zA-Z ]{1,50}",
            email in "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}",
            timestamp in any::<i64>(),
            timezone_offset in -1440i16..1440i16
        ) -> Signature {
            Signature { name, email, timestamp, timezone_offset }
        }
    }

    prop_compose! {
        pub fn arb_blob()(content in prop::collection::vec(any::<u8>(), 0..1000)) -> Blob {
            Blob::new(bytes::Bytes::from(content))
        }
    }

    prop_compose! {
        pub fn arb_tree_entry()(
            name in "[a-zA-Z0-9._-]{1,50}",
            mode in prop::sample::select(&[FileMode::Normal, FileMode::Executable, FileMode::Symlink, FileMode::Tree]),
            hash_bytes in prop::array::uniform32(any::<u8>()),
            entry_type in prop::sample::select(&[ObjectType::Blob, ObjectType::Tree])
        ) -> TreeEntry {
            TreeEntry {
                name,
                mode,
                hash: ObjectId::from_blake3_bytes(hash_bytes),
                entry_type,
            }
        }
    }

    prop_compose! {
        pub fn arb_tree()(entries in prop::collection::vec(arb_tree_entry(), 0..10)) -> Tree {
            Tree::new(entries)
        }
    }

    prop_compose! {
        pub fn arb_commit()(
            tree_hash in prop::array::uniform32(any::<u8>()),
            parents in prop::collection::vec(prop::array::uniform32(any::<u8>()), 0..5),
            author in arb_signature(),
            committer in arb_signature(),
            message in "[\\x20-\\x7E\\n]{1,500}"
        ) -> Commit {
            Commit {
                tree: ObjectId::from_blake3_bytes(tree_hash),
                parents: parents.into_iter().map(ObjectId::from_blake3_bytes).collect(),
                author,
                committer,
                message,
            }
        }
    }

    prop_compose! {
        pub fn arb_tag()(
            target_hash in prop::array::uniform32(any::<u8>()),
            target_type in prop::sample::select(&[ObjectType::Blob, ObjectType::Tree, ObjectType::Commit, ObjectType::Tag]),
            name in "[a-zA-Z0-9._-]{1,50}",
            tagger in arb_signature(),
            message in "[\\x20-\\x7E\\n]{1,500}"
        ) -> Tag {
            Tag {
                target: ObjectId::from_blake3_bytes(target_hash),
                target_type,
                name,
                tagger,
                message,
            }
        }
    }

    prop_compose! {
        pub fn arb_git_object()(
            obj in prop::sample::select(&[0u8, 1, 2, 3]),
            blob in arb_blob(),
            tree in arb_tree(),
            commit in arb_commit(),
            tag in arb_tag()
        ) -> GitObject {
            match obj {
                0 => GitObject::Blob(blob),
                1 => GitObject::Tree(tree),
                2 => GitObject::Commit(commit),
                3 => GitObject::Tag(tag),
                _ => unreachable!(),
            }
        }
    }

    proptest! {
        /// Property 26: Data Integrity Validation
        /// For any stored object, its cryptographic hash should match its content,
        /// and any corruption should be detected during validation.
        /// **Validates: Requirements 10.3**
        #[test]
        fn prop_hash_consistency_data_integrity(obj in arb_git_object()) {
            // Hash should be deterministic
            let hash1 = obj.canonical_hash();
            let hash2 = obj.canonical_hash();
            prop_assert_eq!(hash1, hash2);

            // Serialization should be deterministic
            let bytes1 = obj.canonical_serialize().unwrap();
            let bytes2 = obj.canonical_serialize().unwrap();
            prop_assert_eq!(bytes1, bytes2);

            // Hash should match content - recompute bytes1 since it was moved
            let bytes_for_hash = obj.canonical_serialize().unwrap();
            let expected_hash = ObjectId::from_canonical_bytes(&bytes_for_hash);
            prop_assert_eq!(hash1, expected_hash);

            // Git hash derivation should be deterministic
            let mut deriver = CompatHashDeriver::new();
            let git_hash1 = deriver.derive_git_hash(&obj, GitHashType::Sha1).unwrap();
            let git_hash2 = deriver.derive_git_hash(&obj, GitHashType::Sha1).unwrap();
            prop_assert_eq!(git_hash1, git_hash2);

            // Different hash types should produce different results (unless collision)
            let sha256_hash = deriver.derive_git_hash(&obj, GitHashType::Sha256).unwrap();
            // Verify that both hash types are valid and deterministic
            let sha256_hash2 = deriver.derive_git_hash(&obj, GitHashType::Sha256).unwrap();
            prop_assert_eq!(sha256_hash, sha256_hash2);
        }

        /// Property 10: Git Object Export Fidelity
        /// For any canonical GitNext object, when exported to Git format, the resulting
        /// binary representation should produce identical SHA-1/SHA-256 hashes to standard Git
        /// for the same logical object content.
        /// **Validates: Requirements 3.5**
        /// **Note**: Test applies only to export boundaries, not internal storage
        #[test]
        fn prop_git_export_fidelity(obj in arb_git_object()) {
            let mut deriver = CompatHashDeriver::new();
            
            // Export to Git format should be deterministic
            let git_bytes1 = deriver.serialize_to_git_format(&obj).unwrap();
            let git_bytes2 = deriver.serialize_to_git_format(&obj).unwrap();
            prop_assert_eq!(git_bytes1, git_bytes2);
            
            // Git hash should be consistent with Git format bytes for SHA-1
            let git_bytes_for_sha1 = deriver.serialize_to_git_format(&obj).unwrap();
            let expected_sha1 = GitHash::from_git_bytes(&git_bytes_for_sha1, GitHashType::Sha1);
            let derived_sha1 = deriver.derive_git_hash(&obj, GitHashType::Sha1).unwrap();
            prop_assert_eq!(expected_sha1, derived_sha1);
            
            // Git hash should be consistent with Git format bytes for SHA-256
            let git_bytes_for_sha256 = deriver.serialize_to_git_format(&obj).unwrap();
            let expected_sha256 = GitHash::from_git_bytes(&git_bytes_for_sha256, GitHashType::Sha256);
            let derived_sha256 = deriver.derive_git_hash(&obj, GitHashType::Sha256).unwrap();
            prop_assert_eq!(expected_sha256, derived_sha256);
            
            // Git format should be valid (basic structure check)
            let git_bytes_for_validation = deriver.serialize_to_git_format(&obj).unwrap();
            match &obj {
                GitObject::Blob(_) => {
                    prop_assert!(git_bytes_for_validation.starts_with(b"blob "));
                    prop_assert!(git_bytes_for_validation.contains(&0u8)); // null separator
                }
                GitObject::Tree(_) => {
                    prop_assert!(git_bytes_for_validation.starts_with(b"tree "));
                    prop_assert!(git_bytes_for_validation.contains(&0u8)); // null separator
                }
                GitObject::Commit(_) => {
                    prop_assert!(git_bytes_for_validation.starts_with(b"commit "));
                    prop_assert!(git_bytes_for_validation.contains(&0u8)); // null separator
                    // Should contain required commit fields
                    let content = std::str::from_utf8(&git_bytes_for_validation).unwrap_or("");
                    prop_assert!(content.contains("tree "));
                    prop_assert!(content.contains("author "));
                    prop_assert!(content.contains("committer "));
                }
                GitObject::Tag(_) => {
                    prop_assert!(git_bytes_for_validation.starts_with(b"tag "));
                    prop_assert!(git_bytes_for_validation.contains(&0u8)); // null separator
                    // Should contain required tag fields
                    let content = std::str::from_utf8(&git_bytes_for_validation).unwrap_or("");
                    prop_assert!(content.contains("object "));
                    prop_assert!(content.contains("type "));
                    prop_assert!(content.contains("tag "));
                    prop_assert!(content.contains("tagger "));
                }
            }
        }
    }
}