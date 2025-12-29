use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::fmt;

/// Content hash using BLAKE3 (256-bit)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// For Git compatibility: compute SHA-1
    pub fn to_git_sha1(&self) -> GitHash {
        GitHash::from_content(self.as_bytes())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

/// Git-compatible SHA-1 hash (for compatibility layer)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct GitHash([u8; 20]);

impl GitHash {
    pub fn from_content(content: &[u8]) -> Self {
        let mut hasher = Sha1::new();
        hasher.update(content);
        let result = hasher.finalize();
        let mut bytes = [0u8; 20];
        bytes.copy_from_slice(&result);
        Self(bytes)
    }
}

/// Repository ID (stable across clones/forks)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RepoId(uuid::Uuid);

impl RepoId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl Default for RepoId {
    fn default() -> Self {
        Self::new()
    }
}

/// Artifact ID (stable identity for code entities)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactId(uuid::Uuid);

/// Operation ID (for operation log)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperationId(ulid::Ulid);

impl OperationId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new())
    }

    /// Sortable by creation time
    pub fn timestamp(&self) -> i64 {
        self.0.timestamp_ms() as i64
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

/// Core object types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Object {
    Blob(Blob),
    Tree(Tree),
    Commit(Commit),
    Tag(Tag),
}

impl Object {
    pub fn hash(&self) -> Hash {
        let bytes = bincode::serialize(self).unwrap();
        Hash::from_bytes(&bytes)
    }

    pub fn object_type(&self) -> ObjectType {
        match self {
            Object::Blob(_) => ObjectType::Blob,
            Object::Tree(_) => ObjectType::Tree,
            Object::Commit(_) => ObjectType::Commit,
            Object::Tag(_) => ObjectType::Tag,
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

/// Blob: Raw content with optional chunking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Blob {
    /// For small files: inline content
    pub content: Option<bytes::Bytes>,

    /// For large files: content-defined chunks
    pub chunks: Option<Vec<Hash>>,

    /// Total size in bytes
    pub size: u64,

    /// Optional metadata
    pub metadata: BlobMetadata,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BlobMetadata {
    pub mime_type: Option<String>,
    pub encoding: Option<String>,
    pub language: Option<String>,
}

/// Tree: Directory structure
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
    pub hash: Hash,
    pub entry_type: ObjectType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileMode {
    Normal = 0o100644,
    Executable = 0o100755,
    Symlink = 0o120000,
    Tree = 0o040000,
}

/// Commit: Snapshot with history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Commit {
    pub tree: Hash,
    pub parents: Vec<Hash>,
    pub author: Signature,
    pub committer: Signature,
    pub message: String,

    /// Generation number for fast ancestry queries
    pub generation: u64,

    /// NEW: Artifact changes in this commit
    pub artifact_changes: Vec<ArtifactChange>,

    /// Optional: GPG signature
    pub signature: Option<bytes::Bytes>,
}

impl Commit {
    /// Compute generation number from parents
    pub fn compute_generation(parents: &[u64]) -> u64 {
        parents.iter().max().map(|g| g + 1).unwrap_or(0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Signature {
    pub name: String,
    pub email: String,
    pub timestamp: i64,
    pub timezone_offset: i16,
}

/// Tag: Named reference to object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub target: Hash,
    pub target_type: ObjectType,
    pub name: String,
    pub tagger: Signature,
    pub message: String,
    pub signature: Option<bytes::Bytes>,
}

/// Artifact change tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactChange {
    pub artifact_id: ArtifactId,
    pub change_type: ChangeType,
    pub old_content: Option<Hash>,
    pub new_content: Option<Hash>,
    pub metadata: ArtifactMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChangeType {
    Created,
    Modified,
    Deleted,
    Renamed,
    Split,
    Merged,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub artifact_type: ArtifactType,
    pub name: String,
    pub path: String,
    pub language: Option<String>,
    pub signature: Option<String>, // Function signature, type definition, etc.
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactType {
    File,
    Function,
    Type,
    Module,
    Class,
    Interface,
}

/// Reference types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Reference {
    Direct(Hash),
    Symbolic(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefUpdate {
    pub name: String,
    pub old: Option<Hash>,
    pub new: Hash,
}

/// Repository metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoMetadata {
    pub id: RepoId,
    pub created_at: i64,
    pub default_branch: String,
    pub description: Option<String>,
}

/// Operation log entry (never lose work!)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: OperationId,
    pub timestamp: i64,
    pub command: String,
    pub args: Vec<String>,
    pub before_refs: Vec<(String, Hash)>,
    pub after_refs: Vec<(String, Hash)>,
    pub user: String,
}

impl Operation {
    pub fn new(command: String, args: Vec<String>) -> Self {
        Self {
            id: OperationId::new(),
            timestamp: chrono::Utc::now().timestamp(),
            command,
            args,
            before_refs: Vec::new(),
            after_refs: Vec::new(),
            user: whoami::username(),
        }
    }
}

/// Merge conflict representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    pub path: String,
    pub ours: Hash,
    pub theirs: Hash,
    pub base: Option<Hash>,
    pub regions: Vec<ConflictRegion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictRegion {
    pub start_line: usize,
    pub end_line: usize,
    pub ours_content: String,
    pub theirs_content: String,
    pub base_content: Option<String>,
}
