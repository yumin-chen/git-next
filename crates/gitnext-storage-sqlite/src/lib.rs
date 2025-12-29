use async_trait::async_trait;
use gitnext_core::*;
use gitnext_storage::*;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    pub async fn new(path: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect(&format!("sqlite:{}", path))
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let backend = Self { pool };
        backend.initialize_schema().await?;
        Ok(backend)
    }

    pub async fn in_memory() -> Result<Self> {
        Self::new(":memory:").await
    }

    async fn initialize_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS objects (
                hash BLOB PRIMARY KEY NOT NULL,
                type INTEGER NOT NULL,
                data BLOB NOT NULL,
                size INTEGER NOT NULL,
                created_at INTEGER NOT NULL
            ) STRICT;
            CREATE INDEX IF NOT EXISTS idx_objects_type ON objects(type);
            CREATE TABLE IF NOT EXISTS refs (
                name TEXT PRIMARY KEY NOT NULL,
                target_hash BLOB NOT NULL,
                ref_type TEXT NOT NULL,
                symbolic_target TEXT,
                updated_at INTEGER NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS commit_graph (
                hash BLOB PRIMARY KEY NOT NULL,
                parent_hashes BLOB NOT NULL,
                generation INTEGER NOT NULL,
                author_timestamp INTEGER NOT NULL,
                committer_timestamp INTEGER NOT NULL,
                tree_hash BLOB NOT NULL,
                FOREIGN KEY (hash) REFERENCES objects(hash)
            ) STRICT;
            CREATE INDEX IF NOT EXISTS idx_commit_generation ON commit_graph(generation);
            CREATE TABLE IF NOT EXISTS artifact_versions (
                artifact_id BLOB NOT NULL,
                commit_hash BLOB NOT NULL,
                content_hash BLOB NOT NULL,
                metadata BLOB NOT NULL,
                PRIMARY KEY (artifact_id, commit_hash),
                FOREIGN KEY (commit_hash) REFERENCES objects(hash)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS operations (
                id BLOB PRIMARY KEY NOT NULL,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                args BLOB NOT NULL,
                before_refs BLOB NOT NULL,
                after_refs BLOB NOT NULL,
                username TEXT NOT NULL
            ) STRICT;
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL
            ) STRICT;
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for SqliteBackend {
    async fn write_object(&self, hash: Hash, object: &Object) -> Result<()> {
        let data = bincode::serialize(object).map_err(|e| StorageError::Serialization(e.to_string()))?;
        sqlx::query(
            "INSERT INTO objects (hash, type, data, size, created_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(hash.as_bytes())
        .bind(object.object_type() as i32)
        .bind(&data)
        .bind(data.len() as i64)
        .bind(chrono::Utc::now().timestamp())
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn read_object(&self, hash: Hash) -> Result<Object> {
        let row: (Vec<u8>,) = sqlx::query_as("SELECT data FROM objects WHERE hash = ?")
            .bind(hash.as_bytes())
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?
            .ok_or(StorageError::ObjectNotFound(hash))?;
        bincode::deserialize(&row.0).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    async fn exists_object(&self, hash: Hash) -> Result<bool> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM objects WHERE hash = ?")
            .bind(hash.as_bytes())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(count > 0)
    }

    async fn read_ref(&self, name: &str) -> Result<Reference> {
        let row: (String, Vec<u8>, Option<String>) =
            sqlx::query_as("SELECT ref_type, target_hash, symbolic_target FROM refs WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?
                .ok_or_else(|| StorageError::RefNotFound(name.to_string()))?;
        match row.0.as_str() {
            "direct" => {
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&row.1);
                Ok(Reference::Direct(Hash(hash_bytes)))
            }
            "symbolic" => Ok(Reference::Symbolic(row.2.unwrap_or_default())),
            _ => Err(StorageError::Backend("Invalid ref type".to_string())),
        }
    }

    async fn update_ref(&self, name: &str, old: Option<Hash>, new: Hash) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(|e| StorageError::Backend(e.to_string()))?;
        if let Some(old_hash) = old {
            let current_ref = self.read_ref(name).await?;
            if let Reference::Direct(current_hash) = current_ref {
                if current_hash != old_hash {
                    return Err(StorageError::RefUpdateConflict(name.to_string()));
                }
            } else {
                return Err(StorageError::RefUpdateConflict(name.to_string()));
            }
        }
        sqlx::query(
            "INSERT OR REPLACE INTO refs (name, ref_type, target_hash, symbolic_target, updated_at) VALUES (?, 'direct', ?, NULL, ?)",
        )
        .bind(name)
        .bind(new.as_bytes())
        .bind(chrono::Utc::now().timestamp())
        .execute(&mut *tx)
        .await
        .map_err(|e| StorageError::Backend(e.to_string()))?;
        tx.commit().await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn delete_ref(&self, name: &str) -> Result<()> {
        sqlx::query("DELETE FROM refs WHERE name = ?").bind(name).execute(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn list_refs(&self, prefix: Option<&str>) -> Result<Vec<(String, Hash)>> {
        let query_str = "SELECT name, target_hash FROM refs WHERE ref_type = 'direct' AND name LIKE ?";
        let rows: Vec<(String, Vec<u8>)> = sqlx::query_as(query_str)
            .bind(format!("{}%", prefix.unwrap_or("")))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(rows.into_iter().map(|(name, mut hash_bytes)| {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&hash_bytes);
                (name, Hash(hash))
            }).collect())
    }

    async fn write_commit_node(&self, hash: Hash, node: &CommitNode) -> Result<()> {
        let parent_hashes = bincode::serialize(&node.parents).map_err(|e| StorageError::Serialization(e.to_string()))?;
        sqlx::query("INSERT INTO commit_graph (hash, parent_hashes, generation, author_timestamp, committer_timestamp, tree_hash) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(hash.as_bytes())
            .bind(parent_hashes)
            .bind(node.generation as i64)
            .bind(node.author_timestamp)
            .bind(node.committer_timestamp)
            .bind(node.tree.as_bytes())
            .execute(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn read_commit_node(&self, hash: Hash) -> Result<CommitNode> {
        let row: (Vec<u8>, i64, i64, i64, Vec<u8>) = sqlx::query_as("SELECT parent_hashes, generation, author_timestamp, committer_timestamp, tree_hash FROM commit_graph WHERE hash = ?")
            .bind(hash.as_bytes())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?.ok_or(StorageError::ObjectNotFound(hash))?;

        let parents: Vec<Hash> = bincode::deserialize(&row.0).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let mut tree_hash_bytes = [0u8; 32];
        tree_hash_bytes.copy_from_slice(&row.4);

        Ok(CommitNode {
            hash,
            parents,
            generation: row.1 as u64,
            author_timestamp: row.2,
            committer_timestamp: row.3,
            tree: Hash(tree_hash_bytes),
        })
    }

    async fn query_commits(&self, query: &CommitQuery) -> Result<Vec<Hash>> {
        let mut qb = sqlx::QueryBuilder::new("SELECT hash FROM commit_graph WHERE 1=1");
        if let Some(since) = query.since_timestamp {
            qb.push(" AND author_timestamp >= ").push_bind(since);
        }
        if let Some(until) = query.until_timestamp {
            qb.push(" AND author_timestamp <= ").push_bind(until);
        }
        qb.push(" ORDER BY generation DESC");
        if let Some(limit) = query.limit {
            qb.push(" LIMIT ").push_bind(limit as i64);
        }

        let rows: Vec<(Vec<u8>,)> = qb.build_query_as().fetch_all(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(rows.into_iter().map(|(mut hash_bytes,)| {
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&hash_bytes);
            Hash(hash)
        }).collect())
    }

    async fn write_artifact_version(&self, version: &ArtifactVersion) -> Result<()> {
        let metadata = bincode::serialize(&version.metadata).map_err(|e| StorageError::Serialization(e.to_string()))?;
        sqlx::query("INSERT INTO artifact_versions (artifact_id, commit_hash, content_hash, metadata) VALUES (?, ?, ?, ?)")
            .bind(version.artifact_id.0.as_bytes())
            .bind(version.commit.as_bytes())
            .bind(version.content_hash.as_bytes())
            .bind(metadata)
            .execute(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn read_artifact_versions(&self, id: ArtifactId) -> Result<Vec<ArtifactVersion>> {
        let rows: Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> = sqlx::query_as("SELECT commit_hash, content_hash, metadata FROM artifact_versions WHERE artifact_id = ?")
            .bind(id.0.as_bytes())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;

        rows.into_iter().map(|(commit_bytes, content_bytes, metadata_bytes)| {
            let mut commit_hash = [0u8; 32];
            commit_hash.copy_from_slice(&commit_bytes);
            let mut content_hash = [0u8; 32];
            content_hash.copy_from_slice(&content_bytes);
            let metadata: ArtifactMetadata = bincode::deserialize(&metadata_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?;
            Ok(ArtifactVersion {
                artifact_id: id,
                commit: Hash(commit_hash),
                content_hash: Hash(content_hash),
                metadata,
            })
        }).collect()
    }

    async fn query_artifacts(&self, _query: &ArtifactQuery) -> Result<Vec<ArtifactId>> { Ok(Vec::new()) }

    async fn append_operation(&self, op: &Operation) -> Result<()> {
        let args = serde_json::to_vec(&op.args).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let before_refs = bincode::serialize(&op.before_refs).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let after_refs = bincode::serialize(&op.after_refs).map_err(|e| StorageError::Serialization(e.to_string()))?;

        sqlx::query("INSERT INTO operations (id, timestamp, command, args, before_refs, after_refs, username) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(op.id.0.to_bytes())
            .bind(op.timestamp)
            .bind(&op.command)
            .bind(args)
            .bind(before_refs)
            .bind(after_refs)
            .bind(&op.user)
            .execute(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn read_operations(&self, since: Option<OperationId>) -> Result<Vec<Operation>> {
        let q = "SELECT id, timestamp, command, args, before_refs, after_refs, username FROM operations WHERE timestamp > ? ORDER BY timestamp";
        let since_ts = since.map(|s| s.timestamp()).unwrap_or(0);
        let rows: Vec<(Vec<u8>, i64, String, Vec<u8>, Vec<u8>, Vec<u8>, String)> = sqlx::query_as(q).bind(since_ts).fetch_all(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;

        rows.into_iter().map(|(id_bytes, ts, cmd, args_bytes, before_bytes, after_bytes, user)| {
            Ok(Operation {
                id: OperationId(ulid::Ulid::from_bytes(id_bytes.try_into().unwrap())),
                timestamp: ts,
                command: cmd,
                args: serde_json::from_slice(&args_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?,
                before_refs: bincode::deserialize(&before_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?,
                after_refs: bincode::deserialize(&after_bytes).map_err(|e| StorageError::Serialization(e.to_string()))?,
                user,
            })
        }).collect()
    }

    async fn begin_transaction(&self) -> Result<Box<dyn Transaction>> {
        let tx = self.pool.begin().await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(Box::new(SqliteTransaction { tx: Some(tx) }))
    }

    async fn read_metadata(&self) -> Result<RepoMetadata> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT value FROM metadata WHERE key = 'repo'").fetch_optional(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        if let Some(row) = row {
            bincode::deserialize(&row.0).map_err(|e| StorageError::Serialization(e.to_string()))
        } else {
            let meta = RepoMetadata {id: RepoId::new(), created_at: chrono::Utc::now().timestamp(), default_branch: "main".to_string(), description: None };
            self.write_metadata(&meta).await?;
            Ok(meta)
        }
    }

    async fn write_metadata(&self, meta: &RepoMetadata) -> Result<()> {
        let data = bincode::serialize(meta).map_err(|e| StorageError::Serialization(e.to_string()))?;
        sqlx::query("INSERT OR REPLACE INTO metadata (key, value) VALUES ('repo', ?)").bind(data).execute(&self.pool).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn find_unreachable(&self, _roots: Vec<Hash>) -> Result<Vec<Hash>> { Ok(Vec::new()) }
    async fn delete_objects(&self, _hashes: Vec<Hash>) -> Result<()> { Ok(()) }
}

struct SqliteTransaction {
    tx: Option<sqlx::Transaction<'static, sqlx::Sqlite>>,
}

#[async_trait]
impl Transaction for SqliteTransaction {
    async fn write_object(&mut self, hash: Hash, object: &Object) -> Result<()> {
        let data = bincode::serialize(object).map_err(|e| StorageError::Serialization(e.to_string()))?;
        sqlx::query("INSERT INTO objects (hash, type, data, size, created_at) VALUES (?, ?, ?, ?, ?)")
            .bind(hash.as_bytes()).bind(object.object_type() as i32).bind(&data).bind(data.len() as i64).bind(chrono::Utc::now().timestamp())
            .execute(&mut **self.tx.as_mut().unwrap()).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn update_ref(&mut self, name: &str, old: Option<Hash>, new: Hash) -> Result<()> {
        if let Some(old_hash) = old {
            let (current_hash_bytes,): (Vec<u8>,) = sqlx::query_as("SELECT target_hash FROM refs WHERE name = ?").bind(name).fetch_one(&mut **self.tx.as_mut().unwrap()).await.map_err(|e| StorageError::Backend(e.to_string()))?;
            let mut current_hash = [0u8; 32];
            current_hash.copy_from_slice(&current_hash_bytes);
            if Hash(current_hash) != old_hash { return Err(StorageError::RefUpdateConflict(name.to_string())); }
        }
        sqlx::query("INSERT OR REPLACE INTO refs (name, ref_type, target_hash, symbolic_target, updated_at) VALUES (?, 'direct', ?, NULL, ?)")
            .bind(name).bind(new.as_bytes()).bind(chrono::Utc::now().timestamp())
            .execute(&mut **self.tx.as_mut().unwrap()).await.map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.tx.take().unwrap().commit().await.map_err(|e| StorageError::Backend(e.to_string()))
    }

    async fn rollback(mut self: Box<Self>) -> Result<()> {
        self.tx.take().unwrap().rollback().await.map_err(|e| StorageError::Backend(e.to_string()))
    }
}
