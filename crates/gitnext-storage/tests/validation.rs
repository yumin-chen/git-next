//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//-
//! Unified validation test suite for Storage trait implementations.
//!
//! This suite is designed to run against any backend that implements the `Storage` trait.
//! It uses a macro-based approach to generate tests for different storage backends,
//! ensuring consistent behavior and compliance with storage requirements.

use gitnext_storage::{MemoryStorage, SqliteStorage, Storage, ReferenceTarget};
use gitnext_core::{Blob, GitObject, ObjectId};
use proptest::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::runtime::Runtime;

// Helper to get a Tokio runtime for async tests
fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Macro to generate a full validation test suite for a given storage backend.
///
/// This macro takes the name of the test module and an async expression to initialize
/// the storage backend. It generates a series of unit and property-based tests
/// that cover all core storage functionalities.
///
/// The tests validate:
/// - Basic CRUD operations (store, load, list)
/// - Transaction atomicity (commit, rollback)
/// - Error handling and data integrity
/// - Concurrency and isolation
/// - Behavioral consistency via property testing
///
/// Each test is explicitly mapped to the requirements it validates.
#[macro_export]
macro_rules! validation_suite {
    ($suite_name:ident, $storage_init:expr) => {
        #[cfg(test)]
        mod $suite_name {
            use super::*;
            use gitnext_storage::{Storage, ReferenceTarget};
            use gitnext_core::{Blob, GitObject, ObjectId};
            use proptest::prelude::*;
            use std::sync::Arc;

            async fn create_storage() -> Arc<dyn Storage> {
                let storage = $storage_init.await;
                Arc::new(storage)
            }

            /// Validates: 2.1, 2.2, 2.3, 10.2, 10.5
            #[tokio::test]
            async fn test_storage_basic_operations() {
                let storage = create_storage().await;

                // Test storing and loading an object
                let blob = Blob::new(bytes::Bytes::from("hello world"));
                let object = GitObject::Blob(blob);
                let id = object.canonical_hash();

                storage.store_object(&id, &object).await.unwrap();

                let loaded = storage.load_object(&id).await.unwrap();
                assert!(loaded.is_some());

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

            /// Validates: 2.1
            #[tokio::test]
            async fn test_storage_hash_validation() {
                let storage = create_storage().await;

                let blob = Blob::new(bytes::Bytes::from("test content"));
                let object = GitObject::Blob(blob);
                let correct_id = object.canonical_hash();

                // Store with correct ID should succeed
                storage.store_object(&correct_id, &object).await.unwrap();

                // Store with incorrect ID should fail
                let wrong_id = ObjectId::from_canonical_bytes(&[0u8; 32]);
                let result = storage.store_object(&wrong_id, &object).await;
                assert!(result.is_err());
            }

            /// Validates: 2.1, 10.2, 10.5
            #[tokio::test]
            async fn test_transaction_commit() {
                let storage = create_storage().await;

                let blob1 = Blob::new(bytes::Bytes::from("content 1"));
                let object1 = GitObject::Blob(blob1);
                let id1 = object1.canonical_hash();

                let blob2 = Blob::new(bytes::Bytes::from("content 2"));
                let object2 = GitObject::Blob(blob2);
                let id2 = object2.canonical_hash();

                // Start a transaction
                let mut tx = storage.transaction().await.unwrap();
                tx.store_object(&id1, &object1).await.unwrap();
                tx.store_object(&id2, &object2).await.unwrap();
                tx.update_ref("refs/heads/main", &id1).await.unwrap();

                // Before commit, objects should not be visible
                assert!(storage.load_object(&id1).await.unwrap().is_none());
                assert!(storage.load_object(&id2).await.unwrap().is_none());

                // Commit the transaction
                tx.commit().await.unwrap();

                // After commit, objects should be visible
                assert!(storage.load_object(&id1).await.unwrap().is_some());
                assert!(storage.load_object(&id2).await.unwrap().is_some());

                let refs = storage.list_refs().await.unwrap();
                assert_eq!(refs.len(), 1);
            }

            /// Validates: 2.1, 10.2, 10.5
            #[tokio::test]
            async fn test_transaction_rollback() {
                let storage = create_storage().await;

                let blob = Blob::new(bytes::Bytes::from("test content"));
                let object = GitObject::Blob(blob);
                let id = object.canonical_hash();

                // Start a transaction
                let mut tx = storage.transaction().await.unwrap();
                tx.store_object(&id, &object).await.unwrap();
                tx.update_ref("refs/heads/main", &id).await.unwrap();
                tx.rollback().await.unwrap();

                // After rollback, nothing should be visible
                assert!(storage.load_object(&id).await.unwrap().is_none());
                assert!(storage.list_refs().await.unwrap().is_empty());
            }

            /// Validates: 2.7
            #[tokio::test]
            async fn test_concurrent_transactions_do_not_interfere() {
                let storage = create_storage().await;
                let storage_arc = Arc::clone(&storage);

                let blob1 = Blob::new(bytes::Bytes::from("one"));
                let obj1 = GitObject::Blob(blob1);
                let id1 = obj1.canonical_hash();

                let blob2 = Blob::new(bytes::Bytes::from("two"));
                let obj2 = GitObject::Blob(blob2);
                let id2 = obj2.canonical_hash();

                let (tx1, tx2) = (
                    storage.transaction().await.unwrap(),
                    storage_arc.transaction().await.unwrap(),
                );

                let task1 = tokio::spawn(async move {
                    let mut tx1 = tx1;
                    tx1.store_object(&id1, &obj1).await.unwrap();
                    tx1.commit().await.unwrap();
                });

                let task2 = tokio::spawn(async move {
                    let mut tx2 = tx2;
                    tx2.store_object(&id2, &obj2).await.unwrap();
                    tx2.commit().await.unwrap();
                });

                task1.await.unwrap();
                task2.await.unwrap();

                assert!(storage.load_object(&id1).await.unwrap().is_some());
                assert!(storage.load_object(&id2).await.unwrap().is_some());
            }

            /// Validates: 2.1, 10.2, 10.5
            #[tokio::test]
            async fn test_transaction_rollback_on_error() {
                let storage = create_storage().await;

                let good_blob = Blob::new(bytes::Bytes::from("good"));
                let good_obj = GitObject::Blob(good_blob);
                let good_id = good_obj.canonical_hash();

                let bad_blob = Blob::new(bytes::Bytes::from("bad"));
                let bad_obj = GitObject::Blob(bad_blob);
                let bad_id = ObjectId::from_canonical_bytes(&[0u8; 32]);

                let mut tx = storage.transaction().await.unwrap();

                tx.store_object(&good_id, &good_obj).await.unwrap();

                let res = tx.store_object(&bad_id, &bad_obj).await;
                assert!(res.is_err());

                // The transaction should be rolled back automatically,
                // but for this test we explicitly roll it back.
                // In a real scenario, the transaction would be dropped
                // and the rollback would happen implicitly.
                let _ = tx.rollback().await;

                assert!(storage.load_object(&good_id).await.unwrap().is_none());
            }

            proptest! {
                /// Validates: 2.7
                #[test]
                fn prop_storage_backend_consistency(
                    objects in prop::collection::vec(gitnext_core::tests::arb_git_object(), 1..10)
                ) {
                    runtime().block_on(async {
                        let storage = create_storage().await;

                        for object in &objects {
                            let id = object.canonical_hash();
                            storage.store_object(&id, object).await.unwrap();
                            let loaded = storage.load_object(&id).await.unwrap();
                            prop_assert!(loaded.is_some());
                            prop_assert_eq!(loaded.unwrap().canonical_hash(), id);
                        }

                        Ok(())
                    })?;
                }

                /// Validates: 2.7, 10.2, 10.5
                #[test]
                fn prop_transaction_rollback_consistency(
                    initial_objects in prop::collection::vec(gitnext_core::tests::arb_git_object(), 0..5),
                    tx_objects in prop::collection::vec(gitnext_core::tests::arb_git_object(), 1..5)
                ) {
                    runtime().block_on(async {
                        let storage = create_storage().await;
                        let mut initial_ids = HashSet::new();

                        for obj in &initial_objects {
                            let id = obj.canonical_hash();
                            storage.store_object(&id, obj).await.unwrap();
                            initial_ids.insert(id);
                        }

                        let mut tx = storage.transaction().await.unwrap();
                        for obj in &tx_objects {
                            tx.store_object(&obj.canonical_hash(), obj).await.unwrap();
                        }
                        tx.rollback().await.unwrap();

                        for obj in &initial_objects {
                            prop_assert!(storage.load_object(&obj.canonical_hash()).await.unwrap().is_some());
                        }
                        for obj in &tx_objects {
                            let id = obj.canonical_hash();
                            if !initial_ids.contains(&id) {
                                prop_assert!(storage.load_object(&id).await.unwrap().is_none());
                            }
                        }

                        Ok(())
                    })?;
                }

                /// Validates: 2.7
                #[test]
                fn prop_mixed_transaction_commit_or_rollback(
                    objects in prop::collection::vec(gitnext_core::tests::arb_git_object(), 1..5),
                    ref_names in prop::collection::vec("[a-zA-Z0-9/_-]{1,50}", 1..3),
                    should_commit in prop::bool::ANY
                ) {
                    runtime().block_on(async {
                        let storage = create_storage().await;
                        let mut object_ids = Vec::new();

                        let mut tx = storage.transaction().await.unwrap();
                        for (i, object) in objects.iter().enumerate() {
                            let id = object.canonical_hash();
                            object_ids.push(id);
                            tx.store_object(&id, object).await.unwrap();
                            if i < ref_names.len() {
                                tx.update_ref(&ref_names[i], &id).await.unwrap();
                            }
                        }

                        if should_commit {
                            tx.commit().await.unwrap();
                        } else {
                            tx.rollback().await.unwrap();
                        }

                        for id in &object_ids {
                            let loaded = storage.load_object(id).await.unwrap();
                            if should_commit {
                                prop_assert!(loaded.is_some());
                            } else {
                                prop_assert!(loaded.is_none());
                            }
                        }

                        let refs = storage.list_refs().await.unwrap();
                        let expected_ref_count = if should_commit {
                             std::cmp::min(objects.len(), ref_names.len())
                        } else {
                            0
                        };
                        prop_assert_eq!(refs.len(), expected_ref_count);

                        Ok(())
                    })?;
                }
            }
        }
    };
}

// Generate the test suite for MemoryStorage
validation_suite!(memory_storage_tests, async {
    MemoryStorage::new()
});

// Generate the test suite for SqliteStorage
validation_suite!(sqlite_storage_tests, async {
    SqliteStorage::new_in_memory().await.unwrap()
});

/// Cross-backend consistency property tests.
///
/// These tests ensure that different storage backends behave identically
/// for the same sequence of operations. This is critical for ensuring
/// that the storage abstraction is consistent.
///
/// Validates: 2.7
#[cfg(test)]
mod cross_backend_consistency_tests {
    use super::*;
    use gitnext_core::tests::arb_git_object;

    proptest! {
        #[test]
        fn prop_memory_sqlite_consistency(
            objects in prop::collection::vec(arb_git_object(), 1..5),
            ref_names in prop::collection::vec("[a-zA-Z0-9/_-]{1,50}", 1..3)
        ) {
            runtime().block_on(async {
                let memory_storage = MemoryStorage::new();
                let sqlite_storage = SqliteStorage::new_in_memory().await.unwrap();
                let storages: Vec<Box<dyn Storage>> = vec![
                    Box::new(memory_storage),
                    Box::new(sqlite_storage),
                ];

                let mut stored_ids = Vec::new();
                for object in &objects {
                    let id = object.canonical_hash();
                    for storage in &storages {
                        storage.store_object(&id, object).await.unwrap();
                    }
                    stored_ids.push(id);
                }

                for (i, ref_name) in ref_names.iter().enumerate() {
                    if i < stored_ids.len() {
                        let target_id = stored_ids[i];
                        for storage in &storages {
                            storage.update_ref(ref_name, &target_id).await.unwrap();
                        }
                    }
                }

                // Verify that both backends have the same state
                let mem_refs = storages[0].list_refs().await.unwrap();
                let sql_refs = storages[1].list_refs().await.unwrap();
                prop_assert_eq!(mem_refs.len(), sql_refs.len());

                for id in &stored_ids {
                    let mem_obj = storages[0].load_object(id).await.unwrap();
                    let sql_obj = storages[1].load_object(id).await.unwrap();
                    prop_assert_eq!(mem_obj.is_some(), sql_obj.is_some());
                }

                Ok(())
            })?;
        }
    }
}