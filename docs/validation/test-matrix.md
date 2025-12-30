# Test Matrix

| Test Case | Storage Backend | Requirements Validated | Transaction Behavior | Expected Output vs. Observed Output |
| --- | --- | --- | --- | --- |
| `test_storage_basic_operations` | Memory, SQLite | 2.1, 2.2, 2.3, 10.2, 10.5 | N/A (Direct Operations) | Expected: Objects stored, loaded, and references updated. Observed: Pass. |
| `test_storage_hash_validation` | Memory, SQLite | 2.1 | N/A (Direct Operations) | Expected: Storing with mismatched hash fails. Observed: Pass. |
| `test_transaction_commit` | Memory, SQLite | 2.1, 10.2, 10.5 | Commit | Expected: Changes are visible after commit. Observed: Pass. |
| `test_transaction_rollback` | Memory, SQLite | 2.1, 10.2, 10.5 | Rollback | Expected: Changes are not visible after rollback. Observed: Pass. |
| `test_concurrent_transactions_do_not_interfere` | Memory, SQLite | 2.7 | Concurrent Commits | Expected: Both transactions commit without data loss. Observed: Pass. |
| `test_transaction_rollback_on_error` | Memory, SQLite | 2.1, 10.2, 10.5 | Rollback on Error | Expected: Transaction is rolled back when an error occurs. Observed: Pass. |
| `prop_storage_backend_consistency` | Memory, SQLite | 2.7 | N/A (Direct Operations) | Expected: Storage remains consistent after random operations. Observed: Pass. |
| `prop_transaction_rollback_consistency` | Memory, SQLite | 2.7, 10.2, 10.5 | Rollback | Expected: Storage state is unchanged after a rolled-back transaction. Observed: Pass. |
| `prop_mixed_transaction_commit_or_rollback` | Memory, SQLite | 2.7 | Commit or Rollback | Expected: Storage state is consistent after a random sequence of operations followed by a commit or rollback. Observed: Pass. |
| `prop_memory_sqlite_consistency` | Memory & SQLite | 2.7 | N/A (Direct Operations) | Expected: Both backends have identical state after the same sequence of operations. Observed: Pass. |
