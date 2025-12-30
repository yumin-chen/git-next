# GitNext

> A modern Git reimagination in Rust with WASM compilation, keeping Git compatibility while fixing fundamental issues.

GitNext is a next-generation Git implementation built in **pure Rust**, designed for **safety, performance, flexibility**, and **browser/embedded compatibility**.

---

## Design Principles

**GitNext** fixes fundamental Git limitations while remaining fully compatible with GitHub/GitLab workflows, enabling browser-based, embedded, and cloud-native Git operations.

1. **Data model first, storage second** – Abstract DAG & operations
2. **Pure Rust + WASM** – Run everywhere (CLI, browser, edge, embedded)
3. **Git protocol compatibility** – Interop with GitHub/GitLab
4. **Storage agnostic** – Memory, SQLite, PostgreSQL, IndexedDB, S3
5. **Parallel & async by default** – Modern concurrency primitives
6. **Type-safe & memory-safe** – Zero UB, no crashes
7. **Queryable from day 1** – Built-in indexes, no full scans
8. **Never lose work** – Operation log, undo/redo anything

### Features

- **Safe**: Rust, memory-safe, zero UB
- **Fast**: Parallel, async, indexed operations
- **Flexible**: Pluggable storage backends, WASM support
- **Compatible**: Works with existing Git workflows
- **Recoverable**: Never lose work, full operation log

---

## 2. Rust Workspace & Crate Organization

GitNext is structured as a **multi-crate workspace**:

```
gitnext/                  # Workspace root
├─ crates/                      # All library crates
│  ├─ gitnext-core/             # Core data model
│  ├─ gitnext-storage/          # Storage abstraction traits
│  ├─ gitnext-storage-memory/   # In-memory backend
│  ├─ gitnext-storage-sqlite/   # SQLite backend
│  ├─ gitnext-storage-indexeddb/# Browser backend
│  ├─ gitnext-storage-postgres/ # PostgreSQL backend
│  ├─ gitnext-storage-s3/       # Cloud backend
│  ├─ gitnext-objects/          # Object model (commits, trees, blobs)
│  ├─ gitnext-operations/       # High-level repository operations
│  ├─ gitnext-merge/            # Merge algorithms
│  ├─ gitnext-query/            # Query layer
│  ├─ gitnext-identity/         # Artifact tracking
│  ├─ gitnext-protocol/         # Git wire protocol
│  ├─ gitnext-compat/           # Git compatibility layer
│  ├─ gitnext-cli/              # CLI tool
│  ├─ gitnext-wasm/             # WASM bindings
│  └─ gitnext-server/           # Git server
├─ examples/                     # Example apps & usage demos
├─ docs/                         # Architecture docs, design specs, diagrams
├─ tests/                        # Integration and regression tests
├─ benchmarks/                   # Performance benchmarks
├─ scripts/                      # Build/deployment/CI scripts
├─ Cargo.toml                    # Workspace definition & dependencies
└─ ReadMe.md                     # This document
```

### Notes

- **`crates/`**: All core functionality, backends, operations, and bindings are organized into separate crates.
- **`examples/`**: Showcases how to use multiple crates together, including CLI and WASM usage.
- **`docs/`**: Architecture diagrams, TSDs, ADRs, and semantic specifications.
- **`tests/`**: End-to-end and cross-backend integration tests.
- **`benchmarks/`**: Performance benchmarks for repository operations, query, and clone/push/pull.
- **`scripts/`**: Utility scripts for building, releasing, or testing across multiple platforms.

---

## Crates Overview

| Crate                         | Responsibility          | Notes                                                          |
| ----------------------------- | ----------------------- | -------------------------------------------------------------- |
| **gitnext-core**              | Core DAG & data model   | Immutable commit graph, trees, blobs, tags                     |
| **gitnext-objects**           | Object model            | Low-level objects and content-addressed storage representation |
| **gitnext-operations**        | Repository operations   | High-level operations: commit, branch, undo/redo               |
| **gitnext-merge**             | Merge algorithms        | Three-way merges, semantic merge strategies                    |
| **gitnext-query**             | Query layer             | Indexed queries, filters, ancestry checks                      |
| **gitnext-identity**          | Artifact tracking       | Stable IDs for commits, trees, and objects                     |
| **gitnext-storage**           | Storage abstraction     | Unified trait for backends                                     |
| **gitnext-storage-memory**    | In-memory backend       | Fast, ephemeral storage for testing or caching                 |
| **gitnext-storage-sqlite**    | SQLite backend          | Async, transactional, local storage                            |
| **gitnext-storage-indexeddb** | Browser backend         | WASM-compatible persistent storage                             |
| **gitnext-storage-postgres**  | PostgreSQL backend      | Server-side persistent storage                                 |
| **gitnext-storage-s3**        | Cloud backend           | Remote object storage                                          |
| **gitnext-protocol**          | Git wire protocol       | Push/fetch/packfile handling                                   |
| **gitnext-compat**            | Git compatibility layer | Maps GitNext objects to standard Git objects                   |
| **gitnext-cli**               | CLI tool                | User-facing command-line interface                             |
| **gitnext-wasm**              | WASM bindings           | Browser & edge runtime support                                 |
| **gitnext-server**            | Git server              | Multi-user server implementation                               |


---

## Architecture Layers

```
┌─────────────────────────────────────┐
│ CLI / WASM / Server                 │  User interfaces
├─────────────────────────────────────┤
│ Git Protocol Compatibility Layer     │  Push/pull/fetch translation
├─────────────────────────────────────┤
│ Repository Operations                │  High-level operations (commit, merge, query)
├─────────────────────────────────────┤
│ Query / Identity / Merge             │  Indexed ancestry, artifact tracking
├─────────────────────────────────────┤
│ Core Data Model                      │  Commits, trees, blobs, tags
├─────────────────────────────────────┤
│ Storage Abstraction                  │  Backend trait (Storage)
├─────────────────────────────────────┤
│ Backends: SQLite│Postgres│S3│Memory  │  Concrete implementations
└─────────────────────────────────────┘
```

---

## CLI Example

```bash
# Install
cargo install gitnext-cli

# Initialize repo
gitnext init my-project
cd my-project

# Make a commit
echo "hello" > README.md
gitnext commit -m "Initial commit"

# Query commits
gitnext log -n 10
gitnext query --since $(date -d '1 week ago' +%s)

# Undo last operation
gitnext undo

# Clone from Git
gitnext clone https://github.com/user/repo.git
```

---

## WASM Example

```javascript
import { Repository } from "gitnext-wasm";

const repo = await Repository.open("browser_repo");
await repo.commit("Browser commit");
```

* Uses **IndexedDB** as backend
* Fully async and parallel

---

## Roadmap

**Phase 1: MVP**

* Core data model, storage trait, SQLite & memory backend, repository ops, CLI basics

**Phase 2: Performance**

* Indexed commit graph, generation numbers, parallel ops, benchmarks

**Phase 3: WASM**

* IndexedDB backend, browser demo, service worker sync

**Phase 4: Git Compatibility**

* Git object format, protocol handlers, clone/push/pull, test GitHub/GitLab

**Phase 5: Advanced Features**

* Artifact tracking, semantic merge, query DSL, operation log UI

**Phase 6: Production**

* PostgreSQL & S3 backend, server implementation, multi-user support, security audit

---
