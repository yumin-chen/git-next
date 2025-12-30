# GitNext

> A next-generation agent-aware Git implementation in Rust with WASM compilation, providing multi-agent orchestration while maintaining Git compatibility.

GitNext is a revolutionary Git implementation built in **pure Rust**, designed for **agent-aware operations, multi-agent orchestration, semantic conflict detection, and enterprise-grade security** while maintaining full Git compatibility.

---

## Design Principles

**GitNext** transforms Git into a multi-agent orchestration platform while remaining fully compatible with existing Git workflows, enabling sophisticated agent-driven development with enterprise-grade security and performance.

### Agent-Aware Design Principles
1. **Agent-First Architecture** – Multi-agent causality and coordination built-in
2. **Interval Tree Clocks** – O(log agents) causality tracking vs O(agents²) vector clocks
3. **Semantic Understanding** – Resource dependency graphs beyond line-level diffs
4. **Determinism Contracts** – Pre-execution validation with environment snapshots
5. **Capability-Based Security** – Fine-grained authorization with audit trails
6. **Workflow Orchestration** – Native support for complex multi-agent workflows
7. **Semantic Query Indices** – Multi-dimensional indexing for agent timeline analysis

### Core Engineering Principles
8. **Data model first, storage second** – Abstract DAG & operations
9. **Pure Rust + WASM** – Run everywhere (CLI, browser, edge, embedded)
10. **Git protocol compatibility** – Interop with GitHub/GitLab
11. **Storage agnostic** – Memory, SQLite, PostgreSQL, IndexedDB, S3
12. **Parallel & async by default** – Modern concurrency primitives
13. **Type-safe & memory-safe** – Zero UB, no crashes
14. **Queryable from day 1** – Built-in indexes, no full scans
15. **Never lose work** – Operation log, undo/redo anything

### Features

#### Agent-Aware Capabilities
- **Multi-Agent Causality**: Interval Tree Clocks for scalable agent coordination
- **Semantic Conflict Detection**: Resource dependency graphs for intelligent conflict resolution
- **Determinism Contracts**: Pre-execution validation ensuring reproducible agent operations
- **Agent Authorization**: Capability-Based Access Control with comprehensive audit trails
- **Workflow Orchestration**: Native support for complex multi-agent workflows
- **Semantic Query Indices**: Multi-dimensional indexing for agent timeline and performance analysis

#### Core Engineering Features
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

### Phase 1: Agent-Aware Foundation (Critical for MVP)
* **Interval Tree Clocks**: O(log agents) causality tracking replacing vector clocks
* **Determinism Contracts**: Pre-execution validation with environment snapshots
* **Semantic Conflict Detection**: Resource dependency graphs for intelligent merging
* **Agent Authorization**: Capability-Based Access Control with storage layer enforcement

### Phase 2: Production-Grade Security & Performance
* **Agent Capabilities**: Cryptographic tokens with fine-grained permissions
* **Audit Logging**: Immutable authorization decisions with detailed reasoning
* **Semantic Query Indices**: Multi-dimensional indexing for agent timeline analysis
* **Enhanced Storage**: Authorization-enforcing backends (SQLite, PostgreSQL)

### Phase 3: Workflow Orchestration
* **First-Class Workflows**: Workflow definitions as repository primitives
* **Workflow Execution Engine**: Storage-layer execution with dependency resolution
* **Workflow Validation**: Pre-execution DAG validation and deadlock prevention
* **Workflow State Management**: Checkpointing, rollback, and monitoring

### Phase 4: Advanced Multi-Agent Features
* **Git Protocol with Agent Extensions**: Agent metadata preservation in Git compatibility
* **Browser Agent Support**: Full agent capabilities in WASM/IndexedDB
* **Performance Optimization**: Parallel agent operations with efficient causality
* **Cross-Platform Consistency**: Identical agent behavior across all platforms

### Phase 5: Enterprise Integration
* **Advanced Storage**: PostgreSQL, S3 with authorization enforcement
* **Multi-Agent Server**: GitNext server with agent coordination
* **Comprehensive Testing**: Property-based testing with 100+ iterations
* **Production Tooling**: Monitoring, metrics, and operational dashboards

---
