# summdb

A purpose-built embedded database for container registry metadata.
Named after my daughter Summer.

## What it is
Fast indexed metadata store for OCI container registries — manifests, 
tags, layers, referrers. Designed to replace CosmosDB-backed metadata 
services (e.g. Azure Container Registry) with a low-latency embedded store.

## Key design decisions
- Storage abstraction trait (get/put/delete/scan) — pluggable backends
- Start with redb (pure Rust, no C++ deps), RocksDB backend later
- Single node first, WAL replication in future
- Key schema:
    M:{tenant}:{repo}:{digest}     → manifest metadata (mediaType, size, arch, os, created)
    T:{tenant}:{repo}:{tag}        → digest string
    L:{digest}                     → layer metadata (size, compressed_size)
- Blob storage is source of truth for manifest content
- RocksDB/redb owns the index only
- Batch writes (10k per txn) for bulk load, insert in key order

## Workspace structure
summdb-core      → key schema, types, errors
summdb-storage   → StorageEngine trait + redb impl
summdb-engine    → query logic, pagination
summdb-server    → gRPC or HTTP, OCI distribution spec API