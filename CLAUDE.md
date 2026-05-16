# summdb

An embedded database for container registry metadata, with a REST API and web UI for querying and visualizing it.

Named after my daughter Summer.

## What it is

A purpose-built indexed metadata store for OCI container registries — manifests, tags, layers. Ingest from any registry that speaks the OCI Distribution Spec, then query the result locally without round-tripping to the registry for every lookup. Useful for inspecting large or complex registries (Docker Hub, ghcr, ACR, Harbor, GAR) at human latency, and for tooling that needs to ask cross-cutting questions about registry contents that the Distribution Spec API doesn't directly answer.

## Design goals

- **Performance.** Sub-millisecond point lookups, bounded scans via prefix keys, indexed back-references (layer → manifests) so common reverse queries are O(log n) instead of O(N). Compact binary on-disk encoding (postcard) to minimize page count and IO. The target is registries with millions of manifests served from a laptop-class machine.
- **Embedded.** Single binary, no external services, no daemon to manage. Open the `.db` file and you have a working index.
- **Pluggable storage.** A `StorageEngine` trait abstracts the KV layer (`get`/`put`/`delete`/`scan_prefix`/`merge`). Currently redb (pure Rust, single-writer MVCC); RocksDB or sled remain options if the workload demands it.
- **Self-contained UI.** Web view served on the same port as the API. No separate frontend build or deploy.

## What it isn't (yet)

- A registry. summdb indexes registry *metadata*; blobs still live in the source registry.
- Multi-tenant. Keys are scoped to `(repo, digest)`; no tenant separation.
- Replicated. Single-node embedded store. WAL replication is on the roadmap.

## Key schema

Single redb table, `&str` keys, `&[u8]` values, postcard-encoded structured values.

| Prefix | Key                        | Value |
|--------|----------------------------|-------|
| `T:`   | `T:{repo}:{tag}`           | digest as raw UTF-8 |
| `M:`   | `M:{repo}:{digest}`        | `ManifestRecord { repo, digest, media_type, size, platform, layers: Vec<String> }` |
| `L:`   | `L:{layer_digest}`         | `LayerRecord { size, manifests: Vec<ManifestRef> }` |

The `L:` map is an inverted index for "which manifests reference this layer." It's maintained on every manifest write via `StorageEngine::merge`, an atomic read-modify-write inside a single redb transaction, so concurrent writers sharing a layer don't lose updates.

## Workspace

```
summdb-core      types, key encoding, error types
summdb-storage   StorageEngine trait + redb implementation
summdb-server    axum REST + web UI, default port 1031
summdb-import    OCI Distribution walker — parallel bulk import with Bearer-token auth
```

## Commands

```bash
# Run the server (REST API + /ui)
cargo run -p summdb-server -- --db summdb.db

# Bulk-import from a registry (anonymous, or pass --user/--pass)
cargo run --release -p summdb-import -- \
  --registry https://ghcr.io \
  --repo homebrew/core/hello \
  --parallelism 5 \
  --db summdb.db
```

Stop the server before importing — redb is single-writer.
