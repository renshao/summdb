# summdb

An embedded database for container registry metadata, with a REST API and web UI for querying and visualizing it.

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

## Quick start

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

The server listens on port 1031 by default; the web UI is at `/ui`.

Stop the server before importing — redb is single-writer.

## API

| Method | Path |
|--------|------|
| `GET` | `/v1/repos` |
| `GET` | `/v1/repos/:repo/stats` |
| `GET` | `/v1/repos/:repo/layers` |
| `GET` `PUT` | `/v1/repos/:repo/tags/:tag` |
| `GET` | `/v1/repos/:repo/tags` |
| `GET` `PUT` | `/v1/repos/:repo/manifests/:digest` |
| `GET` | `/v1/repos/:repo/manifests/:digest/body` |
| `GET` | `/v1/repos/:repo/manifests` |
| `GET` | `/v1/layers/:digest` |
| `GET` | `/v1/layers/:digest/manifests` |

## Key schema

Single redb table (`data`), binary `&[u8]` keys, `&[u8]` values, postcard-encoded structured values.

Repo names are interned to a `u32` (`RepoId`) so they don't repeat in every key; digests are stored as the raw 32 sha256 bytes, not hex. Keys are built by `summdb-core::keys`:

| Prefix | Key layout                              | Value |
|--------|-----------------------------------------|-------|
| `T`    | `T` + repo_id(BE u32) + tag bytes       | digest as raw 32 bytes |
| `M`    | `M` + repo_id(BE u32) + digest(32B)     | `ManifestRecord { repo, digest, media_type, total_layer_size, platform, layers, children, parent, tags }` |
| `B`    | `B` + repo_id(BE u32) + digest(32B)     | raw manifest JSON, zstd-compressed |
| `L`    | `L` + digest(32B)                       | `LayerRecord { size, manifests: Vec<ManifestRef> }` |
| `RI`   | `RI` + repo name bytes                  | repo id (BE u32) — interner forward map |
| `IR`   | `IR` + repo_id(BE u32)                  | repo name — interner reverse map |

Because redb orders `&[u8]` keys lexicographically, prefix scans come back sorted and the endpoints return that order unchanged: `/v1/repos/:repo/manifests` is ascending by digest (raw-byte order, which is the same order as sorting the lowercase hex), and `/v1/repos/:repo/tags` is ascending by tag name.

The `L` map is an inverted index for "which manifests reference this layer." It's maintained on every manifest write via `StorageEngine::merge`, an atomic read-modify-write inside a single redb transaction, so concurrent writers sharing a layer don't lose updates. The same merge path keeps each manifest's `tags` list in sync when a tag is repointed (`summdb_storage::ops::set_tag`).

## Workspace

```
summdb-core      types, key encoding, error types
summdb-storage   StorageEngine trait + redb implementation, repo interner, tag ops
summdb-server    axum REST + web UI, default port 1031
summdb-import    OCI Distribution walker — parallel bulk import with Bearer-token auth
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
