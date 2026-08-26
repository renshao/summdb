# summdb

@README.md

## Working rules

- **Stop the server before importing.** redb is single-writer; a running `summdb-server` holds the lock and the import will fail.
- **Don't commit `.db` files.** `summdb.db` is a local scratch index, not source. (`.dbs/` is gitignored; a top-level `*.db` is not.)
- Keep README.md the single source of truth for the schema, API, and workspace layout — update it there rather than restating it here.
