# `tests` — workspace integration crate

This crate exercises `pg_wasm` through **SQL only** using [`tokio-postgres`](https://docs.rs/tokio-postgres/) (no `pgrx` in the test binary).

## Layout

Integration cases live as `#[tokio::test]` modules under `tests/src/`. Shared helpers are in `tests/src/common/mod.rs`.

Wasm fixtures are built in `build.rs`:

- `fixtures/core.wat` + `fixtures/itest.wit` → `itest.component.wasm` (add / spin / spin-param / grow).
- `http_search_guest` (`tests/http_search_guest`, WASIp2 + `wasi:http`) → `http_search.component.wasm` (export `search-titles` → HTTPS GET Hacker News Algolia JSON).

## Prerequisites

- [`wasm-tools`](https://github.com/bytecodealliance/wasm-tools) on `PATH` (workspace uses the same major line as `wit-component` / `wasmtime`).
- **`wasm32-wasip2` Rust target** (required to compile `http_search_guest` during `tests` builds): `rustup target add wasm32-wasip2`.
- PostgreSQL with the `pg_wasm` extension available (typically `cargo pgrx install` / `cargo pgrx start` from the workspace root).

## Running

Default `cargo test -p tests` only compiles the ignored integration tests (no database required).

The suite uses a dedicated database named `pgwasm_itest` (created from `template0` on first connect).
If a prior run left that database in a bad state, drop it manually from the maintenance DB:

```bash
psql "$DATABASE_URL" -c "DROP DATABASE IF EXISTS pgwasm_itest WITH (FORCE);"
```

With pgrx-managed Postgres, the client port is **`28800 + PostgreSQL major version`** (for example **28813** for PG13, **28817** for PG17).

```bash
# From workspace root: install the extension for your pgrx PG version (include pg_test if you
# built with --features pg17 pg_test), start Postgres, then run ignored tests on a single thread
# (tests recreate the `pgwasm_itest` database):
cd pg_wasm && cargo pgrx install --features "pg17 pg_test" --no-default-features && cd ..
cargo pgrx start || true
export DATABASE_URL="postgres://localhost:$((28800 + 17))/pgwasm_itest"
cargo test -p tests -- --ignored --test-threads=1
```

CI-style one-liner (install extension, start if needed, run ignored tests):

```bash
cd /path/to/pg_wasm/repo
cargo pgrx install --features "pg17 pg_test" --no-default-features && cargo pgrx start || true
export DATABASE_URL="postgres://localhost:$((28800 + 17))/pgwasm_itest"
cargo test -p tests -- --ignored --test-threads=1
```

Adjust the `17` in the port calculation to match the PostgreSQL major you start with `cargo pgrx run` / `cargo pgrx start`.

## WASI HTTP integration tests (outbound network)

`wasi_http_search` proves `wasi:http` + outbound TLS against `https://hn.algolia.com/api/v1/search?query=postgresql` (JSON `hits[].title`). These tests stay **`#[ignore]`** and are further gated by **`RUN_WASI_HTTP_ITEST=1`** so default `cargo test` / CI do not hit the public internet.

```bash
export DATABASE_URL="postgres://localhost:$((28800 + 17))/pgwasm_itest"
RUN_WASI_HTTP_ITEST=1 cargo test -p tests wasi_http -- --ignored --test-threads=1
```
