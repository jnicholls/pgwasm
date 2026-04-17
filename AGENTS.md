# Agent instructions

This file orients automated assistants and humans working in this repository.

## Rust coding standards

Apply these rules to **all Rust sources** (`*.rs`) in this workspace.

### Edition, language, and standard library

- Prefer **Rust 2024 edition** capabilities and idioms when they match existing project style.
- Prefer features and APIs from the **latest Rust toolchain** used by the project; treat the standard library as authoritative at https://doc.rust-lang.org/std/.

### `#[derive(...)]` attribute order

- List every `#[derive(...)]` proc-macro attribute in **strict alphabetical order** (e.g. `Clone` before `Debug` before `Eq`).

### `use` import layout

- Split `use` lines into **exactly three sections**, in this order, with **one blank line** between sections:
  1. **Standard library** (`std`, `core`, `alloc`, etc.).
  2. **External crates** (dependencies from crates.io or git).
  3. **Project internals** (`crate::...`, `super::...`, `self::...`).
- Inside each section, group imports by **top-level crate or module** and use **brace lists** `{}` when pulling multiple items from the same path.

### `Option` and `Result` handling

- **Do not** call `unwrap()` on `Option` or `Result` except inside **tests** (unit tests, integration tests, `#[cfg(test)]` modules).
- **Avoid** `expect()` when a **better** pattern exists for the situation, such as propagating with `?`, enriching errors (`map_err`, `context`), or an explicit `match` / `if let` that preserves intent.

### Import depth and symbol paths

- **Types**: do not spell out long paths at every use site (for example `crate::module1::module2::MyType`). Import `MyType` (or its parent module, per local style) at the top of the file or module.
- **Functions and constants**: do not call through long paths like `crate::module1::module2::function()`. Import the **leaf module** you need (for example `use crate::module1::module2`) and call **`module2::function()`** so references stay shallow (typically **two path segments** after the import).

When in doubt, match patterns already used in neighboring modules in this repository.

## Testing (`pg_wasm` / pgrx)

The extension is built with **pgrx**. Tests are organized in three layers (same idea as [ParadeDB’s testing docs](https://github.com/paradedb/paradedb/blob/main/CONTRIBUTING.md#testing)); more detail lives in `.cursor/rules/pg-wasm-pgrx-testing.mdc`.

### Layers

| Layer | Where | How to run |
|-------|--------|------------|
| **pg regress** | `pg_wasm/tests/pg_regress/` (`sql/`, `expected/`, optional `common/`) | `cargo pgrx regress` from `pg_wasm/` (pgrx installs the extension for the run) |
| **Integration** | Workspace crate `tests/` when present | `cargo test -p tests` with Postgres up, extension installed, `DATABASE_URL` set; tests use a **client** library only |
| **Unit** | `pg_wasm/src/` | **`#[pg_test]`** → `cargo pgrx test -p pg_wasm`. Plain **`#[test]`** only if the code is **pure Rust** and does not call pgrx/Postgres APIs that need a loaded backend |

Use regress for small, stable **golden** SQL output; use integration tests for heavier or non-deterministic checks; use unit tests for in-backend pgrx behavior (`#[pg_test]`) or host-safe Rust (`#[test]`).

### Host test binary vs Postgres

`cargo test` builds a **normal host executable**. It does **not** execute as a Postgres backend, so any test or `#[cfg(test)]` path that relies on **Postgres-only symbols** can fail to link or load with **unresolved symbols**.

- **`#[pg_test]`**: run with **`cargo pgrx test`**, not as a substitute for plain `cargo test` unless every compiled test path is host-safe.
- **`#[test]` in `pg_wasm`**: no pgrx calls that assume a running backend.
- **Integration crate**: depend on a Postgres **wire protocol** client; exercise the extension with SQL (`CREATE EXTENSION`, etc.). The extension is a **`cdylib`**—do not link it as a Rust dependency for routine integration tests. Avoid putting **`pgrx`** in that test binary unless you deliberately handle linking.
- **`pub mod pg_test` in `lib.rs`**: keep it minimal (pgrx-required hooks only).

For regress SQL, keep output **deterministic** (`ORDER BY`, stable data, `EXPLAIN (COSTS OFF, TIMING OFF)` when comparing plans).

### Commands (cheat sheet)

- Regress: `cargo pgrx regress` (and flags from pgrx for a single PG version or test name).
- In-Postgres units: `cargo pgrx test -p pg_wasm`.
- Integration: `cargo test -p tests` after `cargo pgrx install` / `cargo pgrx start` (or your own Postgres) and **`DATABASE_URL`** set; with pgrx-managed Postgres, port is often **`28800 + major version`** (see ParadeDB’s [`tests/README.md`](https://github.com/paradedb/paradedb/blob/main/tests/README.md)).
