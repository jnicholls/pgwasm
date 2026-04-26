# Agent instructions

This file orients automated assistants and humans working in this repository.

## Rust coding standards

Apply these rules to **Rust sources** (`*.rs`) and **Cargo manifests** (`Cargo.toml`) in this workspace.

### Edition, language, and standard library

- Prefer **Rust 2024 edition** capabilities and idioms when they match existing project style.
- Prefer features and APIs from the **latest Rust toolchain** used by the project; treat the standard library as authoritative at https://doc.rust-lang.org/std/.

### Symbol visibility

Prioritize symbol visibility in this order, from most restrictive to least restrictive:

1. Private (default). If a symbol does not need to be referenced outside of its (sub)module hierarchy, keep it private.
2. `pub(crate)`. If a symbol needs to be referenced in another module tree within the crate, scope its visibility to `pub(crate)` only.
3. `pub`. Only if a symbol must be referenced by another crate entirely—whether within this workspace or by a third-party user—scope its visibility to `pub`.

### `#[derive(...)]` attribute order

- List every `#[derive(...)]` proc-macro attribute in **strict alphabetical order** (e.g. `Clone` before `Debug` before `Eq`).

### `use` import layout

- Split `use` lines into **exactly three sections**, in this order, with **one blank line** between sections:
  1. **Standard library** (`std`, `core`, `alloc`, etc.).
  2. **External crates** (dependencies from crates.io or git).
  3. **Project internals** (`crate::...`, `super::...`, `self::...`).
- Inside each section, group imports by **top-level crate or second-level std module** and use **brace lists** `{}` when pulling multiple items from the same path.
- Inside each section, ensure listings are in strict alphabetical order. `cargo fmt` will enforce this.

✅ GOOD
```rust
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{atomic::{AtomicU64, Ordering}, Arc};

use anyhow::Context;
use serde::{de::Deserialize, ser::Serialize};

use crate::{foo::{Bar, Baz}, fud::Dud};
```

❌ BAD
```rust
use anyhow::Context;
use crate::foo::{Bar, Baz};
use crate::fud::Dud;
use serde::de::Deserialize;
use serde::ser::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
```

### `Cargo.toml` dependencies

- Keep dependency **keys** in **strict alphabetical order** within each table you touch: `[dependencies]`, `[dev-dependencies]`, `[build-dependencies]`, `[workspace.dependencies]` (workspace roots), and target-specific tables such as `[target.'cfg(...)'.dependencies]` (sort **within that table only**).
- Do not reorder unrelated keys or tables for their own sake; when adding or editing dependencies, preserve **alphabetical order** in the affected table.

```toml
# ✅ GOOD
[dependencies]
anyhow = "1"
serde = { version = "1", features = ["derive"] }
thiserror = "2"

# ❌ BAD (crate names not alphabetical)
[dependencies]
thiserror = "2"
anyhow = "1"
```

### `Option` and `Result` handling

- **Do not** call `unwrap()` on `Option` or `Result` except inside **tests** (unit tests, integration tests, `#[cfg(test)]` modules).
- **Avoid** `expect()` when a **better** pattern exists for the situation, such as propagating with `?`, enriching errors (`map_err`, `context`), or an explicit `match` / `if let` that preserves intent.

### Import depth and symbol paths

- **Types**: do not spell out long paths at every use site (for example `crate::module1::module2::MyType`). Import `MyType` (or its parent module, per local style) at the top of the file or module.
- **Functions and constants**: do not call through long paths like `crate::module1::module2::function()`. Import the **leaf module** you need (for example `use crate::module1::module2`) and call **`module2::function()`** so references stay shallow (typically **two path segments** after the import).

When in doubt, match patterns already used in neighboring modules in this repository.

### Checks and actions to perform after iterating on code

- `cargo fmt --all` to ensure all code is formatted according to our rustfmt rules.
- `cargo check --workspace` to ensure the whole workspace has valid syntax and type checks pass.
- `cargo clippy --workspace -- -D warnings` to check all lints and treat warnings as errors. Only #[allow(dead_code)] may be used on code that is intended to be used in future work. Otherwise, all warnings should be fixed, not allowed.
- `cargo test --workspace` to run all Rust host tests to ensure they pass.
- `cargo pgrx test pg17 -p pg_wasm` to run all PostgreSQL/pgrx tests and ensure they pass on at least PostgreSQL 17.

## Crate dependencies and API documentation

For **external crates**, assistants should only use APIs and behavior that match the **versions the project actually depends on**, as declared in **`Cargo.toml`** (workspace and crate manifests) and as **resolved** in practice (see `Cargo.lock`, or `cargo tree` / `cargo metadata` when versions need to be confirmed). When looking up types, traits, or functions, use documentation for **that** release — typically **https://docs.rs/** plus the crate name and a **version path segment** that matches the resolved crate version — not arbitrary newer releases.

More detail: `.cursor/rules/dependency-api-docs.mdc`.

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
