# pgwasm

[![CI](https://github.com/jnicholls/pgwasm/actions/workflows/ci.yml/badge.svg)](https://github.com/jnicholls/pgwasm/actions/workflows/ci.yml)

`pgwasm` runs WebAssembly **components** inside PostgreSQL and exposes
their exports as strongly-typed SQL functions. A component's WIT world is
mapped automatically to PostgreSQL types (records, enums, variants, flags,
domains, lists), its bytes are compiled once and cached on disk, and every
invocation runs inside a [Wasmtime](https://wasmtime.dev/) sandbox whose
capabilities are configured by the database administrator.

## Table of contents

- [Why pgwasm](#why-pgwasm)
- [Quick start](#quick-start)
- [A minimal component](#a-minimal-component)
- [Calling it from SQL](#calling-it-from-sql)
- [Sandbox and policy](#sandbox-and-policy)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)

## Why pgwasm

- **Components first.** Write your UDF in any language that can target a
  [WebAssembly component](https://component-model.bytecodealliance.org/)
  and ship its WIT world. `pgwasm` turns each exported function into a
  regular `pg_proc` row with real PostgreSQL parameter and return types.
- **Typed, not buffered.** Records become composite types, enums become PG
  enums, variants and flags become composite / domain types, lists become
  arrays or `bytea`. You do not marshal JSON on either side.
- **Run once, call many.** Modules are compiled at `pgwasm.pgwasm_load` time and
  their AOT artifacts live under `$PGDATA/pgwasm/<module_id>/`. Per-call
  overhead is a pool'd component instance plus argument marshaling.
- **Strong sandbox.** WASI filesystem, sockets, HTTP, and environment
  access are all **off by default**. Administrators widen the ceiling with
  `pgwasm.*` GUCs; per-module overrides can only **narrow** that ceiling.
- **Durable, observable.** Module metadata is in catalog tables; counters
  and gauges live in shared memory; `pgwasm_modules`, `pgwasm_functions`,
  `pgwasm_stats`, and related SRFs expose both.

## Quick start

```sh
# Build and install the extension into the pgrx-managed Postgres (v13 by default).
cargo pgrx install --release

# Start the pgrx-managed cluster if you are not running your own.
cargo pgrx start
```

In your database:

```sql
CREATE EXTENSION pgwasm;

-- Optional: make sure the extension is enabled (this is the default).
SHOW pgwasm.enabled;
```

> The roles `pgwasm_loader` (may load / unload / reload / reconfigure
> modules) and `pgwasm_reader` (may read `pgwasm_stats`) are created by
> `CREATE EXTENSION`. Grant membership deliberately; loading a module is a
> privileged operation.

## A minimal component

`arith.wit`:

```wit
package example:arith;

world arith {
    export add: func(a: s32, b: s32) -> s32;
}
```

Compile your language of choice to a WebAssembly component that
implements this world (for example, `cargo component build --release`
for Rust). You should end up with `arith.component.wasm`.

## Calling it from SQL

```sql
-- Load the component from bytes (bytea overload).
SELECT pgwasm.pgwasm_load(
    wasm    => pg_read_binary_file('arith.component.wasm'),
    name    => 'arith',
    options => '{}'::jsonb
) AS module_id;

-- The loader creates one pg_proc row per WIT export, named <prefix>_<export>.
SELECT arith_add(2, 3);          --> 5

-- Inspect everything the extension knows about this module.
SELECT module_id, name, abi, generation FROM pgwasm.pgwasm_modules();
SELECT export_id, sql_name, wasm_name, signature FROM pgwasm.pgwasm_functions();

-- Tear it down when you are done.
SELECT pgwasm.pgwasm_unload(module_id) FROM pgwasm.pgwasm_modules() WHERE name = 'arith';
```

The `text` overload `pgwasm.pgwasm_load(path text, ...)` is available for file
system loads, subject to `pgwasm.allow_load_from_file`,
`pgwasm.module_path`, and `pgwasm.allowed_path_prefixes`. See
[docs/guc.md](docs/guc.md#path-and-io-controls) for the full list.

## Sandbox and policy

Every `pgwasm.*` GUC defaults to the safe option: all WASI surfaces are
off, SPI access is off, fuel is off, and the invocation deadline is 5 s.
To let a module reach the network, an administrator must set both the
master toggle (`pgwasm.allow_wasi`) and the specific capability
(`pgwasm.allow_wasi_net`, `pgwasm.allow_wasi_http`), and populate
`pgwasm.allowed_hosts`. A module author can still opt to narrow further
-- see the `policy` / `limits` keys in the `options` JSON accepted by
`pgwasm.pgwasm_load` and `pgwasm.pgwasm_reconfigure`.

See [docs/guc.md](docs/guc.md) for every GUC, default, scope, and
hot/cold reconfiguration semantics.

## Documentation

- [docs/architecture.md](docs/architecture.md) — the full v2 design:
  catalog, shared memory, runtime, trampoline, type mapping, policy,
  metrics.
- [docs/guc.md](docs/guc.md) — every `pgwasm.*` GUC with type, default,
  scope (`USERSET` / `SUSET` / `POSTMASTER`), and whether it can be
  changed live or requires a restart.
- [docs/wit-mapping.md](docs/wit-mapping.md) — the canonical WIT →
  PostgreSQL type table with WIT and SQL examples for every kind
  (primitives, composites, generics, resources).

## Development

This is a [pgrx](https://github.com/pgcentralfoundation/pgrx) extension
pinned to pgrx `0.18` with Wasmtime `43`. The repository is a Cargo
workspace; the extension crate is `pgwasm/`.

Useful commands (see [AGENTS.md](AGENTS.md) for the authoritative testing
guide):

```sh
# Fast host-only type / borrow check.
cargo check -p pgwasm

# Regress tests: deterministic SQL goldens. pgrx installs the extension.
cd pgwasm && cargo pgrx regress

# In-backend unit tests (`#[pg_test]`).
cargo pgrx test -p pgwasm
```

### Test lanes (host vs backend)

- `cargo test` is a **host-only** lane and must stay free of direct Postgres
  backend symbol dependencies.
- `cargo pgrx test -p pgwasm` is the **backend** lane for `#[pg_test]` and
  any code paths that require pgrx/Postgres runtime symbols.
- For CI, run both lanes explicitly (do not treat one as a substitute for the
  other):
  - `cargo test`
  - `cargo pgrx test pg17 -p pgwasm`

When adding or modifying Rust code, follow
[.cursor/rules/rust-coding-standards.mdc](.cursor/rules/rust-coding-standards.mdc)
(alphabetical `#[derive(...)]`, three-block `use` layout, alphabetical
Cargo dependency keys, no `unwrap()` outside tests).

## License

`pgwasm` is licensed under the terms of the [LICENSE](LICENSE) file at
the root of this repository.
