# pg_wasm

`pg_wasm` runs WebAssembly **components** inside PostgreSQL and exposes
their exports as strongly-typed SQL functions. A component's WIT world is
mapped automatically to PostgreSQL types (records, enums, variants, flags,
domains, lists), its bytes are compiled once and cached on disk, and every
invocation runs inside a [Wasmtime](https://wasmtime.dev/) sandbox whose
capabilities are configured by the database administrator.

> Status: this is the **v2** rewrite. The
> [implementation plan](.cursor/plans/pg_wasm_extension_implementation_v2.plan.md)
> tracks what ships; all listed v2 implementation tasks are complete, with
> the build-feature split closed intentionally without implementation.

## Table of contents

- [Why pg_wasm](#why-pg_wasm)
- [Quick start](#quick-start)
- [A minimal component](#a-minimal-component)
- [Calling it from SQL](#calling-it-from-sql)
- [Sandbox and policy](#sandbox-and-policy)
- [Documentation](#documentation)
- [Development](#development)
- [License](#license)

## Why pg_wasm

- **Components first.** Write your UDF in any language that can target a
  [WebAssembly component](https://component-model.bytecodealliance.org/)
  and ship its WIT world. `pg_wasm` turns each exported function into a
  regular `pg_proc` row with real PostgreSQL parameter and return types.
- **Typed, not buffered.** Records become composite types, enums become PG
  enums, variants and flags become composite / domain types, lists become
  arrays or `bytea`. You do not marshal JSON on either side.
- **Run once, call many.** Modules are compiled at `wasm.load` time and
  their AOT artifacts live under `$PGDATA/pg_wasm/<module_id>/`. Per-call
  overhead is a pool'd component instance plus argument marshaling.
- **Strong sandbox.** WASI filesystem, sockets, HTTP, and environment
  access are all **off by default**. Administrators widen the ceiling with
  `pg_wasm.*` GUCs; per-module overrides can only **narrow** that ceiling.
- **Durable, observable.** Module metadata is in catalog tables; counters
  and gauges live in shared memory; `wasm.modules`, `wasm.functions`,
  `wasm.stats` and friends expose both as SRF
  views.

## Quick start

```sh
# Build and install the extension into the pgrx-managed Postgres (v13 by default).
cargo pgrx install --release

# Start the pgrx-managed cluster if you are not running your own.
cargo pgrx start
```

In your database:

```sql
CREATE EXTENSION pg_wasm;

-- Optional: make sure the extension is enabled (this is the default).
SHOW pg_wasm.enabled;
```

> The roles `wasm_loader` (may load / unload / reload / reconfigure
> modules) and `wasm_reader` (may read `wasm.stats`) are created by
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
SELECT wasm.load(
    wasm    => pg_read_binary_file('arith.component.wasm'),
    name    => 'arith',
    options => '{}'::jsonb
) AS module_id;

-- The loader creates one pg_proc row per WIT export, named <prefix>_<export>.
SELECT arith_add(2, 3);          --> 5

-- Inspect everything the extension knows about this module.
SELECT module_id, name, abi, generation FROM wasm.modules();
SELECT export_id, sql_name, wasm_name, signature FROM wasm.functions();

-- Tear it down when you are done.
SELECT wasm.unload(module_id) FROM wasm.modules() WHERE name = 'arith';
```

The `text` overload `wasm.load(path text, ...)` is available for file
system loads, subject to `pg_wasm.allow_load_from_file`,
`pg_wasm.module_path`, and `pg_wasm.allowed_path_prefixes`. See
[docs/guc.md](docs/guc.md#path-and-io-controls) for the full list.

## Sandbox and policy

Every `pg_wasm.*` GUC defaults to the safe option: all WASI surfaces are
off, SPI access is off, fuel is off, and the invocation deadline is 5 s.
To let a module reach the network, an administrator must set both the
master toggle (`pg_wasm.allow_wasi`) and the specific capability
(`pg_wasm.allow_wasi_net`, `pg_wasm.allow_wasi_http`), and populate
`pg_wasm.allowed_hosts`. A module author can still opt to narrow further
-- see the `policy` / `limits` keys in the `options` JSON accepted by
`wasm.load` and `wasm.reconfigure`.

See [docs/guc.md](docs/guc.md) for every GUC, default, scope, and
hot/cold reconfiguration semantics.

## Documentation

- [docs/architecture.md](docs/architecture.md) — the full v2 design:
  catalog, shared memory, runtime, trampoline, type mapping, policy,
  metrics.
- [docs/guc.md](docs/guc.md) — every `pg_wasm.*` GUC with type, default,
  scope (`USERSET` / `SUSET` / `POSTMASTER`), and whether it can be
  changed live or requires a restart.
- [docs/wit-mapping.md](docs/wit-mapping.md) — the canonical WIT →
  PostgreSQL type table with WIT and SQL examples for every kind
  (primitives, composites, generics, resources).
- [.cursor/plans/pg_wasm_extension_implementation_v2.plan.md](.cursor/plans/pg_wasm_extension_implementation_v2.plan.md)
  — authoritative status of every v2 todo.

## Development

This is a [pgrx](https://github.com/pgcentralfoundation/pgrx) extension
pinned to pgrx `0.18` with Wasmtime `43`. The repository is a Cargo
workspace; the extension crate is `pg_wasm/`.

Useful commands (see [AGENTS.md](AGENTS.md) for the authoritative testing
guide):

```sh
# Fast host-only type / borrow check.
cargo check -p pg_wasm

# Regress tests: deterministic SQL goldens. pgrx installs the extension.
cargo pgrx regress

# In-backend unit tests (`#[pg_test]`).
cargo pgrx test -p pg_wasm
```

### Test lanes (host vs backend)

- `cargo test` is a **host-only** lane and must stay free of direct Postgres
  backend symbol dependencies.
- `cargo pgrx test -p pg_wasm` is the **backend** lane for `#[pg_test]` and
  any code paths that require pgrx/Postgres runtime symbols.
- For CI, run both lanes explicitly (do not treat one as a substitute for the
  other):
  - `cargo test`
  - `cargo pgrx test pg17 -p pg_wasm`

When adding or modifying Rust code, follow
[.cursor/rules/rust-coding-standards.mdc](.cursor/rules/rust-coding-standards.mdc)
(alphabetical `#[derive(...)]`, three-block `use` layout, alphabetical
Cargo dependency keys, no `unwrap()` outside tests).

## License

`pg_wasm` is licensed under the terms of the [LICENSE](LICENSE) file at
the root of this repository.
