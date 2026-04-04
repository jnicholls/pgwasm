# pg_wasm

**pg_wasm** is a PostgreSQL extension, written in Rust with [pgrx](https://github.com/pgcentralfoundation/pgrx), that loads **WebAssembly** modules inside the database backend and exposes WASM exports as ordinary SQL functions. Each loaded module gets a name prefix; calls are dispatched through a single native trampoline while the server tracks which WASM export to run.

## Features

- **Load WASM from SQL** — `pg_wasm_load(bytea, …)` embeds a module in the current session’s backend; optional `pg_wasm_load(text, …)` reads from disk when allowed by configuration.
- **Automatic ABI detection** — Classifies binaries as **core WASM**, **WebAssembly components**, or **Extism** plugins (via import modules), with an optional JSON override.
- **Pluggable runtimes** — Build with **Wasmtime** and/or **Extism**; the loader picks a backend based on ABI and `options.runtime` (`auto`, `wasmtime`, `extism`). Extism uses Wasmtime internally; direct Wasmtime supports components, WASI preview 1/2, and full host policy.
- **Dynamic SQL functions** — Supported exports become `schema.prefix_exportname(...)` without hand-written `CREATE FUNCTION` per export; strict C-language functions share one trampoline symbol.
- **Scalar and buffer APIs** — Integer, boolean, and float scalars; `text`, `bytea`, and `jsonb` via explicit `exports` hints in load options.
- **Optional WASI** — Modules that import WASI can be loaded when global and per-module policy allows it.
- **Lifecycle hooks** — Optional `on_load`, `on_unload`, and `on_reconfigure` WASM exports driven from load/reconfigure/unload.
- **Introspection** — Table functions `pg_wasm_modules()`, `pg_wasm_functions()`, and `pg_wasm_stats()` for catalog and per-backend metrics.
- **Operational controls** — `pg_wasm.*` GUCs for module size, paths, WASI capabilities, fuel, memory limits, and metrics collection.

For a deeper walkthrough of modules and data flow, see [docs/architecture.md](docs/architecture.md).

## Requirements

- **Rust** toolchain (edition 2024; see workspace `Cargo.toml`).
- **PostgreSQL** installation with development headers (`pg_config` on your `PATH`, or pass `--pg-config` to pgrx).
- **[cargo-pgrx](https://crates.io/crates/cargo-pgrx)** compatible with the pgrx version pinned in this repo (see `[workspace.dependencies]` in the root `Cargo.toml`).

At least one runtime feature must stay enabled: `runtime-wasmtime` and/or `runtime-extism` (defaults include both Wasmtime and Extism).

## Getting started

### 1. Install and initialize cargo-pgrx

```bash
cargo install cargo-pgrx --locked
cargo pgrx init
```

`cargo pgrx init` records where PostgreSQL is installed; re-run or edit config if you switch versions.

### 2. Choose a PostgreSQL major version feature

The `pg_wasm` crate maps each PostgreSQL major to a Cargo feature (`pg13` … `pg18`). The default in `pg_wasm/Cargo.toml` is `pg13`. To target another version, build with default features disabled and your version enabled, for example:

```bash
cd pg_wasm
cargo build --no-default-features --features "pg17,runtime-wasmtime"
```

Add `runtime-extism` if you want Extism-backed loads (e.g. `abi: extism` or modules with `extism:host/*` imports).

### 3. Build and install the extension

From the `pg_wasm` crate directory:

```bash
cd pg_wasm
cargo pgrx install --release
```

If `pg_config` is not on your `PATH`:

```bash
cargo pgrx install --release --pg-config /path/to/pg_config
```

### 4. Enable in the database

Connect as a superuser (the extension is marked superuser-only in its control file) and run:

```sql
CREATE EXTENSION pg_wasm;
```

Extension objects live in the schema created for the extension (often `pg_wasm`; use `SELECT extnamespace::regnamespace FROM pg_extension WHERE extname = 'pg_wasm'` if unsure).

## Usage examples

Assume your extension schema is `pg_wasm` and you have a small WASM binary that exports `add(integer, integer) -> integer` and `forty_two() -> integer` in the shapes this extension expects for auto-registered scalars.

### Load from `bytea` and call generated functions

```sql
-- Replace <hex> with the lowercase hex encoding of your .wasm file.
SELECT pg_wasm.pg_wasm_load(
  decode('<hex>', 'hex')::bytea,
  'demo'::text
) AS module_id;

-- Functions are named <prefix>_<wasm_export_name>
SELECT pg_wasm.demo_add(2, 3);
SELECT pg_wasm.demo_forty_two();

SELECT pg_wasm.pg_wasm_unload(<module_id from above>);
```

`pg_wasm_load` requires a **superuser** session.

### Load options: `text` / `bytea` / `jsonb` exports

When WASM uses memory-style I/O, describe signatures in JSON:

```sql
SELECT pg_wasm.pg_wasm_load(
  decode('<hex>', 'hex')::bytea,
  'buf'::text,
  '{
    "exports": {
      "echo_mem": {
        "args": ["bytea"],
        "returns": "bytea"
      }
    }
  }'::jsonb
);

SELECT pg_wasm.buf_echo_mem('\xdeadbeef'::bytea);
```

### Load from a filesystem path

Path loads are **off** by default. Set a base directory, allow file loads, then use the `text` overload:

```sql
SET pg_wasm.module_path = '/absolute/path/to/modules';
SET pg_wasm.allow_load_from_file = on;

SELECT pg_wasm.pg_wasm_load('my_module.wasm'::text, 'fsdemo'::text);
```

Optional comma-separated `pg_wasm.allowed_path_prefixes` restricts which directories are acceptable when you use that mode.

### Introspection

```sql
SELECT * FROM pg_wasm.pg_wasm_modules();
SELECT * FROM pg_wasm.pg_wasm_functions();
SELECT * FROM pg_wasm.pg_wasm_stats();
```

Stats reflect the **current backend process** only.

### Reconfigure a loaded module

```sql
SELECT pg_wasm.pg_wasm_reconfigure_module(<module_id>, '{"allow_env": false}'::jsonb);
```

## Development

Run the extension’s pgrx test suite from the `pg_wasm` directory (with the same PostgreSQL feature you use for development):

```bash
cd pg_wasm
cargo pgrx test pg17
```

Adjust `pg17` to match your `cargo pgrx init` / feature set.

## License

This project is licensed under the **BSD 3-Clause License**. See [LICENSE](LICENSE) for the full text.
