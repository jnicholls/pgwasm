# AGENTS.md

## Cursor Cloud specific instructions

### Overview

**pg_wasm** is a PostgreSQL extension (Rust + pgrx) that loads WebAssembly modules inside the database backend and exposes WASM exports as SQL functions. There is one main crate: `pg_wasm/` (the extension).

### Toolchain requirements

| Tool | Version | Purpose |
|------|---------|---------|
| Rust | >= 1.85 stable (edition 2024) | Compile the extension |
| PostgreSQL 17 | with `-dev` headers, `pg_config` on `PATH` | Target database |
| `cargo-pgrx` | `=0.17.0` (must match pinned pgrx) | Build/test/install the extension |
| `wasm-tools` | latest | CLI for WASM manipulation |
| `lld` | system package | Linker backend (needed by Rust >= 1.86 on Linux) |
| C/C++ compilers | gcc/g++ | Build dependencies, stubs |

### Linker configuration (Rust >= 1.86 on Linux)

Rust >= 1.86 defaults to `rust-lld` on `x86_64-linux-gnu`, which rejects unresolved PostgreSQL symbols in the pgrx test binary. The repo's `.cargo/config.toml` includes a `[target.x86_64-unknown-linux-gnu]` section that passes `--unresolved-symbols=ignore-all` to the linker. The `lld` system package **must** be installed for this to work.

### PG extension directory permissions

`cargo pgrx install` and `cargo pgrx test` need write access to PostgreSQL's extension directory. Run once during setup:

```bash
sudo chmod -R a+w /usr/share/postgresql/17/extension/ /usr/lib/postgresql/17/lib/
```

### Common commands

All commands run from the `pg_wasm/` directory and target PG 17 with both runtimes enabled:

| Action | Command |
|--------|---------|
| Build | `cargo build --no-default-features --features "pg17,runtime-wasmtime,runtime-extism"` |
| Lint | `cargo clippy --no-default-features --features "pg17,runtime-wasmtime,runtime-extism"` |
| Test | `cargo pgrx test pg17 --no-default-features --features "pg17" --features "runtime-wasmtime" --features "runtime-extism"` |
| Run (interactive psql) | `cargo pgrx run pg17 --no-default-features --features "pg17 runtime-wasmtime runtime-extism"` |
| Install | `cargo pgrx install --pg-config /usr/bin/pg_config --no-default-features --features "pg17 runtime-wasmtime runtime-extism"` |

### Gotchas

- **Feature passing to `cargo pgrx test`**: Features must be passed as separate `--features` flags (space-separated values), not comma-separated. Comma-separated values are treated as a single feature name by pgrx and silently ignored.
- **Test mutex cascade**: If the first pgrx pg_test fails (e.g. due to permissions or install failure), all remaining pg_tests will fail with "Could not obtain test mutex" — fix the root cause and re-run.
- **WASM modules are per-backend**: Loaded modules live in the PostgreSQL backend process memory. A `psql` session that loads a module can call its functions, but a separate `psql` session cannot (it will see the `pg_proc` rows but get "no wasm dispatch entry" errors).
- **pgrx init**: `cargo pgrx init --pg17 $(which pg_config)` only needs to run once per PG version. Re-run if PostgreSQL is reinstalled.
