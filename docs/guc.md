# `pg_wasm` GUCs

Every `pg_wasm.*` GUC recognized by the extension, sourced directly from
[`pg_wasm/src/guc.rs`](../pg_wasm/src/guc.rs). Use this page as the
operational reference when configuring a cluster; see
[`docs/architecture.md`](architecture.md#7-policy-and-gucs) for the
design rationale.

## Conventions

- **Type** is the SQL-visible GUC type (`bool`, `int`, `string`, `enum`).
- **Default** matches the compile-time default registered in
  `guc.rs`.
- **Scope** is the pgrx `GucContext`, mapped to the Postgres
  [GUC context names](https://www.postgresql.org/docs/current/view-pg-settings.html):
  - `USERSET` — any connected role can change it in their session.
  - `SUSET` — superuser (or a role with `pg_read_all_settings` / the GUC
    granted) can change it at session or via `ALTER SYSTEM`.
  - `POSTMASTER` — fixed at postmaster start; requires a restart.
- **Hot / cold** answers "can this take effect on a running cluster?".
  `Hot` means `SET` or `ALTER SYSTEM SET` + `SELECT pg_reload_conf()` is
  enough. `Cold` means you must bounce Postgres (or in some cases set it
  before `shared_preload_libraries` loads `pg_wasm`).

All `pg_wasm.*` GUCs registered today are `SUSET`. They can all be changed
via `ALTER SYSTEM SET` + `pg_reload_conf()` without restarting the
cluster, but runtime effects vary — some are consulted on every
invocation, others only at module load. The **Effect** column below
captures when a change becomes visible.

## Master switch

| GUC | Type | Default | Scope | Hot / cold | Effect |
|-----|------|---------|-------|------------|--------|
| `pg_wasm.enabled` | `bool` | `on` | `SUSET` | Hot | Global kill switch. When `off`, loaded modules cannot be invoked and new loads are refused. Narrows: everything; overrides cannot re-enable. |

## Path and IO controls

| GUC | Type | Default | Scope | Hot / cold | Effect |
|-----|------|---------|-------|------------|--------|
| `pg_wasm.allow_load_from_file` | `bool` | `off` | `SUSET` | Hot | Allows the `pg_wasm.load(path text, ...)` overload to read module bytes from disk. When `off`, only the `bytea` overload is accepted. |
| `pg_wasm.module_path` | `string` | `''` | `SUSET` | Hot (load-time) | Base directory used to resolve relative paths passed to the `text` overload of `pg_wasm.load`. |
| `pg_wasm.allowed_path_prefixes` | `string` | `''` | `SUSET` | Hot (load-time) | Comma-separated list of canonical path prefixes a module file must live under. Empty means "no path load is accepted". |
| `pg_wasm.follow_symlinks` | `bool` | `off` | `SUSET` | Hot (load-time) | When `off`, canonical path resolution rejects symlink traversal for module file loads. |
| `pg_wasm.max_module_bytes` | `int` (bytes) | `33554432` (32 MiB) | `SUSET` | Hot (load-time) | Hard upper bound on the module byte length accepted by `pg_wasm.load`. Range `1 .. i32::MAX`. |

## WASI capability gates

Each capability gate narrows the master `pg_wasm.allow_wasi` toggle.
Turning a specific capability on has no effect unless `allow_wasi` is
also on. Per-module `policy` overrides can only narrow further.

| GUC | Type | Default | Scope | Hot / cold | Effect |
|-----|------|---------|-------|------------|--------|
| `pg_wasm.allow_wasi` | `bool` | `off` | `SUSET` | Hot (load-time) | Master WASI toggle. Required for any `allow_wasi_*` to have effect. |
| `pg_wasm.allow_wasi_stdio` | `bool` | `off` | `SUSET` | Hot (load-time) | Permits WASI stdout/stderr integration. |
| `pg_wasm.allow_wasi_env` | `bool` | `off` | `SUSET` | Hot (load-time) | Permits guest access to selected process environment variables via WASI. |
| `pg_wasm.allow_wasi_fs` | `bool` | `off` | `SUSET` | Hot (load-time) | Permits filesystem preopens configured by `pg_wasm.wasi_preopens`. |
| `pg_wasm.allow_wasi_net` | `bool` | `off` | `SUSET` | Hot (load-time) | Permits TCP/UDP sockets, subject to `pg_wasm.allowed_hosts`. |
| `pg_wasm.allow_wasi_http` | `bool` | `off` | `SUSET` | Hot (load-time) | Permits `wasi:http` imports through `wasmtime-wasi-http`. |
| `pg_wasm.wasi_preopens` | `string` | `''` | `SUSET` | Hot (load-time) | Comma-separated `guest=host` mappings used when FS access is enabled. |
| `pg_wasm.allowed_hosts` | `string` | `''` | `SUSET` | Hot (load-time) | Comma-separated `host:port` entries bounding outbound socket / HTTP connectivity. |

## Host capability gates

| GUC | Type | Default | Scope | Hot / cold | Effect |
|-----|------|---------|-------|------------|--------|
| `pg_wasm.allow_spi` | `bool` | `off` | `SUSET` | Hot (load-time) | Exposes the `pg_wasm:host/query` interface so a guest can issue read-only SPI queries back into the executing backend. |

## Resource limits

These GUCs are read per-invocation (except `max_instances_total`, which
is a process-wide counter and `instances_per_module`, which sizes the
backend-local pool on first use). Changes take effect on the next call
or next backend respectively.

| GUC | Type | Default | Scope | Hot / cold | Effect |
|-----|------|---------|-------|------------|--------|
| `pg_wasm.max_memory_pages` | `int` | `1024` | `SUSET` | Hot (per-call) | Maximum linear memory pages per invocation `Store` (`1024` pages ≈ 64 MiB). Enforced via `wasmtime::StoreLimits`. |
| `pg_wasm.max_instances_total` | `int` | `0` | `SUSET` | Hot (per-call) | Process-wide live instance cap. `0` means unbounded. |
| `pg_wasm.instances_per_module` | `int` | `1` | `SUSET` | Hot (next pool miss) | Backend-local instance-pool size per module. |
| `pg_wasm.fuel_enabled` | `bool` | `off` | `SUSET` | Hot (per-call) | Enables deterministic fuel budgeting. Requires `Config::consume_fuel` on the shared engine, so flipping it on / off resets the per-backend engine on next use. |
| `pg_wasm.fuel_per_invocation` | `int` | `100000000` | `SUSET` | Hot (per-call) | Fuel budget assigned to each invocation when fuel is enabled. Range `1 .. i32::MAX`. |
| `pg_wasm.invocation_deadline_ms` | `int` (ms) | `5000` | `SUSET` | Hot (per-call) | Per-invocation wall-clock cap enforced via epoch interruption. `0` disables the deadline. |
| `pg_wasm.epoch_tick_ms` | `int` (ms) | `10` | `SUSET` | Hot (next engine build) | Resolution of the epoch ticker thread that advances `wasmtime::Engine` epochs. Changes apply when the engine is rebuilt (for example, after `pg_wasm.enabled` flips off and on). Range `1 .. i32::MAX`. |

## Observability

| GUC | Type | Default | Scope | Hot / cold | Effect |
|-----|------|---------|-------|------------|--------|
| `pg_wasm.collect_metrics` | `bool` | `on` | `SUSET` | Hot (per-call) | Enables shared-memory counter increments for invocations, errors, total_ns, and peak pages. When `off`, `pg_wasm.stats()` rows stop advancing for new samples. |
| `pg_wasm.log_level` | `enum` | `notice` | `SUSET` | Hot | Minimum level used by `pg_wasm` lifecycle / runtime `RAISE` events. Accepted values: `error`, `warning`, `notice`, `info`, `log`, `debug1`..`debug5`. |

## Notes

- **All `allow_*` GUCs default to `off`.** The extension is intentionally
  useless until an administrator widens a capability. Per-module
  `options.policy` can only narrow; see
  [`docs/architecture.md`](architecture.md#72-per-module-overrides).
- **Shared-memory sizing is not GUC-controlled.** The constants
  `SHMEM_MODULE_SLOTS` and `SHMEM_EXPORT_SLOTS` live in
  `pg_wasm/src/shmem.rs`; overflow degrades to non-shared counters with
  `shared := false` in `pg_wasm.stats()`.
- **Changing a GUC never reloads existing modules.** Policy is re-read on
  the next instantiation; byte-level state (compiled artifacts, cached
  `ModuleHandle`s) is only rebuilt by `pg_wasm.reload` or a generation
  bump.
