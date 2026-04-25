---
name: pg_wasm Extension Implementation (v2)
overview: |
  Implement a pgrx-based PostgreSQL extension, `pg_wasm`, that binds WebAssembly
  modules and components to SQL-visible functions. v2 centers on the WebAssembly
  Component Model (WIT) with Wasmtime as the single runtime, auto-registers
  user-defined WIT types as PostgreSQL composite types, enums, and domains,
  enforces layered sandbox policy through GUCs and per-module overrides, and
  supports full lifecycle (load, unload, reload, reconfigure) with durable
  catalog tables and on-disk compiled artifacts. Design reference:
  `docs/architecture.md`.
todos:
  - id: bootstrap-layout
    content: |
      Restructure `pg_wasm/src/` into the v2 module layout (guc, errors, catalog,
      artifacts, shmem, registry, config, policy, abi, wit, runtime, mapping,
      proc_reg, trampoline, lifecycle, hooks, views). Pin workspace deps:
      `wasmtime = "43"` (with `component-model` feature; `cranelift` default),
      `wasmtime-wasi = "43"`, `wasmtime-wasi-http = "43"`,
      `wit-component = "0.247"`, `wit-parser = "0.247"`, `wasmparser = "0.247"`,
      plus `serde_json`, `thiserror`, `anyhow`, `sha2`. Keep `hello_pg_wasm`
      temporarily as a smoke test.
    status: completed
  - id: errors-and-guc
    content: Implement `errors::PgWasmError` with SQLSTATE mapping and define every `pg_wasm.*` GUC in `guc.rs` (enabled, allow_load_from_file, module_path, allowed_path_prefixes, max_module_bytes, allow_wasi, allow_wasi_{stdio,env,fs,net,http}, wasi_preopens, allowed_hosts, allow_spi, max_memory_pages, max_instances_total, instances_per_module, fuel_enabled, fuel_per_invocation, invocation_deadline_ms, epoch_tick_ms, collect_metrics, log_level, follow_symlinks). Register them in `_PG_init`.
    status: completed
  - id: catalog-schema
    content: Add `pg_wasm.modules`, `pg_wasm.exports`, `pg_wasm.wit_types`, `pg_wasm.dependencies` tables in versioned SQL. Implement `catalog::{modules,exports,wit_types}` CRUD via SPI. Set up `pg_wasm_loader` and `pg_wasm_reader` roles with minimal grants. Add `catalog::migrations` that validates shape on `_PG_init`.
    status: completed
  - id: shmem-and-generation
    content: Implement `shmem.rs` with a per-cluster segment sized by fixed compile-time constants (module slots and export slots). Provide `bump_generation(module_id)`, `read_generation()`, and atomic per-export counters. Protect mutators with `pg_wasm.CatalogLock` (LWLock). Wire into `shmem_request_hook` and `shmem_startup_hook`.
    status: completed
  - id: artifacts-layout
    content: Implement `artifacts.rs` for `$PGDATA/pg_wasm/<module_id>/` (module.wasm, module.cwasm, world.wit). Include atomic write (temp + rename), directory fsync, checksum verification (sha256), and a `prune_stale` helper for orphaned dirs.
    status: completed
  - id: policy-resolve
    content: Define `config::{LoadOptions, PolicyOverrides, Limits}` and `policy::{EffectivePolicy, resolve}`. Enforce narrowing semantics (overrides can only deny/tighten). Cover with host-only unit tests for every combination.
    status: completed
  - id: abi-detect
    content: |
      Implement `abi::detect` using `wasmparser` 0.247 to classify bytes as
      `Component` or `Core`. Drive it off `wasmparser::Parser::parse_all` and
      match the first `Payload::Version { encoding: Encoding::Component | Encoding::Module }`.
      Also run `wasmparser::validate(bytes)` for full validation before handing
      bytes to Wasmtime. Honor `options.abi` only to force `core` parsing;
      reject unknown encodings with `PgWasmError::ValidationFailed`. Add
      host-only unit tests with hand-crafted binaries.
    status: completed
  - id: engine-and-epoch-ticker
    content: |
      Implement `runtime::engine::shared_engine()` returning a lazily-initialized
      `wasmtime::Engine` (v43). Configure via `wasmtime::Config` using only
      methods that exist in v43: `wasm_component_model(true)`,
      `epoch_interruption(true)`, `consume_fuel(pg_wasm.fuel_enabled)`,
      `cache(None)` (we manage our own on-disk cache), and
      `parallel_compilation(false)`. Do NOT call the removed/deprecated
      `async_support` or `cache_config_load_default` methods. Drive the epoch
      ticker thread from `_PG_init` reading `pg_wasm.epoch_tick_ms`; the thread
      holds an `EngineWeak` (from `Engine::weak()`) and invokes
      `Engine::increment_epoch()` per tick, calling `EngineWeak::upgrade()`
      each tick so the thread exits naturally when the last `Engine` reference
      is dropped.
    status: completed
  - id: trampoline-stub
    content: Add `trampoline::pg_wasm_udf_trampoline` C entry point that resolves `fn_oid` through `registry::FN_OID_MAP`. Initially returns a constant; wire `registry` with a generation-aware cache that refreshes from catalog on miss.
    status: completed
  - id: proc-reg-ddl
    content: Implement `proc_reg::{register, unregister}` wrapping `ProcedureCreate` / `RemoveFunctionById` and `recordDependencyOn(DEPENDENCY_EXTENSION)`. Validate name collision handling per `options.replace_exports`.
    status: completed
  - id: core-module-scalar-path
    content: Implement `runtime::core` for core modules with scalar-only ABI (i32/i64/f32/f64/bool). Implement `mapping::scalars` and end-to-end load -> trampoline -> call on a fixture `add_i32.wat`. Verify via pg_regress golden output.
    status: completed
  - id: wit-type-resolver
    content: |
      Implement `wit::world` (parse components via `wit_component::decode` from
      wit-component 0.247, destructuring the `DecodedWasm::Component(Resolve, WorldId)`
      variant) and `wit::typing` with the full `wit_to_pg` mapping over
      `wit_parser::{Resolve, Type, TypeDef, TypeDefKind}` (bool, s*/u*, f32/f64,
      char, string, list<u8>, list<T>, option, result, tuple, record, variant,
      enum, flags, resource/handle). Produce a stable plan keyed by module
      prefix. Normalize world output with `wit_component::WitPrinter` for
      storage in `pg_wasm.modules.wit_world`.
    status: completed
  - id: udt-registration
    content: Implement `wit::udt::register_type_plan` that issues `CREATE TYPE`, `CREATE DOMAIN`, `CREATE ENUM` DDL via SPI and records rows in `pg_wasm.wit_types` with `recordDependencyOn`. Idempotent for reload-compatible definitions; updates OIDs in-place when definitions match.
    status: completed
  - id: component-compile-and-pool
    content: |
      Implement `runtime::component` to compile a
      `wasmtime::component::Component` (via `Component::from_binary`),
      AOT-precompile a `.cwasm` to disk via
      `Engine::precompile_component(bytes)`, and record the artifact's
      `Engine::precompile_compatibility_hash` alongside it. On cold backends
      reload with `Engine::detect_precompiled_file` + the unsafe
      `Component::deserialize_file`. Stand up a `wasmtime::component::Linker`
      and wire WASI via `wasmtime_wasi::p2::add_to_linker_sync` (the v43 path;
      the older `wasmtime_wasi::preview2::*` module was renamed to `p2`) with
      a per-store `WasiCtx` built from `wasmtime_wasi::WasiCtxBuilder` and a
      `WasiView` impl returning `WasiCtxView { ctx, table }`. Wire HTTP (when
      enabled) via `wasmtime_wasi_http::p2::add_to_linker_sync`. Implement
      `runtime::pool` with a per-module bounded instance pool sized by
      `pg_wasm.instances_per_module` (new GUC).
    status: completed
  - id: component-marshal-dynamic
    content: |
      Implement `mapping::composite` and `mapping::list` on the dynamic
      `wasmtime::component::Val` path. For each WIT type produce a marshaler
      that consumes a PG `Datum` and returns a `Val`, and vice versa. Cover
      records (named + anonymous tuples), variants, enums, flags, options,
      results, and typed lists. Call exports via
      `wasmtime::component::Func::call(&mut store, &[Val], &mut [Val])` (v43
      takes a caller-provided result slice rather than returning a `Vec`) and
      call `Func::post_return` after each invocation before reusing the
      instance.
    status: completed
  - id: load-orchestration
    content: Implement `lifecycle::load` running AuthZ -> read -> validate -> classify -> resolve WIT -> plan types -> plan exports -> resolve policy -> compile + persist -> register procs -> on-load hook -> bump generation. All DDL runs via SPI inside one transaction; failure rolls everything back and removes the module dir.
    status: pending
  - id: unload-orchestration
    content: Implement `lifecycle::unload` with `on-unload` hook, `RemoveFunctionById`, UDT drop (respecting `pg_wasm.dependencies` and `options.cascade`), catalog row deletion, artifact dir removal, generation bump.
    status: completed
  - id: reload-orchestration
    content: Implement `lifecycle::reload` that preserves `fn_oid` / `pg_type.oid` when signatures/definitions are unchanged, issues `ALTER TYPE` where possible, and errors on breaking changes unless `options.breaking_changes_allowed`. Atomic module.wasm swap via temp + rename.
    status: pending
  - id: reconfigure-orchestration
    content: Implement `lifecycle::reconfigure` that updates `policy` / `limits` rows, calls `on-reconfigure` hook, and bumps generation. Confirm `StoreLimits` and epoch deadlines pick up the new values on next call via integration test.
    status: completed
  - id: host-interfaces
    content: Implement `pg_wasm:host/log` (maps to `ereport(NOTICE/INFO/WARNING)`) and `pg_wasm:host/query` (SPI read-only by default, gated by `pg_wasm.allow_spi`). Provide WIT text in `pg_wasm/wit/host.wit` and wire into the component `Linker`.
    status: completed
  - id: invocation-path
    content: |
      Flesh out `trampoline::pg_wasm_udf_trampoline` to borrow a pooled
      instance, build per-call `StoreLimits` via `wasmtime::StoreLimitsBuilder`
      and attach with `Store::limiter`, set fuel via `Store::set_fuel` (and
      read with `Store::get_fuel` afterwards for metrics), set the epoch
      deadline via `Store::set_epoch_deadline` (ticks = deadline_ms /
      epoch_tick_ms), marshal args, call the typed export, unmarshal, and
      update shmem counters. Downcast `wasmtime::Error` via
      `err.downcast_ref::<wasmtime::Trap>()`: `Trap::Interrupt` ->
      `ERRCODE_QUERY_CANCELED`, `Trap::OutOfFuel` ->
      `ERRCODE_PROGRAM_LIMIT_EXCEEDED`, other `Trap` variants ->
      `PgWasmError::Trap { kind }` with `ERRCODE_EXTERNAL_ROUTINE_EXCEPTION`.
      Wrap in `std::panic::catch_unwind`.
    status: pending
  - id: metrics-and-views
    content: Implement `views::{modules, functions, stats, wit_types, policy_effective}` as SRF table functions backed by catalog rows and shmem atomics. Add grants so `pg_wasm_reader` can read `stats()`. Add regress tests asserting counter shape and monotonicity.
    status: completed
  - id: hooks
    content: Implement `hooks::{on_load, on_unload, on_reconfigure}` invocations with config blob passing. Hooks are optional component exports with stable names; absence is not an error. on-unload failures are logged, not fatal.
    status: pending
  - id: error-mapping
    content: Finalize `errors::PgWasmError` -> `ereport` conversion, including SQLSTATE, MESSAGE, DETAIL (module_id, export_id, wasmtime_version), HINT (policy hints on denials).
    status: pending
  - id: concurrency-safety
    content: Add `pg_wasm.CatalogLock` (LWLock tranche) held during load/unload/reload/reconfigure catalog mutation and shmem generation bumps. Confirm in-flight invocations complete against the old handle under reload. Stress-test with an integration test issuing concurrent loads + calls.
    status: pending
  - id: pg_upgrade-and-extension-upgrade
    content: |
      Verify artifacts survive `pg_upgrade`. Implement the
      `Engine::precompile_compatibility_hash` + `Engine::detect_precompiled_file`
      gate in `artifacts.rs`: on cold attach, if the hash stored alongside
      `module.cwasm` does not match the running engine's hash (or if
      `detect_precompiled_file` returns `None`/`Some(Precompiled::Module)` when
      we expect `Component`), delete the stale artifact and recompile from
      `module.wasm`. Add `sql/pg_wasm--X.Y--X.Z.sql` scaffolding;
      `catalog::migrations` validates shape on `_PG_init`.
    status: pending
  - id: test-corpus-and-pg_regress
    content: Build component fixtures (arith, strings, records, enums, variants, hooks, policy_probe, resources) and core fixtures (add_i32, echo_mem). Author pg_regress suites for lifecycle, WIT mappings, policy narrowing, error classes, metrics. Deterministic output with `ORDER BY` and `EXPLAIN (COSTS OFF, TIMING OFF)`.
    status: pending
  - id: integration-tests
    content: Add workspace `tests/` crate using `tokio-postgres`. Cover concurrent-backend load visibility via generation bumps, backend restart recovery, query cancellation via epoch interruption, fuel exhaustion, memory-pages limit, WASI policy denials.
    status: pending
  - id: docs-and-readme
    content: Update `README.md` with component-first usage. Write `docs/guc.md` (every GUC with default, scope, hot/cold reconfig), `docs/wit-mapping.md` (the full WIT -> PG table with examples). Reference them from `docs/architecture.md`.
    status: completed
  - id: build-features
    content: Set `default = ["pg13", "component-model"]`. Feature `core-only` builds without component model by gating `wit/`, `runtime/component`, `mapping/composite`, `mapping/list`. Confirm cargo check passes in both configurations and on `pg13..pg18`.
    status: pending
isProject: false
---

# pg_wasm Extension Implementation Plan (v2)

## Current state

- Workspace with [Cargo.toml](../../Cargo.toml) and [pg_wasm/Cargo.toml](../../pg_wasm/Cargo.toml); pgrx 0.18; PG 13–18 feature flags.
- [pg_wasm/src/lib.rs](../../pg_wasm/src/lib.rs) is the minimal `hello_pg_wasm` stub plus the pgrx test scaffolding.
- Prior v1 experiment (see branch `origin/v1`) explored Wasmtime + Extism with buffer-style ABI and explicit `exports` hints; v2 narrows to **Wasmtime + Component Model (WIT)** with automatic UDT registration.

The authoritative architectural design is in [`docs/architecture.md`](../../docs/architecture.md); this plan is the execution roadmap against that design.

---

## Goals re-stated

1. **Wasmtime-only runtime** with first-class Component Model + WIT support; core modules kept as a degraded scalar path.
2. **Automatic WIT → PostgreSQL type mapping**, including UDT registration of records, variants, enums, flags, and domains for unsigned integers.
3. **Durable state**: catalog tables under the extension schema and on-disk compiled artifacts under `$PGDATA/pg_wasm/`.
4. **Full lifecycle**: `load`, `unload`, `reload`, `reconfigure` with generation-driven cache invalidation across backends.
5. **Strong, narrowable sandbox**: extension-scope GUCs define the ceiling; per-module overrides may only narrow.
6. **Low per-call overhead**: compile at load, amortize instantiate via per-backend instance pools, keep the trampoline thin.
7. **Observability**: SRF views over catalog + shared-memory counters.

---

## Implementation order

The `todos` list above is authoritative. Each entry is designed to land in its own commit and be individually testable. Group boundaries (informational; all still land one-at-a-time):

1. **Foundation**: `bootstrap-layout`, `errors-and-guc`, `catalog-schema`, `shmem-and-generation`, `artifacts-layout`.
2. **Policy and ABI**: `policy-resolve`, `abi-detect`.
3. **Runtime skeleton**: `engine-and-epoch-ticker`, `trampoline-stub`, `proc-reg-ddl`, `core-module-scalar-path`.
4. **Component Model + WIT**: `wit-type-resolver`, `udt-registration`, `component-compile-and-pool`, `component-marshal-dynamic`.
5. **Lifecycle**: `load-orchestration`, `unload-orchestration`, `reload-orchestration`, `reconfigure-orchestration`.
6. **Host surfaces**: `host-interfaces`, `invocation-path`, `hooks`.
7. **Error model and concurrency**: `error-mapping`, `concurrency-safety`.
8. **Operations**: `pg_upgrade-and-extension-upgrade`, `metrics-and-views`.
9. **Testing**: `test-corpus-and-pg_regress`, `integration-tests`.
10. **Polish**: `docs-and-readme`, `build-features`.

---

## Key design decisions captured from the design doc

The full rationale lives in [`docs/architecture.md`](../../docs/architecture.md). Summarized for this plan:

- **One trampoline symbol**, many `pg_proc` rows, resolution via `flinfo->fn_oid` → `(module_id, export)` in a generation-aware process-local cache.
- **Durable catalog** (`pg_wasm.modules`, `pg_wasm.exports`, `pg_wasm.wit_types`, `pg_wasm.dependencies`) plus **on-disk artifacts** (`$PGDATA/pg_wasm/<module_id>/{module.wasm,module.cwasm,world.wit}`).
- **Shared memory** carries the generation counter and per-export atomic counters; sized by fixed constants; overflow falls back to non-shared counters with `shared := false`.
- **Policy narrowing** is enforced in `policy::resolve`; per-module overrides can only deny what GUCs permit.
- **WIT resolver** is deterministic and stable so reload can preserve OIDs on unchanged types.
- **Wasmtime configuration**: component model on, epoch interruption on, parallel compilation off, async off, fuel optional.
- **Instance pool** per module per backend, bounded by `pg_wasm.instances_per_module`; fresh `Store` per call with policy-driven `StoreLimits`.
- **Host interfaces** limited to `pg_wasm:host/log` and `pg_wasm:host/query`; everything else is WASI behind feature-scoped allow flags.

---

## Risks and mitigations

- **WIT dynamic marshaling overhead.** Walking the type tree on every call is measurable. Mitigation: cache the marshal plan per export at load time; revisit with bindgen-generated specializations after v2 lands if profiling shows hot spots.
- **Wasmtime vs PG version interactions.** pgrx, PG major versions, and Wasmtime all move. Mitigation: lock Wasmtime in the workspace, run the full `cargo pgrx test` matrix on pg13..pg18 in CI, treat `Engine::is_compatible_with_*` as the upgrade oracle.
- **Reload OID preservation corner cases.** `ALTER TYPE ADD/DROP ATTRIBUTE` on composite types has restrictions (e.g. must not have dependent rows of the type). Mitigation: detect unsupported transitions up front, error with a specific hint, require `breaking_changes_allowed` to continue.
- **Shared-memory sizing.** Module/export shared-counter slots are fixed constants. Mitigation: document the overflow behavior (`shared := false`) and surface it in `pg_wasm.stats()`.
- **WASI surface growth.** New WASI interfaces arrive regularly. Mitigation: explicit allow-list in `runtime::wasi::build_linker`; unknown interfaces cause instantiation failure with a helpful error.
- **Epoch-ticker thread lifecycle.** A per-process thread must not outlive the backend. Mitigation: start lazily via `OnceLock`, terminate on `atexit` hook registered from `_PG_init`; avoid storing any pgrx handles inside the thread.

---

## Out of scope for v2

- Extism and Wasmer backends.
- Shared-memory-backed guest linear memory (explicit non-goal).
- Hot-patching individual exports (replaced by reload).
- `wasi:keyvalue`, `wasi:blobstore` and other experimental WASI worlds (tracked as open questions in the design doc).

---

## References

Pinned crate versions: **wasmtime 43**, **wasmtime-wasi 43**,
**wasmtime-wasi-http 43**, **wasmparser 0.247**, **wit-component 0.247**,
**wit-parser 0.247**. All API references in this plan and in
[`docs/architecture.md`](../../docs/architecture.md) must match these
versions. Every URL below is pinned to the exact version — do not follow
`/latest/` links when verifying APIs.

### Internal

- Design doc: [`docs/architecture.md`](../../docs/architecture.md)
- Testing rules: [`AGENTS.md`](../../AGENTS.md),
  `.cursor/rules/pg-wasm-pgrx-testing.mdc`

### pgrx

- pgrx 0.18: https://docs.rs/pgrx/0.18

### Wasmtime 43 (core embedding)

- Crate root: https://docs.rs/wasmtime/43.0.0/wasmtime/
- `Config` (all config knobs, v43 names — note `async_support` is
  `#[doc(hidden)]`/no-op, and there is no `cache_config_load_default`; use
  `Config::cache(Option<Cache>)` instead):
  https://docs.rs/wasmtime/43.0.0/wasmtime/struct.Config.html
- `Engine` (including `precompile_component`, `precompile_module`,
  `precompile_compatibility_hash`, `detect_precompiled`,
  `detect_precompiled_file`, `increment_epoch`, `weak`, `EngineWeak`):
  https://docs.rs/wasmtime/43.0.0/wasmtime/struct.Engine.html
- `Store` (including `set_fuel`, `get_fuel`, `set_epoch_deadline`,
  `epoch_deadline_trap`, `limiter`): https://docs.rs/wasmtime/43.0.0/wasmtime/struct.Store.html
- `StoreLimits` / `StoreLimitsBuilder`:
  https://docs.rs/wasmtime/43.0.0/wasmtime/struct.StoreLimits.html
- `Trap` (v43 variants incl. `Interrupt`, `OutOfFuel`, `MemoryOutOfBounds`,
  etc.): https://docs.rs/wasmtime/43.0.0/wasmtime/enum.Trap.html
- `Cache` and `CacheConfig`:
  https://docs.rs/wasmtime/43.0.0/wasmtime/struct.Cache.html,
  https://docs.rs/wasmtime/43.0.0/wasmtime/struct.CacheConfig.html
- `Precompiled`: https://docs.rs/wasmtime/43.0.0/wasmtime/enum.Precompiled.html

### Wasmtime 43 (component model)

- `component` module overview:
  https://docs.rs/wasmtime/43.0.0/wasmtime/component/index.html
- `component::Component` (incl. `from_binary`, `serialize`, unsafe
  `deserialize` / `deserialize_file`):
  https://docs.rs/wasmtime/43.0.0/wasmtime/component/struct.Component.html
- `component::Linker`:
  https://docs.rs/wasmtime/43.0.0/wasmtime/component/struct.Linker.html
- `component::Func` (note v43 `call` signature
  `fn call(&mut store, params: &[Val], results: &mut [Val])` and
  `post_return`): https://docs.rs/wasmtime/43.0.0/wasmtime/component/struct.Func.html
- `component::Val`:
  https://docs.rs/wasmtime/43.0.0/wasmtime/component/enum.Val.html
- `component::ResourceTable`:
  https://docs.rs/wasmtime/43.0.0/wasmtime/component/struct.ResourceTable.html
- `component::bindgen!`:
  https://docs.rs/wasmtime/43.0.0/wasmtime/component/macro.bindgen.html

### wasmtime-wasi 43

- Crate overview: https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/
- WASIp2 module (entry points live under `p2`, not `preview2`):
  https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/p2/index.html
- `p2::add_to_linker_sync`:
  https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/p2/fn.add_to_linker_sync.html
- `WasiCtxBuilder`:
  https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/struct.WasiCtxBuilder.html
- `WasiView`, `WasiCtx`, `WasiCtxView`:
  https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/trait.WasiView.html,
  https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/struct.WasiCtx.html,
  https://docs.rs/wasmtime-wasi/43.0.0/wasmtime_wasi/struct.WasiCtxView.html

### wasmtime-wasi-http 43

- Crate overview:
  https://docs.rs/wasmtime-wasi-http/43.0.0/wasmtime_wasi_http/
- `p2::add_to_linker_sync` / `add_only_http_to_linker_sync`:
  https://docs.rs/wasmtime-wasi-http/43.0.0/wasmtime_wasi_http/p2/fn.add_to_linker_sync.html
- `WasiHttpCtx` / `WasiHttpView` / `WasiHttpCtxView`:
  https://docs.rs/wasmtime-wasi-http/43.0.0/wasmtime_wasi_http/struct.WasiHttpCtx.html,
  https://docs.rs/wasmtime-wasi-http/43.0.0/wasmtime_wasi_http/p2/trait.WasiHttpView.html,
  https://docs.rs/wasmtime-wasi-http/43.0.0/wasmtime_wasi_http/p2/struct.WasiHttpCtxView.html

### wasmparser 0.247

- Crate overview: https://docs.rs/wasmparser/0.247.0/wasmparser/
- `Parser`: https://docs.rs/wasmparser/0.247.0/wasmparser/struct.Parser.html
- `Validator`: https://docs.rs/wasmparser/0.247.0/wasmparser/struct.Validator.html
- `validate` free function:
  https://docs.rs/wasmparser/0.247.0/wasmparser/fn.validate.html
- `Payload` / `Encoding`:
  https://docs.rs/wasmparser/0.247.0/wasmparser/enum.Payload.html,
  https://docs.rs/wasmparser/0.247.0/wasmparser/enum.Encoding.html

### wit-component 0.247

- Crate overview: https://docs.rs/wit-component/0.247.0/wit_component/
- `decode` + `DecodedWasm` (v0.247 variants are
  `WitPackage(Resolve, Id<Package>)` and `Component(Resolve, Id<World>)`):
  https://docs.rs/wit-component/0.247.0/wit_component/fn.decode.html,
  https://docs.rs/wit-component/0.247.0/wit_component/enum.DecodedWasm.html
- `WitPrinter`:
  https://docs.rs/wit-component/0.247.0/wit_component/struct.WitPrinter.html
- `ComponentEncoder`, `embed_component_metadata`, `Linker`:
  https://docs.rs/wit-component/0.247.0/wit_component/

### wit-parser 0.247

- Crate overview: https://docs.rs/wit-parser/0.247.0/wit_parser/
- `Resolve`: https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Resolve.html
- `Type` / `TypeDef` / `TypeDefKind`:
  https://docs.rs/wit-parser/0.247.0/wit_parser/enum.Type.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.TypeDef.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/enum.TypeDefKind.html
- `Record` / `Enum` / `Flags` / `Variant` / `Result_` / `Tuple` / `Handle`:
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Record.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Enum.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Flags.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Variant.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Result_.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.Tuple.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/enum.Handle.html
- `World` / `WorldItem` / `WorldKey`:
  https://docs.rs/wit-parser/0.247.0/wit_parser/struct.World.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/enum.WorldItem.html,
  https://docs.rs/wit-parser/0.247.0/wit_parser/enum.WorldKey.html
