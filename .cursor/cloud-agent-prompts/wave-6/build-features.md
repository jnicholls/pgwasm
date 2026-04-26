# Wave-6 Cloud Agent: `build-features` (**CANCELLED — do not run**)

This prompt is **retired**. The plan todo was marked `completed` with a
note that the work will not be done. Keep the file for history only.

---

# Original prompt (historical)

## ~~Wave-6 Cloud Agent: `build-features` (RUN AFTER `error-mapping` MERGES)~~

**Branch**: `wave-6/build-features` (base: `main`)
**PR title**: `[wave-6] build-features: component-model feature flag + core-only build`

Read `@.cursor/cloud-agent-prompts/wave-6/README.md` and
`@.cursor/cloud-agent-prompts/wave-1/README.md` first.

## Task (copied verbatim)

> Set `default = ["pg13", "component-model"]`. Feature `core-only`
> builds without component model by gating `wit/`, `runtime/component`,
> `mapping/composite`, `mapping/list`. Confirm cargo check passes in
> both configurations and on `pg13..pg18`.

Design ref: `docs/architecture.md` §§ "Build features and PG
versions".

## Files you own

- `pg_wasm/Cargo.toml` — add `component-model` feature; adjust
  `default`.
- Narrow `#[cfg(feature = "component-model")]` gates across:
  - `pg_wasm/src/wit/` (entire module)
  - `pg_wasm/src/runtime/component.rs`
  - `pg_wasm/src/runtime/pool.rs` (pool is generic; gate only the
    `Component`-typed slot path — the trait-level pool stays available
    for the core path if it reuses it)
  - `pg_wasm/src/mapping/composite.rs`
  - `pg_wasm/src/mapping/list.rs`
  - `pg_wasm/src/runtime/host.rs` (host/query interface depends on
    component linker; gate if needed)
  - `pg_wasm/src/lifecycle/*.rs` — component-specific branches only
    (policy/compile/plan-types steps)
- Any call-site `#[cfg(feature = "component-model")]` at the module
  declaration or mod-level in `pg_wasm/src/lib.rs` — the **only**
  permitted edit to `lib.rs` for this task is adding `#[cfg]` on the
  mod declarations. Do not change `_PG_init` body.

## Files you must not touch

- `pg_wasm.control`, workspace `Cargo.toml`.
- Any non-cfg-gate substantive edit to existing code. You are only
  adding feature gates, not refactoring.
- Existing GUC definitions — if a GUC is component-model-only, gate
  its registration site inside `guc.rs` with
  `#[cfg(feature = "component-model")]`. If gating would change
  observable GUC behavior, STOP and note it.

## Implementation notes

- **Cargo**:
  ```toml
  [features]
  default = ["pg13", "component-model"]
  component-model = [
      "wasmtime/component-model",
      "dep:wasmtime-wasi",
      "dep:wasmtime-wasi-http",
      "dep:wit-component",
      "dep:wit-parser",
  ]
  core-only = [] # mutually exclusive with component-model at build time
  ```
  Mark the component-only deps as `optional = true`:
  ```toml
  wasmtime-wasi = { workspace = true, optional = true }
  wasmtime-wasi-http = { workspace = true, optional = true }
  wit-component = { workspace = true, optional = true }
  wit-parser = { workspace = true, optional = true }
  ```
  `wasmparser` stays non-optional because core-only still needs ABI
  detection and validation.
- Keep `[dependencies]` keys in alphabetical order per
  `AGENTS.md`.
- **Core-only invariants**:
  - `abi::detect`, `runtime::core`, `mapping::scalars`,
    `proc_reg`, `trampoline`, `registry`, `catalog`, `shmem`,
    `artifacts`, `policy`, `config`, `guc`, `errors`, `views` all
    compile without the feature. `lifecycle::load` must compile to a
    core-only code path.
  - Loading a component under core-only returns
    `PgWasmError::Unsupported("core-only build")`.
- **CI matrix** mention: add a `# CI matrix` comment block in
  `pg_wasm/Cargo.toml` (in a `##` comment, since Cargo.toml is TOML)
  listing the combinations:
  - `pg13..pg18` × `default` (component-model)
  - `pg13..pg18` × `core-only`
  Actual CI config changes are out of scope for this PR.

## Validation expectations

- Both `cargo check -p pg_wasm --no-default-features --features pg13`
  and `cargo check -p pg_wasm --features pg13,core-only
  --no-default-features` pass.
- Re-run across `pg13..pg18` locally if feasible; otherwise rely on
  CI to cover. Document in PR description what you ran.

## Final commit

Flip `build-features`'s `status:` line to `completed`.
