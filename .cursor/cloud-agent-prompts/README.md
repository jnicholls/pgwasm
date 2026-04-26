# pg_wasm Cloud Agent prompts

Prompt sets for driving the
`.cursor/plans/pg_wasm_extension_implementation_v2.plan.md` implementation
via Cursor Cloud Agents, organized into parallelizable waves.

## Shared rules

Every prompt is self-contained. Each one references its wave's
`README.md` for shared rules (branch/base conventions, workspace rules,
pinned dependency versions, "do not touch" list, plan-file status-flip
instruction, merge-conflict policy).

## Wave index

| Wave | PRs in parallel | Todos |
|------|-----------------|-------|
| 1    | 10              | `catalog-schema`, `shmem-and-generation`, `artifacts-layout`, `policy-resolve`, `abi-detect`, `engine-and-epoch-ticker`, `trampoline-stub`, `proc-reg-ddl`, `wit-type-resolver`, `docs-and-readme` |
| 2    | 5               | `core-module-scalar-path`, `udt-registration`, `component-compile-and-pool`, `component-marshal-dynamic`, `reconfigure-orchestration` |
| 3    | 4               | `unload-orchestration`, `host-interfaces`, `invocation-path`, `metrics-and-views` |
| 4    | 2               | `load-orchestration`, `hooks` |
| 5    | 3               | `reload-orchestration`, `concurrency-safety`, `pg_upgrade-and-extension-upgrade` |
| 6    | **1 at a time** | `error-mapping` only (`build-features` closed without implementation) |
| 7    | 2               | `test-corpus-and-pg_regress`, `integration-tests` |

**Do not start a later wave until every PR of the previous wave has
merged into `main`.** Each wave's prompts assume the prior waves'
outputs exist on `main`.

## Wave-by-wave launch procedure

For each wave:

1. Verify every PR of the previous wave is merged into `main` and that
   `main` builds (`cargo check -p pg_wasm`).
2. Pull `main` locally so Cloud Agents fork from a clean base.
3. Open the Cloud Agent picker (`cursor-agent -c` or `Cmd+E` → Cloud) for
   each todo in the wave.
4. Set the model (e.g. `Codex 5.3 High`), base branch = `main`, and
   paste the contents of `wave-N/<todo-id>.md`.
5. Launch all agents in the wave. Review and merge PRs as they come in.

## Cross-cutting notes

- **Plan-file merge conflicts**: every prompt ends by flipping its todo's
  `status: pending` → `status: completed` in the plan. When multiple
  same-wave PRs land back-to-back, later PRs will rebase-conflict on
  the YAML list; the prompt instructs the agent to keep incoming main
  changes AND re-apply its own status flip for its own todo. No other
  edits to the plan file are allowed.
- **Wave 6** originally scheduled `error-mapping` then `build-features`; the latter was **closed without implementation**, so only `error-mapping` remains.
- **`concurrency-safety` in Wave 5** also has a cross-cutting tint
  (wraps LWLock usage around lifecycle entry points). The prompt pins a
  narrow diff and scopes to the `lifecycle/*` + `shmem.rs` boundary,
  but if it's running at the same time as `reload-orchestration` you
  may see light conflicts on `lifecycle/`. Prefer merging
  `reload-orchestration` first, then `concurrency-safety`.
