# Wave-6 Cloud Agent prompts — RUN SERIALLY

Launch **after every Wave-5 PR has merged into `main`**.

Wave 6 has one active cross-cutting todo: **`error-mapping.md`**. The
`build-features` plan item was **closed without implementation** (see
plan file); do not launch `build-features.md`.

Shared rules: see `.cursor/cloud-agent-prompts/wave-1/README.md`.

## Ownership matrix (Wave 6)

| Prompt             | Branch                    | Files owned |
|--------------------|---------------------------|-------------|
| `error-mapping.md` | `wave-6/error-mapping`    | `pg_wasm/src/errors.rs` (full rewrite), narrow edits across every `ereport!`/`Err(PgWasmError::...)` call site |
| ~~`build-features.md`~~ (cancelled) | — | — |

## "Do not touch"

- Anything not needed to achieve the task. Keep per-file diff narrow.
- `Cargo.toml` (workspace) — keep workspace `Cargo.toml` unchanged unless a prompt explicitly allows it.
- `pg_wasm.control`, `pg_wasm/src/lib.rs` (beyond feature gates).

## Scheduling note

Run `error-mapping` when ready; there is no follow-on `build-features` agent for this wave.
