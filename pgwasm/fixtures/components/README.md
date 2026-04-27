# Component test corpus (`pgwasm/fixtures/components`)

Each subdirectory is a **standalone** Rust crate (not a workspace member) that builds a WebAssembly **component** for `pgwasm` regress tests.

## Build

From this directory:

```bash
./build_all.sh
```

Requirements:

- `rustup target add wasm32-wasip2` (most fixtures)
- `rustup target add wasm32-unknown-unknown` (fixtures that avoid WASI imports: `strings`, `resources`, `trap`)
- `wasm-tools` on `PATH` (for `wasm-tools validate`)

The script writes `*/component.wasm` next to each crate’s `world.wit`.

## Regenerate embedded SQL in pg_regress

After changing any fixture:

```bash
./build_all.sh
python3 generate_pg_regress_sql.py
```

Then refresh goldens if needed:

```bash
cd ../..   # repo root
cd pgwasm && cargo pgrx regress --resetdb
```

## Fixture list

| Directory       | Purpose |
|----------------|---------|
| `arith`        | Minimal `add(a,b)` component (also used by lifecycle/metrics). |
| `strings`    | `list<u8>` / `bytea` helpers (`wasm32-unknown-unknown`). |
| `records`    | Record-shaped WIT (reserved for when export registration supports composites). |
| `enums`      | Enum WIT (same). |
| `variants`   | Variant WIT (same). |
| `hooks`      | `on-reconfigure` hook guest. |
| `policy_probe` | Trivial `ping` export for policy/limit tests. |
| `resources`  | Resource + `borrow` (`wasm32-unknown-unknown`). |
| `trap`       | `boom()` trap for `HV00` / external routine exception regress. |
| `wit_roundtrip` | Bool, `s32`, `s64`, `string` round-trips used by `wit_mapping.sql`. |
