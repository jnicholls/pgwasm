# Core module fixtures (`.wat`)

These are **core** (non-component) WebAssembly modules used by `wasm._core_invoke_scalar` regress tests.

| File        | Exports | Notes |
|-------------|---------|--------|
| `add_i32.wat` | `add(i32,i32)->i32` | Used by `core_scalar.sql`. |
| `echo_mem.wat` | `echo_i64(i64)->i64` | Identity on `i64`; exercises memory + `i64` ABI. |

Assemble with `wat2wasm` if you need a `.wasm` on disk; regress embeds bytes as hex in SQL.
