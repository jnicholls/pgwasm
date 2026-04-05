//! macOS-only dev-dependency: defines PostgreSQL server symbols that `pgrx` references so
//! `cargo test` / `cargo pgrx test` can load the test harness without embedding into `postgres`.
//! These stubs are **not** safe if PostgreSQL internals are actually called; they exist only
//! so dyld can resolve symbols for unit tests that do not execute those paths.

/// Reference this from `pg_wasm` under `cfg(all(test, target_os = "macos"))` so the static
/// archive is linked into the test binary.
#[inline(never)]
pub fn ensure_linked() {}
