//! Single exported C symbol for all dynamically registered WASM UDFs (`…_wrapper` from `#[pg_extern]`).

/// `prosrc` / `AS` symbol PostgreSQL resolves for every WASM-backed function (see plan §11).
pub const TRAMPOLINE_PG_SRC: &str = "pg_wasm_udf_trampoline_wrapper";
