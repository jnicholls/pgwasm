//! Extension GUCs (plan §8, §14).

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Base directory for relative `pg_wasm_load(text)` paths (`pg_wasm.module_path`).
pub static PG_WASM_MODULE_PATH: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Maximum WASM module size in bytes (`pg_wasm.max_module_bytes`).
pub static PG_WASM_MAX_MODULE_BYTES: GucSetting<i32> =
    GucSetting::<i32>::new(32 * 1024 * 1024);

/// Allow reading modules from filesystem paths (`pg_wasm.allow_load_from_file`). Default off.
pub static PG_WASM_ALLOW_LOAD_FROM_FILE: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Comma-separated absolute path prefixes; empty means only paths under [`PG_WASM_MODULE_PATH`].
pub static PG_WASM_ALLOWED_PATH_PREFIXES: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub fn init() {
    GucRegistry::define_string_guc(
        c"pg_wasm.module_path",
        c"Directory used to resolve relative paths in pg_wasm_load(text).",
        c"Unset disables relative path loads until set. Absolute paths still require an allow rule.",
        &PG_WASM_MODULE_PATH,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_wasm.max_module_bytes",
        c"Maximum WASM binary size accepted by pg_wasm_load.",
        c"Applies to both bytea and filesystem loads.",
        &PG_WASM_MAX_MODULE_BYTES,
        1024,
        i32::MAX,
        GucContext::Suset,
        GucFlags::UNIT_BYTE,
    );
    GucRegistry::define_bool_guc(
        c"pg_wasm.allow_load_from_file",
        c"When on, pg_wasm_load(text) may read files under allowed prefixes.",
        c"Default off; pg_wasm_load(bytea) does not require this.",
        &PG_WASM_ALLOW_LOAD_FROM_FILE,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_wasm.allowed_path_prefixes",
        c"Comma-separated absolute directory prefixes for pg_wasm_load(text).",
        c"If empty, resolved paths must fall under pg_wasm.module_path.",
        &PG_WASM_ALLOWED_PATH_PREFIXES,
        GucContext::Suset,
        GucFlags::default(),
    );
}

#[must_use]
pub fn module_path_cstr() -> Option<CString> {
    PG_WASM_MODULE_PATH.get()
}

#[must_use]
pub fn max_module_bytes() -> usize {
    PG_WASM_MAX_MODULE_BYTES.get().max(0) as usize
}

#[must_use]
pub fn allow_load_from_file() -> bool {
    PG_WASM_ALLOW_LOAD_FROM_FILE.get()
}

#[must_use]
pub fn allowed_path_prefixes_raw() -> Option<CString> {
    PG_WASM_ALLOWED_PATH_PREFIXES.get()
}
