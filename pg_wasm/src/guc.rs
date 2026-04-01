//! Extension GUCs (plan §8, §14).

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::config::{HostPolicy, PolicyOverrides};
use crate::registry::ModuleId;

/// Base directory for relative `pg_wasm_load(text)` paths (`pg_wasm.module_path`).
pub static PG_WASM_MODULE_PATH: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Maximum WASM module size in bytes (`pg_wasm.max_module_bytes`).
pub static PG_WASM_MAX_MODULE_BYTES: GucSetting<i32> = GucSetting::<i32>::new(32 * 1024 * 1024);

/// Allow reading modules from filesystem paths (`pg_wasm.allow_load_from_file`). Default off.
pub static PG_WASM_ALLOW_LOAD_FROM_FILE: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Comma-separated absolute path prefixes; empty means only paths under [`PG_WASM_MODULE_PATH`].
pub static PG_WASM_ALLOWED_PATH_PREFIXES: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Allow linking WASI for modules that import `wasi_snapshot_preview1` (`pg_wasm.allow_wasi`).
pub static PG_WASM_ALLOW_WASI: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Allow WASI access to process environment (`pg_wasm.allow_wasi_env`).
pub static PG_WASM_ALLOW_WASI_ENV: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Allow preopening `pg_wasm.module_path` read-only for WASI (`pg_wasm.allow_wasi_fs_read`).
pub static PG_WASM_ALLOW_WASI_FS_READ: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Allow mutate permissions on the preopened module path (`pg_wasm.allow_wasi_fs_write`).
pub static PG_WASM_ALLOW_WASI_FS_WRITE: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Allow WASI sockets / inherited network (`pg_wasm.allow_wasi_network`).
pub static PG_WASM_ALLOW_WASI_NETWORK: GucSetting<bool> = GucSetting::<bool>::new(false);

/// When off, per-call timing and invocation counters are not updated (`pg_wasm.collect_metrics`).
pub static PG_WASM_COLLECT_METRICS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Max Wasm linear memory pages (64 KiB each) per guest store. `0` = no extension-side cap (plan §10).
pub static PG_WASM_MAX_MEMORY_PAGES: GucSetting<i32> = GucSetting::<i32>::new(4096);

/// Fuel units per wasm invocation when fuel is enabled (`pg_wasm.fuel_per_invocation`). `0` = unlimited.
pub static PG_WASM_FUEL_PER_INVOCATION: GucSetting<i32> = GucSetting::<i32>::new(500_000_000);

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
    GucRegistry::define_bool_guc(
        c"pg_wasm.allow_wasi",
        c"When on, modules that import WASI preview1 may be instantiated with a WASI host.",
        c"Default off. Per-module options can only narrow this and other WASI capability GUCs.",
        &PG_WASM_ALLOW_WASI,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_wasm.allow_wasi_env",
        c"When WASI is enabled, inherit the backend process environment into the guest.",
        c"Default off.",
        &PG_WASM_ALLOW_WASI_ENV,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_wasm.allow_wasi_fs_read",
        c"When WASI is enabled and pg_wasm.module_path is set, preopen it read-only as /.",
        c"Default off.",
        &PG_WASM_ALLOW_WASI_FS_READ,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_wasm.allow_wasi_fs_write",
        c"When WASI is enabled and the module path is preopened, allow guest write/mutate.",
        c"Requires allow_wasi_fs_read. Default off.",
        &PG_WASM_ALLOW_WASI_FS_WRITE,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_wasm.allow_wasi_network",
        c"When WASI is enabled, allow inherited host network for preview1 socket-related imports.",
        c"Default off.",
        &PG_WASM_ALLOW_WASI_NETWORK,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"pg_wasm.collect_metrics",
        c"When on, pg_wasm records per-export invocation counts, errors, and timings in this backend process.",
        c"When off, the trampoline skips timing updates (slightly lower overhead).",
        &PG_WASM_COLLECT_METRICS,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_wasm.max_memory_pages",
        c"Maximum WebAssembly linear memory size in 64 KiB pages for each guest store (host grow + wasm memory.grow).",
        c"0 disables this cap (module wasm max still applies). Per-module max_memory_pages narrows this.",
        &PG_WASM_MAX_MEMORY_PAGES,
        0,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"pg_wasm.fuel_per_invocation",
        c"Wasmtime fuel budget for each UDF call, lifecycle hook, or host entry into guest code.",
        c"0 means unlimited fuel. Per-module fuel in load options narrows this.",
        &PG_WASM_FUEL_PER_INVOCATION,
        0,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );
}

#[must_use]
pub fn collect_metrics() -> bool {
    PG_WASM_COLLECT_METRICS.get()
}

#[must_use]
pub fn host_policy_from_gucs() -> HostPolicy {
    HostPolicy {
        allow_env: PG_WASM_ALLOW_WASI_ENV.get(),
        allow_fs_read: PG_WASM_ALLOW_WASI_FS_READ.get(),
        allow_fs_write: PG_WASM_ALLOW_WASI_FS_WRITE.get(),
        allow_network: PG_WASM_ALLOW_WASI_NETWORK.get(),
        allow_wasi: PG_WASM_ALLOW_WASI.get(),
    }
}

/// Merge extension GUCs with per-module overrides (plan §6). Overrides may only narrow.
#[must_use]
pub fn effective_host_policy(overrides: &PolicyOverrides) -> HostPolicy {
    let g = host_policy_from_gucs();
    HostPolicy {
        allow_env: narrow_bool(g.allow_env, overrides.allow_env),
        allow_fs_read: narrow_bool(g.allow_fs_read, overrides.allow_fs_read),
        allow_fs_write: narrow_bool(g.allow_fs_write, overrides.allow_fs_write),
        allow_network: narrow_bool(g.allow_network, overrides.allow_network),
        allow_wasi: narrow_bool(g.allow_wasi, overrides.allow_wasi),
    }
}

fn narrow_bool(global: bool, module: Option<bool>) -> bool {
    match module {
        Some(false) => false,
        _ => global,
    }
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

/// Effective Wasm page cap: extension GUC intersected with optional per-module override.
#[must_use]
pub fn effective_max_memory_pages(module: ModuleId) -> u32 {
    let g = PG_WASM_MAX_MEMORY_PAGES.get().max(0) as u32;
    let m = crate::registry::module_resource_limits(module).and_then(|r| r.max_memory_pages);
    match (g, m) {
        (0, None) => 0,
        (0, Some(mp)) => mp,
        (g, None) => g,
        (g, Some(mp)) => g.min(mp),
    }
}

/// Fuel for one guest entry: GUC (0 = unlimited) narrowed by per-module override.
#[must_use]
pub fn effective_fuel_per_invocation(module: ModuleId) -> u64 {
    let g_raw = PG_WASM_FUEL_PER_INVOCATION.get().max(0) as u64;
    let global = if g_raw == 0 { u64::MAX } else { g_raw };
    let m = crate::registry::module_resource_limits(module).and_then(|r| r.fuel);
    match m {
        None => global,
        Some(f) => global.min(f),
    }
}
