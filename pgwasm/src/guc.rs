//! Extension-level GUC definitions and defaults.

use std::ffi::CString;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};

#[derive(Clone, Copy, Debug, Eq, PartialEq, PostgresGucEnum)]
pub(crate) enum PgWasmLogLevel {
    #[name = c"error"]
    Error,
    #[name = c"warning"]
    Warning,
    #[name = c"notice"]
    Notice,
    #[name = c"info"]
    Info,
    #[name = c"log"]
    Log,
    #[name = c"debug1"]
    Debug1,
    #[name = c"debug2"]
    Debug2,
    #[name = c"debug3"]
    Debug3,
    #[name = c"debug4"]
    Debug4,
    #[name = c"debug5"]
    Debug5,
}

pub(crate) static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
pub(crate) static ALLOW_LOAD_FROM_FILE: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static MODULE_PATH: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));
pub(crate) static ALLOWED_PATH_PREFIXES: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));
pub(crate) static MAX_MODULE_BYTES: GucSetting<i32> = GucSetting::<i32>::new(33_554_432);
pub(crate) static ALLOW_WASI: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static ALLOW_WASI_STDIO: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static ALLOW_WASI_ENV: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static ALLOW_WASI_FS: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static ALLOW_WASI_NET: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static ALLOW_WASI_HTTP: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static WASI_PREOPENS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));
pub(crate) static ALLOWED_HOSTS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c""));
pub(crate) static ALLOW_SPI: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static MAX_MEMORY_PAGES: GucSetting<i32> = GucSetting::<i32>::new(1_024);
pub(crate) static MAX_INSTANCES_TOTAL: GucSetting<i32> = GucSetting::<i32>::new(0);
pub(crate) static INSTANCES_PER_MODULE: GucSetting<i32> = GucSetting::<i32>::new(1);
pub(crate) static FUEL_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
pub(crate) static FUEL_PER_INVOCATION: GucSetting<i32> = GucSetting::<i32>::new(100_000_000);
pub(crate) static INVOCATION_DEADLINE_MS: GucSetting<i32> = GucSetting::<i32>::new(5_000);
pub(crate) static EPOCH_TICK_MS: GucSetting<i32> = GucSetting::<i32>::new(10);
pub(crate) static COLLECT_METRICS: GucSetting<bool> = GucSetting::<bool>::new(true);
pub(crate) static LOG_LEVEL: GucSetting<PgWasmLogLevel> =
    GucSetting::<PgWasmLogLevel>::new(PgWasmLogLevel::Notice);
pub(crate) static FOLLOW_SYMLINKS: GucSetting<bool> = GucSetting::<bool>::new(false);

pub(crate) fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pgwasm.enabled",
        c"Enable the pgwasm extension runtime.",
        c"Global kill switch for pgwasm module management and invocation.",
        &ENABLED,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_load_from_file",
        c"Allow loading WebAssembly modules from filesystem paths.",
        c"Controls whether pgwasm.pgwasm_load(text, ...) can read modules from disk.",
        &ALLOW_LOAD_FROM_FILE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pgwasm.module_path",
        c"Base directory for relative module load paths.",
        c"Relative paths passed to pgwasm.pgwasm_load(text, ...) are resolved against this directory.",
        &MODULE_PATH,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pgwasm.allowed_path_prefixes",
        c"Allowed canonical path prefixes for module file loads.",
        c"Comma-separated canonical path prefixes that bound filesystem module loads.",
        &ALLOWED_PATH_PREFIXES,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pgwasm.max_module_bytes",
        c"Maximum accepted WebAssembly module size in bytes.",
        c"Hard upper bound for module byte length accepted by pgwasm.pgwasm_load.",
        &MAX_MODULE_BYTES,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_wasi",
        c"Allow WASI host interfaces for loaded modules.",
        c"Master WASI capability gate. Individual WASI capability GUCs can only narrow this.",
        &ALLOW_WASI,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_wasi_stdio",
        c"Allow WASI stdio access.",
        c"Permits WASI stdout/stderr integration for module executions.",
        &ALLOW_WASI_STDIO,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_wasi_env",
        c"Allow WASI environment variable access.",
        c"Permits module access to selected process environment variables through WASI.",
        &ALLOW_WASI_ENV,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_wasi_fs",
        c"Allow WASI filesystem preopen access.",
        c"Permits filesystem preopens configured by pgwasm.wasi_preopens.",
        &ALLOW_WASI_FS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_wasi_net",
        c"Allow WASI socket networking.",
        c"Permits TCP/UDP networking subject to pgwasm.allowed_hosts policy.",
        &ALLOW_WASI_NET,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_wasi_http",
        c"Allow WASI HTTP host interfaces.",
        c"Permits wasi:http imports through wasmtime-wasi-http bindings.",
        &ALLOW_WASI_HTTP,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pgwasm.wasi_preopens",
        c"WASI filesystem preopen mappings.",
        c"Comma-separated guest=host mappings used when WASI filesystem access is enabled.",
        &WASI_PREOPENS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pgwasm.allowed_hosts",
        c"Allowed outbound host:port list for WASI networking.",
        c"Comma-separated host:port entries that bound outbound socket and HTTP connectivity.",
        &ALLOWED_HOSTS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.allow_spi",
        c"Allow host SPI query interface to modules.",
        c"Permits guest calls through the pgwasm:host/query host interface.",
        &ALLOW_SPI,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pgwasm.max_memory_pages",
        c"Maximum linear memory pages per invocation store.",
        c"Upper bound on Wasm linear memory pages enforced through StoreLimits.",
        &MAX_MEMORY_PAGES,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pgwasm.max_instances_total",
        c"Maximum process-local Wasm instances across all modules.",
        c"Process-wide instance cap. A value of zero means unbounded.",
        &MAX_INSTANCES_TOTAL,
        0,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pgwasm.instances_per_module",
        c"Maximum pooled instances per module per backend.",
        c"Upper bound for backend-local instance pooling per module.",
        &INSTANCES_PER_MODULE,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.fuel_enabled",
        c"Enable deterministic fuel budgeting for invocations.",
        c"When enabled, each invocation is configured with a fuel budget and consumption is tracked.",
        &FUEL_ENABLED,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pgwasm.fuel_per_invocation",
        c"Fuel budget assigned to each invocation.",
        c"Maximum fuel assigned to each invocation when fuel accounting is enabled.",
        &FUEL_PER_INVOCATION,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pgwasm.invocation_deadline_ms",
        c"Per-invocation wall-clock deadline in milliseconds.",
        c"Epoch-based invocation timeout. A value of zero disables deadline enforcement.",
        &INVOCATION_DEADLINE_MS,
        0,
        i32::MAX,
        GucContext::Suset,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_int_guc(
        c"pgwasm.epoch_tick_ms",
        c"Epoch ticker interval in milliseconds.",
        c"Resolution used by the epoch ticker thread that advances Wasmtime epochs.",
        &EPOCH_TICK_MS,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::UNIT_MS,
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.collect_metrics",
        c"Collect shared and process-local invocation metrics.",
        c"Controls whether invocation counters and timings are collected for pgwasm views.",
        &COLLECT_METRICS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_enum_guc(
        c"pgwasm.log_level",
        c"Runtime lifecycle logging verbosity.",
        c"Minimum logging level used by pgwasm lifecycle and runtime events.",
        &LOG_LEVEL,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pgwasm.follow_symlinks",
        c"Allow symlink traversal during module path resolution.",
        c"When disabled, canonical path resolution rejects symlink traversal for module file loads.",
        &FOLLOW_SYMLINKS,
        GucContext::Suset,
        GucFlags::default(),
    );
}
