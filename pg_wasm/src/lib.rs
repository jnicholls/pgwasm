#![allow(dead_code)]

use pgrx::prelude::*;

mod abi;
mod artifacts;
mod catalog;
mod config;
mod errors;
mod guc;
mod hooks;
mod lifecycle;
mod mapping;
mod policy;
mod proc_reg;
mod registry;
mod runtime;
mod shmem;
mod trampoline;
mod views;
mod wit;

::pgrx::pg_module_magic!(name, version);

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    shmem::init();
    runtime::init();
    catalog::init();
}

#[pg_extern]
fn hello_pg_wasm() -> &'static str {
    "Hello, pg_wasm"
}

mod sql_api {
    #![allow(clippy::result_large_err)]

    use pgrx::prelude::*;

    #[pg_extern]
    fn reconfigure(
        module_name: &str,
        policy: Option<pgrx::Json>,
        limits: Option<pgrx::Json>,
    ) -> core::result::Result<bool, pgrx::pg_sys::panic::ErrorReport> {
        crate::lifecycle::reconfigure::reconfigure_impl(module_name, policy, limits)
            .map_err(crate::errors::PgWasmError::into_error_report)
    }

    #[pg_extern]
    fn unload(
        module_name: &str,
        cascade: default!(bool, false),
    ) -> core::result::Result<bool, pgrx::pg_sys::panic::ErrorReport> {
        crate::lifecycle::unload::unload_impl(module_name, cascade)
            .map_err(crate::errors::PgWasmError::into_error_report)
    }

    #[pg_extern]
    fn unload_all() -> core::result::Result<i64, pgrx::pg_sys::panic::ErrorReport> {
        crate::lifecycle::unload::unload_all_impl()
            .map(|n| n as i64)
            .map_err(crate::errors::PgWasmError::into_error_report)
    }

    #[pg_extern]
    fn load(
        module_name: &str,
        bytes_or_path: pgrx::Json,
        options: default!(Option<pgrx::Json>, NULL),
    ) -> core::result::Result<bool, pgrx::pg_sys::panic::ErrorReport> {
        crate::lifecycle::load::load_impl(module_name, bytes_or_path, options)
            .map_err(crate::errors::PgWasmError::into_error_report)
    }
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    use pgrx::Json;
    use pgrx::prelude::*;
    use pgrx::spi::Spi;
    use serde_json::json;

    use crate::catalog::modules;
    use crate::config::{Limits, PolicyOverrides};
    use crate::errors::PgWasmError;
    use crate::lifecycle::unload::test_support as unload_test;
    use crate::lifecycle::{load, reconfigure, unload};
    use crate::policy::{self, GucSnapshot};
    use crate::shmem;
    use crate::trampoline;

    /// Avoid epoch deadline traps during guest runs that call SPI while the shared engine ticker
    /// advances the epoch (other tests in the same suite use epoch interruption heavily).
    const RELAXED_INVOCATION_DEADLINE_MS: i32 = 86_400_000;

    #[pg_test]
    fn test_hello_pg_wasm() {
        assert_eq!("Hello, pg_wasm", crate::hello_pg_wasm());
    }

    #[pg_test]
    fn reconfigure_narrows_updates_catalog_and_bumps_generation() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("reconf_mod_{}", std::process::id());
        let new_module = modules::NewModule {
            abi: "component".to_string(),
            artifact_path: "/tmp/stub.wasm".to_string(),
            digest: vec![1, 2, 3],
            generation: 0,
            limits: json!({}),
            name: name.clone(),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![0; 32],
            wit_world: "default".to_string(),
        };
        let inserted = modules::insert(&new_module).expect("stub module insert");

        let gen_before = shmem::read_generation();

        let narrow = Json(json!({ "allow_wasi_net": false }));
        let ok = reconfigure::reconfigure_impl(name.as_str(), Some(narrow), None)
            .expect("narrow reconfigure");
        assert!(ok);

        let after = modules::get_by_name(&name)
            .expect("module read")
            .expect("module should exist");
        assert_eq!(after.generation, 1);
        assert_eq!(
            after.policy.get("allow_wasi_net"),
            Some(&serde_json::Value::Bool(false))
        );
        assert!(shmem::read_generation() > gen_before);

        let widen = Json(json!({ "allow_wasi_http": true }));
        let err = reconfigure::reconfigure_impl(name.as_str(), Some(widen), None)
            .expect_err("widen should be denied");
        match err {
            PgWasmError::PermissionDenied(message) => {
                assert!(
                    message.contains("allow_wasi_http"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }

        let unchanged = modules::get_by_name(&name)
            .expect("module read")
            .expect("module should still exist");
        assert_eq!(unchanged.generation, 1);

        modules::delete(inserted.module_id).expect("stub module delete");
    }

    #[pg_test]
    fn reconfigure_effective_limits_follow_catalog() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("reconf_limits_{}", std::process::id());
        let new_module = modules::NewModule {
            abi: "component".to_string(),
            artifact_path: "/tmp/stub.wasm".to_string(),
            digest: vec![4, 5, 6],
            generation: 0,
            limits: json!({}),
            name: name.clone(),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![1; 32],
            wit_world: "default".to_string(),
        };
        let inserted = modules::insert(&new_module).expect("stub module insert");

        let patch_limits = Json(json!({ "fuel_per_invocation": 99 }));
        reconfigure::reconfigure_impl(name.as_str(), None, Some(patch_limits)).expect("limits");

        let row = modules::get_by_name(&name)
            .expect("read module")
            .expect("exists");
        assert_eq!(
            row.limits.get("fuel_per_invocation"),
            Some(&serde_json::Value::Number(99.into()))
        );

        let limits = Limits {
            fuel_per_invocation: Some(99),
            ..Default::default()
        };
        let overrides = PolicyOverrides::default();
        let effective = policy::resolve(&GucSnapshot::from_gucs(), Some(&overrides), Some(&limits))
            .expect("resolve after catalog write");

        assert_eq!(effective.fuel_per_invocation, 99);
        // TODO(wave-3: invocation-path): assert `StoreLimits` / epoch deadline on the next pooled
        // invocation once the trampoline reads catalog-backed limits per call.

        modules::delete(inserted.module_id).expect("cleanup");
    }

    #[pg_test]
    fn host_log_guest_emits_notice() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        static LOG_GUEST_WASM: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/log_guest.wasm"));

        let engine = crate::runtime::engine::shared_engine();
        let component = wasmtime::component::Component::from_binary(engine, LOG_GUEST_WASM)
            .expect("log guest component");
        let guc = GucSnapshot::from_gucs();
        let mut policy = policy::resolve(&guc, None, None).expect("policy");
        policy.invocation_deadline_ms = RELAXED_INVOCATION_DEADLINE_MS;
        let linker = crate::runtime::component::build_linker(engine, &policy).expect("linker");
        let ctx = crate::runtime::component::build_store_ctx(&policy).expect("store ctx");
        let mut store = wasmtime::Store::new(engine, ctx);
        crate::runtime::component::ensure_component_spi_matches_policy(engine, &component, &policy)
            .expect("spi policy vs component imports");
        let instance = linker
            .instantiate(&mut store, &component)
            .expect("instantiate log guest");
        let run = instance
            .get_typed_func::<(), ()>(&mut store, "run")
            .expect("export run");
        trampoline::configure_store_for_invocation(&mut store, &policy).expect("store limits/fuel");
        run.call(&mut store, ()).expect("run should succeed");
    }

    #[pg_test]
    fn host_query_guest_reads_two_columns_when_spi_enabled() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        static QUERY_GUEST_WASM: &[u8] =
            include_bytes!(concat!(env!("OUT_DIR"), "/query_guest.wasm"));

        let engine = crate::runtime::engine::shared_engine();
        let component = wasmtime::component::Component::from_binary(engine, QUERY_GUEST_WASM)
            .expect("query guest component");
        let mut guc = GucSnapshot::from_gucs();
        guc.allow_spi = true;
        let mut policy = policy::resolve(&guc, None, None).expect("policy with spi");
        policy.invocation_deadline_ms = RELAXED_INVOCATION_DEADLINE_MS;
        let linker = crate::runtime::component::build_linker(engine, &policy).expect("linker");
        let ctx = crate::runtime::component::build_store_ctx(&policy).expect("store ctx");
        let mut store = wasmtime::Store::new(engine, ctx);
        crate::runtime::component::ensure_component_spi_matches_policy(engine, &component, &policy)
            .expect("spi policy vs component imports");
        let instance = linker
            .instantiate(&mut store, &component)
            .expect("instantiate query guest");
        let run = instance
            .get_typed_func::<(), (String,)>(&mut store, "run")
            .expect("export run");
        trampoline::configure_store_for_invocation(&mut store, &policy).expect("store limits/fuel");
        let (summary,) = run.call(&mut store, ()).expect("run");
        assert!(
            summary.starts_with("cols=2") && summary.contains("cells=2"),
            "unexpected summary: {summary}"
        );
    }

    #[pg_test]
    fn host_query_guest_instantiation_denied_when_spi_disabled() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        static QUERY_GUEST_WASM: &[u8] =
            include_bytes!(concat!(env!("OUT_DIR"), "/query_guest.wasm"));

        let engine = crate::runtime::engine::shared_engine();
        let component = wasmtime::component::Component::from_binary(engine, QUERY_GUEST_WASM)
            .expect("query guest component");
        let mut guc = GucSnapshot::from_gucs();
        guc.allow_spi = false;
        let policy = policy::resolve(&guc, None, None).expect("policy without spi");
        let err = crate::runtime::component::ensure_component_spi_matches_policy(
            engine, &component, &policy,
        )
        .expect_err("query guest should be rejected when SPI is disabled");
        let msg = err.to_string();
        assert!(
            msg.contains("pg_wasm.allow_spi"),
            "expected GUC hint in error, got: {msg}"
        );
    }

    #[pg_test]
    fn host_query_rejects_write_sql() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        static WRITE_GUEST_WASM: &[u8] =
            include_bytes!(concat!(env!("OUT_DIR"), "/write_query_guest.wasm"));

        let engine = crate::runtime::engine::shared_engine();
        let component = wasmtime::component::Component::from_binary(engine, WRITE_GUEST_WASM)
            .expect("write query guest");
        let mut guc = GucSnapshot::from_gucs();
        guc.allow_spi = true;
        let mut policy = policy::resolve(&guc, None, None).expect("policy");
        policy.invocation_deadline_ms = RELAXED_INVOCATION_DEADLINE_MS;
        let linker = crate::runtime::component::build_linker(engine, &policy).expect("linker");
        let ctx = crate::runtime::component::build_store_ctx(&policy).expect("store ctx");
        let mut store = wasmtime::Store::new(engine, ctx);
        crate::runtime::component::ensure_component_spi_matches_policy(engine, &component, &policy)
            .expect("spi policy vs component imports");
        let instance = linker
            .instantiate(&mut store, &component)
            .expect("instantiate");
        let run = instance
            .get_typed_func::<(), (String,)>(&mut store, "run")
            .expect("export run");
        trampoline::configure_store_for_invocation(&mut store, &policy).expect("store limits/fuel");
        let (out,) = run.call(&mut store, ()).expect("run");
        assert!(
            out.contains("read-only") || out.contains("DELETE"),
            "expected read-only rejection, got: {out}"
        );
    }

    #[pg_test]
    fn host_query_disabled_per_invocation_when_catalog_narrows_spi() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        static QUERY_GUEST_WASM: &[u8] =
            include_bytes!(concat!(env!("OUT_DIR"), "/query_guest.wasm"));

        let engine = crate::runtime::engine::shared_engine();
        let component = wasmtime::component::Component::from_binary(engine, QUERY_GUEST_WASM)
            .expect("query guest");
        let mut guc = GucSnapshot::from_gucs();
        guc.allow_spi = true;
        let mut policy_allow = policy::resolve(&guc, None, None).expect("allow");
        policy_allow.invocation_deadline_ms = RELAXED_INVOCATION_DEADLINE_MS;
        let linker =
            crate::runtime::component::build_linker(engine, &policy_allow).expect("linker");

        let mut guc_off = guc.clone();
        guc_off.allow_spi = false;
        let policy_deny = policy::resolve(&guc_off, None, None).expect("deny");
        let mut ctx = crate::runtime::component::build_store_ctx(&policy_allow).expect("ctx");
        ctx.host.allow_spi = policy_deny.allow_spi;
        let mut store = wasmtime::Store::new(engine, ctx);
        crate::runtime::component::ensure_component_spi_matches_policy(
            engine,
            &component,
            &policy_allow,
        )
        .expect("spi policy vs component imports");
        let instance = linker
            .instantiate(&mut store, &component)
            .expect("instantiate");
        let run = instance
            .get_typed_func::<(), (String,)>(&mut store, "run")
            .expect("run export");
        trampoline::configure_store_for_invocation(&mut store, &policy_allow)
            .expect("store limits/fuel");
        let (out,) = run.call(&mut store, ()).expect("run");
        assert!(
            out.starts_with("ERR:") && out.contains("pg_wasm.allow_spi"),
            "expected SPI denial in guest error string, got: {out}"
        );
    }

    #[pg_test]
    fn unload_unregisters_proc_and_deletes_catalog_rows() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("unload_basic_{}", std::process::id());
        let mid = unload_test::insert_stub_module(&name);
        let fn_name = format!("unload_fn_{}", std::process::id());
        let fn_oid = unload_test::register_dummy_sql_fn(&fn_name);
        let _export_id = unload_test::insert_export_with_fn(mid, "unload_ex", fn_oid);

        assert!(unload_test::pg_proc_exists(fn_oid));
        assert_eq!(unload_test::count_modules_where_name(&name), 1);
        assert_eq!(unload_test::count_exports_for_module(mid), 1);

        unload::unload_impl(&name, false).expect("unload");

        assert!(!unload_test::pg_proc_exists(fn_oid));
        assert_eq!(unload_test::count_modules_where_name(&name), 0);
        assert_eq!(unload_test::count_exports_for_module(mid), 0);
    }

    #[pg_test]
    fn unload_udt_respects_dependencies_and_cascade() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let pid = std::process::id();
        let name_a = format!("unload_mod_a_{pid}");
        let name_b = format!("unload_mod_b_{pid}");
        let mid_a = unload_test::insert_stub_module(&name_a);
        let mid_b = unload_test::insert_stub_module(&name_b);

        let type_name = format!("wasm.tunload_T_{pid}");
        let typ_oid = unload_test::create_composite_type(&type_name);
        let _wit = unload_test::insert_wit_type_row(mid_a, "T", typ_oid);
        unload_test::insert_dependency(mid_b, mid_a);

        let err = unload::unload_impl(&name_a, false).expect_err("unload without cascade");
        match err {
            PgWasmError::InvalidConfiguration(msg) => {
                assert!(
                    msg.contains("cascade"),
                    "expected cascade hint in message: {msg}"
                );
            }
            other => panic!("expected InvalidConfiguration, got {other:?}"),
        }
        assert!(unload_test::type_exists(typ_oid));

        unload::unload_impl(&name_a, true).expect("unload with cascade");
        assert_eq!(unload_test::count_modules_where_name(&name_a), 0);
        assert!(!unload_test::type_exists(typ_oid));

        modules::delete(mid_b).expect("cleanup module B");
    }

    #[pg_test]
    fn unload_removes_artifact_directory_after_commit() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("unload_artifact_{}", std::process::id());
        let mid = unload_test::insert_stub_module(&name);
        let mid_u64 = mid as u64;
        unload_test::ensure_artifact_dir(mid_u64);
        assert!(unload_test::artifact_dir_exists(mid_u64));

        unload::unload_impl(&name, false).expect("unload");
        // `#[pg_test]` runs inside a client transaction that the framework always rolls back, so
        // `PgXactCallbackEvent::Commit` never fires; mirror production post-commit cleanup here.
        unload::run_post_commit_unload_work(mid_u64);

        assert!(!unload_test::artifact_dir_exists(mid_u64));
    }

    #[pg_test]
    fn unload_defers_disk_cleanup_until_post_commit_work() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("unload_defer_{}", std::process::id());
        let mid = unload_test::insert_stub_module(&name);
        let mid_u64 = mid as u64;
        unload_test::ensure_artifact_dir(mid_u64);
        assert!(unload_test::artifact_dir_exists(mid_u64));

        unload::unload_impl(&name, false).expect("unload");
        // Catalog mutations are visible; pool/artifact/shmem cleanup is registered for commit only.
        assert!(
            unload_test::artifact_dir_exists(mid_u64),
            "artifact dir must remain until commit callback or explicit post-commit work"
        );

        unload::run_post_commit_unload_work(mid_u64);
        assert!(!unload_test::artifact_dir_exists(mid_u64));
    }

    #[pg_test]
    fn unload_missing_module_returns_not_found() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let err = unload::unload_impl("no_such_wasm_module___", false).expect_err("not found");
        match err {
            PgWasmError::NotFound(msg) => {
                assert!(msg.contains("no_such_wasm_module___"), "message: {msg}");
            }
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    /// Minimal component fixture (`foo:fixture` world, export `add: func() -> s32`).
    const LOAD_FIXTURE_COMPONENT_WASM_HEX: &str = "0061736d0d00010001550061736d010000000105016000017f030201000707010361646400000a06010400412a0b002f0970726f647563657273010c70726f6365737365642d6279010d7769742d636f6d706f6e656e7407302e3234372e300204010000000705014000007a060901000001000361646408060100000000000b09010003616464010000003d0e636f6d706f6e656e742d6e616d6501080000010003616464010900110100046d61696e010900120100046d61696e010c010200036164640103616464002f0970726f647563657273010c70726f6365737365642d6279010d7769742d636f6d706f6e656e7407302e3234372e30";

    fn decode_hex_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[pg_test]
    fn load_transaction_rollback_removes_artifact_dir() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("load_rb_{}", std::process::id());
        let bytes = decode_hex_bytes(LOAD_FIXTURE_COMPONENT_WASM_HEX);

        // `#[pg_test]` already runs inside a transaction; nested `BEGIN` via SPI is invalid.
        // Use an internal subtransaction so rollback exercises the same catalog + artifact rules.
        unsafe {
            pg_sys::BeginInternalSubTransaction(c"pg_wasm_load_rb_test".as_ptr());
        }

        let ok = load::load_impl(
            &name,
            pgrx::Json(serde_json::json!({ "bytes": bytes })),
            None,
        )
        .expect("load in subtransaction");
        assert!(ok);

        let row = modules::get_by_name(&name)
            .expect("module read")
            .expect("module row");
        let mid_u = u64::try_from(row.module_id).unwrap();
        assert!(unload_test::artifact_dir_exists(mid_u));

        unsafe {
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
        }

        assert_eq!(unload_test::count_modules_where_name(&name), 0);
        assert!(!unload_test::artifact_dir_exists(mid_u));
    }

    #[pg_test]
    fn load_policy_widen_allow_spi_is_denied() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("load_pol_{}", std::process::id());
        let bytes = decode_hex_bytes(LOAD_FIXTURE_COMPONENT_WASM_HEX);
        let err = load::load_impl(
            &name,
            pgrx::Json(serde_json::json!({ "bytes": bytes })),
            Some(pgrx::Json(serde_json::json!({
                "overrides": { "allow_spi": true }
            }))),
        )
        .expect_err("policy widen should be denied");

        match err {
            PgWasmError::PermissionDenied(msg) => {
                assert!(msg.contains("allow_spi"), "message: {msg}");
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }
    }
}

// TODO(wave-4): replace with lifecycle::load — temporary regress hook for core scalar path.
// Registered in the extension schema (`wasm` per pg_wasm.control); SQL calls `wasm._core_invoke_scalar`.
#[cfg(feature = "pg_test")]
mod core_invoke_regress {
    use pgrx::prelude::*;
    use wasmtime::Val;

    use crate::errors::PgWasmError;
    use crate::mapping::scalars;
    use crate::policy;
    use crate::runtime::core as runtime_core;
    use crate::runtime::engine;

    #[pg_extern]
    pub fn _core_invoke_scalar(bytes: &[u8], export: &str, i32args: Vec<i32>) -> i32 {
        match invoke_inner(bytes, export, &i32args) {
            Ok(v) => v,
            Err(err) => {
                ereport!(PgLogLevel::ERROR, err.sqlstate(), err.to_string());
                unreachable!("ereport should not return");
            }
        }
    }

    fn invoke_inner(bytes: &[u8], export: &str, i32args: &[i32]) -> Result<i32, PgWasmError> {
        let wasm_engine = engine::try_shared_engine()?;
        let loaded = runtime_core::compile(wasm_engine, bytes)?;
        let guc_snapshot = policy::GucSnapshot::from_gucs();
        let effective = policy::resolve(&guc_snapshot, None, None)?;
        let vals: Vec<Val> = scalars::i32_vec_to_vals(i32args);
        match runtime_core::invoke(&loaded, export, &vals, &effective)? {
            Val::I32(i) => Ok(i),
            other => Err(PgWasmError::Internal(format!(
                "core export returned non-i32 scalar: {other:?}"
            ))),
        }
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(all(test, feature = "pg_test"))]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
