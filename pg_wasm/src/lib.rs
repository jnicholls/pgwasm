use pgrx::{JsonB, prelude::*};

mod abi;
mod composite_layout;
mod config;
mod guc;
mod load;
mod mapping;
mod metrics;
mod proc_reg;
mod registry;
mod runtime;
#[cfg(feature = "runtime-wasmtime")]
mod track_b_component_types;
mod trampoline;
mod views;

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
#[used]
static PG_WASM_MACOS_TEST_STUB_LINK: fn() = pg_wasm_macos_test_stub::ensure_linked;

::pgrx::pg_module_magic!(name, version);

#[cfg(not(any(feature = "runtime-wasmtime", feature = "runtime-extism")))]
compile_error!("pg_wasm: enable at least one runtime feature: runtime-wasmtime or runtime-extism");

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::init();
}

#[pg_extern(name = "pg_wasm_load")]
fn pg_wasm_load_bytea(
    wasm: &[u8],
    module_name: default!(Option<&str>, "NULL"),
    options: default!(Option<JsonB>, "NULL"),
) -> i64 {
    match load::load_from_bytes(wasm, module_name, options) {
        Ok(id) => id.0,
        Err(e) => error!("{e}"),
    }
}

#[pg_extern(name = "pg_wasm_load")]
fn pg_wasm_load_path(
    path: &str,
    module_name: default!(Option<&str>, "NULL"),
    options: default!(Option<JsonB>, "NULL"),
) -> i64 {
    let bytes = match load::resolve_path_and_read(path) {
        Ok(b) => b,
        Err(e) => error!("{e}"),
    };
    match load::load_from_bytes(&bytes, module_name, options) {
        Ok(id) => id.0,
        Err(e) => error!("{e}"),
    }
}

#[pg_extern]
fn pg_wasm_unload(module_id: i64) {
    if let Err(e) = load::unload_module(module_id) {
        error!("{e}");
    }
}

#[pg_extern]
fn pg_wasm_reconfigure_module(module_id: i64, options: Option<JsonB>) {
    if let Err(e) = load::reconfigure_module(module_id, options) {
        error!("{e}");
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::{JsonB, pg_sys::panic::CaughtError, prelude::*, spi::Spi};

    use crate::{
        abi::WasmAbiKind,
        config::{ModuleResourceLimits, PolicyOverrides},
        mapping::{ExportHintMap, ExportSignature},
        metrics,
        registry::{self, RegisteredFunction},
        runtime::{self, ModuleExecutionBackend, RuntimeKind, WasmRuntimeBackend},
    };

    fn caught_error_message(cause: CaughtError) -> String {
        match cause {
            CaughtError::PostgresError(e) | CaughtError::ErrorReport(e) => e.message().to_string(),
            CaughtError::RustPanic { ereport, .. } => ereport.message().to_string(),
        }
    }

    /// `CREATE FUNCTION` pointing at the trampoline, then registry + `SELECT` returns the placeholder.
    fn extension_schema_name() -> String {
        Spi::get_one::<String>(
            "SELECT n.nspname::text FROM pg_extension e \
             JOIN pg_namespace n ON n.oid = e.extnamespace \
             WHERE e.extname = 'pg_wasm'",
        )
        .expect("spi ext schema")
        .expect("pg_wasm extension schema")
    }

    fn wasm_fixture_hex_lower() -> String {
        wasm_bytes_hex_lower(include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm")))
    }

    fn wasm_echo_mem_hex_lower() -> String {
        wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/test_echo_mem.wasm"
        )))
    }

    fn wasm_spin_hex_lower() -> String {
        wasm_bytes_hex_lower(include_bytes!(concat!(env!("OUT_DIR"), "/test_spin.wasm")))
    }

    fn wasm_bytes_hex_lower(wasm: &[u8]) -> String {
        let mut s = String::with_capacity(wasm.len() * 2);
        for b in wasm {
            use std::fmt::Write;
            write!(&mut s, "{b:02x}").unwrap();
        }
        s
    }

    #[pg_test]
    fn test_pg_wasm_load_sql_defaults_omit_optional_args() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_fixture_hex_lower();
        let mid = Spi::get_one::<i64>(&format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea)",
        ))
        .expect("load spi")
        .expect("module id");
        let add = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.m{mid}_add(10, 20)"))
            .expect("add")
            .expect("non-null");
        assert_eq!(add, 30);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload");

        let mid2 = Spi::get_one::<i64>(&format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'defaults_partial'::text)",
        ))
        .expect("load with name only")
        .expect("module id");
        let add2 = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.defaults_partial_add(1, 1)"))
            .expect("add2")
            .expect("non-null");
        assert_eq!(add2, 2);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid2})")).expect("unload2");

        let dir = std::env::temp_dir().join(format!("pg_wasm_path_only_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir path-only");
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm"));
        std::fs::write(dir.join("add.wasm"), wasm).expect("write path-only fixture");
        let canon = dir.canonicalize().expect("canonicalize path-only");
        let mp = canon.to_string_lossy().replace('\'', "''");
        Spi::run(&format!("SET pg_wasm.module_path = '{mp}'")).expect("set module_path");
        Spi::run("SET pg_wasm.allow_load_from_file = on").expect("set allow_load");
        let mid3 =
            Spi::get_one::<i64>(&format!("SELECT {ext_nsp}.pg_wasm_load('add.wasm'::text)",))
                .expect("path load one arg")
                .expect("module id");
        let add3 = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.m{mid3}_add(3, 4)"))
            .expect("path add")
            .expect("non-null");
        assert_eq!(add3, 7);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid3})")).expect("unload3");
    }

    #[pg_test]
    fn test_pg_wasm_metrics_and_table_functions() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_fixture_hex_lower();
        let load_sql =
            format!("SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'met'::text)",);
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load spi")
            .expect("module id");
        let _ = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.met_add(2, 3)"))
            .expect("add")
            .expect("non-null");
        let _ = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.met_forty_two()"))
            .expect("42")
            .expect("non-null");

        let inv = Spi::get_one::<i64>(&format!(
            "SELECT total_invocations FROM {ext_nsp}.pg_wasm_modules() WHERE module_id = {}",
            mid
        ))
        .expect("modules spi")
        .expect("inv col");
        assert!(
            inv >= 2,
            "expected at least 2 wasm invocations recorded on this backend, got {inv}"
        );

        let fn_rows: i64 = Spi::get_one(&format!(
            "SELECT count(*)::bigint FROM {ext_nsp}.pg_wasm_functions() WHERE module_id = {}",
            mid
        ))
        .expect("fn count")
        .expect("cnt");
        assert!(fn_rows >= 2, "expected >= 2 pg_wasm_functions rows");

        let add_inv = Spi::get_one::<i64>(&format!(
            "SELECT invocations FROM {ext_nsp}.pg_wasm_stats() \
             WHERE module_id = {} AND wasm_export_name = 'add'",
            mid
        ))
        .expect("stats add")
        .expect("add inv");
        assert_eq!(add_inv, 1);

        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload");
    }

    #[pg_test]
    fn test_pg_wasm_load_bytea_invokes_exports() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_fixture_hex_lower();
        let load_sql =
            format!("SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'ld1'::text)",);
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load spi")
            .expect("module id");
        let add = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.ld1_add(1, 2)"))
            .expect("add")
            .expect("add non-null");
        assert_eq!(add, 3);
        let ft = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.ld1_forty_two()"))
            .expect("42")
            .expect("42 non-null");
        assert_eq!(ft, 42);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload");
    }

    #[pg_test]
    fn test_pg_wasm_load_bytea_echo_mem_exports() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_echo_mem_hex_lower();
        let opts = serde_json::json!({
            "exports": {
                "echo_mem": {
                    "args": ["bytea"],
                    "returns": "bytea"
                }
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'echo_pg'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load echo wasm")
            .expect("module id");
        let out = Spi::get_one::<Vec<u8>>(&format!(
            "SELECT {ext_nsp}.echo_pg_echo_mem('\\xdeadbeef'::bytea)"
        ))
        .expect("echo spi")
        .expect("echo non-null");
        assert_eq!(out, &[0xde, 0xad, 0xbe, 0xef]);

        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload echo");
    }

    #[pg_test]
    fn test_pg_wasm_load_jsonb_echo_mem_exports() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_echo_mem_hex_lower();
        let opts = serde_json::json!({
            "exports": {
                "echo_mem": {
                    "args": ["jsonb"],
                    "returns": "jsonb"
                }
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'ejson'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load echo json")
            .expect("module id");
        let j_out = Spi::get_one::<JsonB>(&format!(
            "SELECT {ext_nsp}.ejson_echo_mem('{{\"k\":42}}'::jsonb)"
        ))
        .expect("json echo")
        .expect("json");
        assert_eq!(j_out.0["k"], serde_json::json!(42));

        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload ejson");
    }

    #[pg_test]
    fn test_pg_wasm_load_from_path_relative() {
        let ext_nsp = extension_schema_name();
        let dir = std::env::temp_dir().join(format!("pg_wasm_modpath_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir modpath");
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm"));
        std::fs::write(dir.join("add.wasm"), wasm).expect("write fixture");
        let canon = dir.canonicalize().expect("canonicalize modpath");
        let mp = canon.to_string_lossy().replace('\'', "''");
        Spi::run(&format!("SET pg_wasm.module_path = '{mp}'")).expect("set module_path");
        Spi::run("SET pg_wasm.allow_load_from_file = on").expect("set allow_load");
        let load_sql = format!("SELECT {ext_nsp}.pg_wasm_load('add.wasm'::text, 'pmod'::text)");
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("path load")
            .expect("module id");
        let v = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.pmod_add(5, 7)"))
            .unwrap()
            .unwrap();
        assert_eq!(v, 12);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload path mod");
    }

    #[pg_test]
    fn test_dynamic_proc_is_extension_member() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_fixture_hex_lower();
        let load_sql =
            format!("SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'depwm'::text)",);
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load dep")
            .expect("mid");
        let oid = Spi::get_one::<pg_sys::Oid>(&format!(
            "SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid \
             WHERE n.nspname = '{ext_nsp}' AND p.proname = 'depwm_add' LIMIT 1",
        ))
        .expect("oid spi")
        .expect("depwm_add oid");
        let member = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_depend d \
             WHERE d.classid = 'pg_proc'::regclass AND d.objid = {oid}::oid \
             AND d.refclassid = 'pg_extension'::regclass \
             AND d.refobjid = (SELECT e.oid FROM pg_extension e WHERE e.extname = 'pg_wasm') \
             AND d.deptype = 'e'::\"char\")",
        ))
        .expect("spi pg_depend")
        .expect("dep membership row");
        assert!(
            member,
            "dynamic pg_proc should depend on pg_wasm extension (DROP EXTENSION)"
        );
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload dep");
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_trampoline_dispatch_via_sql_function() {
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm"));
        let mid = registry::alloc_module_id();
        runtime::wasmtime_backend::compile_store_and_list_exports(
            mid,
            wasm,
            &ExportHintMap::new(),
            WasmAbiKind::CoreWasm,
        )
        .expect("smoke compile");
        registry::record_module_policy_overrides(mid, PolicyOverrides::default());
        registry::record_module_resource_limits(mid, ModuleResourceLimits::default());
        registry::record_module_needs_wasi(mid, false);
        registry::record_module_execution_backend(mid, ModuleExecutionBackend::Wasmtime);
        registry::record_module_abi(mid, WasmAbiKind::CoreWasm);

        let create_sql = concat!(
            "CREATE OR REPLACE FUNCTION public.pg_wasm_trampoline_smoke() ",
            "RETURNS integer LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE ",
            "AS '$libdir/pg_wasm', 'pg_wasm_udf_trampoline'",
        );
        Spi::run(create_sql).expect("create pg_wasm_trampoline_smoke");

        let oid = Spi::get_one::<pg_sys::Oid>(
            "SELECT 'public.pg_wasm_trampoline_smoke()'::regprocedure::oid",
        )
        .expect("spi get oid")
        .expect("missing regprocedure oid");

        registry::register_fn_oid(
            oid,
            RegisteredFunction {
                module_id: mid,
                export_name: "forty_two".into(),
                signature: ExportSignature::default(),
                metrics: metrics::alloc_export_stats(),
            },
        );

        let v = Spi::get_one::<i32>("SELECT public.pg_wasm_trampoline_smoke()")
            .expect("spi select")
            .expect("null result");
        assert_eq!(v, 42);

        Spi::run("DROP FUNCTION public.pg_wasm_trampoline_smoke()")
            .expect("drop pg_wasm_trampoline_smoke");
        registry::unregister_fn_oid(oid);
        let _ = registry::take_module_abi(mid);
        registry::take_module_wasi_and_policy(mid);
        let _ = registry::take_module_execution_backend(mid);
        runtime::wasmtime_backend::remove_compiled_module(mid);
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_pg_wasm_load_component_add() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/test_add.component.wasm"
        )));
        let load_sql =
            format!("SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'cadd'::text)",);
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load component")
            .expect("mid");
        let v = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.cadd_add(3, 4)"))
            .expect("call component add")
            .expect("non-null");
        assert_eq!(v, 7);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload component");
    }

    /// `fixtures/marshal_matrix.wasm`: jsonb hints, `echo-point` / `echo-tuple` identity (arg + ret).
    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_marshal_matrix_jsonb_roundtrip() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/marshal_matrix.component.wasm"
        )));
        let opts = serde_json::json!({
            "exports": {
                "echo-point": { "args": ["jsonb"], "returns": "jsonb" },
                "echo-tuple": { "args": ["jsonb"], "returns": "jsonb" }
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'mmjb'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load marshal_matrix")
            .expect("mid");
        let j = Spi::get_one::<JsonB>(&format!(
            "SELECT {ext_nsp}.mmjb_echo_point('{{\"x\":3,\"y\":4}}'::jsonb)"
        ))
        .expect("echo-point")
        .expect("json");
        assert_eq!(j.0, serde_json::json!({"x": 3, "y": 4}));
        let j2 = Spi::get_one::<JsonB>(&format!(
            "SELECT {ext_nsp}.mmjb_echo_tuple('[10,20]'::jsonb)"
        ))
        .expect("echo-tuple")
        .expect("json2");
        assert_eq!(j2.0, serde_json::json!([10, 20]));
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload mmjb");
    }

    /// Track A: user `CREATE TYPE` + `{"kind":"composite","type":...}` hints; same semantics as jsonb path.
    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_marshal_matrix_track_a_composite() {
        let ext_nsp = extension_schema_name();
        let _ = Spi::run("DROP SCHEMA IF EXISTS mmtrac CASCADE");
        Spi::run("CREATE SCHEMA mmtrac").expect("mmtrac schema");
        Spi::run("CREATE TYPE mmtrac.point_t AS (x int, y int)").expect("point_t");
        Spi::run("CREATE TYPE mmtrac.pair_t AS (f1 int, f2 int)").expect("pair_t");

        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/marshal_matrix.component.wasm"
        )));
        let opts = serde_json::json!({
            "exports": {
                "echo-point": {
                    "args": [{ "kind": "composite", "type": "mmtrac.point_t" }],
                    "returns": { "kind": "composite", "type": "mmtrac.point_t" }
                },
                "echo-tuple": {
                    "args": [{ "kind": "composite", "type": "mmtrac.pair_t" }],
                    "returns": { "kind": "composite", "type": "mmtrac.pair_t" }
                }
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'mmta'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load track a")
            .expect("mid");
        let x = Spi::get_one::<i32>(&format!(
            "SELECT ({ext_nsp}.mmta_echo_point(ROW(11,22)::mmtrac.point_t)).x"
        ))
        .expect("x")
        .expect("non-null");
        assert_eq!(x, 11);
        let y = Spi::get_one::<i32>(&format!(
            "SELECT ({ext_nsp}.mmta_echo_point(ROW(11,22)::mmtrac.point_t)).y"
        ))
        .expect("y")
        .expect("non-null");
        assert_eq!(y, 22);
        let f1 = Spi::get_one::<i32>(&format!(
            "SELECT ({ext_nsp}.mmta_echo_tuple(ROW(7,8)::mmtrac.pair_t)).f1"
        ))
        .expect("f1")
        .expect("nn");
        assert_eq!(f1, 7);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload mmta");
        let _ = Spi::run("DROP SCHEMA IF EXISTS mmtrac CASCADE");
    }

    /// Track A negative: composite attribute layout does not match WIT record.
    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_marshal_matrix_track_a_composite_rejected_on_mismatch() {
        let ext_nsp = extension_schema_name();
        let _ = Spi::run("DROP SCHEMA IF EXISTS mmneg CASCADE");
        Spi::run("CREATE SCHEMA mmneg").expect("mmneg");
        Spi::run("CREATE TYPE mmneg.bad_point AS (only_col int)").expect("bad_point");

        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/marshal_matrix.component.wasm"
        )));
        let opts = serde_json::json!({
            "exports": {
                "echo-point": {
                    "args": [{ "kind": "composite", "type": "mmneg.bad_point" }],
                    "returns": { "kind": "composite", "type": "mmneg.bad_point" }
                }
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'mmng'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let msg = PgTryBuilder::new(|| match Spi::get_one::<i64>(&load_sql) {
            Err(e) => format!("{e}"),
            Ok(Some(_)) => "__unexpected_ok__".to_string(),
            Ok(None) => "__unexpected_null__".to_string(),
        })
        .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, caught_error_message)
        .execute();
        assert!(
            msg != "__unexpected_ok__" && msg != "__unexpected_null__",
            "expected load failure, got {msg:?}"
        );
        assert!(
            msg.contains("composite") || msg.contains("attribute") || msg.contains("field"),
            "unexpected error: {msg}"
        );
        let _ = Spi::run("DROP SCHEMA IF EXISTS mmneg CASCADE");
    }

    /// Track B: `pg_wasm.auto_create_component_types` creates extension-schema composites; unload drops them.
    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_marshal_matrix_track_b_auto_composite_lifecycle() {
        let ext_nsp = extension_schema_name();
        Spi::run("SET pg_wasm.auto_create_component_types = on").expect("guc on");

        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/marshal_matrix.component.wasm"
        )));
        let load_sql =
            format!("SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'mmtb'::text)",);
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load track b")
            .expect("mid");

        let cnt_before: i64 = Spi::get_one(&format!(
            "SELECT count(*)::bigint FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = '{ext_nsp}' AND t.typtype = 'c' AND t.typname LIKE 'wct_m{mid}_%'"
        ))
        .expect("count types")
        .expect("cnt");
        assert!(
            cnt_before >= 1,
            "expected auto-generated composite types, got {cnt_before}"
        );

        let fq: String = Spi::get_one(&format!(
            "SELECT (quote_ident(n.nspname) || '.' || quote_ident(t.typname))::text \
             FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             JOIN pg_catalog.pg_attribute ax ON ax.attrelid = t.typrelid AND ax.attname = 'x' \
                 AND ax.attnum > 0 AND NOT ax.attisdropped \
             JOIN pg_catalog.pg_attribute ay ON ay.attrelid = t.typrelid AND ay.attname = 'y' \
                 AND ay.attnum > 0 AND NOT ay.attisdropped \
             WHERE n.nspname = '{ext_nsp}' AND t.typname LIKE 'wct_m{mid}_%' \
             ORDER BY t.oid DESC LIMIT 1"
        ))
        .expect("fq type")
        .expect("row");

        let x = Spi::get_one::<i32>(&format!(
            "SELECT ({ext_nsp}.mmtb_echo_point(ROW(30,40)::{fq})).x"
        ))
        .expect("call tb")
        .expect("x");
        assert_eq!(x, 30);

        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload mmtb");

        let cnt_after: i64 = Spi::get_one(&format!(
            "SELECT count(*)::bigint FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
             WHERE n.nspname = '{ext_nsp}' AND t.typtype = 'c' AND t.typname LIKE 'wct_m{mid}_%'"
        ))
        .expect("count after")
        .expect("cnt2");
        assert_eq!(cnt_after, 0, "Track B types should be dropped on unload");

        Spi::run("SET pg_wasm.auto_create_component_types = off").expect("guc off");
    }

    #[pg_test]
    fn test_pg_wasm_lifecycle_hooks() {
        let ext_nsp = extension_schema_name();
        let hex =
            wasm_bytes_hex_lower(include_bytes!(concat!(env!("OUT_DIR"), "/test_hooks.wasm")));
        let opts = serde_json::json!({
            "hooks": {
                "on_load": "wasm_nop",
                "on_unload": "wasm_nop",
                "on_reconfigure": "wasm_rc",
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'hk'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load hooks wasm")
            .expect("module id");
        let v = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.hk_add(2, 3)"))
            .expect("add")
            .expect("non-null");
        assert_eq!(v, 5);
        Spi::run(&format!(
            "SELECT {ext_nsp}.pg_wasm_reconfigure_module({mid}, '{{\"allow_env\": false}}'::jsonb)"
        ))
        .expect("reconfigure with hook");
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload hooks mod");
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_wasmtime_backend_instantiates() {
        runtime::wasmtime_backend::with_backend(|b| {
            assert_eq!(b.kind(), RuntimeKind::Wasmtime);
        });
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_wasm_load_wasi_rejected_without_allow_wasi_guc() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/test_wasi_fd_write.wasm"
        )));
        Spi::run("SET pg_wasm.allow_wasi = off").expect("guc");
        let load_sql =
            format!("SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'wasi_x'::text)",);
        let msg = PgTryBuilder::new(|| match Spi::get_one::<i64>(&load_sql) {
            Err(e) => format!("{e}"),
            Ok(Some(_)) => "__unexpected_ok__".to_string(),
            Ok(None) => "__unexpected_null__".to_string(),
        })
        .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, caught_error_message)
        .execute();
        assert!(
            msg != "__unexpected_ok__" && msg != "__unexpected_null__",
            "load should have failed, got {msg:?}"
        );
        assert!(
            msg.contains("WASI") || msg.contains("wasi"),
            "unexpected error: {msg}"
        );
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_wasm_load_wasi_succeeds_when_allowed() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/test_wasi_fd_write.wasm"
        )));
        Spi::run("SET pg_wasm.allow_wasi = on").expect("guc on");
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'wasi_ok'::text)",
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load wasi wasm")
            .expect("module id");
        let v = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.wasi_ok_forty_two()"))
            .expect("call")
            .expect("non-null");
        assert_eq!(v, 42);
        Spi::run(&format!(
            "SELECT {ext_nsp}.pg_wasm_reconfigure_module({mid}, '{{\"allow_wasi\": true}}'::jsonb)"
        ))
        .expect("reconfigure no-op narrow");
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload wasi");
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_resource_fuel_limits_infinite_loop() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_spin_hex_lower();
        let opts = serde_json::json!({ "fuel": 8000 }).to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'sp'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load spin")
            .expect("mid");
        let msg = PgTryBuilder::new(|| {
            match Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.sp_spin()")) {
                Err(e) => format!("{e}"),
                Ok(Some(_)) => "__unexpected_ok__".to_string(),
                Ok(None) => "__unexpected_null__".to_string(),
            }
        })
        .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, caught_error_message)
        .execute();
        assert!(
            msg.contains("fuel")
                || msg.contains("trap")
                || msg.contains("wasm")
                || msg.contains("Fuel"),
            "expected fuel/trap style error, got {msg:?}"
        );
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload spin");
    }

    #[cfg(all(feature = "runtime-extism", feature = "runtime-wasmtime"))]
    #[pg_test]
    fn test_extism_abi_override_fuel_limits_spin() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_spin_hex_lower();
        let opts = serde_json::json!({
            "abi": "extism",
            "fuel": 8000,
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'exsp'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load spin as extism abi")
            .expect("mid");
        let msg = PgTryBuilder::new(|| {
            match Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.exsp_spin()")) {
                Err(e) => format!("{e}"),
                Ok(Some(_)) => "__unexpected_ok__".to_string(),
                Ok(None) => "__unexpected_null__".to_string(),
            }
        })
        .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, caught_error_message)
        .execute();
        assert!(
            msg.contains("fuel")
                || msg.contains("Fuel")
                || msg.contains("trap")
                || msg.contains("wasm")
                || msg.contains("plugin ran out of fuel"),
            "expected fuel/trap style error, got {msg:?}"
        );
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload exspin");
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_resource_memory_cap_blocks_host_grow() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_echo_mem_hex_lower();
        let opts = serde_json::json!({
            "max_memory_pages": 1,
            "exports": {
                "echo_mem": {
                    "args": ["bytea"],
                    "returns": "bytea"
                }
            }
        })
        .to_string();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'lowmem'::text, '{}'::jsonb)",
            opts.replace('\'', "''"),
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("load echo low mem")
            .expect("mid");
        let msg = PgTryBuilder::new(|| {
            match Spi::get_one::<Vec<u8>>(&format!(
                "SELECT {ext_nsp}.lowmem_echo_mem('\\x01'::bytea)"
            )) {
                Err(e) => format!("{e}"),
                Ok(Some(_)) => "__unexpected_ok__".to_string(),
                Ok(None) => "__unexpected_null__".to_string(),
            }
        })
        .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, caught_error_message)
        .execute();
        assert!(
            msg.contains("memory")
                || msg.contains("grow")
                || msg.contains("limit")
                || msg.contains("wasm")
                || msg.contains("Memory"),
            "expected memory limit error, got {msg:?}"
        );
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload lowmem");
    }

    #[cfg(feature = "runtime-wasmtime")]
    #[pg_test]
    fn test_reconfigure_rejects_revoking_wasi_for_wasi_module() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_bytes_hex_lower(include_bytes!(concat!(
            env!("OUT_DIR"),
            "/test_wasi_fd_write.wasm"
        )));
        Spi::run("SET pg_wasm.allow_wasi = on").expect("guc on");
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'wasi_rc'::text)",
        );
        let mid = Spi::get_one::<i64>(&load_sql).expect("load").expect("mid");
        let rc_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_reconfigure_module({mid}, '{{\"allow_wasi\": false}}'::jsonb)",
        );
        let msg = PgTryBuilder::new(|| match Spi::run(&rc_sql) {
            Err(e) => format!("{e}"),
            Ok(()) => "__unexpected_ok__".to_string(),
        })
        .catch_when(PgSqlErrorCode::ERRCODE_INTERNAL_ERROR, caught_error_message)
        .execute();
        assert_ne!(
            msg, "__unexpected_ok__",
            "reconfigure should have failed, got ok"
        );
        assert!(
            msg.contains("WASI") || msg.contains("policy"),
            "unexpected error: {msg}"
        );
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload");
    }
}

/// Required by `cargo pgrx test`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}

#[cfg(test)]
mod rust_tests {
    use pgrx::pg_sys;

    use crate::{config::LoadOptions, registry, trampoline};

    #[test]
    fn trampoline_link_symbol_is_pg_wasm_udf_trampoline() {
        assert_eq!(trampoline::TRAMPOLINE_PG_SYMBOL, "pg_wasm_udf_trampoline");
    }

    #[test]
    fn registry_lookup_miss_for_invalid_oid() {
        assert!(registry::lookup_by_fn_oid(pg_sys::InvalidOid).is_none());
    }

    #[test]
    fn load_options_parse_hooks_object() {
        let j = serde_json::json!({
            "hooks": {
                "on_load": "a",
                "on_unload": "b",
                "on_reconfigure": "c",
            }
        });
        let o = LoadOptions::from_jsonb(Some(pgrx::JsonB(j)));
        assert_eq!(o.hook_on_load.as_deref(), Some("a"));
        assert_eq!(o.hook_on_unload.as_deref(), Some("b"));
        assert_eq!(o.hook_on_reconfigure.as_deref(), Some("c"));
    }

    #[test]
    fn load_options_parse_resource_limits() {
        let j = serde_json::json!({
            "max_memory_pages": 128,
            "fuel": 999_999
        });
        let o = LoadOptions::from_jsonb(Some(pgrx::JsonB(j)));
        assert_eq!(o.resource_limits.max_memory_pages, Some(128));
        assert_eq!(o.resource_limits.fuel, Some(999_999));
    }
}

/// Fuel exhaustion under the same engine settings as `WasmtimeBackend`, outside PostgreSQL.
#[cfg(all(test, feature = "runtime-wasmtime"))]
mod wasmtime_trap_smoke {
    use wasmtime::{Config, Engine, Instance, Module, Store, StoreLimitsBuilder};

    fn engine_fuel() -> Engine {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_backtrace_max_frames(None);
        config.signals_based_traps(false);
        unsafe {
            config.cranelift_flag_set("enable_heap_access_spectre_mitigation", "false");
            config.cranelift_flag_set("enable_table_access_spectre_mitigation", "false");
        }
        Engine::new(&config).expect("engine")
    }

    #[test]
    fn spin_trapped_as_fuel_error_not_abort() {
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/test_spin.wasm"));
        let engine = engine_fuel();
        let module = Module::new(&engine, wasm).expect("module");
        let limits = StoreLimitsBuilder::new().memory_size(2 * 65536).build();
        let mut store = Store::new(&engine, limits);
        store.limiter(|s| s);
        store.set_fuel(8000).expect("fuel");
        let instance = Instance::new(&mut store, &module, &[]).expect("instance");
        let f = instance
            .get_typed_func::<(), i32>(&mut store, "spin")
            .expect("spin");
        let err = f.call(&mut store, ()).expect_err("expected trap");
        let trap = err
            .downcast_ref::<wasmtime::Trap>()
            .copied()
            .unwrap_or_else(|| {
                panic!(
                    "expected Trap root (Display={err:?} root={:?})",
                    err.root_cause()
                )
            });
        assert_eq!(trap, wasmtime::Trap::OutOfFuel, "full error: {err:#}");
    }
}
