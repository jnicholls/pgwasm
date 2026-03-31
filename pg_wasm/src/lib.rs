use pgrx::prelude::*;

#[cfg(feature = "runtime_wasmtime")]
use pgrx::JsonB;

mod config;
#[cfg(feature = "runtime_wasmtime")]
mod guc;
mod mapping;
mod proc_reg;
mod registry;
mod runtime;
mod trampoline;

#[cfg(feature = "runtime_wasmtime")]
mod load;

pub use config::{HostPolicy, LoadOptions};
pub use mapping::{ExportSignature, PgWasmArgDesc, PgWasmReturnDesc, PgWasmTypeKind};
pub use registry::{
    ModuleId, RegisteredFunction, lookup_by_fn_oid, register_fn_oid, unregister_fn_oid,
};
pub use runtime::{RuntimeKind, StubWasmBackend, WasmRuntimeBackend};
pub use proc_reg::{RegisterError, drop_wasm_trampoline_proc, register_wasm_trampoline_proc};
pub use trampoline::TRAMPOLINE_PG_SYMBOL;

#[cfg(feature = "runtime_extism")]
pub use runtime::extism_backend::ExtismBackend;
#[cfg(feature = "runtime_wasmer")]
pub use runtime::wasmer_backend::WasmerBackend;
#[cfg(feature = "runtime_wasmtime")]
pub use runtime::wasmtime_backend::WasmtimeBackend;

::pgrx::pg_module_magic!(name, version);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    #[cfg(feature = "runtime_wasmtime")]
    guc::init();
}

#[pg_extern]
fn hello_pg_wasm() -> &'static str {
    "Hello, pg_wasm"
}

#[cfg(feature = "runtime_wasmtime")]
#[pg_extern(name = "pg_wasm_load")]
fn pg_wasm_load_bytea(
    wasm: &[u8],
    module_name: Option<&str>,
    options: Option<JsonB>,
) -> i64 {
    match crate::load::load_from_bytes(wasm, module_name, options) {
        Ok(id) => id.0,
        Err(e) => error!("{e}"),
    }
}

#[cfg(feature = "runtime_wasmtime")]
#[pg_extern(name = "pg_wasm_load")]
fn pg_wasm_load_path(path: &str, module_name: Option<&str>, options: Option<JsonB>) -> i64 {
    let bytes = match crate::load::resolve_path_and_read(path) {
        Ok(b) => b,
        Err(e) => error!("{e}"),
    };
    match crate::load::load_from_bytes(&bytes, module_name, options) {
        Ok(id) => id.0,
        Err(e) => error!("{e}"),
    }
}

#[cfg(feature = "runtime_wasmtime")]
#[pg_extern]
fn pg_wasm_unload(module_id: i64) {
    if let Err(e) = crate::load::unload_module(module_id) {
        error!("{e}");
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::{prelude::*, spi::Spi};

    use crate::{
        mapping::ExportSignature,
        registry::{self, RegisteredFunction, register_fn_oid},
    };

    #[pg_test]
    fn test_hello_pg_wasm() {
        assert_eq!("Hello, pg_wasm", crate::hello_pg_wasm());
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

    #[cfg(feature = "runtime_wasmtime")]
    fn wasm_fixture_hex_lower() -> String {
        const W: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm"));
        let mut s = String::with_capacity(W.len() * 2);
        for b in W {
            use std::fmt::Write;
            write!(&mut s, "{b:02x}").unwrap();
        }
        s
    }

    #[cfg(feature = "runtime_wasmtime")]
    #[pg_test]
    fn test_pg_wasm_load_bytea_invokes_exports() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_fixture_hex_lower();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'ld1'::text, NULL::jsonb)",
        );
        let mid = Spi::get_one::<i64>(&load_sql).expect("load spi").expect("module id");
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

    #[cfg(feature = "runtime_wasmtime")]
    #[pg_test]
    fn test_pg_wasm_load_from_path_relative() {
        let ext_nsp = extension_schema_name();
        let dir =
            std::env::temp_dir().join(format!("pg_wasm_modpath_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("mkdir modpath");
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm"));
        std::fs::write(dir.join("add.wasm"), wasm).expect("write fixture");
        let canon = dir.canonicalize().expect("canonicalize modpath");
        let mp = canon.to_string_lossy().replace('\'', "''");
        Spi::run(&format!("SET pg_wasm.module_path = '{mp}'")).expect("set module_path");
        Spi::run("SET pg_wasm.allow_load_from_file = on").expect("set allow_load");
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load('add.wasm'::text, 'pmod'::text, NULL::jsonb)",
        );
        let mid = Spi::get_one::<i64>(&load_sql)
            .expect("path load")
            .expect("module id");
        let v = Spi::get_one::<i32>(&format!("SELECT {ext_nsp}.pmod_add(5, 7)"))
            .unwrap()
            .unwrap();
        assert_eq!(v, 12);
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload path mod");
    }

    #[cfg(feature = "runtime_wasmtime")]
    #[pg_test]
    fn test_dynamic_proc_is_extension_member() {
        let ext_nsp = extension_schema_name();
        let hex = wasm_fixture_hex_lower();
        let load_sql = format!(
            "SELECT {ext_nsp}.pg_wasm_load(decode('{hex}','hex')::bytea, 'depwm'::text, NULL::jsonb)",
        );
        let mid = Spi::get_one::<i64>(&load_sql).expect("load dep").expect("mid");
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
        assert!(member, "dynamic pg_proc should depend on pg_wasm extension (DROP EXTENSION)");
        Spi::run(&format!("SELECT {ext_nsp}.pg_wasm_unload({mid})")).expect("unload dep");
    }

    #[cfg(feature = "runtime_wasmtime")]
    #[pg_test]
    fn test_trampoline_dispatch_via_sql_function() {
        let wasm = include_bytes!(concat!(env!("OUT_DIR"), "/test_add.wasm"));
        let mid = registry::alloc_module_id();
        crate::runtime::wasmtime_backend::compile_store_and_list_exports(mid, wasm)
            .expect("smoke compile");

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

        register_fn_oid(
            oid,
            RegisteredFunction {
                module_id: mid,
                export_name: "forty_two".into(),
                signature: ExportSignature::default(),
            },
        );

        let v = Spi::get_one::<i32>("SELECT public.pg_wasm_trampoline_smoke()")
            .expect("spi select")
            .expect("null result");
        assert_eq!(v, 42);

        Spi::run("DROP FUNCTION public.pg_wasm_trampoline_smoke()")
            .expect("drop pg_wasm_trampoline_smoke");
        crate::unregister_fn_oid(oid);
        crate::runtime::wasmtime_backend::remove_compiled_module(mid);
    }

    #[cfg(feature = "runtime_wasmtime")]
    #[pg_test]
    fn test_wasmtime_backend_instantiates() {
        use crate::{RuntimeKind, WasmRuntimeBackend};

        crate::runtime::wasmtime_backend::with_backend(|b| {
            assert_eq!(b.kind(), RuntimeKind::Wasmtime);
        });
    }
}

/// Required by `cargo pgrx test`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}

#[cfg(test)]
mod rust_tests {
    use pgrx::pg_sys;

    #[test]
    fn trampoline_link_symbol_is_pg_wasm_udf_trampoline() {
        assert_eq!(crate::TRAMPOLINE_PG_SYMBOL, "pg_wasm_udf_trampoline");
    }

    #[test]
    fn registry_lookup_miss_for_invalid_oid() {
        assert!(crate::lookup_by_fn_oid(pg_sys::InvalidOid).is_none());
    }
}
