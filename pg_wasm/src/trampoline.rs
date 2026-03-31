//! Single exported C symbol for all dynamically registered WASM UDFs.
//!
//! PostgreSQL `prosrc` must be [`TRAMPOLINE_PG_SYMBOL`] (not the `…_wrapper` suffix used by
//! `#[pg_extern]`), with a matching `pg_finfo_*` entry for the v1 call convention.

use pgrx::{pg_sys, prelude::*};

/// `CREATE FUNCTION … AS '$libdir/pg_wasm', '…'` link name for the trampoline body.
pub const TRAMPOLINE_PG_SYMBOL: &str = "pg_wasm_udf_trampoline";

#[unsafe(no_mangle)]
#[doc(hidden)]
pub extern "C" fn pg_finfo_pg_wasm_udf_trampoline() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

/// Entry point for every WASM-backed SQL function; dispatch uses `flinfo->fn_oid` and the registry.
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn pg_wasm_udf_trampoline(
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    unsafe { pgrx::pg_sys::ffi::pg_guard_ffi_boundary(|| dispatch_from_trampoline(fcinfo)) }
}

fn dispatch_from_trampoline(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    if fcinfo.is_null() {
        error!("pg_wasm: null fcinfo in trampoline");
    }
    let flinfo = unsafe { (*fcinfo).flinfo };
    if flinfo.is_null() {
        error!("pg_wasm: null flinfo in trampoline");
    }
    let oid = unsafe { (*flinfo).fn_oid };
    match crate::registry::lookup_by_fn_oid(oid) {
        Some(reg) => trampoline_invoke_wasm(reg, fcinfo),
        None => error!("pg_wasm: no wasm dispatch entry for function OID {}", oid),
    }
}

fn trampoline_invoke_wasm(reg: crate::registry::RegisteredFunction, fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    #[cfg(feature = "runtime_wasmtime")]
    {
        use pgrx::fcinfo::pg_getarg;
        let v: i32 = match reg.signature.args.len() {
            0 => match crate::runtime::wasmtime_backend::call_i32_arity0(
                reg.module_id,
                &reg.export_name,
            ) {
                Ok(v) => v,
                Err(e) => error!("pg_wasm: wasm invoke failed: {e}"),
            },
            2 => {
                let a = unsafe { pg_getarg::<i32>(fcinfo, 0) };
                let b = unsafe { pg_getarg::<i32>(fcinfo, 1) };
                match (a, b) {
                    (Some(a), Some(b)) => {
                        match crate::runtime::wasmtime_backend::call_i32_arity2(
                            reg.module_id,
                            &reg.export_name,
                            a,
                            b,
                        ) {
                            Ok(v) => v,
                            Err(e) => error!("pg_wasm: wasm invoke failed: {e}"),
                        }
                    }
                    _ => error!("pg_wasm: unexpected NULL in strict wasm function args"),
                }
            }
            n => error!("pg_wasm: unsupported WASM-backed function arity ({n} args)"),
        };
        v.into_datum()
            .expect("pg_wasm: trampoline int4 into_datum failed")
    }
    #[cfg(not(feature = "runtime_wasmtime"))]
    {
        let _ = (reg, fcinfo);
        error!("pg_wasm: built without a WebAssembly runtime (enable runtime_wasmtime)");
    }
}
