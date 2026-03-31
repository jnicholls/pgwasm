//! Single exported C symbol for all dynamically registered WASM UDFs.
//!
//! PostgreSQL `prosrc` must be [`TRAMPOLINE_PG_SYMBOL`] (not the `…_wrapper` suffix used by
//! `#[pg_extern]`), with a matching `pg_finfo_*` entry for the v1 call convention.

use pgrx::{fcinfo::pg_getarg, pg_sys, prelude::*, JsonB};

use crate::mapping::{ExportSignature, PgWasmReturnDesc, PgWasmTypeKind};

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

fn uses_buffer_io(sig: &ExportSignature) -> bool {
    let buf_ret = matches!(
        sig.ret.kind,
        PgWasmTypeKind::String | PgWasmTypeKind::Bytes
    );
    if !buf_ret {
        return false;
    }
    if sig.args.len() > 1 {
        return false;
    }
    sig.args.is_empty()
        || matches!(
            sig.args[0].kind,
            PgWasmTypeKind::String | PgWasmTypeKind::Bytes
        )
}

fn trampoline_invoke_wasm(reg: crate::registry::RegisteredFunction, fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    #[cfg(feature = "runtime_wasmtime")]
    {
        let sig = &reg.signature;
        if uses_buffer_io(sig) {
            invoke_buffer_io(&reg, fcinfo)
        } else {
            invoke_scalar(&reg, fcinfo)
        }
    }
    #[cfg(not(feature = "runtime_wasmtime"))]
    {
        let _ = (reg, fcinfo);
        error!("pg_wasm: built without a WebAssembly runtime (enable runtime_wasmtime)");
    }
}

#[cfg(feature = "runtime_wasmtime")]
fn invoke_buffer_io(
    reg: &crate::registry::RegisteredFunction,
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    let input: Vec<u8> = match reg.signature.args.len() {
        0 => Vec::new(),
        1 => {
            let desc = &reg.signature.args[0];
            match (desc.pg_oid, desc.kind) {
                (pg_sys::TEXTOID, PgWasmTypeKind::String) => {
                    let s =
                        unsafe { pg_getarg::<String>(fcinfo, 0) }.expect("pg_wasm: NULL text arg");
                    s.into_bytes()
                }
                (_, PgWasmTypeKind::Bytes) if desc.pg_oid == pg_sys::JSONBOID => {
                    let j =
                        unsafe { pg_getarg::<JsonB>(fcinfo, 0) }.expect("pg_wasm: NULL jsonb arg");
                    serde_json::to_vec(&j.0)
                        .unwrap_or_else(|e| error!("pg_wasm: jsonb encode: {e}"))
                }
                (_, PgWasmTypeKind::Bytes) => unsafe { pg_getarg::<Vec<u8>>(fcinfo, 0) }
                    .expect("pg_wasm: NULL bytea arg"),
                _ => error!("pg_wasm: unsupported buffer arg combination"),
            }
        }
        _ => error!("pg_wasm: invalid buffer signature"),
    };
    let out = match crate::runtime::wasmtime_backend::call_mem_in_out(
        reg.module_id,
        &reg.export_name,
        &input,
    ) {
        Ok(b) => b,
        Err(e) => error!("pg_wasm: wasm invoke failed: {e}"),
    };
    buffer_output_datum(&reg.signature.ret, &out)
}

#[cfg(feature = "runtime_wasmtime")]
fn buffer_output_datum(ret: &PgWasmReturnDesc, out: &[u8]) -> pg_sys::Datum {
    match (ret.pg_oid, ret.kind) {
        (pg_sys::TEXTOID, PgWasmTypeKind::String) => {
            let s = String::from_utf8(out.to_vec()).unwrap_or_else(|e| {
                error!("pg_wasm: wasm output is not valid UTF-8: {e}");
            });
            s.into_datum()
                .unwrap_or_else(|| error!("pg_wasm: text into_datum failed"))
        }
        (_, PgWasmTypeKind::Bytes) if ret.pg_oid == pg_sys::JSONBOID => {
            let v: serde_json::Value = serde_json::from_slice(out).unwrap_or_else(|e| {
                error!("pg_wasm: wasm jsonb output invalid: {e}");
            });
            JsonB(v)
                .into_datum()
                .unwrap_or_else(|| error!("pg_wasm: jsonb into_datum failed"))
        }
        (_, PgWasmTypeKind::Bytes) => out
            .into_datum()
            .unwrap_or_else(|| error!("pg_wasm: bytea into_datum failed")),
        _ => error!("pg_wasm: unsupported buffer return type"),
    }
}

#[cfg(feature = "runtime_wasmtime")]
fn invoke_scalar(reg: &crate::registry::RegisteredFunction, fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    use crate::runtime::wasmtime_backend::{
        call_f32_arity0, call_f32_arity1, call_f32_arity2, call_f64_arity0, call_f64_arity1,
        call_f64_arity2, call_i32_arity0, call_i32_arity1, call_i32_arity2, call_i64_arity0,
        call_i64_arity1,
    };
    let mid = reg.module_id;
    let name = reg.export_name.as_str();
    let sig = &reg.signature;

    match (
        sig.args.as_slice(),
        sig.ret.pg_oid,
        sig.ret.kind,
    ) {
        ([], _, PgWasmTypeKind::I32) if sig.ret.pg_oid == pg_sys::INT4OID => {
            let v = call_i32_arity0(mid, name).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
            v.into_datum()
                .unwrap_or_else(|| error!("pg_wasm: int4 into_datum failed"))
        }
        ([], _, PgWasmTypeKind::I32) if sig.ret.pg_oid == pg_sys::INT2OID => {
            let v = call_i32_arity0(mid, name).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
            (v as i16).into_datum()
                .unwrap_or_else(|| error!("pg_wasm: int2 into_datum failed"))
        }
        ([], _, PgWasmTypeKind::Bool) => {
            let v = call_i32_arity0(mid, name).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
            (v != 0).into_datum()
                .unwrap_or_else(|| error!("pg_wasm: bool into_datum failed"))
        }
        ([], _, PgWasmTypeKind::I64) => {
            let v = call_i64_arity0(mid, name).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
            v.into_datum()
                .unwrap_or_else(|| error!("pg_wasm: int8 into_datum failed"))
        }
        ([], _, PgWasmTypeKind::F32) => {
            let v = call_f32_arity0(mid, name).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
            v.into_datum()
                .unwrap_or_else(|| error!("pg_wasm: float4 into_datum failed"))
        }
        ([], _, PgWasmTypeKind::F64) => {
            let v = call_f64_arity0(mid, name).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
            v.into_datum()
                .unwrap_or_else(|| error!("pg_wasm: float8 into_datum failed"))
        }

        ([a], _, _) => {
            match (
                a.pg_oid,
                a.kind,
                sig.ret.pg_oid,
                sig.ret.kind,
            ) {
                (pg_sys::INT4OID, PgWasmTypeKind::I32, pg_sys::INT4OID, PgWasmTypeKind::I32) => {
                    let x = unsafe { pg_getarg::<i32>(fcinfo, 0) }
                        .expect("pg_wasm: NULL strict arg");
                    let v = call_i32_arity1(mid, name, x).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: int4 into_datum failed"))
                }
                (pg_sys::INT2OID, PgWasmTypeKind::I32, pg_sys::INT2OID, PgWasmTypeKind::I32) => {
                    let x = unsafe { pg_getarg::<i16>(fcinfo, 0) }
                        .expect("pg_wasm: NULL strict arg");
                    let v = call_i32_arity1(mid, name, x as i32)
                        .unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    (v as i16).into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: int2 into_datum failed"))
                }
                (_, PgWasmTypeKind::Bool, _, PgWasmTypeKind::Bool) => {
                    let x = unsafe { pg_getarg::<bool>(fcinfo, 0) }
                        .expect("pg_wasm: NULL strict arg");
                    let v = call_i32_arity1(mid, name, if x { 1 } else { 0 })
                        .unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    (v != 0).into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: bool into_datum failed"))
                }
                (_, PgWasmTypeKind::I64, _, PgWasmTypeKind::I64) => {
                    let x = unsafe { pg_getarg::<i64>(fcinfo, 0) }
                        .expect("pg_wasm: NULL strict arg");
                    let v = call_i64_arity1(mid, name, x).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: int8 into_datum failed"))
                }
                (_, PgWasmTypeKind::F32, _, PgWasmTypeKind::F32) => {
                    let x = unsafe { pg_getarg::<f32>(fcinfo, 0) }
                        .expect("pg_wasm: NULL strict arg");
                    let v = call_f32_arity1(mid, name, x).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: float4 into_datum failed"))
                }
                (_, PgWasmTypeKind::F64, _, PgWasmTypeKind::F64) => {
                    let x = unsafe { pg_getarg::<f64>(fcinfo, 0) }
                        .expect("pg_wasm: NULL strict arg");
                    let v = call_f64_arity1(mid, name, x).unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: float8 into_datum failed"))
                }
                _ => error!(
                    "pg_wasm: unsupported 1-arg wasm signature (OID {:?} / {:?})",
                    (a.pg_oid, a.kind),
                    (sig.ret.pg_oid, sig.ret.kind)
                ),
            }
        }

        ([a, b], _, _) => {
            match (a.kind, b.kind, sig.ret.kind) {
                (PgWasmTypeKind::I32, PgWasmTypeKind::I32, PgWasmTypeKind::I32)
                    if a.pg_oid == pg_sys::INT4OID
                        && b.pg_oid == pg_sys::INT4OID
                        && sig.ret.pg_oid == pg_sys::INT4OID =>
                {
                    let x = unsafe { pg_getarg::<i32>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                    let y = unsafe { pg_getarg::<i32>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                    let v = call_i32_arity2(mid, name, x, y)
                        .unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: int4 into_datum failed"))
                }
                (PgWasmTypeKind::Bool, PgWasmTypeKind::Bool, PgWasmTypeKind::Bool) => {
                    let x = unsafe { pg_getarg::<bool>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                    let y = unsafe { pg_getarg::<bool>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                    let v = call_i32_arity2(
                        mid,
                        name,
                        if x { 1 } else { 0 },
                        if y { 1 } else { 0 },
                    )
                    .unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    (v != 0).into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: bool into_datum failed"))
                }
                (PgWasmTypeKind::F32, PgWasmTypeKind::F32, PgWasmTypeKind::F32) => {
                    let x = unsafe { pg_getarg::<f32>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                    let y = unsafe { pg_getarg::<f32>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                    let v = call_f32_arity2(mid, name, x, y)
                        .unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: float4 into_datum failed"))
                }
                (PgWasmTypeKind::F64, PgWasmTypeKind::F64, PgWasmTypeKind::F64) => {
                    let x = unsafe { pg_getarg::<f64>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                    let y = unsafe { pg_getarg::<f64>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                    let v = call_f64_arity2(mid, name, x, y)
                        .unwrap_or_else(|e| error!("pg_wasm: wasm: {e}"));
                    v.into_datum()
                        .unwrap_or_else(|| error!("pg_wasm: float8 into_datum failed"))
                }
                _ => error!("pg_wasm: unsupported 2-arg wasm signature"),
            }
        }

        _ => error!(
            "pg_wasm: unsupported WASM-backed function ({} args)",
            sig.args.len()
        ),
    }
}
