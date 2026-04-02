//! Single exported C symbol for all dynamically registered WASM UDFs.
//!
//! PostgreSQL `prosrc` must be [`TRAMPOLINE_PG_SYMBOL`] (not the `…_wrapper` suffix used by
//! `#[pg_extern]`), with a matching `pg_finfo_*` entry for the v1 call convention.

use pgrx::{JsonB, fcinfo::pg_getarg, pg_sys, prelude::*};

use crate::mapping::{ExportSignature, PgWasmReturnDesc, PgWasmTypeKind};

#[cfg(feature = "_pg_wasm_runtime")]
use crate::registry::RegisteredFunction;

/// `CREATE FUNCTION … AS '$libdir/pg_wasm', '…'` link name for the trampoline body.
pub const TRAMPOLINE_PG_SYMBOL: &str = "pg_wasm_udf_trampoline";

#[unsafe(no_mangle)]
#[doc(hidden)]
pub extern "C" fn pg_finfo_pg_wasm_udf_trampoline() -> &'static pg_sys::Pg_finfo_record {
    const V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

/// Entry point for every WASM-backed SQL function; dispatch uses `flinfo->fn_oid` and the registry.
#[cfg(feature = "_pg_wasm_runtime")]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn pg_wasm_udf_trampoline(
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    // Match `#[pg_extern]`: `error!` raises a Rust panic that must be caught by
    // `pgrx_extern_c_guard` and turned into a Postgres `ereport` via `do_ereport`.
    // Nesting `pg_guard_ffi_boundary` around the same paths breaks that unwinding and
    // can SIGABRT the backend when wasm traps.
    unsafe {
        pgrx::pg_sys::panic::pgrx_extern_c_guard(|| {
            let prepared = prepare_wasm_trampoline(fcinfo);
            let t0 = crate::metrics::timer_start();
            let wasm_result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_wasm_isolated(&prepared)
            })) {
                Ok(r) => r,
                Err(payload) => Err(panic_payload_to_string(payload)),
            };
            match wasm_result {
                Ok(v) => finish_wasm_trampoline_ok(prepared, t0, v),
                Err(e) => {
                    crate::metrics::timer_finish_err(&prepared.reg.metrics, t0);
                    error!("pg_wasm: wasm: {e}");
                }
            }
        })
    }
}

#[cfg(not(feature = "_pg_wasm_runtime"))]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn pg_wasm_udf_trampoline(
    fcinfo: pg_sys::FunctionCallInfo,
) -> pg_sys::Datum {
    unsafe {
        pgrx::pg_sys::ffi::pg_guard_ffi_boundary(|| {
            let _ = fcinfo;
            error!("pg_wasm: built without a WebAssembly runtime (enable runtime_wasmer, runtime_wasmtime, or runtime_extism)");
        })
    }
}

#[cfg(feature = "_pg_wasm_runtime")]
struct PreparedWasmCall {
    reg: RegisteredFunction,
    inv: WasmInvocation,
}

#[cfg(feature = "_pg_wasm_runtime")]
enum WasmInvocation {
    MemInOut(Vec<u8>),
    I32Arity0,
    I32Arity1(i32),
    I32Arity2(i32, i32),
    BoolArity0,
    BoolArity1(bool),
    BoolArity2(bool, bool),
    I64Arity0,
    I64Arity1(i64),
    F32Arity0,
    F32Arity1(f32),
    F32Arity2(f32, f32),
    F64Arity0,
    F64Arity1(f64),
    F64Arity2(f64, f64),
}

#[cfg(feature = "_pg_wasm_runtime")]
enum WasmValue {
    Bytes(Vec<u8>),
    I32(i32),
    Bool(bool),
    I64(i64),
    F32(f32),
    F64(f64),
}

#[cfg(feature = "_pg_wasm_runtime")]
fn prepare_wasm_trampoline(fcinfo: pg_sys::FunctionCallInfo) -> PreparedWasmCall {
    if fcinfo.is_null() {
        error!("pg_wasm: null fcinfo in trampoline");
    }
    let flinfo = unsafe { (*fcinfo).flinfo };
    if flinfo.is_null() {
        error!("pg_wasm: null flinfo in trampoline");
    }
    let oid = unsafe { (*flinfo).fn_oid };
    let reg = match crate::registry::lookup_by_fn_oid(oid) {
        Some(r) => r,
        None => error!("pg_wasm: no wasm dispatch entry for function OID {}", oid),
    };
    let sig = &reg.signature;
    let inv = if uses_buffer_io(sig) {
        prepare_buffer_invocation(sig, fcinfo)
    } else {
        prepare_scalar_invocation(&reg, fcinfo)
    };
    PreparedWasmCall { reg, inv }
}

#[cfg(feature = "_pg_wasm_runtime")]
fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "panic during wasm isolate".to_string()
}

#[cfg(feature = "_pg_wasm_runtime")]
fn run_wasm_isolated(p: &PreparedWasmCall) -> Result<WasmValue, String> {
    use crate::runtime::dispatch::{
        call_bool_result_arity0, call_bool_result_arity1, call_bool_result_arity2, call_f32_arity0,
        call_f32_arity1, call_f32_arity2, call_f64_arity0, call_f64_arity1, call_f64_arity2,
        call_i32_arity0, call_i32_arity1, call_i32_arity2, call_i64_arity0, call_i64_arity1,
        call_mem_in_out,
    };
    let mid = p.reg.module_id;
    let ex = p.reg.export_name.as_str();
    let backend = crate::registry::module_execution_backend(mid).ok_or_else(|| {
        format!(
            "pg_wasm: no execution backend registered for module id {}",
            mid.0
        )
    })?;
    match &p.inv {
        WasmInvocation::MemInOut(buf) => {
            call_mem_in_out(backend, mid, ex, buf).map(WasmValue::Bytes)
        }
        WasmInvocation::I32Arity0 => call_i32_arity0(backend, mid, ex).map(WasmValue::I32),
        WasmInvocation::I32Arity1(a) => {
            call_i32_arity1(backend, mid, ex, *a).map(WasmValue::I32)
        }
        WasmInvocation::I32Arity2(a, b) => {
            call_i32_arity2(backend, mid, ex, *a, *b).map(WasmValue::I32)
        }
        WasmInvocation::BoolArity0 => {
            call_bool_result_arity0(backend, mid, ex).map(WasmValue::Bool)
        }
        WasmInvocation::BoolArity1(a) => {
            call_bool_result_arity1(backend, mid, ex, *a).map(WasmValue::Bool)
        }
        WasmInvocation::BoolArity2(a, b) => {
            call_bool_result_arity2(backend, mid, ex, *a, *b).map(WasmValue::Bool)
        }
        WasmInvocation::I64Arity0 => call_i64_arity0(backend, mid, ex).map(WasmValue::I64),
        WasmInvocation::I64Arity1(a) => {
            call_i64_arity1(backend, mid, ex, *a).map(WasmValue::I64)
        }
        WasmInvocation::F32Arity0 => call_f32_arity0(backend, mid, ex).map(WasmValue::F32),
        WasmInvocation::F32Arity1(a) => {
            call_f32_arity1(backend, mid, ex, *a).map(WasmValue::F32)
        }
        WasmInvocation::F32Arity2(a, b) => {
            call_f32_arity2(backend, mid, ex, *a, *b).map(WasmValue::F32)
        }
        WasmInvocation::F64Arity0 => call_f64_arity0(backend, mid, ex).map(WasmValue::F64),
        WasmInvocation::F64Arity1(a) => {
            call_f64_arity1(backend, mid, ex, *a).map(WasmValue::F64)
        }
        WasmInvocation::F64Arity2(a, b) => {
            call_f64_arity2(backend, mid, ex, *a, *b).map(WasmValue::F64)
        }
    }
}

#[cfg(feature = "_pg_wasm_runtime")]
fn finish_wasm_trampoline_ok(
    prepared: PreparedWasmCall,
    t0: Option<std::time::Instant>,
    v: WasmValue,
) -> pg_sys::Datum {
    let reg = &prepared.reg;
    crate::metrics::timer_finish_ok(&reg.metrics, t0);
    wasm_value_into_datum(reg, v)
}

#[cfg(feature = "_pg_wasm_runtime")]
fn uses_buffer_io(sig: &ExportSignature) -> bool {
    let buf_ret = matches!(sig.ret.kind, PgWasmTypeKind::String | PgWasmTypeKind::Bytes);
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

#[cfg(feature = "_pg_wasm_runtime")]
fn prepare_buffer_invocation(
    sig: &ExportSignature,
    fcinfo: pg_sys::FunctionCallInfo,
) -> WasmInvocation {
    let input: Vec<u8> = match sig.args.len() {
        0 => Vec::new(),
        1 => {
            let desc = &sig.args[0];
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
                (_, PgWasmTypeKind::Bytes) => {
                    unsafe { pg_getarg::<Vec<u8>>(fcinfo, 0) }.expect("pg_wasm: NULL bytea arg")
                }
                _ => error!("pg_wasm: unsupported buffer arg combination"),
            }
        }
        _ => error!("pg_wasm: invalid buffer signature"),
    };
    WasmInvocation::MemInOut(input)
}

#[cfg(feature = "_pg_wasm_runtime")]
fn prepare_scalar_invocation(
    reg: &RegisteredFunction,
    fcinfo: pg_sys::FunctionCallInfo,
) -> WasmInvocation {
    let sig = &reg.signature;
    match (sig.args.as_slice(), sig.ret.pg_oid, sig.ret.kind) {
        ([], _, PgWasmTypeKind::I32) if sig.ret.pg_oid == pg_sys::INT4OID => {
            WasmInvocation::I32Arity0
        }
        ([], _, PgWasmTypeKind::I32) if sig.ret.pg_oid == pg_sys::INT2OID => {
            WasmInvocation::I32Arity0
        }
        ([], _, PgWasmTypeKind::Bool) => WasmInvocation::BoolArity0,
        ([], _, PgWasmTypeKind::I64) => WasmInvocation::I64Arity0,
        ([], _, PgWasmTypeKind::F32) => WasmInvocation::F32Arity0,
        ([], _, PgWasmTypeKind::F64) => WasmInvocation::F64Arity0,

        ([a], _, _) => match (a.pg_oid, a.kind, sig.ret.pg_oid, sig.ret.kind) {
            (pg_sys::INT4OID, PgWasmTypeKind::I32, pg_sys::INT4OID, PgWasmTypeKind::I32) => {
                let x = unsafe { pg_getarg::<i32>(fcinfo, 0) }.expect("pg_wasm: NULL strict arg");
                WasmInvocation::I32Arity1(x)
            }
            (pg_sys::INT2OID, PgWasmTypeKind::I32, pg_sys::INT2OID, PgWasmTypeKind::I32) => {
                let x = unsafe { pg_getarg::<i16>(fcinfo, 0) }.expect("pg_wasm: NULL strict arg");
                WasmInvocation::I32Arity1(x as i32)
            }
            (_, PgWasmTypeKind::Bool, _, PgWasmTypeKind::Bool) => {
                let x = unsafe { pg_getarg::<bool>(fcinfo, 0) }.expect("pg_wasm: NULL strict arg");
                WasmInvocation::BoolArity1(x)
            }
            (_, PgWasmTypeKind::I64, _, PgWasmTypeKind::I64) => {
                let x = unsafe { pg_getarg::<i64>(fcinfo, 0) }.expect("pg_wasm: NULL strict arg");
                WasmInvocation::I64Arity1(x)
            }
            (_, PgWasmTypeKind::F32, _, PgWasmTypeKind::F32) => {
                let x = unsafe { pg_getarg::<f32>(fcinfo, 0) }.expect("pg_wasm: NULL strict arg");
                WasmInvocation::F32Arity1(x)
            }
            (_, PgWasmTypeKind::F64, _, PgWasmTypeKind::F64) => {
                let x = unsafe { pg_getarg::<f64>(fcinfo, 0) }.expect("pg_wasm: NULL strict arg");
                WasmInvocation::F64Arity1(x)
            }
            _ => error!(
                "pg_wasm: unsupported 1-arg wasm signature (OID {:?} / {:?})",
                (a.pg_oid, a.kind),
                (sig.ret.pg_oid, sig.ret.kind)
            ),
        },

        ([a, b], _, _) => match (a.kind, b.kind, sig.ret.kind) {
            (PgWasmTypeKind::I32, PgWasmTypeKind::I32, PgWasmTypeKind::I32)
                if a.pg_oid == pg_sys::INT4OID
                    && b.pg_oid == pg_sys::INT4OID
                    && sig.ret.pg_oid == pg_sys::INT4OID =>
            {
                let x = unsafe { pg_getarg::<i32>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                let y = unsafe { pg_getarg::<i32>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                WasmInvocation::I32Arity2(x, y)
            }
            (PgWasmTypeKind::Bool, PgWasmTypeKind::Bool, PgWasmTypeKind::Bool) => {
                let x = unsafe { pg_getarg::<bool>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                let y = unsafe { pg_getarg::<bool>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                WasmInvocation::BoolArity2(x, y)
            }
            (PgWasmTypeKind::F32, PgWasmTypeKind::F32, PgWasmTypeKind::F32) => {
                let x = unsafe { pg_getarg::<f32>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                let y = unsafe { pg_getarg::<f32>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                WasmInvocation::F32Arity2(x, y)
            }
            (PgWasmTypeKind::F64, PgWasmTypeKind::F64, PgWasmTypeKind::F64) => {
                let x = unsafe { pg_getarg::<f64>(fcinfo, 0) }.expect("pg_wasm: NULL arg");
                let y = unsafe { pg_getarg::<f64>(fcinfo, 1) }.expect("pg_wasm: NULL arg");
                WasmInvocation::F64Arity2(x, y)
            }
            _ => error!("pg_wasm: unsupported 2-arg wasm signature"),
        },

        _ => error!(
            "pg_wasm: unsupported WASM-backed function ({} args)",
            sig.args.len()
        ),
    }
}

#[cfg(feature = "_pg_wasm_runtime")]
fn wasm_value_into_datum(reg: &RegisteredFunction, v: WasmValue) -> pg_sys::Datum {
    let sig = &reg.signature;
    match v {
        WasmValue::Bytes(b) => buffer_output_datum(&sig.ret, &b),
        WasmValue::I32(v) => match (sig.ret.pg_oid, sig.ret.kind) {
            (pg_sys::INT4OID, PgWasmTypeKind::I32) => v
                .into_datum()
                .unwrap_or_else(|| error!("pg_wasm: int4 into_datum failed")),
            (pg_sys::INT2OID, PgWasmTypeKind::I32) => (v as i16)
                .into_datum()
                .unwrap_or_else(|| error!("pg_wasm: int2 into_datum failed")),
            _ => error!("pg_wasm: internal error: I32 wasm value for unexpected PG return type"),
        },
        WasmValue::Bool(v) => v
            .into_datum()
            .unwrap_or_else(|| error!("pg_wasm: bool into_datum failed")),
        WasmValue::I64(v) => v
            .into_datum()
            .unwrap_or_else(|| error!("pg_wasm: int8 into_datum failed")),
        WasmValue::F32(v) => v
            .into_datum()
            .unwrap_or_else(|| error!("pg_wasm: float4 into_datum failed")),
        WasmValue::F64(v) => v
            .into_datum()
            .unwrap_or_else(|| error!("pg_wasm: float8 into_datum failed")),
    }
}

#[cfg(feature = "_pg_wasm_runtime")]
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
