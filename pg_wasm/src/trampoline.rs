//! Trampoline entrypoint dispatch helpers.

use std::panic::{AssertUnwindSafe, catch_unwind};

use pgrx::notice;
use pgrx::pg_guard;
use pgrx::pg_sys::{Datum, FunctionCallInfo, Oid};

use crate::registry;

#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn pg_wasm_udf_trampoline(fcinfo: FunctionCallInfo) -> Datum {
    let result = catch_unwind(AssertUnwindSafe(|| unsafe { trampoline_impl(fcinfo) }));

    match result {
        Ok(datum) => datum,
        Err(payload) => {
            let panic_message = panic_payload_message(payload);
            notice!("pg_wasm_udf_trampoline panic: {panic_message}");
            Datum::from(0_i32)
        }
    }
}

unsafe fn trampoline_impl(fcinfo: FunctionCallInfo) -> Datum {
    let fn_oid = unsafe { fn_oid_from_fcinfo(fcinfo) };
    if registry::resolve_fn_oid(fn_oid).is_none() {
        registry::refresh_from_catalog();
        let _ = registry::resolve_fn_oid(fn_oid);
    }

    Datum::from(0_i32)
}

unsafe fn fn_oid_from_fcinfo(fcinfo: FunctionCallInfo) -> Oid {
    if fcinfo.is_null() {
        return Oid::INVALID;
    }

    let flinfo = unsafe { (*fcinfo).flinfo };
    if flinfo.is_null() {
        Oid::INVALID
    } else {
        unsafe { (*flinfo).fn_oid }
    }
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic payload was not a string".to_owned()
    }
}
