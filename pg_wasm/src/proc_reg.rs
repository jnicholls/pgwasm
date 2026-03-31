//! Dynamic registration of SQL functions backed by [`crate::trampoline::pg_wasm_udf_trampoline`].
//!
//! Uses PostgreSQL’s `ProcedureCreate` / `RemoveFunctionById` so `pg_proc.proname` / argument
//! types match what callers execute, while all bodies share one C symbol.

use std::{ffi::CString, ptr};

use pgrx::{pg_sys::Oid, prelude::*, spi::Spi};

use crate::{
    mapping::ExportSignature,
    registry::{ModuleId, RegisteredFunction, register_fn_oid, unregister_fn_oid},
    trampoline::TRAMPOLINE_PG_SYMBOL,
};

/// `pg_extension.extname` / control file stem (see `pg_wasm.control`).
const PG_WASM_EXTENSION_NAME: &str = "pg_wasm";

#[derive(Debug, thiserror::Error)]
pub enum RegisterError {
    #[error("pg_wasm: invalid SQL identifier `{0}`")]
    BadIdentifier(String),
    #[error("pg_wasm: {0}")]
    Message(String),
}

pub(crate) fn assert_sql_identifier(id: &str) -> Result<(), RegisterError> {
    let mut ch = id.chars();
    let Some(first) = ch.next() else {
        return Err(RegisterError::BadIdentifier(id.into()));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(RegisterError::BadIdentifier(id.into()));
    }
    if !ch.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(RegisterError::BadIdentifier(id.into()));
    }
    Ok(())
}

fn c_language_validator_oid() -> Result<Oid, RegisterError> {
    Spi::get_one::<Oid>("SELECT lanvalidator FROM pg_catalog.pg_language WHERE lanname = 'c'")
        .map_err(|e| RegisterError::Message(format!("SPI (read pg_language): {e}")))?
        .ok_or_else(|| RegisterError::Message("pg_language entry for `c` not found".into()))
}

/// Create a `pg_proc` row for a strict/int4 C function with the given argument OIDs, attach it to
/// the trampoline symbol, and register [`RegisteredFunction`] metadata.
///
/// `sql_basename` is the bare function name (no schema); `schema` selects `pg_namespace`.
pub fn register_wasm_trampoline_proc(
    schema: &str,
    sql_basename: &str,
    arg_types: &[Oid],
    return_type: Oid,
    reg: RegisteredFunction,
) -> Result<Oid, RegisterError> {
    assert_sql_identifier(schema)?;
    assert_sql_identifier(sql_basename)?;

    let lang_validator = c_language_validator_oid()?;

    let schema_c = CString::new(schema).map_err(|e| RegisterError::Message(e.to_string()))?;
    let nsp_oid = unsafe { pg_sys::get_namespace_oid(schema_c.as_ptr(), false) };

    let procname = CString::new(sql_basename).map_err(|e| RegisterError::Message(e.to_string()))?;
    let prosrc =
        CString::new(TRAMPOLINE_PG_SYMBOL).map_err(|e| RegisterError::Message(e.to_string()))?;
    let probin =
        CString::new("$libdir/pg_wasm").map_err(|e| RegisterError::Message(e.to_string()))?;

    let lang_c = CString::new("c").map_err(|e| RegisterError::Message(e.to_string()))?;
    let lang_oid = unsafe { pg_sys::get_language_oid(lang_c.as_ptr(), false) };
    let proowner = unsafe { pg_sys::GetUserId() };

    let parameter_types = unsafe {
        if arg_types.is_empty() {
            pg_sys::buildoidvector(ptr::null(), 0)
        } else {
            pg_sys::buildoidvector(arg_types.as_ptr(), arg_types.len() as i32)
        }
    };

    let addr = unsafe {
        procedure_create(
            procname.as_ptr(),
            nsp_oid,
            return_type,
            proowner,
            lang_oid,
            lang_validator,
            prosrc.as_ptr(),
            probin.as_ptr(),
            true,
            parameter_types,
        )
    };

    record_runtime_extension_membership(&addr)?;

    let oid = addr.objectId;
    register_fn_oid(oid, reg);
    Ok(oid)
}

/// `ProcedureCreate` already calls `recordDependencyOnCurrentExtension`, but that only records
/// membership while `CREATE EXTENSION` is running (`creating_extension`). Dynamic registration from
/// ordinary SQL must attach the new `pg_proc` row to the extension so `DROP EXTENSION` removes it.
fn record_runtime_extension_membership(addr: &pg_sys::ObjectAddress) -> Result<(), RegisterError> {
    unsafe {
        if pg_sys::creating_extension {
            return Ok(());
        }
        let extname =
            CString::new(PG_WASM_EXTENSION_NAME).map_err(|e| RegisterError::Message(e.to_string()))?;
        let ext_oid = pg_sys::get_extension_oid(extname.as_ptr(), false);
        if ext_oid == pg_sys::InvalidOid {
            return Err(RegisterError::Message(
                "pg_wasm extension is not installed in this database".into(),
            ));
        }
        let extension = pg_sys::ObjectAddress {
            classId: pg_sys::ExtensionRelationId,
            objectId: ext_oid,
            objectSubId: 0,
        };
        pg_sys::recordDependencyOn(
            addr,
            &extension,
            pg_sys::DependencyType::DEPENDENCY_EXTENSION,
        );
    }
    Ok(())
}

/// Drops a function previously registered with [`register_wasm_trampoline_proc`] and removes its
/// trampoline metadata.
pub fn drop_wasm_trampoline_proc(oid: Oid) {
    if oid == pg_sys::InvalidOid {
        return;
    }
    unsafe {
        pg_sys::RemoveFunctionById(oid);
    }
    unregister_fn_oid(oid);
}

#[allow(clippy::too_many_arguments)]
#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn procedure_create(
    procedure_name: *const std::ffi::c_char,
    proc_namespace: Oid,
    return_type: Oid,
    proowner: Oid,
    language_object_id: Oid,
    language_validator: Oid,
    prosrc: *const std::ffi::c_char,
    probin: *const std::ffi::c_char,
    is_strict: bool,
    parameter_types: *mut pg_sys::oidvector,
) -> pg_sys::ObjectAddress {
    let prokind = pg_sys::PROKIND_FUNCTION as std::ffi::c_char;
    let volatility = pg_sys::PROVOLATILE_VOLATILE as std::ffi::c_char;
    let parallel = pg_sys::PROPARALLEL_UNSAFE as std::ffi::c_char;
    let null_datum = pg_sys::Datum::null();
    let null_list: *mut pg_sys::List = ptr::null_mut();

    #[cfg(feature = "pg13")]
    {
        pg_sys::ProcedureCreate(
            procedure_name,
            proc_namespace,
            false,
            false,
            return_type,
            proowner,
            language_object_id,
            language_validator,
            prosrc,
            probin,
            prokind,
            false,
            false,
            is_strict,
            volatility,
            parallel,
            parameter_types,
            null_datum,
            null_datum,
            null_datum,
            null_list,
            null_datum,
            null_datum,
            pg_sys::InvalidOid,
            100.0_f32,
            0.0_f32,
        )
    }
    #[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16", feature = "pg17"))]
    {
        pg_sys::ProcedureCreate(
            procedure_name,
            proc_namespace,
            false,
            false,
            return_type,
            proowner,
            language_object_id,
            language_validator,
            prosrc,
            probin,
            ptr::null_mut::<pg_sys::Node>(),
            prokind,
            false,
            false,
            is_strict,
            volatility,
            parallel,
            parameter_types,
            null_datum,
            null_datum,
            null_datum,
            null_list,
            null_datum,
            null_datum,
            pg_sys::InvalidOid,
            100.0_f32,
            0.0_f32,
        )
    }
    #[cfg(feature = "pg18")]
    {
        pg_sys::ProcedureCreate(
            procedure_name,
            proc_namespace,
            false,
            false,
            return_type,
            proowner,
            language_object_id,
            language_validator,
            prosrc,
            probin,
            ptr::null_mut::<pg_sys::Node>(),
            prokind,
            false,
            false,
            is_strict,
            volatility,
            parallel,
            parameter_types,
            null_datum,
            null_datum,
            null_datum,
            null_list,
            null_datum,
            null_list,
            null_datum,
            pg_sys::InvalidOid,
            100.0_f32,
            0.0_f32,
        )
    }
}

/// Test / introspection helper: register `schema.sql_name()` → `int4` with no SQL args.
#[pg_extern]
fn pg_wasm_debug_register_zeroary_i32(
    schema: &str,
    sql_name: &str,
    module_id: i64,
    export_name: &str,
) -> Oid {
    if let Err(e) = assert_sql_identifier(export_name) {
        error!("{e}");
    }
    let reg = RegisteredFunction {
        module_id: ModuleId(module_id),
        export_name: export_name.to_string(),
        signature: ExportSignature::default(),
    };
    match register_wasm_trampoline_proc(schema, sql_name, &[], pg_sys::INT4OID, reg) {
        Ok(oid) => oid,
        Err(e) => error!("{e}"),
    }
}

/// Register `schema.sql_name(integer, integer)` returning `integer` (two `int4` arguments).
#[pg_extern]
fn pg_wasm_debug_register_binary_i32(
    schema: &str,
    sql_name: &str,
    module_id: i64,
    export_name: &str,
) -> Oid {
    if let Err(e) = assert_sql_identifier(export_name) {
        error!("{e}");
    }
    let reg = RegisteredFunction {
        module_id: ModuleId(module_id),
        export_name: export_name.to_string(),
        signature: ExportSignature::default(),
    };
    match register_wasm_trampoline_proc(
        schema,
        sql_name,
        &[pg_sys::INT4OID, pg_sys::INT4OID],
        pg_sys::INT4OID,
        reg,
    ) {
        Ok(oid) => oid,
        Err(e) => error!("{e}"),
    }
}

#[pg_extern]
fn pg_wasm_debug_unregister(func_oid: Oid) {
    drop_wasm_trampoline_proc(func_oid);
}
