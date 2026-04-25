//! Host component interfaces (`pg-wasm:host/*`): logging and SPI query.

use std::ffi::{CStr, c_void};

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use wasmtime::component::{HasSelf, Linker};

use crate::errors::PgWasmError;
use crate::mapping::scalars::{self, HostQuerySpiParam};
use crate::policy::EffectivePolicy;
use crate::runtime::component::StoreCtx;

mod bindings {
    wasmtime::component::bindgen!({
        world: "host-only",
        path: "wit",
    });
}

use bindings::pg_wasm::host::{log, query};

const LOG_MESSAGE_MAX_BYTES: usize = 1024 * 1024;
const LOG_TRUNCATION_SUFFIX: &str = "\n…[truncated by pg_wasm host log cap]";

/// Register `pg-wasm:host/log` and optionally `pg-wasm:host/query` on `linker`.
pub(crate) fn add_to_linker(
    linker: &mut Linker<StoreCtx>,
    policy: &EffectivePolicy,
) -> Result<(), PgWasmError> {
    log::add_to_linker::<StoreCtx, HasSelf<StoreCtx>>(linker, |state| state).map_err(|error| {
        PgWasmError::Internal(format!("failed to add pg-wasm:host/log to linker: {error}"))
    })?;
    if policy.allow_spi {
        query::add_to_linker::<StoreCtx, HasSelf<StoreCtx>>(linker, |state| state).map_err(
            |error| {
                PgWasmError::Internal(format!(
                    "failed to add pg-wasm:host/query to linker: {error}"
                ))
            },
        )?;
    }
    Ok(())
}

fn truncate_log_message(message: String) -> String {
    if message.len() <= LOG_MESSAGE_MAX_BYTES {
        return message;
    }
    let take = LOG_MESSAGE_MAX_BYTES.saturating_sub(LOG_TRUNCATION_SUFFIX.len());
    let mut out = String::with_capacity(LOG_MESSAGE_MAX_BYTES);
    for (byte_i, ch) in message.char_indices() {
        let ch_len = ch.len_utf8();
        if byte_i + ch_len > take {
            break;
        }
        out.push(ch);
    }
    out.push_str(LOG_TRUNCATION_SUFFIX);
    out
}

impl log::Host for StoreCtx {
    fn log(&mut self, level: log::Level, message: String) {
        let message = truncate_log_message(message);
        let severity = match level {
            log::Level::Info => PgLogLevel::INFO,
            log::Level::Notice => PgLogLevel::NOTICE,
            log::Level::Warning => PgLogLevel::WARNING,
        };
        ereport!(
            severity,
            PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            message,
        );
    }
}

impl query::Host for StoreCtx {
    fn read(&mut self, sql: String, params: Vec<query::Value>) -> Result<query::ResultSet, String> {
        if !self.host.allow_spi {
            return Err(
                "permission denied: SPI host query is disabled for this invocation (pg_wasm.allow_spi)"
                    .to_string(),
            );
        }
        query_read_impl(sql, params)
    }
}

fn query_read_impl(sql: String, params: Vec<query::Value>) -> Result<query::ResultSet, String> {
    let trimmed = sql.trim_start();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("SELECT")
        && !upper.starts_with("WITH")
        && !upper.starts_with("VALUES")
        && !upper.starts_with("TABLE")
    {
        return Err(
            "only read-only statements are allowed (expected SELECT, WITH, VALUES, or TABLE)"
                .to_string(),
        );
    }

    Spi::connect(|client| {
        let mut args: Vec<DatumWithOid<'static>> = Vec::with_capacity(params.len());
        for (index, value) in params.iter().enumerate() {
            let spi = wit_value_to_spi_param(index, value)?;
            args.push(scalars::host_query_spi_arg(index, spi)?);
        }

        let tup_table = client
            .select(&sql, None, &args)
            .map_err(|e| e.to_string())?;

        let mut column_names = Vec::new();
        let mut column_typoids = Vec::new();
        let ncols = tup_table.columns().map_err(|e| e.to_string())?;
        for i in 1..=ncols {
            let name = tup_table.column_name(i).map_err(|e| e.to_string())?;
            column_names.push(name);
            column_typoids.push(
                tup_table
                    .column_type_oid(i)
                    .map_err(|e| e.to_string())?
                    .value(),
            );
        }

        let mut rows_out = Vec::new();
        let cursor = tup_table;
        while let Some(row) = cursor.get_heap_tuple().map_err(|e| e.to_string())? {
            let mut columns = Vec::new();
            for (col_idx, typoid) in column_typoids.iter().enumerate() {
                let entry = row
                    .get_datum_by_ordinal(col_idx + 1)
                    .map_err(|e| e.to_string())?;
                let cell = match entry.value::<pg_sys::Datum>().map_err(|e| e.to_string())? {
                    None => query::Value::Null,
                    Some(datum) => {
                        datum_to_host_value(*typoid, datum, false).map_err(|e| e.to_string())?
                    }
                };
                columns.push(cell);
            }
            rows_out.push(query::Row { columns });
        }

        Ok(query::ResultSet {
            column_names,
            rows: rows_out,
        })
    })
}

fn wit_value_to_spi_param(
    _index: usize,
    value: &query::Value,
) -> Result<HostQuerySpiParam, String> {
    match value {
        query::Value::Null => Ok(HostQuerySpiParam::Null),
        query::Value::Bool(b) => Ok(HostQuerySpiParam::Bool(*b)),
        query::Value::Int(n) => Ok(HostQuerySpiParam::Int(*n)),
        query::Value::Float(f) => Ok(HostQuerySpiParam::Float(*f)),
        query::Value::Text(s) => Ok(HostQuerySpiParam::Text(s.clone())),
        query::Value::Bytea(bytes) => Ok(HostQuerySpiParam::Bytea(bytes.clone())),
    }
}

fn datum_to_host_value(
    typoid: pg_sys::Oid,
    datum: pg_sys::Datum,
    is_null: bool,
) -> Result<query::Value, PgWasmError> {
    if is_null {
        return Ok(query::Value::Null);
    }

    unsafe {
        let type_form = pg_sys::get_typtype(typoid);
        if type_form == pg_sys::TYPTYPE_PSEUDO as i8 {
            return text_fallback(typoid, datum);
        }

        if typoid == pg_sys::BOOLOID {
            let b = bool::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode bool for host query row".to_string())
            })?;
            return Ok(query::Value::Bool(b));
        }
        if typoid == pg_sys::INT2OID {
            let v = i16::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode int2 for host query row".to_string())
            })?;
            return Ok(query::Value::Int(i64::from(v)));
        }
        if typoid == pg_sys::INT4OID {
            let v = i32::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode int4 for host query row".to_string())
            })?;
            return Ok(query::Value::Int(i64::from(v)));
        }
        if typoid == pg_sys::INT8OID {
            let v = i64::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode int8 for host query row".to_string())
            })?;
            return Ok(query::Value::Int(v));
        }
        if typoid == pg_sys::FLOAT4OID {
            let f = f32::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode float4 for host query row".to_string())
            })?;
            return Ok(query::Value::Float(f64::from(f)));
        }
        if typoid == pg_sys::FLOAT8OID {
            let f = f64::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode float8 for host query row".to_string())
            })?;
            return Ok(query::Value::Float(f));
        }
        if typoid == pg_sys::TEXTOID || typoid == pg_sys::VARCHAROID || typoid == pg_sys::BPCHAROID
        {
            let s = String::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode text for host query row".to_string())
            })?;
            return Ok(query::Value::Text(s));
        }
        if typoid == pg_sys::BYTEAOID {
            let bytes: &[u8] = <&[u8]>::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode bytea for host query row".to_string())
            })?;
            return Ok(query::Value::Bytea(bytes.to_vec()));
        }

        text_fallback(typoid, datum)
    }
}

fn text_fallback(typoid: pg_sys::Oid, datum: pg_sys::Datum) -> Result<query::Value, PgWasmError> {
    let mut typ_output = pg_sys::InvalidOid;
    let mut typ_is_varlena = false;
    unsafe {
        pg_sys::getTypeOutputInfo(typoid, &mut typ_output, &mut typ_is_varlena);
    }
    if typ_output == pg_sys::InvalidOid {
        return Err(PgWasmError::Internal(format!(
            "no output function for type OID {typoid}"
        )));
    }
    let cstr = unsafe { pg_sys::OidOutputFunctionCall(typ_output, datum) };
    if cstr.is_null() {
        return Err(PgWasmError::Internal(
            "type output function returned null for host query cell".to_string(),
        ));
    }
    let s = unsafe {
        let s = CStr::from_ptr(cstr).to_string_lossy().into_owned();
        pg_sys::pfree(cstr as *mut c_void);
        s
    };
    Ok(query::Value::Text(s))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use wit_parser::Resolve;

    #[test]
    fn host_wit_parses() {
        let mut resolve = Resolve::default();
        let wit_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("wit");
        resolve
            .push_path(wit_dir.join("host.wit"))
            .expect("host.wit should parse");
    }
}
