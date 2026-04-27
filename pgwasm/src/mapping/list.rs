//! `list<T>` and `list<u8>` marshaling helpers for `wasmtime::component::Val`.

use pgrx::datum::DatumWithOid;
use pgrx::pg_sys;
use pgrx::prelude::*;
use pgrx::spi::Spi;

use wasmtime::component::Val;

use crate::errors::PgWasmError;

/// Build `Val::List` of `Val::U8` from a `bytea` datum (copying bytes into guest-shaped list).
pub(crate) fn bytea_datum_to_u8_list(
    datum: pg_sys::Datum,
    is_null: bool,
) -> Result<Val, PgWasmError> {
    if is_null {
        return Err(PgWasmError::ValidationFailed(
            "bytea list marshaling: unexpected SQL NULL".to_string(),
        ));
    }
    let bytes = unsafe { Vec::<u8>::from_datum(datum, false) }.ok_or_else(|| {
        PgWasmError::Internal("bytea list marshaling: failed to read bytea datum".to_string())
    })?;
    Ok(Val::List(bytes.into_iter().map(Val::U8).collect()))
}

/// Serialize `Val::List` of `Val::U8` into a `bytea` datum.
pub(crate) fn u8_list_val_to_bytea(val: &Val) -> Result<(pg_sys::Datum, bool), PgWasmError> {
    let Val::List(items) = val else {
        return Err(PgWasmError::ValidationFailed(
            "expected Val::List for bytea marshaling".to_string(),
        ));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let Val::U8(b) = item else {
            return Err(PgWasmError::ValidationFailed(
                "expected Val::U8 elements in bytea list".to_string(),
            ));
        };
        out.push(*b);
    }
    let datum = out.into_datum().ok_or_else(|| {
        PgWasmError::Internal("bytea list marshaling: failed to build bytea datum".to_string())
    })?;
    Ok((datum, false))
}

/// `int4[]` datum → `Val::List` of `Val::S32` via SPI (`array_to_string` / split).
pub(crate) fn array_datum_to_list_i32<F>(
    datum: pg_sys::Datum,
    is_null: bool,
    mut map: F,
) -> Result<Val, PgWasmError>
where
    F: FnMut(i32) -> Result<Val, PgWasmError>,
{
    if is_null {
        return Ok(Val::List(Vec::new()));
    }
    let arg = unsafe { DatumWithOid::new(datum, pg_sys::INT4ARRAYOID) };
    let s: Option<String> = Spi::get_one_with_args(
        "SELECT NULLIF(array_to_string($1::int4[], ','), '')",
        &[arg],
    )
    .map_err(|e| PgWasmError::Internal(format!("list marshaling (int4[]): {e}")))?;
    let Some(s) = s else {
        return Ok(Val::List(Vec::new()));
    };
    if s.is_empty() {
        return Ok(Val::List(Vec::new()));
    }
    let mut items = Vec::new();
    for part in s.split(',') {
        let v: i32 = part.trim().parse().map_err(|_| {
            PgWasmError::Internal(format!(
                "list marshaling: invalid int4 array element `{part}`"
            ))
        })?;
        items.push(map(v)?);
    }
    Ok(Val::List(items))
}

/// `Val::List` of `Val::S32` → `int4[]` via SPI (CSV → typed array).
pub(crate) fn list_val_to_int4_array(val: &Val) -> Result<(pg_sys::Datum, bool), PgWasmError> {
    let Val::List(items) = val else {
        return Err(PgWasmError::ValidationFailed(
            "expected Val::List for array marshaling".to_string(),
        ));
    };
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        let Val::S32(v) = item else {
            return Err(PgWasmError::Unsupported(
                "list→int4[] only supports Val::S32 elements in this build".to_string(),
            ));
        };
        parts.push(v.to_string());
    }
    let arr: Array<i32> = if parts.is_empty() {
        Spi::get_one("SELECT ARRAY[]::int4[] AS a")
            .map_err(|e| PgWasmError::Internal(format!("list marshaling (empty array): {e}")))?
            .ok_or_else(|| {
                PgWasmError::Internal(
                    "list marshaling: empty array query returned no row".to_string(),
                )
            })?
    } else {
        let csv = parts.join(",");
        let arg = unsafe { DatumWithOid::new(csv, pg_sys::TEXTOID) };
        Spi::get_one_with_args(
            "SELECT ARRAY(SELECT unnest(string_to_array($1, ','))::int4) AS a",
            &[arg],
        )
        .map_err(|e| PgWasmError::Internal(format!("list marshaling (build array): {e}")))?
        .ok_or_else(|| {
            PgWasmError::Internal("list marshaling: ARRAY build query returned no row".to_string())
        })?
    };
    let datum = arr.into_datum().ok_or_else(|| {
        PgWasmError::Internal("list marshaling: failed to extract int4[] datum".to_string())
    })?;
    Ok((datum, false))
}
