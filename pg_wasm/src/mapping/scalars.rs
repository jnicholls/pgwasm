//! `Datum` ↔ Wasm scalar conversions for the core module path.

use pgrx::prelude::*;
use wasmtime::Val;

use crate::errors::PgWasmError;

/// Convert a non-null `Datum` for `typoid` into an `i32` for Wasm `i32` parameters.
///
/// Supported OIDs: `BOOL`, `INT2`, `INT4`, `INT8` (range-checked), `FLOAT4`, `FLOAT8`.
pub(crate) fn datum_to_i32(
    typoid: pg_sys::Oid,
    datum: pg_sys::Datum,
    is_null: bool,
) -> Result<i32, PgWasmError> {
    if is_null {
        return Err(PgWasmError::InvalidConfiguration(
            "null scalar not supported for core i32 invoke".to_string(),
        ));
    }

    unsafe {
        if typoid == pg_sys::BOOLOID {
            let b = bool::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode bool datum for scalar mapping".to_string())
            })?;
            return Ok(i32::from(b));
        }
        if typoid == pg_sys::INT2OID {
            let v = i16::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode int2 datum for scalar mapping".to_string())
            })?;
            return Ok(i32::from(v));
        }
        if typoid == pg_sys::INT4OID {
            return i32::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode int4 datum for scalar mapping".to_string())
            });
        }
        if typoid == pg_sys::INT8OID {
            let v = i64::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal("failed to decode int8 datum for scalar mapping".to_string())
            })?;
            return i32::try_from(v).map_err(|_| {
                PgWasmError::InvalidConfiguration(format!(
                    "int8 value {v} is out of range for wasm i32"
                ))
            });
        }
        if typoid == pg_sys::FLOAT4OID {
            let f = f32::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal(
                    "failed to decode float4 datum for scalar mapping".to_string(),
                )
            })?;
            return Ok(f.to_bits() as i32);
        }
        if typoid == pg_sys::FLOAT8OID {
            let f = f64::from_datum(datum, false).ok_or_else(|| {
                PgWasmError::Internal(
                    "failed to decode float8 datum for scalar mapping".to_string(),
                )
            })?;
            let f32v = f as f32;
            return Ok(f32v.to_bits() as i32);
        }
    }

    Err(PgWasmError::Unsupported(format!(
        "OID {typoid} is not supported for core scalar i32 mapping"
    )))
}

/// Build a `Datum` of type `INT4` from a Wasm `i32` result (raw integer, not float bits).
pub(crate) fn i32_to_int4_datum(value: i32) -> pg_sys::Datum {
    value.into_datum().expect("int4 into_datum should succeed")
}

/// Interpret `value` as Wasm `i32` and map to `FLOAT4OID` / `FLOAT8OID` / `INT4OID` as requested.
pub(crate) fn i32_result_to_datum(
    result_typoid: pg_sys::Oid,
    value: i32,
) -> Result<pg_sys::Datum, PgWasmError> {
    if result_typoid == pg_sys::INT4OID {
        return Ok(i32_to_int4_datum(value));
    }
    if result_typoid == pg_sys::FLOAT4OID {
        let f = f32::from_bits(value as u32);
        return f.into_datum().ok_or_else(|| {
            PgWasmError::Internal("failed to convert f32 to float4 datum".to_string())
        });
    }
    if result_typoid == pg_sys::FLOAT8OID {
        let f = f32::from_bits(value as u32) as f64;
        return f.into_datum().ok_or_else(|| {
            PgWasmError::Internal("failed to convert f64 to float8 datum".to_string())
        });
    }

    Err(PgWasmError::Unsupported(format!(
        "result OID {result_typoid} is not supported for core scalar mapping"
    )))
}

/// Map `int[]` elements to `Val::I32` for the core scalar invoke path.
pub(crate) fn i32_vec_to_vals(args: &[i32]) -> Vec<Val> {
    args.iter().copied().map(Val::I32).collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn i64_to_i32_bounds() {
        assert!(i32::try_from(0i64).is_ok());
        assert!(i32::try_from(i64::from(i32::MAX)).is_ok());
        assert!(i32::try_from(i64::from(i32::MIN)).is_ok());
        assert!(i32::try_from(i64::from(i32::MAX) + 1).is_err());
        assert!(i32::try_from(i64::from(i32::MIN) - 1).is_err());
    }

    #[test]
    fn int16_fits_in_wasm_i32() {
        assert_eq!(i32::from(i16::MAX), i32::from(32_767i16));
        assert_eq!(i32::from(i16::MIN), i32::from(-32_768i16));
    }

    #[test]
    fn float32_bits_roundtrip_for_scalar_path() {
        let original: f32 = -3.5;
        let bits = original.to_bits() as i32;
        assert_eq!(f32::from_bits(bits as u32), original);
    }
}
