//! `Datum` ↔ Wasm scalar conversions for the core module path.

#[cfg(not(test))]
use pgrx::datum::DatumWithOid;
#[cfg(not(test))]
use pgrx::prelude::*;
use wasmtime::Val;

/// Map `int[]` elements to `Val::I32` for the core scalar invoke path.
pub(crate) fn i32_vec_to_vals(args: &[i32]) -> Vec<Val> {
    args.iter().copied().map(Val::I32).collect()
}

/// Scalar SPI parameter for the component host `query.read` path.
#[cfg(not(test))]
pub(crate) enum HostQuerySpiParam {
    Bool(bool),
    Bytea(Vec<u8>),
    Float(f64),
    Int(i64),
    Null,
    Text(String),
}

/// Convert [`HostQuerySpiParam`] to [`DatumWithOid`] for `SpiClient::select`.
#[cfg(not(test))]
pub(crate) fn host_query_spi_arg(
    _index: usize,
    value: HostQuerySpiParam,
) -> core::result::Result<DatumWithOid<'static>, String> {
    match value {
        HostQuerySpiParam::Null => Ok(DatumWithOid::null_oid(pg_sys::UNKNOWNOID)),
        HostQuerySpiParam::Bool(b) => Ok(unsafe { DatumWithOid::new(b, pg_sys::BOOLOID) }),
        HostQuerySpiParam::Int(n) => Ok(unsafe { DatumWithOid::new(n, pg_sys::INT8OID) }),
        HostQuerySpiParam::Float(f) => Ok(unsafe { DatumWithOid::new(f, pg_sys::FLOAT8OID) }),
        HostQuerySpiParam::Text(s) => Ok(unsafe { DatumWithOid::new(s, pg_sys::TEXTOID) }),
        HostQuerySpiParam::Bytea(bytes) => {
            let slice = bytes.as_slice();
            Ok(unsafe { DatumWithOid::new(slice, pg_sys::BYTEAOID) })
        }
    }
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
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
