//! PostgreSQL composite datums ↔ Wasmtime [`Val`] for WIT `record` / `tuple` (Track A / B).

use std::num::NonZeroUsize;

use pgrx::pg_sys;
use pgrx::prelude::*;
use wasmtime::component::Val;

use crate::composite_layout;
use crate::mapping::MarshalType;

/// Decode a composite SQL datum using the layout implied by `mt` (`record` or `tuple`).
///
/// # Safety
///
/// `datum` must be a valid non-null composite datum for the expected type.
pub unsafe fn composite_datum_to_val(
    datum: pg_sys::Datum,
    mt: &MarshalType,
) -> Result<Val, String> {
    if !composite_layout::marshal_type_uses_composite_surface(mt) {
        return Err("pg_wasm: composite_datum_to_val expects record or tuple marshal type".into());
    }
    let tup = unsafe { PgHeapTuple::from_composite_datum(datum) };
    match mt {
        MarshalType::Record(fields) => {
            let mut pairs = Vec::with_capacity(fields.len());
            for (name, fty) in fields {
                let v = read_named_field(&tup, name, fty)?;
                pairs.push((name.clone(), v));
            }
            Ok(Val::Record(pairs))
        }
        MarshalType::Tuple(elems) => {
            let mut out = Vec::with_capacity(elems.len());
            for (i, fty) in elems.iter().enumerate() {
                let attno = NonZeroUsize::new(i + 1)
                    .ok_or_else(|| "pg_wasm: invalid composite attribute number".to_string())?;
                let v = read_indexed_field(&tup, attno, fty)?;
                out.push(v);
            }
            Ok(Val::Tuple(out))
        }
        _ => Err("pg_wasm: internal error: composite marshal type mismatch".into()),
    }
}

fn read_named_field(
    tup: &PgHeapTuple<'_, AllocatedByRust>,
    name: &str,
    mt: &MarshalType,
) -> Result<Val, String> {
    if composite_layout::marshal_type_uses_composite_surface(mt) {
        let inner = tup
            .get_by_name::<PgHeapTuple<'_, AllocatedByRust>>(name)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("pg_wasm: NULL composite field {name:?}"))?;
        let d = inner
            .into_composite_datum()
            .ok_or_else(|| format!("pg_wasm: empty composite field {name:?}"))?;
        return unsafe { composite_datum_to_val(d, mt) };
    }
    read_scalar_named(tup, name, mt)
}

fn read_indexed_field(
    tup: &PgHeapTuple<'_, AllocatedByRust>,
    attno: NonZeroUsize,
    mt: &MarshalType,
) -> Result<Val, String> {
    if composite_layout::marshal_type_uses_composite_surface(mt) {
        let inner = tup
            .get_by_index::<PgHeapTuple<'_, AllocatedByRust>>(attno)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("pg_wasm: NULL composite attribute {attno}"))?;
        let d = inner
            .into_composite_datum()
            .ok_or_else(|| format!("pg_wasm: empty composite attribute {attno}"))?;
        return unsafe { composite_datum_to_val(d, mt) };
    }
    read_scalar_indexed(tup, attno, mt)
}

fn read_scalar_named(
    tup: &PgHeapTuple<'_, AllocatedByRust>,
    name: &str,
    mt: &MarshalType,
) -> Result<Val, String> {
    match mt {
        MarshalType::Bool => {
            let v = tup
                .get_by_name::<bool>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL bool field {name:?}"))?;
            Ok(Val::Bool(v))
        }
        MarshalType::S8 => {
            let v = tup
                .get_by_name::<i32>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 field {name:?}"))?;
            Ok(Val::S8(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for s8".to_string()
            })?))
        }
        MarshalType::U8 => {
            let v = tup
                .get_by_name::<i32>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 field {name:?}"))?;
            Ok(Val::U8(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for u8".to_string()
            })?))
        }
        MarshalType::S16 => {
            let v = tup
                .get_by_name::<i32>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 field {name:?}"))?;
            Ok(Val::S16(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for s16".to_string()
            })?))
        }
        MarshalType::U16 => {
            let v = tup
                .get_by_name::<i32>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 field {name:?}"))?;
            Ok(Val::U16(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for u16".to_string()
            })?))
        }
        MarshalType::S32 | MarshalType::U32 => {
            let v = tup
                .get_by_name::<i32>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL int field {name:?}"))?;
            match mt {
                MarshalType::S32 => Ok(Val::S32(v)),
                MarshalType::U32 => {
                    Ok(Val::U32(v.try_into().map_err(|_| {
                        "pg_wasm: negative int4 cannot be WIT u32".to_string()
                    })?))
                }
                _ => unreachable!(),
            }
        }
        MarshalType::S64 | MarshalType::U64 => {
            let v = tup
                .get_by_name::<i64>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL bigint field {name:?}"))?;
            match mt {
                MarshalType::S64 => Ok(Val::S64(v)),
                MarshalType::U64 => {
                    Ok(Val::U64(v.try_into().map_err(|_| {
                        "pg_wasm: negative int8 cannot be WIT u64".to_string()
                    })?))
                }
                _ => unreachable!(),
            }
        }
        MarshalType::F32 => {
            let v = tup
                .get_by_name::<f32>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL float4 field {name:?}"))?;
            Ok(Val::Float32(v))
        }
        MarshalType::F64 => {
            let v = tup
                .get_by_name::<f64>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL float8 field {name:?}"))?;
            Ok(Val::Float64(v))
        }
        MarshalType::Char => {
            let s = tup
                .get_by_name::<String>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL text field {name:?}"))?;
            let mut it = s.chars();
            let ch = it
                .next()
                .ok_or_else(|| format!("pg_wasm: empty text for char field {name:?}"))?;
            if it.next().is_some() {
                return Err(format!(
                    "pg_wasm: expected a single Unicode scalar for char field {name:?}"
                ));
            }
            Ok(Val::Char(ch))
        }
        MarshalType::String => {
            let s = tup
                .get_by_name::<String>(name)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL text field {name:?}"))?;
            Ok(Val::String(s))
        }
        _ => Err(format!(
            "pg_wasm: composite field type {mt:?} is not supported as a non-aggregate SQL column"
        )),
    }
}

fn read_scalar_indexed(
    tup: &PgHeapTuple<'_, AllocatedByRust>,
    attno: NonZeroUsize,
    mt: &MarshalType,
) -> Result<Val, String> {
    let idx = attno.get();
    match mt {
        MarshalType::Bool => {
            let v = tup
                .get_by_index::<bool>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL bool attribute {idx}"))?;
            Ok(Val::Bool(v))
        }
        MarshalType::S8 => {
            let v = tup
                .get_by_index::<i32>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 attribute {idx}"))?;
            Ok(Val::S8(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for s8".to_string()
            })?))
        }
        MarshalType::U8 => {
            let v = tup
                .get_by_index::<i32>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 attribute {idx}"))?;
            Ok(Val::U8(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for u8".to_string()
            })?))
        }
        MarshalType::S16 => {
            let v = tup
                .get_by_index::<i32>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 attribute {idx}"))?;
            Ok(Val::S16(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for s16".to_string()
            })?))
        }
        MarshalType::U16 => {
            let v = tup
                .get_by_index::<i32>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL i32 attribute {idx}"))?;
            Ok(Val::U16(v.try_into().map_err(|_| {
                "pg_wasm: i32 out of range for u16".to_string()
            })?))
        }
        MarshalType::S32 | MarshalType::U32 => {
            let v = tup
                .get_by_index::<i32>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL int attribute {idx}"))?;
            match mt {
                MarshalType::S32 => Ok(Val::S32(v)),
                MarshalType::U32 => {
                    Ok(Val::U32(v.try_into().map_err(|_| {
                        "pg_wasm: negative int4 cannot be WIT u32".to_string()
                    })?))
                }
                _ => unreachable!(),
            }
        }
        MarshalType::S64 | MarshalType::U64 => {
            let v = tup
                .get_by_index::<i64>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL bigint attribute {idx}"))?;
            match mt {
                MarshalType::S64 => Ok(Val::S64(v)),
                MarshalType::U64 => {
                    Ok(Val::U64(v.try_into().map_err(|_| {
                        "pg_wasm: negative int8 cannot be WIT u64".to_string()
                    })?))
                }
                _ => unreachable!(),
            }
        }
        MarshalType::F32 => {
            let v = tup
                .get_by_index::<f32>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL float4 attribute {idx}"))?;
            Ok(Val::Float32(v))
        }
        MarshalType::F64 => {
            let v = tup
                .get_by_index::<f64>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL float8 attribute {idx}"))?;
            Ok(Val::Float64(v))
        }
        MarshalType::Char => {
            let s = tup
                .get_by_index::<String>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL text attribute {idx}"))?;
            let mut it = s.chars();
            let ch = it
                .next()
                .ok_or_else(|| format!("pg_wasm: empty text for char attribute {idx}"))?;
            if it.next().is_some() {
                return Err(format!(
                    "pg_wasm: expected a single Unicode scalar for char attribute {idx}"
                ));
            }
            Ok(Val::Char(ch))
        }
        MarshalType::String => {
            let s = tup
                .get_by_index::<String>(attno)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("pg_wasm: NULL text attribute {idx}"))?;
            Ok(Val::String(s))
        }
        _ => Err(format!(
            "pg_wasm: composite attribute type {mt:?} is not supported as a non-aggregate SQL column"
        )),
    }
}

/// Encode `val` as a datum of composite type `typoid` (`record` / `tuple` shapes only).
pub fn val_to_composite_datum(
    val: &Val,
    typoid: pg_sys::Oid,
    mt: &MarshalType,
) -> Result<pg_sys::Datum, String> {
    if !composite_layout::marshal_type_uses_composite_surface(mt) {
        return Err("pg_wasm: val_to_composite_datum expects record or tuple marshal type".into());
    }
    let mut tup = PgHeapTuple::new_composite_type_by_oid(typoid)
        .map_err(|e| format!("pg_wasm: composite tuple builder: {e}"))?;
    match (val, mt) {
        (Val::Record(pairs), MarshalType::Record(fields)) => {
            if pairs.len() != fields.len() {
                return Err(format!(
                    "pg_wasm: record value length {} does not match WIT field count {}",
                    pairs.len(),
                    fields.len()
                ));
            }
            for ((vn, vv), (wn, wt)) in pairs.iter().zip(fields.iter()) {
                if vn != wn {
                    return Err(format!(
                        "pg_wasm: record field name mismatch: value {vn:?} vs WIT {wn:?}"
                    ));
                }
                write_named_field(&mut tup, wn.as_str(), wt, vv)?;
            }
        }
        (Val::Tuple(items), MarshalType::Tuple(elems)) => {
            if items.len() != elems.len() {
                return Err(format!(
                    "pg_wasm: tuple value length {} does not match WIT arity {}",
                    items.len(),
                    elems.len()
                ));
            }
            for (i, (vv, wt)) in items.iter().zip(elems.iter()).enumerate() {
                let attno = NonZeroUsize::new(i + 1)
                    .ok_or_else(|| "pg_wasm: invalid composite attribute number".to_string())?;
                write_indexed_field(&mut tup, attno, wt, vv)?;
            }
        }
        _ => {
            return Err(format!(
                "pg_wasm: value shape does not match WIT composite marshal type ({mt:?})"
            ));
        }
    }
    tup.into_composite_datum()
        .ok_or_else(|| "pg_wasm: into_composite_datum returned NULL".to_string())
}

fn write_named_field(
    tup: &mut PgHeapTuple<'_, AllocatedByRust>,
    name: &str,
    mt: &MarshalType,
    val: &Val,
) -> Result<(), String> {
    if composite_layout::marshal_type_uses_composite_surface(mt) {
        let child_oid = field_typoid_named(tup, name)?;
        let child_datum = val_to_composite_datum(val, child_oid, mt)?;
        let child_tup = unsafe { PgHeapTuple::from_composite_datum(child_datum) };
        tup.set_by_name(name, child_tup)
            .map_err(|e| format!("pg_wasm: set composite field {name:?}: {e}"))?;
        return Ok(());
    }
    write_scalar_named(tup, name, mt, val)
}

fn write_indexed_field(
    tup: &mut PgHeapTuple<'_, AllocatedByRust>,
    attno: NonZeroUsize,
    mt: &MarshalType,
    val: &Val,
) -> Result<(), String> {
    if composite_layout::marshal_type_uses_composite_surface(mt) {
        let child_oid = field_typoid_at(tup, attno)?;
        let child_datum = val_to_composite_datum(val, child_oid, mt)?;
        let child_tup = unsafe { PgHeapTuple::from_composite_datum(child_datum) };
        tup.set_by_index(attno, child_tup)
            .map_err(|e| format!("pg_wasm: set composite attribute {attno}: {e}"))?;
        return Ok(());
    }
    write_scalar_indexed(tup, attno, mt, val)
}

fn field_typoid_named(
    tup: &PgHeapTuple<'_, AllocatedByRust>,
    name: &str,
) -> Result<pg_sys::Oid, String> {
    let (_, att) = tup
        .get_attribute_by_name(name)
        .ok_or_else(|| format!("pg_wasm: composite has no attribute {name:?}"))?;
    Ok(att.atttypid)
}

fn field_typoid_at(
    tup: &PgHeapTuple<'_, AllocatedByRust>,
    attno: NonZeroUsize,
) -> Result<pg_sys::Oid, String> {
    let att = tup
        .get_attribute_by_index(attno)
        .ok_or_else(|| format!("pg_wasm: composite has no attribute {attno}"))?;
    Ok(att.atttypid)
}

fn write_scalar_named(
    tup: &mut PgHeapTuple<'_, AllocatedByRust>,
    name: &str,
    mt: &MarshalType,
    val: &Val,
) -> Result<(), String> {
    match (mt, val) {
        (MarshalType::Bool, Val::Bool(b)) => tup
            .set_by_name(name, *b)
            .map_err(|e| format!("pg_wasm: set bool {name:?}: {e}")),
        (MarshalType::S8, Val::S8(x)) => tup
            .set_by_name(name, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set s8 {name:?}: {e}")),
        (MarshalType::U8, Val::U8(x)) => tup
            .set_by_name(name, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set u8 {name:?}: {e}")),
        (MarshalType::S16, Val::S16(x)) => tup
            .set_by_name(name, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set s16 {name:?}: {e}")),
        (MarshalType::U16, Val::U16(x)) => tup
            .set_by_name(name, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set u16 {name:?}: {e}")),
        (MarshalType::S32, Val::S32(x)) => tup
            .set_by_name(name, *x)
            .map_err(|e| format!("pg_wasm: set s32 {name:?}: {e}")),
        (MarshalType::U32, Val::U32(x)) => {
            let as_i32: i32 = (*x).try_into().map_err(|_| {
                format!("pg_wasm: u32 value {x} does not fit PostgreSQL int4 for field {name:?}")
            })?;
            tup.set_by_name(name, as_i32)
                .map_err(|e| format!("pg_wasm: set u32 {name:?}: {e}"))
        }
        (MarshalType::S64, Val::S64(x)) => tup
            .set_by_name(name, *x)
            .map_err(|e| format!("pg_wasm: set s64 {name:?}: {e}")),
        (MarshalType::U64, Val::U64(x)) => {
            let as_i64: i64 = (*x).try_into().map_err(|_| {
                format!("pg_wasm: u64 value {x} does not fit PostgreSQL int8 for field {name:?}")
            })?;
            tup.set_by_name(name, as_i64)
                .map_err(|e| format!("pg_wasm: set u64 {name:?}: {e}"))
        }
        (MarshalType::F32, Val::Float32(x)) => tup
            .set_by_name(name, *x)
            .map_err(|e| format!("pg_wasm: set f32 {name:?}: {e}")),
        (MarshalType::F64, Val::Float64(x)) => tup
            .set_by_name(name, *x)
            .map_err(|e| format!("pg_wasm: set f64 {name:?}: {e}")),
        (MarshalType::Char, Val::Char(c)) => tup
            .set_by_name(name, c.to_string())
            .map_err(|e| format!("pg_wasm: set char {name:?}: {e}")),
        (MarshalType::String, Val::String(s)) => tup
            .set_by_name(name, s.as_str())
            .map_err(|e| format!("pg_wasm: set string {name:?}: {e}")),
        _ => Err(format!(
            "pg_wasm: cannot write value {val:?} as SQL field {name:?} for WIT type {mt:?}"
        )),
    }
}

fn write_scalar_indexed(
    tup: &mut PgHeapTuple<'_, AllocatedByRust>,
    attno: NonZeroUsize,
    mt: &MarshalType,
    val: &Val,
) -> Result<(), String> {
    let idx = attno.get();
    match (mt, val) {
        (MarshalType::Bool, Val::Bool(b)) => tup
            .set_by_index(attno, *b)
            .map_err(|e| format!("pg_wasm: set bool attribute {idx}: {e}")),
        (MarshalType::S8, Val::S8(x)) => tup
            .set_by_index(attno, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set s8 attribute {idx}: {e}")),
        (MarshalType::U8, Val::U8(x)) => tup
            .set_by_index(attno, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set u8 attribute {idx}: {e}")),
        (MarshalType::S16, Val::S16(x)) => tup
            .set_by_index(attno, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set s16 attribute {idx}: {e}")),
        (MarshalType::U16, Val::U16(x)) => tup
            .set_by_index(attno, i32::from(*x))
            .map_err(|e| format!("pg_wasm: set u16 attribute {idx}: {e}")),
        (MarshalType::S32, Val::S32(x)) => tup
            .set_by_index(attno, *x)
            .map_err(|e| format!("pg_wasm: set s32 attribute {idx}: {e}")),
        (MarshalType::U32, Val::U32(x)) => {
            let as_i32: i32 = (*x).try_into().map_err(|_| {
                format!("pg_wasm: u32 value {x} does not fit PostgreSQL int4 for attribute {idx}")
            })?;
            tup.set_by_index(attno, as_i32)
                .map_err(|e| format!("pg_wasm: set u32 attribute {idx}: {e}"))
        }
        (MarshalType::S64, Val::S64(x)) => tup
            .set_by_index(attno, *x)
            .map_err(|e| format!("pg_wasm: set s64 attribute {idx}: {e}")),
        (MarshalType::U64, Val::U64(x)) => {
            let as_i64: i64 = (*x).try_into().map_err(|_| {
                format!("pg_wasm: u64 value {x} does not fit PostgreSQL int8 for attribute {idx}")
            })?;
            tup.set_by_index(attno, as_i64)
                .map_err(|e| format!("pg_wasm: set u64 attribute {idx}: {e}"))
        }
        (MarshalType::F32, Val::Float32(x)) => tup
            .set_by_index(attno, *x)
            .map_err(|e| format!("pg_wasm: set f32 attribute {idx}: {e}")),
        (MarshalType::F64, Val::Float64(x)) => tup
            .set_by_index(attno, *x)
            .map_err(|e| format!("pg_wasm: set f64 attribute {idx}: {e}")),
        (MarshalType::Char, Val::Char(c)) => tup
            .set_by_index(attno, c.to_string())
            .map_err(|e| format!("pg_wasm: set char attribute {idx}: {e}")),
        (MarshalType::String, Val::String(s)) => tup
            .set_by_index(attno, s.as_str())
            .map_err(|e| format!("pg_wasm: set string attribute {idx}: {e}")),
        _ => Err(format!(
            "pg_wasm: cannot write value {val:?} as SQL attribute {idx} for WIT type {mt:?}"
        )),
    }
}
