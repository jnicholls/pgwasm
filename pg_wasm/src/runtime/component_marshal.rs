//! JSON interchange for WIT `record` / `tuple` / `variant` / … mapped to PostgreSQL `jsonb`.
//!
//! Primitive component values use dedicated SQL types; complex shapes round-trip through
//! [`serde_json::Value`] and [`wasmtime::component::Val`].

use pgrx::pg_sys;

use wasmtime::component::Val;

use crate::composite_layout;
use crate::mapping::{MarshalType, PgWasmReturnDesc, PgWasmTypeKind};

/// Rust-side argument payload extracted from PostgreSQL before lowering to [`Val`].
#[derive(Clone, Debug)]
pub enum PreparedComponentArg {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Int32Array(Vec<i32>),
    StringArray(Vec<String>),
    Json(serde_json::Value),
    /// Pre-lifted composite argument (`record` / `tuple` SQL composite types).
    WasmVal(Val),
}

/// Return payload after lifting a component result, before building a [`pg_sys::Datum`].
#[derive(Clone, Debug)]
pub enum DynReturnPayload {
    Bool(bool),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Int32Array(Vec<i32>),
    StringArray(Vec<String>),
    Json(serde_json::Value),
    Datum(pg_sys::Datum),
}

pub fn args_to_vals(
    plan: &[MarshalType],
    args: &[PreparedComponentArg],
) -> Result<Vec<Val>, String> {
    if plan.len() != args.len() {
        return Err(format!(
            "pg_wasm: component call plan has {} params but {} arguments were prepared",
            plan.len(),
            args.len()
        ));
    }
    plan.iter()
        .zip(args)
        .map(|(m, a)| prepared_arg_to_val(a, m))
        .collect()
}

fn prepared_arg_to_val(arg: &PreparedComponentArg, m: &MarshalType) -> Result<Val, String> {
    match (arg, m) {
        (PreparedComponentArg::Bool(v), MarshalType::Bool) => Ok(Val::Bool(*v)),
        (PreparedComponentArg::I32(v), MarshalType::S8) => {
            Ok(Val::S8((*v).try_into().map_err(|_| {
                "pg_wasm: i32 out of range for WIT s8".to_string()
            })?))
        }
        (PreparedComponentArg::I32(v), MarshalType::U8) => {
            Ok(Val::U8((*v).try_into().map_err(|_| {
                "pg_wasm: i32 out of range for WIT u8".to_string()
            })?))
        }
        (PreparedComponentArg::I32(v), MarshalType::S16) => {
            Ok(Val::S16((*v).try_into().map_err(|_| {
                "pg_wasm: i32 out of range for WIT s16".to_string()
            })?))
        }
        (PreparedComponentArg::I32(v), MarshalType::U16) => {
            Ok(Val::U16((*v).try_into().map_err(|_| {
                "pg_wasm: i32 out of range for WIT u16".to_string()
            })?))
        }
        (PreparedComponentArg::I32(v), MarshalType::S32) => Ok(Val::S32(*v)),
        (PreparedComponentArg::I32(v), MarshalType::U32) => {
            Ok(Val::U32((*v).try_into().map_err(|_| {
                "pg_wasm: negative i32 cannot be passed as WIT u32".to_string()
            })?))
        }
        (PreparedComponentArg::I64(v), MarshalType::S64) => Ok(Val::S64(*v)),
        (PreparedComponentArg::I64(v), MarshalType::U64) => {
            Ok(Val::U64((*v).try_into().map_err(|_| {
                "pg_wasm: negative i64 cannot be passed as WIT u64".to_string()
            })?))
        }
        (PreparedComponentArg::F32(v), MarshalType::F32) => Ok(Val::Float32(*v)),
        (PreparedComponentArg::F64(v), MarshalType::F64) => Ok(Val::Float64(*v)),
        (PreparedComponentArg::String(s), MarshalType::Char) => {
            let mut it = s.chars();
            let ch = it
                .next()
                .ok_or_else(|| "pg_wasm: empty text for WIT char".to_string())?;
            if it.next().is_some() {
                return Err("pg_wasm: expected a single Unicode scalar for WIT char".into());
            }
            Ok(Val::Char(ch))
        }
        (PreparedComponentArg::String(s), MarshalType::String) => Ok(Val::String(s.clone())),
        (PreparedComponentArg::Bytes(b), MarshalType::List(inner))
            if matches!(inner.as_ref(), MarshalType::U8) =>
        {
            Ok(Val::List(b.iter().copied().map(Val::U8).collect()))
        }
        (PreparedComponentArg::Int32Array(xs), MarshalType::List(inner))
            if matches!(inner.as_ref(), MarshalType::S32 | MarshalType::U32) =>
        {
            let vals: Result<Vec<Val>, String> =
                xs.iter()
                    .map(|x| match inner.as_ref() {
                        MarshalType::S32 => Ok(Val::S32(*x)),
                        MarshalType::U32 => Ok(Val::U32((*x).try_into().map_err(|_| {
                            "pg_wasm: negative array element for WIT u32".to_string()
                        })?)),
                        _ => unreachable!(),
                    })
                    .collect();
            Ok(Val::List(vals?))
        }
        (PreparedComponentArg::StringArray(ss), MarshalType::List(inner))
            if matches!(inner.as_ref(), MarshalType::String) =>
        {
            Ok(Val::List(ss.iter().cloned().map(Val::String).collect()))
        }
        (PreparedComponentArg::Json(j), _) if json_interchange_marshal(m) => json_to_val(j, m),
        (PreparedComponentArg::WasmVal(v), _)
            if composite_layout::marshal_type_uses_composite_surface(m) =>
        {
            Ok(v.clone())
        }
        _ => Err(format!(
            "pg_wasm: SQL argument does not match WIT marshal type ({m:?})"
        )),
    }
}

fn json_interchange_marshal(m: &MarshalType) -> bool {
    matches!(
        m,
        MarshalType::Record(_)
            | MarshalType::Tuple(_)
            | MarshalType::Variant(_)
            | MarshalType::Option(_)
            | MarshalType::Result { .. }
            | MarshalType::Enum(_)
            | MarshalType::Flags(_)
    )
}

pub fn val_to_return_payload(
    v: Val,
    m: &MarshalType,
    ret: &PgWasmReturnDesc,
) -> Result<DynReturnPayload, String> {
    if ret.kind == PgWasmTypeKind::Composite
        && composite_layout::marshal_type_uses_composite_surface(m)
    {
        let d = crate::runtime::composite_marshal::val_to_composite_datum(&v, ret.pg_oid, m)?;
        return Ok(DynReturnPayload::Datum(d));
    }
    if json_interchange_marshal(m) {
        return Ok(DynReturnPayload::Json(val_to_json(&v, m)?));
    }
    match (v, m) {
        (Val::Bool(v), MarshalType::Bool) => Ok(DynReturnPayload::Bool(v)),
        (Val::S8(v), MarshalType::S8) => Ok(DynReturnPayload::I32(i32::from(v))),
        (Val::U8(v), MarshalType::U8) => Ok(DynReturnPayload::I32(i32::from(v))),
        (Val::S16(v), MarshalType::S16) => Ok(DynReturnPayload::I32(i32::from(v))),
        (Val::U16(v), MarshalType::U16) => Ok(DynReturnPayload::I32(i32::from(v))),
        (Val::S32(v), MarshalType::S32) => Ok(DynReturnPayload::I32(v)),
        (Val::U32(v), MarshalType::U32) => {
            Ok(DynReturnPayload::I32(v.try_into().map_err(|_| {
                "pg_wasm: u32 result does not fit int4".to_string()
            })?))
        }
        (Val::S64(v), MarshalType::S64) => Ok(DynReturnPayload::I64(v)),
        (Val::U64(v), MarshalType::U64) => {
            Ok(DynReturnPayload::I64(v.try_into().map_err(|_| {
                "pg_wasm: u64 result does not fit int8".to_string()
            })?))
        }
        (Val::Float32(v), MarshalType::F32) => Ok(DynReturnPayload::F32(v)),
        (Val::Float64(v), MarshalType::F64) => Ok(DynReturnPayload::F64(v)),
        (Val::Char(c), MarshalType::Char) => Ok(DynReturnPayload::String(c.to_string())),
        (Val::String(s), MarshalType::String) => Ok(DynReturnPayload::String(s)),
        (Val::List(items), MarshalType::List(inner))
            if matches!(inner.as_ref(), MarshalType::U8) =>
        {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    Val::U8(b) => out.push(b),
                    _ => return Err("pg_wasm: list<u8> result contains non-u8 element".into()),
                }
            }
            Ok(DynReturnPayload::Bytes(out))
        }
        (Val::List(items), MarshalType::List(inner))
            if matches!(inner.as_ref(), MarshalType::S32 | MarshalType::U32) =>
        {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                match (inner.as_ref(), it) {
                    (MarshalType::S32, Val::S32(x)) => out.push(x),
                    (MarshalType::U32, Val::U32(x)) => {
                        out.push(x.try_into().map_err(|_| {
                            "pg_wasm: u32 list element does not fit int4".to_string()
                        })?)
                    }
                    _ => return Err("pg_wasm: integer list result has wrong element type".into()),
                }
            }
            Ok(DynReturnPayload::Int32Array(out))
        }
        (Val::List(items), MarshalType::List(inner))
            if matches!(inner.as_ref(), MarshalType::String) =>
        {
            let mut out = Vec::with_capacity(items.len());
            for it in items {
                match it {
                    Val::String(s) => out.push(s),
                    _ => return Err("pg_wasm: list<string> result contains non-string".into()),
                }
            }
            Ok(DynReturnPayload::StringArray(out))
        }
        _ => Err(format!(
            "pg_wasm: component return value does not match expected WIT type ({m:?})"
        )),
    }
}

fn json_to_val(j: &serde_json::Value, m: &MarshalType) -> Result<Val, String> {
    Ok(match m {
        MarshalType::Record(fields) => {
            let obj = j
                .as_object()
                .ok_or_else(|| "pg_wasm: jsonb record expected JSON object".to_string())?;
            let mut pairs = Vec::with_capacity(fields.len());
            for (name, ty) in fields {
                let v = obj
                    .get(name)
                    .ok_or_else(|| format!("pg_wasm: jsonb record missing field {name:?}"))?;
                pairs.push((name.clone(), json_to_val(v, ty)?));
            }
            Val::Record(pairs)
        }
        MarshalType::Tuple(types) => {
            let arr = j
                .as_array()
                .ok_or_else(|| "pg_wasm: jsonb tuple expected JSON array".to_string())?;
            if arr.len() != types.len() {
                return Err(format!(
                    "pg_wasm: jsonb tuple length {} does not match WIT arity {}",
                    arr.len(),
                    types.len()
                ));
            }
            Val::Tuple(
                types
                    .iter()
                    .zip(arr.iter())
                    .map(|(t, v)| json_to_val(v, t))
                    .collect::<Result<_, _>>()?,
            )
        }
        MarshalType::Variant(cases) => {
            let obj = j
                .as_object()
                .ok_or_else(|| "pg_wasm: jsonb variant expected JSON object".to_string())?;
            let tag = obj
                .get("tag")
                .and_then(|t| t.as_str())
                .ok_or_else(|| "pg_wasm: jsonb variant missing string \"tag\"".to_string())?;
            let case_ty = cases
                .iter()
                .find(|(n, _)| n == tag)
                .ok_or_else(|| format!("pg_wasm: jsonb variant unknown tag {tag:?}"))?;
            let payload = match &case_ty.1 {
                None => {
                    if obj.contains_key("val") && !obj.get("val").unwrap().is_null() {
                        return Err(format!(
                            "pg_wasm: jsonb variant case {tag:?} expects no payload"
                        ));
                    }
                    None
                }
                Some(inner) => {
                    let v = obj.get("val").ok_or_else(|| {
                        format!("pg_wasm: jsonb variant case {tag:?} missing \"val\"")
                    })?;
                    Some(Box::new(json_to_val(v, inner)?))
                }
            };
            Val::Variant(tag.to_string(), payload)
        }
        MarshalType::Option(inner) => {
            if j.is_null() {
                Val::Option(None)
            } else {
                Val::Option(Some(Box::new(json_to_val(j, inner)?)))
            }
        }
        MarshalType::Result { ok, err } => {
            let obj = j
                .as_object()
                .ok_or_else(|| "pg_wasm: jsonb result expected JSON object".to_string())?;
            if let Some(okv) = obj.get("ok") {
                if obj.contains_key("err") {
                    return Err("pg_wasm: jsonb result must not contain both ok and err".into());
                }
                let lifted = match ok.as_ref() {
                    None if okv.is_null() => None,
                    None => {
                        return Err("pg_wasm: jsonb result ok branch is not a payload type".into());
                    }
                    Some(t) => Some(Box::new(json_to_val(okv, t)?)),
                };
                Val::Result(Ok(lifted))
            } else if let Some(errv) = obj.get("err") {
                let lifted = match err.as_ref() {
                    None if errv.is_null() => None,
                    None => {
                        return Err("pg_wasm: jsonb result err branch is not a payload type".into());
                    }
                    Some(t) => Some(Box::new(json_to_val(errv, t)?)),
                };
                Val::Result(Err(lifted))
            } else {
                return Err("pg_wasm: jsonb result needs ok or err key".into());
            }
        }
        MarshalType::Enum(names) => {
            let s = j
                .as_str()
                .ok_or_else(|| "pg_wasm: jsonb enum expected JSON string".to_string())?;
            if !names.iter().any(|n| n == s) {
                return Err(format!("pg_wasm: jsonb enum value {s:?} is not a case"));
            }
            Val::Enum(s.to_string())
        }
        MarshalType::Flags(names) => {
            let active: std::collections::HashSet<&str> = match j {
                serde_json::Value::Array(items) => items
                    .iter()
                    .map(|x| {
                        x.as_str().ok_or_else(|| {
                            "pg_wasm: jsonb flags array must contain strings".to_string()
                        })
                    })
                    .collect::<Result<_, _>>()?,
                _ => {
                    return Err("pg_wasm: jsonb flags expected JSON array of strings".into());
                }
            };
            let mut set: Vec<String> = Vec::new();
            for n in names {
                if active.contains(n.as_str()) {
                    set.push(n.clone());
                }
            }
            Val::Flags(set)
        }
        MarshalType::List(inner) => {
            let arr = j
                .as_array()
                .ok_or_else(|| "pg_wasm: jsonb list expected JSON array".to_string())?;
            Val::List(
                arr.iter()
                    .map(|e| json_to_val(e, inner))
                    .collect::<Result<_, _>>()?,
            )
        }
        MarshalType::Bool => Val::Bool(
            j.as_bool()
                .ok_or_else(|| "pg_wasm: jsonb bool expected".to_string())?,
        ),
        MarshalType::S8 => Val::S8(
            j.as_i64()
                .and_then(|n| i8::try_from(n).ok())
                .ok_or_else(|| "pg_wasm: jsonb s8 expected".to_string())?,
        ),
        MarshalType::U8 => Val::U8(
            j.as_u64()
                .and_then(|n| u8::try_from(n).ok())
                .ok_or_else(|| "pg_wasm: jsonb u8 expected".to_string())?,
        ),
        MarshalType::S16 => Val::S16(
            j.as_i64()
                .and_then(|n| i16::try_from(n).ok())
                .ok_or_else(|| "pg_wasm: jsonb s16 expected".to_string())?,
        ),
        MarshalType::U16 => Val::U16(
            j.as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .ok_or_else(|| "pg_wasm: jsonb u16 expected".to_string())?,
        ),
        MarshalType::S32 => Val::S32(
            j.as_i64()
                .and_then(|n| i32::try_from(n).ok())
                .ok_or_else(|| "pg_wasm: jsonb s32 expected".to_string())?,
        ),
        MarshalType::U32 => Val::U32(
            j.as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| "pg_wasm: jsonb u32 expected".to_string())?,
        ),
        MarshalType::S64 => Val::S64(
            j.as_i64()
                .ok_or_else(|| "pg_wasm: jsonb s64 expected".to_string())?,
        ),
        MarshalType::U64 => Val::U64(
            j.as_u64()
                .ok_or_else(|| "pg_wasm: jsonb u64 expected".to_string())?,
        ),
        MarshalType::F32 => Val::Float32(
            j.as_f64()
                .map(|x| x as f32)
                .ok_or_else(|| "pg_wasm: jsonb float32 expected".to_string())?,
        ),
        MarshalType::F64 => Val::Float64(
            j.as_f64()
                .ok_or_else(|| "pg_wasm: jsonb float64 expected".to_string())?,
        ),
        MarshalType::Char => {
            let s = j
                .as_str()
                .ok_or_else(|| "pg_wasm: jsonb char expected JSON string".to_string())?;
            let mut it = s.chars();
            let ch = it
                .next()
                .ok_or_else(|| "pg_wasm: jsonb char string is empty".to_string())?;
            if it.next().is_some() {
                return Err("pg_wasm: jsonb char must be one Unicode scalar".into());
            }
            Val::Char(ch)
        }
        MarshalType::String => Val::String(
            j.as_str()
                .ok_or_else(|| "pg_wasm: jsonb string expected".to_string())?
                .to_string(),
        ),
    })
}

fn val_to_json(v: &Val, m: &MarshalType) -> Result<serde_json::Value, String> {
    Ok(match (v, m) {
        (_, MarshalType::Record(fields)) => {
            let Val::Record(pairs) = v else {
                return Err("pg_wasm: internal error: Val/Record marshal mismatch".into());
            };
            if pairs.len() != fields.len() {
                return Err("pg_wasm: record value field count mismatch".into());
            }
            let mut obj = serde_json::Map::new();
            for ((exp_name, exp_ty), (got_name, got_val)) in fields.iter().zip(pairs.iter()) {
                if exp_name != got_name {
                    return Err(format!(
                        "pg_wasm: record field order mismatch (expected {exp_name}, got {got_name})"
                    ));
                }
                obj.insert(exp_name.clone(), val_to_json(got_val, exp_ty)?);
            }
            serde_json::Value::Object(obj)
        }
        (_, MarshalType::Tuple(types)) => {
            let Val::Tuple(items) = v else {
                return Err("pg_wasm: internal error: Val/Tuple marshal mismatch".into());
            };
            if items.len() != types.len() {
                return Err("pg_wasm: tuple value arity mismatch".into());
            }
            let arr: Result<Vec<_>, _> = types
                .iter()
                .zip(items.iter())
                .map(|(t, x)| val_to_json(x, t))
                .collect();
            serde_json::Value::Array(arr?)
        }
        (_, MarshalType::Variant(cases)) => {
            let Val::Variant(tag, payload) = v else {
                return Err("pg_wasm: internal error: Val/Variant marshal mismatch".into());
            };
            let case_ty = cases
                .iter()
                .find(|(n, _)| n == tag)
                .ok_or_else(|| format!("pg_wasm: unknown variant case {tag:?}"))?;
            let mut obj = serde_json::Map::new();
            obj.insert("tag".to_string(), serde_json::Value::String(tag.clone()));
            match (&case_ty.1, payload) {
                (None, None) => {}
                (Some(inner), Some(pv)) => {
                    obj.insert("val".to_string(), val_to_json(pv, inner)?);
                }
                (None, Some(_)) => {
                    return Err(format!(
                        "pg_wasm: variant case {tag:?} should have no payload"
                    ));
                }
                (Some(_), None) => {
                    return Err(format!("pg_wasm: variant case {tag:?} missing payload"));
                }
            }
            serde_json::Value::Object(obj)
        }
        (_, MarshalType::Option(inner)) => match v {
            Val::Option(None) => serde_json::Value::Null,
            Val::Option(Some(x)) => val_to_json(x, inner)?,
            _ => return Err("pg_wasm: Val/Option marshal mismatch".into()),
        },
        (_, MarshalType::Result { ok, err }) => match v {
            Val::Result(Ok(x)) => {
                let mut obj = serde_json::Map::new();
                let jv = match (ok.as_ref(), x) {
                    (None, None) => serde_json::Value::Null,
                    (Some(t), Some(x)) => val_to_json(x, t)?,
                    (None, Some(_)) | (Some(_), None) => {
                        return Err("pg_wasm: result ok payload mismatch".into());
                    }
                };
                obj.insert("ok".to_string(), jv);
                serde_json::Value::Object(obj)
            }
            Val::Result(Err(x)) => {
                let mut obj = serde_json::Map::new();
                let jv = match (err.as_ref(), x) {
                    (None, None) => serde_json::Value::Null,
                    (Some(t), Some(x)) => val_to_json(x, t)?,
                    (None, Some(_)) | (Some(_), None) => {
                        return Err("pg_wasm: result err payload mismatch".into());
                    }
                };
                obj.insert("err".to_string(), jv);
                serde_json::Value::Object(obj)
            }
            _ => return Err("pg_wasm: Val/Result marshal mismatch".into()),
        },
        (_, MarshalType::Enum(names)) => {
            let Val::Enum(tag) = v else {
                return Err("pg_wasm: Val/Enum marshal mismatch".into());
            };
            if !names.iter().any(|n| n == tag) {
                return Err(format!("pg_wasm: unknown enum case {tag:?}"));
            }
            serde_json::Value::String(tag.clone())
        }
        (_, MarshalType::Flags(declared)) => {
            let Val::Flags(active) = v else {
                return Err("pg_wasm: Val/Flags marshal mismatch".into());
            };
            let arr: Vec<serde_json::Value> = declared
                .iter()
                .filter(|n| active.iter().any(|a| a == *n))
                .cloned()
                .map(serde_json::Value::String)
                .collect();
            serde_json::Value::Array(arr)
        }
        (_, MarshalType::List(inner)) => {
            let Val::List(items) = v else {
                return Err("pg_wasm: Val/List marshal mismatch".into());
            };
            let arr: Result<Vec<_>, _> = items.iter().map(|x| val_to_json(x, inner)).collect();
            serde_json::Value::Array(arr?)
        }
        (Val::Bool(b), MarshalType::Bool) => serde_json::Value::Bool(*b),
        (Val::S8(x), MarshalType::S8) => serde_json::Value::from(i64::from(*x)),
        (Val::U8(x), MarshalType::U8) => serde_json::Value::from(*x),
        (Val::S16(x), MarshalType::S16) => serde_json::Value::from(i64::from(*x)),
        (Val::U16(x), MarshalType::U16) => serde_json::Value::from(*x),
        (Val::S32(x), MarshalType::S32) => serde_json::Value::from(*x),
        (Val::U32(x), MarshalType::U32) => serde_json::Value::from(*x),
        (Val::S64(x), MarshalType::S64) => serde_json::Value::from(*x),
        (Val::U64(x), MarshalType::U64) => serde_json::Value::from(*x),
        (Val::Float32(x), MarshalType::F32) => serde_json::Number::from_f64(f64::from(*x))
            .map(serde_json::Value::Number)
            .ok_or_else(|| "pg_wasm: f32 not representable in JSON".to_string())?,
        (Val::Float64(x), MarshalType::F64) => serde_json::Number::from_f64(*x)
            .map(serde_json::Value::Number)
            .ok_or_else(|| "pg_wasm: f64 not representable in JSON".to_string())?,
        (Val::String(s), MarshalType::String) => serde_json::Value::String(s.clone()),
        (Val::Char(c), MarshalType::Char) => serde_json::Value::String(c.to_string()),
        _ => {
            return Err(format!(
                "pg_wasm: val_to_json unsupported for this Val / {m:?} combination"
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    //! JSON `jsonb` path round-trips for [`MarshalType`] (PostgreSQL ↔ component dynamic calls).
    //!
    //! **Coverage checklist (arg and ret use the same encoding):**
    //!
    //! | Variant | Covered here |
    //! |---------|----------------|
    //! | Bool, S8–U64, F32, F64, Char, String | `json_all_scalar_variants_roundtrip` |
    //! | List(U8), List(S32), List(U32), List(String) | `json_list_variants_roundtrip` |
    //! | Record, Tuple | `json_record_roundtrip`, `json_empty_record_and_tuple`, `json_tuple_roundtrip` |
    //! | Variant, Option, Result, Enum, Flags | `json_variant_roundtrip`, `json_option_result_enum_flags_roundtrip` |
    //!
    //! Composite SQL encoding (Track A/B) is tested in `pg_test` and `composite_marshal` integration tests.

    use super::*;

    fn assert_json_roundtrip(m: &MarshalType, j: serde_json::Value) {
        let val = super::json_to_val(&j, m).expect("json_to_val");
        let back = super::val_to_json(&val, m).expect("val_to_json");
        assert_eq!(j, back, "marshal type {m:?}");
    }

    #[test]
    fn json_all_scalar_variants_roundtrip() {
        assert_json_roundtrip(&MarshalType::Bool, serde_json::json!(true));
        assert_json_roundtrip(&MarshalType::S8, serde_json::json!(-42));
        assert_json_roundtrip(&MarshalType::U8, serde_json::json!(200));
        assert_json_roundtrip(&MarshalType::S16, serde_json::json!(-1000));
        assert_json_roundtrip(&MarshalType::U16, serde_json::json!(50000));
        assert_json_roundtrip(&MarshalType::S32, serde_json::json!(-1_000_000));
        assert_json_roundtrip(&MarshalType::U32, serde_json::json!(3_000_000_000u64));
        assert_json_roundtrip(
            &MarshalType::S64,
            serde_json::json!(-9_223_372_036_854_775_808i64),
        );
        assert_json_roundtrip(
            &MarshalType::U64,
            serde_json::json!(18_446_744_073_709_551_615u64),
        );
        assert_json_roundtrip(&MarshalType::F32, serde_json::json!(1.5));
        assert_json_roundtrip(&MarshalType::F64, serde_json::json!(2.25));
        assert_json_roundtrip(&MarshalType::Char, serde_json::json!("π"));
        assert_json_roundtrip(&MarshalType::String, serde_json::json!("hello"));
    }

    #[test]
    fn json_list_variants_roundtrip() {
        let u8l = MarshalType::List(Box::new(MarshalType::U8));
        assert_json_roundtrip(&u8l, serde_json::json!([1, 2, 3]));

        let s32l = MarshalType::List(Box::new(MarshalType::S32));
        assert_json_roundtrip(&s32l, serde_json::json!([-1, 0, 2]));

        let u32l = MarshalType::List(Box::new(MarshalType::U32));
        assert_json_roundtrip(&u32l, serde_json::json!([0u64, 1u64]));

        let sl = MarshalType::List(Box::new(MarshalType::String));
        assert_json_roundtrip(&sl, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn json_record_roundtrip() {
        let m = MarshalType::Record(vec![
            ("x".into(), MarshalType::S32),
            ("y".into(), MarshalType::String),
        ]);
        assert_json_roundtrip(&m, serde_json::json!({"x": 1, "y": "hi"}));
    }

    #[test]
    fn json_empty_record_and_tuple() {
        let rec = MarshalType::Record(vec![]);
        assert_json_roundtrip(&rec, serde_json::json!({}));
        let tup = MarshalType::Tuple(vec![]);
        assert_json_roundtrip(&tup, serde_json::json!([]));
    }

    #[test]
    fn json_tuple_roundtrip() {
        let m = MarshalType::Tuple(vec![MarshalType::S32, MarshalType::Bool]);
        assert_json_roundtrip(&m, serde_json::json!([7, true]));
    }

    #[test]
    fn json_nested_record_in_record() {
        let inner = MarshalType::Record(vec![("n".into(), MarshalType::S32)]);
        let m = MarshalType::Record(vec![("r".into(), inner)]);
        assert_json_roundtrip(&m, serde_json::json!({"r": {"n": 99}}));
    }

    #[test]
    fn json_variant_roundtrip() {
        let m = MarshalType::Variant(vec![
            ("a".into(), None),
            ("b".into(), Some(MarshalType::S32)),
        ]);
        assert_json_roundtrip(&m, serde_json::json!({"tag": "a"}));
        assert_json_roundtrip(&m, serde_json::json!({"tag": "b", "val": 7}));
    }

    #[test]
    fn json_option_result_enum_flags_roundtrip() {
        let opt = MarshalType::Option(Box::new(MarshalType::S32));
        assert_json_roundtrip(&opt, serde_json::Value::Null);
        assert_json_roundtrip(&opt, serde_json::json!(42));

        let res_unit = MarshalType::Result {
            ok: None,
            err: None,
        };
        assert_json_roundtrip(&res_unit, serde_json::json!({"ok": null}));
        assert_json_roundtrip(&res_unit, serde_json::json!({"err": null}));

        let res_payload = MarshalType::Result {
            ok: Some(Box::new(MarshalType::S32)),
            err: Some(Box::new(MarshalType::String)),
        };
        assert_json_roundtrip(&res_payload, serde_json::json!({"ok": -3}));
        assert_json_roundtrip(&res_payload, serde_json::json!({"err": "oops"}));

        let en = MarshalType::Enum(vec!["red".into(), "green".into()]);
        assert_json_roundtrip(&en, serde_json::json!("green"));

        let fl = MarshalType::Flags(vec!["r".into(), "w".into(), "x".into()]);
        assert_json_roundtrip(&fl, serde_json::json!(["r", "x"]));
    }
}
