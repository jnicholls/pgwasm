//! Dynamic `wasmtime::component::Val` marshaling driven by a precomputed [`MarshalPlan`].
//!
//! Composite rows need a concrete composite type OID. When `pg_oid` is [`pg_sys::InvalidOid`],
//! record/variant/tuple marshaling returns [`PgWasmError::Unsupported`] until UDT registration
//! supplies OIDs.

#[cfg(feature = "pg_test")]
use std::ffi::CStr;
use std::{collections::HashMap, num::NonZeroUsize};

use pgrx::{
    AnyNumeric, WhoAllocated, heap_tuple::PgHeapTuple, pg_sys, prelude::*, tupdesc::PgTupleDesc,
};
use wasmtime::{
    Store,
    component::{Func, Val},
};

use super::list;
use crate::{
    errors::{PgWasmError, map_wasmtime_err},
    wit::{
        signature::{ExportSignature, TypeShape},
        typing::{CompositeField, PgType, TypePlan},
    },
};

/// One parameter or result slot: WIT `option<T>` maps to the same PostgreSQL type as `T` with
/// SQL NULL representing `none`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExportSlot {
    pub(crate) is_option: bool,
    pub(crate) pg_oid: pg_sys::Oid,
    pub(crate) pg_type: PgType,
}

/// Export surface used to derive parameter/result [`MarshalPlan`] rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Export {
    pub(crate) params: Vec<ExportSlot>,
    pub(crate) result: Option<ExportSlot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScalarKind {
    Bool,
    Char,
    F32,
    F64,
    S16,
    S32,
    S64,
    S8,
    String,
    U16,
    U32,
    U64,
    U8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FieldPlan {
    pub(crate) name: String,
    pub(crate) plan: Box<MarshalPlan>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CasePlan {
    pub(crate) name: String,
    pub(crate) payload: Option<Box<MarshalPlan>>,
}

/// Precomputed marshaling shape (mirrors WIT / `PgType` without holding `Resolve`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MarshalPlan {
    Scalar(ScalarKind),
    Record {
        fields: Vec<FieldPlan>,
        pg_oid: pg_sys::Oid,
    },
    Variant {
        cases: Vec<CasePlan>,
        pg_oid: pg_sys::Oid,
    },
    Enum(Vec<String>),
    Flags {
        names: Vec<String>,
    },
    Option(Box<MarshalPlan>),
    #[cfg(feature = "pg_test")]
    Result {
        ok: Box<MarshalPlan>,
        err: Box<MarshalPlan>,
    },
    Tuple {
        elements: Vec<MarshalPlan>,
        pg_oid: pg_sys::Oid,
    },
    /// `list<T>` for non-`u8` element types.
    List(Box<MarshalPlan>),
    /// `list<u8>` stored as PostgreSQL `bytea`.
    ListU8,
}

fn is_anonymous_tuple_shape(fields: &[CompositeField]) -> bool {
    fields
        .iter()
        .enumerate()
        .all(|(i, f)| f.name == format!("f{i}"))
}

/// Build marshalers for every parameter plus optional result type.
pub(crate) fn plan_marshaler(
    _plan: &TypePlan,
    export: &Export,
) -> Result<Vec<MarshalPlan>, PgWasmError> {
    let mut out = Vec::with_capacity(export.params.len() + export.result.as_ref().map_or(0, |_| 1));
    for p in &export.params {
        let inner = pg_type_to_marshal_plan(&p.pg_type, p.pg_oid)?;
        out.push(if p.is_option {
            MarshalPlan::Option(Box::new(inner))
        } else {
            inner
        });
    }
    if let Some(r) = &export.result {
        let inner = pg_type_to_marshal_plan(&r.pg_type, r.pg_oid)?;
        out.push(if r.is_option {
            MarshalPlan::Option(Box::new(inner))
        } else {
            inner
        });
    }
    Ok(out)
}

pub(crate) fn plan_marshaler_from_signature<F>(
    signature: &ExportSignature,
    mut type_oid: F,
) -> Result<Vec<MarshalPlan>, PgWasmError>
where
    F: FnMut(&str) -> Result<pg_sys::Oid, PgWasmError>,
{
    let mut out =
        Vec::with_capacity(signature.params.len() + signature.result.as_ref().map_or(0, |_| 1));
    for param in &signature.params {
        out.push(type_shape_to_marshal_plan(&param.ty, &mut type_oid)?);
    }
    if let Some(result) = &signature.result {
        out.push(type_shape_to_marshal_plan(result, &mut type_oid)?);
    }
    Ok(out)
}

fn type_shape_to_marshal_plan<F>(
    shape: &TypeShape,
    type_oid: &mut F,
) -> Result<MarshalPlan, PgWasmError>
where
    F: FnMut(&str) -> Result<pg_sys::Oid, PgWasmError>,
{
    Ok(match shape {
        TypeShape::Bool => MarshalPlan::Scalar(ScalarKind::Bool),
        TypeShape::Char => MarshalPlan::Scalar(ScalarKind::Char),
        TypeShape::F32 => MarshalPlan::Scalar(ScalarKind::F32),
        TypeShape::F64 => MarshalPlan::Scalar(ScalarKind::F64),
        TypeShape::S8 => MarshalPlan::Scalar(ScalarKind::S8),
        TypeShape::S16 => MarshalPlan::Scalar(ScalarKind::S16),
        TypeShape::S32 => MarshalPlan::Scalar(ScalarKind::S32),
        TypeShape::S64 => MarshalPlan::Scalar(ScalarKind::S64),
        TypeShape::String => MarshalPlan::Scalar(ScalarKind::String),
        TypeShape::U8 => MarshalPlan::Scalar(ScalarKind::U8),
        TypeShape::U16 => MarshalPlan::Scalar(ScalarKind::U16),
        TypeShape::U32 => MarshalPlan::Scalar(ScalarKind::U32),
        TypeShape::U64 => MarshalPlan::Scalar(ScalarKind::U64),
        TypeShape::Borrow { .. } | TypeShape::Own { .. } | TypeShape::Resource { .. } => {
            MarshalPlan::Scalar(ScalarKind::S64)
        }
        TypeShape::Enum { cases, .. } => MarshalPlan::Enum(cases.clone()),
        TypeShape::Flags { names, .. } => MarshalPlan::Flags {
            names: names.clone(),
        },
        TypeShape::List { element, .. } if matches!(element.as_ref(), TypeShape::U8) => {
            MarshalPlan::ListU8
        }
        TypeShape::List { element, .. } => {
            MarshalPlan::List(Box::new(type_shape_to_marshal_plan(element, type_oid)?))
        }
        TypeShape::Option { inner, .. } => {
            MarshalPlan::Option(Box::new(type_shape_to_marshal_plan(inner, type_oid)?))
        }
        TypeShape::Record { fields, type_key } => MarshalPlan::Record {
            fields: fields
                .iter()
                .map(|field| {
                    Ok::<FieldPlan, PgWasmError>(FieldPlan {
                        name: field.name.clone(),
                        plan: Box::new(type_shape_to_marshal_plan(&field.ty, type_oid)?),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            pg_oid: type_oid(type_key)?,
        },
        TypeShape::Tuple { elements, type_key } => MarshalPlan::Tuple {
            elements: elements
                .iter()
                .map(|element| type_shape_to_marshal_plan(element, type_oid))
                .collect::<Result<Vec<_>, _>>()?,
            pg_oid: type_oid(type_key)?,
        },
        TypeShape::TypeAlias { inner, .. } => type_shape_to_marshal_plan(inner, type_oid)?,
        TypeShape::Variant { cases, type_key } => MarshalPlan::Variant {
            cases: cases
                .iter()
                .map(|case| {
                    Ok::<CasePlan, PgWasmError>(CasePlan {
                        name: case.name.clone(),
                        payload: case
                            .payload
                            .as_ref()
                            .map(|payload| type_shape_to_marshal_plan(payload, type_oid))
                            .transpose()?
                            .map(Box::new),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            pg_oid: type_oid(type_key)?,
        },
        TypeShape::ErrorContext => {
            return Err(PgWasmError::Unsupported(
                "WIT error-context values cannot be marshaled as SQL text yet".to_string(),
            ));
        }
        TypeShape::FixedLengthList { .. }
        | TypeShape::Future { .. }
        | TypeShape::Map { .. }
        | TypeShape::Result { .. }
        | TypeShape::Stream { .. } => {
            return Err(PgWasmError::Unsupported(format!(
                "WIT shape `{shape:?}` is not supported for dynamic marshaling in this build"
            )));
        }
    })
}

fn pg_type_to_marshal_plan(
    pg: &PgType,
    composite_oid: pg_sys::Oid,
) -> Result<MarshalPlan, PgWasmError> {
    Ok(match pg {
        PgType::Scalar(name) => match *name {
            "boolean" => MarshalPlan::Scalar(ScalarKind::Bool),
            "double precision" => MarshalPlan::Scalar(ScalarKind::F64),
            "int2" => MarshalPlan::Scalar(ScalarKind::S16),
            "int4" => MarshalPlan::Scalar(ScalarKind::S32),
            "int8" => MarshalPlan::Scalar(ScalarKind::S64),
            "real" => MarshalPlan::Scalar(ScalarKind::F32),
            "text" => MarshalPlan::Scalar(ScalarKind::String),
            "bytea" => MarshalPlan::ListU8,
            other => {
                return Err(PgWasmError::Unsupported(format!(
                    "scalar PG type `{other}` is not supported for dynamic component marshaling"
                )));
            }
        },
        PgType::Array(inner) => {
            if matches!(inner.as_ref(), PgType::Scalar(s) if *s == "int4") {
                MarshalPlan::List(Box::new(MarshalPlan::Scalar(ScalarKind::S32)))
            } else if matches!(inner.as_ref(), PgType::Scalar(s) if *s == "int8") {
                MarshalPlan::List(Box::new(MarshalPlan::Scalar(ScalarKind::S64)))
            } else {
                return Err(PgWasmError::Unsupported(
                    "only list<int4>, list<int8>, and list<u8>/bytea are supported for list marshaling"
                        .to_string(),
                ));
            }
        }
        PgType::Composite(fields) if is_anonymous_tuple_shape(fields) => {
            let elements = fields
                .iter()
                .map(|f| pg_type_to_marshal_plan(&f.ty, composite_oid))
                .collect::<Result<Vec<_>, _>>()?;
            MarshalPlan::Tuple {
                elements,
                pg_oid: composite_oid,
            }
        }
        PgType::Composite(fields) => MarshalPlan::Record {
            fields: fields
                .iter()
                .map(|f| {
                    Ok::<FieldPlan, PgWasmError>(FieldPlan {
                        name: f.name.clone(),
                        plan: Box::new(pg_type_to_marshal_plan(&f.ty, composite_oid)?),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            pg_oid: composite_oid,
        },
        PgType::Enum(names) => MarshalPlan::Enum(names.clone()),
        PgType::Variant(cases) => MarshalPlan::Variant {
            cases: cases
                .iter()
                .map(|c| {
                    Ok::<CasePlan, PgWasmError>(CasePlan {
                        name: c.name.clone(),
                        payload: c
                            .payload
                            .as_ref()
                            .map(|p| pg_type_to_marshal_plan(p, composite_oid))
                            .transpose()?
                            .map(Box::new),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            pg_oid: composite_oid,
        },
        PgType::Domain {
            base,
            flag_names: Some(names),
            ..
        } if *base == "int4" => MarshalPlan::Flags {
            names: names.clone(),
        },
        PgType::Domain { base, .. } if *base == "int2" => MarshalPlan::Scalar(ScalarKind::S16),
        PgType::Domain { base, .. } if *base == "int4" => MarshalPlan::Scalar(ScalarKind::S32),
        PgType::Domain { base, .. } if *base == "int8" => MarshalPlan::Scalar(ScalarKind::S64),
        PgType::Domain { base, .. } if *base == "numeric" => MarshalPlan::Scalar(ScalarKind::U64),
        PgType::Domain { base, .. } if *base == "text" => MarshalPlan::Scalar(ScalarKind::Char),
        PgType::Domain { .. } => {
            return Err(PgWasmError::Unsupported(
                "domain types (other than flags/int4) are not supported for dynamic marshaling"
                    .to_string(),
            ));
        }
    })
}

pub(crate) fn datum_to_val(
    plan: &MarshalPlan,
    datum: pg_sys::Datum,
    is_null: bool,
    oid: pg_sys::Oid,
) -> Result<Val, PgWasmError> {
    match plan {
        MarshalPlan::Option(inner) => {
            if is_null {
                return Ok(Val::Option(None));
            }
            Ok(Val::Option(Some(Box::new(datum_to_val(
                inner, datum, false, oid,
            )?))))
        }
        MarshalPlan::ListU8 => {
            if is_null {
                return Err(PgWasmError::ValidationFailed(
                    "SQL NULL is not valid for non-option list<u8>".to_string(),
                ));
            }
            list::bytea_datum_to_u8_list(datum, false)
        }
        MarshalPlan::List(inner) => {
            if is_null {
                return Err(PgWasmError::ValidationFailed(
                    "SQL NULL is not valid for non-option list".to_string(),
                ));
            }
            if matches!(inner.as_ref(), MarshalPlan::Scalar(ScalarKind::S32)) {
                return list::array_datum_to_list_i32(datum, false, |v| Ok(Val::S32(v)));
            }
            if matches!(inner.as_ref(), MarshalPlan::Scalar(ScalarKind::S64)) {
                return list::array_datum_to_list_i64(datum, false, |v| Ok(Val::S64(v)));
            }
            Err(PgWasmError::Unsupported(
                "list element type is not supported for this marshaling plan".to_string(),
            ))
        }
        _ => {
            if is_null {
                return Err(PgWasmError::ValidationFailed(
                    "SQL NULL is not valid for this Wasm type".to_string(),
                ));
            }
            datum_to_val_non_null(plan, datum, oid)
        }
    }
}

fn read_heap_attr_val<A: WhoAllocated>(
    tup: &PgHeapTuple<'_, A>,
    plan: &MarshalPlan,
    attno: NonZeroUsize,
) -> Result<Val, PgWasmError> {
    match plan {
        MarshalPlan::Option(inner) => {
            let v = read_optional_field_val(tup, inner, attno)?;
            Ok(Val::Option(v.map(Box::new)))
        }
        _ => read_optional_field_val(tup, plan, attno)?.ok_or_else(|| {
            PgWasmError::ValidationFailed("composite field is SQL NULL".to_string())
        }),
    }
}

fn read_optional_field_val<A: WhoAllocated>(
    tup: &PgHeapTuple<'_, A>,
    plan: &MarshalPlan,
    attno: NonZeroUsize,
) -> Result<Option<Val>, PgWasmError> {
    read_scalar_attr(tup, plan, attno, "")
}

fn read_scalar_attr<A: WhoAllocated>(
    tup: &PgHeapTuple<'_, A>,
    plan: &MarshalPlan,
    attno: NonZeroUsize,
    context: &str,
) -> Result<Option<Val>, PgWasmError> {
    let ctx = |error| {
        if context.is_empty() {
            PgWasmError::Internal(format!("{error}"))
        } else {
            PgWasmError::Internal(format!("{context}: {error}"))
        }
    };
    match plan {
        MarshalPlan::Scalar(ScalarKind::Bool) => Ok(tup
            .get_by_index::<bool>(attno)
            .map_err(&ctx)?
            .map(Val::Bool)),
        MarshalPlan::Scalar(ScalarKind::Char) => {
            let value = tup.get_by_index::<String>(attno).map_err(&ctx)?;
            value.map(|s| string_to_char_val(&s)).transpose()
        }
        MarshalPlan::Scalar(ScalarKind::F32) => Ok(tup
            .get_by_index::<f32>(attno)
            .map_err(&ctx)?
            .map(Val::Float32)),
        MarshalPlan::Scalar(ScalarKind::F64) => Ok(tup
            .get_by_index::<f64>(attno)
            .map_err(&ctx)?
            .map(Val::Float64)),
        MarshalPlan::Scalar(ScalarKind::S8) => {
            let value = tup.get_by_index::<i16>(attno).map_err(&ctx)?;
            value
                .map(|v| {
                    i8::try_from(v).map(Val::S8).map_err(|_| {
                        PgWasmError::ValidationFailed(format!(
                            "int2 value {v} is out of range for WIT s8"
                        ))
                    })
                })
                .transpose()
        }
        MarshalPlan::Scalar(ScalarKind::S16) => {
            Ok(tup.get_by_index::<i16>(attno).map_err(&ctx)?.map(Val::S16))
        }
        MarshalPlan::Scalar(ScalarKind::S32) => {
            Ok(tup.get_by_index::<i32>(attno).map_err(&ctx)?.map(Val::S32))
        }
        MarshalPlan::Scalar(ScalarKind::S64) => {
            Ok(tup.get_by_index::<i64>(attno).map_err(&ctx)?.map(Val::S64))
        }
        MarshalPlan::Scalar(ScalarKind::String) => Ok(tup
            .get_by_index::<String>(attno)
            .map_err(&ctx)?
            .map(Val::String)),
        MarshalPlan::Scalar(ScalarKind::U8) => {
            let value = tup.get_by_index::<i16>(attno).map_err(&ctx)?;
            value
                .map(|v| {
                    u8::try_from(v).map(Val::U8).map_err(|_| {
                        PgWasmError::ValidationFailed(format!(
                            "int2 value {v} is out of range for WIT u8"
                        ))
                    })
                })
                .transpose()
        }
        MarshalPlan::Scalar(ScalarKind::U16) => {
            let value = tup.get_by_index::<i32>(attno).map_err(&ctx)?;
            value
                .map(|v| {
                    u16::try_from(v).map(Val::U16).map_err(|_| {
                        PgWasmError::ValidationFailed(format!(
                            "int4 value {v} is out of range for WIT u16"
                        ))
                    })
                })
                .transpose()
        }
        MarshalPlan::Scalar(ScalarKind::U32) => {
            let value = tup.get_by_index::<i64>(attno).map_err(&ctx)?;
            value
                .map(|v| {
                    u32::try_from(v).map(Val::U32).map_err(|_| {
                        PgWasmError::ValidationFailed(format!(
                            "int8 value {v} is out of range for WIT u32"
                        ))
                    })
                })
                .transpose()
        }
        MarshalPlan::Scalar(ScalarKind::U64) => {
            let value = tup.get_by_index::<AnyNumeric>(attno).map_err(&ctx)?;
            value
                .map(|v| {
                    u64::try_from(v).map(Val::U64).map_err(|error| {
                        PgWasmError::ValidationFailed(format!(
                            "numeric value is out of range for WIT u64: {error}"
                        ))
                    })
                })
                .transpose()
        }
        _ => Err(PgWasmError::Unsupported(
            "variant payload read supports only scalar types in this build".to_string(),
        )),
    }
}

fn read_tuple_element<A: WhoAllocated>(
    tup: &PgHeapTuple<'_, A>,
    plan: &MarshalPlan,
    attno: NonZeroUsize,
) -> Result<Val, PgWasmError> {
    read_heap_attr_val(tup, plan, attno)
}

#[cfg(feature = "pg_test")]
fn read_field_as_val<A: WhoAllocated>(
    tup: &PgHeapTuple<'_, A>,
    plan: &MarshalPlan,
    attno: NonZeroUsize,
) -> Result<Option<Val>, PgWasmError> {
    read_scalar_attr(tup, plan, attno, "result field read")
}

#[cfg(feature = "pg_test")]
fn read_result_fields_from_heap_tuple<A: WhoAllocated>(
    tup: PgHeapTuple<'_, A>,
    ok_plan: &MarshalPlan,
    err_plan: &MarshalPlan,
) -> Result<Val, PgWasmError> {
    let ok_v = read_field_as_val(&tup, ok_plan, NonZeroUsize::new(1).unwrap())?;
    let err_v = read_field_as_val(&tup, err_plan, NonZeroUsize::new(2).unwrap())?;
    match (ok_v, err_v) {
        (Some(ov), None) => Ok(Val::Result(Ok(Some(Box::new(ov))))),
        (None, Some(ev)) => Ok(Val::Result(Err(Some(Box::new(ev))))),
        (None, None) => Err(PgWasmError::ValidationFailed(
            "WIT result composite must set exactly one of ok/err".to_string(),
        )),
        (Some(_), Some(_)) => Err(PgWasmError::ValidationFailed(
            "WIT result composite must set exactly one of ok/err".to_string(),
        )),
    }
}

fn datum_to_val_non_null(
    plan: &MarshalPlan,
    datum: pg_sys::Datum,
    _oid: pg_sys::Oid,
) -> Result<Val, PgWasmError> {
    match plan {
        MarshalPlan::Scalar(kind) => scalar_datum_to_val(kind, datum),
        MarshalPlan::Enum(names) => {
            let label = unsafe { String::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: enum label (text) could not be read".to_string())
            })?;
            if !names.iter().any(|n| n == &label) {
                return Err(PgWasmError::ValidationFailed(format!(
                    "enum label `{label}` is not one of the declared cases"
                )));
            }
            Ok(Val::Enum(label))
        }
        MarshalPlan::Flags { names } => {
            let bits = unsafe { i32::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal(
                    "marshaling: flags domain (int4) could not be read".to_string(),
                )
            })?;
            Ok(Val::Flags(names_for_flag_bits(names, bits)?))
        }
        #[cfg(feature = "pg_test")]
        MarshalPlan::Result { ok, err } => {
            let tup = unsafe { PgHeapTuple::from_composite_datum(datum) };
            read_result_fields_from_heap_tuple(tup, ok, err)
        }
        MarshalPlan::Record { fields, pg_oid } => {
            if *pg_oid == pg_sys::InvalidOid {
                return Err(PgWasmError::Unsupported(
                    "record marshaling requires a registered composite type OID (UDT phase)"
                        .to_string(),
                ));
            }
            let tup = unsafe { PgHeapTuple::from_composite_datum(datum) };
            let mut pairs = Vec::with_capacity(fields.len());
            for f in fields {
                let (attno, _) = tup.get_attribute_by_name(f.name.as_str()).ok_or_else(|| {
                    PgWasmError::Internal(format!(
                        "record field `{}` not in tuple descriptor",
                        f.name
                    ))
                })?;
                let val = read_heap_attr_val(&tup, f.plan.as_ref(), attno)
                    .map_err(|e| PgWasmError::Internal(format!("record field {}: {e}", f.name)))?;
                pairs.push((f.name.clone(), val));
            }
            Ok(Val::Record(pairs))
        }
        MarshalPlan::Tuple { elements, pg_oid } => {
            if *pg_oid == pg_sys::InvalidOid {
                return Err(PgWasmError::Unsupported(
                    "tuple marshaling requires a concrete composite type OID".to_string(),
                ));
            }
            let tup = unsafe { PgHeapTuple::from_composite_datum(datum) };
            let mut items = Vec::with_capacity(elements.len());
            for (i, elem_plan) in elements.iter().enumerate() {
                let attr = NonZeroUsize::new(i + 1).unwrap();
                let v = read_tuple_element(&tup, elem_plan, attr)
                    .map_err(|e| PgWasmError::Internal(format!("tuple element {i}: {e}")))?;
                items.push(v);
            }
            Ok(Val::Tuple(items))
        }
        MarshalPlan::Variant { cases, pg_oid } => {
            if *pg_oid == pg_sys::InvalidOid {
                return Err(PgWasmError::Unsupported(
                    "variant marshaling requires a registered composite type OID (UDT phase)"
                        .to_string(),
                ));
            }
            let tup = unsafe { PgHeapTuple::from_composite_datum(datum) };
            let disc: String = tup
                .get_by_index(NonZeroUsize::new(1).unwrap())
                .map_err(|e| PgWasmError::Internal(format!("variant discriminant: {e}")))?
                .ok_or_else(|| {
                    PgWasmError::ValidationFailed(
                        "variant composite missing discriminant".to_string(),
                    )
                })?;
            let case = cases.iter().find(|c| c.name == disc).ok_or_else(|| {
                PgWasmError::ValidationFailed(format!("unknown variant case `{disc}`"))
            })?;
            let payload_val = match &case.payload {
                None => None,
                Some(plan) => read_optional_field_val(&tup, plan, NonZeroUsize::new(2).unwrap())
                    .map_err(|e| PgWasmError::Internal(format!("variant payload: {e}")))?
                    .map(Box::new),
            };
            Ok(Val::Variant(disc, payload_val))
        }
        MarshalPlan::Option(_) | MarshalPlan::List(_) | MarshalPlan::ListU8 => Err(
            PgWasmError::Internal("datum_to_val_non_null: unexpected plan".to_string()),
        ),
    }
}

fn scalar_datum_to_val(kind: &ScalarKind, datum: pg_sys::Datum) -> Result<Val, PgWasmError> {
    Ok(match kind {
        ScalarKind::Bool => {
            let v = unsafe { bool::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: boolean datum could not be read".to_string())
            })?;
            Val::Bool(v)
        }
        ScalarKind::Char => {
            let s = unsafe { String::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: char text datum could not be read".to_string())
            })?;
            string_to_char_val(&s)?
        }
        ScalarKind::F32 => {
            let v = unsafe { f32::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: real datum could not be read".to_string())
            })?;
            Val::Float32(v)
        }
        ScalarKind::F64 => {
            let v = unsafe { f64::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal(
                    "marshaling: double precision datum could not be read".to_string(),
                )
            })?;
            Val::Float64(v)
        }
        ScalarKind::S8 => {
            let v = unsafe { i16::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int2 datum could not be read".to_string())
            })?;
            Val::S8(i8::try_from(v).map_err(|_| {
                PgWasmError::ValidationFailed(format!("int2 value {v} is out of range for WIT s8"))
            })?)
        }
        ScalarKind::S16 => {
            let v = unsafe { i16::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int2 datum could not be read".to_string())
            })?;
            Val::S16(v)
        }
        ScalarKind::S32 => {
            let v = unsafe { i32::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int4 datum could not be read".to_string())
            })?;
            Val::S32(v)
        }
        ScalarKind::S64 => {
            let v = unsafe { i64::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int8 datum could not be read".to_string())
            })?;
            Val::S64(v)
        }
        ScalarKind::String => {
            let v = unsafe { String::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: text datum could not be read".to_string())
            })?;
            Val::String(v)
        }
        ScalarKind::U8 => {
            let v = unsafe { i16::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int2 datum could not be read".to_string())
            })?;
            Val::U8(u8::try_from(v).map_err(|_| {
                PgWasmError::ValidationFailed(format!("int2 value {v} is out of range for WIT u8"))
            })?)
        }
        ScalarKind::U16 => {
            let v = unsafe { i32::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int4 datum could not be read".to_string())
            })?;
            Val::U16(u16::try_from(v).map_err(|_| {
                PgWasmError::ValidationFailed(format!("int4 value {v} is out of range for WIT u16"))
            })?)
        }
        ScalarKind::U32 => {
            let v = unsafe { i64::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: int8 datum could not be read".to_string())
            })?;
            Val::U32(u32::try_from(v).map_err(|_| {
                PgWasmError::ValidationFailed(format!("int8 value {v} is out of range for WIT u32"))
            })?)
        }
        ScalarKind::U64 => {
            let v = unsafe { AnyNumeric::from_datum(datum, false) }.ok_or_else(|| {
                PgWasmError::Internal("marshaling: numeric datum could not be read".to_string())
            })?;
            Val::U64(u64::try_from(v).map_err(|error| {
                PgWasmError::ValidationFailed(format!(
                    "numeric value is out of range for WIT u64: {error}"
                ))
            })?)
        }
    })
}

fn scalar_val_to_datum(
    kind: &ScalarKind,
    value: &Val,
) -> Result<(pg_sys::Datum, bool), PgWasmError> {
    let datum = match (kind, value) {
        (ScalarKind::Bool, Val::Bool(v)) => v.into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build boolean datum".to_string())
        })?,
        (ScalarKind::Char, Val::Char(v)) => v.to_string().into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build text datum for char".to_string())
        })?,
        (ScalarKind::F32, Val::Float32(v)) => v.into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build real datum".to_string())
        })?,
        (ScalarKind::F64, Val::Float64(v)) => v.into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build double precision datum".to_string())
        })?,
        (ScalarKind::S8, Val::S8(v)) => i16::from(*v).into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int2 datum for s8".to_string())
        })?,
        (ScalarKind::S16, Val::S16(v)) => v.into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int2 datum".to_string())
        })?,
        (ScalarKind::S32, Val::S32(v)) => v.into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int4 datum".to_string())
        })?,
        (ScalarKind::S64, Val::S64(v)) => v.into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int8 datum".to_string())
        })?,
        (ScalarKind::String, Val::String(v)) => v.clone().into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build text datum".to_string())
        })?,
        (ScalarKind::U8, Val::U8(v)) => i16::from(*v).into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int2 datum for u8".to_string())
        })?,
        (ScalarKind::U16, Val::U16(v)) => i32::from(*v).into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int4 datum for u16".to_string())
        })?,
        (ScalarKind::U32, Val::U32(v)) => i64::from(*v).into_datum().ok_or_else(|| {
            PgWasmError::Internal("marshaling: failed to build int8 datum for u32".to_string())
        })?,
        (ScalarKind::U64, Val::U64(v)) => AnyNumeric::try_from(*v)
            .map_err(|error| {
                PgWasmError::ValidationFailed(format!(
                    "failed to convert WIT u64 to numeric: {error}"
                ))
            })?
            .into_datum()
            .ok_or_else(|| {
                PgWasmError::Internal("marshaling: failed to build numeric datum".to_string())
            })?,
        _ => {
            return Err(PgWasmError::ValidationFailed(
                "Wasm value shape does not match scalar marshaling plan".to_string(),
            ));
        }
    };
    Ok((datum, false))
}

fn string_to_char_val(value: &str) -> Result<Val, PgWasmError> {
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(PgWasmError::ValidationFailed(
            "text value is empty; expected one Unicode scalar for WIT char".to_string(),
        ));
    };
    if chars.next().is_some() {
        return Err(PgWasmError::ValidationFailed(format!(
            "text value `{value}` contains more than one Unicode scalar for WIT char"
        )));
    }
    Ok(Val::Char(ch))
}

fn names_for_flag_bits(names: &[String], bits: i32) -> Result<Vec<String>, PgWasmError> {
    let mut out = Vec::new();
    for (i, n) in names.iter().enumerate() {
        if i >= 32 {
            return Err(PgWasmError::Unsupported(
                "flags with more than 32 members are not supported".to_string(),
            ));
        }
        if (bits & (1 << i)) != 0 {
            out.push(n.clone());
        }
    }
    if bits != 0 {
        let mask = if names.len() >= 32 {
            -1i32
        } else {
            (1i32 << names.len()) - 1
        };
        if bits & !mask != 0 {
            return Err(PgWasmError::ValidationFailed(
                "flags integer contains bits outside the declared shape".to_string(),
            ));
        }
    }
    Ok(out)
}

fn flag_bits_for_names(names: &[String], set: &[String]) -> Result<i32, PgWasmError> {
    let mut bits: i32 = 0;
    let index: HashMap<&str, usize> = names
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i))
        .collect();
    for s in set {
        let i = index.get(s.as_str()).ok_or_else(|| {
            PgWasmError::ValidationFailed(format!(
                "unknown flag name `{s}` for this WIT flags type"
            ))
        })?;
        if *i >= 32 {
            return Err(PgWasmError::Unsupported(
                "flags with more than 32 members are not supported".to_string(),
            ));
        }
        bits |= 1i32 << (*i as i32);
    }
    Ok(bits)
}

pub(crate) fn val_to_datum(
    plan: &MarshalPlan,
    val: &Val,
) -> Result<(pg_sys::Datum, bool), PgWasmError> {
    match (plan, val) {
        (MarshalPlan::Option(_inner), Val::Option(None)) => Ok((pg_sys::Datum::from(0usize), true)),
        (MarshalPlan::Option(inner), Val::Option(Some(v))) => val_to_datum(inner, v),
        (MarshalPlan::Scalar(kind), value) => scalar_val_to_datum(kind, value),
        (MarshalPlan::Enum(names), Val::Enum(label)) => {
            if !names.iter().any(|n| n == label) {
                return Err(PgWasmError::ValidationFailed(format!(
                    "enum label `{label}` is not one of the declared cases"
                )));
            }
            let d = label.clone().into_datum().ok_or_else(|| {
                PgWasmError::Internal("marshaling: failed to build text datum for enum".to_string())
            })?;
            Ok((d, false))
        }
        (MarshalPlan::Flags { names }, Val::Flags(set)) => {
            let bits = flag_bits_for_names(names, set)?;
            let d = bits.into_datum().ok_or_else(|| {
                PgWasmError::Internal(
                    "marshaling: failed to build int4 datum for flags".to_string(),
                )
            })?;
            Ok((d, false))
        }
        #[cfg(feature = "pg_test")]
        (MarshalPlan::Result { ok, err }, Val::Result(Ok(maybe))) => {
            build_result_composite(ok, err, maybe.as_deref(), true)
        }
        #[cfg(feature = "pg_test")]
        (MarshalPlan::Result { ok, err }, Val::Result(Err(maybe))) => {
            build_result_composite(ok, err, maybe.as_deref(), false)
        }
        (MarshalPlan::List(inner), v @ Val::List(_)) => {
            if matches!(inner.as_ref(), MarshalPlan::Scalar(ScalarKind::S32)) {
                return list::list_val_to_int4_array(v);
            }
            if matches!(inner.as_ref(), MarshalPlan::Scalar(ScalarKind::S64)) {
                return list::list_val_to_int8_array(v);
            }
            Err(PgWasmError::Unsupported(
                "list element type is not supported for this marshaling plan".to_string(),
            ))
        }
        (MarshalPlan::ListU8, v @ Val::List(_)) => list::u8_list_val_to_bytea(v),
        (MarshalPlan::Record { fields, pg_oid }, Val::Record(pairs)) => {
            if *pg_oid == pg_sys::InvalidOid {
                return Err(PgWasmError::Unsupported(
                    "record marshaling requires a registered composite type OID (UDT phase)"
                        .to_string(),
                ));
            }
            let map: HashMap<&str, &Val> = pairs.iter().map(|(k, v)| (k.as_str(), v)).collect();
            let datums: Vec<Option<pg_sys::Datum>> = fields
                .iter()
                .map(|f| {
                    let v = map.get(f.name.as_str()).ok_or_else(|| {
                        PgWasmError::ValidationFailed(format!("missing record field `{}`", f.name))
                    })?;
                    let (d, n) = val_to_datum(&f.plan, v)?;
                    Ok::<Option<pg_sys::Datum>, PgWasmError>(if n { None } else { Some(d) })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let tupdesc = PgTupleDesc::for_composite_type_by_oid(*pg_oid).ok_or_else(|| {
                PgWasmError::Internal("marshaling: composite type lookup failed".to_string())
            })?;
            let tup = unsafe { PgHeapTuple::from_datums(tupdesc, datums) }
                .map_err(|e| PgWasmError::Internal(format!("record heap_form_tuple: {e}")))?;
            let d = tup.into_composite_datum().ok_or_else(|| {
                PgWasmError::Internal("marshaling: failed to build composite datum".to_string())
            })?;
            Ok((d, false))
        }
        (MarshalPlan::Tuple { elements, pg_oid }, Val::Tuple(items)) => {
            if *pg_oid == pg_sys::InvalidOid {
                return Err(PgWasmError::Unsupported(
                    "tuple marshaling requires a concrete composite type OID".to_string(),
                ));
            }
            if elements.len() != items.len() {
                return Err(PgWasmError::ValidationFailed(
                    "tuple length does not match marshaling plan".to_string(),
                ));
            }
            let datums: Vec<Option<pg_sys::Datum>> = elements
                .iter()
                .zip(items.iter())
                .map(|(plan_elem, val_elem)| {
                    let (d, n) = val_to_datum(plan_elem, val_elem)?;
                    Ok::<Option<pg_sys::Datum>, PgWasmError>(if n { None } else { Some(d) })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let tupdesc = PgTupleDesc::for_composite_type_by_oid(*pg_oid).ok_or_else(|| {
                PgWasmError::Internal("marshaling: tuple composite type lookup failed".to_string())
            })?;
            let tup = unsafe { PgHeapTuple::from_datums(tupdesc, datums) }
                .map_err(|e| PgWasmError::Internal(format!("tuple heap_form_tuple: {e}")))?;
            let d = tup.into_composite_datum().ok_or_else(|| {
                PgWasmError::Internal(
                    "marshaling: failed to build tuple composite datum".to_string(),
                )
            })?;
            Ok((d, false))
        }
        (MarshalPlan::Variant { cases, pg_oid }, Val::Variant(name, payload)) => {
            if *pg_oid == pg_sys::InvalidOid {
                return Err(PgWasmError::Unsupported(
                    "variant marshaling requires a registered composite type OID (UDT phase)"
                        .to_string(),
                ));
            }
            let case = cases.iter().find(|c| c.name == *name).ok_or_else(|| {
                PgWasmError::ValidationFailed(format!("unknown variant case `{name}`"))
            })?;
            let (disc_d, _) = val_to_datum(
                &MarshalPlan::Scalar(ScalarKind::String),
                &Val::String(name.clone()),
            )?;
            let payload_datum: Option<pg_sys::Datum> = match (&case.payload, payload) {
                (None, None) => None,
                (Some(plan), Some(v)) => Some(val_to_datum(plan, v)?.0),
                _ => {
                    return Err(PgWasmError::ValidationFailed(
                        "variant payload presence does not match case definition".to_string(),
                    ));
                }
            };
            let datums = vec![Some(disc_d), payload_datum];
            let tupdesc = PgTupleDesc::for_composite_type_by_oid(*pg_oid).ok_or_else(|| {
                PgWasmError::Internal(
                    "marshaling: variant composite type lookup failed".to_string(),
                )
            })?;
            let tup = unsafe { PgHeapTuple::from_datums(tupdesc, datums) }
                .map_err(|e| PgWasmError::Internal(format!("variant heap_form_tuple: {e}")))?;
            let d = tup.into_composite_datum().ok_or_else(|| {
                PgWasmError::Internal(
                    "marshaling: failed to build variant composite datum".to_string(),
                )
            })?;
            Ok((d, false))
        }
        _ => Err(PgWasmError::ValidationFailed(
            "Wasm value shape does not match marshaling plan".to_string(),
        )),
    }
}

#[cfg(feature = "pg_test")]
fn build_result_composite(
    ok_plan: &MarshalPlan,
    err_plan: &MarshalPlan,
    branch: Option<&Val>,
    is_ok_arm: bool,
) -> Result<(pg_sys::Datum, bool), PgWasmError> {
    let ok_oid = scalar_type_oid(ok_plan)?;
    let err_oid = scalar_type_oid(err_plan)?;
    let (ok_cell, err_cell) = if is_ok_arm {
        let v = branch.ok_or_else(|| {
            PgWasmError::ValidationFailed("result ok arm missing value".to_string())
        })?;
        let (d, n) = val_to_datum(ok_plan, v)?;
        if n {
            return Err(PgWasmError::Unsupported(
                "NULL ok payload is not supported for result composite in this build".to_string(),
            ));
        }
        (Some(d), None)
    } else {
        let v = branch.ok_or_else(|| {
            PgWasmError::ValidationFailed("result err arm missing value".to_string())
        })?;
        let (d, n) = val_to_datum(err_plan, v)?;
        if n {
            return Err(PgWasmError::Unsupported(
                "NULL err payload is not supported for result composite in this build".to_string(),
            ));
        }
        (None, Some(d))
    };
    let datum = heap_tuple_two_fields(ok_oid, err_oid, ok_cell, err_cell)?;
    Ok((datum, false))
}

#[cfg(feature = "pg_test")]
fn scalar_type_oid(plan: &MarshalPlan) -> Result<pg_sys::Oid, PgWasmError> {
    match plan {
        MarshalPlan::Scalar(ScalarKind::Bool) => Ok(pg_sys::BOOLOID),
        MarshalPlan::Scalar(ScalarKind::Char) => Ok(pg_sys::TEXTOID),
        MarshalPlan::Scalar(ScalarKind::F32) => Ok(pg_sys::FLOAT4OID),
        MarshalPlan::Scalar(ScalarKind::F64) => Ok(pg_sys::FLOAT8OID),
        MarshalPlan::Scalar(ScalarKind::S8 | ScalarKind::S16 | ScalarKind::U8) => {
            Ok(pg_sys::INT2OID)
        }
        MarshalPlan::Scalar(ScalarKind::S32 | ScalarKind::U16) => Ok(pg_sys::INT4OID),
        MarshalPlan::Scalar(ScalarKind::S64 | ScalarKind::U32) => Ok(pg_sys::INT8OID),
        MarshalPlan::Scalar(ScalarKind::String) => Ok(pg_sys::TEXTOID),
        MarshalPlan::Scalar(ScalarKind::U64) => Ok(pg_sys::NUMERICOID),
        _ => Err(PgWasmError::Unsupported(
            "result composite supports only scalar ok/err types in this build".to_string(),
        )),
    }
}

#[cfg(feature = "pg_test")]
fn heap_tuple_two_fields(
    ok_type: pg_sys::Oid,
    err_type: pg_sys::Oid,
    ok: Option<pg_sys::Datum>,
    err: Option<pg_sys::Datum>,
) -> Result<pg_sys::Datum, PgWasmError> {
    let name_ok: &CStr = c"ok";
    let name_err: &CStr = c"err";
    let tupdesc_ptr = unsafe {
        let desc = pg_sys::CreateTemplateTupleDesc(2);
        if desc.is_null() {
            return Err(PgWasmError::Internal(
                "CreateTemplateTupleDesc returned null".to_string(),
            ));
        }
        pg_sys::TupleDescInitEntry(desc, 1, name_ok.as_ptr(), ok_type, -1, 0);
        pg_sys::TupleDescInitEntry(desc, 2, name_err.as_ptr(), err_type, -1, 0);
        pg_sys::BlessTupleDesc(desc)
    };
    let tupdesc = unsafe { PgTupleDesc::from_pg(tupdesc_ptr) };
    let datums = vec![ok, err];
    let tup = unsafe { PgHeapTuple::from_datums(tupdesc, datums) }
        .map_err(|e| PgWasmError::Internal(format!("result heap_form_tuple: {e}")))?;
    tup.into_composite_datum().ok_or_else(|| {
        PgWasmError::Internal("result composite: into_composite_datum returned null".to_string())
    })
}

/// Call a dynamic component function and write results into `results`.
pub(crate) fn invoke_component<T>(
    func: &Func,
    store: &mut Store<T>,
    args: &[Val],
    results: &mut [Val],
) -> Result<(), PgWasmError> {
    func.call(store, args, results).map_err(map_wasmtime_err)?;
    Ok(())
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use super::*;

    #[test]
    fn plan_scalar_export() {
        let export = Export {
            params: vec![ExportSlot {
                is_option: false,
                pg_oid: pg_sys::INT4OID,
                pg_type: PgType::Scalar("int4"),
            }],
            result: Some(ExportSlot {
                is_option: false,
                pg_oid: pg_sys::BOOLOID,
                pg_type: PgType::Scalar("boolean"),
            }),
        };
        let plans = plan_marshaler(&TypePlan { entries: vec![] }, &export).unwrap();
        assert_eq!(plans.len(), 2);
        assert!(matches!(plans[0], MarshalPlan::Scalar(ScalarKind::S32)));
        assert!(matches!(plans[1], MarshalPlan::Scalar(ScalarKind::Bool)));
    }

    #[test]
    fn flags_bits_round_trip() {
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let set = vec!["a".to_string(), "c".to_string()];
        let bits = flag_bits_for_names(&names, &set).unwrap();
        let back = names_for_flag_bits(&names, bits).unwrap();
        assert_eq!(back, set);
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use pgrx::{prelude::*, spi::Spi};

    use super::*;

    #[pg_test]
    fn pg_dynamic_marshal_round_trips() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        Spi::run("DROP TYPE IF EXISTS pgwasm.marshal_pg_row CASCADE").unwrap();
        Spi::run("CREATE TYPE pgwasm.marshal_pg_row AS (a int4, b text)").unwrap();
        let row_oid: pg_sys::Oid = Spi::get_one("SELECT 'pgwasm.marshal_pg_row'::regtype::oid")
            .unwrap()
            .unwrap();

        let record_plan = MarshalPlan::Record {
            fields: vec![
                FieldPlan {
                    name: "a".to_string(),
                    plan: Box::new(MarshalPlan::Scalar(ScalarKind::S32)),
                },
                FieldPlan {
                    name: "b".to_string(),
                    plan: Box::new(MarshalPlan::Scalar(ScalarKind::String)),
                },
            ],
            pg_oid: row_oid,
        };
        let v = Val::Record(vec![
            ("a".to_string(), Val::S32(1)),
            ("b".to_string(), Val::String("z".to_string())),
        ]);
        let (d, n) = val_to_datum(&record_plan, &v).unwrap();
        assert!(!n);
        let v2 = datum_to_val(&record_plan, d, false, row_oid).unwrap();
        assert_eq!(v2, v);

        let tuple_plan = MarshalPlan::Tuple {
            elements: vec![
                MarshalPlan::Scalar(ScalarKind::S32),
                MarshalPlan::Scalar(ScalarKind::String),
            ],
            pg_oid: row_oid,
        };
        let tv = Val::Tuple(vec![Val::S32(9), Val::String("n".to_string())]);
        let (td, tn) = val_to_datum(&tuple_plan, &tv).unwrap();
        assert!(!tn);
        let tv2 = datum_to_val(&tuple_plan, td, false, row_oid).unwrap();
        assert_eq!(tv2, tv);

        Spi::run("DROP TYPE IF EXISTS pgwasm.marshal_pg_var CASCADE").unwrap();
        Spi::run("CREATE TYPE pgwasm.marshal_pg_var AS (disc text, payload int4)").unwrap();
        let var_oid: pg_sys::Oid = Spi::get_one("SELECT 'pgwasm.marshal_pg_var'::regtype::oid")
            .unwrap()
            .unwrap();
        let var_plan = MarshalPlan::Variant {
            cases: vec![
                CasePlan {
                    name: "empty".to_string(),
                    payload: None,
                },
                CasePlan {
                    name: "n".to_string(),
                    payload: Some(Box::new(MarshalPlan::Scalar(ScalarKind::S32))),
                },
            ],
            pg_oid: var_oid,
        };
        let vv = Val::Variant("n".to_string(), Some(Box::new(Val::S32(5))));
        let (vd, vn) = val_to_datum(&var_plan, &vv).unwrap();
        assert!(!vn);
        let vv2 = datum_to_val(&var_plan, vd, false, var_oid).unwrap();
        assert_eq!(vv2, vv);

        let names = vec!["x".to_string(), "y".to_string()];
        let enum_plan = MarshalPlan::Enum(names.clone());
        let ev = Val::Enum("y".to_string());
        let (ed, en) = val_to_datum(&enum_plan, &ev).unwrap();
        assert!(!en);
        let ev2 = datum_to_val(&enum_plan, ed, false, pg_sys::InvalidOid).unwrap();
        assert_eq!(ev2, ev);

        let opt_plan = MarshalPlan::Option(Box::new(MarshalPlan::Scalar(ScalarKind::S32)));
        let ov = Val::Option(Some(Box::new(Val::S32(7))));
        let (od, on) = val_to_datum(&opt_plan, &ov).unwrap();
        assert!(!on);
        let ov2 = datum_to_val(&opt_plan, od, false, pg_sys::InvalidOid).unwrap();
        assert_eq!(ov2, ov);
        let onone = Val::Option(None);
        let (odn, onn) = val_to_datum(&opt_plan, &onone).unwrap();
        assert!(onn);
        let onone2 = datum_to_val(&opt_plan, odn, true, pg_sys::InvalidOid).unwrap();
        assert_eq!(onone2, onone);

        let scalar_cases = vec![
            (
                MarshalPlan::Scalar(ScalarKind::S8),
                (-8i16).into_datum().unwrap(),
                Val::S8(-8),
            ),
            (
                MarshalPlan::Scalar(ScalarKind::U8),
                250i16.into_datum().unwrap(),
                Val::U8(250),
            ),
            (
                MarshalPlan::Scalar(ScalarKind::U16),
                65_535i32.into_datum().unwrap(),
                Val::U16(65_535),
            ),
            (
                MarshalPlan::Scalar(ScalarKind::U32),
                4_294_967_295i64.into_datum().unwrap(),
                Val::U32(u32::MAX),
            ),
            (
                MarshalPlan::Scalar(ScalarKind::F32),
                1.5f32.into_datum().unwrap(),
                Val::Float32(1.5),
            ),
            (
                MarshalPlan::Scalar(ScalarKind::F64),
                2.25f64.into_datum().unwrap(),
                Val::Float64(2.25),
            ),
            (
                MarshalPlan::Scalar(ScalarKind::Char),
                "A".to_string().into_datum().unwrap(),
                Val::Char('A'),
            ),
        ];
        for (plan, datum, expected) in scalar_cases {
            let value = datum_to_val(&plan, datum, false, pg_sys::InvalidOid).unwrap();
            assert_eq!(value, expected);
            let (roundtrip_datum, roundtrip_null) = val_to_datum(&plan, &value).unwrap();
            assert!(!roundtrip_null);
            let roundtrip =
                datum_to_val(&plan, roundtrip_datum, false, pg_sys::InvalidOid).unwrap();
            assert_eq!(roundtrip, expected);
        }

        let numeric = AnyNumeric::try_from(u64::MAX).unwrap();
        let u64_plan = MarshalPlan::Scalar(ScalarKind::U64);
        let u64_val = datum_to_val(
            &u64_plan,
            numeric.into_datum().unwrap(),
            false,
            pg_sys::InvalidOid,
        )
        .unwrap();
        assert_eq!(u64_val, Val::U64(u64::MAX));
        let (u64_datum, u64_null) = val_to_datum(&u64_plan, &u64_val).unwrap();
        assert!(!u64_null);
        assert_eq!(
            datum_to_val(&u64_plan, u64_datum, false, pg_sys::InvalidOid).unwrap(),
            u64_val
        );

        let invalid_char = "AB".to_string().into_datum().unwrap();
        assert!(
            datum_to_val(
                &MarshalPlan::Scalar(ScalarKind::Char),
                invalid_char,
                false,
                pg_sys::InvalidOid
            )
            .is_err()
        );

        let res_plan = MarshalPlan::Result {
            ok: Box::new(MarshalPlan::Scalar(ScalarKind::S32)),
            err: Box::new(MarshalPlan::Scalar(ScalarKind::String)),
        };
        let rv = Val::Result(Ok(Some(Box::new(Val::S32(42)))));
        let (rd, rn) = val_to_datum(&res_plan, &rv).unwrap();
        assert!(!rn);
        let rv2 = datum_to_val(&res_plan, rd, false, pg_sys::InvalidOid).unwrap();
        assert_eq!(rv2, rv);

        let list_plan = MarshalPlan::List(Box::new(MarshalPlan::Scalar(ScalarKind::S32)));
        let arr: Array<i32> = Spi::get_one("SELECT ARRAY[10, 20]::int4[] AS a")
            .unwrap()
            .unwrap();
        let ad = arr.into_datum().unwrap();
        let lv = datum_to_val(&list_plan, ad, false, pg_sys::InvalidOid).unwrap();
        let (ld, ln) = val_to_datum(&list_plan, &lv).unwrap();
        assert!(!ln);
        let _ = ld;
        let Val::List(items) = &lv else {
            panic!("expected list");
        };
        assert_eq!(items.len(), 2);
        assert!(matches!(items[0], Val::S32(10)));
        assert!(matches!(items[1], Val::S32(20)));

        let list_i64_plan = MarshalPlan::List(Box::new(MarshalPlan::Scalar(ScalarKind::S64)));
        let arr_i64: Array<i64> = Spi::get_one("SELECT ARRAY[10000000000, -3]::int8[] AS a")
            .unwrap()
            .unwrap();
        let ad_i64 = arr_i64.into_datum().unwrap();
        let lv_i64 = datum_to_val(&list_i64_plan, ad_i64, false, pg_sys::InvalidOid).unwrap();
        let (ld_i64, ln_i64) = val_to_datum(&list_i64_plan, &lv_i64).unwrap();
        assert!(!ln_i64);
        let lv_i64_roundtrip =
            datum_to_val(&list_i64_plan, ld_i64, false, pg_sys::InvalidOid).unwrap();
        assert_eq!(lv_i64_roundtrip, lv_i64);
        let Val::List(items_i64) = &lv_i64 else {
            panic!("expected int8 list");
        };
        assert_eq!(items_i64.len(), 2);
        assert!(matches!(items_i64[0], Val::S64(10000000000)));
        assert!(matches!(items_i64[1], Val::S64(-3)));

        Spi::run("DROP TYPE IF EXISTS pgwasm.marshal_pg_var CASCADE").unwrap();
        Spi::run("DROP TYPE IF EXISTS pgwasm.marshal_pg_row CASCADE").unwrap();
    }
}
