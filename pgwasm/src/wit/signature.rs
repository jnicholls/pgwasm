//! Stable, serialized WIT export signatures used for catalog compatibility checks and marshaling.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use wit_parser::{Function, FunctionKind, Type, TypeDefKind, WorldItem};

use super::{typing, world};
use crate::errors::{PgWasmError, Result};

const SIGNATURE_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ExportSignature {
    pub(crate) kind: String,
    pub(crate) params: Vec<ParamShape>,
    pub(crate) result: Option<TypeShape>,
    pub(crate) version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ParamShape {
    pub(crate) name: String,
    pub(crate) ty: TypeShape,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum TypeShape {
    Bool,
    Borrow {
        type_key: String,
    },
    Char,
    Enum {
        cases: Vec<String>,
        type_key: String,
    },
    ErrorContext,
    F32,
    F64,
    FixedLengthList {
        element: Box<TypeShape>,
        length: u32,
        type_key: String,
    },
    Flags {
        names: Vec<String>,
        type_key: String,
    },
    Future {
        element: Option<Box<TypeShape>>,
        type_key: String,
    },
    List {
        element: Box<TypeShape>,
        type_key: String,
    },
    Map {
        key: Box<TypeShape>,
        type_key: String,
        value: Box<TypeShape>,
    },
    Option {
        inner: Box<TypeShape>,
        type_key: String,
    },
    Own {
        type_key: String,
    },
    Record {
        fields: Vec<FieldShape>,
        type_key: String,
    },
    Resource {
        type_key: String,
    },
    Result {
        err: Option<Box<TypeShape>>,
        ok: Option<Box<TypeShape>>,
        type_key: String,
    },
    S16,
    S32,
    S64,
    S8,
    Stream {
        element: Option<Box<TypeShape>>,
        type_key: String,
    },
    String,
    Tuple {
        elements: Vec<TypeShape>,
        type_key: String,
    },
    TypeAlias {
        inner: Box<TypeShape>,
        type_key: String,
    },
    U16,
    U32,
    U64,
    U8,
    Variant {
        cases: Vec<CaseShape>,
        type_key: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FieldShape {
    pub(crate) name: String,
    pub(crate) ty: TypeShape,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CaseShape {
    pub(crate) name: String,
    pub(crate) payload: Option<TypeShape>,
}

pub(crate) fn export_signature_json(
    decoded: &world::DecodedWorld,
    wasm_export: &str,
) -> Result<Value> {
    let signature = export_signature(decoded, wasm_export)?;
    serde_json::to_value(signature)
        .map_err(|error| PgWasmError::Internal(format!("serialize WIT signature: {error}")))
}

pub(crate) fn export_signature(
    decoded: &world::DecodedWorld,
    wasm_export: &str,
) -> Result<ExportSignature> {
    let func = find_export_function(decoded, wasm_export)?;
    let params = func
        .params
        .iter()
        .map(|param| {
            Ok(ParamShape {
                name: param.name.clone(),
                ty: shape_for_type(&decoded.resolve, param.ty)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let result = func
        .result
        .map(|ty| shape_for_type(&decoded.resolve, ty))
        .transpose()?;

    Ok(ExportSignature {
        kind: "wit-function".to_string(),
        params,
        result,
        version: SIGNATURE_VERSION,
    })
}

pub(crate) fn parse_export_signature(value: &Value) -> Result<ExportSignature> {
    let signature: ExportSignature = serde_json::from_value(value.clone()).map_err(|error| {
        PgWasmError::Unsupported(format!(
            "catalog export signature is not a normalized WIT signature: {error}"
        ))
    })?;
    if signature.version != SIGNATURE_VERSION {
        return Err(PgWasmError::Unsupported(format!(
            "unsupported WIT signature version {}; expected {SIGNATURE_VERSION}",
            signature.version
        )));
    }
    if signature.kind != "wit-function" {
        return Err(PgWasmError::Unsupported(format!(
            "unsupported export signature kind `{}`",
            signature.kind
        )));
    }
    Ok(signature)
}

pub(crate) fn export_signatures_differ(old: &Value, new: &Value) -> bool {
    if old == new {
        return false;
    }

    let (Ok(old_sig), Ok(new_sig)) = (parse_export_signature(old), parse_export_signature(new))
    else {
        return true;
    };

    if old_sig.kind != new_sig.kind
        || old_sig.version != new_sig.version
        || old_sig.params.len() != new_sig.params.len()
    {
        return true;
    }

    for (old_param, new_param) in old_sig.params.iter().zip(&new_sig.params) {
        if old_param.name != new_param.name || shapes_differ(&old_param.ty, &new_param.ty) {
            return true;
        }
    }

    match (&old_sig.result, &new_sig.result) {
        (None, None) => false,
        (Some(old_result), Some(new_result)) => shapes_differ(old_result, new_result),
        _ => true,
    }
}

fn shapes_differ(old: &TypeShape, new: &TypeShape) -> bool {
    match (old, new) {
        (TypeShape::Bool, TypeShape::Bool)
        | (TypeShape::Char, TypeShape::Char)
        | (TypeShape::ErrorContext, TypeShape::ErrorContext)
        | (TypeShape::F32, TypeShape::F32)
        | (TypeShape::F64, TypeShape::F64)
        | (TypeShape::S8, TypeShape::S8)
        | (TypeShape::S16, TypeShape::S16)
        | (TypeShape::S32, TypeShape::S32)
        | (TypeShape::S64, TypeShape::S64)
        | (TypeShape::String, TypeShape::String)
        | (TypeShape::U8, TypeShape::U8)
        | (TypeShape::U16, TypeShape::U16)
        | (TypeShape::U32, TypeShape::U32)
        | (TypeShape::U64, TypeShape::U64) => false,
        (TypeShape::Borrow { type_key: old_key }, TypeShape::Borrow { type_key: new_key })
        | (
            TypeShape::Enum {
                type_key: old_key, ..
            },
            TypeShape::Enum {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::FixedLengthList {
                type_key: old_key, ..
            },
            TypeShape::FixedLengthList {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Flags {
                type_key: old_key, ..
            },
            TypeShape::Flags {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Future {
                type_key: old_key, ..
            },
            TypeShape::Future {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::List {
                type_key: old_key, ..
            },
            TypeShape::List {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Map {
                type_key: old_key, ..
            },
            TypeShape::Map {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Option {
                type_key: old_key, ..
            },
            TypeShape::Option {
                type_key: new_key, ..
            },
        )
        | (TypeShape::Own { type_key: old_key }, TypeShape::Own { type_key: new_key })
        | (
            TypeShape::Record {
                type_key: old_key, ..
            },
            TypeShape::Record {
                type_key: new_key, ..
            },
        )
        | (TypeShape::Resource { type_key: old_key }, TypeShape::Resource { type_key: new_key })
        | (
            TypeShape::Result {
                type_key: old_key, ..
            },
            TypeShape::Result {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Stream {
                type_key: old_key, ..
            },
            TypeShape::Stream {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Tuple {
                type_key: old_key, ..
            },
            TypeShape::Tuple {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::TypeAlias {
                type_key: old_key, ..
            },
            TypeShape::TypeAlias {
                type_key: new_key, ..
            },
        )
        | (
            TypeShape::Variant {
                type_key: old_key, ..
            },
            TypeShape::Variant {
                type_key: new_key, ..
            },
        ) => old_key != new_key,
        _ => true,
    }
}

fn find_export_function<'a>(
    decoded: &'a world::DecodedWorld,
    wasm_export: &str,
) -> Result<&'a Function> {
    let world = decoded
        .resolve
        .worlds
        .get(decoded.world_id)
        .ok_or_else(|| PgWasmError::InvalidModule("decoded world missing".to_string()))?;

    for item in world.exports.values() {
        match item {
            WorldItem::Function(func)
                if export_wasm_name(&decoded.resolve, func) == wasm_export =>
            {
                return Ok(func);
            }
            WorldItem::Interface { id, .. } => {
                let Some(iface) = decoded.resolve.interfaces.get(*id) else {
                    continue;
                };
                for func in iface.functions.values() {
                    if export_wasm_name(&decoded.resolve, func) == wasm_export {
                        return Ok(func);
                    }
                }
            }
            WorldItem::Function(_) | WorldItem::Type { .. } => {}
        }
    }

    Err(PgWasmError::Internal(format!(
        "could not locate WIT function `{wasm_export}` for signature JSON"
    )))
}

fn export_wasm_name(resolve: &wit_parser::Resolve, func: &Function) -> String {
    match &func.kind {
        FunctionKind::Freestanding | FunctionKind::AsyncFreestanding => func.name.clone(),
        FunctionKind::Method(id) | FunctionKind::AsyncMethod(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}#{}", func.name)
        }
        FunctionKind::Static(id) | FunctionKind::AsyncStatic(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}!{}", func.name)
        }
        FunctionKind::Constructor(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}#{type_name}")
        }
    }
}

fn type_name_for_id(resolve: &wit_parser::Resolve, type_id: wit_parser::TypeId) -> String {
    resolve.types[type_id]
        .name
        .clone()
        .unwrap_or_else(|| format!("type{}", type_id.index()))
}

fn shape_for_type(resolve: &wit_parser::Resolve, ty: Type) -> Result<TypeShape> {
    Ok(match ty {
        Type::Bool => TypeShape::Bool,
        Type::Char => TypeShape::Char,
        Type::ErrorContext => TypeShape::ErrorContext,
        Type::F32 => TypeShape::F32,
        Type::F64 => TypeShape::F64,
        Type::Id(type_id) => shape_for_type_id(resolve, type_id)?,
        Type::S16 => TypeShape::S16,
        Type::S32 => TypeShape::S32,
        Type::S64 => TypeShape::S64,
        Type::S8 => TypeShape::S8,
        Type::String => TypeShape::String,
        Type::U16 => TypeShape::U16,
        Type::U32 => TypeShape::U32,
        Type::U64 => TypeShape::U64,
        Type::U8 => TypeShape::U8,
    })
}

fn shape_for_type_id(
    resolve: &wit_parser::Resolve,
    type_id: wit_parser::TypeId,
) -> Result<TypeShape> {
    let type_key = typing::export_type_key_for_id(resolve, type_id)?;
    let typedef = resolve.types.get(type_id).ok_or_else(|| {
        PgWasmError::InvalidModule(format!("type id {type_id:?} was not present in resolve"))
    })?;
    Ok(match &typedef.kind {
        TypeDefKind::Enum(enm) => TypeShape::Enum {
            cases: enm.cases.iter().map(|case| case.name.clone()).collect(),
            type_key,
        },
        TypeDefKind::FixedLengthList(element, length) => TypeShape::FixedLengthList {
            element: Box::new(shape_for_type(resolve, *element)?),
            length: *length,
            type_key,
        },
        TypeDefKind::Flags(flags) => TypeShape::Flags {
            names: flags.flags.iter().map(|flag| flag.name.clone()).collect(),
            type_key,
        },
        TypeDefKind::Future(element) => TypeShape::Future {
            element: element
                .map(|ty| shape_for_type(resolve, ty))
                .transpose()?
                .map(Box::new),
            type_key,
        },
        TypeDefKind::Handle(wit_parser::Handle::Borrow(handle_id)) => TypeShape::Borrow {
            type_key: typing::export_type_key_for_id(resolve, *handle_id)?,
        },
        TypeDefKind::Handle(wit_parser::Handle::Own(handle_id)) => TypeShape::Own {
            type_key: typing::export_type_key_for_id(resolve, *handle_id)?,
        },
        TypeDefKind::List(element) => TypeShape::List {
            element: Box::new(shape_for_type(resolve, *element)?),
            type_key,
        },
        TypeDefKind::Map(key, value) => TypeShape::Map {
            key: Box::new(shape_for_type(resolve, *key)?),
            type_key,
            value: Box::new(shape_for_type(resolve, *value)?),
        },
        TypeDefKind::Option(inner) => TypeShape::Option {
            inner: Box::new(shape_for_type(resolve, *inner)?),
            type_key,
        },
        TypeDefKind::Record(record) => TypeShape::Record {
            fields: record
                .fields
                .iter()
                .map(|field| {
                    Ok(FieldShape {
                        name: field.name.clone(),
                        ty: shape_for_type(resolve, field.ty)?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            type_key,
        },
        TypeDefKind::Resource => TypeShape::Resource { type_key },
        TypeDefKind::Result(result) => TypeShape::Result {
            err: result
                .err
                .map(|ty| shape_for_type(resolve, ty))
                .transpose()?
                .map(Box::new),
            ok: result
                .ok
                .map(|ty| shape_for_type(resolve, ty))
                .transpose()?
                .map(Box::new),
            type_key,
        },
        TypeDefKind::Stream(element) => TypeShape::Stream {
            element: element
                .map(|ty| shape_for_type(resolve, ty))
                .transpose()?
                .map(Box::new),
            type_key,
        },
        TypeDefKind::Tuple(tuple) => TypeShape::Tuple {
            elements: tuple
                .types
                .iter()
                .copied()
                .map(|ty| shape_for_type(resolve, ty))
                .collect::<Result<Vec<_>>>()?,
            type_key,
        },
        TypeDefKind::Type(alias) => TypeShape::TypeAlias {
            inner: Box::new(shape_for_type(resolve, *alias)?),
            type_key,
        },
        TypeDefKind::Unknown => {
            return Err(PgWasmError::InvalidModule(format!(
                "encountered unknown WIT type definition for {type_id:?}"
            )));
        }
        TypeDefKind::Variant(variant) => TypeShape::Variant {
            cases: variant
                .cases
                .iter()
                .map(|case| {
                    Ok(CaseShape {
                        name: case.name.clone(),
                        payload: case.ty.map(|ty| shape_for_type(resolve, ty)).transpose()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            type_key,
        },
    })
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use super::*;

    fn fixture_decoded_world(wit_source: &str, world_name: &str) -> world::DecodedWorld {
        let mut resolve = wit_parser::Resolve::default();
        let pkg = resolve
            .push_str("fixture.wit", wit_source)
            .expect("fixture wit should parse");
        let world_id = resolve
            .select_world(&[pkg], Some(world_name))
            .expect("fixture world should exist");
        world::DecodedWorld {
            resolve,
            world_id,
            wit_text: wit_source.to_string(),
        }
    }

    #[test]
    fn export_signature_is_normalized_and_round_trips_json() {
        let decoded = fixture_decoded_world(
            r#"
                package test:fixture;

                interface api {
                    record person {
                        id: u32,
                        name: string,
                    }

                    flags perms {
                        read,
                        write,
                    }

                    echo: func(person: person, perms-arg: perms, maybe: option<string>) -> result<string, u8>;
                }

                world fixture {
                    export api;
                }
            "#,
            "fixture",
        );

        let value = export_signature_json(&decoded, "echo").expect("signature should serialize");
        let parsed = parse_export_signature(&value).expect("signature should parse");

        assert_eq!(parsed.version, SIGNATURE_VERSION);
        assert_eq!(parsed.params.len(), 3);
        assert!(matches!(parsed.params[0].ty, TypeShape::Record { .. }));
        assert!(matches!(parsed.params[1].ty, TypeShape::Flags { .. }));
        assert!(matches!(parsed.params[2].ty, TypeShape::Option { .. }));
        assert!(matches!(parsed.result, Some(TypeShape::Result { .. })));
    }
}
