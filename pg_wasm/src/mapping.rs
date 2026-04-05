//! PostgreSQL ↔ WASM value representation and export registration hints.

use std::collections::HashMap;

use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::spi::Spi;
use serde_json::Value;

/// Maps export name → SQL types and optional WIT note (for future component support).
pub type ExportHintMap = HashMap<String, ExportTypeHint>;

/// Per-export types from `pg_wasm_load(..., '{"exports":{...}}')`.
#[derive(Clone, Debug)]
pub struct ExportTypeHint {
    pub args: Vec<(Oid, PgWasmTypeKind)>,
    pub ret: (Oid, PgWasmTypeKind),
    /// Optional WIT / component interface hint (ignored for core wasm today).
    pub wit_interface: Option<String>,
}

/// Classifies how a SQL argument maps to the WASM ABI for core modules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgWasmTypeKind {
    I32,
    I64,
    Bool,
    F32,
    F64,
    /// UTF-8 text: wasm `(i32, i32)` ptr+len; see `crate::runtime::wasmtime_backend::MEM_IO_INPUT_BASE`.
    /// For WebAssembly components, `string` uses the component-model dynamic call path instead.
    String,
    /// Raw bytes; `jsonb` uses JSON UTF-8 bytes (same wasm shape as [`PgWasmTypeKind::String`]).
    Bytes,
    /// PostgreSQL `int4[]` for component `list<s32>` / `list<u32>` (and similar integer lists).
    Int4Array,
    /// PostgreSQL `text[]` for component `list<string>`.
    TextArray,
    /// PostgreSQL composite type (Track A user types or Track B auto-generated).
    Composite,
}

/// Describes one SQL argument position for dynamic dispatch.
#[derive(Clone, Debug)]
pub struct PgWasmArgDesc {
    pub pg_oid: Oid,
    pub kind: PgWasmTypeKind,
}

/// Describes the return mapping for a WASM export registered as a UDF.
#[derive(Clone, Debug)]
pub struct PgWasmReturnDesc {
    pub pg_oid: Oid,
    pub kind: PgWasmTypeKind,
}

impl Default for PgWasmReturnDesc {
    fn default() -> Self {
        Self {
            pg_oid: pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        }
    }
}

/// Structural typing for a subset of WIT / component interface types (host-side mirror).
///
/// Used with [`ExportSignature::component_dynamic_plan`] to lift and lower
/// component-model dynamic values against PostgreSQL datums.
///
/// **Supported for auto-mapped component exports:** integer and float scalars, `char`, `string`,
/// `list<u8>` (`bytea`), `list<s32|u32>` (`int4[]`), `list<string>` (`text[]`), and aggregate types
/// (`record`, `tuple`, `variant`, `option`, `result`, `enum`, `flags`) via `jsonb` with the JSON
/// encoding in `pg_wasm::runtime::component_marshal` (Wasmtime feature). **Not supported:** resources
/// (`own` / `borrow`), futures, streams, `error-context`, and nested lists other than the specific
/// list element types above.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarshalType {
    Bool,
    S8,
    U8,
    S16,
    U16,
    S32,
    U32,
    S64,
    U64,
    F32,
    F64,
    Char,
    String,
    List(Box<MarshalType>),
    Record(Vec<(String, MarshalType)>),
    Tuple(Vec<MarshalType>),
    Variant(Vec<(String, Option<MarshalType>)>),
    Option(Box<MarshalType>),
    Result {
        ok: Option<Box<MarshalType>>,
        err: Option<Box<MarshalType>>,
    },
    Enum(Vec<String>),
    Flags(Vec<String>),
}

/// Full parameter/result [`MarshalType`] plan for one component function export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentDynCallPlan {
    pub params: Vec<MarshalType>,
    pub result: MarshalType,
}

/// Per-export signature for catalog + trampoline (WIT layout reserved).
#[derive(Clone, Debug, Default)]
pub struct ExportSignature {
    pub args: Vec<PgWasmArgDesc>,
    pub ret: PgWasmReturnDesc,
    /// Optional WIT interface note from load `exports` JSON (reserved for diagnostics).
    #[allow(dead_code)]
    pub wit_interface: Option<String>,
    /// When set, this export uses the host runtime’s dynamic component `Func::call` path with this plan.
    pub component_dynamic_plan: Option<ComponentDynCallPlan>,
}

/// Parse `exports` object from load `options` JSON (see [`ExportTypeHint`]).
pub fn parse_export_hints(val: &Value) -> Result<ExportHintMap, String> {
    let Some(obj) = val.get("exports").and_then(|e| e.as_object()) else {
        return Ok(ExportHintMap::new());
    };
    let mut out = ExportHintMap::new();
    for (name, spec) in obj {
        let spec = spec.as_object().ok_or_else(|| {
            format!("pg_wasm_load exports.{name}: expected object with args/returns (or return)")
        })?;
        let args_val = spec
            .get("args")
            .ok_or_else(|| format!("pg_wasm_load exports.{name}: missing \"args\""))?;
        let args_arr = args_val.as_array().ok_or_else(|| {
            format!("pg_wasm_load exports.{name}.args: expected JSON array of type names")
        })?;
        let mut args = Vec::with_capacity(args_arr.len());
        for (j, a) in args_arr.iter().enumerate() {
            args.push(
                parse_one_sql_type(a)
                    .map_err(|e| format!("pg_wasm_load exports.{name}.args[{j}]: {e}"))?,
            );
        }
        let ret_key = if spec.contains_key("returns") {
            "returns"
        } else if spec.contains_key("return") {
            "return"
        } else {
            return Err(format!(
                "pg_wasm_load exports.{name}: missing \"returns\" (or \"return\")"
            ));
        };
        let ret = parse_one_sql_type(
            spec.get(ret_key)
                .ok_or_else(|| format!("pg_wasm_load exports.{name}: missing returns value"))?,
        )
        .map_err(|e| format!("pg_wasm_load exports.{name}.{ret_key}: {e}"))?;
        let wit_interface = spec.get("wit").and_then(|w| w.as_str()).map(str::to_string);
        out.insert(
            name.clone(),
            ExportTypeHint {
                args,
                ret,
                wit_interface,
            },
        );
    }
    Ok(out)
}

fn parse_one_sql_type(v: &Value) -> Result<(Oid, PgWasmTypeKind), String> {
    if let Some(obj) = v.as_object() {
        let kind = obj
            .get("kind")
            .and_then(|k| k.as_str())
            .ok_or_else(|| "type object requires string \"kind\"".to_string())?;
        return match kind {
            "jsonb" => Ok((pg_sys::JSONBOID, PgWasmTypeKind::Bytes)),
            "composite" => {
                let t = obj.get("type").and_then(|x| x.as_str()).ok_or_else(|| {
                    "composite type object requires string \"type\" (regtype)".to_string()
                })?;
                let oid = resolve_regtype_oid(t)?;
                if !pg_type_oid_is_composite(oid)? {
                    return Err(format!("pg_wasm: {t:?} is not a composite type"));
                }
                Ok((oid, PgWasmTypeKind::Composite))
            }
            other => Err(format!(
                "unknown type.kind {other:?} (use \"jsonb\" or \"composite\")"
            )),
        };
    }
    if let Some(n) = v.as_u64() {
        let oid = Oid::from(n as u32);
        if oid == pg_sys::InvalidOid {
            return Err("InvalidOid not allowed".into());
        }
        if pg_type_oid_is_composite(oid)? {
            return Ok((oid, PgWasmTypeKind::Composite));
        }
        // Custom non-composite OIDs: opaque byte payload (legacy).
        return Ok((oid, PgWasmTypeKind::Bytes));
    }
    if let Some(s) = v.as_str() {
        if let Ok(desc) = sql_name_to_pg_descriptor(s) {
            return Ok(desc);
        }
        let oid = resolve_regtype_oid(s)?;
        if pg_type_oid_is_composite(oid)? {
            return Ok((oid, PgWasmTypeKind::Composite));
        }
        return Err(format!(
            "pg_wasm: type name {s:?} is not a built-in alias and not a composite type (use schema.type for composites)"
        ));
    }
    Err("expected string type name, numeric OID, or type object {kind,...}".into())
}

/// `SELECT oid FROM pg_type WHERE oid = 'typename'::regtype::oid` (qualified names supported).
pub fn resolve_regtype_oid(qualified: &str) -> Result<Oid, String> {
    let esc = qualified.replace('\'', "''");
    let sql = format!("SELECT ('{esc}'::regtype)::oid");
    Spi::get_one::<Oid>(&sql)
        .map_err(|e| format!("pg_wasm: regtype {qualified:?}: {e}"))?
        .ok_or_else(|| format!("pg_wasm: unknown type name {qualified:?}"))
}

/// True if `oid` is a PostgreSQL composite (`pg_type.typtype = 'c'`).
pub fn pg_type_oid_is_composite(oid: Oid) -> Result<bool, String> {
    if oid == pg_sys::InvalidOid {
        return Ok(false);
    }
    let sql = format!(
        "SELECT typtype::text = 'c' FROM pg_catalog.pg_type WHERE oid = {}",
        u32::from(oid)
    );
    Spi::get_one::<bool>(&sql)
        .map_err(|e| format!("pg_wasm: pg_type lookup: {e}"))?
        .ok_or_else(|| format!("pg_wasm: pg_type oid {} not found", u32::from(oid)))
}

/// Builtin SQL type names → PostgreSQL OID + marshal kind.
pub fn sql_name_to_pg_descriptor(name: &str) -> Result<(Oid, PgWasmTypeKind), String> {
    match name.trim().to_ascii_lowercase().as_str() {
        "int2" | "smallint" => Ok((pg_sys::INT2OID, PgWasmTypeKind::I32)),
        "int4" | "integer" | "int" => Ok((pg_sys::INT4OID, PgWasmTypeKind::I32)),
        "int8" | "bigint" => Ok((pg_sys::INT8OID, PgWasmTypeKind::I64)),
        "bool" | "boolean" => Ok((pg_sys::BOOLOID, PgWasmTypeKind::Bool)),
        "float4" | "real" => Ok((pg_sys::FLOAT4OID, PgWasmTypeKind::F32)),
        "float8" | "double precision" | "double" => Ok((pg_sys::FLOAT8OID, PgWasmTypeKind::F64)),
        "text" | "varchar" => Ok((pg_sys::TEXTOID, PgWasmTypeKind::String)),
        "bytea" => Ok((pg_sys::BYTEAOID, PgWasmTypeKind::Bytes)),
        "json" | "jsonb" => Ok((pg_sys::JSONBOID, PgWasmTypeKind::Bytes)),
        "int4[]" | "integer[]" => Ok((pg_sys::INT4ARRAYOID, PgWasmTypeKind::Int4Array)),
        "text[]" => Ok((pg_sys::TEXTARRAYOID, PgWasmTypeKind::TextArray)),
        other => Err(format!(
            "unknown SQL type name {other:?} (use int2–int8, bool, float4/8, text, text[], bytea, int4[], json/jsonb, or a numeric OID)"
        )),
    }
}

/// Turn a resolved hint into registry/call metadata (caller validates wasm shapes).
pub fn signature_from_hint(hint: &ExportTypeHint) -> ExportSignature {
    let args = hint
        .args
        .iter()
        .map(|(oid, k)| PgWasmArgDesc {
            pg_oid: *oid,
            kind: *k,
        })
        .collect();
    let ret = PgWasmReturnDesc {
        pg_oid: hint.ret.0,
        kind: hint.ret.1,
    };
    ExportSignature {
        args,
        ret,
        wit_interface: hint.wit_interface.clone(),
        component_dynamic_plan: None,
    }
}

/// `true` when this plan cannot be expressed with the legacy `get_typed_func` tuple fast path.
pub fn component_plan_needs_dynamic_call(plan: &ComponentDynCallPlan) -> bool {
    plan.params.iter().any(marshal_type_non_primitive_fast)
        || marshal_type_non_primitive_fast(&plan.result)
}

fn marshal_type_non_primitive_fast(m: &MarshalType) -> bool {
    match m {
        MarshalType::Bool
        | MarshalType::S32
        | MarshalType::U32
        | MarshalType::S64
        | MarshalType::U64
        | MarshalType::F32
        | MarshalType::F64 => false,
        MarshalType::S8
        | MarshalType::U8
        | MarshalType::S16
        | MarshalType::U16
        | MarshalType::Char
        | MarshalType::String
        | MarshalType::List(_)
        | MarshalType::Record(_)
        | MarshalType::Tuple(_)
        | MarshalType::Variant(_)
        | MarshalType::Option(_)
        | MarshalType::Result { .. }
        | MarshalType::Enum(_)
        | MarshalType::Flags(_) => true,
    }
}

/// Build SQL-facing argument/return descriptors from a marshal plan.
pub fn pg_descriptors_from_marshal_plan(
    plan: &ComponentDynCallPlan,
) -> Option<(Vec<PgWasmArgDesc>, PgWasmReturnDesc)> {
    let args: Option<Vec<PgWasmArgDesc>> =
        plan.params.iter().map(marshal_type_to_arg_desc).collect();
    let args = args?;
    let ret = marshal_type_to_return_desc(&plan.result)?;
    Some((args, ret))
}

fn marshal_type_to_arg_desc(m: &MarshalType) -> Option<PgWasmArgDesc> {
    Some(match m {
        MarshalType::Bool => PgWasmArgDesc {
            pg_oid: pg_sys::BOOLOID,
            kind: PgWasmTypeKind::Bool,
        },
        MarshalType::S8 | MarshalType::U8 | MarshalType::S16 | MarshalType::U16 => PgWasmArgDesc {
            pg_oid: pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        },
        MarshalType::S32 | MarshalType::U32 => PgWasmArgDesc {
            pg_oid: pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        },
        MarshalType::S64 | MarshalType::U64 => PgWasmArgDesc {
            pg_oid: pg_sys::INT8OID,
            kind: PgWasmTypeKind::I64,
        },
        MarshalType::F32 => PgWasmArgDesc {
            pg_oid: pg_sys::FLOAT4OID,
            kind: PgWasmTypeKind::F32,
        },
        MarshalType::F64 => PgWasmArgDesc {
            pg_oid: pg_sys::FLOAT8OID,
            kind: PgWasmTypeKind::F64,
        },
        MarshalType::Char | MarshalType::String => PgWasmArgDesc {
            pg_oid: pg_sys::TEXTOID,
            kind: PgWasmTypeKind::String,
        },
        MarshalType::List(inner) => match inner.as_ref() {
            MarshalType::U8 => PgWasmArgDesc {
                pg_oid: pg_sys::BYTEAOID,
                kind: PgWasmTypeKind::Bytes,
            },
            MarshalType::S32 | MarshalType::U32 => PgWasmArgDesc {
                pg_oid: pg_sys::INT4ARRAYOID,
                kind: PgWasmTypeKind::Int4Array,
            },
            MarshalType::String => PgWasmArgDesc {
                pg_oid: pg_sys::TEXTARRAYOID,
                kind: PgWasmTypeKind::TextArray,
            },
            _ => return None,
        },
        MarshalType::Record(_)
        | MarshalType::Tuple(_)
        | MarshalType::Variant(_)
        | MarshalType::Enum(_)
        | MarshalType::Flags(_)
        | MarshalType::Option(_)
        | MarshalType::Result { .. } => PgWasmArgDesc {
            pg_oid: pg_sys::JSONBOID,
            kind: PgWasmTypeKind::Bytes,
        },
    })
}

fn marshal_type_to_return_desc(m: &MarshalType) -> Option<PgWasmReturnDesc> {
    let a = marshal_type_to_arg_desc(m)?;
    Some(PgWasmReturnDesc {
        pg_oid: a.pg_oid,
        kind: a.kind,
    })
}

/// Returns `true` if the export hint matches the signature implied by the WIT-backed marshal plan.
pub fn export_hint_matches_marshal_plan(
    hint: &ExportTypeHint,
    plan: &ComponentDynCallPlan,
) -> Result<(), String> {
    let Some((args, ret)) = pg_descriptors_from_marshal_plan(plan) else {
        return Err(
            "pg_wasm: could not derive PostgreSQL types from component export (unsupported WIT shape)"
                .into(),
        );
    };
    if hint.args.len() != args.len() {
        return Err(format!(
            "pg_wasm: export hint lists {} arguments but WIT signature has {}",
            hint.args.len(),
            args.len()
        ));
    }
    for (i, ((ho, hk), d)) in hint.args.iter().zip(&args).enumerate() {
        let mt = &plan.params[i];
        if slot_hint_matches_wit_descriptor(*ho, *hk, d, mt)? {
            continue;
        }
        return Err(format!(
            "pg_wasm: export hint arg[{i}] (oid={ho}, kind={hk:?}) does not match WIT mapping (oid={}, kind={:?})",
            d.pg_oid, d.kind
        ));
    }
    let ret_mt = &plan.result;
    let ret_default = PgWasmArgDesc {
        pg_oid: ret.pg_oid,
        kind: ret.kind,
    };
    if !slot_hint_matches_wit_descriptor(hint.ret.0, hint.ret.1, &ret_default, ret_mt)? {
        return Err(format!(
            "pg_wasm: export hint return (oid={}, kind={:?}) does not match WIT mapping (oid={}, kind={:?})",
            hint.ret.0, hint.ret.1, ret.pg_oid, ret.kind
        ));
    }
    Ok(())
}

fn slot_hint_matches_wit_descriptor(
    ho: Oid,
    hk: PgWasmTypeKind,
    default: &PgWasmArgDesc,
    mt: &MarshalType,
) -> Result<bool, String> {
    if ho == default.pg_oid && hk == default.kind {
        return Ok(true);
    }
    if hk == PgWasmTypeKind::Composite && pg_type_oid_is_composite(ho)? {
        if crate::composite_layout::marshal_type_uses_composite_surface(mt) {
            crate::composite_layout::validate_composite_typoid_matches_marshal(ho, mt)?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// Expected PostgreSQL `typid` for a leaf marshal slot (no `record` / `tuple` aggregates).
pub(crate) fn marshal_leaf_expected_pg_typoid(mt: &MarshalType) -> Option<Oid> {
    match mt {
        MarshalType::Record(_) | MarshalType::Tuple(_) => None,
        _ => marshal_type_to_arg_desc(mt).map(|a| a.pg_oid),
    }
}

/// Build [`ExportSignature`] from a validated component hint and WIT marshal plan.
pub fn export_signature_from_component_hint(
    plan: ComponentDynCallPlan,
    hint: &ExportTypeHint,
) -> Option<ExportSignature> {
    let args = hint
        .args
        .iter()
        .map(|(oid, k)| PgWasmArgDesc {
            pg_oid: *oid,
            kind: *k,
        })
        .collect();
    let ret = PgWasmReturnDesc {
        pg_oid: hint.ret.0,
        kind: hint.ret.1,
    };
    let dynamic = component_plan_needs_dynamic_call(&plan).then(|| plan);
    Some(ExportSignature {
        args,
        ret,
        wit_interface: hint.wit_interface.clone(),
        component_dynamic_plan: dynamic,
    })
}
