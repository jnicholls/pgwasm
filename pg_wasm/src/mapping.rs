//! PostgreSQL ↔ WASM value representation and export registration hints (plan §3).

use std::collections::HashMap;

use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use serde_json::Value;

/// Maps export name → SQL types and optional WIT note (for future component support).
pub type ExportHintMap = HashMap<String, ExportTypeHint>;

/// Per-export types from `pg_wasm_load(..., '{"exports":{...}}')`.
#[derive(Clone, Debug)]
pub struct ExportTypeHint {
    pub args: Vec<(Oid, PgWasmTypeKind)>,
    pub ret: (Oid, PgWasmTypeKind),
    /// Optional WIT / component interface hint (ignored for core wasm today; plan §3).
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
    String,
    /// Raw bytes; `jsonb` uses JSON UTF-8 bytes (same wasm shape as [`PgWasmTypeKind::String`]).
    Bytes,
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

/// Per-export signature for catalog + trampoline (WIT layout reserved).
#[derive(Clone, Debug, Default)]
pub struct ExportSignature {
    pub args: Vec<PgWasmArgDesc>,
    pub ret: PgWasmReturnDesc,
    pub wit_interface: Option<String>,
}

/// Parse `exports` object from load `options` JSON (see [`ExportTypeHint`]).
pub fn parse_export_hints(val: &Value) -> Result<ExportHintMap, String> {
    let Some(obj) = val.get("exports").and_then(|e| e.as_object()) else {
        return Ok(ExportHintMap::new());
    };
    let mut out = ExportHintMap::new();
    for (name, spec) in obj {
        let spec = spec.as_object().ok_or_else(|| {
            format!(
                "pg_wasm_load exports.{name}: expected object with args/returns (or return)"
            )
        })?;
        let args_val = spec
            .get("args")
            .ok_or_else(|| format!("pg_wasm_load exports.{name}: missing \"args\""))?;
        let args_arr = args_val.as_array().ok_or_else(|| {
            format!("pg_wasm_load exports.{name}.args: expected JSON array of type names")
        })?;
        let mut args = Vec::with_capacity(args_arr.len());
        for (j, a) in args_arr.iter().enumerate() {
            args.push(parse_one_sql_type(a).map_err(|e| {
                format!("pg_wasm_load exports.{name}.args[{j}]: {e}")
            })?);
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
        let ret = parse_one_sql_type(spec.get(ret_key).ok_or_else(|| {
            format!("pg_wasm_load exports.{name}: missing returns value")
        })?)
        .map_err(|e| format!("pg_wasm_load exports.{name}.{ret_key}: {e}"))?;
        let wit_interface = spec
            .get("wit")
            .and_then(|w| w.as_str())
            .map(str::to_string);
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
    if let Some(n) = v.as_u64() {
        let oid = Oid::from(n as u32);
        if oid == pg_sys::InvalidOid {
            return Err("InvalidOid not allowed".into());
        }
        // Custom OIDs: treat as opaque byte payload (SQL should use types binary-compatible with bytea / cast).
        return Ok((oid, PgWasmTypeKind::Bytes));
    }
    if let Some(s) = v.as_str() {
        return sql_name_to_pg_descriptor(s);
    }
    Err("expected string type name or numeric type OID".into())
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
        other => Err(format!(
            "unknown SQL type name {other:?} (use int2–int8, bool, float4/8, text, bytea, json/jsonb, or a numeric OID)"
        )),
    }
}

/// Turn a resolved hint into registry/call metadata (caller validates wasm shapes).
#[must_use]
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
    }
}
