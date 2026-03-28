//! PostgreSQL ↔ WASM value representation (marshal/unmarshal filled in with the trampoline).

use pgrx::pg_sys::Oid;

/// Classifies how a SQL argument maps to the WASM ABI for core modules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgWasmTypeKind {
    I32,
    I64,
    F32,
    F64,
    /// Length-prefixed UTF-8 or pointer/length pair (runtime-specific).
    String,
    /// Opaque bytes (e.g. JSONB serialized).
    Bytes,
}

/// Describes one SQL argument position for dynamic dispatch.
#[derive(Debug, Clone)]
pub struct PgWasmArgDesc {
    pub pg_oid: Oid,
    pub kind: PgWasmTypeKind,
}

/// Describes the return mapping for a WASM export registered as a UDF.
#[derive(Debug, Clone)]
pub struct PgWasmReturnDesc {
    pub pg_oid: Oid,
    pub kind: PgWasmTypeKind,
}

impl Default for PgWasmReturnDesc {
    fn default() -> Self {
        Self {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        }
    }
}

/// Placeholder for the per-export signature table used by the trampoline.
#[derive(Debug, Clone, Default)]
pub struct ExportSignature {
    pub args: Vec<PgWasmArgDesc>,
    pub ret: PgWasmReturnDesc,
}
