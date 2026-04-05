//! List core wasm function exports from raw bytes using `wasmparser` (for backends without a
//! host `Module` type, e.g. Extism `CompiledPlugin`).

use wasmparser::{Encoding, ExternalKind, FuncType, Parser, Payload, TypeRef, ValType};

use crate::mapping::{
    ExportHintMap, ExportSignature, ExportTypeHint, PgWasmArgDesc, PgWasmReturnDesc,
    PgWasmTypeKind, signature_from_hint,
};

fn valtype_discriminant(a: ValType, b: ValType) -> bool {
    std::mem::discriminant(&a) == std::mem::discriminant(&b)
}

fn val_slices_eq(a: &[ValType], b: &[ValType]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| valtype_discriminant(*x, *y))
}

fn wasm_types_for_hint(hint: &ExportTypeHint) -> Result<(Vec<ValType>, Vec<ValType>), String> {
    if hint.args.is_empty() && matches!(hint.ret.1, PgWasmTypeKind::String | PgWasmTypeKind::Bytes)
    {
        return Ok((vec![ValType::I32, ValType::I32], vec![ValType::I32]));
    }
    let mut params = Vec::new();
    for (_, k) in &hint.args {
        match k {
            PgWasmTypeKind::I32 | PgWasmTypeKind::Bool => params.push(ValType::I32),
            PgWasmTypeKind::I64 => params.push(ValType::I64),
            PgWasmTypeKind::F32 => params.push(ValType::F32),
            PgWasmTypeKind::F64 => params.push(ValType::F64),
            PgWasmTypeKind::String | PgWasmTypeKind::Bytes => {
                params.push(ValType::I32);
                params.push(ValType::I32);
            }
            PgWasmTypeKind::Int4Array | PgWasmTypeKind::TextArray => {
                return Err(
                    "pg_wasm: int4[] / text[] export hints apply to WebAssembly components only"
                        .into(),
                );
            }
            PgWasmTypeKind::Composite => {
                return Err(
                    "pg_wasm: composite type hints apply to WebAssembly components only".into(),
                );
            }
        }
    }
    let results = vec![match hint.ret.1 {
        PgWasmTypeKind::I32 | PgWasmTypeKind::Bool => ValType::I32,
        PgWasmTypeKind::I64 => ValType::I64,
        PgWasmTypeKind::F32 => ValType::F32,
        PgWasmTypeKind::F64 => ValType::F64,
        PgWasmTypeKind::String | PgWasmTypeKind::Bytes => ValType::I32,
        PgWasmTypeKind::Int4Array | PgWasmTypeKind::TextArray => {
            return Err(
                "pg_wasm: int4[] / text[] export hints apply to WebAssembly components only".into(),
            );
        }
        PgWasmTypeKind::Composite => {
            return Err(
                "pg_wasm: composite type hints apply to WebAssembly components only".into(),
            );
        }
    }];
    Ok((params, results))
}

fn hint_matches_wasm(
    hint: &ExportTypeHint,
    params: &[ValType],
    results: &[ValType],
) -> Result<(), String> {
    let (exp_p, exp_r) = wasm_types_for_hint(hint)?;
    if !val_slices_eq(params, &exp_p) || !val_slices_eq(results, &exp_r) {
        return Err(format!(
            "wasm params/results {params:?} -> {results:?} do not match load options for this export (expected {exp_p:?} -> {exp_r:?})"
        ));
    }
    Ok(())
}

fn uses_linear_memory(hint: &ExportTypeHint) -> bool {
    hint.args
        .iter()
        .any(|(_, k)| matches!(k, PgWasmTypeKind::String | PgWasmTypeKind::Bytes))
        || matches!(hint.ret.1, PgWasmTypeKind::String | PgWasmTypeKind::Bytes)
}

fn map_export_sig_auto(params: &[ValType], results: &[ValType]) -> Option<ExportSignature> {
    if results.len() != 1 {
        return None;
    }
    let r = results[0];
    let ret = match r {
        ValType::I32 => (pgrx::pg_sys::INT4OID, PgWasmTypeKind::I32),
        ValType::I64 => (pgrx::pg_sys::INT8OID, PgWasmTypeKind::I64),
        ValType::F32 => (pgrx::pg_sys::FLOAT4OID, PgWasmTypeKind::F32),
        ValType::F64 => (pgrx::pg_sys::FLOAT8OID, PgWasmTypeKind::F64),
        _ => return None,
    };

    let args: Vec<PgWasmArgDesc> = match params {
        [] => vec![],
        [ValType::I32] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        }],
        [ValType::I64] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT8OID,
            kind: PgWasmTypeKind::I64,
        }],
        [ValType::F32] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::FLOAT4OID,
            kind: PgWasmTypeKind::F32,
        }],
        [ValType::F64] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::FLOAT8OID,
            kind: PgWasmTypeKind::F64,
        }],
        [ValType::I32, ValType::I32] => vec![
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::INT4OID,
                kind: PgWasmTypeKind::I32,
            },
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::INT4OID,
                kind: PgWasmTypeKind::I32,
            },
        ],
        [ValType::F32, ValType::F32] => vec![
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::FLOAT4OID,
                kind: PgWasmTypeKind::F32,
            },
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::FLOAT4OID,
                kind: PgWasmTypeKind::F32,
            },
        ],
        [ValType::F64, ValType::F64] => vec![
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::FLOAT8OID,
                kind: PgWasmTypeKind::F64,
            },
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::FLOAT8OID,
                kind: PgWasmTypeKind::F64,
            },
        ],
        _ => return None,
    };

    Some(ExportSignature {
        args,
        ret: PgWasmReturnDesc {
            pg_oid: ret.0,
            kind: ret.1,
        },
        wit_interface: None,
        component_dynamic_plan: None,
    })
}

fn module_exports_memory_wasm(wasm: &[u8]) -> Result<bool, String> {
    let parser = Parser::new(0);
    for payload in parser.parse_all(wasm) {
        let payload = payload.map_err(|e| e.to_string())?;
        if let Payload::ExportSection(reader) = payload {
            for g in reader {
                let export = g.map_err(|e| e.to_string())?;
                if export.kind == ExternalKind::Memory {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

/// Returns `(exports, needs_wasi)` for a **core** wasm module (`\0asm`).
pub fn list_core_exports_from_wasm_bytes(
    wasm: &[u8],
    export_hints: &ExportHintMap,
) -> Result<(Vec<(String, ExportSignature)>, bool), String> {
    let parser = Parser::new(0);
    let mut type_defs: Vec<FuncType> = Vec::new();
    let mut import_func_count: u32 = 0;
    let mut imports_wasi = false;
    let mut func_body_type_indices: Vec<u32> = Vec::new();
    let mut func_exports: Vec<(String, u32)> = Vec::new();

    for payload in parser.parse_all(wasm) {
        let payload = payload.map_err(|e| e.to_string())?;
        match payload {
            Payload::Version { encoding, .. } => {
                if encoding != Encoding::Module {
                    return Err(
                        "pg_wasm: wasm_bytes_exports only supports core modules, not components"
                            .into(),
                    );
                }
            }
            Payload::TypeSection(reader) => {
                for ty in reader.into_iter_err_on_gc_types() {
                    let ft = ty.map_err(|e| e.to_string())?;
                    type_defs.push(ft);
                }
            }
            Payload::ImportSection(reader) => {
                for imp in reader.into_imports() {
                    let imp = imp.map_err(|e| e.to_string())?;
                    if imp.module == "wasi_snapshot_preview1" || imp.module == "wasi_unstable" {
                        imports_wasi = true;
                    }
                    if matches!(imp.ty, TypeRef::Func(_) | TypeRef::FuncExact(_)) {
                        import_func_count = import_func_count.saturating_add(1);
                    }
                }
            }
            Payload::FunctionSection(reader) => {
                for idx in reader {
                    func_body_type_indices.push(idx.map_err(|e| e.to_string())?);
                }
            }
            Payload::ExportSection(reader) => {
                for g in reader {
                    let export = g.map_err(|e| e.to_string())?;
                    if export.kind == ExternalKind::Func {
                        func_exports.push((export.name.to_string(), export.index));
                    }
                }
            }
            _ => {}
        }
    }

    let has_memory = module_exports_memory_wasm(wasm)?;

    let mut defined_sigs: Vec<(Vec<ValType>, Vec<ValType>)> = Vec::new();
    for type_idx in &func_body_type_indices {
        let ft = type_defs
            .get(*type_idx as usize)
            .ok_or_else(|| format!("pg_wasm: invalid function type index {type_idx}"))?;
        defined_sigs.push((ft.params().to_vec(), ft.results().to_vec()));
    }

    let mut out = Vec::new();
    for (name, fidx) in func_exports {
        if fidx < import_func_count {
            continue;
        }
        let local = (fidx - import_func_count) as usize;
        let Some((params, results)) = defined_sigs.get(local).cloned() else {
            return Err(format!(
                "pg_wasm: export {name:?} function index {fidx} out of range"
            ));
        };

        if let Some(hint) = export_hints.get(name.as_str()) {
            hint_matches_wasm(hint, &params, &results)?;
            if uses_linear_memory(hint) && !has_memory {
                return Err(format!(
                    "pg_wasm: export {name:?} needs linear memory (export a `memory` from wasm)"
                ));
            }
            out.push((name, signature_from_hint(hint)));
            continue;
        }

        if let Some(sig) = map_export_sig_auto(&params, &results) {
            out.push((name, sig));
        }
    }

    Ok((out, imports_wasi))
}
