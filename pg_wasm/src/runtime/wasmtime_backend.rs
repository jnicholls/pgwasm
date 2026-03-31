//! Wasmtime backend: process singleton owns [`wasmtime::Engine`] and compiled [`Module`]s.
//!
//! Callers outside this module should use the free functions ([`compile_store_and_list_exports`],
//! [`remove_compiled_module`], [`call_mem_in_out`], …) so global locking stays an implementation
//! detail.
//!
//! **Buffer convention:** for `text` / `bytea` / `jsonb`, wasm exports `(i32, i32) -> i32`. The host writes
//! the input at [`MEM_IO_INPUT_BASE`] with the given length; the guest writes output at
//! `align8(base + input_len)` and returns the output length as `i32`. The module must export `memory`.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use wasmtime::{Engine, ExternType, Instance, Memory, Module, Store, ValType};

use super::{RuntimeKind, WasmRuntimeBackend};
use crate::{
    mapping::{
        ExportHintMap, ExportSignature, ExportTypeHint, PgWasmArgDesc, PgWasmReturnDesc,
        PgWasmTypeKind, signature_from_hint,
    },
    registry::ModuleId,
};

static INSTANCE: OnceLock<Mutex<WasmtimeBackend>> = OnceLock::new();

/// Guest linear memory offset where the host writes the input slice (see module-level docs).
pub const MEM_IO_INPUT_BASE: u32 = 1024;

/// Upper bound on returned byte length from a single buffer-style wasm call (16 MiB).
const MEM_IO_MAX_OUT: u32 = 16 * 1024 * 1024;

fn mutex() -> &'static Mutex<WasmtimeBackend> {
    INSTANCE.get_or_init(|| Mutex::new(WasmtimeBackend::empty()))
}

/// Compile `wasm`, store under `id`, and return exports to register as SQL functions.
///
/// `export_hints` disambiguates wasm shapes (e.g. `(i32,i32)->i32` as `bytea`/`text` vs two
/// `int4`s) and enables buffer returns with zero SQL arguments.
pub fn compile_store_and_list_exports(
    id: ModuleId,
    wasm: &[u8],
    export_hints: &ExportHintMap,
) -> Result<Vec<(String, ExportSignature)>, String> {
    let mut g = mutex().lock().map_err(|e| e.to_string())?;
    match g.compile_store(id, wasm, export_hints) {
        Ok(out) => Ok(out),
        Err(e) => {
            g.remove_stored(id);
            Err(e)
        }
    }
}

pub fn remove_compiled_module(id: ModuleId) {
    if let Ok(mut g) = mutex().lock() {
        g.remove_stored(id);
    }
}

fn wasm_types_for_hint(hint: &ExportTypeHint) -> Result<(Vec<ValType>, Vec<ValType>), String> {
    if hint.args.is_empty()
        && matches!(
            hint.ret.1,
            PgWasmTypeKind::String | PgWasmTypeKind::Bytes
        )
    {
        return Ok((
            vec![ValType::I32, ValType::I32],
            vec![ValType::I32],
        ));
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
        }
    }
    let results = vec![match hint.ret.1 {
        PgWasmTypeKind::I32 | PgWasmTypeKind::Bool => ValType::I32,
        PgWasmTypeKind::I64 => ValType::I64,
        PgWasmTypeKind::F32 => ValType::F32,
        PgWasmTypeKind::F64 => ValType::F64,
        PgWasmTypeKind::String | PgWasmTypeKind::Bytes => ValType::I32,
    }];
    Ok((params, results))
}

fn valtype_eq(a: &ValType, b: &ValType) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

fn val_slices_eq(a: &[ValType], b: &[ValType]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| valtype_eq(x, y))
}

fn hint_matches_wasm(
    hint: &ExportTypeHint,
    params: &[ValType],
    results: &[ValType],
) -> Result<(), String> {
    let (exp_p, exp_r) = wasm_types_for_hint(hint)?;
    if !val_slices_eq(params, &exp_p) || !val_slices_eq(results, &exp_r) {
        return Err(format!(
            "wasm params/results {:?} -> {:?} do not match load options for this export (expected {:?} -> {:?})",
            params, results, exp_p, exp_r
        ));
    }
    Ok(())
}

fn module_exports_memory(module: &Module) -> bool {
    module
        .exports()
        .any(|e| e.name() == "memory" && matches!(e.ty(), ExternType::Memory(_)))
}

/// Copy `input` into guest memory at [`MEM_IO_INPUT_BASE`], call `export` `(ptr,len)->out_len`, read output.
pub fn call_mem_in_out(
    module: ModuleId,
    export: &str,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    let (engine, arc) = {
        let g = mutex().lock().map_err(|e| e.to_string())?;
        let arc = g
            .modules
            .get(&module)
            .cloned()
            .ok_or_else(|| format!("pg_wasm: no wasm module for id {}", module.0))?;
        Ok::<_, String>((g.engine.clone(), arc))
    }?;
    let mut store = Store::new(&engine, ());
    let instance = Instance::new(&mut store, &arc, &[]).map_err(|e| e.to_string())?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| "pg_wasm: wasm module has no exported `memory`".to_string())?;
    let f = instance
        .get_typed_func::<(i32, i32), i32>(&mut store, export)
        .map_err(|e| e.to_string())?;

    let base = MEM_IO_INPUT_BASE as usize;
    let out_base = base + ((input.len() + 7) & !7);
    let need = out_base.saturating_add(MEM_IO_MAX_OUT as usize);
    grow_memory_to(&mut store, &memory, need)?;

    memory
        .write(&mut store, base, input)
        .map_err(|e| e.to_string())?;

    let out_len = f
        .call(&mut store, (MEM_IO_INPUT_BASE as i32, input.len() as i32))
        .map_err(|e| e.to_string())?;
    if out_len < 0 {
        return Err(format!("pg_wasm: wasm returned negative output length {out_len}"));
    }
    let out_len = out_len as u32;
    if out_len > MEM_IO_MAX_OUT {
        return Err(format!(
            "pg_wasm: wasm output length {out_len} exceeds cap ({MEM_IO_MAX_OUT})"
        ));
    }
    let end = out_base + out_len as usize;
    grow_memory_to(&mut store, &memory, end)?;

    let mut out = vec![0u8; out_len as usize];
    memory
        .read(&mut store, out_base, &mut out)
        .map_err(|e| e.to_string())?;
    Ok(out)
}

fn grow_memory_to(store: &mut Store<()>, memory: &Memory, need: usize) -> Result<(), String> {
    let page = 65536usize;
    let mut current = memory.data_size(&mut *store);
    while current < need {
        memory
            .grow(&mut *store, 1)
            .map_err(|e| format!("pg_wasm: memory.grow failed: {e}"))?;
        current += page;
    }
    Ok(())
}

macro_rules! scalar_call {
    ($module:expr, $export:expr, $($T:ty),* => $R:ty, $args:expr) => {{
        let (engine, arc) = {
            let g = mutex().lock().map_err(|e| e.to_string())?;
            let arc = g
                .modules
                .get(&$module)
                .cloned()
                .ok_or_else(|| format!("pg_wasm: no wasm module for id {}", $module.0))?;
            Ok::<_, String>((g.engine.clone(), arc))
        }?;
        let mut store = Store::new(&engine, ());
        let instance = Instance::new(&mut store, &arc, &[]).map_err(|e| e.to_string())?;
        let f = instance
            .get_typed_func::<($($T,)*), $R>(&mut store, $export)
            .map_err(|e| e.to_string())?;
        f.call(&mut store, $args).map_err(|e| e.to_string())
    }};
}

pub fn call_i32_arity0(module: ModuleId, export: &str) -> Result<i32, String> {
    scalar_call!(module, export, => i32, ())
}

pub fn call_i32_arity1(module: ModuleId, export: &str, a: i32) -> Result<i32, String> {
    scalar_call!(module, export, i32 => i32, (a,))
}

pub fn call_i32_arity2(module: ModuleId, export: &str, a: i32, b: i32) -> Result<i32, String> {
    scalar_call!(module, export, i32, i32 => i32, (a, b))
}

pub fn call_i64_arity0(module: ModuleId, export: &str) -> Result<i64, String> {
    scalar_call!(module, export, => i64, ())
}

pub fn call_i64_arity1(module: ModuleId, export: &str, a: i64) -> Result<i64, String> {
    scalar_call!(module, export, i64 => i64, (a,))
}

pub fn call_f32_arity0(module: ModuleId, export: &str) -> Result<f32, String> {
    scalar_call!(module, export, => f32, ())
}

pub fn call_f32_arity1(module: ModuleId, export: &str, a: f32) -> Result<f32, String> {
    scalar_call!(module, export, f32 => f32, (a,))
}

pub fn call_f32_arity2(module: ModuleId, export: &str, a: f32, b: f32) -> Result<f32, String> {
    scalar_call!(module, export, f32, f32 => f32, (a, b))
}

pub fn call_f64_arity0(module: ModuleId, export: &str) -> Result<f64, String> {
    scalar_call!(module, export, => f64, ())
}

pub fn call_f64_arity1(module: ModuleId, export: &str, a: f64) -> Result<f64, String> {
    scalar_call!(module, export, f64 => f64, (a,))
}

pub fn call_f64_arity2(module: ModuleId, export: &str, a: f64, b: f64) -> Result<f64, String> {
    scalar_call!(module, export, f64, f64 => f64, (a, b))
}

pub fn with_backend<R>(f: impl FnOnce(&WasmtimeBackend) -> R) -> R {
    let g = mutex().lock().expect("pg_wasm: wasmtime backend mutex poisoned");
    f(&g)
}

pub struct WasmtimeBackend {
    engine: Engine,
    modules: HashMap<ModuleId, Arc<Module>>,
}

impl WasmtimeBackend {
    fn empty() -> Self {
        Self {
            engine: Engine::default(),
            modules: HashMap::new(),
        }
    }

    fn compile_store(
        &mut self,
        id: ModuleId,
        wasm: &[u8],
        export_hints: &ExportHintMap,
    ) -> Result<Vec<(String, ExportSignature)>, String> {
        let module = Module::from_binary(&self.engine, wasm).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for export in module.exports() {
            let ExternType::Func(ft) = export.ty() else {
                continue;
            };
            let name = export.name();
            let params: Vec<ValType> = ft.params().collect();
            let results: Vec<ValType> = ft.results().collect();

            if let Some(hint) = export_hints.get(name) {
                hint_matches_wasm(hint, &params, &results)?;
                if uses_linear_memory(hint) && !module_exports_memory(&module) {
                    return Err(format!(
                        "pg_wasm: export {name:?} needs linear memory (export a `memory` from wasm)"
                    ));
                }
                out.push((name.to_string(), signature_from_hint(hint)));
                continue;
            }

            if let Some(sig) = map_export_sig_auto(&params, &results) {
                out.push((name.to_string(), sig));
            }
        }
        self.modules.insert(id, Arc::new(module));
        Ok(out)
    }

    fn remove_stored(&mut self, id: ModuleId) {
        self.modules.remove(&id);
    }
}

impl WasmRuntimeBackend for WasmtimeBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Wasmtime
    }

    fn label(&self) -> &'static str {
        "wasmtime"
    }
}

fn uses_linear_memory(hint: &ExportTypeHint) -> bool {
    hint.args.iter().any(|(_, k)| {
        matches!(
            k,
            PgWasmTypeKind::String | PgWasmTypeKind::Bytes
        )
    }) || matches!(
        hint.ret.1,
        PgWasmTypeKind::String | PgWasmTypeKind::Bytes
    )
}

fn map_export_sig_auto(params: &[ValType], results: &[ValType]) -> Option<ExportSignature> {
    if results.len() != 1 {
        return None;
    }
    let r = &results[0];
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
    })
}
