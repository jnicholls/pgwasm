//! Wasmer backend: core WebAssembly modules only (no components, no WASI in this backend).

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use wasmer::{ExternType, Instance, Memory, Module, Pages, Store, Type, imports};

use super::{RuntimeKind, WasmRuntimeBackend};
use crate::{
    abi::WasmAbiKind,
    mapping::{
        ExportHintMap, ExportSignature, ExportTypeHint, PgWasmArgDesc, PgWasmReturnDesc,
        PgWasmTypeKind, signature_from_hint,
    },
    registry::{self, ModuleId},
};

static INSTANCE: OnceLock<Mutex<WasmerBackendState>> = OnceLock::new();

pub const MEM_IO_INPUT_BASE: u32 = 1024;
const MEM_IO_MAX_OUT: u32 = 16 * 1024 * 1024;

struct WasmerBackendState {
    artifacts: HashMap<ModuleId, Arc<Module>>,
}

fn mutex() -> &'static Mutex<WasmerBackendState> {
    INSTANCE.get_or_init(|| {
        Mutex::new(WasmerBackendState {
            artifacts: HashMap::new(),
        })
    })
}

fn core_module_imports_wasi(module: &Module) -> bool {
    module
        .imports()
        .any(|imp| imp.module() == "wasi_snapshot_preview1" || imp.module() == "wasi_unstable")
}

fn wasmer_types_for_hint(hint: &ExportTypeHint) -> Result<(Vec<Type>, Vec<Type>), String> {
    if hint.args.is_empty() && matches!(hint.ret.1, PgWasmTypeKind::String | PgWasmTypeKind::Bytes)
    {
        return Ok((vec![Type::I32, Type::I32], vec![Type::I32]));
    }
    let mut params = Vec::new();
    for (_, k) in &hint.args {
        match k {
            PgWasmTypeKind::I32 | PgWasmTypeKind::Bool => params.push(Type::I32),
            PgWasmTypeKind::I64 => params.push(Type::I64),
            PgWasmTypeKind::F32 => params.push(Type::F32),
            PgWasmTypeKind::F64 => params.push(Type::F64),
            PgWasmTypeKind::String | PgWasmTypeKind::Bytes => {
                params.push(Type::I32);
                params.push(Type::I32);
            }
        }
    }
    let results = vec![match hint.ret.1 {
        PgWasmTypeKind::I32 | PgWasmTypeKind::Bool => Type::I32,
        PgWasmTypeKind::I64 => Type::I64,
        PgWasmTypeKind::F32 => Type::F32,
        PgWasmTypeKind::F64 => Type::F64,
        PgWasmTypeKind::String | PgWasmTypeKind::Bytes => Type::I32,
    }];
    Ok((params, results))
}

fn type_disc(a: Type, b: Type) -> bool {
    std::mem::discriminant(&a) == std::mem::discriminant(&b)
}

fn type_slices_eq(a: &[Type], b: &[Type]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| type_disc(*x, *y))
}

fn hint_matches_wasm(
    hint: &ExportTypeHint,
    params: &[Type],
    results: &[Type],
) -> Result<(), String> {
    let (exp_p, exp_r) = wasmer_types_for_hint(hint)?;
    if !type_slices_eq(params, &exp_p) || !type_slices_eq(results, &exp_r) {
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

fn map_export_sig_auto(params: &[Type], results: &[Type]) -> Option<ExportSignature> {
    if results.len() != 1 {
        return None;
    }
    let r = results[0];
    let ret = match r {
        Type::I32 => (pgrx::pg_sys::INT4OID, PgWasmTypeKind::I32),
        Type::I64 => (pgrx::pg_sys::INT8OID, PgWasmTypeKind::I64),
        Type::F32 => (pgrx::pg_sys::FLOAT4OID, PgWasmTypeKind::F32),
        Type::F64 => (pgrx::pg_sys::FLOAT8OID, PgWasmTypeKind::F64),
        _ => return None,
    };

    let args: Vec<PgWasmArgDesc> = match params {
        [] => vec![],
        [Type::I32] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        }],
        [Type::I64] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT8OID,
            kind: PgWasmTypeKind::I64,
        }],
        [Type::F32] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::FLOAT4OID,
            kind: PgWasmTypeKind::F32,
        }],
        [Type::F64] => vec![PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::FLOAT8OID,
            kind: PgWasmTypeKind::F64,
        }],
        [Type::I32, Type::I32] => vec![
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::INT4OID,
                kind: PgWasmTypeKind::I32,
            },
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::INT4OID,
                kind: PgWasmTypeKind::I32,
            },
        ],
        [Type::F32, Type::F32] => vec![
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::FLOAT4OID,
                kind: PgWasmTypeKind::F32,
            },
            PgWasmArgDesc {
                pg_oid: pgrx::pg_sys::FLOAT4OID,
                kind: PgWasmTypeKind::F32,
            },
        ],
        [Type::F64, Type::F64] => vec![
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

fn module_exports_memory(module: &Module) -> bool {
    module
        .exports()
        .any(|e| matches!(e.ty(), ExternType::Memory(_)) && e.name() == "memory")
}

fn list_core_exports(
    module: &Module,
    export_hints: &ExportHintMap,
) -> Result<Vec<(String, ExportSignature)>, String> {
    let mut out = Vec::new();
    for export in module.exports() {
        let ExternType::Function(ft) = export.ty() else {
            continue;
        };
        let name = export.name();
        let params: Vec<Type> = ft.params().to_vec();
        let results: Vec<Type> = ft.results().to_vec();

        if let Some(hint) = export_hints.get(name) {
            hint_matches_wasm(hint, &params, &results)?;
            if uses_linear_memory(hint) && !module_exports_memory(module) {
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
    Ok(out)
}

pub fn compile_store_and_list_exports(
    id: ModuleId,
    wasm: &[u8],
    export_hints: &ExportHintMap,
    abi: WasmAbiKind,
) -> Result<(Vec<(String, ExportSignature)>, bool), String> {
    if abi != WasmAbiKind::CoreWasm {
        return Err(
            "pg_wasm: Wasmer backend only supports core WebAssembly modules (not components)"
                .into(),
        );
    }
    let store = Store::default();
    let module = Module::new(&store, wasm).map_err(|e| format!("wasmer compile: {e}"))?;
    let needs_wasi = core_module_imports_wasi(&module);
    if needs_wasi {
        return Err(
            "pg_wasm: module imports WASI; the Wasmer backend does not implement WASI — use runtime \"wasmtime\" in load options"
                .into(),
        );
    }
    let out = list_core_exports(&module, export_hints)?;
    let mut g = mutex().lock().map_err(|e| e.to_string())?;
    g.artifacts.insert(id, Arc::new(module));
    Ok((out, false))
}

pub fn remove_compiled_module(id: ModuleId) {
    if let Ok(mut g) = mutex().lock() {
        g.artifacts.remove(&id);
    }
}

fn get_module(id: ModuleId) -> Result<Arc<Module>, String> {
    let g = mutex().lock().map_err(|e| e.to_string())?;
    g.artifacts
        .get(&id)
        .cloned()
        .ok_or_else(|| format!("pg_wasm: no wasmer module for id {}", id.0))
}

fn instantiate(id: ModuleId) -> Result<(Instance, Store), String> {
    let _ = registry::module_needs_wasi(id).ok_or_else(|| {
        format!(
            "pg_wasm: no metadata for wasm module id {} (not loaded in this backend)",
            id.0
        )
    })?;
    let module = get_module(id)?;
    let mut store = Store::default();
    let import_object = imports! {};
    let instance = Instance::new(&mut store, &module, &import_object)
        .map_err(|e| format!("pg_wasm: wasmer instantiate: {e}"))?;
    Ok((instance, store))
}

fn grow_memory_to(mem: &Memory, store: &mut Store, need: u64) -> Result<(), String> {
    let page = 65536u64;
    let view = mem.view(store);
    let mut current = view.data_size();
    while current < need {
        mem.grow(store, Pages(1))
            .map_err(|e| format!("pg_wasm: wasmer memory.grow failed: {e}"))?;
        current = current.saturating_add(page);
    }
    Ok(())
}

fn after_guest_call(module: ModuleId, instance: &Instance, store: &mut Store) {
    if let Ok(mem) = instance.exports.get_memory("memory") {
        let sz = mem.view(store).data_size();
        crate::metrics::record_memory_sample(module, sz);
    }
}

pub fn call_mem_in_out(module: ModuleId, export: &str, input: &[u8]) -> Result<Vec<u8>, String> {
    let (instance, mut store) = instantiate(module)?;
    let mem = instance
        .exports
        .get_memory("memory")
        .map_err(|_| "pg_wasm: wasm module has no exported `memory`".to_string())?;
    let f = instance
        .exports
        .get_typed_function::<(i32, i32), i32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer typed export {export:?}: {e}"))?;

    let base = MEM_IO_INPUT_BASE as u64;
    let out_base = base + (((input.len() as u64) + 7) & !7);
    let need = out_base.saturating_add(u64::from(MEM_IO_MAX_OUT));
    grow_memory_to(&mem, &mut store, need)?;

    mem.view(&store)
        .write(base, input)
        .map_err(|e| format!("pg_wasm: wasmer memory write: {e}"))?;

    let out_len = f
        .call(&mut store, MEM_IO_INPUT_BASE as i32, input.len() as i32)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    if out_len < 0 {
        return Err(format!(
            "pg_wasm: wasm returned negative output length {out_len}"
        ));
    }
    let out_len = out_len as u32;
    if out_len > MEM_IO_MAX_OUT {
        return Err(format!(
            "pg_wasm: wasm output length {out_len} exceeds cap ({MEM_IO_MAX_OUT})"
        ));
    }
    let end = out_base + u64::from(out_len);
    grow_memory_to(&mem, &mut store, end)?;

    let mut out = vec![0u8; out_len as usize];
    mem.view(&store)
        .read(out_base, &mut out)
        .map_err(|e| format!("pg_wasm: wasmer memory read: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(out)
}

pub fn call_lifecycle_hook(
    module: ModuleId,
    export_name: &str,
    config: &[u8],
) -> Result<(), String> {
    let (instance, mut store) = instantiate(module)?;
    if instance
        .exports
        .get_typed_function::<(), ()>(&store, export_name)
        .is_ok()
    {
        let f = instance
            .exports
            .get_typed_function::<(), ()>(&store, export_name)
            .map_err(|e| e.to_string())?;
        let _ = f.call(&mut store);
        after_guest_call(module, &instance, &mut store);
        return Ok(());
    }
    if instance
        .exports
        .get_typed_function::<(i32, i32), ()>(&store, export_name)
        .is_ok()
    {
        let mem = instance.exports.get_memory("memory").map_err(|_| {
            "pg_wasm: lifecycle hook (ptr,len) requires exported `memory`".to_string()
        })?;
        let f = instance
            .exports
            .get_typed_function::<(i32, i32), ()>(&store, export_name)
            .map_err(|e| e.to_string())?;
        let ptr = MEM_IO_INPUT_BASE as i32;
        let len = i32::try_from(config.len())
            .map_err(|_| "pg_wasm: lifecycle config exceeds i32::MAX bytes".to_string())?;
        if !config.is_empty() {
            let base = u64::from(MEM_IO_INPUT_BASE);
            let need = base + config.len() as u64;
            grow_memory_to(&mem, &mut store, need)?;
            mem.view(&store)
                .write(base, config)
                .map_err(|e| format!("pg_wasm: wasmer lifecycle write: {e}"))?;
        }
        f.call(&mut store, ptr, len)
            .map_err(|e| format!("pg_wasm: wasmer lifecycle: {e}"))?;
        after_guest_call(module, &instance, &mut store);
        return Ok(());
    }
    Ok(())
}

pub fn call_i32_arity0(module: ModuleId, export: &str) -> Result<i32, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(), i32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_i32_arity1(module: ModuleId, export: &str, a: i32) -> Result<i32, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<i32, i32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_i32_arity2(module: ModuleId, export: &str, a: i32, b: i32) -> Result<i32, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(i32, i32), i32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a, b)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_i64_arity0(module: ModuleId, export: &str) -> Result<i64, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(), i64>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_i64_arity1(module: ModuleId, export: &str, a: i64) -> Result<i64, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<i64, i64>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_f32_arity0(module: ModuleId, export: &str) -> Result<f32, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(), f32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_f32_arity1(module: ModuleId, export: &str, a: f32) -> Result<f32, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<f32, f32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_f32_arity2(module: ModuleId, export: &str, a: f32, b: f32) -> Result<f32, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(f32, f32), f32>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a, b)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_f64_arity0(module: ModuleId, export: &str) -> Result<f64, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(), f64>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_f64_arity1(module: ModuleId, export: &str, a: f64) -> Result<f64, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<f64, f64>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_f64_arity2(module: ModuleId, export: &str, a: f64, b: f64) -> Result<f64, String> {
    let (instance, mut store) = instantiate(module)?;
    let f = instance
        .exports
        .get_typed_function::<(f64, f64), f64>(&store, export)
        .map_err(|e| format!("pg_wasm: wasmer export {export:?}: {e}"))?;
    let r = f
        .call(&mut store, a, b)
        .map_err(|e| format!("pg_wasm: wasmer call: {e}"))?;
    after_guest_call(module, &instance, &mut store);
    Ok(r)
}

pub fn call_bool_result_arity0(module: ModuleId, export: &str) -> Result<bool, String> {
    call_i32_arity0(module, export).map(|v| v != 0)
}

pub fn call_bool_result_arity1(module: ModuleId, export: &str, a: bool) -> Result<bool, String> {
    call_i32_arity1(module, export, if a { 1 } else { 0 }).map(|v| v != 0)
}

pub fn call_bool_result_arity2(
    module: ModuleId,
    export: &str,
    a: bool,
    b: bool,
) -> Result<bool, String> {
    call_i32_arity2(module, export, if a { 1 } else { 0 }, if b { 1 } else { 0 }).map(|v| v != 0)
}

pub struct WasmerBackend;

impl WasmRuntimeBackend for WasmerBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Wasmer
    }

    fn label(&self) -> &'static str {
        "wasmer"
    }
}

/// For dispatch tests; Wasmer has no global engine handle like Wasmtime.
#[cfg(any(test, feature = "pg_test"))]
#[must_use]
pub fn execution_backend() -> super::selection::ModuleExecutionBackend {
    super::selection::ModuleExecutionBackend::Wasmer
}
