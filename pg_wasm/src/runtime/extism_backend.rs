//! Extism backend: loads wasm through Extism’s `CompiledPlugin` / `Plugin` API (Wasmtime inside Extism).

use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
};

use extism::{CompiledPlugin, Manifest, Plugin, PluginBuilder, Wasm, WasmMetadata};

use super::{RuntimeKind, WasmRuntimeBackend};
use crate::{
    abi::WasmAbiKind,
    guc,
    mapping::{ExportHintMap, ExportSignature},
    registry::ModuleId,
};

static INSTANCE: OnceLock<Mutex<ExtismBackendState>> = OnceLock::new();

struct ExtismBackendState {
    artifacts: HashMap<ModuleId, Arc<CompiledPlugin>>,
}

fn mutex() -> &'static Mutex<ExtismBackendState> {
    INSTANCE.get_or_init(|| Mutex::new(ExtismBackendState {
        artifacts: HashMap::new(),
    }))
}

fn get_compiled(id: ModuleId) -> Result<Arc<CompiledPlugin>, String> {
    let g = mutex().lock().map_err(|e| e.to_string())?;
    g.artifacts
        .get(&id)
        .cloned()
        .ok_or_else(|| format!("pg_wasm: no extism compiled module for id {}", id.0))
}

fn with_plugin<R>(
    id: ModuleId,
    f: impl FnOnce(&mut Plugin) -> Result<R, String>,
) -> Result<R, String> {
    let compiled = get_compiled(id)?;
    let mut plugin =
        Plugin::new_from_compiled(&compiled).map_err(|e| format!("pg_wasm: extism Plugin: {e}"))?;
    f(&mut plugin)
}

/// Build an Extism manifest aligned with effective GUC + per-module policy and resource limits
/// (requires `registry::record_module_resource_limits` and `record_module_policy_overrides` before compile).
fn extism_manifest_for_load(
    id: ModuleId,
    wasm: &[u8],
    needs_wasi: bool,
) -> Result<Manifest, String> {
    let wasm_entry = Wasm::Data {
        data: wasm.to_vec(),
        meta: WasmMetadata {
            name: Some("main".to_string()),
            hash: None,
        },
    };
    let mut manifest = Manifest::new(vec![wasm_entry]);
    let pages = guc::effective_max_memory_pages(id);
    if pages > 0 {
        manifest = manifest.with_memory_max(pages);
    }
    let overrides = crate::registry::module_policy_overrides(id).unwrap_or_default();
    let policy = guc::effective_host_policy(&overrides);
    if !policy.allow_network {
        manifest = manifest.disallow_all_hosts();
    }
    if needs_wasi && (policy.allow_fs_read || policy.allow_fs_write) {
        if let Some(cs) = guc::module_path_cstr() {
            let base_str = cs.to_str().map_err(|_| {
                "pg_wasm.module_path must be valid UTF-8 for Extism WASI allowed_paths".to_string()
            })?;
            let mut paths = BTreeMap::new();
            if policy.allow_fs_write {
                paths.insert(base_str.to_string(), PathBuf::from("/"));
            } else if policy.allow_fs_read {
                paths.insert(format!("ro:{base_str}"), PathBuf::from("/"));
            }
            if !paths.is_empty() {
                manifest.allowed_paths = Some(paths);
            }
        }
    }
    Ok(manifest)
}

pub fn compile_store_and_list_exports(
    id: ModuleId,
    wasm: &[u8],
    export_hints: &ExportHintMap,
    abi: WasmAbiKind,
) -> Result<(Vec<(String, ExportSignature)>, bool), String> {
    match abi {
        WasmAbiKind::ComponentModel => {
            return Err(
                "pg_wasm: Extism backend does not load WebAssembly components".into(),
            );
        }
        WasmAbiKind::CoreWasm | WasmAbiKind::Extism => {}
    }
    let (exports, needs_wasi) =
        super::wasm_bytes_exports::list_core_exports_from_wasm_bytes(wasm, export_hints)?;
    let manifest = extism_manifest_for_load(id, wasm, needs_wasi)?;
    let fuel = guc::effective_fuel_per_invocation(id);
    let mut builder = PluginBuilder::new(manifest).with_wasi(needs_wasi);
    if fuel != u64::MAX {
        builder = builder.with_fuel_limit(fuel);
    }
    let compiled = CompiledPlugin::new(builder).map_err(|e| format!("pg_wasm: extism compile: {e}"))?;
    let mut g = mutex().lock().map_err(|e| e.to_string())?;
    g.artifacts.insert(id, Arc::new(compiled));
    Ok((exports, needs_wasi))
}

pub fn remove_compiled_module(id: ModuleId) {
    if let Ok(mut g) = mutex().lock() {
        g.artifacts.remove(&id);
    }
}

pub fn call_mem_in_out(module: ModuleId, export: &str, input: &[u8]) -> Result<Vec<u8>, String> {
    with_plugin(module, |p| {
        p.call::<Vec<u8>, Vec<u8>>(export, input.to_vec())
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_lifecycle_hook(
    module: ModuleId,
    export_name: &str,
    config: &[u8],
) -> Result<(), String> {
    with_plugin(module, |p| {
        if config.is_empty() {
            match p.call::<(), ()>(export_name, ()) {
                Ok(()) => Ok(()),
                Err(e) => {
                    let m = e.to_string();
                    if m.contains("unknown") || m.contains("not found") || m.contains("export") {
                        Ok(())
                    } else {
                        Err(m)
                    }
                }
            }
        } else {
            match p.call::<Vec<u8>, ()>(export_name, config.to_vec()) {
                Ok(()) => Ok(()),
                Err(e) => {
                    let m = e.to_string();
                    if m.contains("unknown") || m.contains("not found") || m.contains("export") {
                        Ok(())
                    } else {
                        Err(m)
                    }
                }
            }
        }
    })?;
    Ok(())
}

pub fn call_i32_arity0(module: ModuleId, export: &str) -> Result<i32, String> {
    with_plugin(module, |p| {
        p.call::<(), i32>(export, ())
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_i32_arity1(module: ModuleId, export: &str, a: i32) -> Result<i32, String> {
    with_plugin(module, |p| {
        p.call::<i32, i32>(export, a)
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_i32_arity2(module: ModuleId, export: &str, _a: i32, _b: i32) -> Result<i32, String> {
    let _ = (module, export);
    Err(
        "pg_wasm: Extism runtime does not support 2×i32 scalar wasm calls; use the wasmtime runtime"
            .into(),
    )
}

pub fn call_i64_arity0(module: ModuleId, export: &str) -> Result<i64, String> {
    with_plugin(module, |p| {
        p.call::<(), i64>(export, ())
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_i64_arity1(module: ModuleId, export: &str, a: i64) -> Result<i64, String> {
    with_plugin(module, |p| {
        p.call::<i64, i64>(export, a)
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_f32_arity0(module: ModuleId, export: &str) -> Result<f32, String> {
    with_plugin(module, |p| {
        p.call::<(), f32>(export, ())
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_f32_arity1(module: ModuleId, export: &str, a: f32) -> Result<f32, String> {
    with_plugin(module, |p| {
        p.call::<f32, f32>(export, a)
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_f32_arity2(module: ModuleId, export: &str, _a: f32, _b: f32) -> Result<f32, String> {
    let _ = (module, export);
    Err(
        "pg_wasm: Extism runtime does not support 2×f32 scalar wasm calls; use the wasmtime runtime"
            .into(),
    )
}

pub fn call_f64_arity0(module: ModuleId, export: &str) -> Result<f64, String> {
    with_plugin(module, |p| {
        p.call::<(), f64>(export, ())
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_f64_arity1(module: ModuleId, export: &str, a: f64) -> Result<f64, String> {
    with_plugin(module, |p| {
        p.call::<f64, f64>(export, a)
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_f64_arity2(module: ModuleId, export: &str, _a: f64, _b: f64) -> Result<f64, String> {
    let _ = (module, export);
    Err(
        "pg_wasm: Extism runtime does not support 2×f64 scalar wasm calls; use the wasmtime runtime"
            .into(),
    )
}

pub fn call_bool_result_arity0(module: ModuleId, export: &str) -> Result<bool, String> {
    with_plugin(module, |p| {
        p.call::<(), bool>(export, ())
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_bool_result_arity1(module: ModuleId, export: &str, a: bool) -> Result<bool, String> {
    with_plugin(module, |p| {
        p.call::<bool, bool>(export, a)
            .map_err(|e| format!("pg_wasm: extism call {export:?}: {e}"))
    })
}

pub fn call_bool_result_arity2(
    module: ModuleId,
    export: &str,
    _a: bool,
    _b: bool,
) -> Result<bool, String> {
    let _ = (module, export);
    Err(
        "pg_wasm: Extism runtime does not support 2×bool scalar wasm calls; use the wasmtime runtime"
            .into(),
    )
}

pub struct ExtismBackend;

impl WasmRuntimeBackend for ExtismBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Extism
    }

    fn label(&self) -> &'static str {
        "extism"
    }
}
