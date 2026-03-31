//! Wasmtime backend: process singleton owns [`wasmtime::Engine`] and compiled [`Module`]s.
//!
//! Callers outside this module should use the free functions ([`compile_store_and_list_exports`],
//! [`remove_compiled_module`], [`call_i32_arity0`], …) so global locking stays an implementation
//! detail. [`WasmtimeBackend`] remains the type that implements [`WasmRuntimeBackend`] when you
//! use [`with_backend`] for tests or diagnostics.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use wasmtime::{Engine, ExternType, Instance, Module, Store, ValType};

use super::{RuntimeKind, WasmRuntimeBackend};
use crate::{
    mapping::{
        ExportSignature, PgWasmArgDesc, PgWasmReturnDesc, PgWasmTypeKind,
    },
    registry::ModuleId,
};

static INSTANCE: OnceLock<Mutex<WasmtimeBackend>> = OnceLock::new();

fn mutex() -> &'static Mutex<WasmtimeBackend> {
    INSTANCE.get_or_init(|| Mutex::new(WasmtimeBackend::empty()))
}

/// Compile `wasm`, store under `id`, and return exports we can expose as strict `int4` SQL functions.
pub fn compile_store_and_list_exports(
    id: ModuleId,
    wasm: &[u8],
) -> Result<Vec<(String, ExportSignature)>, String> {
    let mut g = mutex().lock().map_err(|e| e.to_string())?;
    match g.compile_store(id, wasm) {
        Ok(out) => Ok(out),
        Err(e) => {
            g.remove_stored(id);
            Err(e)
        }
    }
}

/// Remove a compiled module from the process table (no-op if missing or mutex poisoned).
pub fn remove_compiled_module(id: ModuleId) {
    if let Ok(mut g) = mutex().lock() {
        g.remove_stored(id);
    }
}

/// Invoke a `()->i32` export. Locks only long enough to clone [`Engine`] + [`Module`].
pub fn call_i32_arity0(module: ModuleId, export: &str) -> Result<i32, String> {
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
    let f = instance
        .get_typed_func::<(), i32>(&mut store, export)
        .map_err(|e| e.to_string())?;
    f.call(&mut store, ()).map_err(|e| e.to_string())
}

/// Invoke an `(i32,i32)->i32` export. Locks only long enough to clone [`Engine`] + [`Module`].
pub fn call_i32_arity2(module: ModuleId, export: &str, a: i32, b: i32) -> Result<i32, String> {
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
    let f = instance
        .get_typed_func::<(i32, i32), i32>(&mut store, export)
        .map_err(|e| e.to_string())?;
    f.call(&mut store, (a, b)).map_err(|e| e.to_string())
}

/// Run `f` with a reference to the process backend (holds the global mutex for the duration of `f`).
pub fn with_backend<R>(f: impl FnOnce(&WasmtimeBackend) -> R) -> R {
    let g = mutex().lock().expect("pg_wasm: wasmtime backend mutex poisoned");
    f(&g)
}

/// Owns the wasmtime [`Engine`] and all compiled modules for this backend process.
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
    ) -> Result<Vec<(String, ExportSignature)>, String> {
        let module = Module::from_binary(&self.engine, wasm).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for export in module.exports() {
            if let ExternType::Func(ft) = export.ty() {
                let params: Vec<ValType> = ft.params().collect();
                let results: Vec<ValType> = ft.results().collect();
                if let Some(sig) = map_export_sig(&params, &results) {
                    out.push((export.name().to_string(), sig));
                }
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

fn map_export_sig(params: &[ValType], results: &[ValType]) -> Option<ExportSignature> {
    if results.len() != 1 || !matches!(&results[0], ValType::I32) {
        return None;
    }
    let mut args = Vec::new();
    for p in params {
        if !matches!(p, &ValType::I32) {
            return None;
        }
        args.push(PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        });
    }
    if args.len() > 2 {
        return None;
    }
    Some(ExportSignature {
        args,
        ret: PgWasmReturnDesc {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        },
    })
}
