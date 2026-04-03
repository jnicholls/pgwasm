//! Wasmtime backend: process singleton owns [`Engine`] and compiled [`Module`] / [`component::Component`] artifacts.
//!
//! Unified [`HostPolicy`] is applied to WASI preview1 (core modules) via [`WasiCtxBuilder::build_p1`]
//! and to WASI preview2 (components) via [`WasiCtxBuilder::build`] + [`wasmtime_wasi::p2::add_to_linker_sync`].

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};

use wasmtime::{
    Config, Engine, ExternType, Instance, Linker, Memory, Module, Store, StoreLimits,
    StoreLimitsBuilder, Val, ValType,
    component::{self, Component},
};
use wasmtime_wasi::{
    DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView, p1::WasiP1Ctx,
};

use super::{RuntimeKind, WasmRuntimeBackend};
use crate::{
    abi::WasmAbiKind,
    config::HostPolicy,
    guc,
    mapping::{
        ExportHintMap, ExportSignature, ExportTypeHint, PgWasmArgDesc, PgWasmReturnDesc,
        PgWasmTypeKind, signature_from_hint,
    },
    registry::{self, ModuleId},
};

static INSTANCE: OnceLock<Mutex<WasmtimeBackend>> = OnceLock::new();

/// Guest linear memory offset where the host writes the input slice (core wasm only; see module-level docs).
pub const MEM_IO_INPUT_BASE: u32 = 1024;

/// Upper bound on returned byte length from a single buffer-style wasm call (16 MiB).
const MEM_IO_MAX_OUT: u32 = 16 * 1024 * 1024;

/// Per-[`Store`] state when a core module is linked with WASI preview1.
pub struct PgWasmStoreState {
    pub wasi: WasiP1Ctx,
    pub limits: StoreLimits,
}

/// Per-[`Store`] state for component model + WASI preview2 (`WasiView`).
pub struct PgWasmP2StoreState {
    pub ctx: WasiCtx,
    pub table: wasmtime::component::ResourceTable,
    pub limits: StoreLimits,
}

impl PgWasmP2StoreState {
    fn new(policy: &HostPolicy, limits: StoreLimits) -> Result<Self, String> {
        let mut b = WasiCtxBuilder::new();
        apply_wasi_builder_policy(&mut b, policy)?;
        Ok(Self {
            ctx: b.build(),
            table: wasmtime::component::ResourceTable::new(),
            limits,
        })
    }
}

impl WasiView for PgWasmP2StoreState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

#[derive(Clone)]
enum CompiledArtifact {
    Component(Arc<Component>),
    Core(Arc<Module>),
}

enum InstanceBundle {
    ComponentPlain {
        instance: component::Instance,
        store: Store<StoreLimits>,
    },
    ComponentWasi {
        instance: component::Instance,
        store: Store<PgWasmP2StoreState>,
    },
    CorePlain {
        instance: Instance,
        store: Store<StoreLimits>,
    },
    CoreWasi {
        instance: Instance,
        store: Store<PgWasmStoreState>,
    },
}

fn apply_wasi_builder_policy(
    builder: &mut WasiCtxBuilder,
    policy: &HostPolicy,
) -> Result<(), String> {
    builder.allow_blocking_current_thread(true);
    if policy.allow_env {
        builder.inherit_env();
    }
    if policy.allow_network {
        builder.inherit_network();
    } else {
        builder.allow_tcp(false);
        builder.allow_udp(false);
        builder.allow_ip_name_lookup(false);
    }
    if policy.allow_fs_read || policy.allow_fs_write {
        if let Some(cs) = guc::module_path_cstr() {
            let base = cs.to_str().map_err(|_| {
                "pg_wasm.module_path must be valid UTF-8 for WASI preopen".to_string()
            })?;
            let mut dir_perms = DirPerms::empty();
            if policy.allow_fs_read {
                dir_perms |= DirPerms::READ;
            }
            if policy.allow_fs_write {
                dir_perms |= DirPerms::MUTATE;
            }
            let mut file_perms = FilePerms::empty();
            if policy.allow_fs_read {
                file_perms |= FilePerms::READ;
            }
            if policy.allow_fs_write {
                file_perms |= FilePerms::WRITE;
            }
            builder
                .preopened_dir(base, "/", dir_perms, file_perms)
                .map_err(|e| format!("pg_wasm: WASI preopen (pg_wasm.module_path): {e}"))?;
        }
    }
    Ok(())
}

fn build_wasi_p1_ctx(policy: &HostPolicy) -> Result<WasiP1Ctx, String> {
    let mut b = WasiCtxBuilder::new();
    apply_wasi_builder_policy(&mut b, policy)?;
    Ok(b.build_p1())
}

fn core_module_imports_wasi(module: &Module) -> bool {
    module
        .imports()
        .any(|imp| imp.module() == "wasi_snapshot_preview1" || imp.module() == "wasi_unstable")
}

fn component_imports_wasi(comp: &Component) -> bool {
    let engine = comp.engine();
    comp.component_type()
        .imports(engine)
        .any(|(name, _)| name.starts_with("wasi:"))
}

fn hint_kind_matches_component_type(kind: &PgWasmTypeKind, t: &component::types::Type) -> bool {
    use component::types::Type;
    match (kind, t) {
        (PgWasmTypeKind::Bool, Type::Bool) => true,
        (PgWasmTypeKind::I32, Type::S32 | Type::U32) => true,
        (PgWasmTypeKind::I64, Type::S64 | Type::U64) => true,
        (PgWasmTypeKind::F32, Type::Float32) => true,
        (PgWasmTypeKind::F64, Type::Float64) => true,
        _ => false,
    }
}

fn hint_matches_component_func(
    hint: &ExportTypeHint,
    f: &component::types::ComponentFunc,
) -> Result<(), String> {
    if f.async_() {
        return Err("pg_wasm: component export is async; not supported".into());
    }
    let params: Vec<_> = f.params().collect();
    let results: Vec<_> = f.results().collect();
    if params.len() != hint.args.len() {
        return Err(format!(
            "pg_wasm: component export has {} parameters but export hint lists {}",
            params.len(),
            hint.args.len()
        ));
    }
    for ((_, hk), (_, pt)) in hint.args.iter().zip(params.iter()) {
        if !hint_kind_matches_component_type(hk, pt) {
            return Err(format!(
                "pg_wasm: component parameter type does not match export hint (hint={hk:?}, wit={pt:?})"
            ));
        }
    }
    if results.len() != 1 {
        return Err(format!(
            "pg_wasm: component export must have a single result for this mapping (has {})",
            results.len()
        ));
    }
    if !hint_kind_matches_component_type(&hint.ret.1, &results[0]) {
        return Err(format!(
            "pg_wasm: component return type {:?} does not match export hint {:?}",
            results[0], hint.ret.1
        ));
    }
    Ok(())
}

fn map_component_func_to_sig(f: &component::types::ComponentFunc) -> Option<ExportSignature> {
    if f.async_() {
        return None;
    }
    let params: Vec<_> = f.params().map(|(_, t)| t).collect();
    let results: Vec<_> = f.results().collect();
    if results.len() != 1 {
        return None;
    }
    let args: Vec<PgWasmArgDesc> = params
        .iter()
        .map(component_type_to_arg)
        .collect::<Option<_>>()?;
    let ret = component_type_to_return(&results[0])?;
    Some(ExportSignature {
        args,
        ret,
        wit_interface: None,
    })
}

fn component_type_to_arg(t: &component::types::Type) -> Option<PgWasmArgDesc> {
    use component::types::Type;
    Some(match t {
        Type::Bool => PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::BOOLOID,
            kind: PgWasmTypeKind::Bool,
        },
        Type::S32 | Type::U32 => PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        },
        Type::S64 | Type::U64 => PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::INT8OID,
            kind: PgWasmTypeKind::I64,
        },
        Type::Float32 => PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::FLOAT4OID,
            kind: PgWasmTypeKind::F32,
        },
        Type::Float64 => PgWasmArgDesc {
            pg_oid: pgrx::pg_sys::FLOAT8OID,
            kind: PgWasmTypeKind::F64,
        },
        _ => return None,
    })
}

fn component_type_to_return(t: &component::types::Type) -> Option<PgWasmReturnDesc> {
    use component::types::Type;
    Some(match t {
        Type::Bool => PgWasmReturnDesc {
            pg_oid: pgrx::pg_sys::BOOLOID,
            kind: PgWasmTypeKind::Bool,
        },
        Type::S32 | Type::U32 => PgWasmReturnDesc {
            pg_oid: pgrx::pg_sys::INT4OID,
            kind: PgWasmTypeKind::I32,
        },
        Type::S64 | Type::U64 => PgWasmReturnDesc {
            pg_oid: pgrx::pg_sys::INT8OID,
            kind: PgWasmTypeKind::I64,
        },
        Type::Float32 => PgWasmReturnDesc {
            pg_oid: pgrx::pg_sys::FLOAT4OID,
            kind: PgWasmTypeKind::F32,
        },
        Type::Float64 => PgWasmReturnDesc {
            pg_oid: pgrx::pg_sys::FLOAT8OID,
            kind: PgWasmTypeKind::F64,
        },
        _ => return None,
    })
}

const WASM_PAGE_BYTES: u64 = 65536;

/// Short error text for PostgreSQL `ereport`: `Error::to_string()` includes a wasm backtrace and is
/// very large, which has proven brittle with pgrx's `pg_guard_ffi_boundary` + `error!` path.
fn wasmtime_to_host_string(err: wasmtime::Error) -> String {
    err.downcast_ref::<wasmtime::Trap>()
        .map(|t| format!("{t:?}"))
        .unwrap_or_else(|| format!("{err:#}"))
}

fn store_limits_for_module(module: ModuleId) -> StoreLimits {
    let pages = guc::effective_max_memory_pages(module);
    if pages == 0 {
        StoreLimitsBuilder::new().build()
    } else {
        let bytes_u64 = u64::from(pages).saturating_mul(WASM_PAGE_BYTES);
        let cap = usize::try_from(bytes_u64.min(u64::try_from(usize::MAX).unwrap_or(u64::MAX)))
            .unwrap_or(usize::MAX);
        StoreLimitsBuilder::new().memory_size(cap).build()
    }
}

fn prime_store_fuel<S>(store: &mut Store<S>, module: ModuleId) -> Result<(), String> {
    let fuel = guc::effective_fuel_per_invocation(module);
    store
        .set_fuel(fuel)
        .map_err(|e| format!("pg_wasm: set_fuel: {e}"))
}

fn instantiate_bundle(module: ModuleId) -> Result<InstanceBundle, String> {
    let needs_wasi = registry::module_needs_wasi(module).ok_or_else(|| {
        format!(
            "pg_wasm: no metadata for wasm module id {} (not loaded in this backend)",
            module.0
        )
    })?;
    let overrides = registry::module_policy_overrides(module).ok_or_else(|| {
        format!(
            "pg_wasm: no policy metadata for wasm module id {}",
            module.0
        )
    })?;
    let abi = registry::module_abi(module)
        .ok_or_else(|| format!("pg_wasm: no ABI metadata for wasm module id {}", module.0))?;
    let policy = guc::effective_host_policy(&overrides);
    if needs_wasi && !policy.allow_wasi {
        return Err(
            "pg_wasm: module imports WASI but effective host policy denies WASI (see pg_wasm.allow_wasi and per-module allow_wasi)"
                .into(),
        );
    }

    let (engine, artifact) = {
        let g = mutex().lock().map_err(|e| e.to_string())?;
        let art = g
            .artifacts
            .get(&module)
            .cloned()
            .ok_or_else(|| format!("pg_wasm: no wasm module for id {}", module.0))?;
        (g.engine.clone(), art)
    };

    match (&artifact, abi) {
        (CompiledArtifact::Core(_), WasmAbiKind::ComponentModel)
        | (CompiledArtifact::Component(_), WasmAbiKind::CoreWasm) => {
            return Err("pg_wasm: internal error: ABI does not match compiled artifact".into());
        }
        _ => {}
    }

    let lim = store_limits_for_module(module);

    match artifact {
        CompiledArtifact::Core(arc) => {
            if !needs_wasi {
                let mut store = Store::new(&engine, lim);
                store.limiter(|s| s);
                prime_store_fuel(&mut store, module)?;
                let instance =
                    Instance::new(&mut store, &arc, &[]).map_err(wasmtime_to_host_string)?;
                return Ok(InstanceBundle::CorePlain { store, instance });
            }
            let wasi = build_wasi_p1_ctx(&policy)?;
            let mut store = Store::new(&engine, PgWasmStoreState { wasi, limits: lim });
            store.limiter(|s| &mut s.limits);
            prime_store_fuel(&mut store, module)?;
            let mut linker = Linker::new(&engine);
            wasmtime_wasi::p1::add_to_linker_sync(&mut linker, |s: &mut PgWasmStoreState| {
                &mut s.wasi
            })
            .map_err(wasmtime_to_host_string)?;
            let instance = linker
                .instantiate(&mut store, &arc)
                .map_err(wasmtime_to_host_string)?;
            Ok(InstanceBundle::CoreWasi { store, instance })
        }
        CompiledArtifact::Component(arc) => {
            if !needs_wasi {
                let linker = component::Linker::new(&engine);
                let mut store = Store::new(&engine, lim);
                store.limiter(|s| s);
                prime_store_fuel(&mut store, module)?;
                let instance = linker
                    .instantiate(&mut store, &arc)
                    .map_err(wasmtime_to_host_string)?;
                return Ok(InstanceBundle::ComponentPlain { store, instance });
            }
            let mut linker = component::Linker::new(&engine);
            wasmtime_wasi::p2::add_to_linker_sync(&mut linker).map_err(wasmtime_to_host_string)?;
            let state = PgWasmP2StoreState::new(&policy, lim)?;
            let mut store = Store::new(&engine, state);
            store.limiter(|s| &mut s.limits);
            prime_store_fuel(&mut store, module)?;
            let instance = linker
                .instantiate(&mut store, &arc)
                .map_err(wasmtime_to_host_string)?;
            Ok(InstanceBundle::ComponentWasi { store, instance })
        }
    }
}

fn mutex() -> &'static Mutex<WasmtimeBackend> {
    INSTANCE.get_or_init(|| Mutex::new(WasmtimeBackend::empty()))
}

/// Compile `wasm`, store under `id`, and return exports to register as SQL functions plus whether WASI
/// imports are present (preview1 for core, `wasi:*` for components).
pub fn compile_store_and_list_exports(
    id: ModuleId,
    wasm: &[u8],
    export_hints: &ExportHintMap,
    abi: WasmAbiKind,
) -> Result<(Vec<(String, ExportSignature)>, bool), String> {
    let mut g = mutex().lock().map_err(|e| e.to_string())?;
    match g.compile_store(id, wasm, export_hints, abi) {
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

/// Invoke a lifecycle export if present: core wasm supports `() -> ()` or `(i32, i32) -> ()` with
/// `config` written to linear memory at [`MEM_IO_INPUT_BASE`]; components support `() -> ()` only.
/// Missing export is ignored (optional hook name).
pub fn call_lifecycle_hook(
    module: ModuleId,
    export_name: &str,
    config: &[u8],
) -> Result<(), String> {
    let bundle = instantiate_bundle(module)?;
    match bundle {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => call_lifecycle_hook_core(&mut store, &instance, module, export_name, config),
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => call_lifecycle_hook_core(&mut store, &instance, module, export_name, config),
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => call_lifecycle_hook_component(&mut store, &instance, export_name, config),
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => call_lifecycle_hook_component(&mut store, &instance, export_name, config),
    }
}

fn call_lifecycle_hook_core<S>(
    store: &mut Store<S>,
    instance: &Instance,
    module: ModuleId,
    export_name: &str,
    config: &[u8],
) -> Result<(), String> {
    let Some(func) = instance.get_func(&mut *store, export_name) else {
        return Ok(());
    };
    let ty = func.ty(&mut *store);
    let params: Vec<ValType> = ty.params().collect();
    let results: Vec<ValType> = ty.results().collect();

    match (params.as_slice(), results.as_slice()) {
        ([], []) => {
            func.call(&mut *store, &[], &mut [])
                .map_err(wasmtime_to_host_string)?;
        }
        ([ValType::I32, ValType::I32], []) => {
            let memory = instance.get_memory(&mut *store, "memory").ok_or_else(|| {
                "pg_wasm: lifecycle hook (ptr,len) requires exported `memory`".to_string()
            })?;
            let ptr = MEM_IO_INPUT_BASE as i32;
            let len = i32::try_from(config.len())
                .map_err(|_| "pg_wasm: lifecycle config exceeds i32::MAX bytes".to_string())?;
            if !config.is_empty() {
                let base = MEM_IO_INPUT_BASE as usize;
                let need = base + config.len();
                grow_memory_to(store, &memory, need)?;
                memory
                    .write(&mut *store, base, config)
                    .map_err(|e| e.to_string())?;
            }
            func.call(&mut *store, &[Val::I32(ptr), Val::I32(len)], &mut [])
                .map_err(wasmtime_to_host_string)?;
        }
        _ => {
            return Err(format!(
                "pg_wasm: lifecycle export {export_name:?} must be () -> () or (i32, i32) -> () for core wasm"
            ));
        }
    }
    after_guest_call_core(module, store, instance);
    Ok(())
}

fn call_lifecycle_hook_component<T>(
    store: &mut Store<T>,
    instance: &component::Instance,
    export_name: &str,
    config: &[u8],
) -> Result<(), String> {
    let Some(func) = instance.get_func(&mut *store, export_name) else {
        return Ok(());
    };
    let ty = func.ty(&mut *store);
    if ty.params().len() != 0 || ty.results().len() != 0 {
        return Err(format!(
            "pg_wasm: lifecycle export {export_name:?} must be () -> () for WebAssembly components"
        ));
    }
    let _ = config;
    func.call(&mut *store, &[], &mut [])
        .map_err(wasmtime_to_host_string)
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

pub fn call_mem_in_out(module: ModuleId, export: &str, input: &[u8]) -> Result<Vec<u8>, String> {
    let bundle = instantiate_bundle(module)?;
    match bundle {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => call_mem_in_out_impl(module, &mut store, &instance, export, input),
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => call_mem_in_out_impl(module, &mut store, &instance, export, input),
        InstanceBundle::ComponentPlain { .. } | InstanceBundle::ComponentWasi { .. } => Err(
            "pg_wasm: bytea/text/jsonb buffer calling convention is only supported for core WebAssembly modules"
                .into(),
        ),
    }
}

fn call_mem_in_out_impl<S>(
    module: ModuleId,
    store: &mut Store<S>,
    instance: &Instance,
    export: &str,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    let memory = instance
        .get_memory(&mut *store, "memory")
        .ok_or_else(|| "pg_wasm: wasm module has no exported `memory`".to_string())?;
    let f = instance
        .get_typed_func::<(i32, i32), i32>(&mut *store, export)
        .map_err(wasmtime_to_host_string)?;

    let base = MEM_IO_INPUT_BASE as usize;
    let out_base = base + ((input.len() + 7) & !7);
    let need = out_base.saturating_add(MEM_IO_MAX_OUT as usize);
    grow_memory_to(store, &memory, need)?;

    memory
        .write(&mut *store, base, input)
        .map_err(|e| e.to_string())?;

    let out_len = f
        .call(&mut *store, (MEM_IO_INPUT_BASE as i32, input.len() as i32))
        .map_err(wasmtime_to_host_string)?;
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
    let end = out_base + out_len as usize;
    grow_memory_to(store, &memory, end)?;

    let mut out = vec![0u8; out_len as usize];
    memory
        .read(&mut *store, out_base, &mut out)
        .map_err(|e| e.to_string())?;
    after_guest_call_core(module, store, instance);
    Ok(out)
}

fn grow_memory_to<S>(store: &mut Store<S>, memory: &Memory, need: usize) -> Result<(), String> {
    let page = 65536usize;
    let mut current = memory.data_size(&mut *store);
    while current < need {
        memory.grow(&mut *store, 1).map_err(|e| {
            format!(
                "pg_wasm: memory.grow failed: {}",
                wasmtime_to_host_string(e)
            )
        })?;
        current += page;
    }
    Ok(())
}

fn after_guest_call_core<S>(module: ModuleId, store: &mut Store<S>, instance: &Instance) {
    if let Some(mem) = instance.get_memory(&mut *store, "memory") {
        let sz = mem.data_size(&mut *store) as u64;
        crate::metrics::record_memory_sample(module, sz);
    }
}

fn map_wasmtime_err<T>(r: wasmtime::Result<T>) -> Result<T, String> {
    r.map_err(wasmtime_to_host_string)
}

pub fn call_i32_arity0(module: ModuleId, export: &str) -> Result<i32, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), i32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), i32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (i32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (i32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
    }
}

pub fn call_i32_arity1(module: ModuleId, export: &str, a: i32) -> Result<i32, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(i32,), i32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(i32,), i32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(i32,), (i32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(i32,), (i32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
    }
}

pub fn call_i32_arity2(module: ModuleId, export: &str, a: i32, b: i32) -> Result<i32, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(i32, i32), i32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a, b)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(i32, i32), i32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a, b)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(
                instance.get_typed_func::<(i32, i32), (i32,)>(&mut store, export),
            )?;
            Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(
                instance.get_typed_func::<(i32, i32), (i32,)>(&mut store, export),
            )?;
            Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
        }
    }
}

pub fn call_i64_arity0(module: ModuleId, export: &str) -> Result<i64, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), i64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), i64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (i64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (i64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
    }
}

pub fn call_i64_arity1(module: ModuleId, export: &str, a: i64) -> Result<i64, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(i64,), i64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(i64,), i64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(i64,), (i64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(i64,), (i64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
    }
}

pub fn call_f32_arity0(module: ModuleId, export: &str) -> Result<f32, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), f32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), f32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (f32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (f32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
    }
}

pub fn call_f32_arity1(module: ModuleId, export: &str, a: f32) -> Result<f32, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(f32,), f32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(f32,), f32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f32,), (f32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f32,), (f32,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
    }
}

pub fn call_f32_arity2(module: ModuleId, export: &str, a: f32, b: f32) -> Result<f32, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f32, f32), f32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a, b)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f32, f32), f32>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a, b)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(
                instance.get_typed_func::<(f32, f32), (f32,)>(&mut store, export),
            )?;
            Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(
                instance.get_typed_func::<(f32, f32), (f32,)>(&mut store, export),
            )?;
            Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
        }
    }
}

pub fn call_f64_arity0(module: ModuleId, export: &str) -> Result<f64, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), f64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), f64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, ()))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (f64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(), (f64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
        }
    }
}

pub fn call_f64_arity1(module: ModuleId, export: &str, a: f64) -> Result<f64, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(f64,), f64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(instance.get_typed_func::<(f64,), f64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a,)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f64,), (f64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f64,), (f64,)>(&mut store, export))?;
            Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
        }
    }
}

pub fn call_f64_arity2(module: ModuleId, export: &str, a: f64, b: f64) -> Result<f64, String> {
    match instantiate_bundle(module)? {
        InstanceBundle::CorePlain {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f64, f64), f64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a, b)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::CoreWasi {
            mut store,
            instance,
        } => {
            let f =
                map_wasmtime_err(instance.get_typed_func::<(f64, f64), f64>(&mut store, export))?;
            let r = map_wasmtime_err(f.call(&mut store, (a, b)))?;
            after_guest_call_core(module, &mut store, &instance);
            Ok(r)
        }
        InstanceBundle::ComponentPlain {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(
                instance.get_typed_func::<(f64, f64), (f64,)>(&mut store, export),
            )?;
            Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
        }
        InstanceBundle::ComponentWasi {
            mut store,
            instance,
        } => {
            let f = map_wasmtime_err(
                instance.get_typed_func::<(f64, f64), (f64,)>(&mut store, export),
            )?;
            Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
        }
    }
}

pub fn call_bool_result_arity0(module: ModuleId, export: &str) -> Result<bool, String> {
    match registry::module_abi(module) {
        Some(WasmAbiKind::ComponentModel) => match instantiate_bundle(module)? {
            InstanceBundle::ComponentPlain {
                mut store,
                instance,
            } => {
                let f =
                    map_wasmtime_err(instance.get_typed_func::<(), (bool,)>(&mut store, export))?;
                Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
            }
            InstanceBundle::ComponentWasi {
                mut store,
                instance,
            } => {
                let f =
                    map_wasmtime_err(instance.get_typed_func::<(), (bool,)>(&mut store, export))?;
                Ok(map_wasmtime_err(f.call(&mut store, ()))?.0)
            }
            _ => Err(
                "pg_wasm: internal error: expected component instance state for component ABI"
                    .into(),
            ),
        },
        Some(WasmAbiKind::CoreWasm) | None => {
            let v = call_i32_arity0(module, export)?;
            Ok(v != 0)
        }
        Some(WasmAbiKind::Extism) => Err("pg_wasm: Extism bool calls are not supported".into()),
    }
}

pub fn call_bool_result_arity1(module: ModuleId, export: &str, a: bool) -> Result<bool, String> {
    match registry::module_abi(module) {
        Some(WasmAbiKind::ComponentModel) => match instantiate_bundle(module)? {
            InstanceBundle::ComponentPlain {
                mut store,
                instance,
            } => {
                let f = map_wasmtime_err(
                    instance.get_typed_func::<(bool,), (bool,)>(&mut store, export),
                )?;
                Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
            }
            InstanceBundle::ComponentWasi {
                mut store,
                instance,
            } => {
                let f = map_wasmtime_err(
                    instance.get_typed_func::<(bool,), (bool,)>(&mut store, export),
                )?;
                Ok(map_wasmtime_err(f.call(&mut store, (a,)))?.0)
            }
            _ => Err(
                "pg_wasm: internal error: expected component instance state for component ABI"
                    .into(),
            ),
        },
        Some(WasmAbiKind::CoreWasm) | None => {
            let v = call_i32_arity1(module, export, if a { 1 } else { 0 })?;
            Ok(v != 0)
        }
        Some(WasmAbiKind::Extism) => Err("pg_wasm: Extism bool calls are not supported".into()),
    }
}

pub fn call_bool_result_arity2(
    module: ModuleId,
    export: &str,
    a: bool,
    b: bool,
) -> Result<bool, String> {
    match registry::module_abi(module) {
        Some(WasmAbiKind::ComponentModel) => match instantiate_bundle(module)? {
            InstanceBundle::ComponentPlain {
                mut store,
                instance,
            } => {
                let f = map_wasmtime_err(
                    instance.get_typed_func::<(bool, bool), (bool,)>(&mut store, export),
                )?;
                Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
            }
            InstanceBundle::ComponentWasi {
                mut store,
                instance,
            } => {
                let f = map_wasmtime_err(
                    instance.get_typed_func::<(bool, bool), (bool,)>(&mut store, export),
                )?;
                Ok(map_wasmtime_err(f.call(&mut store, (a, b)))?.0)
            }
            _ => Err(
                "pg_wasm: internal error: expected component instance state for component ABI"
                    .into(),
            ),
        },
        Some(WasmAbiKind::CoreWasm) | None => {
            let v = call_i32_arity2(module, export, if a { 1 } else { 0 }, if b { 1 } else { 0 })?;
            Ok(v != 0)
        }
        Some(WasmAbiKind::Extism) => Err("pg_wasm: Extism bool calls are not supported".into()),
    }
}

#[cfg(any(test, feature = "pg-test"))]
pub fn with_backend<R>(f: impl FnOnce(&WasmtimeBackend) -> R) -> R {
    let g = mutex()
        .lock()
        .expect("pg_wasm: wasmtime backend mutex poisoned");
    f(&g)
}

pub struct WasmtimeBackend {
    artifacts: HashMap<ModuleId, CompiledArtifact>,
    engine: Engine,
}

impl WasmtimeBackend {
    fn empty() -> Self {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.wasm_backtrace_max_frames(None);
        // Postgres uses sigsetjmp/siglongjmp for `elog(ERROR)` (see pgrx `pg_guard_ffi_boundary`).
        // Wasmtime's process-wide trap handlers can fight that model; disable signal-based traps
        // so Cranelift emits explicit checks instead (`Config::signals_based_traps`).
        config.signals_based_traps(false);
        unsafe {
            config.cranelift_flag_set("enable_heap_access_spectre_mitigation", "false");
            config.cranelift_flag_set("enable_table_access_spectre_mitigation", "false");
        }
        let engine = Engine::new(&config).unwrap_or_else(|e| {
            panic!("pg_wasm: wasmtime Engine::new failed: {e}");
        });
        Self {
            artifacts: HashMap::new(),
            engine,
        }
    }

    fn compile_store(
        &mut self,
        id: ModuleId,
        wasm: &[u8],
        export_hints: &ExportHintMap,
        abi: WasmAbiKind,
    ) -> Result<(Vec<(String, ExportSignature)>, bool), String> {
        match abi {
            WasmAbiKind::ComponentModel => {
                let comp = Component::new(&self.engine, wasm).map_err(wasmtime_to_host_string)?;
                let needs_wasi = component_imports_wasi(&comp);
                let out = self.list_component_exports(&comp, export_hints)?;
                self.artifacts
                    .insert(id, CompiledArtifact::Component(Arc::new(comp)));
                Ok((out, needs_wasi))
            }
            WasmAbiKind::CoreWasm => {
                let module = Module::new(&self.engine, wasm).map_err(wasmtime_to_host_string)?;
                let needs_wasi = core_module_imports_wasi(&module);
                let out = self.list_core_exports(&module, export_hints)?;
                self.artifacts
                    .insert(id, CompiledArtifact::Core(Arc::new(module)));
                Ok((out, needs_wasi))
            }
            WasmAbiKind::Extism => {
                Err("pg_wasm: Extism modules are not compiled by the wasmtime backend".into())
            }
        }
    }

    fn list_core_exports(
        &self,
        module: &Module,
        export_hints: &ExportHintMap,
    ) -> Result<Vec<(String, ExportSignature)>, String> {
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

    fn list_component_exports(
        &self,
        comp: &Component,
        export_hints: &ExportHintMap,
    ) -> Result<Vec<(String, ExportSignature)>, String> {
        let engine = &self.engine;
        let mut out = Vec::new();
        for (name, item) in comp.component_type().exports(engine) {
            let component::types::ComponentItem::ComponentFunc(f) = item else {
                continue;
            };
            if let Some(hint) = export_hints.get(name) {
                hint_matches_component_func(hint, &f)?;
                if uses_linear_memory(hint) {
                    return Err(format!(
                        "pg_wasm: export {name:?} uses buffer-style types; not supported for components"
                    ));
                }
                out.push((name.to_string(), signature_from_hint(hint)));
                continue;
            }
            if let Some(sig) = map_component_func_to_sig(&f) {
                out.push((name.to_string(), sig));
            }
        }

        Ok(out)
    }

    fn remove_stored(&mut self, id: ModuleId) {
        self.artifacts.remove(&id);
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
    hint.args
        .iter()
        .any(|(_, k)| matches!(k, PgWasmTypeKind::String | PgWasmTypeKind::Bytes))
        || matches!(hint.ret.1, PgWasmTypeKind::String | PgWasmTypeKind::Bytes)
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
