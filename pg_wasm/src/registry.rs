//! In-process registry for loaded modules and `fn_oid` → export metadata.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicI64, Ordering},
    },
};

use pgrx::pg_sys::Oid;

use crate::abi::WasmAbiKind;
use crate::config::{ModuleResourceLimits, PolicyOverrides};
use crate::mapping::ExportSignature;
use crate::metrics::ExportStats;
use crate::runtime::ModuleExecutionBackend;

static NEXT_MODULE_ID: AtomicI64 = AtomicI64::new(1);

/// Stable handle for a loaded module (bigint / sequence in SQL).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ModuleId(pub i64);

/// Metadata for one dynamically registered UDF.
#[derive(Clone, Debug)]
pub struct RegisteredFunction {
    pub module_id: ModuleId,
    pub export_name: String,
    pub signature: ExportSignature,
    pub metrics: Arc<ExportStats>,
}

/// SQL/catalog row for a loaded module (UDF name prefix, runtime hint).
#[derive(Clone, Debug)]
pub struct ModuleCatalogEntry {
    pub name_prefix: String,
    pub runtime: String,
}

static MODULE_CATALOG: OnceLock<Mutex<HashMap<ModuleId, ModuleCatalogEntry>>> = OnceLock::new();

fn module_catalog_map() -> &'static Mutex<HashMap<ModuleId, ModuleCatalogEntry>> {
    MODULE_CATALOG.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn record_module_catalog(module: ModuleId, entry: ModuleCatalogEntry) {
    let mut g = module_catalog_map()
        .lock()
        .expect("module catalog map poisoned");
    g.insert(module, entry);
}

#[must_use]
pub fn take_module_catalog(module: ModuleId) -> Option<ModuleCatalogEntry> {
    let mut g = module_catalog_map()
        .lock()
        .expect("module catalog map poisoned");
    g.remove(&module)
}

pub fn module_catalog(module: ModuleId) -> Option<ModuleCatalogEntry> {
    let g = module_catalog_map()
        .lock()
        .expect("module catalog map poisoned");
    g.get(&module).cloned()
}

pub fn list_module_catalog() -> Vec<(ModuleId, ModuleCatalogEntry)> {
    let g = module_catalog_map()
        .lock()
        .expect("module catalog map poisoned");
    g.iter().map(|(k, v)| (*k, v.clone())).collect()
}

pub fn iter_fn_oid_entries() -> Vec<(Oid, RegisteredFunction)> {
    let g = fn_oid_map().lock().expect("fn_oid map poisoned");
    g.iter().map(|(oid, reg)| (*oid, reg.clone())).collect()
}

static FN_OID_MAP: OnceLock<Mutex<HashMap<Oid, RegisteredFunction>>> = OnceLock::new();

/// Dynamic `pg_proc` OIDs registered for each loaded module (for unload).
static MODULE_PROCS: OnceLock<Mutex<HashMap<ModuleId, Vec<Oid>>>> = OnceLock::new();

static MODULE_ABI: OnceLock<Mutex<HashMap<ModuleId, WasmAbiKind>>> = OnceLock::new();

static MODULE_NEEDS_WASI: OnceLock<Mutex<HashMap<ModuleId, bool>>> = OnceLock::new();

static MODULE_POLICY_OVERRIDES: OnceLock<Mutex<HashMap<ModuleId, PolicyOverrides>>> =
    OnceLock::new();

static MODULE_RESOURCE_LIMITS: OnceLock<Mutex<HashMap<ModuleId, ModuleResourceLimits>>> =
    OnceLock::new();

/// Persisted lifecycle hook export names (`on_unload`, `on_reconfigure`); `on_load` runs at load only.
#[derive(Clone, Debug, Default)]
pub struct ModuleHooks {
    pub on_unload: Option<String>,
    pub on_reconfigure: Option<String>,
}

static MODULE_HOOKS: OnceLock<Mutex<HashMap<ModuleId, ModuleHooks>>> = OnceLock::new();

static MODULE_EXECUTION_BACKEND: OnceLock<Mutex<HashMap<ModuleId, ModuleExecutionBackend>>> =
    OnceLock::new();

/// Track B: auto-generated composite type names per module `(schema, typname)` for `DROP TYPE` on unload.
static MODULE_TRACK_B_TYPES: OnceLock<Mutex<HashMap<ModuleId, Vec<(String, String)>>>> =
    OnceLock::new();

fn module_hooks_map() -> &'static Mutex<HashMap<ModuleId, ModuleHooks>> {
    MODULE_HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_execution_backend_map() -> &'static Mutex<HashMap<ModuleId, ModuleExecutionBackend>> {
    MODULE_EXECUTION_BACKEND.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_track_b_types_map() -> &'static Mutex<HashMap<ModuleId, Vec<(String, String)>>> {
    MODULE_TRACK_B_TYPES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn record_module_track_b_types(module: ModuleId, types: Vec<(String, String)>) {
    let mut g = module_track_b_types_map()
        .lock()
        .expect("module track-b types map poisoned");
    g.insert(module, types);
}

/// Remove and return Track B generated type names (for `DROP TYPE`); empty if none.
#[must_use]
pub fn take_module_track_b_types(module: ModuleId) -> Vec<(String, String)> {
    let mut g = module_track_b_types_map()
        .lock()
        .expect("module track-b types map poisoned");
    g.remove(&module).unwrap_or_default()
}

pub fn record_module_execution_backend(module: ModuleId, backend: ModuleExecutionBackend) {
    let mut g = module_execution_backend_map()
        .lock()
        .expect("module execution backend map poisoned");
    g.insert(module, backend);
}

pub fn module_execution_backend(module: ModuleId) -> Option<ModuleExecutionBackend> {
    let g = module_execution_backend_map()
        .lock()
        .expect("module execution backend map poisoned");
    g.get(&module).copied()
}

#[must_use]
pub fn take_module_execution_backend(module: ModuleId) -> Option<ModuleExecutionBackend> {
    let mut g = module_execution_backend_map()
        .lock()
        .expect("module execution backend map poisoned");
    g.remove(&module)
}

pub fn record_module_hooks(module: ModuleId, hooks: ModuleHooks) {
    let mut g = module_hooks_map()
        .lock()
        .expect("module hooks map poisoned");
    g.insert(module, hooks);
}

pub fn module_hooks(module: ModuleId) -> Option<ModuleHooks> {
    let g = module_hooks_map()
        .lock()
        .expect("module hooks map poisoned");
    g.get(&module).cloned()
}

#[must_use]
pub fn take_module_hooks(module: ModuleId) -> Option<ModuleHooks> {
    let mut g = module_hooks_map()
        .lock()
        .expect("module hooks map poisoned");
    g.remove(&module)
}

fn fn_oid_map() -> &'static Mutex<HashMap<Oid, RegisteredFunction>> {
    FN_OID_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_procs() -> &'static Mutex<HashMap<ModuleId, Vec<Oid>>> {
    MODULE_PROCS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_abi_map() -> &'static Mutex<HashMap<ModuleId, WasmAbiKind>> {
    MODULE_ABI.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_needs_wasi_map() -> &'static Mutex<HashMap<ModuleId, bool>> {
    MODULE_NEEDS_WASI.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_policy_overrides_map() -> &'static Mutex<HashMap<ModuleId, PolicyOverrides>> {
    MODULE_POLICY_OVERRIDES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_resource_limits_map() -> &'static Mutex<HashMap<ModuleId, ModuleResourceLimits>> {
    MODULE_RESOURCE_LIMITS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record detected or overridden ABI after a successful load.
pub fn record_module_abi(module: ModuleId, abi: WasmAbiKind) {
    let mut g = module_abi_map().lock().expect("module abi map poisoned");
    g.insert(module, abi);
}

pub fn module_abi(module: ModuleId) -> Option<WasmAbiKind> {
    let g = module_abi_map().lock().expect("module abi map poisoned");
    g.get(&module).copied()
}

pub fn record_module_policy_overrides(module: ModuleId, policy: PolicyOverrides) {
    let mut p = module_policy_overrides_map()
        .lock()
        .expect("module policy map poisoned");
    p.insert(module, policy);
}

pub fn record_module_needs_wasi(module: ModuleId, needs_wasi: bool) {
    let mut w = module_needs_wasi_map()
        .lock()
        .expect("module wasi map poisoned");
    w.insert(module, needs_wasi);
}

pub fn replace_module_policy_overrides(
    module: ModuleId,
    policy: PolicyOverrides,
) -> Result<(), ()> {
    let mut p = module_policy_overrides_map()
        .lock()
        .expect("module policy map poisoned");
    if !p.contains_key(&module) {
        return Err(());
    }
    p.insert(module, policy);
    Ok(())
}

pub fn module_needs_wasi(module: ModuleId) -> Option<bool> {
    let g = module_needs_wasi_map()
        .lock()
        .expect("module wasi map poisoned");
    g.get(&module).copied()
}

pub fn module_policy_overrides(module: ModuleId) -> Option<PolicyOverrides> {
    let g = module_policy_overrides_map()
        .lock()
        .expect("module policy map poisoned");
    g.get(&module).copied()
}

pub fn record_module_resource_limits(module: ModuleId, limits: ModuleResourceLimits) {
    let mut g = module_resource_limits_map()
        .lock()
        .expect("module resource limits map poisoned");
    g.insert(module, limits);
}

pub fn module_resource_limits(module: ModuleId) -> Option<ModuleResourceLimits> {
    let g = module_resource_limits_map()
        .lock()
        .expect("module resource limits map poisoned");
    g.get(&module).copied()
}

pub fn replace_module_resource_limits(
    module: ModuleId,
    limits: ModuleResourceLimits,
) -> Result<(), ()> {
    let mut g = module_resource_limits_map()
        .lock()
        .expect("module resource limits map poisoned");
    if !g.contains_key(&module) {
        return Err(());
    }
    g.insert(module, limits);
    Ok(())
}

#[must_use]
pub fn take_module_resource_limits(module: ModuleId) -> Option<ModuleResourceLimits> {
    let mut g = module_resource_limits_map()
        .lock()
        .expect("module resource limits map poisoned");
    g.remove(&module)
}

/// Remove and return stored ABI for `module` (e.g. on unload).
#[must_use]
pub fn take_module_abi(module: ModuleId) -> Option<WasmAbiKind> {
    let mut g = module_abi_map().lock().expect("module abi map poisoned");
    g.remove(&module)
}

pub fn take_module_wasi_and_policy(module: ModuleId) {
    let mut w = module_needs_wasi_map()
        .lock()
        .expect("module wasi map poisoned");
    w.remove(&module);
    let mut p = module_policy_overrides_map()
        .lock()
        .expect("module policy map poisoned");
    p.remove(&module);
    let _ = take_module_resource_limits(module);
}

#[must_use]
pub fn alloc_module_id() -> ModuleId {
    ModuleId(NEXT_MODULE_ID.fetch_add(1, Ordering::Relaxed))
}

pub fn record_module_proc(module: ModuleId, proc_oid: Oid) {
    let mut g = module_procs().lock().expect("module procs poisoned");
    g.entry(module).or_default().push(proc_oid);
}

/// Removes and returns procedure OIDs registered for `module`, without dropping catalog objects.
#[must_use]
pub fn take_module_proc_oids(module: ModuleId) -> Vec<Oid> {
    let mut g = module_procs().lock().expect("module procs poisoned");
    g.remove(&module).unwrap_or_default()
}

/// Register trampoline dispatch metadata for a `pg_proc` OID (typically right after `ProcedureCreate`).
pub fn unregister_fn_oid(oid: Oid) {
    let mut g = fn_oid_map().lock().expect("fn_oid map poisoned");
    g.remove(&oid);
}

/// Register trampoline target metadata for a `pg_proc` OID.
pub fn register_fn_oid(oid: Oid, entry: RegisteredFunction) {
    let mut g = fn_oid_map().lock().expect("fn_oid map poisoned");
    g.insert(oid, entry);
}

/// Look up metadata for the current `fcinfo->flinfo->fn_oid`.
pub fn lookup_by_fn_oid(oid: Oid) -> Option<RegisteredFunction> {
    let g = fn_oid_map().lock().expect("fn_oid map poisoned");
    g.get(&oid).cloned()
}
