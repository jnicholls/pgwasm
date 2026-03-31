//! In-process registry for loaded modules and `fn_oid` → export metadata.

use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

#[cfg(feature = "runtime_wasmtime")]
use std::sync::atomic::{AtomicI64, Ordering};

use pgrx::pg_sys::Oid;

use crate::mapping::ExportSignature;

#[cfg(feature = "runtime_wasmtime")]
use crate::abi::WasmAbiKind;

#[cfg(feature = "runtime_wasmtime")]
use crate::config::PolicyOverrides;

#[cfg(feature = "runtime_wasmtime")]
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
}

static FN_OID_MAP: OnceLock<Mutex<HashMap<Oid, RegisteredFunction>>> = OnceLock::new();

/// Dynamic `pg_proc` OIDs registered for each loaded module (for unload).
#[cfg(feature = "runtime_wasmtime")]
static MODULE_PROCS: OnceLock<Mutex<HashMap<ModuleId, Vec<Oid>>>> = OnceLock::new();

#[cfg(feature = "runtime_wasmtime")]
static MODULE_ABI: OnceLock<Mutex<HashMap<ModuleId, WasmAbiKind>>> = OnceLock::new();

#[cfg(feature = "runtime_wasmtime")]
static MODULE_NEEDS_WASI: OnceLock<Mutex<HashMap<ModuleId, bool>>> = OnceLock::new();

#[cfg(feature = "runtime_wasmtime")]
static MODULE_POLICY_OVERRIDES: OnceLock<Mutex<HashMap<ModuleId, PolicyOverrides>>> =
    OnceLock::new();

fn fn_oid_map() -> &'static Mutex<HashMap<Oid, RegisteredFunction>> {
    FN_OID_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "runtime_wasmtime")]
fn module_procs() -> &'static Mutex<HashMap<ModuleId, Vec<Oid>>> {
    MODULE_PROCS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "runtime_wasmtime")]
fn module_abi_map() -> &'static Mutex<HashMap<ModuleId, WasmAbiKind>> {
    MODULE_ABI.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "runtime_wasmtime")]
fn module_needs_wasi_map() -> &'static Mutex<HashMap<ModuleId, bool>> {
    MODULE_NEEDS_WASI.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "runtime_wasmtime")]
fn module_policy_overrides_map() -> &'static Mutex<HashMap<ModuleId, PolicyOverrides>> {
    MODULE_POLICY_OVERRIDES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Record detected or overridden ABI after a successful load (plan §2).
#[cfg(feature = "runtime_wasmtime")]
pub fn record_module_abi(module: ModuleId, abi: WasmAbiKind) {
    let mut g = module_abi_map().lock().expect("module abi map poisoned");
    g.insert(module, abi);
}

#[cfg(feature = "runtime_wasmtime")]
pub fn record_module_wasi_and_policy(module: ModuleId, needs_wasi: bool, policy: PolicyOverrides) {
    let mut w = module_needs_wasi_map().lock().expect("module wasi map poisoned");
    w.insert(module, needs_wasi);
    let mut p = module_policy_overrides_map().lock().expect("module policy map poisoned");
    p.insert(module, policy);
}

#[cfg(feature = "runtime_wasmtime")]
pub fn replace_module_policy_overrides(module: ModuleId, policy: PolicyOverrides) -> Result<(), ()> {
    let mut p = module_policy_overrides_map().lock().expect("module policy map poisoned");
    if !p.contains_key(&module) {
        return Err(());
    }
    p.insert(module, policy);
    Ok(())
}

#[cfg(feature = "runtime_wasmtime")]
#[must_use]
pub fn module_needs_wasi(module: ModuleId) -> Option<bool> {
    let g = module_needs_wasi_map().lock().expect("module wasi map poisoned");
    g.get(&module).copied()
}

#[cfg(feature = "runtime_wasmtime")]
#[must_use]
pub fn module_policy_overrides(module: ModuleId) -> Option<PolicyOverrides> {
    let g = module_policy_overrides_map().lock().expect("module policy map poisoned");
    g.get(&module).copied()
}

/// Remove and return stored ABI for `module` (e.g. on unload).
#[cfg(feature = "runtime_wasmtime")]
#[must_use]
pub fn take_module_abi(module: ModuleId) -> Option<WasmAbiKind> {
    let mut g = module_abi_map().lock().expect("module abi map poisoned");
    g.remove(&module)
}

#[cfg(feature = "runtime_wasmtime")]
pub fn take_module_wasi_and_policy(module: ModuleId) {
    let mut w = module_needs_wasi_map().lock().expect("module wasi map poisoned");
    w.remove(&module);
    let mut p = module_policy_overrides_map().lock().expect("module policy map poisoned");
    p.remove(&module);
}

#[cfg(feature = "runtime_wasmtime")]
#[must_use]
pub fn alloc_module_id() -> ModuleId {
    ModuleId(NEXT_MODULE_ID.fetch_add(1, Ordering::Relaxed))
}

#[cfg(feature = "runtime_wasmtime")]
pub fn record_module_proc(module: ModuleId, proc_oid: Oid) {
    let mut g = module_procs().lock().expect("module procs poisoned");
    g.entry(module).or_default().push(proc_oid);
}

/// Removes and returns procedure OIDs registered for `module`, without dropping catalog objects.
#[cfg(feature = "runtime_wasmtime")]
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
#[must_use]
pub fn lookup_by_fn_oid(oid: Oid) -> Option<RegisteredFunction> {
    let g = fn_oid_map().lock().expect("fn_oid map poisoned");
    g.get(&oid).cloned()
}
