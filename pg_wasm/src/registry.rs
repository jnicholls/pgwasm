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

fn fn_oid_map() -> &'static Mutex<HashMap<Oid, RegisteredFunction>> {
    FN_OID_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(feature = "runtime_wasmtime")]
fn module_procs() -> &'static Mutex<HashMap<ModuleId, Vec<Oid>>> {
    MODULE_PROCS.get_or_init(|| Mutex::new(HashMap::new()))
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
