//! In-process registry for loaded modules and `fn_oid` → export metadata (populated in later todos).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use pgrx::pg_sys::Oid;

use crate::mapping::ExportSignature;

/// Stable handle for a loaded module (bigint / sequence in SQL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModuleId(pub i64);

/// Metadata for one dynamically registered UDF.
#[derive(Debug, Clone)]
pub struct RegisteredFunction {
    pub module_id: ModuleId,
    pub export_name: String,
    pub signature: ExportSignature,
}

static FN_OID_MAP: OnceLock<Mutex<HashMap<Oid, RegisteredFunction>>> = OnceLock::new();

fn fn_oid_map() -> &'static Mutex<HashMap<Oid, RegisteredFunction>> {
    FN_OID_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Register trampoline target metadata (no-op until dynamic registration is implemented).
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
