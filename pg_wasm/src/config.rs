//! Load options, host policy, and resource hints for WASM modules (wired up in later todos).

use pgrx::JsonB;

/// Per-module and load-time options passed as JSONB to `pg_wasm_load` (see plan §8–9).
#[derive(Debug, Default)]
pub struct LoadOptions {
    /// Preferred runtime when multiple backends are compiled in (`wasmtime`, `wasmer`, `extism`).
    pub runtime: Option<String>,
    /// Optional export names for lifecycle hooks (`on_load`, `on_unload`, `on_reconfigure`).
    pub hook_on_load: Option<String>,
    pub hook_on_unload: Option<String>,
    pub hook_on_reconfigure: Option<String>,
    /// Opaque JSON preserved for future keys (fuel, memory pages, etc.).
    pub raw: Option<JsonB>,
}

impl LoadOptions {
    #[must_use]
    pub fn from_jsonb(j: Option<JsonB>) -> Self {
        let Some(j) = j else {
            return Self::default();
        };
        // Full JSON parsing is implemented when load APIs land; keep shape and stash blob.
        Self {
            raw: Some(j),
            ..Default::default()
        }
    }
}

/// Effective host capabilities for WASI / imports (extension GUC ∩ per-module options).
#[derive(Debug, Clone, Default)]
pub struct HostPolicy {
    pub allow_wasi: bool,
    pub allow_fs_read: bool,
    pub allow_fs_write: bool,
    pub allow_network: bool,
    pub allow_env: bool,
}

impl HostPolicy {
    /// Conservative defaults: no host access until explicitly enabled.
    #[must_use]
    pub fn restricted() -> Self {
        Self::default()
    }
}
