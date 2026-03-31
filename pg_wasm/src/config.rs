//! Load options, host policy, and resource hints for WASM modules (wired up in later todos).

use pgrx::JsonB;

/// Per-module and load-time options passed as JSONB to `pg_wasm_load` (see plan §8–9).
#[derive(Debug, Default)]
pub struct LoadOptions {
    /// Preferred runtime when multiple backends are compiled in (`wasmtime`, `wasmer`, `extism`).
    pub runtime: Option<String>,
    /// Override ABI detection: `core`, `extism`, or `component` (see plan §2).
    pub abi_override: Option<String>,
    /// Optional export names for lifecycle hooks (`on_load`, `on_unload`, `on_reconfigure`).
    pub hook_on_load: Option<String>,
    pub hook_on_unload: Option<String>,
    pub hook_on_reconfigure: Option<String>,
    /// Opaque JSON preserved for future keys (fuel, memory pages, etc.).
    pub raw: Option<JsonB>,
}

impl LoadOptions {
    /// Export name → SQL types from `options.exports` (see [`crate::mapping::parse_export_hints`]).
    pub fn export_hints(&self) -> Result<crate::mapping::ExportHintMap, String> {
        let Some(JsonB(v)) = &self.raw else {
            return Ok(crate::mapping::ExportHintMap::new());
        };
        crate::mapping::parse_export_hints(v)
    }

    #[must_use]
    pub fn from_jsonb(j: Option<JsonB>) -> Self {
        let Some(JsonB(val)) = j else {
            return Self::default();
        };
        Self {
            runtime: val
                .get("runtime")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            abi_override: val
                .get("abi")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            raw: Some(JsonB(val)),
            ..Default::default()
        }
    }
}

/// Effective host capabilities for WASI / imports (extension GUC ∩ per-module options).
#[derive(Clone, Debug, Default)]
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
