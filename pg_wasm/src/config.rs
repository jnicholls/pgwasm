//! Load options, host policy, and resource hints for WASM modules (wired up in later todos).

use pgrx::JsonB;

/// Per-module policy knobs in `pg_wasm_load` / `pg_wasm_reconfigure_module` JSON. Each flag can only
/// narrow what extension GUCs permit (`crate::guc::effective_host_policy`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PolicyOverrides {
    pub allow_env: Option<bool>,
    pub allow_fs_read: Option<bool>,
    pub allow_fs_write: Option<bool>,
    pub allow_network: Option<bool>,
    pub allow_wasi: Option<bool>,
}

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
    /// Per-module policy overrides (top-level keys or `policy` object).
    pub policy: PolicyOverrides,
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
            policy: policy_overrides_from_json(&val),
            raw: Some(JsonB(val)),
            ..Default::default()
        }
    }
}

/// Effective host capabilities for WASI / imports (extension GUC ∩ per-module options).
#[derive(Clone, Debug, Default)]
pub struct HostPolicy {
    pub allow_env: bool,
    pub allow_fs_read: bool,
    pub allow_fs_write: bool,
    pub allow_network: bool,
    pub allow_wasi: bool,
}

impl HostPolicy {
    /// Conservative defaults: no host access until explicitly enabled.
    #[must_use]
    pub fn restricted() -> Self {
        Self::default()
    }
}

/// Merge `delta` into `base` for `pg_wasm_reconfigure_module`: keys present in `delta` replace prior
/// per-module overrides; omitted keys keep `base`.
#[must_use]
pub fn merge_policy_overrides(base: PolicyOverrides, delta: &serde_json::Value) -> PolicyOverrides {
    let p = policy_overrides_from_json(delta);
    PolicyOverrides {
        allow_env: p.allow_env.or(base.allow_env),
        allow_fs_read: p.allow_fs_read.or(base.allow_fs_read),
        allow_fs_write: p.allow_fs_write.or(base.allow_fs_write),
        allow_network: p.allow_network.or(base.allow_network),
        allow_wasi: p.allow_wasi.or(base.allow_wasi),
    }
}

fn policy_overrides_from_json(val: &serde_json::Value) -> PolicyOverrides {
    let policy_obj = val
        .get("policy")
        .and_then(serde_json::Value::as_object);
    let pick = |key: &str| -> Option<bool> {
        policy_obj
            .and_then(|m| m.get(key))
            .or_else(|| val.get(key))
            .and_then(serde_json::Value::as_bool)
    };
    PolicyOverrides {
        allow_env: pick("allow_env"),
        allow_fs_read: pick("allow_fs_read"),
        allow_fs_write: pick("allow_fs_write"),
        allow_network: pick("allow_network"),
        allow_wasi: pick("allow_wasi"),
    }
}
