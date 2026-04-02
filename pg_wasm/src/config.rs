//! Load options, host policy, lifecycle hook names, and resource hints for WASM modules.

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

impl PolicyOverrides {
    /// JSON object for introspection helpers (e.g. `pg_wasm_modules` table function).
    #[cfg(feature = "_pg_wasm_runtime")]
    #[must_use]
    pub fn to_json_string(self) -> String {
        serde_json::json!({
            "allow_env": self.allow_env,
            "allow_fs_read": self.allow_fs_read,
            "allow_fs_write": self.allow_fs_write,
            "allow_network": self.allow_network,
            "allow_wasi": self.allow_wasi,
        })
        .to_string()
    }
}

/// Per-module resource overrides (`max_memory_pages`, `fuel`) in load/reconfigure JSON (plan §10).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ModuleResourceLimits {
    /// Cap guest linear memory per store (Wasm page = 64 KiB). `None` = use extension GUC only.
    pub max_memory_pages: Option<u32>,
    /// Fuel budget per wasm entry (host call, lifecycle hook, or trampoline). `None` = GUC only.
    pub fuel: Option<u64>,
}

impl ModuleResourceLimits {
    #[must_use]
    pub fn from_json_value(val: &serde_json::Value) -> Self {
        Self {
            max_memory_pages: val.get("max_memory_pages").and_then(json_u32_positive),
            fuel: val.get("fuel").and_then(json_u64_positive),
        }
    }
}

/// Keys present in `delta` replace `base`; use JSON `null` to clear an override (reconfigure).
#[must_use]
pub fn merge_resource_limits(
    base: ModuleResourceLimits,
    delta: &serde_json::Value,
) -> ModuleResourceLimits {
    let mut out = base;
    if let Some(v) = delta.get("max_memory_pages") {
        out.max_memory_pages = if v.is_null() {
            None
        } else {
            json_u32_positive(v)
        };
    }
    if let Some(v) = delta.get("fuel") {
        out.fuel = if v.is_null() {
            None
        } else {
            json_u64_positive(v)
        };
    }
    out
}

fn json_u32_positive(v: &serde_json::Value) -> Option<u32> {
    let n = v.as_u64()?;
    if n == 0 {
        return None;
    }
    Some(n.min(u64::from(u32::MAX)) as u32)
}

fn json_u64_positive(v: &serde_json::Value) -> Option<u64> {
    let n = v.as_u64()?;
    if n == 0 {
        return None;
    }
    Some(n)
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
    /// Per-module resource limits (top-level `max_memory_pages`, `fuel`); merged with GUCs at runtime.
    pub resource_limits: ModuleResourceLimits,
    /// Opaque JSON preserved for future keys.
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

    /// UTF-8 JSON of the full load `options` object for `(ptr, len)` lifecycle hooks; empty when unset.
    #[must_use]
    pub fn config_blob_for_hooks(&self) -> Vec<u8> {
        let Some(JsonB(v)) = &self.raw else {
            return Vec::new();
        };
        serde_json::to_vec(v).unwrap_or_default()
    }

    #[must_use]
    pub fn from_jsonb(j: Option<JsonB>) -> Self {
        let Some(JsonB(val)) = j else {
            return Self::default();
        };
        let (hook_on_load, hook_on_unload, hook_on_reconfigure) = hooks_from_json(&val);
        Self {
            runtime: val
                .get("runtime")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            abi_override: val.get("abi").and_then(|v| v.as_str()).map(str::to_string),
            hook_on_load,
            hook_on_unload,
            hook_on_reconfigure,
            policy: policy_overrides_from_json(&val),
            resource_limits: ModuleResourceLimits::from_json_value(&val),
            raw: Some(JsonB(val)),
        }
    }
}

/// Parse `hooks` object and/or top-level `on_load` / `hook_on_load`-style keys (plan §9).
fn hooks_from_json(val: &serde_json::Value) -> (Option<String>, Option<String>, Option<String>) {
    let nested = val.get("hooks").and_then(|v| v.as_object());
    let pick = |short: &str, long: &str| -> Option<String> {
        let s = nested
            .and_then(|m| m.get(short))
            .and_then(serde_json::Value::as_str)
            .or_else(|| val.get(short).and_then(serde_json::Value::as_str))
            .or_else(|| {
                nested
                    .and_then(|m| m.get(long))
                    .and_then(serde_json::Value::as_str)
            })
            .or_else(|| val.get(long).and_then(serde_json::Value::as_str))?;
        let t = s.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    };
    (
        pick("on_load", "hook_on_load"),
        pick("on_unload", "hook_on_unload"),
        pick("on_reconfigure", "hook_on_reconfigure"),
    )
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
    let policy_obj = val.get("policy").and_then(serde_json::Value::as_object);
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
