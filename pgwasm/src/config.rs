//! Runtime configuration materialized from GUCs and catalog state.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Module ABI hint used by the SQL load/reconfigure JSON payload.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) enum Abi {
    #[default]
    Component,
    Core,
}

/// Per-module resource ceilings that can only tighten extension GUC limits.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct Limits {
    pub(crate) fuel_per_invocation: Option<i32>,
    pub(crate) instances_per_module: Option<i32>,
    pub(crate) invocation_deadline_ms: Option<i32>,
    pub(crate) max_memory_pages: Option<i32>,
}

/// Per-module capability overrides that can only narrow extension GUC policy.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct PolicyOverrides {
    pub(crate) allow_spi: Option<bool>,
    pub(crate) allow_wasi: Option<bool>,
    pub(crate) allow_wasi_env: Option<bool>,
    pub(crate) allow_wasi_fs: Option<bool>,
    pub(crate) allow_wasi_http: Option<bool>,
    pub(crate) allow_wasi_net: Option<bool>,
    pub(crate) allow_wasi_stdio: Option<bool>,
    pub(crate) allowed_hosts: Option<Vec<String>>,
    pub(crate) wasi_preopens: Option<BTreeMap<String, String>>,
}

/// JSON options accepted by `pgwasm.pgwasm_load(...)`.
#[derive(Clone, Debug, Default)]
pub(crate) struct LoadOptions {
    pub(crate) abi: Option<Abi>,
    pub(crate) breaking_changes_allowed: bool,
    pub(crate) cascade: Option<bool>,
    pub(crate) limits: Option<Limits>,
    pub(crate) on_load_hook: bool,
    pub(crate) overrides: Option<PolicyOverrides>,
    pub(crate) replace_exports: bool,
}
