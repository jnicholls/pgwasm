//! Reconfigure catalog `policy` / `limits` for a loaded module and bump generation.

use std::collections::BTreeMap;

use pgrx::Json;
use pgrx::spi::{self, Spi};
use serde_json::Value;

use crate::catalog::{exports, modules};
use crate::config::{Limits, PolicyOverrides};
use crate::errors::{PgWasmError, Result};
use crate::hooks;
use crate::policy;
use crate::shmem;

const ON_RECONFIGURE_WASM_NAME: &str = "on-reconfigure";

/// Reconfigure **does not touch code or types**. It updates `policy` and `limits` in
/// `wasm.modules`, re-computes the effective policy, and bumps shared-memory generation.
///
/// **StoreLimits / epoch deadlines:** the invocation path (Wave 3) is expected to read
/// `policy::GucSnapshot::from_gucs()` and the module's stored JSON on each call, then
/// build a fresh `Store` with `StoreLimits` / epoch deadline derived from that resolve
/// result. Reconfigure does not invalidate pooled instances; limits on the next `Store`
/// reflect the updated catalog row automatically.
pub(crate) fn reconfigure_impl(
    module_name: &str,
    policy_json: Option<Json>,
    limits_json: Option<Json>,
) -> Result<bool> {
    require_loader_or_superuser()?;

    let Some(module) = modules::get_by_name(module_name)? else {
        return Err(PgWasmError::NotFound(format!(
            "no wasm module named `{module_name}`"
        )));
    };

    let merged_policy = merge_json_objects(&module.policy, policy_json)?;
    let merged_limits = merge_json_objects(&module.limits, limits_json)?;

    let override_policy = policy_overrides_from_value(&merged_policy)?;
    let override_limits = limits_from_value(&merged_limits)?;

    let snapshot = policy::GucSnapshot::from_gucs();
    let effective = policy::resolve(&snapshot, Some(&override_policy), Some(&override_limits))?;

    if exports::get_by_module_and_wasm_name(module.module_id, ON_RECONFIGURE_WASM_NAME)?.is_some() {
        let module_id_u64 = u64::try_from(module.module_id)
            .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;
        hooks::on_reconfigure(module_id_u64, &effective)?;
    }

    let next_generation = module.generation.saturating_add(1);
    let updated = modules::NewModule {
        abi: module.abi.clone(),
        artifact_path: module.artifact_path.clone(),
        digest: module.digest.clone(),
        generation: next_generation,
        limits: merged_limits,
        name: module.name.clone(),
        origin: module.origin.clone(),
        policy: merged_policy,
        wasm_sha256: module.wasm_sha256.clone(),
        wit_world: module.wit_world.clone(),
    };

    let Some(_row) = modules::update(module.module_id, &updated)? else {
        return Err(PgWasmError::Internal(
            "catalog update returned no row for existing module".to_string(),
        ));
    };

    shmem::bump_generation(module.module_id as u64);

    Ok(true)
}

fn require_loader_or_superuser() -> Result<()> {
    let allowed = Spi::connect(|client| {
        let rows = client.select(
            "SELECT (
                COALESCE(
                    (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user),
                    false
                )
                OR pg_catalog.pg_has_role(
                    current_user::regrole,
                    'wasm_loader'::regrole,
                    'member'::text
                )
            ) AS allowed",
            Some(1),
            &[],
        )?;
        let row = rows.into_iter().next().ok_or(spi::Error::InvalidPosition)?;
        row.get_by_name::<bool, _>("allowed")?
            .ok_or(spi::Error::InvalidPosition)
    })
    .map_err(|error| PgWasmError::Internal(format!("authorization check failed: {error}")))?;

    if allowed {
        Ok(())
    } else {
        Err(PgWasmError::PermissionDenied(
            "pg_wasm.reconfigure requires superuser or membership in role `wasm_loader`"
                .to_string(),
        ))
    }
}

fn merge_json_objects(
    base: &Value,
    patch: Option<Json>,
) -> core::result::Result<Value, PgWasmError> {
    let mut out = base.as_object().cloned().unwrap_or_default();
    if let Some(Json(patch_value)) = patch {
        let patch_obj = patch_value.as_object().ok_or_else(|| {
            PgWasmError::InvalidConfiguration(
                "policy and limits arguments must be JSON objects".to_string(),
            )
        })?;
        for (key, value) in patch_obj {
            out.insert(key.clone(), value.clone());
        }
    }
    Ok(Value::Object(out))
}

pub(crate) fn policy_overrides_from_value(value: &Value) -> Result<PolicyOverrides> {
    let Some(obj) = value.as_object() else {
        return Err(PgWasmError::InvalidConfiguration(
            "policy must be a JSON object".to_string(),
        ));
    };

    let mut out = PolicyOverrides::default();
    for (key, field_value) in obj {
        match key.as_str() {
            "allow_spi" => out.allow_spi = Some(json_bool(field_value, "allow_spi")?),
            "allow_wasi" => out.allow_wasi = Some(json_bool(field_value, "allow_wasi")?),
            "allow_wasi_env" => {
                out.allow_wasi_env = Some(json_bool(field_value, "allow_wasi_env")?)
            }
            "allow_wasi_fs" => out.allow_wasi_fs = Some(json_bool(field_value, "allow_wasi_fs")?),
            "allow_wasi_http" => {
                out.allow_wasi_http = Some(json_bool(field_value, "allow_wasi_http")?);
            }
            "allow_wasi_net" => {
                out.allow_wasi_net = Some(json_bool(field_value, "allow_wasi_net")?)
            }
            "allow_wasi_stdio" => {
                out.allow_wasi_stdio = Some(json_bool(field_value, "allow_wasi_stdio")?);
            }
            "allowed_hosts" => {
                out.allowed_hosts = Some(json_string_array(field_value, "allowed_hosts")?)
            }
            "wasi_preopens" => {
                out.wasi_preopens = Some(json_string_to_string_map(field_value, "wasi_preopens")?);
            }
            _ => {
                // Ignore unknown keys for forward compatibility with future catalog fields.
            }
        }
    }

    Ok(out)
}

pub(crate) fn limits_from_value(value: &Value) -> Result<Limits> {
    let Some(obj) = value.as_object() else {
        return Err(PgWasmError::InvalidConfiguration(
            "limits must be a JSON object".to_string(),
        ));
    };

    let mut out = Limits::default();
    for (key, field_value) in obj {
        match key.as_str() {
            "fuel_per_invocation" => {
                out.fuel_per_invocation = Some(json_i32(field_value, "fuel_per_invocation")?);
            }
            "instances_per_module" => {
                out.instances_per_module = Some(json_i32(field_value, "instances_per_module")?);
            }
            "invocation_deadline_ms" => {
                out.invocation_deadline_ms = Some(json_i32(field_value, "invocation_deadline_ms")?);
            }
            "max_memory_pages" => {
                out.max_memory_pages = Some(json_i32(field_value, "max_memory_pages")?);
            }
            _ => {}
        }
    }

    Ok(out)
}

fn json_bool(value: &Value, field: &'static str) -> Result<bool> {
    value.as_bool().ok_or_else(|| {
        PgWasmError::InvalidConfiguration(format!("`{field}` must be a JSON boolean"))
    })
}

fn json_i32(value: &Value, field: &'static str) -> Result<i32> {
    let n = value.as_i64().ok_or_else(|| {
        PgWasmError::InvalidConfiguration(format!("`{field}` must be a JSON integer"))
    })?;
    i32::try_from(n).map_err(|_| {
        PgWasmError::InvalidConfiguration(format!("`{field}` is out of range for i32"))
    })
}

fn json_string_array(value: &Value, field: &'static str) -> Result<Vec<String>> {
    let Some(items) = value.as_array() else {
        return Err(PgWasmError::InvalidConfiguration(format!(
            "`{field}` must be a JSON array of strings"
        )));
    };

    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let s = item.as_str().ok_or_else(|| {
            PgWasmError::InvalidConfiguration(format!("`{field}[{index}]` must be a JSON string"))
        })?;
        out.push(s.to_string());
    }

    Ok(out)
}

fn json_string_to_string_map(
    value: &Value,
    field: &'static str,
) -> Result<BTreeMap<String, String>> {
    let Some(obj) = value.as_object() else {
        return Err(PgWasmError::InvalidConfiguration(format!(
            "`{field}` must be a JSON object with string values"
        )));
    };

    let mut out = BTreeMap::new();
    for (guest, host) in obj {
        let host_str = host.as_str().ok_or_else(|| {
            PgWasmError::InvalidConfiguration(format!("`{field}.{guest}` must be a JSON string"))
        })?;
        out.insert(guest.clone(), host_str.to_string());
    }

    Ok(out)
}
