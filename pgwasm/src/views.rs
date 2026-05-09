//! SQL-facing observability views and SRF adapters.

use pgrx::{JsonB, pg_sys::Oid, prelude::*};
use serde_json::{Map, Value};

use crate::{
    catalog::{exports, modules, wit_types},
    errors::{PgWasmError, Result},
    lifecycle::reconfigure,
    policy::{self, GucSnapshot},
    shmem::{self, ExportCounterKind},
};

fn module_export_metrics_in_shmem(module_id: i64) -> Result<bool> {
    let exports = exports::list_by_module(module_id)?;
    let mid = module_id as u64;
    for (export_index, _) in exports.iter().enumerate() {
        let idx = i32::try_from(export_index).map_err(|_| {
            PgWasmError::Internal("export index overflow for shmem probe".to_string())
        })?;
        if shmem::read_export_counter(mid, idx as u32, ExportCounterKind::Invocations).is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// SRF over catalog `pgwasm.modules` with `shared` from shared-memory slot state.
#[allow(clippy::type_complexity)]
pub(crate) fn modules_sql() -> Result<
    TableIterator<
        'static,
        (
            name!(module_id, i64),
            name!(name, String),
            name!(origin, String),
            name!(digest, Vec<u8>),
            name!(loaded_at, TimestampWithTimeZone),
            name!(policy_json, JsonB),
            name!(limits_json, JsonB),
            name!(shared, bool),
        ),
    >,
> {
    let rows = modules::list()?;
    let out = rows
        .into_iter()
        .map(|m| {
            let shared = module_export_metrics_in_shmem(m.module_id)?;
            Ok((
                m.module_id,
                m.name,
                m.origin,
                m.digest,
                m.updated_at,
                JsonB(m.policy),
                JsonB(m.limits),
                shared,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(TableIterator::new(out))
}

/// SRF over catalog `pgwasm.exports` joined to module names.
#[allow(clippy::type_complexity)]
pub(crate) fn functions_sql() -> Result<
    TableIterator<
        'static,
        (
            name!(module_name, String),
            name!(export_name, String),
            name!(fn_oid, Option<Oid>),
            name!(arg_types, Vec<Oid>),
            name!(ret_type, Option<Oid>),
            name!(abi, String),
            name!(last_seen_generation, i64),
        ),
    >,
> {
    let export_rows = exports::list()?;
    let mut out = Vec::with_capacity(export_rows.len());
    for e in export_rows {
        let module_row = modules::get_by_id(e.module_id)?.ok_or_else(|| {
            PgWasmError::Internal(format!(
                "export {} references missing module_id {}",
                e.export_id, e.module_id
            ))
        })?;
        out.push((
            module_row.name,
            e.wasm_name,
            e.fn_oid,
            e.arg_types,
            e.ret_type,
            module_row.abi,
            module_row.generation,
        ));
    }
    out.sort_by(|a, b| {
        a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)).then_with(|| {
            let ao = a.2.map(u32::from).unwrap_or(0);
            let bo = b.2.map(u32::from).unwrap_or(0);
            ao.cmp(&bo)
        })
    });
    Ok(TableIterator::new(out))
}

/// SRF over catalog `pgwasm.wit_types` joined to module names.
#[allow(clippy::type_complexity)]
pub(crate) fn wit_types_sql() -> Result<
    TableIterator<
        'static,
        (
            name!(module_name, String),
            name!(type_key, String),
            name!(kind, String),
            name!(pg_type_oid, Oid),
            name!(last_seen_generation, i64),
        ),
    >,
> {
    let types = wit_types::list()?;
    let mut out = Vec::with_capacity(types.len());
    for t in types {
        let module_row = modules::get_by_id(t.module_id)?.ok_or_else(|| {
            PgWasmError::Internal(format!(
                "wit_type {} references missing module_id {}",
                t.wit_type_id, t.module_id
            ))
        })?;
        let type_key = format!("{}::{}", module_row.name, t.wit_name);
        out.push((
            module_row.name,
            type_key,
            t.kind,
            t.pg_type_oid,
            module_row.generation,
        ));
    }
    out.sort_by(|a, b| {
        a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)).then_with(|| {
            let at: u32 = a.3.into();
            let bt: u32 = b.3.into();
            at.cmp(&bt)
        })
    });
    Ok(TableIterator::new(out))
}

/// One row per module: resolved effective policy and limits as JSONB.
#[allow(clippy::type_complexity)]
pub(crate) fn policy_effective_sql() -> Result<
    TableIterator<
        'static,
        (
            name!(module_name, String),
            name!(policy_json, JsonB),
            name!(limits_json, JsonB),
        ),
    >,
> {
    let snapshot = GucSnapshot::from_gucs();
    let rows = modules::list()?;
    let mut out = Vec::with_capacity(rows.len());
    for m in rows {
        let overrides = reconfigure::policy_overrides_from_value(&m.policy)?;
        let limits = reconfigure::limits_from_value(&m.limits)?;
        let effective = policy::resolve(&snapshot, Some(&overrides), Some(&limits))?;
        let policy_json = JsonB(effective_policy_to_json(&effective));
        let limits_json = JsonB(effective_limits_to_json(&effective));
        out.push((m.name, policy_json, limits_json));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(TableIterator::new(out))
}

/// Per-export counters from shared memory (or zeros when no slot / overflow).
#[allow(clippy::type_complexity)]
pub(crate) fn stats_sql() -> Result<
    TableIterator<
        'static,
        (
            name!(module_name, String),
            name!(export_name, String),
            name!(invocations, i64),
            name!(traps, i64),
            name!(fuel_used_total, i64),
            name!(last_invocation_at, Option<TimestampWithTimeZone>),
            name!(shared, bool),
        ),
    >,
> {
    shmem::with_catalog_lock_shared(stats_locked)
}

#[allow(clippy::type_complexity)]
fn stats_locked() -> Result<
    TableIterator<
        'static,
        (
            name!(module_name, String),
            name!(export_name, String),
            name!(invocations, i64),
            name!(traps, i64),
            name!(fuel_used_total, i64),
            name!(last_invocation_at, Option<TimestampWithTimeZone>),
            name!(shared, bool),
        ),
    >,
> {
    let module_rows = modules::list()?;
    type StatsRow = (
        String,
        String,
        i64,
        i64,
        i64,
        Option<TimestampWithTimeZone>,
        bool,
    );
    let mut out: Vec<StatsRow> = Vec::new();

    for m in module_rows {
        let export_rows = exports::list_by_module(m.module_id)?;
        for (export_index, e) in export_rows.into_iter().enumerate() {
            let mid = m.module_id as u64;
            let idx = i32::try_from(export_index).map_err(|_| {
                PgWasmError::Internal("export index overflow for stats()".to_string())
            })?;

            let invocations =
                shmem::read_export_counter(mid, idx as u32, ExportCounterKind::Invocations)
                    .unwrap_or(0) as i64;
            let traps = shmem::read_export_counter(mid, idx as u32, ExportCounterKind::Traps)
                .unwrap_or(0) as i64;
            let fuel_used_total = shmem::read_export_counter(
                mid,
                idx as u32,
                ExportCounterKind::TotalNs,
            )
            .unwrap_or(0) as i64;

            let shared =
                shmem::read_export_counter(mid, idx as u32, ExportCounterKind::Invocations)
                    .is_some();

            out.push((
                m.name.clone(),
                e.wasm_name,
                invocations,
                traps,
                fuel_used_total,
                None,
                shared,
            ));
        }
    }

    out.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
    });
    Ok(TableIterator::new(out))
}

fn effective_policy_to_json(e: &policy::EffectivePolicy) -> Value {
    let mut m = Map::new();
    m.insert("allow_spi".to_string(), Value::Bool(e.allow_spi));
    m.insert("allow_wasi".to_string(), Value::Bool(e.allow_wasi));
    m.insert("allow_wasi_env".to_string(), Value::Bool(e.allow_wasi_env));
    m.insert("allow_wasi_fs".to_string(), Value::Bool(e.allow_wasi_fs));
    m.insert(
        "allow_wasi_http".to_string(),
        Value::Bool(e.allow_wasi_http),
    );
    m.insert("allow_wasi_net".to_string(), Value::Bool(e.allow_wasi_net));
    m.insert(
        "allow_wasi_stdio".to_string(),
        Value::Bool(e.allow_wasi_stdio),
    );
    m.insert(
        "allowed_hosts".to_string(),
        Value::Array(e.allowed_hosts.iter().cloned().map(Value::String).collect()),
    );
    m.insert(
        "wasi_preopens".to_string(),
        serde_json::to_value(&e.wasi_preopens).unwrap_or_else(|_| Value::Object(Map::new())),
    );
    Value::Object(m)
}

fn effective_limits_to_json(e: &policy::EffectivePolicy) -> Value {
    let mut m = Map::new();
    m.insert(
        "fuel_per_invocation".to_string(),
        Value::Number(e.fuel_per_invocation.into()),
    );
    m.insert(
        "instances_per_module".to_string(),
        Value::Number(e.instances_per_module.into()),
    );
    m.insert(
        "invocation_deadline_ms".to_string(),
        Value::Number(e.invocation_deadline_ms.into()),
    );
    m.insert(
        "max_memory_pages".to_string(),
        Value::Number(e.max_memory_pages.into()),
    );
    Value::Object(m)
}
