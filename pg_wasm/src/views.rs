//! SQL-facing observability views and SRF adapters.

use pgrx::JsonB;
use pgrx::pg_sys::Oid;
use pgrx::prelude::*;
use pgrx::spi::{self, Spi};
use serde_json::{Map, Value};

use crate::catalog::{exports, modules, wit_types};
use crate::errors::{PgWasmError, Result};
use crate::lifecycle::reconfigure;
use crate::policy::{self, GucSnapshot};
use crate::shmem::{self, ExportCounterKind};

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

/// SRF over catalog `wasm.modules` with `shared` from shared-memory slot state.
#[allow(clippy::type_complexity)]
#[pg_extern(parallel_safe, stable, name = "modules")]
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

/// SRF over catalog `wasm.exports` joined to module names.
#[allow(clippy::type_complexity)]
#[pg_extern(parallel_safe, stable, name = "functions")]
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

/// SRF over catalog `wasm.wit_types` joined to module names.
#[allow(clippy::type_complexity)]
#[pg_extern(parallel_safe, stable, name = "wit_types")]
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
#[pg_extern(parallel_safe, stable, name = "policy_effective")]
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
#[pg_extern(parallel_unsafe, stable, name = "stats")]
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

/// Best-effort release of metric slots for `module_id` in `from_id..=to_id` (inclusive).
/// Used by pg_regress: catalog rows are dropped when the database is recreated, but add-in
/// shared memory survives across `DROP DATABASE`, so stale `module_id` keys must be cleared.
#[pg_extern(parallel_unsafe, stable, name = "test_scrub_shmem_slots")]
pub(crate) fn test_scrub_shmem_slots(from_id: i64, to_id: i64) -> Result<i64> {
    require_superuser_for_test_hooks()?;
    if from_id < 1 || to_id < from_id {
        return Err(PgWasmError::InvalidConfiguration(
            "test_scrub_shmem_slots requires 1 <= from_id <= to_id".to_string(),
        ));
    }
    let mut cleared = 0_i64;
    for id in from_id..=to_id {
        shmem::free_slots(id as u64);
        cleared += 1;
    }
    Ok(cleared)
}

/// Regression / manual hook: bump invocation counters for `(module_id, export_index)`.
/// Restricted to superusers so arbitrary roles cannot inflate metrics.
#[pg_extern(parallel_unsafe, stable, name = "test_bump_export_counters")]
pub(crate) fn test_bump_export_counters(module_id: i64, export_index: i32, n: i64) -> Result<i64> {
    require_superuser_for_test_hooks()?;
    if n <= 0 {
        return Err(PgWasmError::InvalidConfiguration(
            "n must be positive".to_string(),
        ));
    }
    let mid = module_id as u64;
    let export_rows = exports::list_by_module(module_id)?;
    let n_exports = export_rows
        .len()
        .max((export_index as usize).saturating_add(1))
        .max(1);
    shmem::allocate_slots(mid, n_exports).map_err(|overflow| {
        PgWasmError::ResourceLimitExceeded(format!(
            "shared-memory slot allocation failed for metrics regression hook: {overflow:?} \
             (build `pg_wasm` with `--features pg_test` for regress unless the extension is in \
             `shared_preload_libraries`)"
        ))
    })?;
    let mut total = 0_i64;
    for _ in 0..n {
        shmem::incr_export_counter(mid, export_index as u32, ExportCounterKind::Invocations);
        total += 1;
    }
    Ok(total)
}

fn require_superuser_for_test_hooks() -> Result<()> {
    let is_super = Spi::connect(|client| {
        let rows = client.select(
            "SELECT COALESCE(rolsuper, false) AS is_super
             FROM pg_catalog.pg_roles
             WHERE rolname = current_user",
            Some(1),
            &[],
        )?;
        let row = rows.into_iter().next().ok_or(spi::Error::InvalidPosition)?;
        row.get_by_name::<bool, _>("is_super")?
            .ok_or(spi::Error::InvalidPosition)
    })
    .map_err(|e| PgWasmError::Internal(format!("superuser check failed: {e}")))?;

    if is_super {
        Ok(())
    } else {
        Err(PgWasmError::PermissionDenied(
            "wasm.test_bump_export_counters requires superuser".to_string(),
        ))
    }
}

pgrx::extension_sql!(
    r#"
-- Observability SRF grants: PostgreSQL reserves the `pg_` prefix on role names, so the
-- reader role shipped with the catalog bootstrap remains `wasm_reader` (see pg_wasm--0.1.0.sql).

GRANT EXECUTE ON FUNCTION
    @extschema@.modules(),
    @extschema@.functions(),
    @extschema@.wit_types(),
    @extschema@.policy_effective(),
    @extschema@.stats()
TO wasm_reader;

CREATE OR REPLACE VIEW @extschema@.modules_view AS
    SELECT * FROM @extschema@.modules();

CREATE OR REPLACE VIEW @extschema@.functions_view AS
    SELECT * FROM @extschema@.functions();

CREATE OR REPLACE VIEW @extschema@.wit_types_view AS
    SELECT * FROM @extschema@.wit_types();

CREATE OR REPLACE VIEW @extschema@.policy_effective_view AS
    SELECT * FROM @extschema@.policy_effective();

CREATE OR REPLACE VIEW @extschema@.stats_view AS
    SELECT * FROM @extschema@.stats();

GRANT SELECT ON TABLE
    @extschema@.modules_view,
    @extschema@.functions_view,
    @extschema@.wit_types_view,
    @extschema@.policy_effective_view,
    @extschema@.stats_view
TO wasm_reader;
"#,
    name = "views_grants_and_aliases",
    requires = [
        modules_sql,
        functions_sql,
        wit_types_sql,
        policy_effective_sql,
        stats_sql,
        test_bump_export_counters,
        test_scrub_shmem_slots,
    ],
);
