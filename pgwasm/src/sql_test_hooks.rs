//! SQL-callable `pgwasm_test_*` entry points for pg_regress and `cargo pgrx test`.
//!
//! Compiled only with the `pg_test` Cargo feature so production installs omit these hooks.

use pgrx::{
    prelude::*,
    spi::{self, Spi},
};

use crate::{
    catalog::exports,
    errors::{ErrorContext, IntoReport, PgWasmError, Result},
    lifecycle::unload,
    shmem::{self, ExportCounterKind},
};

#[pg_extern(name = "pgwasm_test_force_cleanup_stuck_module")]
fn pgwasm_test_force_cleanup_stuck_module(
    module_name: &str,
    cascade: default!(bool, true),
) -> bool {
    unload::force_cleanup_orphaned_module_impl(module_name, cascade)
        .or_report(ErrorContext::default())
}

#[pg_extern(parallel_unsafe, stable, name = "pgwasm_test_scrub_shmem_slots")]
fn pgwasm_test_scrub_shmem_slots(from_id: i64, to_id: i64) -> i64 {
    test_scrub_shmem_slots_impl(from_id, to_id).or_report(ErrorContext::default())
}

#[pg_extern(parallel_unsafe, stable, name = "pgwasm_test_bump_export_counters")]
fn pgwasm_test_bump_export_counters(module_id: i64, export_index: i32, n: i64) -> i64 {
    test_bump_export_counters_impl(module_id, export_index, n).or_report(ErrorContext::default())
}

/// Best-effort release of metric slots for `module_id` in `from_id..=to_id` (inclusive).
/// Used by pg_regress: catalog rows are dropped when the database is recreated, but add-in
/// shared memory survives across `DROP DATABASE`, so stale `module_id` keys must be cleared.
fn test_scrub_shmem_slots_impl(from_id: i64, to_id: i64) -> Result<i64> {
    require_superuser_for_test_hooks()?;
    if from_id < 1 || to_id < from_id {
        return Err(PgWasmError::InvalidConfiguration(
            "pgwasm_test_scrub_shmem_slots requires 1 <= from_id <= to_id".to_string(),
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
fn test_bump_export_counters_impl(module_id: i64, export_index: i32, n: i64) -> Result<i64> {
    require_superuser_for_test_hooks()?;
    if n <= 0 {
        return Err(PgWasmError::InvalidConfiguration(
            "pgwasm_test_bump_export_counters: n must be positive".to_string(),
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
             (build `pgwasm` with `--features pg_test` for regress unless the extension is in \
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
            "pgwasm `pgwasm_test_*` SQL hooks require superuser".to_string(),
        ))
    }
}
