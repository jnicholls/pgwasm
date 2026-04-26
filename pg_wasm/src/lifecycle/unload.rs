//! Unload lifecycle: catalog teardown, optional cascade over `wasm.dependencies`, and post-commit
//! cleanup of pools, artifacts, and shared-memory slots.

use std::collections::BTreeSet;

use pgrx::prelude::*;
use pgrx::spi::{self, Spi};

use crate::artifacts;
use crate::catalog::{exports, modules, wit_types};
use crate::errors::{PgWasmError, Result};
use crate::hooks;
use crate::proc_reg;
use crate::runtime::pool;
use crate::shmem;

const CATALOG_SCHEMA: &str = "wasm";
const ON_UNLOAD_WASM_NAME: &str = "on-unload";

/// Remove a module by name. Returns `Ok(true)` when a row was removed.
///
/// **Post-commit work (steps 7–10):** catalog mutations (unregister `pg_proc`, `DROP TYPE`,
/// delete catalog rows) run in the current transaction. On successful completion this function
/// registers a **one-shot transaction commit callback** via [`pgrx::register_xact_callback`] with
/// [`pgrx::PgXactCallbackEvent::Commit`], matching PostgreSQL’s `RegisterXactCallback(XACT_EVENT_COMMIT)`
/// semantics: [`pool::drain`], artifact directory removal, opportunistic [`artifacts::prune_stale`],
/// [`shmem::bump_generation`], and [`shmem::free_slots`] run only after the catalog transaction
/// commits. If the surrounding transaction rolls back, that callback is not invoked, so on-disk
/// state stays consistent with the rolled-back catalog.
pub(crate) fn unload_impl(module_name: &str, cascade: bool) -> Result<bool> {
    require_loader_or_superuser()?;

    let Some(module) = modules::get_by_name(module_name)? else {
        return Err(PgWasmError::NotFound(format!(
            "no wasm module named `{module_name}`"
        )));
    };

    let module_id = module.module_id;
    let module_id_u64 = u64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;

    let _ = hooks::on_unload(module_id_u64);

    try_on_unload_notice(module_id)?;

    if other_modules_depend_on(module_id)? && !cascade {
        return Err(PgWasmError::InvalidConfiguration(format!(
            "module `{module_name}` is listed as a dependency of another module in `{CATALOG_SCHEMA}.dependencies`; retry with `cascade := true` (hint: `cascade := true`)"
        )));
    }

    // CatalogLock: SPI catalog teardown (pg_proc / types / wasm.* rows). Post-commit work keeps
    // `bump_generation` / `free_slots` outside so they do not nest the same LWLock.
    shmem::with_catalog_lock_exclusive(|| -> Result<()> {
        let export_rows = exports::list_by_module(module_id)?;
        for row in &export_rows {
            if let Some(fn_oid) = row.fn_oid
                && fn_oid != pg_sys::InvalidOid
            {
                proc_reg::unregister(fn_oid)?;
            }
        }

        for row in &export_rows {
            let _ = exports::delete(row.export_id)?;
        }

        // `RemoveFunctionById` does not advance the global command counter; SPI `DROP TYPE` can still
        // consult stale syscache entries and fail with "cache lookup failed for function" when
        // resolving dependent objects. Match PostgreSQL's pattern after DDL that removes `pg_proc`.
        unsafe {
            pg_sys::CommandCounterIncrement();
        }

        let wit_rows = wit_types::list_by_module(module_id)?;
        for row in &wit_rows {
            drop_wit_type_oid(row.pg_type_oid, cascade)?;
            let _ = wit_types::delete(row.wit_type_id)?;
        }

        let deleted = modules::delete(module_id)?;
        if !deleted {
            return Err(PgWasmError::Internal(
                "unload removed catalog rows but modules::delete reported no row".to_string(),
            ));
        }
        Ok(())
    })?;

    let active_for_prune = active_module_ids()?;
    register_post_commit_cleanup(module_id_u64, active_for_prune);

    Ok(true)
}

/// Unload every module row. Restricted to **superuser** (see task: bulk unload for tests).
pub(crate) fn unload_all_impl() -> Result<usize> {
    require_superuser()?;

    let rows = modules::list()?;
    let mut n = 0usize;
    for module in rows {
        // Use cascade so bulk unload succeeds when `wasm.dependencies` rows exist (tests).
        let _ = unload_impl(&module.name, true)?;
        n += 1;
    }
    Ok(n)
}

/// Superuser-only best-effort teardown when a prior `unload` aborted mid-flight: catalog rows may
/// still reference `pg_proc` OIDs that were already removed, so `unload`/`DROP TYPE` can fail with
/// `cache lookup failed for function`. Mirrors `unload_impl` but ignores unregister/type-drop errors.
///
/// Returns `Ok(true)` when a `wasm.modules` row was removed (post-commit cleanup is scheduled).
pub(crate) fn force_cleanup_orphaned_module_impl(module_name: &str, cascade: bool) -> Result<bool> {
    require_superuser()?;

    let Some(module) = modules::get_by_name(module_name)? else {
        return Ok(false);
    };

    let module_id = module.module_id;
    let module_id_u64 = u64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;

    // Do not call `proc_reg::unregister` here: after a partial unload the catalog may still list
    // `fn_oid` values whose `pg_proc` rows are already gone; `RemoveFunctionById` then errors with
    // `cache lookup failed for function`. Bulk-delete catalog children so `modules::delete` cannot
    // be blocked by leftover FK rows or failed per-row SPI deletes.
    // CatalogLock: bulk-delete catalog tree for orphan recovery (SPI).
    let deleted = shmem::with_catalog_lock_exclusive(|| -> Result<bool> {
        let wit_rows = wit_types::list_by_module(module_id)?;
        for row in &wit_rows {
            let _ = drop_wit_type_oid(row.pg_type_oid, cascade);
        }

        modules::delete_module_tree_for_orphan_recovery(module_id)
    })?;
    if deleted {
        let active_for_prune = active_module_ids().unwrap_or_default();
        register_post_commit_cleanup(module_id_u64, active_for_prune);
    }

    Ok(deleted)
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
            "pg_wasm.unload requires superuser or membership in role `wasm_loader`".to_string(),
        ))
    }
}

fn require_superuser() -> Result<()> {
    let is_super = Spi::connect(|client| {
        let rows = client.select(
            "SELECT COALESCE(
                (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user),
                false
            ) AS is_super",
            Some(1),
            &[],
        )?;
        let row = rows.into_iter().next().ok_or(spi::Error::InvalidPosition)?;
        row.get_by_name::<bool, _>("is_super")?
            .ok_or(spi::Error::InvalidPosition)
    })
    .map_err(|error| PgWasmError::Internal(format!("superuser check failed: {error}")))?;

    if is_super {
        Ok(())
    } else {
        Err(PgWasmError::PermissionDenied(
            "pg_wasm.unload_all requires superuser".to_string(),
        ))
    }
}

fn try_on_unload_notice(module_id: i64) -> Result<()> {
    if exports::get_by_module_and_wasm_name(module_id, ON_UNLOAD_WASM_NAME)?.is_none() {
        return Ok(());
    }

    Ok(())
}

fn other_modules_depend_on(unloaded_module_id: i64) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS (
            SELECT 1
            FROM {CATALOG_SCHEMA}.dependencies d
            WHERE d.depends_on_module_id = {unloaded_module_id}
        )"
    );
    Spi::get_one::<bool>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("dependency check failed: {e}")))?
        .ok_or_else(|| {
            PgWasmError::Internal("dependency existence query returned NULL".to_string())
        })
}

fn fq_type_name(oid: pg_sys::Oid) -> Result<String> {
    let sql = format!(
        "SELECT (n.nspname::text || '.' || quote_ident(t.typname::text))::text
         FROM pg_catalog.pg_type AS t
         JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
         WHERE t.oid = {}",
        oid.to_u32()
    );
    Spi::get_one::<String>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error resolving type name: {e}")))?
        .ok_or_else(|| PgWasmError::Internal("type oid not found".to_string()))
}

fn type_in_wasm_schema(oid: pg_sys::Oid) -> Result<bool> {
    let sql = format!(
        "SELECT n.nspname::text = '{CATALOG_SCHEMA}'
         FROM pg_catalog.pg_type AS t
         JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
         WHERE t.oid = {}",
        oid.to_u32()
    );
    Spi::get_one::<bool>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error checking type schema: {e}")))?
        .ok_or_else(|| PgWasmError::Internal("type missing for schema check".to_string()))
}

fn drop_wit_type_oid(oid: pg_sys::Oid, cascade: bool) -> Result<()> {
    if !type_in_wasm_schema(oid)? {
        return Ok(());
    }

    let fq = fq_type_name(oid)?;
    let cascade_sql = if cascade { " CASCADE" } else { "" };
    let typtype: String = Spi::get_one(&format!(
        "SELECT typtype::text FROM pg_catalog.pg_type WHERE oid = {}",
        oid.to_u32()
    ))
    .map_err(|e| PgWasmError::Internal(format!("SPI error reading typtype: {e}")))?
    .ok_or_else(|| PgWasmError::Internal("type oid missing for drop".to_string()))?;

    let sql = if typtype == "d" {
        format!("DROP DOMAIN IF EXISTS {fq}{cascade_sql}")
    } else {
        format!("DROP TYPE IF EXISTS {fq}{cascade_sql}")
    };
    Spi::run(&sql).map_err(|e| PgWasmError::Internal(format!("DROP TYPE/DOMAIN failed: {e}")))?;
    Ok(())
}

fn active_module_ids() -> Result<BTreeSet<u64>> {
    let rows = modules::list()?;
    let mut out = BTreeSet::new();
    for m in rows {
        if let Ok(u) = u64::try_from(m.module_id) {
            out.insert(u);
        }
    }
    Ok(out)
}

/// Pool drain, artifact removal, opportunistic prune, generation bump, and shmem slot release.
///
/// Invoked from the transaction **commit** callback after catalog rows are durably removed.
/// `#[pg_test]` runs each test inside a client transaction that the framework **rolls back** after
/// the test returns, so `PgXactCallbackEvent::Commit` never fires there; tests that need to
/// assert on-disk behavior call this explicitly after `unload_impl` (see `lib.rs` tests).
pub(crate) fn run_post_commit_unload_work(module_id_u64: u64) {
    let active = match active_module_ids() {
        Ok(ids) => ids,
        Err(e) => {
            ereport!(
                PgLogLevel::WARNING,
                PgSqlErrorCode::ERRCODE_WARNING,
                format!("pg_wasm unload: active_module_ids failed: {e}"),
            );
            BTreeSet::new()
        }
    };
    post_commit_unload_work_inner(module_id_u64, &active);
}

fn post_commit_unload_work_inner(module_id_u64: u64, active_ids: &BTreeSet<u64>) {
    if let Err(e) = pool::drain(module_id_u64) {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            format!("pg_wasm unload: pool::drain failed for module_id {module_id_u64}: {e}"),
        );
    }

    if let Err(e) = artifacts::with_artifact_fs_lock(|| {
        let dir = artifacts::module_dir(module_id_u64)?;
        if dir.exists() {
            std::fs::remove_dir_all(&dir)?;
        }
        artifacts::prune_stale_unlocked(active_ids)?;
        Ok(())
    }) {
        ereport!(
            PgLogLevel::WARNING,
            PgSqlErrorCode::ERRCODE_WARNING,
            format!("pg_wasm unload: artifact cleanup failed: {e}"),
        );
    }

    let _ = shmem::bump_generation(module_id_u64);
    shmem::free_slots(module_id_u64);
}

fn register_post_commit_cleanup(module_id_u64: u64, active_ids: BTreeSet<u64>) {
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, move || {
        post_commit_unload_work_inner(module_id_u64, &active_ids);
    });
}

#[cfg(feature = "pg_test")]
pub(crate) mod test_support {
    //! Helpers for `#[pg_test]` in `lib.rs` (this agent cannot add `#[pg_test]` inside this file).

    use pgrx::pg_sys::AsPgCStr;
    use pgrx::prelude::*;
    use pgrx::spi::Spi;
    use serde_json::json;

    use crate::artifacts;
    use crate::catalog::{exports, modules, wit_types};
    use crate::proc_reg::{self, Parallel, ProcSpec, Volatility};

    pub fn extension_oid() -> pg_sys::Oid {
        let oid = unsafe { pg_sys::get_extension_oid("pg_wasm".as_pg_cstr(), false) };
        assert_ne!(
            oid,
            pg_sys::InvalidOid,
            "pg_wasm extension must be installed during pg_test"
        );
        oid
    }

    pub fn insert_stub_module(name: &str) -> i64 {
        let new_module = modules::NewModule {
            abi: "component".to_string(),
            artifact_path: "/tmp/stub.wasm".to_string(),
            digest: vec![9],
            generation: 0,
            limits: json!({}),
            name: name.to_string(),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![8; 32],
            wit_world: "default".to_string(),
        };
        modules::insert(&new_module)
            .expect("stub module insert")
            .module_id
    }

    pub fn register_dummy_sql_fn(name: &str) -> pg_sys::Oid {
        let spec = ProcSpec {
            schema: "public".to_string(),
            name: name.to_string(),
            arg_types: vec![pg_sys::INT4OID],
            arg_names: Vec::new(),
            arg_modes: Vec::new(),
            ret_type: pg_sys::INT4OID,
            returns_set: false,
            volatility: Volatility::Volatile,
            strict: false,
            parallel: Parallel::Unsafe,
            cost: Some(1.0),
        };
        proc_reg::register(&spec, extension_oid(), false).expect("register dummy fn")
    }

    pub fn insert_export_with_fn(module_id: i64, sql_name: &str, fn_oid: pg_sys::Oid) -> i64 {
        let row = exports::insert(&exports::NewExport {
            arg_types: vec![pg_sys::INT4OID],
            fn_oid: Some(fn_oid),
            kind: "function".to_string(),
            module_id,
            ret_type: Some(pg_sys::INT4OID),
            signature: json!({}),
            sql_name: sql_name.to_string(),
            wasm_name: "dummy".to_string(),
        })
        .expect("export insert");
        row.export_id
    }

    pub fn pg_proc_exists(fn_oid: pg_sys::Oid) -> bool {
        Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE oid = {})",
            fn_oid.to_u32()
        ))
        .expect("pg_proc exists query")
        .unwrap_or(false)
    }

    pub fn count_modules_where_name(name: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(*)::bigint FROM wasm.modules WHERE name = '{}'",
            name.replace('\'', "''")
        ))
        .expect("count modules")
        .unwrap_or(0)
    }

    pub fn count_exports_for_module(module_id: i64) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT count(*)::bigint FROM wasm.exports WHERE module_id = {module_id}"
        ))
        .expect("count exports")
        .unwrap_or(0)
    }

    pub fn insert_dependency(module_id: i64, depends_on: i64) {
        Spi::run(&format!(
            "INSERT INTO wasm.dependencies (module_id, depends_on_module_id) VALUES ({module_id}, {depends_on})"
        ))
        .expect("dependency insert");
    }

    pub fn create_composite_type(type_name: &str) -> pg_sys::Oid {
        let escaped = type_name.replace('\'', "''");
        Spi::run(&format!("CREATE TYPE {escaped} AS (x int)")).expect("create type");
        Spi::get_one::<pg_sys::Oid>(&format!("SELECT '{escaped}'::regtype::oid"))
            .expect("type oid")
            .expect("oid missing")
    }

    pub fn insert_wit_type_row(module_id: i64, wit_name: &str, pg_type_oid: pg_sys::Oid) -> i64 {
        wit_types::insert(&wit_types::NewWitType {
            definition: json!({}),
            kind: "composite".to_string(),
            module_id,
            pg_type_oid,
            wit_name: wit_name.to_string(),
        })
        .expect("wit_types insert")
        .wit_type_id
    }

    pub fn type_exists(oid: pg_sys::Oid) -> bool {
        Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE oid = {})",
            oid.to_u32()
        ))
        .expect("type exists")
        .unwrap_or(false)
    }

    pub fn ensure_artifact_dir(module_id: u64) -> std::path::PathBuf {
        let dir = artifacts::ensure_module_dir(module_id).expect("ensure module dir");
        std::fs::write(dir.join("marker.txt"), b"unload-test").expect("write marker");
        dir
    }

    pub fn artifact_dir_exists(module_id: u64) -> bool {
        artifacts::module_dir(module_id)
            .map(|p| p.exists())
            .unwrap_or(false)
    }
}
