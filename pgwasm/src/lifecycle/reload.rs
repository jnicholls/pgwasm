#![allow(clippy::result_large_err)]

//! Reload lifecycle: OID-preserving export/type updates, atomic artifact swap, pool drain, generation bump.
//!
//! ## Transaction semantics and on-disk rollback
//!
//! `module.wasm`, `module.cwasm`, and `world.wit` are replaced with [`crate::artifacts::write_atomic`]
//! (temp file + `rename`). Catalog mutations run in the same transaction via SPI. If the
//! transaction **aborts** after a successful swap, a top-level `PgXactCallbackEvent::Abort` handler
//! restores the three files from byte snapshots taken **before** the swap. (Nested subtransaction
//! abort uses the same pattern as [`super::load`]: `AbortSub` when the subtransaction id matches
//! the one active when reload registered the callback.) On **commit**, the pre-swap snapshots are
//! dropped and no file restore runs.

use std::collections::HashMap;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};

use pgrx::prelude::*;
use pgrx::spi::{self, Spi};
use pgrx::{PgSubXactCallbackEvent, register_subxact_callback};
use serde_json::{Value, json};
use wasmparser::{CompositeInnerType, ExternalKind, Parser, Payload, ValType};
use wit_parser::{Function, FunctionKind, Type, WorldItem, WorldKey};

use crate::abi::{self, Abi, AbiOverride};
use crate::artifacts;
use crate::catalog::{EXTENSION_SCHEMA, exports, modules, wit_types};
use crate::config::{Abi as OptionsAbi, LoadOptions, PolicyOverrides};
use crate::errors::{PgWasmError, Result};
use crate::guc;
use crate::hooks;
use crate::policy::{self, EffectivePolicy, GucSnapshot};
use crate::proc_reg::{self, Parallel, ProcSpec, Volatility};
use crate::runtime::component;
use crate::runtime::core as runtime_core;
use crate::runtime::engine;
use crate::runtime::pool;
use crate::shmem;
use crate::wit::typing;
use crate::wit::udt;
use crate::wit::world;

use super::reconfigure;
use super::unload;

const ON_LOAD_WASM_NAME: &str = "on-load";
const ON_RECONFIGURE_WASM_NAME: &str = "on-reconfigure";

#[derive(Clone, Debug, Eq, PartialEq)]
enum BreakingChange {
    AbiSwitched { from: String, to: String },
    ExportRemoved { wasm_name: String },
    ExportSignatureChanged { wasm_name: String },
    RecordFieldRemovedOrReordered { type_key: String },
    EnumValueRemoved { type_key: String },
}

impl BreakingChange {
    fn detail(&self) -> String {
        match self {
            Self::AbiSwitched { from, to } => {
                format!("module ABI changed from `{from}` to `{to}`")
            }
            Self::ExportRemoved { wasm_name } => {
                format!("export `{wasm_name}` is missing in the new module")
            }
            Self::ExportSignatureChanged { wasm_name } => {
                format!("export `{wasm_name}` has a different WIT signature")
            }
            Self::RecordFieldRemovedOrReordered { type_key } => {
                format!("WIT record `{type_key}` lost or reordered a field (unsafe for ALTER TYPE)")
            }
            Self::EnumValueRemoved { type_key } => {
                format!("WIT enum `{type_key}` removed a case (unsafe for ALTER TYPE)")
            }
        }
    }

    fn hint(&self) -> &'static str {
        match self {
            Self::AbiSwitched { .. } => {
                "Unload the module and load again if an ABI change is intentional, or pass options.breaking_changes_allowed only for export/type-level changes within the same ABI."
            }
            Self::ExportRemoved { .. } | Self::ExportSignatureChanged { .. } => {
                "Set options.breaking_changes_allowed to true to allow unregistering obsolete exports and registering new ones."
            }
            Self::RecordFieldRemovedOrReordered { .. } | Self::EnumValueRemoved { .. } => {
                "WIT type transitions require ADD ATTRIBUTE / safe enum additions; removals or reordering are rejected unless breaking_changes_allowed is true (not all cases are supported)."
            }
        }
    }

    fn into_error(self) -> PgWasmError {
        PgWasmError::BreakingChangeReload {
            detail: self.detail(),
            hint: self.hint().to_string(),
        }
    }
}

#[derive(Clone)]
struct ArtifactSnapshot {
    cwasm: Option<Vec<u8>>,
    wasm: Vec<u8>,
    wit: Option<Vec<u8>>,
}

#[derive(Clone)]
struct RollbackPaths {
    cwasm: PathBuf,
    wasm: PathBuf,
    wit: PathBuf,
}

/// `pgwasm.pgwasm_reload(module_name, bytes_or_path, options)` — see architecture doc "Reload lifecycle".
#[pg_extern(name = "pgwasm_reload")]
pub fn reload(
    module_name: &str,
    bytes_or_path: pgrx::Json,
    options: default!(Option<pgrx::Json>, NULL),
) -> core::result::Result<bool, pgrx::pg_sys::panic::ErrorReport> {
    reload_impl(module_name, bytes_or_path, options).map_err(PgWasmError::into_error_report)
}

/// Superuser-only regress hook: remove a module whose catalog row survived a failed `unload`
/// (stale `fn_oid` references after `RemoveFunctionById`).
#[pg_extern(name = "pgwasm_test_force_cleanup_stuck_module")]
pub fn test_force_cleanup_stuck_module(
    module_name: &str,
    cascade: default!(bool, true),
) -> core::result::Result<bool, pgrx::pg_sys::panic::ErrorReport> {
    unload::force_cleanup_orphaned_module_impl(module_name, cascade)
        .map_err(PgWasmError::into_error_report)
}

pub(crate) fn reload_impl(
    module_name: &str,
    bytes_or_path: pgrx::Json,
    options: Option<pgrx::Json>,
) -> Result<bool> {
    if !guc::ENABLED.get() {
        return Err(PgWasmError::Disabled);
    }
    if module_name.is_empty() {
        return Err(PgWasmError::InvalidConfiguration(
            "module name must not be empty".to_string(),
        ));
    }

    require_loader_or_superuser()?;

    let Some(module_row) = modules::get_by_name(module_name)? else {
        return Err(PgWasmError::NotFound(format!(
            "no wasm module named `{module_name}`"
        )));
    };

    reload_impl_for_module_row(&module_row, bytes_or_path, options)
}

fn reload_impl_for_module_row(
    module_row: &modules::ModuleRow,
    bytes_or_path: pgrx::Json,
    options: Option<pgrx::Json>,
) -> Result<bool> {
    let module_name = module_row.name.as_str();
    let opts = parse_reload_options(options)?;
    let bytes = reload_read_module_bytes(&bytes_or_path)?;
    abi::validate(&bytes)?;

    let abi_override = match opts.abi {
        None | Some(OptionsAbi::Component) => AbiOverride::Auto,
        Some(OptionsAbi::Core) => AbiOverride::ForceCore,
    };
    let classified = abi::detect(&bytes, abi_override)?;

    let catalog_abi = module_row.abi.to_ascii_lowercase();
    let breaking_allowed = opts.breaking_changes_allowed;
    match (catalog_abi.as_str(), classified) {
        ("component", Abi::Core) | ("core", Abi::Component) => {
            if !breaking_allowed {
                return Err(BreakingChange::AbiSwitched {
                    from: catalog_abi.clone(),
                    to: match classified {
                        Abi::Component => "component".to_string(),
                        Abi::Core => "core".to_string(),
                    },
                }
                .into_error());
            }
            return Err(PgWasmError::InvalidConfiguration(
                "reload cannot switch between core and component ABI; unload the module and load again"
                    .to_string(),
            ));
        }
        _ => {}
    }

    let extension_oid = extension_oid()?;
    let guc_snapshot = GucSnapshot::from_gucs();
    let policy_json = json_without_null_entries(&merge_catalog_patch(
        &module_row.policy,
        opts.overrides.as_ref(),
    )?)?;
    let limits_json = json_without_null_entries(&merge_limits_patch(
        &module_row.limits,
        opts.limits.as_ref(),
    )?)?;
    let override_policy = reconfigure::policy_overrides_from_value(&policy_json)?;
    let override_limits = reconfigure::limits_from_value(&limits_json)?;
    let effective = policy::resolve(
        &guc_snapshot,
        Some(&override_policy),
        Some(&override_limits),
    )?;

    let wasm_sha256_bytes = artifacts::sha256_bytes(&bytes);

    let module_id = module_row.module_id;
    let module_id_u64 = u64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;

    artifacts::with_artifact_fs_lock_result(|| match classified {
        Abi::Component => reload_component(
            module_name,
            module_id,
            module_id_u64,
            module_row,
            &bytes,
            &wasm_sha256_bytes,
            &opts,
            extension_oid,
            &effective,
            policy_json,
            limits_json,
        ),
        Abi::Core => reload_core(
            module_name,
            module_id,
            module_id_u64,
            module_row,
            &bytes,
            &wasm_sha256_bytes,
            &opts,
            extension_oid,
            &effective,
            policy_json,
            limits_json,
        ),
    })
}

#[allow(clippy::too_many_arguments)]
fn reload_component(
    module_name: &str,
    module_id: i64,
    module_id_u64: u64,
    module_row: &modules::ModuleRow,
    bytes: &[u8],
    wasm_sha256_bytes: &[u8; 32],
    opts: &LoadOptions,
    extension_oid: pg_sys::Oid,
    effective: &EffectivePolicy,
    policy_json: Value,
    limits_json: Value,
) -> Result<bool> {
    let decoded = world::decode(bytes)?;
    if world_exports_function_named(&decoded, ON_LOAD_WASM_NAME) {
        return Err(PgWasmError::InvalidConfiguration(
            "module exports `on-load` hook; hook invocation is not wired yet (wave-4 hooks)"
                .to_string(),
        ));
    }

    let wit_text = decoded.wit_text.clone();
    let type_plan = typing::plan_types(module_name, &decoded)?;

    let old_exports = exports::list_by_module(module_id)?;

    let wasm_engine = engine::try_shared_engine()?;
    let _compiled = component::compile(wasm_engine, bytes)?;

    let paths = RollbackPaths {
        cwasm: artifacts::module_cwasm_path(module_id_u64)?,
        wasm: artifacts::module_wasm_path(module_id_u64)?,
        wit: artifacts::world_wit_path(module_id_u64)?,
    };
    let snapshot = snapshot_artifacts(&paths)?;
    write_component_wasm_wit(module_id_u64, bytes, &wit_text)?;
    register_artifact_rollback(snapshot, paths);

    let _registered_types = udt::register_type_plan(&type_plan, module_id_u64, extension_oid)
        .map_err(|e| map_udt_error_to_breaking(e, opts.breaking_changes_allowed))?;

    let new_specs = plan_export_proc_specs(module_name, module_id, &decoded)?;
    let new_by_wasm: HashMap<String, &ProcSpec> = new_specs
        .iter()
        .map(|(spec, wasm)| (wasm.clone(), spec))
        .collect();
    let old_by_wasm: HashMap<String, &exports::ExportRow> = old_exports
        .iter()
        .map(|row| (row.wasm_name.clone(), row))
        .collect();

    plan_reload_exports(
        &old_by_wasm,
        &new_by_wasm,
        &decoded,
        opts.breaking_changes_allowed,
    )?;

    let cwasm_path = artifacts::module_cwasm_path(module_id_u64)?;
    let next_generation = module_row.generation.saturating_add(1);
    let wasm_dir = artifacts::module_dir(module_id_u64)?;

    // CatalogLock: export SPI + precompile + module row must not interleave with invocations that
    // `load_precompiled` the cwasm path (a gap between SPI and `precompile_to` produced ENOENT).
    shmem::with_catalog_lock_exclusive(|| -> Result<()> {
        apply_export_changes(
            module_id,
            extension_oid,
            opts,
            &old_exports,
            &new_specs,
            &decoded,
        )?;

        let precompile_hash = component::precompile_to(wasm_engine, bytes, &cwasm_path)?;
        artifacts::write_checksum(&wasm_dir, wasm_sha256_bytes)?;

        let updated = modules::NewModule {
            abi: "component".to_string(),
            artifact_path: cwasm_path.display().to_string(),
            digest: wasm_sha256_bytes.to_vec(),
            generation: next_generation,
            limits: limits_json,
            name: module_name.to_string(),
            origin: module_row.origin.clone(),
            policy: policy_json,
            wasm_sha256: precompile_hash.to_vec(),
            wit_world: wit_text,
        };
        let Some(_row) = modules::update(module_id, &updated)? else {
            return Err(PgWasmError::Internal(
                "catalog update after reload returned no row".to_string(),
            ));
        };
        Ok(())
    })?;

    finish_reload_after_catalog_commit(module_id_u64, module_id, effective)?;

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn reload_core(
    module_name: &str,
    module_id: i64,
    module_id_u64: u64,
    module_row: &modules::ModuleRow,
    bytes: &[u8],
    wasm_sha256_bytes: &[u8; 32],
    opts: &LoadOptions,
    extension_oid: pg_sys::Oid,
    effective: &EffectivePolicy,
    policy_json: Value,
    limits_json: Value,
) -> Result<bool> {
    let wasm_engine = engine::try_shared_engine()?;
    let _loaded = runtime_core::compile(wasm_engine, bytes)?;

    let new_specs = plan_core_export_proc_specs(bytes)?;
    let new_by_wasm: HashMap<String, &ProcSpec> = new_specs
        .iter()
        .map(|(spec, wasm)| (wasm.clone(), spec))
        .collect();

    let old_exports = exports::list_by_module(module_id)?;
    let old_by_wasm: HashMap<String, &exports::ExportRow> = old_exports
        .iter()
        .map(|row| (row.wasm_name.clone(), row))
        .collect();

    plan_reload_exports_core(&old_by_wasm, &new_by_wasm, opts.breaking_changes_allowed)?;

    let paths = RollbackPaths {
        cwasm: artifacts::module_cwasm_path(module_id_u64)?,
        wasm: artifacts::module_wasm_path(module_id_u64)?,
        wit: artifacts::world_wit_path(module_id_u64)?,
    };
    let snapshot = snapshot_artifacts(&paths)?;
    artifacts::write_module_wasm(module_id_u64, bytes)?;
    let wasm_dir = artifacts::module_dir(module_id_u64)?;
    artifacts::write_checksum(&wasm_dir, wasm_sha256_bytes)?;
    register_artifact_rollback(snapshot, paths);

    // CatalogLock: export SPI for core reload only.
    shmem::with_catalog_lock_exclusive(|| -> Result<()> {
        apply_export_changes_core(module_id, extension_oid, opts, &old_exports, &new_specs)
    })?;

    let wasm_path = artifacts::module_wasm_path(module_id_u64)?;
    let next_generation = module_row.generation.saturating_add(1);
    let updated = modules::NewModule {
        abi: "core".to_string(),
        artifact_path: wasm_path.display().to_string(),
        digest: wasm_sha256_bytes.to_vec(),
        generation: next_generation,
        limits: limits_json,
        name: module_name.to_string(),
        origin: module_row.origin.clone(),
        policy: policy_json,
        wasm_sha256: wasm_sha256_bytes.to_vec(),
        wit_world: String::new(),
    };
    shmem::with_catalog_lock_exclusive(|| -> Result<()> {
        let Some(_row) = modules::update(module_id, &updated)? else {
            return Err(PgWasmError::Internal(
                "catalog update after core reload returned no row".to_string(),
            ));
        };
        Ok(())
    })?;

    finish_reload_after_catalog_commit(module_id_u64, module_id, effective)?;

    Ok(true)
}

fn finish_reload_after_catalog_commit(
    module_id_u64: u64,
    module_id: i64,
    effective: &EffectivePolicy,
) -> Result<()> {
    pool::drain(module_id_u64)?;

    if exports::get_by_module_and_wasm_name(module_id, ON_RECONFIGURE_WASM_NAME)?.is_some() {
        hooks::on_reconfigure(module_id_u64, effective)?;
    }

    shmem::bump_generation(module_id_u64);
    Ok(())
}

fn map_udt_error_to_breaking(err: PgWasmError, breaking_allowed: bool) -> PgWasmError {
    if breaking_allowed {
        return err;
    }
    match &err {
        PgWasmError::InvalidConfiguration(msg) => {
            let lower = msg.to_ascii_lowercase();
            if lower.contains("remove") || lower.contains("reorder") || lower.contains("field") {
                return BreakingChange::RecordFieldRemovedOrReordered {
                    type_key: "wit-type".to_string(),
                }
                .into_error();
            }
            if lower.contains("enum") && lower.contains("case") {
                return BreakingChange::EnumValueRemoved {
                    type_key: "wit-enum".to_string(),
                }
                .into_error();
            }
            err
        }
        _ => err,
    }
}

fn plan_reload_exports(
    old_by_wasm: &HashMap<String, &exports::ExportRow>,
    new_by_wasm: &HashMap<String, &ProcSpec>,
    decoded: &world::DecodedWorld,
    breaking_allowed: bool,
) -> Result<()> {
    for (wasm_name, old_row) in old_by_wasm {
        let Some(new_spec) = new_by_wasm.get(wasm_name) else {
            if !breaking_allowed {
                return Err(BreakingChange::ExportRemoved {
                    wasm_name: wasm_name.clone(),
                }
                .into_error());
            }
            continue;
        };
        let new_sig = export_signature_json(decoded, wasm_name)?;
        if old_row.signature != new_sig && !breaking_allowed {
            return Err(BreakingChange::ExportSignatureChanged {
                wasm_name: wasm_name.clone(),
            }
            .into_error());
        }
        if (old_row.arg_types != new_spec.arg_types
            || old_row.ret_type != norm_ret(new_spec.ret_type))
            && !breaking_allowed
        {
            return Err(BreakingChange::ExportSignatureChanged {
                wasm_name: wasm_name.clone(),
            }
            .into_error());
        }
    }

    Ok(())
}

fn plan_reload_exports_core(
    old_by_wasm: &HashMap<String, &exports::ExportRow>,
    new_by_wasm: &HashMap<String, &ProcSpec>,
    breaking_allowed: bool,
) -> Result<()> {
    for (wasm_name, old_row) in old_by_wasm {
        let Some(new_spec) = new_by_wasm.get(wasm_name) else {
            if !breaking_allowed {
                return Err(BreakingChange::ExportRemoved {
                    wasm_name: wasm_name.clone(),
                }
                .into_error());
            }
            continue;
        };
        let new_sig = json!({"abi": "core", "export": wasm_name});
        if old_row.signature != new_sig && !breaking_allowed {
            return Err(BreakingChange::ExportSignatureChanged {
                wasm_name: wasm_name.clone(),
            }
            .into_error());
        }
        if (old_row.arg_types != new_spec.arg_types
            || old_row.ret_type != norm_ret(new_spec.ret_type))
            && !breaking_allowed
        {
            return Err(BreakingChange::ExportSignatureChanged {
                wasm_name: wasm_name.clone(),
            }
            .into_error());
        }
    }

    Ok(())
}

fn norm_ret(ret: pg_sys::Oid) -> Option<pg_sys::Oid> {
    if ret == pg_sys::InvalidOid || ret == pg_sys::VOIDOID {
        None
    } else {
        Some(ret)
    }
}

fn apply_export_changes(
    module_id: i64,
    extension_oid: pg_sys::Oid,
    opts: &LoadOptions,
    old_exports: &[exports::ExportRow],
    new_specs: &[(ProcSpec, String)],
    decoded: &world::DecodedWorld,
) -> Result<()> {
    let old_by_wasm: HashMap<String, &exports::ExportRow> = old_exports
        .iter()
        .map(|row| (row.wasm_name.clone(), row))
        .collect();
    let new_by_wasm: HashMap<String, &ProcSpec> = new_specs
        .iter()
        .map(|(spec, wasm)| (wasm.clone(), spec))
        .collect();

    for old_row in old_exports {
        if !new_by_wasm.contains_key(&old_row.wasm_name) {
            let Some(fn_oid) = old_row.fn_oid else {
                continue;
            };
            proc_reg::unregister(fn_oid)?;
            exports::delete(old_row.export_id)?;
        }
    }

    for (spec, wasm_name) in new_specs {
        if let Some(old_row) = old_by_wasm.get(wasm_name) {
            let new_sig = export_signature_json(decoded, wasm_name)?;
            let sig_changed = old_row.signature != new_sig;
            let shape_changed =
                old_row.arg_types != spec.arg_types || old_row.ret_type != norm_ret(spec.ret_type);
            if sig_changed || shape_changed {
                let Some(old_oid) = old_row.fn_oid else {
                    return Err(PgWasmError::Internal(
                        "export row missing fn_oid".to_string(),
                    ));
                };
                proc_reg::unregister(old_oid)?;
                let fn_oid = proc_reg::register(spec, extension_oid, true)?;
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: Some(fn_oid),
                    kind: "function".to_string(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature: new_sig,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(old_row.export_id, &updated)?;
            } else {
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: old_row.fn_oid,
                    kind: old_row.kind.clone(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature: new_sig,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(old_row.export_id, &updated)?;
            }
        } else if let Some(existing) = exports::get_by_module_and_wasm_name(module_id, wasm_name)? {
            // `old_by_wasm` can miss rows when SPI snapshots diverge (e.g. internal subtransactions);
            // never insert a second `(module_id, wasm_name)` row.
            let new_sig = export_signature_json(decoded, wasm_name)?;
            let sig_changed = existing.signature != new_sig;
            let shape_changed = existing.arg_types != spec.arg_types
                || existing.ret_type != norm_ret(spec.ret_type);
            if sig_changed || shape_changed {
                let Some(old_oid) = existing.fn_oid else {
                    return Err(PgWasmError::Internal(
                        "export row missing fn_oid".to_string(),
                    ));
                };
                proc_reg::unregister(old_oid)?;
                let fn_oid = proc_reg::register(spec, extension_oid, true)?;
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: Some(fn_oid),
                    kind: "function".to_string(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature: new_sig,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(existing.export_id, &updated)?;
            } else {
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: existing.fn_oid,
                    kind: existing.kind.clone(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature: new_sig,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(existing.export_id, &updated)?;
            }
        } else {
            let fn_oid = proc_reg::register(
                spec,
                extension_oid,
                opts.replace_exports || opts.breaking_changes_allowed,
            )?;
            let signature = export_signature_json(decoded, wasm_name)?;
            exports::insert(&exports::NewExport {
                arg_types: spec.arg_types.clone(),
                fn_oid: Some(fn_oid),
                kind: "function".to_string(),
                module_id,
                ret_type: norm_ret(spec.ret_type),
                signature,
                sql_name: spec.name.clone(),
                wasm_name: wasm_name.clone(),
            })?;
        }
    }

    Ok(())
}

fn apply_export_changes_core(
    module_id: i64,
    extension_oid: pg_sys::Oid,
    opts: &LoadOptions,
    old_exports: &[exports::ExportRow],
    new_specs: &[(ProcSpec, String)],
) -> Result<()> {
    let old_by_wasm: HashMap<String, &exports::ExportRow> = old_exports
        .iter()
        .map(|row| (row.wasm_name.clone(), row))
        .collect();
    let new_by_wasm: HashMap<String, &ProcSpec> = new_specs
        .iter()
        .map(|(spec, wasm)| (wasm.clone(), spec))
        .collect();

    for old_row in old_exports {
        if !new_by_wasm.contains_key(&old_row.wasm_name) {
            let Some(fn_oid) = old_row.fn_oid else {
                continue;
            };
            proc_reg::unregister(fn_oid)?;
            exports::delete(old_row.export_id)?;
        }
    }

    for (spec, wasm_name) in new_specs {
        let signature = json!({"abi": "core", "export": wasm_name});
        if let Some(old_row) = old_by_wasm.get(wasm_name) {
            let shape_changed =
                old_row.arg_types != spec.arg_types || old_row.ret_type != norm_ret(spec.ret_type);
            if old_row.signature != signature || shape_changed {
                let Some(old_oid) = old_row.fn_oid else {
                    return Err(PgWasmError::Internal(
                        "export row missing fn_oid".to_string(),
                    ));
                };
                proc_reg::unregister(old_oid)?;
                let fn_oid = proc_reg::register(spec, extension_oid, true)?;
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: Some(fn_oid),
                    kind: "function".to_string(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(old_row.export_id, &updated)?;
            } else {
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: old_row.fn_oid,
                    kind: old_row.kind.clone(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(old_row.export_id, &updated)?;
            }
        } else if let Some(existing) = exports::get_by_module_and_wasm_name(module_id, wasm_name)? {
            let shape_changed = existing.arg_types != spec.arg_types
                || existing.ret_type != norm_ret(spec.ret_type);
            if existing.signature != signature || shape_changed {
                let Some(old_oid) = existing.fn_oid else {
                    return Err(PgWasmError::Internal(
                        "export row missing fn_oid".to_string(),
                    ));
                };
                proc_reg::unregister(old_oid)?;
                let fn_oid = proc_reg::register(spec, extension_oid, true)?;
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: Some(fn_oid),
                    kind: "function".to_string(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(existing.export_id, &updated)?;
            } else {
                let updated = exports::NewExport {
                    arg_types: spec.arg_types.clone(),
                    fn_oid: existing.fn_oid,
                    kind: existing.kind.clone(),
                    module_id,
                    ret_type: norm_ret(spec.ret_type),
                    signature,
                    sql_name: spec.name.clone(),
                    wasm_name: wasm_name.clone(),
                };
                exports::update(existing.export_id, &updated)?;
            }
        } else {
            let fn_oid = proc_reg::register(
                spec,
                extension_oid,
                opts.replace_exports || opts.breaking_changes_allowed,
            )?;
            exports::insert(&exports::NewExport {
                arg_types: spec.arg_types.clone(),
                fn_oid: Some(fn_oid),
                kind: "function".to_string(),
                module_id,
                ret_type: norm_ret(spec.ret_type),
                signature,
                sql_name: spec.name.clone(),
                wasm_name: wasm_name.clone(),
            })?;
        }
    }

    Ok(())
}

fn write_component_wasm_wit(module_id_u64: u64, bytes: &[u8], wit_text: &str) -> Result<()> {
    artifacts::write_module_wasm(module_id_u64, bytes)?;
    artifacts::write_world_wit(module_id_u64, wit_text)?;
    Ok(())
}

fn snapshot_artifacts(paths: &RollbackPaths) -> Result<ArtifactSnapshot> {
    let wasm = fs::read(&paths.wasm).map_err(PgWasmError::Io)?;
    let cwasm = read_optional_file(&paths.cwasm)?;
    let wit = read_optional_file(&paths.wit)?;
    Ok(ArtifactSnapshot { cwasm, wasm, wit })
}

fn read_optional_file(path: &std::path::Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(b)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(PgWasmError::Io(e)),
    }
}

fn register_artifact_rollback(snapshot: ArtifactSnapshot, paths: RollbackPaths) {
    let registered_sub_id = unsafe { pg_sys::GetCurrentSubTransactionId() };
    let snap_abort = snapshot.clone();
    let paths_abort = paths.clone();
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, move || {
        restore_artifacts(&snap_abort, &paths_abort);
    });
    let snap_sub = snapshot;
    let paths_sub = paths;
    register_subxact_callback(PgSubXactCallbackEvent::AbortSub, move |my_subid, _| {
        if my_subid == registered_sub_id {
            restore_artifacts(&snap_sub, &paths_sub);
        }
    });
}

fn restore_artifacts(snapshot: &ArtifactSnapshot, paths: &RollbackPaths) {
    let _ = artifacts::with_artifact_fs_lock(|| {
        let _ = artifacts::write_atomic(&paths.wasm, &snapshot.wasm);
        match &snapshot.cwasm {
            Some(bytes) => {
                let _ = artifacts::write_atomic(&paths.cwasm, bytes);
            }
            None => {
                let _ = fs::remove_file(&paths.cwasm);
            }
        }
        match &snapshot.wit {
            Some(bytes) => {
                let _ = artifacts::write_atomic(&paths.wit, bytes);
            }
            None => {
                let _ = fs::remove_file(&paths.wit);
            }
        }
        Ok::<(), std::io::Error>(())
    });
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
                    'pgwasm_loader'::regrole,
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
    .map_err(|e| PgWasmError::Internal(format!("authz SPI: {e}")))?;

    if allowed {
        Ok(())
    } else {
        Err(PgWasmError::PermissionDenied(
            "pgwasm.pgwasm_reload requires superuser or membership in role `pgwasm_loader`"
                .to_string(),
        ))
    }
}

fn parse_reload_options(options: Option<pgrx::Json>) -> Result<LoadOptions> {
    let map = match options {
        None => return Ok(LoadOptions::default()),
        Some(j) => match j.0 {
            Value::Object(m) => m,
            _ => {
                return Err(PgWasmError::InvalidConfiguration(
                    "options must be a JSON object when provided".to_string(),
                ));
            }
        },
    };

    let mut out = LoadOptions::default();
    for (k, v) in map {
        match k.as_str() {
            "abi" => {
                let s = json_string(&v, "abi")?;
                out.abi = Some(match s.as_str() {
                    "component" => OptionsAbi::Component,
                    "core" => OptionsAbi::Core,
                    other => {
                        return Err(PgWasmError::InvalidConfiguration(format!(
                            "options.abi must be \"component\" or \"core\", got `{other}`"
                        )));
                    }
                });
            }
            "breaking_changes_allowed" => {
                out.breaking_changes_allowed = json_bool(&v, "breaking_changes_allowed")?;
            }
            "limits" => {
                out.limits = Some(serde_json::from_value(v).map_err(|e| {
                    PgWasmError::InvalidConfiguration(format!("options.limits: {e}"))
                })?);
            }
            "overrides" => {
                out.overrides = Some(serde_json::from_value(v).map_err(|e| {
                    PgWasmError::InvalidConfiguration(format!("options.overrides: {e}"))
                })?);
            }
            "replace_exports" => out.replace_exports = json_bool(&v, "replace_exports")?,
            _ => {}
        }
    }
    Ok(out)
}

fn json_string(v: &Value, field: &str) -> Result<String> {
    v.as_str()
        .map(str::to_owned)
        .ok_or_else(|| PgWasmError::InvalidConfiguration(format!("`{field}` must be a string")))
}

fn json_bool(v: &Value, field: &str) -> Result<bool> {
    v.as_bool()
        .ok_or_else(|| PgWasmError::InvalidConfiguration(format!("`{field}` must be a boolean")))
}

fn merge_catalog_patch(base: &Value, overrides: Option<&PolicyOverrides>) -> Result<Value> {
    let Some(p) = overrides else {
        return Ok(base.clone());
    };
    merge_json_objects_reload(base, Some(pgrx::Json(catalog_policy_json(Some(p))?)))
}

fn merge_limits_patch(base: &Value, limits: Option<&crate::config::Limits>) -> Result<Value> {
    let Some(lim) = limits else {
        return Ok(base.clone());
    };
    let patch = serde_json::to_value(lim)
        .map_err(|e| PgWasmError::Internal(format!("serialize limits patch: {e}")))?;
    merge_json_objects_reload(base, Some(pgrx::Json(patch)))
}

/// Same merge semantics as [`super::reconfigure::merge_json_objects`], duplicated here because
/// `reload` is the only lifecycle module this wave may edit.
fn merge_json_objects_reload(
    base: &Value,
    patch: Option<pgrx::Json>,
) -> core::result::Result<Value, PgWasmError> {
    let mut out = base.as_object().cloned().unwrap_or_default();
    if let Some(pgrx::Json(patch_value)) = patch {
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

fn reload_read_module_bytes(bytes_or_path: &pgrx::Json) -> Result<Vec<u8>> {
    match &bytes_or_path.0 {
        Value::Object(map) => {
            if map.contains_key("bytes") {
                return reload_decode_bytea_json(map.get("bytes").ok_or_else(|| {
                    PgWasmError::InvalidConfiguration("bytes key missing".to_string())
                })?);
            }
            if map.contains_key("path") {
                return reload_read_path_payload(map.get("path").ok_or_else(|| {
                    PgWasmError::InvalidConfiguration("path key missing".to_string())
                })?);
            }
            Err(PgWasmError::InvalidConfiguration(
                "bytes_or_path must be an object with `bytes` (bytea) or `path` (text)".to_string(),
            ))
        }
        _ => Err(PgWasmError::InvalidConfiguration(
            "bytes_or_path must be a JSON object".to_string(),
        )),
    }
}

fn reload_decode_bytea_json(v: &Value) -> Result<Vec<u8>> {
    if let Value::String(hex) = v
        && hex.len() % 2 == 0
        && hex.chars().all(|c| c.is_ascii_hexdigit())
    {
        return reload_decode_hex(hex);
    }
    if let Some(bytes) = v.as_array() {
        let mut out = Vec::with_capacity(bytes.len());
        for item in bytes {
            let n = item.as_u64().ok_or_else(|| {
                PgWasmError::InvalidConfiguration(
                    "bytes array must contain u8 elements".to_string(),
                )
            })?;
            let b = u8::try_from(n).map_err(|_| {
                PgWasmError::InvalidConfiguration("bytes array element out of u8 range".to_string())
            })?;
            out.push(b);
        }
        return Ok(out);
    }
    Err(PgWasmError::InvalidConfiguration(
        "bytes must be JSON bytea from PostgreSQL or a JSON array of integers".to_string(),
    ))
}

fn reload_decode_hex(hex: &str) -> Result<Vec<u8>> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<core::result::Result<Vec<_>, _>>()
        .map_err(|_| PgWasmError::InvalidConfiguration("invalid hex in bytes payload".to_string()))
}

fn reload_read_path_payload(v: &Value) -> Result<Vec<u8>> {
    if !guc::ALLOW_LOAD_FROM_FILE.get() {
        return Err(PgWasmError::PermissionDenied(
            "loading from filesystem path is disabled; set pgwasm.allow_load_from_file".to_string(),
        ));
    }
    let path_str = v.as_str().ok_or_else(|| {
        PgWasmError::InvalidConfiguration("path must be a JSON string".to_string())
    })?;
    let path = reload_resolve_load_path(path_str);
    let canonical = reload_resolve_canonical_under_policy(&path)?;
    reload_enforce_allowed_prefixes(&canonical)?;
    let meta = fs::metadata(&canonical)?;
    let len = meta.len();
    let max = u64::try_from(guc::MAX_MODULE_BYTES.get().max(0))
        .map_err(|_| PgWasmError::Internal("max_module_bytes overflow".to_string()))?;
    if len > max {
        return Err(PgWasmError::ResourceLimitExceeded(format!(
            "module file size {len} exceeds pgwasm.max_module_bytes ({max})"
        )));
    }
    let bytes = fs::read(&canonical)?;
    if u64::try_from(bytes.len())
        .map_err(|_| PgWasmError::Internal("length overflow".to_string()))?
        != len
    {
        return Err(PgWasmError::InvalidModule(
            "file size changed while reading".to_string(),
        ));
    }
    Ok(bytes)
}

fn reload_resolve_load_path(path_str: &str) -> PathBuf {
    let raw = Path::new(path_str);
    if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        let base = guc::MODULE_PATH
            .get()
            .map(|c| c.to_string_lossy().into_owned())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        base.join(raw)
    }
}

fn reload_resolve_canonical_under_policy(path: &Path) -> io::Result<PathBuf> {
    if guc::FOLLOW_SYMLINKS.get() {
        return fs::canonicalize(path);
    }

    let mut out = PathBuf::new();
    let mut comps = path.components().peekable();
    if let Some(c) = comps.peek() {
        match c {
            Component::Prefix(p) => {
                out.push(Component::Prefix(*p));
                let _ = comps.next();
            }
            Component::RootDir => {
                out.push(Component::RootDir);
                let _ = comps.next();
            }
            _ => {}
        }
    }

    for comp in comps {
        match comp {
            Component::Prefix(_) | Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = out.pop();
            }
            Component::Normal(name) => {
                out.push(name);
                let meta = fs::symlink_metadata(&out)?;
                if meta.is_symlink() {
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "path `{}` traverses a symlink while pgwasm.follow_symlinks is off",
                            out.display()
                        ),
                    ));
                }
            }
        }
    }

    if out.as_os_str().is_empty() {
        fs::canonicalize(path)
    } else {
        fs::canonicalize(&out)
    }
}

fn reload_enforce_allowed_prefixes(canonical: &Path) -> Result<()> {
    let prefixes = guc::ALLOWED_PATH_PREFIXES
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();

    if prefixes.is_empty() {
        return Err(PgWasmError::InvalidConfiguration(
            "pgwasm.allowed_path_prefixes must list at least one canonical prefix for file loads"
                .to_string(),
        ));
    }

    let canon_str = canonical.to_str().ok_or_else(|| {
        PgWasmError::InvalidConfiguration("canonical path is not valid UTF-8".to_string())
    })?;

    let sep = std::path::MAIN_SEPARATOR;
    let ok = prefixes.iter().any(|prefix| {
        prefix
            .to_str()
            .is_some_and(|p| canon_str == p || canon_str.starts_with(&format!("{p}{sep}")))
    });

    if !ok {
        return Err(PgWasmError::PermissionDenied(format!(
            "resolved path `{}` is not under any entry in pgwasm.allowed_path_prefixes",
            canonical.display()
        )));
    }
    Ok(())
}

/// Catalog JSON can contain explicit `null` for optional serde fields; `reconfigure::limits_from_value`
/// / `policy_overrides_from_value` expect absent keys or concrete scalars.
fn json_without_null_entries(value: &Value) -> Result<Value> {
    let Some(obj) = value.as_object() else {
        return Ok(value.clone());
    };
    let cleaned: serde_json::Map<String, Value> = obj
        .iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(Value::Object(cleaned))
}

fn catalog_policy_json(overrides: Option<&PolicyOverrides>) -> Result<Value> {
    let Some(p) = overrides else {
        return Ok(json!({}));
    };
    let mut m = serde_json::Map::new();
    if let Some(v) = p.allow_spi {
        m.insert("allow_spi".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi {
        m.insert("allow_wasi".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_env {
        m.insert("allow_wasi_env".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_fs {
        m.insert("allow_wasi_fs".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_http {
        m.insert("allow_wasi_http".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_net {
        m.insert("allow_wasi_net".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_stdio {
        m.insert("allow_wasi_stdio".to_string(), json!(v));
    }
    if let Some(ref hosts) = p.allowed_hosts {
        m.insert("allowed_hosts".to_string(), json!(hosts));
    }
    if let Some(ref pre) = p.wasi_preopens {
        let map: serde_json::Map<String, Value> =
            pre.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
        m.insert("wasi_preopens".to_string(), Value::Object(map));
    }
    Ok(Value::Object(m))
}

fn extension_oid() -> Result<pg_sys::Oid> {
    let oid = unsafe { pg_sys::get_extension_oid(c"pgwasm".as_ptr(), false) };
    if oid == pg_sys::InvalidOid {
        return Err(PgWasmError::Internal(
            "extension `pgwasm` oid lookup failed".to_string(),
        ));
    }
    Ok(oid)
}

fn world_exports_function_named(decoded: &world::DecodedWorld, wasm_name: &str) -> bool {
    let Some(world) = decoded.resolve.worlds.get(decoded.world_id) else {
        return false;
    };
    for item in world.exports.values() {
        match item {
            WorldItem::Function(f) if f.name == wasm_name => return true,
            WorldItem::Interface { id, .. } => {
                if let Some(iface) = decoded.resolve.interfaces.get(*id) {
                    for (n, f) in &iface.functions {
                        if n == wasm_name || export_wasm_name(&decoded.resolve, f) == wasm_name {
                            return true;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    false
}

fn export_wasm_name(resolve: &wit_parser::Resolve, func: &Function) -> String {
    match &func.kind {
        FunctionKind::Freestanding | FunctionKind::AsyncFreestanding => func.name.clone(),
        FunctionKind::Method(id) | FunctionKind::AsyncMethod(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}#{}", func.name)
        }
        FunctionKind::Static(id) | FunctionKind::AsyncStatic(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}!{}", func.name)
        }
        FunctionKind::Constructor(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}#{type_name}")
        }
    }
}

fn type_name_for_id(resolve: &wit_parser::Resolve, type_id: wit_parser::TypeId) -> String {
    resolve.types[type_id]
        .name
        .clone()
        .unwrap_or_else(|| format!("type{}", type_id.index()))
}

fn export_signature_json(decoded: &world::DecodedWorld, wasm_export: &str) -> Result<Value> {
    let world = decoded
        .resolve
        .worlds
        .get(decoded.world_id)
        .ok_or_else(|| PgWasmError::InvalidModule("decoded world missing".to_string()))?;

    let mut func: Option<&Function> = None;
    for item in world.exports.values() {
        match item {
            WorldItem::Function(f) => {
                let w = export_wasm_name(&decoded.resolve, f);
                if w == wasm_export {
                    func = Some(f);
                    break;
                }
            }
            WorldItem::Interface { id, .. } => {
                let Some(iface) = decoded.resolve.interfaces.get(*id) else {
                    continue;
                };
                for f in iface.functions.values() {
                    let w = export_wasm_name(&decoded.resolve, f);
                    if w == wasm_export {
                        func = Some(f);
                        break;
                    }
                }
            }
            WorldItem::Type { .. } => {}
        }
        if func.is_some() {
            break;
        }
    }

    let func = func.ok_or_else(|| {
        PgWasmError::Internal(format!(
            "could not locate WIT function `{wasm_export}` for signature JSON"
        ))
    })?;

    let params: Vec<Value> = func
        .params
        .iter()
        .map(|p| json!({"name": p.name, "type": format!("{:?}", p.ty)}))
        .collect();
    let result = func.result.map(|t| json!({"type": format!("{t:?}")}));
    Ok(json!({
        "kind": "wit-function",
        "params": params,
        "result": result,
    }))
}

fn plan_export_proc_specs(
    module_prefix: &str,
    module_id: i64,
    decoded: &world::DecodedWorld,
) -> Result<Vec<(ProcSpec, String)>> {
    let world = decoded
        .resolve
        .worlds
        .get(decoded.world_id)
        .ok_or_else(|| PgWasmError::InvalidModule("decoded world missing".to_string()))?;

    let mut out = Vec::new();
    for (export_key, item) in &world.exports {
        let export_key_str = world_key_to_string(export_key);
        match item {
            WorldItem::Function(f) => {
                let wasm = export_wasm_name(&decoded.resolve, f);
                out.push((
                    proc_spec_for_function(
                        module_prefix,
                        &export_key_str,
                        f,
                        module_id,
                        &decoded.resolve,
                    )?,
                    wasm,
                ));
            }
            WorldItem::Interface { id, .. } => {
                let iface = decoded.resolve.interfaces.get(*id).ok_or_else(|| {
                    PgWasmError::InvalidModule("interface id missing".to_string())
                })?;
                for (func_key, func) in &iface.functions {
                    let sql_key = format!("{export_key_str}/{func_key}");
                    let wasm = export_wasm_name(&decoded.resolve, func);
                    out.push((
                        proc_spec_for_function(
                            module_prefix,
                            &sql_key,
                            func,
                            module_id,
                            &decoded.resolve,
                        )?,
                        wasm,
                    ));
                }
            }
            WorldItem::Type { .. } => {}
        }
    }

    out.sort_by(|a, b| a.0.name.cmp(&b.0.name));
    dedupe_exports_by_wasm_name(out)
}

/// Collapse duplicate `wasm_name` entries (different WIT export keys can map to the same core
/// export). If two specs disagree on argument or result types, the module is ambiguous.
pub(crate) fn dedupe_exports_by_wasm_name(
    v: Vec<(ProcSpec, String)>,
) -> Result<Vec<(ProcSpec, String)>> {
    let mut by_wasm: HashMap<String, Vec<ProcSpec>> = HashMap::new();
    for (spec, wasm) in v {
        by_wasm.entry(wasm).or_default().push(spec);
    }

    let mut out: Vec<(ProcSpec, String)> = Vec::with_capacity(by_wasm.len());
    for (wasm, mut specs) in by_wasm {
        if specs.len() == 1 {
            let spec = specs.pop().ok_or_else(|| {
                PgWasmError::Internal("dedupe_exports_by_wasm_name: empty group".to_string())
            })?;
            out.push((spec, wasm));
            continue;
        }

        specs.sort_by(|a, b| a.name.cmp(&b.name));
        let base = specs
            .first()
            .ok_or_else(|| PgWasmError::Internal("dedupe_exports_by_wasm_name: empty".into()))?;
        for s in specs.iter().skip(1) {
            if base.arg_types != s.arg_types || base.ret_type != s.ret_type {
                return Err(PgWasmError::InvalidModule(format!(
                    "duplicate wasm export `{wasm}` with conflicting SQL signatures"
                )));
            }
        }

        let chosen = specs.into_iter().next().ok_or_else(|| {
            PgWasmError::Internal("dedupe_exports_by_wasm_name: missing representative".to_string())
        })?;
        out.push((chosen, wasm));
    }

    out.sort_by(|a, b| a.0.name.cmp(&b.0.name));
    Ok(out)
}

fn world_key_to_string(key: &WorldKey) -> String {
    match key {
        WorldKey::Name(n) => n.clone(),
        WorldKey::Interface(id) => format!("interface-{}", id.index()),
    }
}

fn proc_spec_for_function(
    module_prefix: &str,
    export_key: &str,
    func: &Function,
    module_id: i64,
    resolve: &wit_parser::Resolve,
) -> Result<ProcSpec> {
    let sql_name = sanitize_sql_identifier(export_key);
    let full_name = format!("{module_prefix}__{sql_name}");
    let mut arg_types = Vec::new();
    for p in &func.params {
        arg_types.push(wit_wasm_type_to_pg_oid(&p.ty, module_id, resolve)?);
    }
    let ret_type = match &func.result {
        None => pg_sys::VOIDOID,
        Some(t) => wit_wasm_type_to_pg_oid(t, module_id, resolve)?,
    };
    let strict = func.params.iter().any(|p| matches!(p.ty, Type::Id(_)));

    Ok(ProcSpec {
        schema: EXTENSION_SCHEMA.to_string(),
        name: full_name,
        arg_types,
        arg_names: Vec::new(),
        arg_modes: Vec::new(),
        ret_type,
        returns_set: false,
        volatility: Volatility::Volatile,
        strict,
        parallel: Parallel::Unsafe,
        cost: Some(100.0),
    })
}

fn wit_wasm_type_to_pg_oid(
    ty: &Type,
    module_id: i64,
    resolve: &wit_parser::Resolve,
) -> Result<pg_sys::Oid> {
    match ty {
        Type::Bool => Ok(pg_sys::BOOLOID),
        Type::S32 => Ok(pg_sys::INT4OID),
        Type::S64 => Ok(pg_sys::INT8OID),
        Type::String => Ok(pg_sys::TEXTOID),
        Type::Id(type_id) => {
            let key = typing::export_type_key_for_id(resolve, *type_id)?;
            let row = wit_types::get_by_module_and_type_key(module_id, &key)?.ok_or_else(|| {
                PgWasmError::Internal(format!(
                    "WIT type `{key}` is not registered in catalog; register UDTs before exports"
                ))
            })?;
            Ok(row.pg_type_oid)
        }
        other => Err(PgWasmError::Unsupported(format!(
            "WIT type `{other:?}` is not supported for automatic SQL export registration in this build"
        ))),
    }
}

fn sanitize_sql_identifier(s: &str) -> String {
    let mut t = s.replace(['/', '-'], "_");
    if t.is_empty() {
        t = "export".to_string();
    }
    if !t
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        t = format!("e_{t}");
    }
    t
}

fn plan_core_export_proc_specs(bytes: &[u8]) -> Result<Vec<(ProcSpec, String)>> {
    let mut export_list = Vec::<(String, u32)>::new();
    for payload in Parser::new(0).parse_all(bytes) {
        let Ok(payload) = payload else {
            continue;
        };
        if let Payload::ExportSection(reader) = payload {
            for rec in reader.into_iter() {
                let Ok(exp) = rec else { continue };
                if exp.kind == ExternalKind::Func {
                    export_list.push((exp.name.to_string(), exp.index));
                }
            }
        }
    }

    let mut type_map: HashMap<u32, (Vec<ValType>, Option<ValType>)> = HashMap::new();
    let mut type_index: u32 = 0;
    for payload in Parser::new(0).parse_all(bytes) {
        let Ok(payload) = payload else {
            continue;
        };
        if let Payload::TypeSection(reader) = payload {
            for group in reader.into_iter() {
                let Ok(group) = group else { continue };
                for st in group.types() {
                    if let CompositeInnerType::Func(ft) = &st.composite_type.inner {
                        let params: Vec<_> = ft.params().to_vec();
                        let ret = ft.results().iter().copied().next();
                        type_map.insert(type_index, (params, ret));
                    }
                    type_index = type_index.saturating_add(1);
                }
            }
        }
    }

    let mut func_types: HashMap<u32, u32> = HashMap::new();
    for payload in Parser::new(0).parse_all(bytes) {
        let Ok(payload) = payload else {
            continue;
        };
        if let Payload::FunctionSection(reader) = payload {
            for (i, ty) in reader.into_iter().enumerate() {
                if let Ok(ty) = ty {
                    func_types.insert(i as u32, ty);
                }
            }
        }
    }

    let mut out = Vec::new();
    for (name, func_idx) in export_list {
        let Some(&type_idx) = func_types.get(&func_idx) else {
            continue;
        };
        let Some((params, ret)) = type_map.get(&type_idx) else {
            continue;
        };
        if params.iter().all(|p| *p == ValType::I32) && matches!(ret, None | Some(ValType::I32)) {
            let arg_types = vec![pg_sys::INT4OID; params.len()];
            let ret_type = match ret {
                None => pg_sys::VOIDOID,
                Some(_) => pg_sys::INT4OID,
            };
            let sql_name = sanitize_sql_identifier(&name);
            out.push((
                ProcSpec {
                    schema: EXTENSION_SCHEMA.to_string(),
                    name: format!("core__{sql_name}"),
                    arg_types,
                    arg_names: Vec::new(),
                    arg_modes: Vec::new(),
                    ret_type,
                    returns_set: false,
                    volatility: Volatility::Volatile,
                    strict: false,
                    parallel: Parallel::Unsafe,
                    cost: Some(100.0),
                },
                name,
            ));
        }
    }

    if out.is_empty() {
        return Err(PgWasmError::Unsupported(
            "core module has no func exports with (i32,...)->i32 signature supported for load in this build"
                .to_string(),
        ));
    }

    out.sort_by(|a, b| a.0.name.cmp(&b.0.name));
    Ok(out)
}
