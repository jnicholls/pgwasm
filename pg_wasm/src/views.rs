//! Table functions for module catalog and WASM call metrics (plan §7–8).
//!
//! These return `TABLE (...)` and can be queried like `SELECT * FROM pg_wasm_modules()`.

use std::collections::HashMap;

use pgrx::{pg_sys, prelude::*};

use crate::registry::{self, ModuleId, iter_fn_oid_entries, list_module_catalog};

/// One row per loaded module: identity, ABI, policy JSON, sampled guest memory, and summed export stats.
#[pg_extern]
fn pg_wasm_modules() -> TableIterator<
    'static,
    (
        name!(module_id, i64),
        name!(module_name, String),
        name!(abi, String),
        name!(runtime, String),
        name!(needs_wasi, bool),
        name!(policy_overrides, String),
        name!(guest_memory_peak_bytes, Option<i64>),
        name!(total_invocations, i64),
        name!(total_errors, i64),
        name!(total_time_ms, f64),
    ),
> {
    let mut inv_by_mod: HashMap<ModuleId, u64> = HashMap::new();
    let mut err_by_mod: HashMap<ModuleId, u64> = HashMap::new();
    let mut time_ns_by_mod: HashMap<ModuleId, u64> = HashMap::new();

    for (_, reg) in iter_fn_oid_entries() {
        let m = reg.module_id;
        let st = &reg.metrics;
        *inv_by_mod.entry(m).or_insert(0) += st.invocations();
        *err_by_mod.entry(m).or_insert(0) += st.errors();
        *time_ns_by_mod.entry(m).or_insert(0) += st.total_time_ns();
    }

    let rows: Vec<_> = list_module_catalog()
        .into_iter()
        .map(|(mid, cat)| {
            let abi = registry::module_abi(mid)
                .map(|k| k.as_label().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let needs_wasi = registry::module_needs_wasi(mid).unwrap_or(false);
            let policy = registry::module_policy_overrides(mid)
                .map(|p| p.to_json_string())
                .unwrap_or_else(|| "{}".to_string());
            let mem = crate::metrics::guest_memory_peak_bytes(mid).map(|b| b as i64);
            let inv = *inv_by_mod.get(&mid).unwrap_or(&0) as i64;
            let err = *err_by_mod.get(&mid).unwrap_or(&0) as i64;
            let tns = *time_ns_by_mod.get(&mid).unwrap_or(&0) as f64;
            let tms = tns / 1_000_000.0;
            (
                mid.0,
                cat.name_prefix,
                abi,
                cat.runtime,
                needs_wasi,
                policy,
                mem,
                inv,
                err,
                tms,
            )
        })
        .collect();
    TableIterator::new(rows)
}

/// One row per dynamically registered SQL function backed by WASM.
#[pg_extern]
fn pg_wasm_functions() -> TableIterator<
    'static,
    (
        name!(module_id, i64),
        name!(sql_function_name, String),
        name!(wasm_export_name, String),
        name!(fn_oid, pg_sys::Oid),
    ),
> {
    let rows: Vec<_> = iter_fn_oid_entries()
        .into_iter()
        .filter_map(|(oid, reg)| {
            let sql_name = registry::module_catalog(reg.module_id)
                .map(|c| format!("{}_{}", c.name_prefix, reg.export_name))?;
            Some((reg.module_id.0, sql_name, reg.export_name, oid))
        })
        .collect();
    TableIterator::new(rows)
}

/// Per-export invocation counts, errors, and timings (this backend process only).
#[pg_extern]
fn pg_wasm_stats() -> TableIterator<
    'static,
    (
        name!(module_id, i64),
        name!(wasm_export_name, String),
        name!(invocations, i64),
        name!(errors, i64),
        name!(total_time_ms, f64),
        name!(avg_time_ms, Option<f64>),
    ),
> {
    let rows: Vec<_> = iter_fn_oid_entries()
        .into_iter()
        .map(|(_, reg)| {
            let inv = reg.metrics.invocations();
            let err = reg.metrics.errors();
            let tns = reg.metrics.total_time_ns() as f64;
            let tms = tns / 1_000_000.0;
            let avg = if inv > 0 {
                Some(tms / inv as f64)
            } else {
                None
            };
            (
                reg.module_id.0,
                reg.export_name,
                inv as i64,
                err as i64,
                tms,
                avg,
            )
        })
        .collect();
    TableIterator::new(rows)
}
