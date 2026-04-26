//! Trampoline entrypoint dispatch helpers.

use std::sync::Arc;

use pgrx::FromDatum;
use pgrx::fcinfo::{pg_arg_is_null, pg_get_nullable_datum, pg_getarg_type};
use pgrx::pg_guard;
use pgrx::pg_sys::{self, Datum, FunctionCallInfo, Oid, Pg_finfo_record};
use pgrx::prelude::PgLogLevel;
use wasmtime::component::Val;
use wasmtime::{Store, StoreLimits, StoreLimitsBuilder};

use crate::artifacts;
use crate::catalog::{exports, modules};
use crate::errors::{PgWasmError, map_wasmtime_err};
use crate::guc;
use crate::mapping::composite::{self, Export, ExportSlot, MarshalPlan, plan_marshaler};
use crate::policy::{self, EffectivePolicy, GucSnapshot};
use crate::registry;
use crate::runtime::component::{self, StoreCtx};
use crate::runtime::core;
use crate::runtime::engine;
use crate::runtime::pool;
use crate::shmem::{self, ExportCounterKind};
use crate::wit::typing::{PgType, TypePlan};

#[pg_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn pg_wasm_udf_trampoline(fcinfo: FunctionCallInfo) -> Datum {
    unsafe { trampoline_impl(fcinfo) }
}

#[doc(hidden)]
#[unsafe(no_mangle)]
pub extern "C" fn pg_finfo_pg_wasm_udf_trampoline() -> &'static Pg_finfo_record {
    const V1_API: Pg_finfo_record = Pg_finfo_record { api_version: 1 };
    &V1_API
}

unsafe fn trampoline_impl(fcinfo: FunctionCallInfo) -> Datum {
    match trampoline_inner(fcinfo) {
        Ok(datum) => datum,
        Err(err) => {
            err.into_error_report().report(PgLogLevel::ERROR);
            // SAFETY: `report(ERROR)` does not return to the caller.
            unsafe { std::hint::unreachable_unchecked() }
        }
    }
}

fn trampoline_inner(fcinfo: FunctionCallInfo) -> Result<Datum, PgWasmError> {
    let fn_oid = unsafe { fn_oid_from_fcinfo(fcinfo) };
    if fn_oid == Oid::INVALID {
        return Err(PgWasmError::Internal(
            "pg_wasm_udf_trampoline: missing fn_oid".to_string(),
        ));
    }

    let export_row = resolve_export_for_fn_oid(fn_oid)?;
    let module_row = modules::get_by_id(export_row.module_id)?
        .ok_or_else(|| PgWasmError::NotFound(format!("module_id {}", export_row.module_id)))?;

    let guc_snapshot = GucSnapshot::from_gucs();
    let override_policy = reconfigure::policy_overrides_from_module_json(&module_row.policy)?;
    let override_limits = reconfigure::limits_from_module_json(&module_row.limits)?;
    let effective = policy::resolve(
        &guc_snapshot,
        Some(&override_policy),
        Some(&override_limits),
    )?;

    let module_id = u64::try_from(export_row.module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;
    let export_index = export_index_in_module(export_row.module_id, export_row.export_id)?;

    let wasm_engine = engine::try_shared_engine()?;

    if module_row.abi.eq_ignore_ascii_case("core") {
        invoke_core_export(
            fcinfo,
            &export_row,
            &module_row,
            module_id,
            export_index,
            wasm_engine,
            &effective,
        )
    } else {
        invoke_component_export(
            fcinfo,
            &export_row,
            &module_row,
            module_id,
            export_index,
            wasm_engine,
            &effective,
        )
    }
}

fn resolve_export_for_fn_oid(fn_oid: Oid) -> Result<exports::ExportRow, PgWasmError> {
    if let Some(entry) = registry::resolve_fn_oid(fn_oid)
        && let Some(row) = exports::get_by_fn_oid(entry.fn_oid)?
    {
        return Ok(row);
    }

    registry::refresh_from_catalog();

    if let Some(entry) = registry::resolve_fn_oid(fn_oid)
        && let Some(row) = exports::get_by_fn_oid(entry.fn_oid)?
    {
        return Ok(row);
    }

    if let Some(row) = exports::get_by_fn_oid(fn_oid)? {
        return Ok(row);
    }

    Err(PgWasmError::NotFound(format!(
        "no wasm export registered for fn_oid {fn_oid}"
    )))
}

fn epoch_tick_ms() -> u64 {
    match u64::try_from(guc::EPOCH_TICK_MS.get()) {
        Ok(0) | Err(_) => 10,
        Ok(v) => v,
    }
}

fn epoch_deadline_ticks(policy: &EffectivePolicy) -> u64 {
    let deadline_ms = policy.invocation_deadline_ms.max(0) as u64;
    let tick_ms = epoch_tick_ms();
    let ticks = deadline_ms.saturating_div(tick_ms);
    ticks.max(1)
}

fn store_limits_for_component(policy: &EffectivePolicy) -> StoreLimits {
    let max_pages = policy.max_memory_pages.max(0);
    let max_bytes = (max_pages as usize).saturating_mul(65_536);
    StoreLimitsBuilder::new()
        .memory_size(max_bytes)
        .instances(1)
        .build()
}

fn fuel_units(policy: &EffectivePolicy) -> Option<u64> {
    if !guc::FUEL_ENABLED.get() {
        return None;
    }
    let per = policy.fuel_per_invocation;
    if per <= 0 {
        return None;
    }
    Some(u64::try_from(per).unwrap_or(u64::MAX))
}

fn configure_store_for_invocation(
    store: &mut Store<StoreCtx>,
    policy: &EffectivePolicy,
) -> Result<Option<u64>, PgWasmError> {
    let limits = store_limits_for_component(policy);
    store.data_mut().limits = limits;
    store.limiter(|ctx| &mut ctx.limits);
    store.epoch_deadline_trap();
    store.set_epoch_deadline(epoch_deadline_ticks(policy));

    if let Some(fuel) = fuel_units(policy) {
        store
            .set_fuel(fuel)
            .map_err(|e| PgWasmError::Internal(format!("set_fuel failed: {e}")))?;
        Ok(Some(fuel))
    } else {
        // Engine always has fuel metering enabled; seed effectively-unbounded fuel when the GUC
        // or per-module policy disables per-invocation limits so `get_fuel` remains valid.
        store
            .set_fuel(u64::MAX)
            .map_err(|e| PgWasmError::Internal(format!("set_fuel failed: {e}")))?;
        Ok(None)
    }
}

fn oid_scalar_pg_type(oid: Oid) -> Result<PgType, PgWasmError> {
    Ok(match oid {
        o if o == pg_sys::BOOLOID => PgType::Scalar("boolean"),
        o if o == pg_sys::INT2OID => PgType::Scalar("int2"),
        o if o == pg_sys::INT4OID => PgType::Scalar("int4"),
        o if o == pg_sys::INT8OID => PgType::Scalar("int8"),
        o if o == pg_sys::TEXTOID => PgType::Scalar("text"),
        _ => {
            return Err(PgWasmError::Unsupported(format!(
                "pg_wasm trampoline: unsupported argument type oid {}",
                u32::from(oid)
            )));
        }
    })
}

fn export_surface_from_proc(
    arg_types: &[Oid],
    ret_type: Option<Oid>,
) -> Result<Export, PgWasmError> {
    let mut params = Vec::with_capacity(arg_types.len());
    for oid in arg_types {
        params.push(ExportSlot {
            is_option: false,
            pg_type: oid_scalar_pg_type(*oid)?,
        });
    }
    let result = match ret_type {
        None => None,
        Some(oid) if oid == pg_sys::VOIDOID => None,
        Some(oid) => Some(ExportSlot {
            is_option: false,
            pg_type: oid_scalar_pg_type(oid)?,
        }),
    };
    Ok(Export { params, result })
}

fn marshal_plans_for_export(export: &Export) -> Result<Vec<MarshalPlan>, PgWasmError> {
    let empty = TypePlan { entries: vec![] };
    plan_marshaler(&empty, export)
}

fn export_index_in_module(module_id: i64, export_id: i64) -> Result<u32, PgWasmError> {
    let list = exports::list_by_module(module_id)?;
    let pos = list
        .iter()
        .position(|row| row.export_id == export_id)
        .ok_or_else(|| {
            PgWasmError::Internal(format!(
                "export_id {export_id} not found under module_id {module_id}"
            ))
        })?;
    u32::try_from(pos).map_err(|_| PgWasmError::Internal("export index overflow".to_string()))
}

fn invoke_component_export(
    fcinfo: FunctionCallInfo,
    export_row: &exports::ExportRow,
    _module_row: &modules::ModuleRow,
    module_id: u64,
    export_index: u32,
    wasm_engine: &wasmtime::Engine,
    effective: &EffectivePolicy,
) -> Result<Datum, PgWasmError> {
    let cwasm_path = artifacts::module_cwasm_path(module_id)?;
    let expected_hash = component::engine_precompile_fingerprint(wasm_engine);
    let component =
        Arc::new(unsafe { component::load_precompiled(wasm_engine, &cwasm_path, &expected_hash)? });

    let export_surface = export_surface_from_proc(&export_row.arg_types, export_row.ret_type)?;
    let plans = marshal_plans_for_export(&export_surface)?;
    let nargs = export_row.arg_types.len();
    if plans.len() < nargs {
        return Err(PgWasmError::Internal(
            "marshal plan arity mismatch".to_string(),
        ));
    }
    let param_plans = &plans[..nargs];
    let result_plan = if export_surface.result.is_some() {
        plans.get(nargs)
    } else {
        None
    };

    let mut pooled =
        pool::acquire_pooled(module_id, Arc::clone(&component), wasm_engine, effective)?;
    let (instance, store) = pooled
        .instance_and_store_mut()
        .ok_or_else(|| PgWasmError::Internal("pooled wasm instance missing slot".to_string()))?;

    let fuel_before = configure_store_for_invocation(store, effective)?;

    let func = instance
        .get_func(&mut *store, export_row.wasm_name.as_str())
        .ok_or_else(|| {
            PgWasmError::InvalidModule(format!(
                "component export `{}` not found",
                export_row.wasm_name
            ))
        })?;

    let mut args_val = Vec::with_capacity(nargs);
    for (i, plan) in param_plans.iter().enumerate() {
        let nd = unsafe { pg_get_nullable_datum(fcinfo, i) };
        let arg_oid = unsafe { pg_getarg_type(fcinfo, i) };
        let val = composite::datum_to_val(plan, nd.value, nd.isnull, arg_oid)?;
        args_val.push(val);
    }

    let mut results_val: Vec<Val> = match result_plan {
        Some(_) => vec![Val::Bool(false); 1],
        None => vec![],
    };

    let call_outcome = func
        .call(&mut *store, &args_val, &mut results_val)
        .map_err(map_wasmtime_err);

    let fuel_after =
        if fuel_before.is_some() {
            Some(store.get_fuel().map_err(|e| {
                PgWasmError::Internal(format!("get_fuel after invocation failed: {e}"))
            })?)
        } else {
            None
        };

    if call_outcome.is_err() {
        pooled.poison();
    } else {
        pooled.release();
    }

    call_outcome?;

    if let (Some(fuel_start), Some(after)) = (fuel_before, fuel_after) {
        let used = fuel_start.saturating_sub(after);
        if used > 0 {
            shmem::add_export_counter(module_id, export_index, ExportCounterKind::TotalNs, used);
        }
    }

    shmem::incr_export_counter(module_id, export_index, ExportCounterKind::Invocations);

    match (result_plan, results_val.first()) {
        (Some(plan), Some(rv)) => {
            let (d, _is_null) = composite::val_to_datum(plan, rv)?;
            Ok(d)
        }
        (None, _) => Ok(Datum::from(0usize)),
        (Some(_), None) => Err(PgWasmError::Internal(
            "component call returned no results".to_string(),
        )),
    }
}

fn invoke_core_export(
    fcinfo: FunctionCallInfo,
    export_row: &exports::ExportRow,
    _module_row: &modules::ModuleRow,
    module_id: u64,
    export_index: u32,
    wasm_engine: &wasmtime::Engine,
    effective: &EffectivePolicy,
) -> Result<Datum, PgWasmError> {
    let wasm_path = artifacts::module_wasm_path(module_id)?;
    let bytes = std::fs::read(&wasm_path).map_err(PgWasmError::from)?;
    let loaded = core::compile(wasm_engine, &bytes)?;

    let nargs = export_row.arg_types.len();
    let mut vals = Vec::with_capacity(nargs);
    for i in 0..nargs {
        if unsafe { pg_arg_is_null(fcinfo, i) } {
            return Err(PgWasmError::ValidationFailed(format!(
                "argument {i} is NULL; core scalar path expects non-null i32"
            )));
        }
        let oid = unsafe { pg_getarg_type(fcinfo, i) };
        if oid != pg_sys::INT4OID {
            return Err(PgWasmError::Unsupported(format!(
                "core invoke: argument {i} must be int4, got oid {}",
                u32::from(oid)
            )));
        }
        let nd = unsafe { pg_get_nullable_datum(fcinfo, i) };
        let v =
            wasmtime::Val::I32(unsafe { i32::from_datum(nd.value, false) }.ok_or_else(|| {
                PgWasmError::Internal(format!("failed to read int4 argument {i}"))
            })?);
        vals.push(v);
    }

    let val = core::invoke(&loaded, export_row.wasm_name.as_str(), &vals, effective)?;

    shmem::incr_export_counter(module_id, export_index, ExportCounterKind::Invocations);

    if let Some(oid) = export_row.ret_type {
        if oid == pg_sys::INT4OID {
            match val {
                wasmtime::Val::I32(i) => Ok(Datum::from(i)),
                other => Err(PgWasmError::Internal(format!(
                    "core export returned unexpected val: {other:?}"
                ))),
            }
        } else {
            Err(PgWasmError::Unsupported(format!(
                "core invoke: unsupported return type oid {}",
                u32::from(oid)
            )))
        }
    } else {
        Ok(Datum::from(0usize))
    }
}

unsafe fn fn_oid_from_fcinfo(fcinfo: FunctionCallInfo) -> Oid {
    if fcinfo.is_null() {
        return Oid::INVALID;
    }

    let flinfo = unsafe { (*fcinfo).flinfo };
    if flinfo.is_null() {
        Oid::INVALID
    } else {
        unsafe { (*flinfo).fn_oid }
    }
}

/// Re-export policy parsing for the trampoline (mirrors `lifecycle::reconfigure` private helpers).
mod reconfigure {
    use serde_json::Value;

    use crate::config::{Limits, PolicyOverrides};
    use crate::errors::{PgWasmError, Result};

    pub(super) fn policy_overrides_from_module_json(value: &Value) -> Result<PolicyOverrides> {
        policy_overrides_from_value(value)
    }

    pub(super) fn limits_from_module_json(value: &Value) -> Result<Limits> {
        limits_from_value(value)
    }

    fn policy_overrides_from_value(value: &Value) -> Result<PolicyOverrides> {
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
                "allow_wasi_fs" => {
                    out.allow_wasi_fs = Some(json_bool(field_value, "allow_wasi_fs")?)
                }
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
                    out.wasi_preopens =
                        Some(json_string_to_string_map(field_value, "wasi_preopens")?);
                }
                _ => {}
            }
        }

        Ok(out)
    }

    fn limits_from_value(value: &Value) -> Result<Limits> {
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
                    out.invocation_deadline_ms =
                        Some(json_i32(field_value, "invocation_deadline_ms")?);
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
                PgWasmError::InvalidConfiguration(format!(
                    "`{field}[{index}]` must be a JSON string"
                ))
            })?;
            out.push(s.to_string());
        }
        Ok(out)
    }

    fn json_string_to_string_map(
        value: &Value,
        field: &'static str,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        let Some(obj) = value.as_object() else {
            return Err(PgWasmError::InvalidConfiguration(format!(
                "`{field}` must be a JSON object of string keys to string values"
            )));
        };

        let mut out = std::collections::BTreeMap::new();
        for (k, v) in obj {
            let vs = v.as_str().ok_or_else(|| {
                PgWasmError::InvalidConfiguration(format!("`{field}.{k}` must be a JSON string"))
            })?;
            out.insert(k.clone(), vs.to_string());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod host_tests {
    use wasmtime::Trap;

    use crate::errors::{PgWasmError, map_wasmtime_err};

    #[test]
    fn map_wasmtime_err_interrupt_out_of_fuel_and_other_trap() {
        let interrupt = wasmtime::Error::from(Trap::Interrupt);
        assert!(matches!(
            map_wasmtime_err(interrupt),
            PgWasmError::Timeout(_)
        ));

        let fuel = wasmtime::Error::from(Trap::OutOfFuel);
        assert!(matches!(
            map_wasmtime_err(fuel),
            PgWasmError::ResourceLimitExceeded(_)
        ));

        let unreachable_trap = wasmtime::Error::from(Trap::UnreachableCodeReached);
        match map_wasmtime_err(unreachable_trap) {
            PgWasmError::Trap { kind } => {
                assert!(kind.contains("unreachable"));
            }
            other => panic!("expected Trap variant, got {other:?}"),
        }
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use pgrx::prelude::*;
    use pgrx::spi::Spi;
    use serde_json::json;
    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};
    use wit_parser::Resolve;

    use crate::artifacts;
    use crate::catalog::exports;
    use crate::catalog::modules::{self, ModuleRow};
    use crate::proc_reg::{self, Parallel, ProcSpec, Volatility};
    use crate::shmem::{ExportCounterKind, allocate_slots, free_slots, read_export_counter};

    static NEXT_MODULE_ID: AtomicU64 = AtomicU64::new(900_000);

    fn next_module_id() -> i64 {
        NEXT_MODULE_ID.fetch_add(1, Ordering::SeqCst) as i64
    }

    fn extension_oid() -> pg_sys::Oid {
        let oid = unsafe { pg_sys::get_extension_oid(c"pg_wasm".as_ptr(), false) };
        assert_ne!(oid, pg_sys::InvalidOid);
        oid
    }

    fn wat_to_component_bytes(wat: &str) -> Vec<u8> {
        wat_to_component_bytes_with_wit(
            wat,
            "package pgwasm:test; world w { export run: func() -> s32; }",
        )
    }

    fn wat_to_component_bytes_with_wit(wat: &str, wit: &str) -> Vec<u8> {
        let wasm_bytes = wat::parse_str(wat).expect("wat parses");
        let mut module = wasm_bytes;
        let mut resolve = Resolve::default();
        let pkg = resolve.push_str("fixture.wit", wit).expect("wit");
        let world_id = resolve.select_world(&[pkg], Some("w")).expect("world");
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8).unwrap();
        ComponentEncoder::default()
            .module(&module)
            .unwrap()
            .validate(true)
            .encode()
            .unwrap()
    }

    fn install_component_module_row(
        name: String,
        digest: Vec<u8>,
        limits: serde_json::Value,
        component_bytes: &[u8],
    ) -> ModuleRow {
        let stub_row = modules::insert(&modules::NewModule {
            abi: "component".to_string(),
            artifact_path: "/tmp/pg_wasm_trampoline_stub".to_string(),
            digest: digest.clone(),
            generation: 0,
            limits: limits.clone(),
            name: name.clone(),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![0_u8; 32],
            wit_world: "default".to_string(),
        })
        .expect("insert module stub");

        let mid = stub_row.module_id as u64;
        artifacts::write_module_wasm(mid, component_bytes).expect("write wasm");
        let engine = crate::runtime::engine::shared_engine();
        let cwasm_path = artifacts::module_cwasm_path(mid).expect("cwasm path");
        let hash = crate::runtime::component::precompile_to(engine, component_bytes, &cwasm_path)
            .expect("precompile");

        let updated = modules::NewModule {
            abi: stub_row.abi.clone(),
            artifact_path: cwasm_path.display().to_string(),
            digest: stub_row.digest.clone(),
            generation: stub_row.generation,
            limits: stub_row.limits.clone(),
            name: stub_row.name.clone(),
            origin: stub_row.origin.clone(),
            policy: stub_row.policy.clone(),
            wasm_sha256: hash.to_vec(),
            wit_world: stub_row.wit_world.clone(),
        };
        modules::update(stub_row.module_id, &updated)
            .expect("module update")
            .expect("updated row")
    }

    fn install_trampoline_fn(
        module_id: i64,
        export_id: i64,
        wasm_name: &str,
        arg_types: Vec<pg_sys::Oid>,
        ret_type: pg_sys::Oid,
    ) -> String {
        let sql_name = format!("trampoline_test_{export_id}");
        let spec = ProcSpec {
            schema: "public".to_string(),
            name: sql_name.clone(),
            arg_types,
            arg_names: Vec::new(),
            arg_modes: Vec::new(),
            ret_type,
            returns_set: false,
            volatility: Volatility::Volatile,
            strict: false,
            parallel: Parallel::Unsafe,
            cost: Some(1.0),
        };
        let fn_oid = proc_reg::register(&spec, extension_oid(), false).expect("register");
        exports::update(
            export_id,
            &exports::NewExport {
                arg_types: spec.arg_types.clone(),
                fn_oid: Some(fn_oid),
                kind: "udf".to_string(),
                module_id,
                ret_type: Some(ret_type),
                signature: json!({}),
                sql_name: sql_name.clone(),
                wasm_name: wasm_name.to_string(),
            },
        )
        .expect("export fn_oid update");
        format!("public.\"{}\"", sql_name.replace('"', "\"\""))
    }

    fn captured_sql_error(fqfn: &str, arg_sql: Option<&str>) -> (String, String, String) {
        let call = match arg_sql {
            None => format!("PERFORM {fqfn}()"),
            Some(a) => format!("PERFORM {fqfn}({a})"),
        };
        Spi::run(
            "CREATE TEMP TABLE IF NOT EXISTS _pgwasm_trap_err (sqlstate text, message text, detail text)",
        )
        .unwrap();
        Spi::run("TRUNCATE _pgwasm_trap_err").unwrap();
        Spi::run(&format!(
            "DO $pgwasm_body$ \
             DECLARE d text; \
             BEGIN {call}; \
             EXCEPTION WHEN OTHERS THEN \
             GET STACKED DIAGNOSTICS d = PG_EXCEPTION_DETAIL; \
             INSERT INTO _pgwasm_trap_err VALUES (SQLSTATE, SQLERRM, coalesce(d, '')); \
             END; $pgwasm_body$"
        ))
        .unwrap();
        let (s, m, d) = Spi::get_three::<String, String, String>(
            "SELECT sqlstate, message, detail FROM _pgwasm_trap_err",
        )
        .expect("read trap capture");
        (
            s.expect("sqlstate"),
            m.expect("message"),
            d.expect("detail"),
        )
    }

    #[pg_test]
    fn trampoline_component_returns_and_increments_counters() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();
        Spi::run("SET pg_wasm.fuel_enabled = on").unwrap();

        let tag = next_module_id();
        let wat = r#"
            (module
              (func (export "run") (result i32)
                i32.const 42
              )
            )
        "#;
        let component_bytes = wat_to_component_bytes(wat);
        let inserted = install_component_module_row(
            format!("tramp_ok_{tag}"),
            vec![1; 32],
            json!({"fuel_per_invocation": 50_000}),
            &component_bytes,
        );
        let mid_u64 = inserted.module_id as u64;

        let export_row = exports::insert(&exports::NewExport {
            arg_types: vec![],
            fn_oid: None,
            kind: "udf".to_string(),
            module_id: inserted.module_id,
            ret_type: Some(pg_sys::INT4OID),
            signature: json!({}),
            sql_name: "run".to_string(),
            wasm_name: "run".to_string(),
        })
        .expect("insert export");

        free_slots(mid_u64);
        allocate_slots(mid_u64, 1).expect("allocate shmem");

        let fqfn = install_trampoline_fn(
            inserted.module_id,
            export_row.export_id,
            "run",
            vec![],
            pg_sys::INT4OID,
        );

        let export_index = super::export_index_in_module(inserted.module_id, export_row.export_id)
            .expect("export index");

        let before = read_export_counter(mid_u64, export_index, ExportCounterKind::Invocations);
        let v: i32 = Spi::get_one(&format!("SELECT {fqfn}()::int4"))
            .expect("spi")
            .expect("scalar");
        assert_eq!(v, 42);
        let after = read_export_counter(mid_u64, export_index, ExportCounterKind::Invocations)
            .expect("counter");
        assert_eq!(after, before.unwrap_or(0) + 1);

        let fuel_metric = read_export_counter(mid_u64, export_index, ExportCounterKind::TotalNs);
        assert!(
            fuel_metric.unwrap_or(0) > 0,
            "fuel-used metric should be non-zero when fuel is enabled"
        );

        let fn_oid = exports::get_by_id(export_row.export_id)
            .expect("read export")
            .expect("row")
            .fn_oid
            .expect("fn_oid");
        proc_reg::unregister(fn_oid).unwrap();
        exports::delete(export_row.export_id).unwrap();
        modules::delete(inserted.module_id).unwrap();
        free_slots(mid_u64);
    }

    #[pg_test(expected = "invocation timed out: invocation interrupted by epoch deadline")]
    fn trampoline_epoch_interrupts_infinite_loop() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let tag = next_module_id();
        let wat = r#"
            (module
              (func (export "run") (result i32)
                (loop (br 0))
                i32.const 0
              )
            )
        "#;
        let component_bytes = wat_to_component_bytes(wat);
        let inserted = install_component_module_row(
            format!("tramp_loop_{tag}"),
            vec![2; 32],
            json!({"invocation_deadline_ms": 100, "fuel_per_invocation": 10_000_000}),
            &component_bytes,
        );
        let mid_u64 = inserted.module_id as u64;

        let export_row = exports::insert(&exports::NewExport {
            arg_types: vec![],
            fn_oid: None,
            kind: "udf".to_string(),
            module_id: inserted.module_id,
            ret_type: Some(pg_sys::INT4OID),
            signature: json!({}),
            sql_name: "run".to_string(),
            wasm_name: "run".to_string(),
        })
        .unwrap();

        free_slots(mid_u64);
        allocate_slots(mid_u64, 1).unwrap();

        let fqfn = install_trampoline_fn(
            inserted.module_id,
            export_row.export_id,
            "run",
            vec![],
            pg_sys::INT4OID,
        );

        // Wasmtime epoch interrupt → `Trap::Interrupt` → `PgWasmError::Timeout` (SQLSTATE 57014).
        // The pgrx harness treats backend ERROR as a client failure unless `expected = ...` matches
        // the message exactly; PL/pgSQL cannot catch pgrx's `panic_any` error path.
        Spi::run(&format!("SELECT {fqfn}()")).unwrap();
    }

    #[pg_test]
    fn trampoline_out_of_fuel_maps_to_program_limit() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();
        Spi::run("SET pg_wasm.fuel_enabled = on").unwrap();

        let tag = next_module_id();
        let wat = r#"
            (module
              (func $spin (param i32) (result i32)
                (local.get 0)
                (if (result i32)
                  (then
                    (call $spin (i32.sub (local.get 0) (i32.const 1)))
                  )
                  (else (i32.const 0))
                )
              )
              (func (export "run") (param i32) (result i32)
                (call $spin (local.get 0))
              )
            )
        "#;
        let wit = "package pgwasm:test; world w { export run: func(x: s32) -> s32; }";
        let component_bytes = wat_to_component_bytes_with_wit(wat, wit);
        let inserted = install_component_module_row(
            format!("tramp_fuel_{tag}"),
            vec![3; 32],
            json!({"fuel_per_invocation": 500}),
            &component_bytes,
        );
        let mid_u64 = inserted.module_id as u64;

        let export_row = exports::insert(&exports::NewExport {
            arg_types: vec![pg_sys::INT4OID],
            fn_oid: None,
            kind: "udf".to_string(),
            module_id: inserted.module_id,
            ret_type: Some(pg_sys::INT4OID),
            signature: json!({}),
            sql_name: "run".to_string(),
            wasm_name: "run".to_string(),
        })
        .unwrap();

        free_slots(mid_u64);
        allocate_slots(mid_u64, 1).unwrap();

        let fqfn = install_trampoline_fn(
            inserted.module_id,
            export_row.export_id,
            "run",
            vec![pg_sys::INT4OID],
            pg_sys::INT4OID,
        );

        let (sqlstate, message, _detail) = captured_sql_error(&fqfn, Some("1000000"));
        assert_eq!(
            sqlstate, "54000",
            "unexpected sqlstate {sqlstate}: {message}"
        );
        assert!(
            message.to_lowercase().contains("fuel"),
            "unexpected message: {message}"
        );

        let fn_oid = exports::get_by_id(export_row.export_id)
            .unwrap()
            .unwrap()
            .fn_oid
            .unwrap();
        proc_reg::unregister(fn_oid).unwrap();
        exports::delete(export_row.export_id).unwrap();
        modules::delete(inserted.module_id).unwrap();
        free_slots(mid_u64);
    }

    #[pg_test]
    fn trampoline_unreachable_maps_to_external_routine_with_detail() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let tag = next_module_id();
        let wat = r#"
            (module
              (func (export "run") (result i32)
                unreachable
              )
            )
        "#;
        let component_bytes = wat_to_component_bytes(wat);
        let inserted = install_component_module_row(
            format!("tramp_trap_{tag}"),
            vec![4; 32],
            json!({"fuel_per_invocation": 50_000}),
            &component_bytes,
        );
        let mid_u64 = inserted.module_id as u64;

        let export_row = exports::insert(&exports::NewExport {
            arg_types: vec![],
            fn_oid: None,
            kind: "udf".to_string(),
            module_id: inserted.module_id,
            ret_type: Some(pg_sys::INT4OID),
            signature: json!({}),
            sql_name: "run".to_string(),
            wasm_name: "run".to_string(),
        })
        .unwrap();

        free_slots(mid_u64);
        allocate_slots(mid_u64, 1).unwrap();

        let fqfn = install_trampoline_fn(
            inserted.module_id,
            export_row.export_id,
            "run",
            vec![],
            pg_sys::INT4OID,
        );

        let (sqlstate, _message, detail) = captured_sql_error(&fqfn, None);
        assert_eq!(sqlstate, "38000", "unexpected sqlstate {sqlstate}");
        assert!(
            detail.to_lowercase().contains("unreachable"),
            "expected trap detail, got: {detail}"
        );

        let fn_oid = exports::get_by_id(export_row.export_id)
            .unwrap()
            .unwrap()
            .fn_oid
            .unwrap();
        proc_reg::unregister(fn_oid).unwrap();
        exports::delete(export_row.export_id).unwrap();
        modules::delete(inserted.module_id).unwrap();
        free_slots(mid_u64);
    }
}
