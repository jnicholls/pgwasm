//! Optional component lifecycle hooks (`on-load`, `on-unload`, `on-reconfigure`).
//!
//! Hooks are **optional** exports with stable names from the module's WIT world. Missing exports
//! are not an error. `on-unload` failures are logged at WARNING and do not fail unload.

use std::sync::Arc;

use pgrx::PgSqlErrorCode;
use pgrx::prelude::*;
use serde_json::Value;
use wasmtime::component::{Component, Val};
use wasmtime::{Engine, Store, StoreLimits, StoreLimitsBuilder};

use crate::artifacts;
use crate::catalog::modules;
use crate::errors::{PgWasmError, Result};
use crate::guc;
use crate::policy::{self, EffectivePolicy, GucSnapshot};
use crate::runtime::component::{self, StoreCtx};
use crate::runtime::engine;
use crate::runtime::pool;

const ON_LOAD: &str = "on-load";
const ON_RECONFIGURE: &str = "on-reconfigure";
const ON_UNLOAD: &str = "on-unload";

pub(crate) fn on_load(module_id: u64, config_blob: &Value) -> Result<()> {
    let Some(module_row) = modules::get_by_id(
        i64::try_from(module_id)
            .map_err(|_| PgWasmError::Internal("module_id does not fit i64".to_string()))?,
    )?
    else {
        return Err(PgWasmError::NotFound(format!("module_id {module_id}")));
    };
    if !module_row.abi.eq_ignore_ascii_case("component") {
        return Ok(());
    }

    let effective = effective_policy_for_module(&module_row)?;
    let engine = engine::try_shared_engine()?;
    let component = load_component(module_id, engine)?;

    let config_json = serde_json::to_string(config_blob).map_err(|e| {
        PgWasmError::InvalidConfiguration(format!("failed to serialize load config JSON: {e}"))
    })?;

    invoke_hook_export(
        engine,
        module_id,
        &component,
        &effective,
        ON_LOAD,
        &[Val::String(config_json)],
    )
}

pub(crate) fn on_unload(module_id: u64) -> Result<()> {
    let Ok(module_id_i64) = i64::try_from(module_id) else {
        return Ok(());
    };
    let Some(module_row) = modules::get_by_id(module_id_i64)? else {
        return Ok(());
    };
    if !module_row.abi.eq_ignore_ascii_case("component") {
        return Ok(());
    }

    let effective = match effective_policy_for_module(&module_row) {
        Ok(p) => p,
        Err(e) => {
            log_hook_unload_warning(module_id, &format!("policy resolve failed: {e}"));
            return Ok(());
        }
    };

    let engine = match engine::try_shared_engine() {
        Ok(e) => e,
        Err(e) => {
            log_hook_unload_warning(module_id, &format!("engine unavailable: {e}"));
            return Ok(());
        }
    };

    let component = match load_component(module_id, engine) {
        Ok(c) => c,
        Err(e) => {
            log_hook_unload_warning(module_id, &format!("load component: {e}"));
            return Ok(());
        }
    };

    if let Err(detail) =
        invoke_hook_export_inner(engine, module_id, &component, &effective, ON_UNLOAD, &[])
    {
        log_hook_unload_warning(module_id, &detail);
    }

    Ok(())
}

pub(crate) fn on_reconfigure(module_id: u64, effective: &EffectivePolicy) -> Result<()> {
    let Some(module_row) = modules::get_by_id(
        i64::try_from(module_id)
            .map_err(|_| PgWasmError::Internal("module_id does not fit i64".to_string()))?,
    )?
    else {
        return Err(PgWasmError::NotFound(format!("module_id {module_id}")));
    };
    if !module_row.abi.eq_ignore_ascii_case("component") {
        return Ok(());
    }

    let engine = engine::try_shared_engine()?;
    let component = load_component(module_id, engine)?;

    let policy_json = effective_policy_json_string(effective)?;
    invoke_hook_export(
        engine,
        module_id,
        &component,
        effective,
        ON_RECONFIGURE,
        &[Val::String(policy_json)],
    )
}

fn invoke_hook_export(
    engine: &Engine,
    module_id: u64,
    component: &Arc<Component>,
    effective: &EffectivePolicy,
    export_name: &str,
    args: &[Val],
) -> Result<()> {
    match invoke_hook_export_inner(engine, module_id, component, effective, export_name, args) {
        Ok(None) => Ok(()),
        Ok(Some(msg)) => Err(PgWasmError::InvalidConfiguration(format!(
            "{export_name} hook rejected configuration: {msg}"
        ))),
        Err(detail) => Err(PgWasmError::InvalidConfiguration(format!(
            "{export_name} hook failed: {detail}"
        ))),
    }
}

/// Returns `Ok(None)` on success, `Ok(Some(msg))` when the guest returned `err(string)`, or
/// `Err(detail)` for host/wasm failures.
fn invoke_hook_export_inner(
    engine: &Engine,
    module_id: u64,
    component: &Arc<Component>,
    effective: &EffectivePolicy,
    export_name: &str,
    args: &[Val],
) -> core::result::Result<Option<String>, String> {
    let mut pooled = pool::acquire_pooled(module_id, Arc::clone(component), engine, effective)
        .map_err(|e| e.to_string())?;
    let (instance, store) = pooled
        .instance_and_store_mut()
        .ok_or_else(|| "pooled wasm instance missing slot".to_string())?;

    let Some(func) = instance.get_func(&mut *store, export_name) else {
        pooled.release();
        return Ok(None);
    };

    configure_store_for_hook(store, effective).map_err(|e| e.to_string())?;

    let mut results = [Val::Bool(false)];
    let call_outcome = func
        .call(&mut *store, args, &mut results)
        .map_err(|e| e.to_string());

    pooled.release();

    call_outcome?;

    guest_result_message(&results[0])
}

fn guest_result_message(val: &Val) -> core::result::Result<Option<String>, String> {
    let Val::Result(r) = val else {
        return Err(format!("expected result<_, string>, got {val:?}"));
    };
    match r {
        Ok(None) => Ok(None),
        Ok(Some(_)) => Err("unexpected ok payload for result<_, string>".to_string()),
        Err(None) => Ok(Some("hook returned error with no message".to_string())),
        Err(Some(boxed)) => match &**boxed {
            Val::String(s) => Ok(Some(s.clone())),
            other => Err(format!("expected string error payload, got {other:?}")),
        },
    }
}

fn effective_policy_for_module(module_row: &modules::ModuleRow) -> Result<EffectivePolicy> {
    let snapshot = GucSnapshot::from_gucs();
    let override_policy = policy_overrides_from_module_json(&module_row.policy)?;
    let override_limits = limits_from_module_json(&module_row.limits)?;
    policy::resolve(&snapshot, Some(&override_policy), Some(&override_limits))
}

fn effective_policy_json_string(effective: &EffectivePolicy) -> Result<String> {
    let v = effective_policy_to_json(effective);
    serde_json::to_string(&v).map_err(|e| {
        PgWasmError::InvalidConfiguration(format!("failed to serialize effective policy: {e}"))
    })
}

fn effective_policy_to_json(e: &EffectivePolicy) -> Value {
    serde_json::json!({
        "allow_spi": e.allow_spi,
        "allow_wasi": e.allow_wasi,
        "allow_wasi_env": e.allow_wasi_env,
        "allow_wasi_fs": e.allow_wasi_fs,
        "allow_wasi_http": e.allow_wasi_http,
        "allow_wasi_net": e.allow_wasi_net,
        "allow_wasi_stdio": e.allow_wasi_stdio,
        "allowed_hosts": e.allowed_hosts,
        "fuel_per_invocation": e.fuel_per_invocation,
        "instances_per_module": e.instances_per_module,
        "invocation_deadline_ms": e.invocation_deadline_ms,
        "max_memory_pages": e.max_memory_pages,
        "wasi_preopens": e.wasi_preopens,
    })
}

fn load_component(module_id: u64, engine: &Engine) -> Result<Arc<Component>> {
    let cwasm_path = artifacts::module_cwasm_path(module_id)?;
    let expected_hash = component::engine_precompile_fingerprint(engine);
    Ok(Arc::new(unsafe {
        component::load_precompiled(engine, &cwasm_path, &expected_hash)?
    }))
}

fn log_hook_unload_warning(module_id: u64, detail: &str) {
    ereport!(
        PgLogLevel::WARNING,
        PgSqlErrorCode::ERRCODE_WARNING,
        format!(
            "pg_wasm: on-unload hook failed for module_id {module_id}: {detail}; unload continues"
        ),
    );
}

fn epoch_tick_ms() -> u64 {
    if cfg!(all(test, not(feature = "pg_test"))) {
        return 10;
    }
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
    #[cfg(all(test, not(feature = "pg_test")))]
    let fuel_enabled = false;
    #[cfg(any(not(test), feature = "pg_test"))]
    let fuel_enabled = guc::FUEL_ENABLED.get();
    if !fuel_enabled {
        return None;
    }
    let per = policy.fuel_per_invocation;
    if per <= 0 {
        return None;
    }
    Some(u64::try_from(per).unwrap_or(u64::MAX))
}

fn configure_store_for_hook(store: &mut Store<StoreCtx>, policy: &EffectivePolicy) -> Result<()> {
    let limits = store_limits_for_component(policy);
    store.data_mut().limits = limits;
    store.limiter(|ctx| &mut ctx.limits);
    store.epoch_deadline_trap();
    store.set_epoch_deadline(epoch_deadline_ticks(policy));

    if let Some(fuel) = fuel_units(policy) {
        store
            .set_fuel(fuel)
            .map_err(|e| PgWasmError::Internal(format!("set_fuel failed: {e}")))?;
    } else {
        store
            .set_fuel(u64::MAX)
            .map_err(|e| PgWasmError::Internal(format!("set_fuel failed: {e}")))?;
    }
    Ok(())
}

fn policy_overrides_from_module_json(value: &Value) -> Result<crate::config::PolicyOverrides> {
    use crate::config::PolicyOverrides;

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
            _ => {}
        }
    }

    Ok(out)
}

fn limits_from_module_json(value: &Value) -> Result<crate::config::Limits> {
    use crate::config::Limits;

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
) -> Result<std::collections::BTreeMap<String, String>> {
    use std::collections::BTreeMap;

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

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use wasmtime::component::Component;
    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};
    use wit_parser::Resolve;

    use super::*;
    use crate::policy::GucSnapshot;

    static HOOK_TEST_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

    fn lock_hook_tests() -> std::sync::MutexGuard<'static, ()> {
        match HOOK_TEST_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
        {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    fn test_engine() -> Result<Engine> {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);
        // `configure_store_for_hook` calls `Store::set_fuel`; that requires fuel metering on the engine.
        config.consume_fuel(true);
        Engine::new(&config).map_err(|e| PgWasmError::Internal(format!("hooks test engine: {e}")))
    }

    fn fixture_empty_component(engine: &Engine) -> Result<Arc<Component>> {
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut resolve = Resolve::default();
        let wit = "package test:hooks-empty; world w { }";
        let pkg = resolve
            .push_str("fixture.wit", wit)
            .map_err(|e| PgWasmError::Internal(format!("fixture wit parse: {e}")))?;
        let world_id = resolve
            .select_world(&[pkg], Some("w"))
            .map_err(|e| PgWasmError::Internal(format!("fixture world: {e}")))?;
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8)
            .map_err(|e| PgWasmError::Internal(format!("embed metadata: {e}")))?;
        let bytes = ComponentEncoder::default()
            .module(&module)
            .map_err(|e| PgWasmError::Internal(format!("encoder module: {e}")))?
            .validate(true)
            .encode()
            .map_err(|e| PgWasmError::Internal(format!("encode component: {e}")))?;
        Ok(Arc::new(Component::from_binary(engine, &bytes).map_err(
            |e| PgWasmError::Internal(format!("compile fixture component: {e}")),
        )?))
    }

    fn fixture_on_load_err_component(engine: &Engine) -> Result<Arc<Component>> {
        let bytes = include_bytes!("../tests/data/on_load_err.wasm");
        Ok(Arc::new(Component::from_binary(engine, bytes).map_err(
            |e| PgWasmError::Internal(format!("compile on_load_err fixture: {e}")),
        )?))
    }

    fn fixture_on_unload_trap_component(engine: &Engine) -> Result<Arc<Component>> {
        let bytes = include_bytes!("../tests/data/on_unload_trap.wasm");
        Ok(Arc::new(Component::from_binary(engine, bytes).map_err(
            |e| PgWasmError::Internal(format!("compile on_unload_trap fixture: {e}")),
        )?))
    }

    fn narrow_policy_for_tests(base: &EffectivePolicy) -> EffectivePolicy {
        let mut p = base.clone();
        p.instances_per_module = 8;
        p
    }

    fn test_policy() -> Result<EffectivePolicy> {
        let guc = GucSnapshot::new_for_test(
            false,
            false,
            false,
            false,
            false,
            false,
            BTreeMap::new(),
            Vec::new(),
            false,
            256,
            8,
            10_000,
            60_000,
        );
        policy::resolve(&guc, None, None)
    }

    #[test]
    fn hooks_no_exports_are_no_ops() -> Result<()> {
        let _guard = lock_hook_tests();
        let engine = test_engine()?;
        let component = fixture_empty_component(&engine)?;
        let policy = narrow_policy_for_tests(&test_policy()?);
        let linker = component::build_linker(&engine, &policy)?;
        let module_id = 9101u64;
        let _ = pool::drain(module_id);
        let _pool = pool::InstancePool::new(module_id, Arc::clone(&component), linker, &policy)?;

        assert!(
            invoke_hook_export_inner(
                &engine,
                module_id,
                &component,
                &policy,
                ON_LOAD,
                &[Val::String("{}".to_string())],
            )
            .unwrap()
            .is_none()
        );

        assert!(
            invoke_hook_export_inner(&engine, module_id, &component, &policy, ON_UNLOAD, &[])
                .unwrap()
                .is_none()
        );

        let eff = test_policy()?;
        let json = effective_policy_json_string(&eff)?;
        assert!(
            invoke_hook_export_inner(
                &engine,
                module_id,
                &component,
                &policy,
                ON_RECONFIGURE,
                &[Val::String(json)],
            )
            .unwrap()
            .is_none()
        );

        let _ = pool::drain(module_id);
        Ok(())
    }

    #[test]
    fn on_load_err_maps_to_invalid_configuration() -> Result<()> {
        let _guard = lock_hook_tests();
        let engine = test_engine()?;
        let component = fixture_on_load_err_component(&engine)?;
        let policy = narrow_policy_for_tests(&test_policy()?);
        let linker = component::build_linker(&engine, &policy)?;
        let module_id = 9102u64;
        let _ = pool::drain(module_id);
        let _pool = pool::InstancePool::new(module_id, Arc::clone(&component), linker, &policy)?;

        let err = invoke_hook_export(
            &engine,
            module_id,
            &component,
            &policy,
            ON_LOAD,
            &[Val::String("{}".to_string())],
        )
        .expect_err("on_load should fail");
        match err {
            PgWasmError::InvalidConfiguration(msg) => assert!(msg.contains("bad"), "msg={msg}"),
            other => panic!("unexpected error: {other:?}"),
        }

        let _ = pool::drain(module_id);
        Ok(())
    }

    #[test]
    fn on_unload_trap_returns_ok_from_inner() -> Result<()> {
        let _guard = lock_hook_tests();
        let engine = test_engine()?;
        let component = fixture_on_unload_trap_component(&engine)?;
        let policy = narrow_policy_for_tests(&test_policy()?);
        let linker = component::build_linker(&engine, &policy)?;
        let module_id = 9103u64;
        let _ = pool::drain(module_id);
        let _pool = pool::InstancePool::new(module_id, Arc::clone(&component), linker, &policy)?;

        let detail =
            invoke_hook_export_inner(&engine, module_id, &component, &policy, ON_UNLOAD, &[]);
        assert!(detail.is_err(), "expected trap from guest, got {detail:?}");

        let _ = pool::drain(module_id);
        Ok(())
    }
}
