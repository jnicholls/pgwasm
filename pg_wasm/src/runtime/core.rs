//! Core WebAssembly modules: compile once, invoke typed scalar exports.

use wasmtime::{
    Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, TypedFunc, Val,
};

use crate::errors::{PgWasmError, map_wasmtime_err};
use crate::guc;
use crate::policy::EffectivePolicy;

use super::engine;

/// Per-invocation store state: holds `StoreLimits` for the limiter callback.
#[derive(Debug)]
struct CoreStoreData {
    limits: StoreLimits,
}

/// Compiled core module plus a linker with no host imports.
pub(crate) struct Loaded {
    linker: Linker<CoreStoreData>,
    module: Module,
}

/// Compile bytes as a core Wasm module and prepare a linker.
pub(crate) fn compile(wasm_engine: &Engine, bytes: &[u8]) -> Result<Loaded, PgWasmError> {
    let module = Module::from_binary(wasm_engine, bytes).map_err(|error| {
        PgWasmError::InvalidModule(format!("failed to compile core module: {error}"))
    })?;

    let linker = Linker::new(wasm_engine);

    Ok(Loaded { linker, module })
}

fn module_has_memory(module: &Module) -> bool {
    for export in module.exports() {
        if export.name() == "memory" {
            return export.ty().memory().is_some();
        }
    }
    false
}

fn store_limits_for_policy(module: &Module, policy: &EffectivePolicy) -> StoreLimits {
    let mut builder = StoreLimitsBuilder::new();
    if module_has_memory(module) {
        let max_pages = policy.max_memory_pages.max(0);
        let max_bytes = (max_pages as usize).saturating_mul(65_536);
        if max_bytes > 0 {
            builder = builder.memory_size(max_bytes);
        }
    }
    builder.build()
}

fn epoch_deadline_ticks(policy: &EffectivePolicy) -> u64 {
    let deadline_ms = policy.invocation_deadline_ms.max(0) as u64;
    let tick_ms = match u64::try_from(guc::EPOCH_TICK_MS.get()) {
        Ok(0) | Err(_) => 1,
        Ok(v) => v,
    };
    let ticks = deadline_ms / tick_ms;
    ticks.max(1)
}

/// Invoke a core export that returns `i32` and takes `n` `i32` parameters from `args`.
pub(crate) fn invoke_i32_n(
    loaded: &Loaded,
    export_name: &str,
    args: &[i32],
    policy: &EffectivePolicy,
) -> Result<i32, PgWasmError> {
    let wasm_engine = engine::shared_engine();
    let limits = store_limits_for_policy(&loaded.module, policy);
    let mut store = Store::new(wasm_engine, CoreStoreData { limits });
    store.limiter(|state| &mut state.limits);
    store.epoch_deadline_trap();
    store.set_epoch_deadline(epoch_deadline_ticks(policy));

    if let Some(fuel) = fuel_units(policy) {
        store.set_fuel(fuel).map_err(|error| {
            PgWasmError::Internal(format!("failed to configure fuel for core invoke: {error}"))
        })?;
    }

    let instance = loaded
        .linker
        .instantiate(&mut store, &loaded.module)
        .map_err(|error| {
            PgWasmError::InvalidModule(format!("failed to instantiate core module: {error}"))
        })?;

    match args.len() {
        0 => {
            let func: TypedFunc<(), i32> = typed_export(&instance, &mut store, export_name)?;
            func.call(&mut store, ())
        }
        1 => {
            let func: TypedFunc<(i32,), i32> = typed_export(&instance, &mut store, export_name)?;
            func.call(&mut store, (args[0],))
        }
        2 => {
            let func: TypedFunc<(i32, i32), i32> =
                typed_export(&instance, &mut store, export_name)?;
            func.call(&mut store, (args[0], args[1]))
        }
        n => {
            return Err(PgWasmError::Unsupported(format!(
                "core invoke supports at most 2 i32 parameters, got {n}"
            )));
        }
    }
    .map_err(map_wasmtime_err)
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

fn typed_export<TParams, TResults>(
    instance: &Instance,
    store: &mut Store<CoreStoreData>,
    export_name: &str,
) -> Result<TypedFunc<TParams, TResults>, PgWasmError>
where
    TParams: wasmtime::WasmParams,
    TResults: wasmtime::WasmResults,
{
    let export = instance
        .get_export(&mut *store, export_name)
        .ok_or_else(|| {
            PgWasmError::InvalidModule(format!("missing export `{export_name}` on core module"))
        })?;
    let func = export.into_func().ok_or_else(|| {
        PgWasmError::InvalidModule(format!("export `{export_name}` is not a function"))
    })?;
    func.typed(&mut *store).map_err(|error| {
        PgWasmError::InvalidModule(format!(
            "export `{export_name}` has incompatible signature: {error}"
        ))
    })
}

/// Invoke export returning a single scalar `Val` (used when the SQL surface picks arity).
pub(crate) fn invoke(
    loaded: &Loaded,
    export_name: &str,
    args: &[Val],
    policy: &EffectivePolicy,
) -> Result<Val, PgWasmError> {
    let i32_args: Vec<i32> = args
        .iter()
        .map(|v| match v {
            Val::I32(i) => Ok(*i),
            _ => Err(PgWasmError::Unsupported(
                "core scalar path supports only i32 arguments for this entry point".to_string(),
            )),
        })
        .collect::<Result<_, _>>()?;
    let i = invoke_i32_n(loaded, export_name, &i32_args, policy)?;
    Ok(Val::I32(i))
}
