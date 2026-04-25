//! Shared Wasmtime engine construction and access.

use std::sync::OnceLock;

use wasmtime::{Cache, Config, Engine};

use crate::errors::PgWasmError;

static SHARED_ENGINE: OnceLock<Engine> = OnceLock::new();

/// Returns the lazily-initialized backend-local Wasmtime engine.
pub(crate) fn shared_engine() -> &'static Engine {
    try_shared_engine().expect("failed to initialize shared Wasmtime engine")
}

/// Fallible shared engine accessor to avoid panics in call sites that can recover.
pub(crate) fn try_shared_engine() -> Result<&'static Engine, PgWasmError> {
    if let Some(engine) = SHARED_ENGINE.get() {
        return Ok(engine);
    }

    // Fuel metering is enabled on the shared engine so per-invocation `Store::set_fuel` works when
    // `pg_wasm.fuel_enabled` is toggled after process start. When fuel is effectively disabled for a
    // call, the trampoline seeds the store with `u64::MAX` units so metering is a no-op in practice.
    let engine = build_engine(true)?;
    let _ = SHARED_ENGINE.set(engine);

    SHARED_ENGINE.get().ok_or_else(|| {
        PgWasmError::Internal("shared Wasmtime engine failed to initialize".to_string())
    })
}

pub(super) fn build_engine(fuel_enabled: bool) -> Result<Engine, PgWasmError> {
    let mut config = Config::new();
    configure_engine(&mut config, fuel_enabled);

    Engine::new(&config).map_err(|error| {
        PgWasmError::Internal(format!("failed to construct Wasmtime engine: {error}"))
    })
}

pub(super) fn configure_engine(config: &mut Config, fuel_enabled: bool) {
    config.wasm_component_model(true);
    config.epoch_interruption(true);
    config.consume_fuel(fuel_enabled);
    config.cache(None::<Cache>);
    config.parallel_compilation(false);
}
