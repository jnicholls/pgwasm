//! Wasmtime engine and epoch-ticker runtime primitives.

use std::ffi::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pgrx::pg_sys;
use wasmtime::EngineWeak;

use crate::guc;

pub(crate) mod engine {
    use std::sync::OnceLock;

    use wasmtime::{Cache, Config, Engine};

    use crate::errors::PgWasmError;
    use crate::guc;

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

        let engine = build_engine(guc::FUEL_ENABLED.get())?;
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
}

static EPOCH_TICKER: OnceLock<EpochTickerState> = OnceLock::new();
static EPOCH_TICKER_EXIT_HOOK: OnceLock<()> = OnceLock::new();

struct EpochTickerState {
    shutdown: Arc<AtomicBool>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// Called once from `_PG_init`. Initializes the shared engine and starts the
/// epoch ticker thread. Also registers an on-exit hook to stop/join the ticker.
pub(crate) fn init() {
    EPOCH_TICKER.get_or_init(|| {
        let tick_ms = read_epoch_tick_ms();
        let tick_duration = Duration::from_millis(tick_ms);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_thread = Arc::clone(&shutdown);
        let weak_engine = engine::shared_engine().weak();
        let handle = thread::Builder::new()
            .name("pg-wasm-epoch-ticker".to_string())
            .spawn(move || run_epoch_ticker_loop(shutdown_for_thread, weak_engine, tick_duration))
            .expect("failed to spawn pg_wasm epoch ticker thread");

        EpochTickerState {
            shutdown,
            handle: Mutex::new(Some(handle)),
        }
    });

    EPOCH_TICKER_EXIT_HOOK.get_or_init(register_epoch_ticker_exit_hook);
}

fn read_epoch_tick_ms() -> u64 {
    match u64::try_from(guc::EPOCH_TICK_MS.get()) {
        Ok(0) | Err(_) => 1,
        Ok(value) => value,
    }
}

fn register_epoch_ticker_exit_hook() {
    unsafe {
        pg_sys::on_proc_exit(Some(on_proc_exit_epoch_ticker), pg_sys::Datum::from(0usize));
    }
}

unsafe extern "C-unwind" fn on_proc_exit_epoch_ticker(_code: c_int, _arg: pg_sys::Datum) {
    stop_epoch_ticker();
}

fn stop_epoch_ticker() {
    let Some(state) = EPOCH_TICKER.get() else {
        return;
    };

    state.shutdown.store(true, Ordering::Relaxed);

    if let Some(handle) = take_epoch_ticker_handle(state) {
        let _ = handle.join();
    }
}

fn take_epoch_ticker_handle(state: &EpochTickerState) -> Option<JoinHandle<()>> {
    match state.handle.lock() {
        Ok(mut guard) => guard.take(),
        Err(poisoned) => {
            let mut guard = poisoned.into_inner();
            guard.take()
        }
    }
}

pub(crate) fn run_epoch_ticker_loop(
    shutdown: Arc<AtomicBool>,
    weak_engine: EngineWeak,
    tick_duration: Duration,
) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        thread::sleep(tick_duration);

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let Some(engine) = weak_engine.upgrade() else {
            break;
        };
        engine.increment_epoch();
    }
}

#[cfg(test)]
mod host_tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::sync::mpsc;

    use super::*;

    fn precompile_compatibility_hash(engine: &wasmtime::Engine) -> u64 {
        let mut hasher = DefaultHasher::new();
        engine.precompile_compatibility_hash().hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn engine_config_smoke_validates_minimal_module_and_stable_hash() {
        let engine = engine::build_engine(false).expect("host test engine should be constructible");
        let minimal_module = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];

        assert!(wasmtime::Module::validate(&engine, &minimal_module).is_ok());

        let first = precompile_compatibility_hash(&engine);
        let second = precompile_compatibility_hash(&engine);
        assert_eq!(first, second);
    }

    #[test]
    fn ticker_loop_exits_when_shutdown_flag_flips() {
        let engine = engine::build_engine(false).expect("host test engine should be constructible");
        let weak_engine = engine.weak();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_for_thread = Arc::clone(&shutdown);
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            run_epoch_ticker_loop(shutdown_for_thread, weak_engine, Duration::from_millis(1));
            let _ = done_tx.send(());
        });

        thread::sleep(Duration::from_millis(20));
        shutdown.store(true, Ordering::Relaxed);

        assert!(done_rx.recv_timeout(Duration::from_secs(1)).is_ok());
        handle
            .join()
            .expect("ticker thread should join after shutdown signal");
    }

    #[test]
    fn ticker_loop_exits_when_engine_drops() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let weak_engine = {
            let engine =
                engine::build_engine(false).expect("host test engine should be constructible");
            engine.weak()
        };
        let shutdown_for_thread = Arc::clone(&shutdown);
        let (done_tx, done_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            run_epoch_ticker_loop(shutdown_for_thread, weak_engine, Duration::from_millis(1));
            let _ = done_tx.send(());
        });

        assert!(done_rx.recv_timeout(Duration::from_secs(1)).is_ok());
        handle
            .join()
            .expect("ticker thread should join after weak engine expires");
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    use super::engine;

    #[pg_test]
    fn shared_engine_returns_same_pointer_within_backend() {
        let first = engine::shared_engine();
        let second = engine::shared_engine();

        assert!(core::ptr::eq(first, second));
    }
}
