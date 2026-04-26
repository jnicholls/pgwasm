//! Wasmtime engine, epoch ticker, and runtime subsystems.

use std::ffi::c_int;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use pgrx::pg_sys;
use wasmtime::EngineWeak;

use crate::guc;

pub(crate) mod component;
pub(crate) mod core;
pub(crate) mod engine;
#[cfg(test)]
#[path = "host_stub.rs"]
pub(crate) mod host;
#[cfg(not(test))]
#[path = "host.rs"]
pub(crate) mod host;
pub(crate) mod pool;

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

#[cfg(all(test, not(feature = "pg_test")))]
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

    #[test]
    fn component_precompile_round_trip() {
        use std::sync::Arc;

        use wasmtime::component::Component;
        use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};

        let engine = engine::build_engine(false).expect("engine");
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut resolve = wit_parser::Resolve::default();
        let wit = "package test:pool; world w { }";
        let pkg = resolve.push_str("fixture.wit", wit).expect("wit parses");
        let world_id = resolve.select_world(&[pkg], Some("w")).expect("world");
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8)
            .expect("embed");
        let component_bytes = ComponentEncoder::default()
            .module(&module)
            .expect("encode")
            .validate(true)
            .encode()
            .expect("component");

        let component = Component::from_binary(&engine, &component_bytes).expect("compile");
        let tmp = tempfile::NamedTempFile::new().expect("temp");
        let hash = super::component::precompile_to(&engine, &component_bytes, tmp.path())
            .expect("precompile");
        let loaded = unsafe {
            super::component::load_precompiled(&engine, tmp.path(), &hash).expect("load")
        };
        assert_eq!(component.serialize().unwrap(), loaded.serialize().unwrap());
        let _ = Arc::new(component);
    }

    #[test]
    fn pool_capacity_blocks_waiters() {
        use std::sync::Arc;

        use wasmtime::component::Component;
        use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};

        use crate::policy::GucSnapshot;

        let engine = engine::build_engine(false).expect("engine");
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut resolve = wit_parser::Resolve::default();
        let wit = "package test:pool2; world w2 { }";
        let pkg = resolve.push_str("fixture.wit", wit).expect("wit");
        let world_id = resolve.select_world(&[pkg], Some("w2")).expect("world");
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8).unwrap();
        let component_bytes = ComponentEncoder::default()
            .module(&module)
            .unwrap()
            .validate(true)
            .encode()
            .unwrap();
        let component =
            Arc::new(Component::from_binary(&engine, &component_bytes).expect("component"));
        let guc = GucSnapshot::from_gucs();
        let policy = crate::policy::resolve(&guc, None, None).expect("policy");
        let mut narrow = policy.clone();
        narrow.instances_per_module = 1;
        let linker = super::component::build_linker(&engine, &narrow).expect("linker");
        let pool = super::pool::InstancePool::new(99, Arc::clone(&component), linker, &narrow)
            .expect("pool");

        let a = pool.acquire(&engine, &narrow).expect("a");
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let b_barrier = Arc::clone(&barrier);
        let b_engine = engine.clone();
        let b_pool = super::pool::InstancePool {
            inner: Arc::clone(&pool.inner),
        };
        let b_policy = narrow.clone();
        let handle = std::thread::spawn(move || {
            b_barrier.wait();
            b_pool.acquire(&b_engine, &b_policy)
        });
        barrier.wait();
        std::thread::sleep(Duration::from_millis(50));
        a.release();
        handle.join().expect("join").expect("b acquire");
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use std::sync::Arc;

    use pgrx::prelude::*;
    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};
    use wit_parser::Resolve;

    use crate::artifacts;

    use super::component;
    use super::engine;

    #[pg_test]
    fn precompile_writes_under_pgdata() {
        let engine = engine::shared_engine();
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut resolve = Resolve::default();
        let wit = "package test:art; world w { }";
        let pkg = resolve.push_str("fixture.wit", wit).unwrap();
        let world_id = resolve.select_world(&[pkg], Some("w")).unwrap();
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8).unwrap();
        let bytes = ComponentEncoder::default()
            .module(&module)
            .unwrap()
            .validate(true)
            .encode()
            .unwrap();

        let dir = artifacts::ensure_module_dir(42).expect("module dir");
        let path = dir.join("smoke.cwasm");
        let hash = component::precompile_to(engine, &bytes, &path).expect("precompile");
        let comp = unsafe { component::load_precompiled(engine, &path, &hash).expect("load") };
        let _ = Arc::new(comp);
    }

    #[pg_test]
    fn shared_engine_returns_same_pointer_within_backend() {
        let first = engine::shared_engine();
        let second = engine::shared_engine();

        assert!(core::ptr::eq(first, second));
    }
}
