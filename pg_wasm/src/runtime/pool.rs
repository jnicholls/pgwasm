//! Bounded per-module component instance pool (Store + Instance).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use wasmtime::component::{Component, Instance, Linker};
use wasmtime::{Engine, Store};

use super::component::{self, StoreCtx};
use crate::errors::PgWasmError;
use crate::policy::EffectivePolicy;

static MODULE_POOLS: OnceLock<Mutex<HashMap<u64, Arc<PoolInner>>>> = OnceLock::new();

fn pools() -> &'static Mutex<HashMap<u64, Arc<PoolInner>>> {
    MODULE_POOLS.get_or_init(|| Mutex::new(HashMap::new()))
}

struct PoolState {
    idle: VecDeque<PooledSlot>,
    in_flight: usize,
}

struct PooledSlot {
    instance: Instance,
    store: Store<StoreCtx>,
}

pub(crate) struct PoolInner {
    capacity: usize,
    component: Arc<Component>,
    cv: Condvar,
    linker: Linker<StoreCtx>,
    module_id: u64,
    state: Mutex<PoolState>,
}

/// Handle returned from [`InstancePool::acquire`]; call [`PooledInstance::release`] when done.
pub(crate) struct PooledInstance {
    pool: Arc<PoolInner>,
    slot: Option<PooledSlot>,
}

impl PooledInstance {
    /// Return the slot to the pool.
    pub(crate) fn release(mut self) {
        if let Some(slot) = self.slot.take() {
            self.pool.return_slot(slot);
        }
    }
}

impl Drop for PooledInstance {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.pool.return_slot(slot);
        }
    }
}

impl PoolInner {
    fn return_slot(&self, slot: PooledSlot) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.idle.push_back(slot);
        if guard.in_flight > 0 {
            guard.in_flight -= 1;
        }
        self.cv.notify_all();
    }

    fn acquire(
        self: &Arc<Self>,
        engine: &Engine,
        policy: &EffectivePolicy,
    ) -> Result<PooledInstance, PgWasmError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let mut guard = match self.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };

            if let Some(slot) = guard.idle.pop_front() {
                guard.in_flight += 1;
                return Ok(PooledInstance {
                    pool: Arc::clone(self),
                    slot: Some(slot),
                });
            }

            if guard.in_flight < self.capacity {
                drop(guard);
                let slot = self.instantiate(engine, policy)?;
                let mut guard = self.state.lock().map_err(|_| {
                    PgWasmError::Internal("component pool mutex poisoned".to_string())
                })?;
                guard.in_flight += 1;
                return Ok(PooledInstance {
                    pool: Arc::clone(self),
                    slot: Some(slot),
                });
            }

            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err(PgWasmError::ResourceLimitExceeded(
                    "timed out waiting for a free pooled wasm instance".to_string(),
                ));
            }
            let (guard_after, wait) = self
                .cv
                .wait_timeout_while(guard, timeout, |s| {
                    s.idle.is_empty() && s.in_flight >= self.capacity
                })
                .map_err(|_| {
                    PgWasmError::Internal("component pool condvar wait failed".to_string())
                })?;
            guard = guard_after;
            if wait.timed_out() && guard.idle.is_empty() && guard.in_flight >= self.capacity {
                return Err(PgWasmError::ResourceLimitExceeded(
                    "timed out waiting for a free pooled wasm instance".to_string(),
                ));
            }
        }
    }

    fn instantiate(
        &self,
        engine: &Engine,
        policy: &EffectivePolicy,
    ) -> Result<PooledSlot, PgWasmError> {
        let linker = self.linker.clone();
        let ctx = component::build_store_ctx(policy)?;
        let mut store = Store::new(engine, ctx);
        let instance = linker
            .instantiate(&mut store, &self.component)
            .map_err(|error| {
                let message = error.to_string();
                if message.contains("pg-wasm:host/query") && message.contains("unknown import") {
                    PgWasmError::PermissionDenied(format!(
                        "component imports pg-wasm:host/query but SPI is disabled; enable pg_wasm.allow_spi ({message})"
                    ))
                } else {
                    PgWasmError::InvalidModule(format!(
                        "failed to instantiate pooled component: {error}"
                    ))
                }
            })?;
        Ok(PooledSlot { instance, store })
    }

    // Clears idle slots only; `in_flight` stays accurate until leases return.
    fn drain(&self) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.idle.clear();
        self.cv.notify_all();
    }
}

/// Per-module pool of pre-instantiated component instances.
pub(crate) struct InstancePool {
    pub(crate) inner: Arc<PoolInner>,
}

impl InstancePool {
    /// Create a pool for `module_id` with capacity `policy.instances_per_module` (minimum 1).
    pub(crate) fn new(
        module_id: u64,
        component: Arc<Component>,
        linker: Linker<StoreCtx>,
        policy: &EffectivePolicy,
    ) -> Result<Self, PgWasmError> {
        let capacity = policy.instances_per_module.max(1) as usize;
        let inner = Arc::new(PoolInner {
            capacity,
            component,
            cv: Condvar::new(),
            linker,
            module_id,
            state: Mutex::new(PoolState {
                idle: VecDeque::new(),
                in_flight: 0,
            }),
        });
        let mut map = pools()
            .lock()
            .map_err(|_| PgWasmError::Internal("module pool map mutex poisoned".to_string()))?;
        if map.insert(module_id, Arc::clone(&inner)).is_some() {
            return Err(PgWasmError::InvalidConfiguration(format!(
                "instance pool for module_id {module_id} already exists"
            )));
        }
        Ok(Self { inner })
    }

    pub(crate) fn acquire(
        &self,
        engine: &Engine,
        policy: &EffectivePolicy,
    ) -> Result<PooledInstance, PgWasmError> {
        self.inner.acquire(engine, policy)
    }

    pub(crate) fn drain(&self) {
        self.inner.drain();
    }
}

/// Remove and drain the pool for `module_id`, if any.
pub(crate) fn drain(module_id: u64) -> Result<(), PgWasmError> {
    let mut map = pools()
        .lock()
        .map_err(|_| PgWasmError::Internal("module pool map mutex poisoned".to_string()))?;
    if let Some(inner) = map.remove(&module_id) {
        inner.drain();
    }
    Ok(())
}
