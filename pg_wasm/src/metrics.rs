//! Per-export invocation counters and timings; optional sampled guest linear memory (plan §7–8).
//!
//! Stats are **process-local** (each PostgreSQL backend has its own counters).

#[cfg(feature = "_pg_wasm_runtime")]
use std::{
    collections::HashMap,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[cfg(feature = "_pg_wasm_runtime")]
use crate::registry::ModuleId;

#[cfg(feature = "_pg_wasm_runtime")]
static MEMORY_PEAK_BYTES: OnceLock<Mutex<HashMap<ModuleId, u64>>> = OnceLock::new();

#[cfg(feature = "_pg_wasm_runtime")]
fn memory_peaks() -> &'static Mutex<HashMap<ModuleId, u64>> {
    MEMORY_PEAK_BYTES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Counters updated from the trampoline on each WASM call (when collection is enabled).
#[cfg(feature = "_pg_wasm_runtime")]
#[derive(Debug, Default)]
pub struct ExportStats {
    pub(super) invocations: AtomicU64,
    pub(super) errors: AtomicU64,
    pub(super) total_time_ns: AtomicU64,
}

#[cfg(feature = "_pg_wasm_runtime")]
impl ExportStats {
    #[must_use]
    pub fn invocations(&self) -> u64 {
        self.invocations.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn total_time_ns(&self) -> u64 {
        self.total_time_ns.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "_pg_wasm_runtime")]
#[must_use]
pub fn alloc_export_stats() -> std::sync::Arc<ExportStats> {
    std::sync::Arc::new(ExportStats::default())
}

#[cfg(feature = "_pg_wasm_runtime")]
#[must_use]
pub fn collecting() -> bool {
    crate::guc::collect_metrics()
}

#[cfg(feature = "_pg_wasm_runtime")]
#[must_use]
pub fn timer_start() -> Option<Instant> {
    collecting().then(Instant::now)
}

#[cfg(feature = "_pg_wasm_runtime")]
pub fn timer_finish_ok(stats: &ExportStats, start: Option<Instant>) {
    if !collecting() {
        return;
    }
    let Some(t0) = start else { return };
    let elapsed = t0.elapsed();
    stats.invocations.fetch_add(1, Ordering::Relaxed);
    stats
        .total_time_ns
        .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
}

#[cfg(feature = "_pg_wasm_runtime")]
pub fn timer_finish_err(stats: &ExportStats, _start: Option<Instant>) {
    if !collecting() {
        return;
    }
    stats.errors.fetch_add(1, Ordering::Relaxed);
}

#[cfg(feature = "_pg_wasm_runtime")]
pub fn record_memory_sample(module: ModuleId, byte_size: u64) {
    if !collecting() || byte_size == 0 {
        return;
    }
    let mut g = memory_peaks()
        .lock()
        .expect("pg_wasm metrics memory peak map poisoned");
    let e = g.entry(module).or_insert(0);
    *e = (*e).max(byte_size);
}

#[cfg(feature = "_pg_wasm_runtime")]
#[must_use]
pub fn guest_memory_peak_bytes(module: ModuleId) -> Option<u64> {
    let g = memory_peaks()
        .lock()
        .expect("pg_wasm metrics memory peak map poisoned");
    g.get(&module).copied()
}

#[cfg(feature = "_pg_wasm_runtime")]
pub fn remove_module_memory_peak(module: ModuleId) {
    let mut g = memory_peaks()
        .lock()
        .expect("pg_wasm metrics memory peak map poisoned");
    g.remove(&module);
}
