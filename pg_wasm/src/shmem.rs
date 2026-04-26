//! Shared-memory state and generation metadata.

use std::{
    array,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering},
};

#[cfg(any(test, feature = "pg_test"))]
use std::sync::OnceLock;

use pgrx::{pg_guard, pg_sys};

/// Fixed shared-memory module metrics slot count.
///
/// If loaded modules exceed this bound, overflowed modules use process-local
/// dynamic counters and are reported as non-shared (degraded mode).
pub(crate) const SHMEM_MODULE_SLOTS: usize = 256;

/// Fixed shared-memory export metrics slot count.
///
/// If loaded exports exceed this bound, overflowed exports use process-local
/// dynamic counters and are reported as non-shared (degraded mode).
pub(crate) const SHMEM_EXPORT_SLOTS: usize = 4_096;

const ADDIN_SHMEM_INIT_LOCK_OFFSET: usize = 21;
const SHMEM_STATE_NAME: &std::ffi::CStr = c"pg_wasm.SharedState";
const CATALOG_LOCK_TRANCHE_NAME: &std::ffi::CStr = c"pg_wasm.CatalogLock";

static SHARED_STATE: AtomicPtr<SharedState> = AtomicPtr::new(std::ptr::null_mut());
#[cfg(any(test, feature = "pg_test"))]
static TEST_FALLBACK_STATE: OnceLock<&'static SharedState> = OnceLock::new();

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;

static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

/// Called once from `_PG_init`.
pub(crate) fn init() {
    unsafe {
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        request_shmem_resources();

        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
        {
            PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
            pg_sys::shmem_request_hook = Some(on_shmem_request_hook);
        }

        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(on_shmem_startup_hook);
    }
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_request_hook() {
    unsafe {
        let previous = PREV_SHMEM_REQUEST_HOOK;
        pg_sys::shmem_request_hook = previous;
        if let Some(previous_hook) = previous {
            pg_sys::submodules::ffi::pg_guard_ffi_boundary(|| previous_hook());
        }

        request_shmem_resources();
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_startup_hook() {
    unsafe {
        let previous = PREV_SHMEM_STARTUP_HOOK;
        pg_sys::shmem_startup_hook = previous;
        if let Some(previous_hook) = previous {
            pg_sys::submodules::ffi::pg_guard_ffi_boundary(|| previous_hook());
        }

        initialize_shared_state();
    }
}

unsafe fn request_shmem_resources() {
    unsafe {
        pg_sys::RequestAddinShmemSpace(std::mem::size_of::<SharedState>());
        pg_sys::RequestNamedLWLockTranche(CATALOG_LOCK_TRANCHE_NAME.as_ptr(), 1);
    }
}

unsafe fn initialize_shared_state() {
    unsafe {
        let addin_shmem_init_lock =
            &raw mut (*pg_sys::MainLWLockArray.add(ADDIN_SHMEM_INIT_LOCK_OFFSET)).lock;
        let addin_guard =
            RawLwLockGuard::acquire(addin_shmem_init_lock, pg_sys::LWLockMode::LW_EXCLUSIVE);

        let mut found = false;
        let shmem_ptr = pg_sys::ShmemInitStruct(
            SHMEM_STATE_NAME.as_ptr(),
            std::mem::size_of::<SharedState>(),
            &mut found,
        )
        .cast::<SharedState>();
        assert!(shmem_ptr.is_aligned(), "shared memory state is not aligned");

        let lock_padded = pg_sys::GetNamedLWLockTranche(CATALOG_LOCK_TRANCHE_NAME.as_ptr());
        assert!(
            !lock_padded.is_null(),
            "named catalog lock tranche is missing"
        );

        let catalog_lock = &raw mut (*lock_padded).lock;
        let tranche_id = (*catalog_lock).tranche as i32;
        pg_sys::LWLockRegisterTranche(tranche_id, CATALOG_LOCK_TRANCHE_NAME.as_ptr());

        if !found {
            shmem_ptr.write(SharedState::new(catalog_lock));
        } else {
            (*shmem_ptr)
                .catalog_lock
                .store(catalog_lock, Ordering::Release);
        }

        SHARED_STATE.store(shmem_ptr, Ordering::Release);
        drop(addin_guard);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExportCounterKind {
    Errors,
    Invocations,
    Oom,
    RejectedByPolicy,
    TotalNs,
    Traps,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShmemOverflow {
    ExportSlots,
    ModuleSlots,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SlotRefs {
    pub(crate) export_len: usize,
    pub(crate) export_start: usize,
    pub(crate) module_slot: usize,
}

#[repr(C)]
struct SharedState {
    generation: AtomicU64,
    catalog_lock: AtomicPtr<pg_sys::LWLock>,
    module_slots: [ModuleSlot; SHMEM_MODULE_SLOTS],
    export_slots: [ExportSlot; SHMEM_EXPORT_SLOTS],
}

impl SharedState {
    fn new(catalog_lock: *mut pg_sys::LWLock) -> Self {
        Self {
            generation: AtomicU64::new(0),
            catalog_lock: AtomicPtr::new(catalog_lock),
            module_slots: array::from_fn(|_| ModuleSlot::new()),
            export_slots: array::from_fn(|_| ExportSlot::new()),
        }
    }
}

#[repr(C)]
struct ModuleSlot {
    occupied: AtomicBool,
    module_id: AtomicU64,
    export_start: AtomicU32,
    export_len: AtomicU32,
    invocations: AtomicU64,
    traps: AtomicU64,
    errors: AtomicU64,
    total_ns: AtomicU64,
    rejected_by_policy: AtomicU64,
    oom: AtomicU64,
}

impl ModuleSlot {
    fn new() -> Self {
        Self {
            occupied: AtomicBool::new(false),
            module_id: AtomicU64::new(0),
            export_start: AtomicU32::new(0),
            export_len: AtomicU32::new(0),
            invocations: AtomicU64::new(0),
            traps: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            rejected_by_policy: AtomicU64::new(0),
            oom: AtomicU64::new(0),
        }
    }

    fn claim(&self, module_id: u64, export_start: usize, export_len: usize) {
        self.module_id.store(module_id, Ordering::Relaxed);
        self.export_start
            .store(export_start as u32, Ordering::Relaxed);
        self.export_len.store(export_len as u32, Ordering::Relaxed);
        self.invocations.store(0, Ordering::Relaxed);
        self.traps.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_ns.store(0, Ordering::Relaxed);
        self.rejected_by_policy.store(0, Ordering::Relaxed);
        self.oom.store(0, Ordering::Relaxed);
        self.occupied.store(true, Ordering::Release);
    }

    fn clear(&self) {
        self.occupied.store(false, Ordering::Release);
        self.module_id.store(0, Ordering::Relaxed);
        self.export_start.store(0, Ordering::Relaxed);
        self.export_len.store(0, Ordering::Relaxed);
        self.invocations.store(0, Ordering::Relaxed);
        self.traps.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_ns.store(0, Ordering::Relaxed);
        self.rejected_by_policy.store(0, Ordering::Relaxed);
        self.oom.store(0, Ordering::Relaxed);
    }

    fn counter(&self, counter_kind: ExportCounterKind) -> &AtomicU64 {
        match counter_kind {
            ExportCounterKind::Errors => &self.errors,
            ExportCounterKind::Invocations => &self.invocations,
            ExportCounterKind::Oom => &self.oom,
            ExportCounterKind::RejectedByPolicy => &self.rejected_by_policy,
            ExportCounterKind::TotalNs => &self.total_ns,
            ExportCounterKind::Traps => &self.traps,
        }
    }
}

#[repr(C)]
struct ExportSlot {
    occupied: AtomicBool,
    module_id: AtomicU64,
    export_index: AtomicU32,
    invocations: AtomicU64,
    traps: AtomicU64,
    errors: AtomicU64,
    total_ns: AtomicU64,
    rejected_by_policy: AtomicU64,
    oom: AtomicU64,
}

impl ExportSlot {
    fn new() -> Self {
        Self {
            occupied: AtomicBool::new(false),
            module_id: AtomicU64::new(0),
            export_index: AtomicU32::new(0),
            invocations: AtomicU64::new(0),
            traps: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            rejected_by_policy: AtomicU64::new(0),
            oom: AtomicU64::new(0),
        }
    }

    fn claim(&self, module_id: u64, export_index: usize) {
        self.module_id.store(module_id, Ordering::Relaxed);
        self.export_index
            .store(export_index as u32, Ordering::Relaxed);
        self.invocations.store(0, Ordering::Relaxed);
        self.traps.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_ns.store(0, Ordering::Relaxed);
        self.rejected_by_policy.store(0, Ordering::Relaxed);
        self.oom.store(0, Ordering::Relaxed);
        self.occupied.store(true, Ordering::Release);
    }

    fn clear(&self) {
        self.occupied.store(false, Ordering::Release);
        self.module_id.store(0, Ordering::Relaxed);
        self.export_index.store(0, Ordering::Relaxed);
        self.invocations.store(0, Ordering::Relaxed);
        self.traps.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.total_ns.store(0, Ordering::Relaxed);
        self.rejected_by_policy.store(0, Ordering::Relaxed);
        self.oom.store(0, Ordering::Relaxed);
    }

    fn counter(&self, counter_kind: ExportCounterKind) -> &AtomicU64 {
        match counter_kind {
            ExportCounterKind::Errors => &self.errors,
            ExportCounterKind::Invocations => &self.invocations,
            ExportCounterKind::Oom => &self.oom,
            ExportCounterKind::RejectedByPolicy => &self.rejected_by_policy,
            ExportCounterKind::TotalNs => &self.total_ns,
            ExportCounterKind::Traps => &self.traps,
        }
    }
}

/// Bump generation under `pg_wasm.CatalogLock` in exclusive mode.
pub(crate) fn bump_generation(_module_id: u64) -> u64 {
    with_catalog_lock_exclusive(|| {
        if let Some(shared_state) = shared_state_ref() {
            shared_state.generation.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            0
        }
    })
}

/// Read generation lock-free.
pub(crate) fn read_generation() -> u64 {
    if let Some(shared_state) = shared_state_ref() {
        shared_state.generation.load(Ordering::Relaxed)
    } else {
        0
    }
}

/// Increment a per-export counter lock-free.
pub(crate) fn incr_export_counter(
    module_id: u64,
    export_index: u32,
    counter_kind: ExportCounterKind,
) {
    add_export_counter(module_id, export_index, counter_kind, 1);
}

/// Add `delta` to a per-export counter lock-free (used for fuel-used metrics).
pub(crate) fn add_export_counter(
    module_id: u64,
    export_index: u32,
    counter_kind: ExportCounterKind,
    delta: u64,
) {
    if delta == 0 {
        return;
    }

    if let Some(shared_state) = shared_state_ref() {
        if let Some(slot_index) = find_export_slot_index(shared_state, module_id, export_index) {
            shared_state.export_slots[slot_index]
                .counter(counter_kind)
                .fetch_add(delta, Ordering::Relaxed);
        }

        if let Some(slot_index) = find_module_slot_index(shared_state, module_id) {
            shared_state.module_slots[slot_index]
                .counter(counter_kind)
                .fetch_add(delta, Ordering::Relaxed);
        }
    }
}

pub(crate) fn read_export_counter(
    module_id: u64,
    export_index: u32,
    counter_kind: ExportCounterKind,
) -> Option<u64> {
    let shared_state = shared_state_ref()?;
    let slot_index = find_export_slot_index(shared_state, module_id, export_index)?;
    Some(
        shared_state.export_slots[slot_index]
            .counter(counter_kind)
            .load(Ordering::Relaxed),
    )
}

/// Reserve one module slot and `n_exports` export slots under CatalogLock.
pub(crate) fn allocate_slots(module_id: u64, n_exports: usize) -> Result<SlotRefs, ShmemOverflow> {
    with_catalog_lock_exclusive(|| {
        let Some(shared_state) = shared_state_ref() else {
            return Err(ShmemOverflow::ModuleSlots);
        };

        if n_exports > SHMEM_EXPORT_SLOTS {
            return Err(ShmemOverflow::ExportSlots);
        }

        if let Some(module_slot) = find_module_slot_index(shared_state, module_id) {
            let export_start = shared_state.module_slots[module_slot]
                .export_start
                .load(Ordering::Relaxed) as usize;
            let export_len = shared_state.module_slots[module_slot]
                .export_len
                .load(Ordering::Relaxed) as usize;
            return Ok(SlotRefs {
                module_slot,
                export_start,
                export_len,
            });
        }

        let module_slot =
            find_free_module_slot_index(shared_state).ok_or(ShmemOverflow::ModuleSlots)?;

        let export_start = if n_exports == 0 {
            0
        } else {
            find_contiguous_free_range(SHMEM_EXPORT_SLOTS, n_exports, |idx| {
                shared_state.export_slots[idx]
                    .occupied
                    .load(Ordering::Acquire)
            })
            .ok_or(ShmemOverflow::ExportSlots)?
        };

        shared_state.module_slots[module_slot].claim(module_id, export_start, n_exports);
        for (offset, slot) in shared_state.export_slots[export_start..export_start + n_exports]
            .iter()
            .enumerate()
        {
            slot.claim(module_id, offset);
        }

        Ok(SlotRefs {
            module_slot,
            export_start,
            export_len: n_exports,
        })
    })
}

/// Release module and export slots under CatalogLock.
pub(crate) fn free_slots(module_id: u64) {
    with_catalog_lock_exclusive(|| {
        let Some(shared_state) = shared_state_ref() else {
            return;
        };

        if let Some(module_slot) = find_module_slot_index(shared_state, module_id) {
            let export_start = shared_state.module_slots[module_slot]
                .export_start
                .load(Ordering::Relaxed) as usize;
            let export_len = shared_state.module_slots[module_slot]
                .export_len
                .load(Ordering::Relaxed) as usize;

            for slot in &shared_state.export_slots[export_start..export_start + export_len] {
                slot.clear();
            }

            shared_state.module_slots[module_slot].clear();
        }
    });
}

pub(crate) fn with_catalog_lock_exclusive<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    with_catalog_lock_mode(pg_sys::LWLockMode::LW_EXCLUSIVE, f)
}

pub(crate) fn with_catalog_lock_shared<F, T>(f: F) -> T
where
    F: FnOnce() -> T,
{
    with_catalog_lock_mode(pg_sys::LWLockMode::LW_SHARED, f)
}

fn with_catalog_lock_mode<F, T>(mode: pg_sys::LWLockMode::Type, f: F) -> T
where
    F: FnOnce() -> T,
{
    if let Some(catalog_lock) = catalog_lock_ptr() {
        unsafe {
            let _guard = RawLwLockGuard::acquire(catalog_lock, mode);
            f()
        }
    } else {
        f()
    }
}

fn shared_state_ref() -> Option<&'static SharedState> {
    #[cfg(any(test, feature = "pg_test"))]
    {
        ensure_test_fallback_state();
    }

    let ptr = SHARED_STATE.load(Ordering::Acquire);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}

#[cfg(any(test, feature = "pg_test"))]
fn ensure_test_fallback_state() {
    if !SHARED_STATE.load(Ordering::Acquire).is_null() {
        return;
    }

    let fallback = TEST_FALLBACK_STATE
        .get_or_init(|| Box::leak(Box::new(SharedState::new(std::ptr::null_mut()))));
    SHARED_STATE.store(
        (*fallback as *const SharedState).cast_mut(),
        Ordering::Release,
    );
}

fn catalog_lock_ptr() -> Option<*mut pg_sys::LWLock> {
    shared_state_ref().and_then(|state| {
        let lock_ptr = state.catalog_lock.load(Ordering::Acquire);
        if lock_ptr.is_null() {
            None
        } else {
            Some(lock_ptr)
        }
    })
}

fn find_module_slot_index(shared_state: &SharedState, module_id: u64) -> Option<usize> {
    shared_state.module_slots.iter().position(|slot| {
        slot.occupied.load(Ordering::Acquire) && slot.module_id.load(Ordering::Relaxed) == module_id
    })
}

fn find_free_module_slot_index(shared_state: &SharedState) -> Option<usize> {
    shared_state
        .module_slots
        .iter()
        .position(|slot| !slot.occupied.load(Ordering::Acquire))
}

fn find_export_slot_index(
    shared_state: &SharedState,
    module_id: u64,
    export_index: u32,
) -> Option<usize> {
    shared_state.export_slots.iter().position(|slot| {
        slot.occupied.load(Ordering::Acquire)
            && slot.module_id.load(Ordering::Relaxed) == module_id
            && slot.export_index.load(Ordering::Relaxed) == export_index
    })
}

fn find_contiguous_free_range<F>(
    total_slots: usize,
    needed_slots: usize,
    mut occupied: F,
) -> Option<usize>
where
    F: FnMut(usize) -> bool,
{
    if needed_slots == 0 {
        return Some(0);
    }

    if needed_slots > total_slots {
        return None;
    }

    let mut run_start = 0usize;
    let mut run_len = 0usize;

    for index in 0..total_slots {
        if occupied(index) {
            run_len = 0;
            continue;
        }

        if run_len == 0 {
            run_start = index;
        }
        run_len += 1;

        if run_len == needed_slots {
            return Some(run_start);
        }
    }

    None
}

struct RawLwLockGuard {
    lock: *mut pg_sys::LWLock,
}

impl RawLwLockGuard {
    unsafe fn acquire(lock: *mut pg_sys::LWLock, mode: pg_sys::LWLockMode::Type) -> Self {
        unsafe {
            pg_sys::LWLockAcquire(lock, mode);
        }
        Self { lock }
    }
}

impl Drop for RawLwLockGuard {
    fn drop(&mut self) {
        unsafe {
            release_unless_elog_unwinding(self.lock);
        }
    }
}

unsafe fn release_unless_elog_unwinding(lock: *mut pg_sys::LWLock) {
    unsafe {
        if pg_sys::InterruptHoldoffCount > 0 {
            pg_sys::LWLockRelease(lock);
        }
    }
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use super::find_contiguous_free_range;

    #[test]
    fn contiguous_free_range_finds_expected_start() {
        let occupied = [true, true, false, false, false, true, false];
        let start = find_contiguous_free_range(occupied.len(), 3, |idx| occupied[idx]);
        assert_eq!(start, Some(2));
    }

    #[test]
    fn contiguous_free_range_handles_impossible_request() {
        let occupied = [true, false, true, false];
        let start = find_contiguous_free_range(occupied.len(), 2, |idx| occupied[idx]);
        assert_eq!(start, None);
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use std::thread;

    use pgrx::prelude::*;

    use super::{
        ExportCounterKind, allocate_slots, bump_generation, free_slots, incr_export_counter,
        read_export_counter, read_generation,
    };

    #[pg_test]
    fn generation_bumps_under_catalog_lock() {
        let module_id = 10_000_u64;
        let before = read_generation();
        let bumped = bump_generation(module_id);
        let after = read_generation();

        assert_eq!(bumped, before + 1);
        assert_eq!(after, before + 1);
    }

    #[pg_test]
    fn export_counter_increments_concurrently() {
        let module_id = 10_001_u64;
        free_slots(module_id);
        allocate_slots(module_id, 1).expect("slot allocation should succeed in shared memory");

        let workers = 8_u64;
        let increments_per_worker = 2_000_u64;

        thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    for _ in 0..increments_per_worker {
                        incr_export_counter(module_id, 0, ExportCounterKind::Invocations);
                    }
                });
            }
        });

        let observed = read_export_counter(module_id, 0, ExportCounterKind::Invocations)
            .expect("counter should exist for allocated slot");
        assert_eq!(observed, workers * increments_per_worker);
    }
}
