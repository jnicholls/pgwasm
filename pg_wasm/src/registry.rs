//! Process-local module and export registry cache.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use pgrx::pg_sys::Oid;

/// Process-local fn_oid registry cache used by the trampoline.
pub(crate) static FN_OID_MAP: OnceLock<RwLock<RegistryInner>> = OnceLock::new();

/// Registry entry for a SQL-visible export.
///
/// Additional metadata fields are expected in later waves.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegistryEntry {
    pub(crate) module_id: u64,
    pub(crate) export_index: u32,
    pub(crate) fn_oid: Oid,
}

/// Per-module view of exports tracked in the registry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ModuleEntry {
    pub(crate) module_id: u64,
    pub(crate) by_export_index: HashMap<u32, RegistryEntry>,
}

#[derive(Debug, Default)]
pub(crate) struct RegistryInner {
    generation: u64,
    by_fn_oid: HashMap<Oid, RegistryEntry>,
    by_module_id: HashMap<u64, ModuleEntry>,
}

impl RegistryInner {
    fn rebuild_from_entries(&mut self, generation: u64, entries: Vec<RegistryEntry>) {
        self.by_fn_oid.clear();
        self.by_module_id.clear();

        for entry in entries {
            let module_id = entry.module_id;
            let export_index = entry.export_index;

            self.by_fn_oid.insert(entry.fn_oid, entry.clone());
            self.by_module_id
                .entry(module_id)
                .or_insert_with(|| ModuleEntry {
                    module_id,
                    by_export_index: HashMap::new(),
                })
                .by_export_index
                .insert(export_index, entry);
        }

        self.generation = generation;
    }
}

trait GenerationSource {
    fn read(&self) -> u64;
}

trait CatalogSource {
    fn list_exports(&self) -> Vec<RegistryEntry>;
}

pub(crate) struct DefaultSources;

// TODO(shmem-and-generation, catalog-schema): swap these stubs for
// `shmem::read_generation()` and `catalog::exports::list()` once those todo
// owners land their Wave-1 changes.
impl GenerationSource for DefaultSources {
    fn read(&self) -> u64 {
        0
    }
}

impl CatalogSource for DefaultSources {
    fn list_exports(&self) -> Vec<RegistryEntry> {
        vec![]
    }
}

pub(crate) fn resolve_fn_oid(fn_oid: Oid) -> Option<RegistryEntry> {
    let sources = DefaultSources;
    resolve_fn_oid_with_sources(fn_oid, &sources)
}

pub(crate) fn refresh_from_catalog() {
    let sources = DefaultSources;
    refresh_from_catalog_with_sources(&sources);
}

fn resolve_fn_oid_with_sources<S>(fn_oid: Oid, sources: &S) -> Option<RegistryEntry>
where
    S: CatalogSource + GenerationSource,
{
    ensure_generation_current(sources);
    registry_read().by_fn_oid.get(&fn_oid).cloned()
}

fn refresh_from_catalog_with_sources<S>(sources: &S)
where
    S: CatalogSource + GenerationSource,
{
    let entries = sources.list_exports();
    let generation = sources.read();
    registry_write().rebuild_from_entries(generation, entries);
}

fn ensure_generation_current<S>(sources: &S)
where
    S: CatalogSource + GenerationSource,
{
    let generation = sources.read();
    if registry_read().generation != generation {
        refresh_from_catalog_with_sources(sources);
    }
}

fn registry_cache() -> &'static RwLock<RegistryInner> {
    FN_OID_MAP.get_or_init(|| RwLock::new(RegistryInner::default()))
}

fn registry_read() -> RwLockReadGuard<'static, RegistryInner> {
    match registry_cache().read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn registry_write() -> RwLockWriteGuard<'static, RegistryInner> {
    match registry_cache().write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};

    use pgrx::pg_sys::Oid;

    use super::{
        refresh_from_catalog_with_sources, registry_write, resolve_fn_oid_with_sources,
        CatalogSource, GenerationSource, RegistryEntry, RegistryInner,
    };

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[derive(Default)]
    struct MockSources {
        entries: Mutex<Vec<RegistryEntry>>,
        generation: AtomicU64,
        list_calls: AtomicUsize,
    }

    impl MockSources {
        fn list_call_count(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }

        fn set_entries(&self, entries: Vec<RegistryEntry>) {
            *self.entries_guard() = entries;
        }

        fn set_generation(&self, generation: u64) {
            self.generation.store(generation, Ordering::SeqCst);
        }

        fn entries_guard(&self) -> std::sync::MutexGuard<'_, Vec<RegistryEntry>> {
            match self.entries.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            }
        }
    }

    impl CatalogSource for MockSources {
        fn list_exports(&self) -> Vec<RegistryEntry> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            self.entries_guard().clone()
        }
    }

    impl GenerationSource for MockSources {
        fn read(&self) -> u64 {
            self.generation.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn resolve_fn_oid_miss_then_hit_after_refresh() {
        let _guard = test_guard();
        reset_registry();

        let sources = MockSources::default();
        let fn_oid = Oid::from(4_242_u32);
        assert_eq!(resolve_fn_oid_with_sources(fn_oid, &sources), None);

        let entry = RegistryEntry {
            module_id: 7,
            export_index: 0,
            fn_oid,
        };
        sources.set_entries(vec![entry.clone()]);
        refresh_from_catalog_with_sources(&sources);

        assert_eq!(resolve_fn_oid_with_sources(fn_oid, &sources), Some(entry));
    }

    #[test]
    fn generation_bump_triggers_refresh() {
        let _guard = test_guard();
        reset_registry();

        let sources = MockSources::default();
        let initial = RegistryEntry {
            module_id: 11,
            export_index: 0,
            fn_oid: Oid::from(501_u32),
        };
        sources.set_entries(vec![initial.clone()]);
        sources.set_generation(1);
        refresh_from_catalog_with_sources(&sources);

        assert_eq!(sources.list_call_count(), 1);
        assert_eq!(
            resolve_fn_oid_with_sources(initial.fn_oid, &sources),
            Some(initial)
        );
        assert_eq!(sources.list_call_count(), 1);

        let refreshed = RegistryEntry {
            module_id: 11,
            export_index: 1,
            fn_oid: Oid::from(777_u32),
        };
        sources.set_entries(vec![refreshed.clone()]);
        sources.set_generation(2);

        assert_eq!(
            resolve_fn_oid_with_sources(refreshed.fn_oid, &sources),
            Some(refreshed)
        );
        assert_eq!(sources.list_call_count(), 2);
        assert_eq!(
            resolve_fn_oid_with_sources(Oid::from(501_u32), &sources),
            None
        );
    }

    fn reset_registry() {
        *registry_write() = RegistryInner::default();
    }

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        match TEST_LOCK.get_or_init(|| Mutex::new(())).lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
