//! On-disk artifact layout and helpers under `$PGDATA/pgwasm/`.
//!
//! ## `pg_upgrade` and extension upgrades
//!
//! - During `pg_upgrade`, the data directory (including `$PGDATA/pgwasm/<module_id>/`) is carried
//!   into the new cluster via the usual data-dir copy / hard-link strategy.
//! - On the first backend start after a new Postgres major, [`check_compat`] may return
//!   [`CompatCheck::StaleRecompile`] when the Wasmtime engine’s precompile compatibility fingerprint
//!   no longer matches the `compat_hash` sidecar next to `module.cwasm`, or when
//!   [`wasmtime::Engine::detect_precompiled_file`] disagrees with the expected artifact kind. Callers
//!   then drop the precompiled blob (see [`invalidate_cwasm`]) and rebuild from `module.wasm`.
//! - Catalog rows are migrated by the extension’s normal dump/restore path; the `wit_world` column
//!   remains authoritative for reconstituting WIT / UDT shape.

use sha2::{Digest, Sha256};
use std::{
    cell::Cell,
    collections::{BTreeSet, hash_map::DefaultHasher},
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{self, ErrorKind, Write as _},
    path::{Path, PathBuf},
    sync::OnceLock,
};

#[cfg(not(unix))]
use std::sync::Mutex;

#[cfg(unix)]
use std::os::fd::AsRawFd;

use wasmtime::{Engine, Precompiled};

use crate::errors::PgWasmError;

pub(crate) const ARTIFACTS_DIRNAME: &str = "pgwasm";
pub(crate) const COMPAT_HASH_FILENAME: &str = "compat_hash";
pub(crate) const CHECKSUM_FILENAME: &str = "sha256";
pub(crate) const MODULE_CWASM_FILENAME: &str = "module.cwasm";
pub(crate) const MODULE_WASM_FILENAME: &str = "module.wasm";
pub(crate) const WORLD_WIT_FILENAME: &str = "world.wit";

/// Serializes mutations under `$PGDATA/pgwasm/` across PostgreSQL backends. `#[pg_test]` uses
/// many concurrent sessions against one cluster; each backend is a separate OS process, so a
/// process-local `Mutex` is insufficient — use `flock` on Unix (non-Unix falls back to `Mutex`).
#[cfg(not(unix))]
static ARTIFACT_FS_LOCK: Mutex<()> = Mutex::new(());

static DATA_DIR_CACHE: OnceLock<PathBuf> = OnceLock::new();

thread_local! {
    /// Nesting depth for [`with_artifact_fs_lock_result`] / [`with_artifact_fs_lock`]. Values `> 0`
    /// mean this thread already holds the process-global artifact `flock` (or `Mutex` fallback).
    static ARTIFACT_FS_NEST: Cell<u32> = const { Cell::new(0) };
}

#[cfg(unix)]
struct ArtifactFlockGuard(File);

#[cfg(unix)]
impl ArtifactFlockGuard {
    fn acquire(root: &Path) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let path = root.join(".artifact_io.lock");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .truncate(true)
            .write(true)
            .open(&path)?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(file))
    }
}

/// Run `f` while holding the cross-process artifact lock (`flock` on Unix).
///
/// Re-entrant: nested calls (e.g. `write_atomic` during `load_impl`, or abort cleanup while a
/// load still holds the lock) skip a second `flock` and run `f` directly.
pub(crate) fn with_artifact_fs_lock_result<T, F>(f: F) -> Result<T, PgWasmError>
where
    F: FnOnce() -> Result<T, PgWasmError>,
{
    let n = ARTIFACT_FS_NEST.with(|c| c.get());
    if n > 0 {
        ARTIFACT_FS_NEST.with(|c| c.set(n.saturating_add(1)));
        struct NestDec;
        impl Drop for NestDec {
            fn drop(&mut self) {
                ARTIFACT_FS_NEST.with(|c| c.set(c.get().saturating_sub(1)));
            }
        }
        let _nest_dec = NestDec;
        return f();
    }

    #[cfg(unix)]
    {
        let root = artifacts_root_dir().map_err(PgWasmError::Io)?;
        let flock_guard = ArtifactFlockGuard::acquire(&root).map_err(PgWasmError::Io)?;
        ARTIFACT_FS_NEST.with(|c| c.set(1));
        struct OuterClear;
        impl Drop for OuterClear {
            fn drop(&mut self) {
                ARTIFACT_FS_NEST.with(|c| c.set(0));
            }
        }
        let _outer_clear = OuterClear;
        let _flock = flock_guard;
        f()
    }
    #[cfg(not(unix))]
    {
        let mutex_guard = ARTIFACT_FS_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ARTIFACT_FS_NEST.with(|c| c.set(1));
        struct OuterClear;
        impl Drop for OuterClear {
            fn drop(&mut self) {
                ARTIFACT_FS_NEST.with(|c| c.set(0));
            }
        }
        let _outer_clear = OuterClear;
        let _mutex_guard = mutex_guard;
        f()
    }
}

pub(crate) fn with_artifact_fs_lock<T>(f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    with_artifact_fs_lock_result(|| f().map_err(PgWasmError::Io)).map_err(|e| match e {
        PgWasmError::Io(io) => io,
        other => io::Error::other(other.to_string()),
    })
}

pub(crate) fn artifacts_root_dir() -> io::Result<PathBuf> {
    Ok(resolve_data_dir()?.join(ARTIFACTS_DIRNAME))
}

pub(crate) fn module_dir_name(module_id: u64) -> String {
    format!("{module_id:016x}")
}

pub(crate) fn module_dir(module_id: u64) -> io::Result<PathBuf> {
    Ok(artifacts_root_dir()?.join(module_dir_name(module_id)))
}

pub(crate) fn ensure_module_dir(module_id: u64) -> io::Result<PathBuf> {
    let module_dir = module_dir(module_id)?;
    fs::create_dir_all(&module_dir)?;
    Ok(module_dir)
}

pub(crate) fn module_wasm_path(module_id: u64) -> io::Result<PathBuf> {
    Ok(module_dir(module_id)?.join(MODULE_WASM_FILENAME))
}

pub(crate) fn module_cwasm_path(module_id: u64) -> io::Result<PathBuf> {
    Ok(module_dir(module_id)?.join(MODULE_CWASM_FILENAME))
}

pub(crate) fn world_wit_path(module_id: u64) -> io::Result<PathBuf> {
    Ok(module_dir(module_id)?.join(WORLD_WIT_FILENAME))
}

pub(crate) fn write_module_wasm(module_id: u64, bytes: &[u8]) -> io::Result<PathBuf> {
    let module_dir = ensure_module_dir(module_id)?;
    let path = module_dir.join(MODULE_WASM_FILENAME);
    write_atomic(&path, bytes)?;
    Ok(path)
}

pub(crate) fn write_module_cwasm(module_id: u64, bytes: &[u8]) -> io::Result<PathBuf> {
    let module_dir = ensure_module_dir(module_id)?;
    let path = module_dir.join(MODULE_CWASM_FILENAME);
    write_atomic(&path, bytes)?;
    Ok(path)
}

pub(crate) fn write_world_wit(module_id: u64, world_wit: &str) -> io::Result<PathBuf> {
    let module_dir = ensure_module_dir(module_id)?;
    let path = module_dir.join(WORLD_WIT_FILENAME);
    write_atomic(&path, world_wit.as_bytes())?;
    Ok(path)
}

pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    // Serialize all writes that participate in the real `$PGDATA/pgwasm/` tree. Do not use
    // `Path::starts_with(artifacts_root)` — symlinks / normalization can make that false for real
    // artifact paths and skip the lock, letting parallel `#[pg_test]` backends race and delete
    // each other's module trees.
    //
    // Host-only unit tests have no `DataDir`; [`artifacts_root_dir`] errors and we skip `flock`
    // (writes go to temp paths in a single process).
    match artifacts_root_dir() {
        Ok(_) => with_artifact_fs_lock(|| write_atomic_unlocked(path, bytes)),
        Err(_) => write_atomic_unlocked(path, bytes),
    }
}

fn write_atomic_unlocked(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("artifact path has no parent: {}", path.display()),
        )
    })?;
    fs::create_dir_all(parent)?;

    let temp_path = temp_sibling_path(path)?;
    let write_result = (|| -> io::Result<()> {
        let mut temp_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)?;
        temp_file.write_all(bytes)?;
        temp_file.sync_all()?;
        fs::rename(&temp_path, path)?;
        fsync_dir(parent)?;
        Ok(())
    })();

    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }

    write_result
}

pub(crate) fn sha256_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

pub(crate) fn write_checksum(module_dir: &Path, sha: &[u8; 32]) -> io::Result<()> {
    let sidecar_content = format!("{}  {MODULE_WASM_FILENAME}\n", sha256_hex(sha));
    write_atomic(
        &module_dir.join(CHECKSUM_FILENAME),
        sidecar_content.as_bytes(),
    )
}

pub(crate) fn verify_checksum(module_dir: &Path) -> Result<(), PgWasmError> {
    let module_bytes = fs::read(module_dir.join(MODULE_WASM_FILENAME))?;
    let expected_sha = parse_checksum_sidecar(&module_dir.join(CHECKSUM_FILENAME))?;
    let actual_sha = sha256_bytes(&module_bytes);

    if actual_sha != expected_sha {
        return Err(PgWasmError::InvalidModule(format!(
            "checksum mismatch for {}",
            module_dir.join(MODULE_WASM_FILENAME).display()
        )));
    }

    Ok(())
}

pub(crate) fn prune_stale(active_ids: &BTreeSet<u64>) -> io::Result<usize> {
    with_artifact_fs_lock(|| prune_stale_unlocked(active_ids))
}

pub(crate) fn prune_stale_unlocked(active_ids: &BTreeSet<u64>) -> io::Result<usize> {
    let root_dir = artifacts_root_dir()?;
    if !root_dir.exists() {
        return Ok(0);
    }

    let mut pruned = 0usize;
    for entry in fs::read_dir(root_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let dir_name = entry.file_name();
        let Some(dir_name) = dir_name.to_str() else {
            continue;
        };
        let Some(module_id) = parse_module_id_dir_name(dir_name) else {
            continue;
        };

        if active_ids.contains(&module_id) {
            continue;
        }

        fs::remove_dir_all(entry.path())?;
        pruned += 1;
    }

    Ok(pruned)
}

fn resolve_data_dir() -> io::Result<PathBuf> {
    #[cfg(all(test, not(feature = "pg_test")))]
    {
        use self::host_tests::lock_test_data_dir_override;

        if let Some(test_data_dir) = lock_test_data_dir_override().clone() {
            return Ok(test_data_dir);
        }
    }

    if let Some(cached_data_dir) = DATA_DIR_CACHE.get() {
        return Ok(cached_data_dir.clone());
    }

    #[cfg(any(not(test), feature = "pg_test"))]
    {
        // SAFETY: PostgreSQL owns DataDir and keeps it valid for backend lifetime.
        let data_dir_ptr = unsafe { pgrx::pg_sys::DataDir };
        if data_dir_ptr.is_null() {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                "PostgreSQL DataDir is not initialized",
            ));
        }

        // SAFETY: DataDir is a NUL-terminated C string when backend is initialized.
        let data_dir = unsafe { std::ffi::CStr::from_ptr(data_dir_ptr.cast()) }
            .to_str()
            .map_err(|error| {
                io::Error::new(
                    ErrorKind::InvalidData,
                    format!("PostgreSQL DataDir is not valid UTF-8: {error}"),
                )
            })?;

        let data_dir = PathBuf::from(data_dir);
        let _ = DATA_DIR_CACHE.set(data_dir.clone());
        Ok(data_dir)
    }

    #[cfg(all(test, not(feature = "pg_test")))]
    {
        Err(io::Error::new(
            ErrorKind::NotFound,
            "PostgreSQL DataDir is unavailable in host tests; set test data dir override first",
        ))
    }
}

fn temp_sibling_path(path: &Path) -> io::Result<PathBuf> {
    let filename = path.file_name().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!("artifact path has no file name: {}", path.display()),
        )
    })?;
    let mut temp_filename = filename.to_os_string();
    temp_filename.push(".tmp");
    Ok(path.with_file_name(temp_filename))
}

fn fsync_dir(path: &Path) -> io::Result<()> {
    let directory = File::open(path)?;
    directory.sync_all()
}

fn parse_module_id_dir_name(dir_name: &str) -> Option<u64> {
    if dir_name.len() != 16 {
        return None;
    }
    if !dir_name.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(dir_name, 16).ok()
}

fn parse_checksum_sidecar(sidecar_path: &Path) -> Result<[u8; 32], PgWasmError> {
    let contents = fs::read_to_string(sidecar_path)?;
    let line = contents
        .lines()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| PgWasmError::InvalidModule("checksum sidecar is empty".to_string()))?;

    let mut fields = line.split_ascii_whitespace();
    let checksum_hex = fields
        .next()
        .ok_or_else(|| PgWasmError::InvalidModule("checksum sidecar is malformed".to_string()))?;
    let filename = fields
        .next()
        .ok_or_else(|| PgWasmError::InvalidModule("checksum sidecar is malformed".to_string()))?;
    if fields.next().is_some() {
        return Err(PgWasmError::InvalidModule(
            "checksum sidecar has unexpected fields".to_string(),
        ));
    }
    if filename != MODULE_WASM_FILENAME {
        return Err(PgWasmError::InvalidModule(format!(
            "checksum sidecar references unexpected file: {filename}"
        )));
    }

    decode_sha256_hex(checksum_hex)
}

fn decode_sha256_hex(input: &str) -> Result<[u8; 32], PgWasmError> {
    if !input.is_ascii() || input.len() != 64 {
        return Err(PgWasmError::InvalidModule(
            "checksum sidecar must contain 64 lowercase hex characters".to_string(),
        ));
    }

    let mut bytes = [0u8; 32];
    for (index, chunk) in input.as_bytes().chunks_exact(2).enumerate() {
        let chunk = std::str::from_utf8(chunk).map_err(|error| {
            PgWasmError::InvalidModule(format!("checksum contains invalid UTF-8: {error}"))
        })?;
        bytes[index] = u8::from_str_radix(chunk, 16).map_err(|error| {
            PgWasmError::InvalidModule(format!("checksum contains non-hex bytes: {error}"))
        })?;
    }

    Ok(bytes)
}

fn sha256_hex(sha: &[u8; 32]) -> String {
    let mut text = String::with_capacity(64);
    for byte in sha {
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

fn compat_hash_hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        let _ = write!(&mut text, "{byte:02x}");
    }
    text
}

fn engine_precompile_fingerprint(engine: &Engine) -> [u8; 32] {
    let mut hasher = DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    let u = hasher.finish();
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&u.to_le_bytes());
    out
}

fn decode_compat_hash_hex(input: &str) -> io::Result<Vec<u8>> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(Vec::new());
    }
    if !input.is_ascii() || !input.len().is_multiple_of(2) {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "compat_hash must contain an even number of ASCII hex digits",
        ));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for chunk in input.as_bytes().chunks_exact(2) {
        let chunk = std::str::from_utf8(chunk).map_err(|error| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("compat_hash contains invalid UTF-8: {error}"),
            )
        })?;
        let byte = u8::from_str_radix(chunk, 16).map_err(|error| {
            io::Error::new(
                ErrorKind::InvalidData,
                format!("compat_hash contains non-hex bytes: {error}"),
            )
        })?;
        bytes.push(byte);
    }
    Ok(bytes)
}

/// Expected precompiled artifact kind on disk (`module.cwasm`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExpectedKind {
    Component,
    Core,
}

/// Outcome of validating `module.cwasm` and its `compat_hash` sidecar against a live [`Engine`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CompatCheck {
    MissingRecompile,
    Ok,
    StaleRecompile,
}

pub(crate) fn write_compat_hash(module_dir: &Path, hash: &[u8]) -> io::Result<()> {
    let text = format!("{}\n", compat_hash_hex(hash));
    write_atomic(&module_dir.join(COMPAT_HASH_FILENAME), text.as_bytes())
}

pub(crate) fn read_compat_hash(module_dir: &Path) -> io::Result<Option<Vec<u8>>> {
    let path = module_dir.join(COMPAT_HASH_FILENAME);
    match fs::read_to_string(&path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
        Ok(contents) => {
            let line = contents
                .lines()
                .find(|line| !line.trim().is_empty())
                .unwrap_or("");
            if line.trim().is_empty() {
                return Ok(None);
            }
            decode_compat_hash_hex(line).map(Some)
        }
    }
}

pub(crate) fn check_compat(
    module_dir: &Path,
    engine: &Engine,
    expected_kind: ExpectedKind,
) -> Result<CompatCheck, PgWasmError> {
    let cwasm_path = module_dir.join(MODULE_CWASM_FILENAME);
    match fs::metadata(&cwasm_path) {
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(CompatCheck::MissingRecompile);
        }
        Err(error) => return Err(PgWasmError::Io(error)),
        Ok(metadata) if !metadata.is_file() => {
            return Ok(CompatCheck::MissingRecompile);
        }
        Ok(_) => {}
    }

    let detected = Engine::detect_precompiled_file(&cwasm_path).map_err(|error| {
        PgWasmError::InvalidModule(format!("precompiled file detection failed: {error}"))
    })?;

    let kind_ok = matches!(
        (expected_kind, detected),
        (ExpectedKind::Component, Some(Precompiled::Component))
            | (ExpectedKind::Core, Some(Precompiled::Module))
    );
    if !kind_ok {
        return Ok(CompatCheck::StaleRecompile);
    }

    let sidecar = read_compat_hash(module_dir).map_err(PgWasmError::Io)?;
    let Some(stored) = sidecar else {
        return Ok(CompatCheck::StaleRecompile);
    };

    let expected = engine_precompile_fingerprint(engine);
    if stored.as_slice() != expected.as_slice() {
        return Ok(CompatCheck::StaleRecompile);
    }

    Ok(CompatCheck::Ok)
}

pub(crate) fn invalidate_cwasm(module_dir: &Path) -> io::Result<()> {
    let cwasm = module_dir.join(MODULE_CWASM_FILENAME);
    match fs::remove_file(&cwasm) {
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        other => other?,
    }
    let sidecar = module_dir.join(COMPAT_HASH_FILENAME);
    match fs::remove_file(&sidecar) {
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        other => other?,
    }
    Ok(())
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::{Mutex, MutexGuard, OnceLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    use wasmtime::{Config, Engine};

    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};
    use wit_parser::Resolve;

    use super::{
        CHECKSUM_FILENAME, COMPAT_HASH_FILENAME, CompatCheck, ExpectedKind, MODULE_CWASM_FILENAME,
        MODULE_WASM_FILENAME, WORLD_WIT_FILENAME, artifacts_root_dir, check_compat,
        engine_precompile_fingerprint, ensure_module_dir, invalidate_cwasm, module_dir_name,
        prune_stale, read_compat_hash, sha256_bytes, verify_checksum, write_atomic, write_checksum,
        write_compat_hash,
    };

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new(name: &str) -> std::io::Result<Self> {
            let mut path = env::temp_dir();
            let suffix = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            path.push(format!(
                "pgwasm_artifacts_{name}_{}_{}",
                process::id(),
                suffix
            ));
            fs::create_dir_all(&path)?;
            Ok(Self { path })
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    struct DataDirGuard;
    static TEST_DATA_DIR_OVERRIDE: Mutex<Option<PathBuf>> = Mutex::new(None);

    static TEST_DATA_DIR_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct TestDataDirLockGuard {
        _guard: MutexGuard<'static, ()>,
    }

    impl TestDataDirLockGuard {
        fn lock() -> Self {
            let guard = TEST_DATA_DIR_LOCK
                .get_or_init(|| Mutex::new(()))
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            Self { _guard: guard }
        }
    }

    fn lock_test_data_dir() -> TestDataDirLockGuard {
        TestDataDirLockGuard::lock()
    }

    pub(super) fn lock_test_data_dir_override() -> MutexGuard<'static, Option<PathBuf>> {
        match TEST_DATA_DIR_OVERRIDE.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    impl DataDirGuard {
        fn new(path: PathBuf) -> Self {
            *lock_test_data_dir_override() = Some(path);
            Self
        }
    }

    impl Drop for DataDirGuard {
        fn drop(&mut self) {
            *lock_test_data_dir_override() = None;
        }
    }

    #[test]
    fn write_atomic_round_trip_cleans_temp_file() {
        let _test_lock = lock_test_data_dir();
        let test_dir = TestDir::new("write_atomic").unwrap();
        let _data_dir_guard = DataDirGuard::new(test_dir.path.clone());

        let path = test_dir.path.join("artifact.bin");
        write_atomic(&path, b"first write").unwrap();
        write_atomic(&path, b"second write").unwrap();

        let temp_sibling = test_dir.path.join("artifact.bin.tmp");
        assert_eq!(fs::read(&path).unwrap(), b"second write");
        assert!(!temp_sibling.exists());
    }

    #[test]
    fn sha256_round_trip() {
        let _test_lock = lock_test_data_dir();
        let test_dir = TestDir::new("sha256").unwrap();
        let _data_dir_guard = DataDirGuard::new(test_dir.path.clone());

        let module_id = 0xfeed_beef_u64;
        let module_dir = ensure_module_dir(module_id).unwrap();
        let wasm_path = module_dir.join(MODULE_WASM_FILENAME);

        let wasm = b"\0asm";
        write_atomic(&wasm_path, wasm).unwrap();
        write_checksum(&module_dir, &sha256_bytes(wasm)).unwrap();
        verify_checksum(&module_dir).unwrap();

        write_atomic(&wasm_path, b"different bytes").unwrap();
        assert!(verify_checksum(&module_dir).is_err());
        assert!(module_dir.join(CHECKSUM_FILENAME).exists());
    }

    #[test]
    fn prune_stale_removes_only_orphaned_module_dirs() {
        let _test_lock = lock_test_data_dir();
        let test_dir = TestDir::new("prune_stale").unwrap();
        let _data_dir_guard = DataDirGuard::new(test_dir.path.clone());
        let root = artifacts_root_dir().unwrap();
        fs::create_dir_all(&root).unwrap();

        let active = 0x0000_0000_0000_00aa_u64;
        let stale = 0x0000_0000_0000_00bb_u64;
        let phantom = 0x0000_0000_0000_00cc_u64;

        fs::create_dir_all(root.join(module_dir_name(active))).unwrap();
        fs::create_dir_all(root.join(module_dir_name(stale))).unwrap();
        fs::create_dir_all(root.join("not-a-module-dir")).unwrap();
        fs::write(root.join("note.txt"), b"metadata").unwrap();

        let active_ids = std::collections::BTreeSet::from([active, phantom]);
        let pruned = prune_stale(&active_ids).unwrap();

        assert_eq!(pruned, 1);
        assert!(root.join(module_dir_name(active)).exists());
        assert!(!root.join(module_dir_name(stale)).exists());
        assert!(root.join("not-a-module-dir").exists());
    }

    fn test_engine() -> Engine {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);
        config.parallel_compilation(false);
        Engine::new(&config).unwrap()
    }

    fn trivial_component_bytes() -> Vec<u8> {
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut resolve = Resolve::default();
        let wit = "package test:compat; world w { }";
        let pkg = resolve.push_str("fixture.wit", wit).unwrap();
        let world_id = resolve.select_world(&[pkg], Some("w")).unwrap();
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8).unwrap();
        ComponentEncoder::default()
            .module(&module)
            .unwrap()
            .validate(true)
            .encode()
            .unwrap()
    }

    #[test]
    fn compat_hash_round_trip() {
        let _test_lock = lock_test_data_dir();
        let test_dir = TestDir::new("compat_hash").unwrap();
        let _data_dir_guard = DataDirGuard::new(test_dir.path.clone());

        let module_id = 0xc0ffee_u64;
        let module_dir = ensure_module_dir(module_id).unwrap();
        let fp = engine_precompile_fingerprint(&test_engine());
        write_compat_hash(&module_dir, &fp).unwrap();
        let read_back = read_compat_hash(&module_dir).unwrap().unwrap();
        assert_eq!(read_back, fp);
    }

    #[test]
    fn invalidate_cwasm_removes_precompiled_leaves_sources() {
        let _test_lock = lock_test_data_dir();
        let test_dir = TestDir::new("invalidate_cwasm").unwrap();
        let _data_dir_guard = DataDirGuard::new(test_dir.path.clone());

        let module_id = 0xdecaf_u64;
        let module_dir = ensure_module_dir(module_id).unwrap();
        let engine = test_engine();
        let bytes = trivial_component_bytes();
        let cwasm = engine.precompile_component(&bytes).unwrap();
        write_atomic(&module_dir.join(MODULE_CWASM_FILENAME), &cwasm).unwrap();
        write_compat_hash(&module_dir, &engine_precompile_fingerprint(&engine)).unwrap();
        write_atomic(&module_dir.join(MODULE_WASM_FILENAME), b"\0asm").unwrap();
        write_atomic(&module_dir.join(WORLD_WIT_FILENAME), b"package x;").unwrap();

        invalidate_cwasm(&module_dir).unwrap();

        assert!(!module_dir.join(MODULE_CWASM_FILENAME).exists());
        assert!(!module_dir.join(COMPAT_HASH_FILENAME).exists());
        assert!(module_dir.join(MODULE_WASM_FILENAME).exists());
        assert!(module_dir.join(WORLD_WIT_FILENAME).exists());
    }

    #[test]
    fn check_compat_reports_stale_when_sidecar_hash_wrong() {
        let _test_lock = lock_test_data_dir();
        let test_dir = TestDir::new("check_compat_stale").unwrap();
        let _data_dir_guard = DataDirGuard::new(test_dir.path.clone());

        let module_id = 0xbadc0de_u64;
        let module_dir = ensure_module_dir(module_id).unwrap();
        let engine = test_engine();
        let bytes = trivial_component_bytes();
        let cwasm = engine.precompile_component(&bytes).unwrap();
        write_atomic(&module_dir.join(MODULE_CWASM_FILENAME), &cwasm).unwrap();

        let wrong = [0xffu8; 32];
        write_compat_hash(&module_dir, &wrong).unwrap();
        assert_eq!(
            check_compat(&module_dir, &engine, ExpectedKind::Component).unwrap(),
            CompatCheck::StaleRecompile
        );

        write_compat_hash(&module_dir, &engine_precompile_fingerprint(&engine)).unwrap();
        assert_eq!(
            check_compat(&module_dir, &engine, ExpectedKind::Component).unwrap(),
            CompatCheck::Ok
        );
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod artifact_pg_tests {
    use pgrx::prelude::*;

    /// Nested `tests` schema: pgrx's harness invokes `tests.<fn>()` for `#[pg_test]`.
    #[pg_schema]
    mod tests {
        use pgrx::prelude::*;
        use pgrx::spi::Spi;

        #[pg_test]
        fn extension_bootstrap_sql_is_installed() {
            Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        }
    }
}
