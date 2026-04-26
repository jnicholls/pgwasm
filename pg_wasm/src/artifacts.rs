//! On-disk artifact layout and helpers under `$PGDATA/pg_wasm/`.

use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Write as _},
    path::{Path, PathBuf},
    sync::OnceLock,
};

#[cfg(not(unix))]
use std::sync::Mutex;

#[cfg(unix)]
use std::os::fd::AsRawFd;

use crate::errors::PgWasmError;

pub(crate) const ARTIFACTS_DIRNAME: &str = "pg_wasm";
pub(crate) const CHECKSUM_FILENAME: &str = "sha256";
pub(crate) const MODULE_CWASM_FILENAME: &str = "module.cwasm";
pub(crate) const MODULE_WASM_FILENAME: &str = "module.wasm";
pub(crate) const WORLD_WIT_FILENAME: &str = "world.wit";

/// Serializes mutations under `$PGDATA/pg_wasm/` across PostgreSQL backends. `#[pg_test]` uses
/// many concurrent sessions against one cluster; each backend is a separate OS process, so a
/// process-local `Mutex` is insufficient — use `flock` on Unix (non-Unix falls back to `Mutex`).
#[cfg(not(unix))]
static ARTIFACT_FS_LOCK: Mutex<()> = Mutex::new(());

static DATA_DIR_CACHE: OnceLock<PathBuf> = OnceLock::new();

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

pub(crate) fn with_artifact_fs_lock<T>(f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
    #[cfg(unix)]
    {
        let root = artifacts_root_dir()?;
        let _guard = ArtifactFlockGuard::acquire(&root)?;
        f()
    }
    #[cfg(not(unix))]
    {
        let _guard = ARTIFACT_FS_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        f()
    }
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
    with_artifact_fs_lock(|| write_atomic_unlocked(path, bytes))
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
    #[cfg(test)]
    {
        use self::tests::lock_test_data_dir_override;

        if let Some(test_data_dir) = lock_test_data_dir_override().clone() {
            return Ok(test_data_dir);
        }
    }

    if let Some(cached_data_dir) = DATA_DIR_CACHE.get() {
        return Ok(cached_data_dir.clone());
    }

    #[cfg(not(test))]
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

    #[cfg(test)]
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

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        sync::{Mutex, MutexGuard, OnceLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::{
        CHECKSUM_FILENAME, MODULE_WASM_FILENAME, artifacts_root_dir, ensure_module_dir,
        module_dir_name, prune_stale, sha256_bytes, verify_checksum, write_atomic, write_checksum,
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
                "pg_wasm_artifacts_{name}_{}_{}",
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
}
