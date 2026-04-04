//! `load_from_bytes`, path resolution, and catalog registration (plan §8).

use std::path::{Path, PathBuf};

use pgrx::{JsonB, prelude::*, spi::Spi};

use crate::{
    abi::{self, WasmAbiKind},
    config::{LoadOptions, merge_policy_overrides, merge_resource_limits},
    guc::{
        allow_load_from_file, allowed_path_prefixes_raw, effective_host_policy, max_module_bytes,
        module_path_cstr,
    },
    proc_reg::{self, RegisterError},
    registry::{self, ModuleCatalogEntry, ModuleHooks, ModuleId, RegisteredFunction},
    runtime::{dispatch, selection::resolve_load_backend},
};

const WASM_MAGIC: &[u8; 4] = b"\0asm";

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("pg_wasm: {0}")]
    Message(String),
}

impl From<RegisterError> for LoadError {
    fn from(e: RegisterError) -> Self {
        LoadError::Message(e.to_string())
    }
}

fn cleanup_failed_load(id: ModuleId, oids: &[pgrx::pg_sys::Oid], backend: crate::runtime::ModuleExecutionBackend) {
    for o in oids {
        proc_reg::drop_wasm_trampoline_proc(*o);
    }
    let _ = registry::take_module_proc_oids(id);
    registry::take_module_wasi_and_policy(id);
    crate::metrics::remove_module_memory_peak(id);
    dispatch::remove_compiled_module(backend, id);
}

/// Internal entry for both `pg_wasm_load` overloads.
pub fn load_from_bytes(
    wasm: &[u8],
    module_name: Option<&str>,
    options: Option<JsonB>,
) -> Result<ModuleId, LoadError> {
    if !unsafe { pg_sys::superuser() } {
        return Err(LoadError::Message(
            "pg_wasm_load requires a superuser session".into(),
        ));
    }
    enforce_size_limit(wasm.len())?;
    validate_wasm_prefix(wasm)?;
    let opts = LoadOptions::from_jsonb(options);
    let abi = match opts.abi_override.as_deref() {
        Some(s) => abi::parse_abi_override(s).ok_or_else(|| {
            LoadError::Message(format!(
                "pg_wasm_load: unknown abi override {s:?} (use core, extism, or component)"
            ))
        })?,
        None => abi::detect_wasm_abi(wasm).map_err(|e| LoadError::Message(e.to_string()))?,
    };

    let backend = resolve_load_backend(abi, opts.runtime.as_deref()).map_err(LoadError::Message)?;

    let schema = extension_schema_name_spi()?;
    let id = registry::alloc_module_id();
    let prefix = module_sql_prefix(module_name, id)?;

    let export_hints = opts.export_hints().map_err(LoadError::Message)?;
    registry::record_module_resource_limits(id, opts.resource_limits);
    registry::record_module_policy_overrides(id, opts.policy);
    let (exports, needs_wasi) =
        match dispatch::compile_store_and_list_exports(backend, id, wasm, &export_hints, abi) {
            Ok(x) => x,
            Err(e) => {
                registry::take_module_wasi_and_policy(id);
                return Err(LoadError::Message(e));
            }
        };

    let effective = effective_host_policy(&opts.policy);
    if needs_wasi && !effective.allow_wasi {
        cleanup_failed_load(id, &[], backend);
        return Err(LoadError::Message(
            "wasm imports WASI but effective policy denies WASI (enable pg_wasm.allow_wasi and ensure per-module allow_wasi is not false)"
                .into(),
        ));
    }

    if exports.is_empty() {
        cleanup_failed_load(id, &[], backend);
        return Err(LoadError::Message(
            "no supported wasm function exports (int/float scalars, or use options \"exports\" for text/bytea/jsonb)".into(),
        ));
    }

    let mut oids = Vec::new();
    for (export_name, sig) in exports {
        if let Err(e) = proc_reg::assert_sql_identifier(&export_name) {
            cleanup_failed_load(id, &oids, backend);
            return Err(e.into());
        }
        let sql_basename = format!("{prefix}_{export_name}");
        if let Err(e) = proc_reg::assert_sql_identifier(&sql_basename) {
            cleanup_failed_load(id, &oids, backend);
            return Err(e.into());
        }

        let arg_oids: Vec<_> = sig.args.iter().map(|a| a.pg_oid).collect();
        let reg = RegisteredFunction {
            module_id: id,
            export_name: export_name.clone(),
            signature: sig,
            metrics: crate::metrics::alloc_export_stats(),
        };
        let oid = match proc_reg::register_wasm_trampoline_proc(
            &schema,
            &sql_basename,
            &arg_oids,
            reg.signature.ret.pg_oid,
            reg,
        ) {
            Ok(o) => o,
            Err(e) => {
                cleanup_failed_load(id, &oids, backend);
                return Err(e.into());
            }
        };
        registry::record_module_proc(id, oid);
        oids.push(oid);
    }

    registry::record_module_abi(id, abi);
    registry::record_module_needs_wasi(id, needs_wasi);
    registry::record_module_execution_backend(id, backend);
    registry::record_module_catalog(
        id,
        ModuleCatalogEntry {
            name_prefix: prefix,
            runtime: backend.as_catalog_str().to_string(),
        },
    );

    let hooks = ModuleHooks {
        on_unload: opts.hook_on_unload.clone(),
        on_reconfigure: opts.hook_on_reconfigure.clone(),
    };
    registry::record_module_hooks(id, hooks);
    if let Some(ref name) = opts.hook_on_load {
        let blob = opts.config_blob_for_hooks();
        if let Err(e) = dispatch::call_lifecycle_hook(backend, id, name, &blob) {
            warning!("pg_wasm: {e}");
        }
    }

    Ok(id)
}

pub fn reconfigure_module(module_id: i64, options: Option<JsonB>) -> Result<(), LoadError> {
    if !unsafe { pg_sys::superuser() } {
        return Err(LoadError::Message(
            "pg_wasm_reconfigure_module requires a superuser session".into(),
        ));
    }
    let mid = ModuleId(module_id);
    let needs_wasi = registry::module_needs_wasi(mid).ok_or_else(|| {
        LoadError::Message(format!(
            "pg_wasm_reconfigure_module: unknown module_id {module_id}"
        ))
    })?;
    let old = registry::module_policy_overrides(mid).ok_or_else(|| {
        LoadError::Message(format!(
            "pg_wasm_reconfigure_module: unknown module_id {module_id}"
        ))
    })?;
    let delta = match options {
        Some(JsonB(v)) => v,
        None => serde_json::Value::Object(serde_json::Map::new()),
    };
    let merged = merge_policy_overrides(old, &delta);
    let effective = effective_host_policy(&merged);
    if needs_wasi && !effective.allow_wasi {
        return Err(LoadError::Message(
            "pg_wasm_reconfigure_module: effective policy would deny WASI for a module that imports it"
                .into(),
        ));
    }
    registry::replace_module_policy_overrides(mid, merged).map_err(|()| {
        LoadError::Message(format!(
            "pg_wasm_reconfigure_module: unknown module_id {module_id}"
        ))
    })?;

    let old_limits = registry::module_resource_limits(mid).ok_or_else(|| {
        LoadError::Message(format!(
            "pg_wasm_reconfigure_module: unknown module_id {module_id}"
        ))
    })?;
    let merged_limits = merge_resource_limits(old_limits, &delta);
    registry::replace_module_resource_limits(mid, merged_limits).map_err(|()| {
        LoadError::Message(format!(
            "pg_wasm_reconfigure_module: unknown module_id {module_id}"
        ))
    })?;

    if let Some(h) = registry::module_hooks(mid) {
        if let Some(name) = h.on_reconfigure {
            let blob = serde_json::to_vec(&delta).unwrap_or_default();
            if let Some(b) = registry::module_execution_backend(mid) {
                if let Err(e) = dispatch::call_lifecycle_hook(b, mid, &name, &blob) {
                    warning!("pg_wasm: {e}");
                }
            }
        }
    }

    Ok(())
}

fn module_sql_prefix(module_name: Option<&str>, id: ModuleId) -> Result<String, LoadError> {
    match module_name {
        Some(name) => {
            proc_reg::assert_sql_identifier(name)?;
            Ok(name.to_string())
        }
        None => Ok(format!("m{}", id.0)),
    }
}

pub fn resolve_path_and_read(path_arg: &str) -> Result<Vec<u8>, LoadError> {
    if !allow_load_from_file() {
        return Err(LoadError::Message(
            "pg_wasm.allow_load_from_file is off; enable it to use pg_wasm_load(text)".into(),
        ));
    }
    if !unsafe { pg_sys::superuser() } {
        return Err(LoadError::Message(
            "pg_wasm_load from path requires a superuser session".into(),
        ));
    }

    let path = Path::new(path_arg);
    let combined: PathBuf = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let Some(base_cs) = module_path_cstr() else {
            return Err(LoadError::Message(
                "relative pg_wasm_load path requires pg_wasm.module_path to be set".into(),
            ));
        };
        let base = base_cs
            .to_str()
            .map_err(|_| LoadError::Message("pg_wasm.module_path is not valid UTF-8".into()))?;
        Path::new(base).join(path)
    };

    let canonical = combined.canonicalize().map_err(|e| {
        LoadError::Message(format!("pg_wasm_load: could not open {combined:?}: {e}"))
    })?;

    check_path_policy(&canonical)?;

    let meta = std::fs::metadata(&canonical)
        .map_err(|e| LoadError::Message(format!("pg_wasm_load: stat {:?}: {e}", canonical)))?;
    let len = meta.len() as usize;
    enforce_size_limit(len)?;

    let bytes = std::fs::read(&canonical)
        .map_err(|e| LoadError::Message(format!("pg_wasm_load: read {:?}: {e}", canonical)))?;
    enforce_size_limit(bytes.len())?;
    Ok(bytes)
}

fn check_path_policy(canonical: &Path) -> Result<(), LoadError> {
    let path_s = canonical.to_string_lossy();
    if let Some(prefixes_cs) = allowed_path_prefixes_raw() {
        let s = prefixes_cs.to_str().map_err(|_| {
            LoadError::Message("pg_wasm.allowed_path_prefixes must be valid UTF-8".into())
        })?;
        let list: Vec<_> = s
            .split(',')
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .collect();
        if !list.is_empty() {
            for p in &list {
                let prefix = Path::new(p).canonicalize().map_err(|e| {
                    LoadError::Message(format!(
                        "pg_wasm.allowed_path_prefixes entry {p:?} is not a usable path: {e}"
                    ))
                })?;
                let pre = prefix.to_string_lossy();
                if path_s.starts_with(pre.as_ref()) {
                    return Ok(());
                }
            }
            return Err(LoadError::Message(format!(
                "path {:?} is not under any entry in pg_wasm.allowed_path_prefixes",
                canonical
            )));
        }
    }

    let Some(base_cs) = module_path_cstr() else {
        return Err(LoadError::Message(
            "set pg_wasm.module_path (or pg_wasm.allowed_path_prefixes) before loading from disk"
                .into(),
        ));
    };
    let base_str = base_cs
        .to_str()
        .map_err(|_| LoadError::Message("pg_wasm.module_path must be valid UTF-8".into()))?;
    let base_canon = Path::new(base_str).canonicalize().map_err(|e| {
        LoadError::Message(format!(
            "pg_wasm.module_path {base_str:?} could not be canonicalized: {e}"
        ))
    })?;
    let base_slash = base_canon.to_string_lossy();
    if !path_s.starts_with(base_slash.as_ref()) {
        return Err(LoadError::Message(format!(
            "resolved path {:?} is not under pg_wasm.module_path {:?}",
            canonical, base_canon
        )));
    }
    Ok(())
}

fn enforce_size_limit(n: usize) -> Result<(), LoadError> {
    let cap = max_module_bytes();
    if cap == 0 {
        return Err(LoadError::Message(
            "pg_wasm.max_module_bytes is 0; increase it to load modules".into(),
        ));
    }
    if n > cap {
        return Err(LoadError::Message(format!(
            "wasm module size {n} exceeds pg_wasm.max_module_bytes ({cap})"
        )));
    }
    Ok(())
}

fn validate_wasm_prefix(bytes: &[u8]) -> Result<(), LoadError> {
    if bytes.len() < 8 {
        return Err(LoadError::Message(
            "input is too small to be a wasm module or component".into(),
        ));
    }
    if &bytes[..4] != WASM_MAGIC {
        return Err(LoadError::Message("missing wasm magic \\0asm".into()));
    }
    match abi::detect_wasm_abi(bytes).map_err(|e| LoadError::Message(e.to_string()))? {
        WasmAbiKind::CoreWasm | WasmAbiKind::ComponentModel => Ok(()),
        WasmAbiKind::Extism => Ok(()),
    }
}

fn extension_schema_name_spi() -> Result<String, LoadError> {
    Spi::get_one::<String>(
        "SELECT n.nspname::text FROM pg_extension e \
         JOIN pg_namespace n ON n.oid = e.extnamespace \
         WHERE e.extname = 'pg_wasm'",
    )
    .map_err(|e| LoadError::Message(format!("spi (extension schema): {e}")))?
    .ok_or_else(|| LoadError::Message("pg_wasm extension is not installed".into()))
}

pub fn unload_module(id: i64) -> Result<(), LoadError> {
    if !unsafe { pg_sys::superuser() } {
        return Err(LoadError::Message(
            "pg_wasm_unload requires a superuser session".into(),
        ));
    }
    let mid = ModuleId(id);
    if let Some(h) = registry::module_hooks(mid) {
        if let Some(name) = h.on_unload {
            if let Some(b) = registry::module_execution_backend(mid) {
                if let Err(e) = dispatch::call_lifecycle_hook(b, mid, &name, &[]) {
                    warning!("pg_wasm: {e}");
                }
            }
        }
    }
    let oids = registry::take_module_proc_oids(mid);
    for oid in oids {
        proc_reg::drop_wasm_trampoline_proc(oid);
    }
    let _ = registry::take_module_abi(mid);
    registry::take_module_wasi_and_policy(mid);
    let _ = registry::take_module_catalog(mid);
    let _ = registry::take_module_hooks(mid);
    crate::metrics::remove_module_memory_peak(mid);
    if let Some(b) = registry::take_module_execution_backend(mid) {
        dispatch::remove_compiled_module(b, mid);
    }
    Ok(())
}
