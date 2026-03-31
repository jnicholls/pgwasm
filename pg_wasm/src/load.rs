//! `load_from_bytes`, path resolution, and catalog registration (plan §8).

use std::path::{Path, PathBuf};

use pgrx::{prelude::*, spi::Spi, JsonB};

use crate::{
    abi::{self, WasmAbiKind},
    config::LoadOptions,
    guc::{allow_load_from_file, allowed_path_prefixes_raw, max_module_bytes, module_path_cstr},
    proc_reg::{self, RegisterError},
    registry::{self, ModuleId, RegisteredFunction},
    runtime::wasmtime_backend,
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

fn cleanup_failed_load(id: ModuleId, oids: &[pgrx::pg_sys::Oid]) {
    for o in oids {
        proc_reg::drop_wasm_trampoline_proc(*o);
    }
    let _ = registry::take_module_proc_oids(id);
    wasmtime_backend::remove_compiled_module(id);
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
        None => abi::detect_wasm_abi(wasm)
            .map_err(|e| LoadError::Message(e.to_string()))?,
    };

    match abi {
        WasmAbiKind::ComponentModel => {
            return Err(LoadError::Message(
                "WebAssembly component model detected; pg_wasm does not run components yet"
                    .into(),
            ));
        }
        WasmAbiKind::Extism => {
            return Err(LoadError::Message(
                "Extism plugin ABI detected; pg_wasm only loads core wasm until Extism is integrated"
                    .into(),
            ));
        }
        WasmAbiKind::CoreWasm => {}
    }

    let schema = extension_schema_name_spi()?;
    let id = registry::alloc_module_id();
    let prefix = module_sql_prefix(module_name, id)?;

    let export_hints = opts.export_hints().map_err(LoadError::Message)?;
    let exports = match wasmtime_backend::compile_store_and_list_exports(id, wasm, &export_hints) {
        Ok(e) => e,
        Err(e) => return Err(LoadError::Message(e)),
    };

    if exports.is_empty() {
        wasmtime_backend::remove_compiled_module(id);
        return Err(LoadError::Message(
            "no supported wasm function exports (int/float scalars, or use options \"exports\" for text/bytea/jsonb)".into(),
        ));
    }

    let mut oids = Vec::new();
    for (export_name, sig) in exports {
        if let Err(e) = proc_reg::assert_sql_identifier(&export_name) {
            cleanup_failed_load(id, &oids);
            return Err(e.into());
        }
        let sql_basename = format!("{prefix}_{export_name}");
        if let Err(e) = proc_reg::assert_sql_identifier(&sql_basename) {
            cleanup_failed_load(id, &oids);
            return Err(e.into());
        }

        let arg_oids: Vec<_> = sig.args.iter().map(|a| a.pg_oid).collect();
        let reg = RegisteredFunction {
            module_id: id,
            export_name: export_name.clone(),
            signature: sig,
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
                cleanup_failed_load(id, &oids);
                return Err(e.into());
            }
        };
        registry::record_module_proc(id, oid);
        oids.push(oid);
    }

    registry::record_module_abi(id, abi);
    Ok(id)
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

    let meta = std::fs::metadata(&canonical).map_err(|e| {
        LoadError::Message(format!("pg_wasm_load: stat {:?}: {e}", canonical))
    })?;
    let len = meta.len() as usize;
    enforce_size_limit(len)?;

    let bytes = std::fs::read(&canonical).map_err(|e| {
        LoadError::Message(format!("pg_wasm_load: read {:?}: {e}", canonical))
    })?;
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
            "input is too small to be a wasm module".into(),
        ));
    }
    if &bytes[..4] != WASM_MAGIC {
        return Err(LoadError::Message("missing wasm magic \\0asm".into()));
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != 1 {
        return Err(LoadError::Message(format!(
            "unsupported wasm version {version} (expected 1)"
        )));
    }
    Ok(())
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
    let oids = registry::take_module_proc_oids(mid);
    for oid in oids {
        proc_reg::drop_wasm_trampoline_proc(oid);
    }
    let _ = registry::take_module_abi(mid);
    wasmtime_backend::remove_compiled_module(mid);
    Ok(())
}
