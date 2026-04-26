//! Load lifecycle: authz, bytes, validate/classify, WIT + policy, compile/persist, catalog + `pg_proc`,
//! generation bump, and transaction abort cleanup for on-disk artifacts.

use std::collections::HashMap;
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};

use pgrx::prelude::*;
use pgrx::spi::{self, Spi};
use pgrx::{PgSubXactCallbackEvent, register_subxact_callback};
use serde_json::{Value, json};
use wasmparser::{CompositeInnerType, ExternalKind, Parser, Payload, ValType};
use wit_parser::{Function, FunctionKind, Type, WorldItem, WorldKey};

use crate::abi::{self, Abi, AbiOverride};
use crate::artifacts;
use crate::catalog::{exports, modules};
use crate::config::{Abi as OptionsAbi, LoadOptions, PolicyOverrides};
use crate::errors::{PgWasmError, Result};
use crate::guc;
use crate::policy::{self, EffectivePolicy, GucSnapshot};
use crate::proc_reg::{self, Parallel, ProcSpec, Volatility};
use crate::runtime::component;
use crate::runtime::core as runtime_core;
use crate::runtime::engine;
use crate::shmem;
use crate::wit::typing::{self, TypePlan};
use crate::wit::udt;
use crate::wit::world;

const ON_LOAD_WASM_NAME: &str = "on-load";
const SCHEMA_WASM: &str = "wasm";

/// `pg_wasm.load(module_name, bytes_or_path, options)` — see architecture doc "Load lifecycle".
pub(crate) fn load_impl(
    module_name: &str,
    bytes_or_path: pgrx::Json,
    options: Option<pgrx::Json>,
) -> Result<bool> {
    if !guc::ENABLED.get() {
        return Err(PgWasmError::Disabled);
    }

    if module_name.is_empty() {
        return Err(PgWasmError::InvalidConfiguration(
            "module name must not be empty".to_string(),
        ));
    }

    require_loader_or_superuser()?;

    let opts = parse_load_options(options)?;
    if modules::get_by_name(module_name)?.is_some() {
        if opts.replace_exports {
            // TODO(wave-5: reload-orchestration): delegate to `lifecycle::reload` when implemented.
            return Err(PgWasmError::InvalidConfiguration(
                "module already loaded; replacing exports requires `pg_wasm.reload(...)` (not yet implemented — see wave-5 reload-orchestration)".to_string(),
            ));
        }
        return Err(PgWasmError::InvalidConfiguration(format!(
            "module `{module_name}` already exists in catalog; set options.replace_exports and call `pg_wasm.reload(...)` when available"
        )));
    }

    let bytes = read_module_bytes(&bytes_or_path)?;
    abi::validate(&bytes)?;

    let abi_override = match opts.abi {
        None | Some(OptionsAbi::Component) => AbiOverride::Auto,
        Some(OptionsAbi::Core) => AbiOverride::ForceCore,
    };
    let classified = abi::detect(&bytes, abi_override)?;

    let extension_oid = extension_oid()?;
    let guc_snapshot = GucSnapshot::from_gucs();
    let effective = policy::resolve(&guc_snapshot, opts.overrides.as_ref(), opts.limits.as_ref())?;

    let wasm_sha256_bytes = artifacts::sha256_bytes(&bytes);
    let policy_json = catalog_policy_json(opts.overrides.as_ref())?;
    let limits_json = serde_json::to_value(opts.limits.clone().unwrap_or_default())
        .map_err(|e| PgWasmError::Internal(format!("serialize limits: {e}")))?;

    match classified {
        Abi::Component => load_component_path(
            module_name,
            &bytes,
            &wasm_sha256_bytes,
            &opts,
            extension_oid,
            &effective,
            policy_json,
            limits_json,
        ),
        Abi::Core => load_core_path(
            module_name,
            &bytes,
            &wasm_sha256_bytes,
            &opts,
            extension_oid,
            &effective,
            policy_json,
            limits_json,
        ),
    }
}

fn catalog_policy_json(overrides: Option<&PolicyOverrides>) -> Result<Value> {
    let Some(p) = overrides else {
        return Ok(json!({}));
    };
    let mut m = serde_json::Map::new();
    if let Some(v) = p.allow_spi {
        m.insert("allow_spi".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi {
        m.insert("allow_wasi".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_env {
        m.insert("allow_wasi_env".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_fs {
        m.insert("allow_wasi_fs".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_http {
        m.insert("allow_wasi_http".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_net {
        m.insert("allow_wasi_net".to_string(), json!(v));
    }
    if let Some(v) = p.allow_wasi_stdio {
        m.insert("allow_wasi_stdio".to_string(), json!(v));
    }
    if let Some(ref hosts) = p.allowed_hosts {
        m.insert("allowed_hosts".to_string(), json!(hosts));
    }
    if let Some(ref pre) = p.wasi_preopens {
        let map: serde_json::Map<String, Value> =
            pre.iter().map(|(k, v)| (k.clone(), json!(v))).collect();
        m.insert("wasi_preopens".to_string(), Value::Object(map));
    }
    Ok(Value::Object(m))
}

#[allow(clippy::too_many_arguments)]
fn load_component_path(
    module_name: &str,
    bytes: &[u8],
    wasm_sha256_bytes: &[u8; 32],
    opts: &LoadOptions,
    extension_oid: pg_sys::Oid,
    _effective: &EffectivePolicy,
    policy_json: Value,
    limits_json: Value,
) -> Result<bool> {
    let decoded = world::decode(bytes)?;
    let wit_text = decoded.wit_text.clone();
    let type_plan = typing::plan_types(module_name, &decoded)?;
    let export_specs = plan_export_proc_specs(module_name, &decoded, &type_plan)?;

    let wasm_engine = engine::try_shared_engine()?;
    let _compiled = component::compile(wasm_engine, bytes)?;

    let placeholder = modules::NewModule {
        abi: "component".to_string(),
        artifact_path: "pending".to_string(),
        digest: wasm_sha256_bytes.to_vec(),
        generation: 0,
        limits: limits_json.clone(),
        name: module_name.to_string(),
        origin: "load".to_string(),
        policy: policy_json.clone(),
        wasm_sha256: wasm_sha256_bytes.to_vec(),
        wit_world: wit_text.clone(),
    };
    let inserted = modules::insert(&placeholder)?;
    let module_id = inserted.module_id;
    let module_id_u64 = u64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;

    register_abort_artifact_cleanup(module_id_u64);

    let cwasm_path = artifacts::module_cwasm_path(module_id_u64)?;
    let precompile_hash = component::precompile_to(wasm_engine, bytes, &cwasm_path)?;
    artifacts::write_module_wasm(module_id_u64, bytes)?;
    artifacts::write_world_wit(module_id_u64, &wit_text)?;
    let wasm_dir = artifacts::module_dir(module_id_u64)?;
    artifacts::write_checksum(&wasm_dir, wasm_sha256_bytes)?;

    let mut export_rows: Vec<exports::NewExport> = Vec::new();
    for (spec, wasm_export) in export_specs {
        let fn_oid = proc_reg::register(&spec, extension_oid, opts.replace_exports)?;
        let signature = export_signature_json(&decoded, wasm_export.as_str())?;
        let arg_types = spec.arg_types.clone();
        let ret_type = if spec.ret_type == pg_sys::InvalidOid {
            None
        } else {
            Some(spec.ret_type)
        };
        export_rows.push(exports::NewExport {
            arg_types,
            fn_oid: Some(fn_oid),
            kind: "function".to_string(),
            module_id,
            ret_type,
            signature,
            sql_name: spec.name.clone(),
            wasm_name: wasm_export,
        });
    }

    for row in &export_rows {
        exports::insert(row)?;
    }

    let _registered_types = udt::register_type_plan(&type_plan, module_id_u64, extension_oid)?;

    let artifact_path = cwasm_path.display().to_string();
    let updated = modules::NewModule {
        abi: "component".to_string(),
        artifact_path,
        digest: wasm_sha256_bytes.to_vec(),
        generation: 0,
        limits: limits_json,
        name: module_name.to_string(),
        origin: "load".to_string(),
        policy: policy_json,
        wasm_sha256: precompile_hash.to_vec(),
        wit_world: wit_text,
    };
    let Some(_row) = modules::update(module_id, &updated)? else {
        return Err(PgWasmError::Internal(
            "catalog update after load returned no row".to_string(),
        ));
    };

    if world_exports_function_named(&decoded, ON_LOAD_WASM_NAME) {
        return Err(PgWasmError::InvalidConfiguration(
            "module exports `on-load` hook; hook invocation is not wired yet (wave-4 hooks)"
                .to_string(),
        ));
    }

    shmem::bump_generation(module_id_u64);
    try_prewarm_component_pool_note(module_id_u64);

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn load_core_path(
    module_name: &str,
    bytes: &[u8],
    wasm_sha256_bytes: &[u8; 32],
    opts: &LoadOptions,
    extension_oid: pg_sys::Oid,
    _effective: &EffectivePolicy,
    policy_json: Value,
    limits_json: Value,
) -> Result<bool> {
    let wasm_engine = engine::try_shared_engine()?;
    let _loaded = runtime_core::compile(wasm_engine, bytes)?;

    let placeholder = modules::NewModule {
        abi: "core".to_string(),
        artifact_path: "pending".to_string(),
        digest: wasm_sha256_bytes.to_vec(),
        generation: 0,
        limits: limits_json.clone(),
        name: module_name.to_string(),
        origin: "load".to_string(),
        policy: policy_json.clone(),
        wasm_sha256: wasm_sha256_bytes.to_vec(),
        wit_world: String::new(),
    };
    let inserted = modules::insert(&placeholder)?;
    let module_id = inserted.module_id;
    let module_id_u64 = u64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit u64".to_string()))?;

    register_abort_artifact_cleanup(module_id_u64);

    artifacts::write_module_wasm(module_id_u64, bytes)?;
    let wasm_dir = artifacts::module_dir(module_id_u64)?;
    artifacts::write_checksum(&wasm_dir, wasm_sha256_bytes)?;

    let export_specs = plan_core_export_proc_specs(bytes)?;
    let mut export_rows: Vec<exports::NewExport> = Vec::new();
    for (spec, wasm_export) in export_specs {
        let fn_oid = proc_reg::register(&spec, extension_oid, opts.replace_exports)?;
        export_rows.push(exports::NewExport {
            arg_types: spec.arg_types.clone(),
            fn_oid: Some(fn_oid),
            kind: "function".to_string(),
            module_id,
            ret_type: if spec.ret_type == pg_sys::InvalidOid {
                None
            } else {
                Some(spec.ret_type)
            },
            signature: json!({"abi": "core", "export": wasm_export}),
            sql_name: spec.name.clone(),
            wasm_name: wasm_export,
        });
    }

    for row in &export_rows {
        exports::insert(row)?;
    }

    let wasm_path = artifacts::module_wasm_path(module_id_u64)?;
    let updated = modules::NewModule {
        abi: "core".to_string(),
        artifact_path: wasm_path.display().to_string(),
        digest: wasm_sha256_bytes.to_vec(),
        generation: 0,
        limits: limits_json,
        name: module_name.to_string(),
        origin: "load".to_string(),
        policy: policy_json,
        wasm_sha256: wasm_sha256_bytes.to_vec(),
        wit_world: String::new(),
    };
    let Some(_row) = modules::update(module_id, &updated)? else {
        return Err(PgWasmError::Internal(
            "catalog update after core load returned no row".to_string(),
        ));
    };

    shmem::bump_generation(module_id_u64);

    Ok(true)
}

fn try_prewarm_component_pool_note(module_id_u64: u64) {
    ereport!(
        PgLogLevel::NOTICE,
        PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
        format!(
            "pg_wasm: instance pool warm-up for module_id {module_id_u64} deferred until first call (prewarm API not available)"
        ),
    );
}

/// Remove on-disk artifacts if the surrounding (sub)transaction aborts. Uses only filesystem I/O
/// (no SPI) because PostgreSQL forbids SPI in transaction abort callbacks.
fn register_abort_artifact_cleanup(module_id_u64: u64) {
    pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, move || {
        remove_module_artifact_dir_if_present(module_id_u64);
    });
    register_subxact_callback(PgSubXactCallbackEvent::AbortSub, move |_, _| {
        remove_module_artifact_dir_if_present(module_id_u64);
    });
}

fn remove_module_artifact_dir_if_present(module_id_u64: u64) {
    if let Ok(dir) = artifacts::module_dir(module_id_u64)
        && dir.exists()
    {
        let _ = fs::remove_dir_all(&dir);
    }
}

fn extension_oid() -> Result<pg_sys::Oid> {
    let oid = unsafe { pg_sys::get_extension_oid(c"pg_wasm".as_ptr(), false) };
    if oid == pg_sys::InvalidOid {
        return Err(PgWasmError::Internal(
            "extension `pg_wasm` oid lookup failed".to_string(),
        ));
    }
    Ok(oid)
}

fn require_loader_or_superuser() -> Result<()> {
    let allowed = Spi::connect(|client| {
        let rows = client.select(
            "SELECT (
                COALESCE(
                    (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user),
                    false
                )
                OR pg_catalog.pg_has_role(
                    current_user::regrole,
                    'wasm_loader'::regrole,
                    'member'::text
                )
            ) AS allowed",
            Some(1),
            &[],
        )?;
        let row = rows.into_iter().next().ok_or(spi::Error::InvalidPosition)?;
        row.get_by_name::<bool, _>("allowed")?
            .ok_or(spi::Error::InvalidPosition)
    })
    .map_err(|e| PgWasmError::Internal(format!("authz SPI: {e}")))?;

    if allowed {
        Ok(())
    } else {
        Err(PgWasmError::PermissionDenied(
            "pg_wasm.load requires superuser or membership in role `wasm_loader`".to_string(),
        ))
    }
}

fn parse_load_options(options: Option<pgrx::Json>) -> Result<LoadOptions> {
    let map = match options {
        None => return Ok(LoadOptions::default()),
        Some(j) => match j.0 {
            Value::Object(m) => m,
            _ => {
                return Err(PgWasmError::InvalidConfiguration(
                    "options must be a JSON object when provided".to_string(),
                ));
            }
        },
    };

    let mut out = LoadOptions::default();
    for (k, v) in map {
        match k.as_str() {
            "abi" => {
                let s = json_string(&v, "abi")?;
                out.abi = Some(match s.as_str() {
                    "component" => OptionsAbi::Component,
                    "core" => OptionsAbi::Core,
                    other => {
                        return Err(PgWasmError::InvalidConfiguration(format!(
                            "options.abi must be \"component\" or \"core\", got `{other}`"
                        )));
                    }
                });
            }
            "breaking_changes_allowed" => {
                out.breaking_changes_allowed = json_bool(&v, "breaking_changes_allowed")?;
            }
            "cascade" => out.cascade = Some(json_bool(&v, "cascade")?),
            "limits" => {
                out.limits = Some(serde_json::from_value(v).map_err(|e| {
                    PgWasmError::InvalidConfiguration(format!("options.limits: {e}"))
                })?);
            }
            "on_load_hook" => out.on_load_hook = json_bool(&v, "on_load_hook")?,
            "overrides" => {
                out.overrides = Some(serde_json::from_value(v).map_err(|e| {
                    PgWasmError::InvalidConfiguration(format!("options.overrides: {e}"))
                })?);
            }
            "replace_exports" => out.replace_exports = json_bool(&v, "replace_exports")?,
            _ => {}
        }
    }
    Ok(out)
}

fn json_string(v: &Value, field: &str) -> Result<String> {
    v.as_str()
        .map(str::to_owned)
        .ok_or_else(|| PgWasmError::InvalidConfiguration(format!("`{field}` must be a string")))
}

fn json_bool(v: &Value, field: &str) -> Result<bool> {
    v.as_bool()
        .ok_or_else(|| PgWasmError::InvalidConfiguration(format!("`{field}` must be a boolean")))
}

fn read_module_bytes(bytes_or_path: &pgrx::Json) -> Result<Vec<u8>> {
    match &bytes_or_path.0 {
        Value::Object(map) => {
            if map.contains_key("bytes") {
                return decode_bytea_json(map.get("bytes").ok_or_else(|| {
                    PgWasmError::InvalidConfiguration("bytes key missing".to_string())
                })?);
            }
            if map.contains_key("path") {
                return read_path_payload(map.get("path").ok_or_else(|| {
                    PgWasmError::InvalidConfiguration("path key missing".to_string())
                })?);
            }
            Err(PgWasmError::InvalidConfiguration(
                "bytes_or_path must be an object with `bytes` (bytea) or `path` (text)".to_string(),
            ))
        }
        _ => Err(PgWasmError::InvalidConfiguration(
            "bytes_or_path must be a JSON object".to_string(),
        )),
    }
}

fn decode_bytea_json(v: &Value) -> Result<Vec<u8>> {
    if let Value::String(hex) = v
        && hex.len() % 2 == 0
        && hex.chars().all(|c| c.is_ascii_hexdigit())
    {
        return decode_hex(hex);
    }
    if let Some(bytes) = v.as_array() {
        let mut out = Vec::with_capacity(bytes.len());
        for item in bytes {
            let n = item.as_u64().ok_or_else(|| {
                PgWasmError::InvalidConfiguration(
                    "bytes array must contain u8 elements".to_string(),
                )
            })?;
            let b = u8::try_from(n).map_err(|_| {
                PgWasmError::InvalidConfiguration("bytes array element out of u8 range".to_string())
            })?;
            out.push(b);
        }
        return Ok(out);
    }
    Err(PgWasmError::InvalidConfiguration(
        "bytes must be JSON bytea from PostgreSQL or a JSON array of integers".to_string(),
    ))
}

fn decode_hex(hex: &str) -> Result<Vec<u8>> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
        .collect::<core::result::Result<Vec<_>, _>>()
        .map_err(|_| PgWasmError::InvalidConfiguration("invalid hex in bytes payload".to_string()))
}

fn read_path_payload(v: &Value) -> Result<Vec<u8>> {
    if !guc::ALLOW_LOAD_FROM_FILE.get() {
        return Err(PgWasmError::PermissionDenied(
            "loading from filesystem path is disabled; set pg_wasm.allow_load_from_file"
                .to_string(),
        ));
    }
    let path_str = v.as_str().ok_or_else(|| {
        PgWasmError::InvalidConfiguration("path must be a JSON string".to_string())
    })?;
    let path = resolve_load_path(path_str);
    let canonical = resolve_canonical_under_policy(&path)?;
    enforce_allowed_prefixes(&canonical)?;
    let meta = fs::metadata(&canonical)?;
    let len = meta.len();
    let max = u64::try_from(guc::MAX_MODULE_BYTES.get().max(0))
        .map_err(|_| PgWasmError::Internal("max_module_bytes overflow".to_string()))?;
    if len > max {
        return Err(PgWasmError::ResourceLimitExceeded(format!(
            "module file size {len} exceeds pg_wasm.max_module_bytes ({max})"
        )));
    }
    let bytes = fs::read(&canonical)?;
    if u64::try_from(bytes.len())
        .map_err(|_| PgWasmError::Internal("length overflow".to_string()))?
        != len
    {
        return Err(PgWasmError::InvalidModule(
            "file size changed while reading".to_string(),
        ));
    }
    Ok(bytes)
}

fn resolve_load_path(path_str: &str) -> PathBuf {
    let raw = Path::new(path_str);
    if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        let base = guc::MODULE_PATH
            .get()
            .map(|c| c.to_string_lossy().into_owned())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        base.join(raw)
    }
}

fn resolve_canonical_under_policy(path: &Path) -> io::Result<PathBuf> {
    if guc::FOLLOW_SYMLINKS.get() {
        return fs::canonicalize(path);
    }

    let mut out = PathBuf::new();
    let mut comps = path.components().peekable();
    if let Some(c) = comps.peek() {
        match c {
            Component::Prefix(p) => {
                out.push(Component::Prefix(*p));
                let _ = comps.next();
            }
            Component::RootDir => {
                out.push(Component::RootDir);
                let _ = comps.next();
            }
            _ => {}
        }
    }

    for comp in comps {
        match comp {
            Component::Prefix(_) | Component::RootDir => {}
            Component::CurDir => {}
            Component::ParentDir => {
                let _ = out.pop();
            }
            Component::Normal(name) => {
                out.push(name);
                let meta = fs::symlink_metadata(&out)?;
                if meta.is_symlink() {
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        format!(
                            "path `{}` traverses a symlink while pg_wasm.follow_symlinks is off",
                            out.display()
                        ),
                    ));
                }
            }
        }
    }

    if out.as_os_str().is_empty() {
        fs::canonicalize(path)
    } else {
        fs::canonicalize(&out)
    }
}

fn enforce_allowed_prefixes(canonical: &Path) -> Result<()> {
    let prefixes = guc::ALLOWED_PATH_PREFIXES
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();

    if prefixes.is_empty() {
        return Err(PgWasmError::InvalidConfiguration(
            "pg_wasm.allowed_path_prefixes must list at least one canonical prefix for file loads"
                .to_string(),
        ));
    }

    let canon_str = canonical.to_str().ok_or_else(|| {
        PgWasmError::InvalidConfiguration("canonical path is not valid UTF-8".to_string())
    })?;

    let sep = std::path::MAIN_SEPARATOR;
    let ok = prefixes.iter().any(|prefix| {
        prefix
            .to_str()
            .is_some_and(|p| canon_str == p || canon_str.starts_with(&format!("{p}{sep}")))
    });

    if !ok {
        return Err(PgWasmError::PermissionDenied(format!(
            "resolved path `{}` is not under any entry in pg_wasm.allowed_path_prefixes",
            canonical.display()
        )));
    }
    Ok(())
}

fn world_exports_function_named(decoded: &world::DecodedWorld, wasm_name: &str) -> bool {
    let Some(world) = decoded.resolve.worlds.get(decoded.world_id) else {
        return false;
    };
    for item in world.exports.values() {
        match item {
            WorldItem::Function(f) if f.name == wasm_name => return true,
            WorldItem::Interface { id, .. } => {
                if let Some(iface) = decoded.resolve.interfaces.get(*id) {
                    for (n, f) in &iface.functions {
                        if n == wasm_name || export_wasm_name(&decoded.resolve, f) == wasm_name {
                            return true;
                        }
                    }
                }
            }
            _ => {}
        }
    }
    false
}

fn export_wasm_name(resolve: &wit_parser::Resolve, func: &Function) -> String {
    match &func.kind {
        FunctionKind::Freestanding | FunctionKind::AsyncFreestanding => func.name.clone(),
        FunctionKind::Method(id) | FunctionKind::AsyncMethod(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}#{}", func.name)
        }
        FunctionKind::Static(id) | FunctionKind::AsyncStatic(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}!{}", func.name)
        }
        FunctionKind::Constructor(id) => {
            let type_name = type_name_for_id(resolve, *id);
            format!("{type_name}#{type_name}")
        }
    }
}

fn type_name_for_id(resolve: &wit_parser::Resolve, type_id: wit_parser::TypeId) -> String {
    resolve.types[type_id]
        .name
        .clone()
        .unwrap_or_else(|| format!("type{}", type_id.index()))
}

fn export_signature_json(decoded: &world::DecodedWorld, wasm_export: &str) -> Result<Value> {
    let world = decoded
        .resolve
        .worlds
        .get(decoded.world_id)
        .ok_or_else(|| PgWasmError::InvalidModule("decoded world missing".to_string()))?;

    let mut func: Option<&Function> = None;
    for item in world.exports.values() {
        match item {
            WorldItem::Function(f) => {
                let w = export_wasm_name(&decoded.resolve, f);
                if w == wasm_export {
                    func = Some(f);
                    break;
                }
            }
            WorldItem::Interface { id, .. } => {
                let Some(iface) = decoded.resolve.interfaces.get(*id) else {
                    continue;
                };
                for f in iface.functions.values() {
                    let w = export_wasm_name(&decoded.resolve, f);
                    if w == wasm_export {
                        func = Some(f);
                        break;
                    }
                }
            }
            WorldItem::Type { .. } => {}
        }
        if func.is_some() {
            break;
        }
    }

    let func = func.ok_or_else(|| {
        PgWasmError::Internal(format!(
            "could not locate WIT function `{wasm_export}` for signature JSON"
        ))
    })?;

    let params: Vec<Value> = func
        .params
        .iter()
        .map(|p| json!({"name": p.name, "type": format!("{:?}", p.ty)}))
        .collect();
    let result = func.result.map(|t| json!({"type": format!("{t:?}")}));
    Ok(json!({
        "kind": "wit-function",
        "params": params,
        "result": result,
    }))
}

fn plan_export_proc_specs(
    module_prefix: &str,
    decoded: &world::DecodedWorld,
    _type_plan: &TypePlan,
) -> Result<Vec<(ProcSpec, String)>> {
    let world = decoded
        .resolve
        .worlds
        .get(decoded.world_id)
        .ok_or_else(|| PgWasmError::InvalidModule("decoded world missing".to_string()))?;

    let mut out = Vec::new();
    for (export_key, item) in &world.exports {
        let export_key_str = world_key_to_string(export_key);
        match item {
            WorldItem::Function(f) => {
                let wasm = export_wasm_name(&decoded.resolve, f);
                out.push((
                    proc_spec_for_function(module_prefix, &export_key_str, f, &decoded.resolve)?,
                    wasm,
                ));
            }
            WorldItem::Interface { id, .. } => {
                let iface = decoded.resolve.interfaces.get(*id).ok_or_else(|| {
                    PgWasmError::InvalidModule("interface id missing".to_string())
                })?;
                for (func_key, func) in &iface.functions {
                    let sql_key = format!("{export_key_str}/{func_key}");
                    let wasm = export_wasm_name(&decoded.resolve, func);
                    out.push((
                        proc_spec_for_function(module_prefix, &sql_key, func, &decoded.resolve)?,
                        wasm,
                    ));
                }
            }
            WorldItem::Type { .. } => {}
        }
    }

    out.sort_by(|a, b| a.0.name.cmp(&b.0.name));
    Ok(out)
}

fn world_key_to_string(key: &WorldKey) -> String {
    match key {
        WorldKey::Name(n) => n.clone(),
        WorldKey::Interface(id) => format!("interface-{}", id.index()),
    }
}

fn proc_spec_for_function(
    module_prefix: &str,
    export_key: &str,
    func: &Function,
    _resolve: &wit_parser::Resolve,
) -> Result<ProcSpec> {
    let sql_name = sanitize_sql_identifier(export_key);
    let full_name = format!("{module_prefix}__{sql_name}");
    let mut arg_types = Vec::new();
    for p in &func.params {
        arg_types.push(wit_scalar_to_pg_oid(&p.ty)?);
    }
    let ret_type = match &func.result {
        None => pg_sys::VOIDOID,
        Some(t) => wit_scalar_to_pg_oid(t)?,
    };
    let strict = func.params.iter().any(|p| matches!(p.ty, Type::Id(_)));

    Ok(ProcSpec {
        schema: SCHEMA_WASM.to_string(),
        name: full_name,
        arg_types,
        arg_names: Vec::new(),
        arg_modes: Vec::new(),
        ret_type,
        returns_set: false,
        volatility: Volatility::Volatile,
        strict,
        parallel: Parallel::Unsafe,
        cost: Some(100.0),
    })
}

fn wit_scalar_to_pg_oid(ty: &Type) -> Result<pg_sys::Oid> {
    match ty {
        Type::Bool => Ok(pg_sys::BOOLOID),
        Type::S32 => Ok(pg_sys::INT4OID),
        Type::S64 => Ok(pg_sys::INT8OID),
        Type::String => Ok(pg_sys::TEXTOID),
        other => Err(PgWasmError::Unsupported(format!(
            "WIT type `{other:?}` is not supported for automatic SQL export registration in this build"
        ))),
    }
}

fn sanitize_sql_identifier(s: &str) -> String {
    let mut t = s.replace(['/', '-'], "_");
    if t.is_empty() {
        t = "export".to_string();
    }
    if !t
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        t = format!("e_{t}");
    }
    t
}

fn plan_core_export_proc_specs(bytes: &[u8]) -> Result<Vec<(ProcSpec, String)>> {
    let mut export_list = Vec::<(String, u32)>::new();
    for payload in Parser::new(0).parse_all(bytes) {
        let Ok(payload) = payload else {
            continue;
        };
        if let Payload::ExportSection(reader) = payload {
            for rec in reader.into_iter() {
                let Ok(exp) = rec else { continue };
                if exp.kind == ExternalKind::Func {
                    export_list.push((exp.name.to_string(), exp.index));
                }
            }
        }
    }

    let mut type_map: HashMap<u32, (Vec<ValType>, Option<ValType>)> = HashMap::new();
    let mut type_index: u32 = 0;
    for payload in Parser::new(0).parse_all(bytes) {
        let Ok(payload) = payload else {
            continue;
        };
        if let Payload::TypeSection(reader) = payload {
            for group in reader.into_iter() {
                let Ok(group) = group else { continue };
                for st in group.types() {
                    if let CompositeInnerType::Func(ft) = &st.composite_type.inner {
                        let params: Vec<_> = ft.params().to_vec();
                        let ret = ft.results().iter().copied().next();
                        type_map.insert(type_index, (params, ret));
                    }
                    type_index = type_index.saturating_add(1);
                }
            }
        }
    }

    let mut func_types: HashMap<u32, u32> = HashMap::new();
    for payload in Parser::new(0).parse_all(bytes) {
        let Ok(payload) = payload else {
            continue;
        };
        if let Payload::FunctionSection(reader) = payload {
            for (i, ty) in reader.into_iter().enumerate() {
                if let Ok(ty) = ty {
                    func_types.insert(i as u32, ty);
                }
            }
        }
    }

    let mut out = Vec::new();
    for (name, func_idx) in export_list {
        let Some(&type_idx) = func_types.get(&func_idx) else {
            continue;
        };
        let Some((params, ret)) = type_map.get(&type_idx) else {
            continue;
        };
        if params.iter().all(|p| *p == ValType::I32) && matches!(ret, None | Some(ValType::I32)) {
            let arg_types = vec![pg_sys::INT4OID; params.len()];
            let ret_type = match ret {
                None => pg_sys::VOIDOID,
                Some(_) => pg_sys::INT4OID,
            };
            let sql_name = sanitize_sql_identifier(&name);
            out.push((
                ProcSpec {
                    schema: SCHEMA_WASM.to_string(),
                    name: format!("core__{sql_name}"),
                    arg_types,
                    arg_names: Vec::new(),
                    arg_modes: Vec::new(),
                    ret_type,
                    returns_set: false,
                    volatility: Volatility::Volatile,
                    strict: false,
                    parallel: Parallel::Unsafe,
                    cost: Some(100.0),
                },
                name,
            ));
        }
    }

    if out.is_empty() {
        return Err(PgWasmError::Unsupported(
            "core module has no func exports with (i32,...)->i32 signature supported for load in this build"
                .to_string(),
        ));
    }

    out.sort_by(|a, b| a.0.name.cmp(&b.0.name));
    Ok(out)
}
