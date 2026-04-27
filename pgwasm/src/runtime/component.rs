//! Wasmtime component compilation, AOT precompile, linker setup (WASI / optional HTTP).

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Engine, Precompiled, StoreLimits};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};
use wasmtime_wasi_http::WasiHttpCtx;
use wasmtime_wasi_http::p2::{WasiHttpCtxView, WasiHttpView};

use crate::artifacts;
use crate::errors::PgWasmError;
use crate::policy::EffectivePolicy;

/// Per-invocation flags for host interfaces (may diverge from linker capabilities later).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HostState {
    /// When false, `pgwasm:host/query` calls fail even if the interface was linked.
    pub(crate) allow_spi: bool,
}

/// Per-`Store` state for component instantiation: WASI + shared resource table + optional HTTP.
pub(crate) struct StoreCtx {
    pub(crate) host: HostState,
    http: WasiHttpCtx,
    /// Per-invocation store limits; filled by the trampoline before each call.
    pub(crate) limits: StoreLimits,
    table: ResourceTable,
    wasi: WasiCtx,
}

impl WasiView for StoreCtx {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for StoreCtx {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            hooks: wasmtime_wasi_http::p2::default_hooks(),
            table: &mut self.table,
        }
    }
}

pub(crate) fn engine_precompile_fingerprint(engine: &Engine) -> [u8; 32] {
    let mut hasher = DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    let u = hasher.finish();
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&u.to_le_bytes());
    out
}

/// Compile a WebAssembly component from bytes.
pub(crate) fn compile(engine: &Engine, bytes: &[u8]) -> Result<Component, PgWasmError> {
    Component::from_binary(engine, bytes).map_err(|error| {
        PgWasmError::InvalidModule(format!("failed to compile component: {error}"))
    })
}

/// AOT-precompile `bytes` to `out_path` and return the engine compatibility fingerprint bytes.
pub(crate) fn precompile_to(
    engine: &Engine,
    bytes: &[u8],
    out_path: &Path,
) -> Result<[u8; 32], PgWasmError> {
    let cwasm = engine
        .precompile_component(bytes)
        .map_err(|error| PgWasmError::Internal(format!("component precompile failed: {error}")))?;
    artifacts::write_atomic(out_path, &cwasm)?;
    Ok(engine_precompile_fingerprint(engine))
}

/// Load a serialized component from disk after validating the precompiled artifact kind and hash.
///
/// # Safety
///
/// The caller must ensure `path` points at bytes produced by this engine family for this
/// `expected_hash`, under `$PGDATA/pgwasm/` (only the extension writes there).
pub(crate) unsafe fn load_precompiled(
    engine: &Engine,
    path: &Path,
    expected_hash: &[u8; 32],
) -> Result<Component, PgWasmError> {
    let detected = Engine::detect_precompiled_file(path).map_err(|error| {
        PgWasmError::InvalidModule(format!("precompiled file detection failed: {error}"))
    })?;
    let Some(Precompiled::Component) = detected else {
        return Err(PgWasmError::InvalidModule(
            "stale_cache: precompiled file is missing, not a component, or wrong format"
                .to_string(),
        ));
    };

    let current = engine_precompile_fingerprint(engine);
    if current != *expected_hash {
        return Err(PgWasmError::InvalidModule(
            "stale_cache: precompile compatibility hash mismatch".to_string(),
        ));
    }

    // SAFETY: `detect_precompiled_file` confirmed this is a component artifact; hash matches.
    unsafe { Component::deserialize_file(engine, path) }.map_err(|error| {
        PgWasmError::InvalidModule(format!(
            "failed to deserialize precompiled component: {error}"
        ))
    })
}

/// Build a component linker with WASI (always), wasi-http when `policy.allow_wasi_http`, and
/// `pgwasm:host/*` when permitted by `policy`.
pub(crate) fn build_linker(
    engine: &Engine,
    policy: &EffectivePolicy,
) -> Result<Linker<StoreCtx>, PgWasmError> {
    let mut linker = Linker::new(engine);
    wasmtime_wasi::p2::add_to_linker_sync(&mut linker).map_err(|error| {
        PgWasmError::Internal(format!("failed to add WASI to component linker: {error}"))
    })?;
    if policy.allow_wasi_http {
        // `wasmtime_wasi_http::p2::add_to_linker_sync` also registers proxy WASI interfaces and
        // duplicates `wasmtime_wasi::p2::add_to_linker_sync` (e.g. `wasi:io/error@0.2.6`). Use the
        // HTTP-only helper after full WASI, matching wasmtime-wasi-http's own p2 tests.
        wasmtime_wasi_http::p2::add_only_http_to_linker_sync(&mut linker).map_err(|error| {
            PgWasmError::Internal(format!(
                "failed to add wasi-http to component linker: {error}"
            ))
        })?;
    }
    super::host::add_to_linker(&mut linker, policy)?;
    Ok(linker)
}

/// Substring matched against component import names (e.g. `pgwasm:host/query@0.1.0`).
const QUERY_HOST_IMPORT_SUBSTR: &str = "pgwasm:host/query";

/// Fail before linker instantiation when the component imports the SPI query interface but
/// `policy.allow_spi` is false, so callers see a policy hint instead of a generic linker error.
pub(crate) fn ensure_component_spi_matches_policy(
    engine: &Engine,
    component: &Component,
    policy: &EffectivePolicy,
) -> Result<(), PgWasmError> {
    if policy.allow_spi {
        return Ok(());
    }
    let imports_query = component
        .component_type()
        .imports(engine)
        .any(|(name, _)| name.contains(QUERY_HOST_IMPORT_SUBSTR));
    if imports_query {
        return Err(PgWasmError::PermissionDenied(
            "component imports pgwasm:host/query but SPI is disabled; enable pgwasm.allow_spi"
                .to_string(),
        ));
    }
    Ok(())
}

/// Construct per-store `StoreCtx` from resolved policy (WASI surface narrowed by GUC + overrides).
pub(crate) fn build_store_ctx(policy: &EffectivePolicy) -> Result<StoreCtx, PgWasmError> {
    let mut builder = WasiCtxBuilder::new();
    if policy.allow_wasi && policy.allow_wasi_stdio {
        builder.inherit_stdio();
    }
    if policy.allow_wasi && policy.allow_wasi_env {
        for (key, value) in std::env::vars() {
            builder.env(&key, &value);
        }
    }
    if policy.allow_wasi && policy.allow_wasi_fs {
        for (guest, host) in &policy.wasi_preopens {
            builder
                .preopened_dir(
                    host.as_str(),
                    guest.as_str(),
                    DirPerms::all(),
                    FilePerms::all(),
                )
                .map_err(|error| {
                    PgWasmError::Internal(format!("WASI preopen failed for {guest}: {error}"))
                })?;
        }
    }
    let wasi = builder.build();
    Ok(StoreCtx {
        host: HostState {
            allow_spi: policy.allow_spi,
        },
        http: WasiHttpCtx::new(),
        limits: StoreLimits::default(),
        table: ResourceTable::new(),
        wasi,
    })
}
