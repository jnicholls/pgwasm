//! Resolve which execution backend handles a load (`options.runtime` + ABI + enabled features).

use crate::abi::WasmAbiKind;

/// Concrete runtime that owns compiled artifacts and invokes exports for a module.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModuleExecutionBackend {
    #[cfg(feature = "runtime-wasmtime")]
    Wasmtime,
    #[cfg(feature = "runtime-extism")]
    Extism,
}

impl ModuleExecutionBackend {
    pub const fn as_catalog_str(self) -> &'static str {
        match self {
            #[cfg(feature = "runtime-wasmtime")]
            Self::Wasmtime => "wasmtime",
            #[cfg(feature = "runtime-extism")]
            Self::Extism => "extism",
        }
    }
}

fn normalize_runtime_opt(opt: Option<&str>) -> Option<String> {
    let s = opt?.trim();
    if s.is_empty() {
        return None;
    }
    Some(s.to_ascii_lowercase())
}

#[allow(unreachable_code)]
fn default_core_backend() -> Result<ModuleExecutionBackend, String> {
    #[cfg(feature = "runtime-wasmtime")]
    {
        return Ok(ModuleExecutionBackend::Wasmtime);
    }
    #[cfg(all(not(feature = "runtime-wasmtime"), feature = "runtime-extism"))]
    {
        return Ok(ModuleExecutionBackend::Extism);
    }
    unreachable!("pg_wasm: enable at least one runtime-wasmtime or runtime-extism feature")
}

/// Pick the backend for `load_from_bytes` after ABI detection.
pub fn resolve_load_backend(
    abi: WasmAbiKind,
    runtime_opt: Option<&str>,
) -> Result<ModuleExecutionBackend, String> {
    let normalized = normalize_runtime_opt(runtime_opt);

    match abi {
        WasmAbiKind::ComponentModel => {
            if let Some(ref s) = normalized {
                if s != "wasmtime" && s != "auto" {
                    return Err(format!(
                        "pg_wasm_load: WebAssembly components require runtime \"wasmtime\" (got {s:?})"
                    ));
                }
            }
            #[cfg(feature = "runtime-wasmtime")]
            {
                return Ok(ModuleExecutionBackend::Wasmtime);
            }
            #[cfg(not(feature = "runtime-wasmtime"))]
            {
                return Err(
                    "pg_wasm_load: component model requires the `runtime-wasmtime` feature".into(),
                );
            }
        }
        WasmAbiKind::Extism => {
            if let Some(ref s) = normalized {
                if s != "extism" && s != "auto" {
                    return Err(format!(
                        "pg_wasm_load: Extism plugin wasm requires runtime \"extism\" (got {s:?})"
                    ));
                }
            }
            #[cfg(feature = "runtime-extism")]
            {
                return Ok(ModuleExecutionBackend::Extism);
            }
            #[cfg(not(feature = "runtime-extism"))]
            {
                return Err(
                    "pg_wasm_load: Extism ABI requires the `runtime-extism` feature".into(),
                );
            }
        }
        WasmAbiKind::CoreWasm => {
            let choice = normalized.as_deref().unwrap_or("auto");
            match choice {
                "auto" => default_core_backend(),
                "wasmtime" => {
                    #[cfg(feature = "runtime-wasmtime")]
                    {
                        Ok(ModuleExecutionBackend::Wasmtime)
                    }
                    #[cfg(not(feature = "runtime-wasmtime"))]
                    {
                        Err("pg_wasm_load: runtime \"wasmtime\" requires the `runtime-wasmtime` feature".into())
                    }
                }
                "extism" => Err(
                    "pg_wasm_load: runtime \"extism\" is only valid for Extism plugin wasm (extism:host imports)"
                        .into(),
                ),
                other => Err(format!(
                    "pg_wasm_load: unknown runtime {other:?} (use wasmtime, extism, or auto)"
                )),
            }
        }
    }
}
