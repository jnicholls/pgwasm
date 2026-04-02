//! Dispatch invocation and teardown to the backend selected at load time.

use crate::registry::ModuleId;
use crate::runtime::selection::ModuleExecutionBackend;

#[cfg(feature = "runtime_extism")]
use super::extism_backend;
#[cfg(feature = "runtime_wasmer")]
use super::wasmer_backend;
#[cfg(feature = "runtime_wasmtime")]
use super::wasmtime_backend;

pub fn remove_compiled_module(backend: ModuleExecutionBackend, id: ModuleId) {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::remove_compiled_module(id),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::remove_compiled_module(id),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::remove_compiled_module(id),
    }
}

pub fn compile_store_and_list_exports(
    backend: ModuleExecutionBackend,
    id: ModuleId,
    wasm: &[u8],
    export_hints: &crate::mapping::ExportHintMap,
    abi: crate::abi::WasmAbiKind,
) -> Result<(Vec<(String, crate::mapping::ExportSignature)>, bool), String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => {
            wasmtime_backend::compile_store_and_list_exports(id, wasm, export_hints, abi)
        }
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => {
            wasmer_backend::compile_store_and_list_exports(id, wasm, export_hints, abi)
        }
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => {
            extism_backend::compile_store_and_list_exports(id, wasm, export_hints, abi)
        }
    }
}

pub fn call_lifecycle_hook(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export_name: &str,
    config: &[u8],
) -> Result<(), String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => {
            wasmtime_backend::call_lifecycle_hook(module, export_name, config)
        }
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => {
            wasmer_backend::call_lifecycle_hook(module, export_name, config)
        }
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => {
            extism_backend::call_lifecycle_hook(module, export_name, config)
        }
    }
}

pub fn call_mem_in_out(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    input: &[u8],
) -> Result<Vec<u8>, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_mem_in_out(module, export, input),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_mem_in_out(module, export, input),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_mem_in_out(module, export, input),
    }
}

pub fn call_i32_arity0(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
) -> Result<i32, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_i32_arity0(module, export),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_i32_arity0(module, export),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_i32_arity0(module, export),
    }
}

pub fn call_i32_arity1(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: i32,
) -> Result<i32, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_i32_arity1(module, export, a),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_i32_arity1(module, export, a),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_i32_arity1(module, export, a),
    }
}

pub fn call_i32_arity2(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: i32,
    b: i32,
) -> Result<i32, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_i32_arity2(module, export, a, b),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_i32_arity2(module, export, a, b),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_i32_arity2(module, export, a, b),
    }
}

pub fn call_bool_result_arity0(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
) -> Result<bool, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => {
            wasmtime_backend::call_bool_result_arity0(module, export)
        }
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_bool_result_arity0(module, export),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_bool_result_arity0(module, export),
    }
}

pub fn call_bool_result_arity1(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: bool,
) -> Result<bool, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => {
            wasmtime_backend::call_bool_result_arity1(module, export, a)
        }
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_bool_result_arity1(module, export, a),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_bool_result_arity1(module, export, a),
    }
}

pub fn call_bool_result_arity2(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: bool,
    b: bool,
) -> Result<bool, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => {
            wasmtime_backend::call_bool_result_arity2(module, export, a, b)
        }
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => {
            wasmer_backend::call_bool_result_arity2(module, export, a, b)
        }
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => {
            extism_backend::call_bool_result_arity2(module, export, a, b)
        }
    }
}

pub fn call_i64_arity0(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
) -> Result<i64, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_i64_arity0(module, export),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_i64_arity0(module, export),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_i64_arity0(module, export),
    }
}

pub fn call_i64_arity1(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: i64,
) -> Result<i64, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_i64_arity1(module, export, a),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_i64_arity1(module, export, a),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_i64_arity1(module, export, a),
    }
}

pub fn call_f32_arity0(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
) -> Result<f32, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_f32_arity0(module, export),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_f32_arity0(module, export),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_f32_arity0(module, export),
    }
}

pub fn call_f32_arity1(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: f32,
) -> Result<f32, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_f32_arity1(module, export, a),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_f32_arity1(module, export, a),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_f32_arity1(module, export, a),
    }
}

pub fn call_f32_arity2(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: f32,
    b: f32,
) -> Result<f32, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_f32_arity2(module, export, a, b),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_f32_arity2(module, export, a, b),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_f32_arity2(module, export, a, b),
    }
}

pub fn call_f64_arity0(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
) -> Result<f64, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_f64_arity0(module, export),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_f64_arity0(module, export),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_f64_arity0(module, export),
    }
}

pub fn call_f64_arity1(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: f64,
) -> Result<f64, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_f64_arity1(module, export, a),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_f64_arity1(module, export, a),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_f64_arity1(module, export, a),
    }
}

pub fn call_f64_arity2(
    backend: ModuleExecutionBackend,
    module: ModuleId,
    export: &str,
    a: f64,
    b: f64,
) -> Result<f64, String> {
    match backend {
        #[cfg(feature = "runtime_wasmtime")]
        ModuleExecutionBackend::Wasmtime => wasmtime_backend::call_f64_arity2(module, export, a, b),
        #[cfg(feature = "runtime_wasmer")]
        ModuleExecutionBackend::Wasmer => wasmer_backend::call_f64_arity2(module, export, a, b),
        #[cfg(feature = "runtime_extism")]
        ModuleExecutionBackend::Extism => extism_backend::call_f64_arity2(module, export, a, b),
    }
}
