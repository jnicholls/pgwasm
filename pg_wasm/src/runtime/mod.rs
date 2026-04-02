//! WASM execution backends. At most one primary backend is used per module; see [`selection`] and
//! [`dispatch`].

mod stub;

#[cfg(feature = "_pg_wasm_runtime")]
pub mod dispatch;
#[cfg(feature = "runtime_extism")]
pub mod extism_backend;
#[cfg(feature = "_pg_wasm_runtime")]
pub mod selection;
#[cfg(feature = "runtime_extism")]
pub mod wasm_bytes_exports;
#[cfg(feature = "runtime_wasmer")]
pub mod wasmer_backend;
#[cfg(feature = "runtime_wasmtime")]
pub mod wasmtime_backend;

pub use stub::StubWasmBackend;
#[cfg(feature = "_pg_wasm_runtime")]
pub use selection::ModuleExecutionBackend;

/// Which concrete runtime executes a module.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeKind {
    Wasmtime,
    Wasmer,
    Extism,
}

/// Common surface for runtime-specific engines (filled in as invocation is implemented).
pub trait WasmRuntimeBackend: Send + Sync {
    fn kind(&self) -> RuntimeKind;

    fn label(&self) -> &'static str;
}
