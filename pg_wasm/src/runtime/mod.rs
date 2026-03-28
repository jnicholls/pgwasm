//! WASM execution backends. At most one primary backend is used per module; selection is implemented later.

mod stub;

#[cfg(feature = "runtime_wasmer")]
pub mod wasmer_backend;
#[cfg(feature = "runtime_wasmtime")]
pub mod wasmtime_backend;
#[cfg(feature = "runtime_extism")]
pub mod extism_backend;

pub use stub::StubWasmBackend;

/// Which concrete runtime executes a module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
