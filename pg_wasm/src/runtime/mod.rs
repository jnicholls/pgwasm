//! WASM execution backends. At most one primary backend is used per module; see [`selection`] and
//! [`dispatch`].

mod stub;

#[cfg(feature = "runtime-wasmtime")]
pub mod component_marshal;
#[cfg(feature = "runtime-wasmtime")]
pub mod composite_marshal;
pub mod dispatch;
#[cfg(feature = "runtime-extism")]
pub mod extism_backend;
pub mod selection;
#[cfg(feature = "runtime-extism")]
pub mod wasm_bytes_exports;
#[cfg(feature = "runtime-wasmtime")]
pub mod wasmtime_backend;

pub use selection::ModuleExecutionBackend;
#[allow(unused_imports)]
pub use stub::StubWasmBackend;

/// Which concrete runtime executes a module.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RuntimeKind {
    Wasmtime,
    Extism,
}

/// Common surface for runtime-specific engines (filled in as invocation is implemented).
pub trait WasmRuntimeBackend: Send + Sync {
    fn kind(&self) -> RuntimeKind;

    fn label(&self) -> &'static str;
}
