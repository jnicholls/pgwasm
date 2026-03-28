use super::{RuntimeKind, WasmRuntimeBackend};

/// Marker for the Extism plugin path; `Plugin` wiring comes in the additional-runtimes todo.
pub struct ExtismBackend;

impl WasmRuntimeBackend for ExtismBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Extism
    }

    fn label(&self) -> &'static str {
        "extism"
    }
}
