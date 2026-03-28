use wasmtime::Engine;

use super::{RuntimeKind, WasmRuntimeBackend};

/// Wasmtime-backed engine holder; compilation and linking are added in later todos.
pub struct WasmtimeBackend {
    #[allow(dead_code)]
    engine: Engine,
}

impl WasmtimeBackend {
    #[must_use]
    pub fn new() -> Self {
        Self {
            engine: Engine::default(),
        }
    }
}

impl Default for WasmtimeBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmRuntimeBackend for WasmtimeBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Wasmtime
    }

    fn label(&self) -> &'static str {
        "wasmtime"
    }
}
