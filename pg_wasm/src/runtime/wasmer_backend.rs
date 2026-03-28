use wasmer::Store;

use super::{RuntimeKind, WasmRuntimeBackend};

/// Wasmer-backed store holder; module load and invoke are added in later todos.
pub struct WasmerBackend {
    #[allow(dead_code)]
    store: Store,
}

impl WasmerBackend {
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: Store::default(),
        }
    }
}

impl Default for WasmerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl WasmRuntimeBackend for WasmerBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Wasmer
    }

    fn label(&self) -> &'static str {
        "wasmer"
    }
}
