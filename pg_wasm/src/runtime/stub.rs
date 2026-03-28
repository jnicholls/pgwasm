use super::{RuntimeKind, WasmRuntimeBackend};

/// Used in tests or when no native backend is linked (not used in default `runtime_wasmtime` builds).
pub struct StubWasmBackend;

impl WasmRuntimeBackend for StubWasmBackend {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Wasmtime
    }

    fn label(&self) -> &'static str {
        "stub"
    }
}
