use pgrx::prelude::*;

mod config;
mod mapping;
mod registry;
mod runtime;
mod trampoline;

pub use config::{HostPolicy, LoadOptions};
pub use mapping::{ExportSignature, PgWasmArgDesc, PgWasmReturnDesc, PgWasmTypeKind};
pub use registry::{ModuleId, RegisteredFunction, lookup_by_fn_oid, register_fn_oid};
pub use runtime::{RuntimeKind, StubWasmBackend, WasmRuntimeBackend};
pub use trampoline::TRAMPOLINE_PG_SRC;

#[cfg(feature = "runtime_wasmtime")]
pub use runtime::wasmtime_backend::WasmtimeBackend;
#[cfg(feature = "runtime_wasmer")]
pub use runtime::wasmer_backend::WasmerBackend;
#[cfg(feature = "runtime_extism")]
pub use runtime::extism_backend::ExtismBackend;

::pgrx::pg_module_magic!(name, version);

#[pg_extern]
fn hello_pg_wasm() -> &'static str {
    "Hello, pg_wasm"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_wasm() {
        assert_eq!("Hello, pg_wasm", crate::hello_pg_wasm());
    }

    #[cfg(feature = "runtime_wasmtime")]
    #[pg_test]
    fn test_wasmtime_backend_instantiates() {
        let _ = crate::WasmtimeBackend::new();
    }
}

/// Required by `cargo pgrx test`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
