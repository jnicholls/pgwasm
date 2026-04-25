#![allow(dead_code)]

use pgrx::prelude::*;

mod abi;
mod artifacts;
mod catalog;
mod config;
mod errors;
mod guc;
mod hooks;
mod lifecycle;
mod mapping;
mod policy;
mod proc_reg;
mod registry;
mod runtime;
mod shmem;
mod trampoline;
mod views;
mod wit;

::pgrx::pg_module_magic!(name, version);

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    shmem::init();
    runtime::init();
    catalog::init();
}

#[pg_extern]
fn hello_pg_wasm() -> &'static str {
    "Hello, pg_wasm"
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_pg_wasm() {
        assert_eq!("Hello, pg_wasm", crate::hello_pg_wasm());
    }
}

// TODO(wave-4): replace with lifecycle::load — temporary regress hook for core scalar path.
// Registered in the extension schema (`wasm` per pg_wasm.control); SQL calls `wasm._core_invoke_scalar`.
#[cfg(feature = "pg_test")]
mod core_invoke_regress {
    use pgrx::prelude::*;
    use wasmtime::Val;

    use crate::errors::PgWasmError;
    use crate::mapping::scalars;
    use crate::policy;
    use crate::runtime::core as runtime_core;
    use crate::runtime::engine;

    #[pg_extern]
    pub fn _core_invoke_scalar(bytes: &[u8], export: &str, i32args: Vec<i32>) -> i32 {
        match invoke_inner(bytes, export, &i32args) {
            Ok(v) => v,
            Err(err) => {
                ereport!(PgLogLevel::ERROR, err.sqlstate(), err.to_string());
                unreachable!("ereport should not return");
            }
        }
    }

    fn invoke_inner(bytes: &[u8], export: &str, i32args: &[i32]) -> Result<i32, PgWasmError> {
        let wasm_engine = engine::try_shared_engine()?;
        let loaded = runtime_core::compile(wasm_engine, bytes)?;
        let guc_snapshot = policy::GucSnapshot::from_gucs();
        let effective = policy::resolve(&guc_snapshot, None, None)?;
        let vals: Vec<Val> = scalars::i32_vec_to_vals(i32args);
        match runtime_core::invoke(&loaded, export, &vals, &effective)? {
            Val::I32(i) => Ok(i),
            other => Err(PgWasmError::Internal(format!(
                "core export returned non-i32 scalar: {other:?}"
            ))),
        }
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
