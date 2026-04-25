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

mod sql_api {
    #![allow(clippy::result_large_err)]

    use pgrx::prelude::*;

    #[pg_extern]
    fn reconfigure(
        module_name: &str,
        policy: Option<pgrx::Json>,
        limits: Option<pgrx::Json>,
    ) -> core::result::Result<bool, pgrx::pg_sys::panic::ErrorReport> {
        crate::lifecycle::reconfigure::reconfigure_impl(module_name, policy, limits)
            .map_err(crate::errors::PgWasmError::into_error_report)
    }
}

#[cfg(feature = "pg_test")]
#[pg_schema]
mod tests {
    use pgrx::Json;
    use pgrx::prelude::*;
    use pgrx::spi::Spi;
    use serde_json::json;

    use crate::catalog::{exports, modules};
    use crate::config::{Limits, PolicyOverrides};
    use crate::errors::PgWasmError;
    use crate::lifecycle::reconfigure;
    use crate::policy::{self, GucSnapshot};
    use crate::shmem;

    #[pg_test]
    fn test_hello_pg_wasm() {
        assert_eq!("Hello, pg_wasm", crate::hello_pg_wasm());
    }

    #[pg_test]
    fn reconfigure_narrows_updates_catalog_and_bumps_generation() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("reconf_mod_{}", std::process::id());
        let new_module = modules::NewModule {
            abi: "component".to_string(),
            artifact_path: "/tmp/stub.wasm".to_string(),
            digest: vec![1, 2, 3],
            generation: 0,
            limits: json!({}),
            name: name.clone(),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![0; 32],
            wit_world: "default".to_string(),
        };
        let inserted = modules::insert(&new_module).expect("stub module insert");

        let hook_export = exports::NewExport {
            arg_types: vec![],
            fn_oid: None,
            kind: "hook".to_string(),
            module_id: inserted.module_id,
            ret_type: None,
            signature: json!({}),
            sql_name: "on_reconfigure_stub".to_string(),
            wasm_name: "on-reconfigure".to_string(),
        };
        let hook_row = exports::insert(&hook_export).expect("hook export insert");

        let gen_before = shmem::read_generation();

        let narrow = Json(json!({ "allow_wasi_net": false }));
        let ok = reconfigure::reconfigure_impl(name.as_str(), Some(narrow), None)
            .expect("narrow reconfigure");
        assert!(ok);

        let after = modules::get_by_name(&name)
            .expect("module read")
            .expect("module should exist");
        assert_eq!(after.generation, 1);
        assert_eq!(
            after.policy.get("allow_wasi_net"),
            Some(&serde_json::Value::Bool(false))
        );
        assert!(shmem::read_generation() > gen_before);

        let widen = Json(json!({ "allow_wasi_http": true }));
        let err = reconfigure::reconfigure_impl(name.as_str(), Some(widen), None)
            .expect_err("widen should be denied");
        match err {
            PgWasmError::PermissionDenied(message) => {
                assert!(
                    message.contains("allow_wasi_http"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected PermissionDenied, got {other:?}"),
        }

        let unchanged = modules::get_by_name(&name)
            .expect("module read")
            .expect("module should still exist");
        assert_eq!(unchanged.generation, 1);

        exports::delete(hook_row.export_id).expect("hook export delete");
        modules::delete(inserted.module_id).expect("stub module delete");
    }

    #[pg_test]
    fn reconfigure_effective_limits_follow_catalog() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();

        let name = format!("reconf_limits_{}", std::process::id());
        let new_module = modules::NewModule {
            abi: "component".to_string(),
            artifact_path: "/tmp/stub.wasm".to_string(),
            digest: vec![4, 5, 6],
            generation: 0,
            limits: json!({}),
            name: name.clone(),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![1; 32],
            wit_world: "default".to_string(),
        };
        let inserted = modules::insert(&new_module).expect("stub module insert");

        let patch_limits = Json(json!({ "fuel_per_invocation": 99 }));
        reconfigure::reconfigure_impl(name.as_str(), None, Some(patch_limits)).expect("limits");

        let row = modules::get_by_name(&name)
            .expect("read module")
            .expect("exists");
        assert_eq!(
            row.limits.get("fuel_per_invocation"),
            Some(&serde_json::Value::Number(99.into()))
        );

        let limits = Limits {
            fuel_per_invocation: Some(99),
            ..Default::default()
        };
        let overrides = PolicyOverrides::default();
        let effective = policy::resolve(&GucSnapshot::from_gucs(), Some(&overrides), Some(&limits))
            .expect("resolve after catalog write");

        assert_eq!(effective.fuel_per_invocation, 99);
        // TODO(wave-3: invocation-path): assert `StoreLimits` / epoch deadline on the next pooled
        // invocation once the trampoline reads catalog-backed limits per call.

        modules::delete(inserted.module_id).expect("cleanup");
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
