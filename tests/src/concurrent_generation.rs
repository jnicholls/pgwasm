//! Backend B observes a module loaded on backend A (catalog + generation propagation).

use crate::common::{
    bootstrap_extension, call_i32, connect, itest_component_wasm, itest_load_options,
    reset_integration_database, reset_pgwasm_gucs, unique_suffix, wasm_load_bytes, wasm_unload,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DATABASE_URL to a cluster with pgwasm installed (see tests/README.md)"]
async fn concurrent_backend_sees_loaded_module_and_generation() {
    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_gen_{suffix}");

    let client_a = connect().await.unwrap();
    bootstrap_extension(&client_a).await.unwrap();
    reset_pgwasm_gucs(&client_a).await.unwrap();

    wasm_load_bytes(
        &client_a,
        &module_name,
        itest_component_wasm(),
        itest_load_options(),
    )
    .await
    .unwrap();

    let client_b = connect().await.unwrap();
    let v = call_i32(&client_b, &module_name, "add").await.unwrap();
    assert_eq!(v, 42, "backend B should call the export registered by A");

    wasm_unload(&client_a, &module_name).await.unwrap();
}
