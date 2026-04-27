//! Policy widen denied at `wasm.load` → ERRCODE_INSUFFICIENT_PRIVILEGE (42501).

use serde_json::json;

use crate::common::{
    bootstrap_extension, connect, itest_component_wasm, reset_integration_database,
    reset_pgwasm_gucs, unique_suffix, wasm_load_bytes,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DATABASE_URL to a cluster with pgwasm installed (see tests/README.md)"]
async fn wasi_policy_widen_denied_at_load_maps_to_insufficient_privilege() {
    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_policy_{suffix}");

    let client = connect().await.unwrap();
    bootstrap_extension(&client).await.unwrap();
    reset_pgwasm_gucs(&client).await.unwrap();
    client
        .batch_execute(
            r"
            SET pgwasm.allow_wasi = on;
            SET pgwasm.allow_wasi_fs = off;
            ",
        )
        .await
        .unwrap();

    let options = json!({
        "overrides": { "allow_wasi_fs": true }
    });
    let err = wasm_load_bytes(&client, &module_name, itest_component_wasm(), options)
        .await
        .expect_err("expected policy widen to be denied at load");
    let text = format!("{err:#}").to_lowercase();
    assert!(
        text.contains("permission denied")
            || text.contains("allow_wasi_fs")
            || text.contains("widen"),
        "unexpected error text: {err:#}"
    );
}
