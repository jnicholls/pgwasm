//! Cold attach after Postgres restart: catalog + `.cwasm` still serve invocations.

use std::path::Path;
use std::process::Command;

use crate::common::{
    bootstrap_extension, call_i32, connect, database_url, itest_component_wasm, itest_load_options,
    reset_integration_database, reset_pgwasm_gucs, unique_suffix, wasm_load_bytes, wasm_unload,
};

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("tests crate should live one level below workspace root")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DATABASE_URL and mutates cluster via cargo pgrx stop/start (see tests/README.md)"]
async fn backend_restart_still_invokes_loaded_module() {
    reset_integration_database().await.unwrap();
    let _ = database_url();

    let suffix = unique_suffix();
    let module_name = format!("itest_restart_{suffix}");

    let client = connect().await.unwrap();
    bootstrap_extension(&client).await.unwrap();
    reset_pgwasm_gucs(&client).await.unwrap();

    wasm_load_bytes(
        &client,
        &module_name,
        itest_component_wasm(),
        itest_load_options(),
    )
    .await
    .unwrap();
    assert_eq!(call_i32(&client, &module_name, "add").await.unwrap(), 42);

    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["pgrx", "stop"])
        .status()
        .expect("spawn cargo pgrx stop");
    assert!(status.success(), "cargo pgrx stop failed: {status:?}");

    let status = Command::new("cargo")
        .current_dir(workspace_root())
        .args(["pgrx", "start"])
        .status()
        .expect("spawn cargo pgrx start");
    assert!(status.success(), "cargo pgrx start failed: {status:?}");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let client2 = connect().await.unwrap();
    assert_eq!(
        call_i32(&client2, &module_name, "add").await.unwrap(),
        42,
        "after restart, wasm should cold-attach from persisted artifacts"
    );

    wasm_unload(&client2, &module_name).await.unwrap();
}
