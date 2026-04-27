//! Tight per-module fuel maps to SQLSTATE 54000 (program limit exceeded).

use serde_json::json;

use crate::common::{
    bootstrap_extension, connect, itest_component_wasm, load_options_with_limits_patch,
    reset_integration_database, reset_pgwasm_gucs, unique_suffix, wasm_fn_ident, wasm_load_bytes,
    wasm_unload,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DATABASE_URL to a cluster with pgwasm installed (see tests/README.md)"]
async fn fuel_exhaustion_maps_to_program_limit_exceeded() {
    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_fuel_{suffix}");

    let client = connect().await.unwrap();
    bootstrap_extension(&client).await.unwrap();
    reset_pgwasm_gucs(&client).await.unwrap();
    client
        .execute("SET pgwasm.fuel_enabled = on", &[])
        .await
        .unwrap();

    let options = load_options_with_limits_patch(json!({ "fuel_per_invocation": 800_i64 }));
    wasm_load_bytes(&client, &module_name, itest_component_wasm(), options)
        .await
        .unwrap();

    let ident = wasm_fn_ident(&module_name, "spin-param");
    let sql = format!(
        r#"SELECT wasm."{}"(1000000::int4)"#,
        ident.replace('"', "\"\"")
    );
    let err = client
        .query_one(&sql, &[])
        .await
        .expect_err("expected fuel exhaustion");
    let db_err = err.as_db_error().expect("postgres error");
    assert_eq!(db_err.code().code(), "54000");
    assert!(
        db_err.message().to_lowercase().contains("fuel"),
        "message: {}",
        db_err.message()
    );

    wasm_unload(&client, &module_name).await.unwrap();
    client.execute("RESET pgwasm.fuel_enabled", &[]).await.ok();
}
