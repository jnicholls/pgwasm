//! `max_memory_pages` enforced via store limits should surface as a trap-style SQL error.

use serde_json::json;

use crate::common::{
    bootstrap_extension, connect, itest_component_wasm, load_options_with_limits_patch,
    reset_integration_database, reset_pgwasm_gucs, unique_suffix, wasm_fn_ident, wasm_load_bytes,
    wasm_unload,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DATABASE_URL to a cluster with pgwasm installed (see tests/README.md)"]
async fn max_memory_pages_triggers_trap_or_resource_error() {
    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_mem_{suffix}");

    let client = connect().await.unwrap();
    bootstrap_extension(&client).await.unwrap();
    reset_pgwasm_gucs(&client).await.unwrap();

    let options = load_options_with_limits_patch(json!({ "max_memory_pages": 1_i64 }));
    wasm_load_bytes(&client, &module_name, itest_component_wasm(), options)
        .await
        .unwrap();

    let ident = wasm_fn_ident(&module_name, "grow");
    let sql = format!(r#"SELECT wasm."{}"()"#, ident.replace('"', "\"\""));
    let err = client
        .query_one(&sql, &[])
        .await
        .expect_err("expected memory growth to fail under tight page cap");
    let db_err = err.as_db_error().expect("postgres error");
    let code = db_err.code().code();
    assert!(
        code == "38000" || code == "54000",
        "unexpected sqlstate {code}: {}",
        db_err.message()
    );
    let msg = db_err.message().to_lowercase();
    assert!(
        msg.contains("memory")
            || msg.contains("grow")
            || msg.contains("limit")
            || msg.contains("wasm")
            || msg.contains("trap")
            || msg.contains("unreachable"),
        "unexpected message: {}",
        db_err.message()
    );

    wasm_unload(&client, &module_name).await.unwrap();
}
