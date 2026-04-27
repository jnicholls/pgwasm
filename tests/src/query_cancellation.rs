//! `pg_cancel_backend` surfaces as SQLSTATE 57014 (epoch / interrupt path).

use crate::common::{
    bootstrap_extension, connect, itest_component_wasm, itest_load_options,
    reset_integration_database, reset_pgwasm_gucs, unique_suffix, wasm_fn_ident, wasm_load_bytes,
    wasm_unload,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires DATABASE_URL to a cluster with pgwasm installed (see tests/README.md)"]
async fn query_cancel_maps_to_query_canceled() {
    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_cancel_{suffix}");

    let setup = connect().await.unwrap();
    bootstrap_extension(&setup).await.unwrap();
    reset_pgwasm_gucs(&setup).await.unwrap();
    setup
        .execute("SET pgwasm.invocation_deadline_ms = '600000'", &[])
        .await
        .unwrap();

    wasm_load_bytes(
        &setup,
        &module_name,
        itest_component_wasm(),
        itest_load_options(),
    )
    .await
    .unwrap();

    let spin_client = connect().await.unwrap();
    let ident = wasm_fn_ident(&module_name, "spin");
    let sql = format!(r#"SELECT wasm."{}"()"#, ident.replace('"', "\"\""));

    let spin_task = tokio::spawn(async move {
        spin_client
            .execute("SET statement_timeout = 0", &[])
            .await
            .ok();
        spin_client.query_one(&sql, &[]).await
    });

    let killer = connect().await.unwrap();
    let pid_row = setup
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap();
    let spin_pid: i32 = pid_row.get(0);

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    killer
        .execute("SELECT pg_cancel_backend($1)", &[&spin_pid])
        .await
        .unwrap();

    let err = spin_task
        .await
        .unwrap()
        .expect_err("spin should be canceled");
    let db_err = err.as_db_error().expect("postgres error");
    assert_eq!(
        db_err.code().code(),
        "57014",
        "expected query_canceled, got {:?}: {}",
        db_err.code(),
        db_err.message()
    );

    wasm_unload(&setup, &module_name).await.unwrap();
}
