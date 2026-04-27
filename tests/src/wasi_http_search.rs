//! Outbound `wasi:http` (WASIp2) against a public JSON API (requires network).

use std::env;

use anyhow::Context;
use serde_json::{Value, json};

use crate::common::{
    bootstrap_extension, call_text, connect, http_search_component_wasm,
    load_options_with_limits_patch, reset_integration_database, reset_pg_wasm_gucs, unique_suffix,
    wasm_fn_ident, wasm_load_bytes, wasm_unload,
};

fn http_search_load_options() -> serde_json::Value {
    // Catalog `limits` must not exceed session GUC ceilings (`resolve_limit` in
    // `pg_wasm` policy). Omit `fuel_per_invocation` (default fixture matches GUC).
    load_options_with_limits_patch(json!({ "invocation_deadline_ms": 30_000_i64 }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DATABASE_URL, RUN_WASI_HTTP_ITEST=1, outbound HTTPS (see tests/README.md)"]
async fn wasi_http_algolia_search_returns_titles() {
    if env::var("RUN_WASI_HTTP_ITEST").ok().as_deref() != Some("1") {
        eprintln!("skip: set RUN_WASI_HTTP_ITEST=1 to run wasi:http network integration test");
        return;
    }

    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_http_{suffix}");

    let client = connect().await.unwrap();
    bootstrap_extension(&client).await.unwrap();
    reset_pg_wasm_gucs(&client).await.unwrap();

    client
        .batch_execute(
            r"
            SET pg_wasm.allow_wasi = on;
            SET pg_wasm.allow_wasi_http = on;
            SET pg_wasm.allow_wasi_net = on;
            SET pg_wasm.allowed_hosts = 'hn.algolia.com:443';
            SET pg_wasm.invocation_deadline_ms = 30000;
            ",
        )
        .await
        .unwrap();

    wasm_load_bytes(
        &client,
        &module_name,
        http_search_component_wasm(),
        http_search_load_options(),
    )
    .await
    .unwrap();

    let json_text = call_text(&client, &module_name, "search-titles")
        .await
        .unwrap();
    assert!(
        !json_text.starts_with("error:"),
        "guest reported failure: {json_text}"
    );

    let v: Value = serde_json::from_str(&json_text).expect("Algolia JSON");
    let hits = v
        .get("hits")
        .and_then(Value::as_array)
        .context("expected top-level hits array")
        .unwrap();
    let titles: Vec<&str> = hits
        .iter()
        .filter_map(|h| h.get("title").and_then(Value::as_str))
        .filter(|t| !t.is_empty())
        .collect();
    assert!(
        titles.len() >= 3,
        "expected at least 3 non-empty titles, got {}",
        titles.len()
    );

    wasm_unload(&client, &module_name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires DATABASE_URL, RUN_WASI_HTTP_ITEST=1 (see tests/README.md)"]
async fn wasi_http_invocation_fails_when_allow_wasi_http_off() {
    if env::var("RUN_WASI_HTTP_ITEST").ok().as_deref() != Some("1") {
        eprintln!("skip: set RUN_WASI_HTTP_ITEST=1 to run wasi:http policy integration test");
        return;
    }

    reset_integration_database().await.unwrap();
    let suffix = unique_suffix();
    let module_name = format!("itest_http_policy_{suffix}");

    let client = connect().await.unwrap();
    bootstrap_extension(&client).await.unwrap();
    reset_pg_wasm_gucs(&client).await.unwrap();

    client
        .batch_execute(
            r"
            SET pg_wasm.allow_wasi = on;
            SET pg_wasm.allow_wasi_http = off;
            SET pg_wasm.allow_wasi_net = on;
            SET pg_wasm.allowed_hosts = 'hn.algolia.com:443';
            SET pg_wasm.invocation_deadline_ms = 30000;
            ",
        )
        .await
        .unwrap();

    wasm_load_bytes(
        &client,
        &module_name,
        http_search_component_wasm(),
        http_search_load_options(),
    )
    .await
    .unwrap();

    let ident = wasm_fn_ident(&module_name, "search-titles");
    let sql = format!(r#"SELECT wasm."{}"()"#, ident.replace('"', "\"\""));
    let err = client
        .query_one(&sql, &[])
        .await
        .expect_err("expected instantiation/link failure without wasi:http");
    let db_err = err.as_db_error().expect("postgres error");
    let msg = db_err.message().to_lowercase();
    let detail = db_err
        .detail()
        .map(|s| s.to_lowercase())
        .unwrap_or_default();
    let combined = format!("{msg} {detail}");
    assert!(
        combined.contains("unknown import")
            || combined.contains("wasi:http")
            || combined.contains("linker")
            || combined.contains("http")
            || combined.contains("wasmtime"),
        "unexpected error text: {} | {}",
        db_err.message(),
        db_err.detail().unwrap_or("")
    );

    wasm_unload(&client, &module_name).await.unwrap();
}
