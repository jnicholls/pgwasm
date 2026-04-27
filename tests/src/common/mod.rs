//! Shared helpers for tokio-postgres integration tests against `pg_wasm`.

use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use url::Url;

static PGWASM_ITEST_RESET: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// PostgreSQL wire URL for integration runs.
///
/// Defaults to the `pgwasm_itest` database on pgrx’s usual port (`28800 + major`). Using a
/// dedicated database avoids a broken `CREATE EXTENSION` state on the template `postgres` DB
/// after repeated `DROP EXTENSION` / `cargo pgrx stop` cycles.
pub(crate) fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost:28817/pgwasm_itest".into())
}

async fn admin_client_for(target: &Url) -> Result<Client> {
    let mut admin = target.clone();
    admin.set_path("/postgres");
    let (client, connection) = tokio_postgres::connect(admin.as_str(), NoTls)
        .await
        .context("connect maintenance postgres db")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// Drop and recreate `pgwasm_itest` from `template0` (call once per test; use `--test-threads=1`).
pub(crate) async fn reset_integration_database() -> Result<()> {
    let target = Url::parse(&database_url()).context("parse DATABASE_URL")?;
    let db = target.path().trim_start_matches('/');
    if db != "pgwasm_itest" {
        return Ok(());
    }
    let _guard = PGWASM_ITEST_RESET.lock().await;
    let admin = admin_client_for(&target).await?;
    admin
        .execute("DROP DATABASE IF EXISTS pgwasm_itest WITH (FORCE)", &[])
        .await
        .context("DROP DATABASE pgwasm_itest")?;
    admin
        .execute("CREATE DATABASE pgwasm_itest TEMPLATE template0", &[])
        .await
        .context("CREATE DATABASE pgwasm_itest from template0")?;
    Ok(())
}

pub(crate) fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}_{}", std::process::id(), nanos)
}

pub(crate) async fn connect() -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(&database_url(), NoTls)
        .await
        .context("connect DATABASE_URL")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

pub(crate) async fn bootstrap_extension(client: &Client) -> Result<()> {
    client
        .batch_execute(
            r"
            DROP EXTENSION IF EXISTS pg_wasm CASCADE;
            CREATE EXTENSION pg_wasm;
            ",
        )
        .await
        .context("bootstrap pg_wasm extension")?;
    Ok(())
}

pub(crate) async fn reset_pg_wasm_gucs(client: &Client) -> Result<()> {
    client
        .batch_execute(
            r"
            RESET pg_wasm.allow_wasi;
            RESET pg_wasm.allow_wasi_fs;
            RESET pg_wasm.allow_wasi_http;
            RESET pg_wasm.allow_wasi_net;
            RESET pg_wasm.allow_wasi_stdio;
            RESET pg_wasm.allowed_hosts;
            RESET pg_wasm.wasi_preopens;
            RESET pg_wasm.fuel_enabled;
            RESET pg_wasm.invocation_deadline_ms;
            ",
        )
        .await
        .context("reset pg_wasm GUCs")?;
    Ok(())
}

pub(crate) fn wasm_hex_literal(wasm: &[u8]) -> String {
    hex::encode(wasm)
}

/// `wasm.load(name, json_build_object('bytes', $hex_text), options)` where `$hex_text` is a hex string.
pub(crate) async fn wasm_load_bytes(
    client: &Client,
    module_name: &str,
    wasm: &[u8],
    options: serde_json::Value,
) -> Result<()> {
    let hex = wasm_hex_literal(wasm);
    let options_text = serde_json::to_string(&options)
        .with_context(|| format!("serialize wasm.load options for {module_name}"))?;
    client
        .execute(
            "SELECT wasm.load($1::text, json_build_object('bytes', $2::text), $3::text::json)",
            &[&module_name, &hex.as_str(), &options_text],
        )
        .await
        .with_context(|| format!("wasm.load({module_name})"))?;
    Ok(())
}

pub(crate) async fn wasm_unload(client: &Client, module_name: &str) -> Result<()> {
    client
        .execute("SELECT wasm.unload($1::text, true)", &[&module_name])
        .await
        .with_context(|| format!("wasm.unload({module_name})"))?;
    Ok(())
}

/// Match `pg_wasm`'s `sanitize_sql_identifier` for export keys (e.g. `spin-param` → `spin_param`).
pub(crate) fn sanitize_export_sql_name(export_key: &str) -> String {
    let mut t = export_key.replace(['/', '-'], "_");
    if t.is_empty() {
        t = "export".to_string();
    }
    if !t
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
    {
        t = format!("e_{t}");
    }
    t
}

pub(crate) fn wasm_fn_ident(module_name: &str, export_sql_name: &str) -> String {
    let export = sanitize_export_sql_name(export_sql_name);
    format!("{}__{export}", module_name.replace('-', "_"))
}

pub(crate) async fn call_i32(
    client: &Client,
    module_name: &str,
    export_sql_name: &str,
) -> Result<i32> {
    let ident = wasm_fn_ident(module_name, export_sql_name);
    let sql = format!(r#"SELECT wasm."{}"()"#, ident.replace('"', "\"\""));
    let row = client.query_one(&sql, &[]).await?;
    let v: i32 = row.get(0);
    Ok(v)
}

pub(crate) async fn call_text(
    client: &Client,
    module_name: &str,
    export_sql_name: &str,
) -> Result<String> {
    let ident = wasm_fn_ident(module_name, export_sql_name);
    let sql = format!(r#"SELECT wasm."{}"()"#, ident.replace('"', "\"\""));
    let row = client.query_one(&sql, &[]).await?;
    let v: String = row.get(0);
    Ok(v)
}

pub(crate) fn itest_component_wasm() -> &'static [u8] {
    include_bytes!(concat!(env!("OUT_DIR"), "/itest.component.wasm"))
}

pub(crate) fn http_search_component_wasm() -> &'static [u8] {
    include_bytes!(concat!(env!("OUT_DIR"), "/http_search.component.wasm"))
}

/// Default `limits` object stored in the catalog for integration fixtures.
///
/// `serde_json` would otherwise serialize absent `Limits` fields as JSON `null`, which
/// `limits_from_value` rejects when hooks re-read catalog rows.
pub(crate) fn default_catalog_limits() -> serde_json::Value {
    json!({
        "fuel_per_invocation": 100_000_000_i64,
        "instances_per_module": 1_i64,
        "invocation_deadline_ms": 5_000_i64,
        "max_memory_pages": 1_024_i64
    })
}

pub(crate) fn load_options_with_limits_patch(patch: serde_json::Value) -> serde_json::Value {
    let mut limits = default_catalog_limits();
    if let Value::Object(ref mut lim_obj) = limits {
        if let Value::Object(patch_obj) = patch {
            for (k, v) in patch_obj {
                lim_obj.insert(k.clone(), v.clone());
            }
        }
    }
    json!({ "limits": limits })
}

pub(crate) fn itest_load_options() -> serde_json::Value {
    load_options_with_limits_patch(json!({}))
}
