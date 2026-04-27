//! Register WIT-derived PostgreSQL types from a `TypePlan` into the catalog.

use std::collections::{HashMap, VecDeque};

use pgrx::prelude::*;
use pgrx::spi::Spi;
use serde_json::{Value, json};

use super::typing::{PgType, TypePlan, TypePlanEntry};
use crate::catalog::wit_types::{self, NewWitType, WitTypeRow};
use crate::errors::{PgWasmError, Result};

use crate::catalog::EXTENSION_SCHEMA as CATALOG_SCHEMA;

/// Result of `register_type_plan`: maps each `type_key` to the registered PostgreSQL type OID.
#[derive(Clone, Debug, Default)]
pub(crate) struct RegisteredTypes {
    pub(crate) by_type_key: HashMap<String, pg_sys::Oid>,
}

/// Register every entry in `plan` in dependency order, idempotent when definitions match.
pub(crate) fn register_type_plan(
    plan: &TypePlan,
    module_id: u64,
    extension_oid: pg_sys::Oid,
) -> Result<RegisteredTypes> {
    if extension_oid == pg_sys::InvalidOid {
        return Err(PgWasmError::InvalidConfiguration(
            "extension_oid must be valid for UDT registration".to_string(),
        ));
    }

    let mid = i64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit i64".to_string()))?;

    let order = topo_sort(plan)?;
    let mut registered = RegisteredTypes::default();

    for index in order {
        let entry = plan
            .entries
            .get(index)
            .ok_or_else(|| PgWasmError::Internal("type plan index out of bounds".to_string()))?;

        let existing = find_row(mid, &entry.type_key)?;
        let desired_def = definition_value(&entry.pg_type);
        let desired_kind = type_kind(&entry.pg_type);

        if let Some(row) = existing {
            if row.definition == desired_def && row.kind == desired_kind {
                registered
                    .by_type_key
                    .insert(entry.type_key.clone(), row.pg_type_oid);
                continue;
            }

            let oid = transition_or_create(mid, extension_oid, entry, Some(&row), &desired_def)?;
            registered.by_type_key.insert(entry.type_key.clone(), oid);
        } else {
            let oid = transition_or_create(mid, extension_oid, entry, None, &desired_def)?;
            registered.by_type_key.insert(entry.type_key.clone(), oid);
        }
    }

    Ok(registered)
}

/// Drop catalog rows and issue `DROP TYPE` / `DROP DOMAIN` for each registered type.
pub(crate) fn unregister_module_types(module_id: u64, cascade: bool) -> Result<()> {
    let mid = i64::try_from(module_id)
        .map_err(|_| PgWasmError::Internal("module_id does not fit i64".to_string()))?;

    let rows = wit_types::list_by_module(mid)?;
    for row in rows {
        drop_type_oid(row.pg_type_oid, cascade)?;
        let _ = wit_types::delete(row.wit_type_id)?;
    }
    Ok(())
}

fn find_row(module_id: i64, type_key: &str) -> Result<Option<WitTypeRow>> {
    Ok(wit_types::list_by_module(module_id)?
        .into_iter()
        .find(|row| row.wit_name == type_key))
}

fn topo_sort(plan: &TypePlan) -> Result<Vec<usize>> {
    let key_to_index: HashMap<&str, usize> = plan
        .entries
        .iter()
        .enumerate()
        .map(|(i, e)| (e.type_key.as_str(), i))
        .collect();

    let mut indegree: Vec<usize> = vec![0; plan.entries.len()];
    let mut adj: Vec<Vec<usize>> = vec![vec![]; plan.entries.len()];

    for (i, entry) in plan.entries.iter().enumerate() {
        for dep in &entry.dependencies {
            let Some(&j) = key_to_index.get(dep.as_str()) else {
                return Err(PgWasmError::InvalidModule(format!(
                    "type plan dependency `{dep}` not found for {}",
                    entry.type_key
                )));
            };
            adj[j].push(i);
            indegree[i] += 1;
        }
    }

    let mut queue: VecDeque<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(i, d)| if *d == 0 { Some(i) } else { None })
        .collect();

    let mut out = Vec::new();
    while let Some(i) = queue.pop_front() {
        out.push(i);
        for &v in &adj[i] {
            indegree[v] -= 1;
            if indegree[v] == 0 {
                queue.push_back(v);
            }
        }
    }

    if out.len() != plan.entries.len() {
        return Err(PgWasmError::InvalidModule(
            "type plan has a cyclic dependency".to_string(),
        ));
    }

    Ok(out)
}

fn type_kind(pg: &PgType) -> String {
    match pg {
        PgType::Scalar(_) => "scalar".to_string(),
        PgType::Domain { .. } => "domain".to_string(),
        PgType::Array(_) => "array".to_string(),
        PgType::Composite(_) => "composite".to_string(),
        PgType::Enum(_) => "enum".to_string(),
        PgType::Variant(_) => "variant".to_string(),
    }
}

fn definition_value(pg: &PgType) -> Value {
    match pg {
        PgType::Scalar(s) => json!({ "kind": "scalar", "name": s }),
        PgType::Domain {
            base,
            check,
            flag_names,
            name,
        } => json!({
            "kind": "domain",
            "base": base,
            "check": check,
            "flag_names": flag_names,
            "name": name,
        }),
        PgType::Array(inner) => json!({
            "kind": "array",
            "inner": definition_value(inner),
        }),
        PgType::Composite(fields) => json!({
            "kind": "composite",
            "fields": fields
                .iter()
                .map(|f| json!({"name": f.name, "ty": definition_value(&f.ty)}))
                .collect::<Vec<_>>(),
        }),
        PgType::Enum(cases) => json!({ "kind": "enum", "cases": cases }),
        PgType::Variant(cases) => json!({
            "kind": "variant",
            "cases": cases.iter().map(|c| json!({
                "name": c.name,
                "payload": c.payload.as_ref().map(definition_value),
            })).collect::<Vec<_>>(),
        }),
    }
}

fn transition_or_create(
    module_id: i64,
    extension_oid: pg_sys::Oid,
    entry: &TypePlanEntry,
    existing: Option<&WitTypeRow>,
    desired_def: &Value,
) -> Result<pg_sys::Oid> {
    let desired_kind = type_kind(&entry.pg_type);

    match (&entry.pg_type, existing) {
        (PgType::Scalar(name), None) => {
            let oid = lookup_builtin_type_oid(name)?;
            insert_catalog_row(
                module_id,
                &entry.type_key,
                oid,
                &desired_kind,
                desired_def,
                extension_oid,
            )?;
            Ok(oid)
        }
        (PgType::Scalar(name), Some(row)) => {
            let oid = lookup_builtin_type_oid(name)?;
            if oid != row.pg_type_oid {
                return Err(PgWasmError::InvalidConfiguration(format!(
                    "scalar type `{}` OID mismatch; breaking change (hint: options.breaking_changes_allowed)",
                    entry.wit_name
                )));
            }
            wit_types::update(
                row.wit_type_id,
                &NewWitType {
                    definition: desired_def.clone(),
                    kind: desired_kind,
                    module_id,
                    pg_type_oid: oid,
                    wit_name: entry.type_key.clone(),
                },
            )?;
            Ok(oid)
        }
        (_, None) => {
            ensure_domains_recursive(module_id, &entry.pg_type)?;
            let fq = sql_qualified_type_name(module_id, &entry.wit_name, &entry.pg_type)?;
            let oid = if regtype_exists(&fq)? {
                // `ensure_domains_recursive` already ran `CREATE DOMAIN` for root `PgType::Domain`
                // (and nested domains). Do not run `build_create_sql` again or PostgreSQL errors with
                // "type ... already exists".
                lookup_regtype_oid(&fq)?
            } else {
                let sql = build_create_sql(module_id, entry)?;
                run_sql(&sql)?;
                lookup_regtype_oid(&fq)?
            };
            insert_catalog_row(
                module_id,
                &entry.type_key,
                oid,
                &desired_kind,
                desired_def,
                extension_oid,
            )?;
            Ok(oid)
        }
        (_, Some(row)) => {
            try_alter_type(row, entry, desired_def, &desired_kind)?;
            Ok(row.pg_type_oid)
        }
    }
}

fn try_alter_type(
    row: &WitTypeRow,
    entry: &TypePlanEntry,
    desired_def: &Value,
    desired_kind: &str,
) -> Result<()> {
    if row.kind != desired_kind {
        return Err(PgWasmError::InvalidConfiguration(format!(
            "type `{}` kind changed from {} to {}; breaking change (hint: options.breaking_changes_allowed)",
            entry.wit_name, row.kind, desired_kind
        )));
    }

    if row.definition == *desired_def {
        return Ok(());
    }

    match &entry.pg_type {
        PgType::Composite(new_fields) => {
            let old_fields = row
                .definition
                .get("fields")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    PgWasmError::Internal("stored composite definition missing fields".to_string())
                })?;

            if new_fields.len() < old_fields.len() {
                return Err(PgWasmError::InvalidConfiguration(format!(
                    "cannot remove fields from composite `{}`",
                    entry.wit_name
                )));
            }

            for (i, old) in old_fields.iter().enumerate() {
                let new_name = new_fields[i].name.as_str();
                let old_name = old.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if new_name != old_name {
                    return Err(PgWasmError::InvalidConfiguration(format!(
                        "cannot reorder/rename composite fields on `{}`",
                        entry.wit_name
                    )));
                }
                let old_ty = old.get("ty").cloned().unwrap_or(Value::Null);
                let new_ty = definition_value(&new_fields[i].ty);
                if old_ty != new_ty {
                    return Err(PgWasmError::InvalidConfiguration(format!(
                        "cannot change field type on `{}` field `{new_name}`",
                        entry.wit_name
                    )));
                }
            }

            let fq = fq_type_name(row.pg_type_oid)?;
            for extra in new_fields.iter().skip(old_fields.len()) {
                let pg_ty = pg_type_sql(module_id_from_row(row), &extra.ty)?;
                let sql = format!(
                    "ALTER TYPE {} ADD ATTRIBUTE {} {}",
                    fq,
                    quote_ident(&extra.name),
                    pg_ty
                );
                run_sql(&sql)?;
            }
        }
        PgType::Enum(new_cases) => {
            let old_cases: Vec<String> = row
                .definition
                .get("cases")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    PgWasmError::Internal("stored enum definition missing cases".to_string())
                })?
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();

            if new_cases.len() < old_cases.len() {
                return Err(PgWasmError::InvalidConfiguration(format!(
                    "cannot remove enum values from `{}`",
                    entry.wit_name
                )));
            }

            for (i, old) in old_cases.iter().enumerate() {
                if new_cases[i] != *old {
                    return Err(PgWasmError::InvalidConfiguration(format!(
                        "cannot reorder/rename enum values on `{}`",
                        entry.wit_name
                    )));
                }
            }

            let fq = fq_type_name(row.pg_type_oid)?;
            for case in new_cases.iter().skip(old_cases.len()) {
                let sql = format!(
                    "ALTER TYPE {} ADD VALUE IF NOT EXISTS {}",
                    fq,
                    quote_literal(case)
                );
                run_sql(&sql)?;
            }
        }
        _ => {
            return Err(PgWasmError::InvalidConfiguration(format!(
                "type `{}` definition changed in a way that is not supported by automatic ALTER (hint: options.breaking_changes_allowed)",
                entry.wit_name
            )));
        }
    }

    wit_types::update(
        row.wit_type_id,
        &NewWitType {
            definition: desired_def.clone(),
            kind: desired_kind.to_string(),
            module_id: row.module_id,
            pg_type_oid: row.pg_type_oid,
            wit_name: entry.type_key.clone(),
        },
    )?;

    Ok(())
}

fn module_id_from_row(row: &WitTypeRow) -> i64 {
    row.module_id
}

fn fq_type_name(oid: pg_sys::Oid) -> Result<String> {
    let sql = format!(
        "SELECT (n.nspname::text || '.' || quote_ident(t.typname::text))::text
         FROM pg_catalog.pg_type AS t
         JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
         WHERE t.oid = {}",
        oid.to_u32()
    );
    Spi::get_one::<String>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error resolving type name: {e}")))?
        .ok_or_else(|| PgWasmError::Internal("type oid not found".to_string()))
}

fn drop_type_oid(oid: pg_sys::Oid, cascade: bool) -> Result<()> {
    if !type_is_in_schema(oid, CATALOG_SCHEMA)? {
        return Ok(());
    }

    let fq = fq_type_name(oid)?;
    let cascade_sql = if cascade { " CASCADE" } else { "" };
    let typtype: String = Spi::get_one(&format!(
        "SELECT typtype::text FROM pg_catalog.pg_type WHERE oid = {}",
        oid.to_u32()
    ))
    .map_err(|e| PgWasmError::Internal(format!("SPI error reading typtype: {e}")))?
    .ok_or_else(|| PgWasmError::Internal("type oid missing for drop".to_string()))?;

    // Types registered via `recordDependencyOn(type, extension, DEPENDENCY_EXTENSION)` are
    // extension members; remove them from the extension before DROP or PostgreSQL rejects it.
    let sql = if typtype == "d" {
        format!("DROP DOMAIN IF EXISTS {fq}{cascade_sql}")
    } else {
        format!("DROP TYPE IF EXISTS {fq}{cascade_sql}")
    };
    run_sql(&sql)
}

fn insert_catalog_row(
    module_id: i64,
    type_key: &str,
    pg_type_oid: pg_sys::Oid,
    kind: &str,
    definition: &Value,
    extension_oid: pg_sys::Oid,
) -> Result<()> {
    let row = wit_types::insert(&NewWitType {
        definition: definition.clone(),
        kind: kind.to_string(),
        module_id,
        pg_type_oid,
        wit_name: type_key.to_string(),
    })?;

    // TODO(wave-5 unload-orchestration): call `recordDependencyOn(DEPENDENCY_EXTENSION)` for
    // wasm-schema types once we can pair each composite/enum with its implicit `T[]` array in
    // the extension catalog (plain `ALTER EXTENSION ... DROP TYPE` rejects arrays that are not
    // extension members).

    let _ = row;
    let _ = extension_oid;
    Ok(())
}

fn type_is_in_schema(type_oid: pg_sys::Oid, schema: &str) -> Result<bool> {
    if type_oid == pg_sys::InvalidOid {
        return Ok(false);
    }
    let schema_esc = schema.replace('\'', "''");
    let sql = format!(
        "SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_type AS t
            JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
            WHERE t.oid = {} AND n.nspname::text = '{}'
        )",
        type_oid.to_u32(),
        schema_esc
    );
    Spi::get_one::<bool>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error checking type schema: {e}")))?
        .ok_or_else(|| PgWasmError::Internal("schema check query returned NULL".to_string()))
}

fn lookup_builtin_type_oid(pg_name: &str) -> Result<pg_sys::Oid> {
    let escaped = pg_name.replace('\'', "''");
    let sql = format!("SELECT ('{escaped}'::regtype)::oid");
    Spi::get_one::<pg_sys::Oid>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error looking up type `{pg_name}`: {e}")))?
        .ok_or_else(|| PgWasmError::Internal(format!("unknown built-in type `{pg_name}`")))
}

fn lookup_regtype_oid(expr: &str) -> Result<pg_sys::Oid> {
    let escaped = expr.replace('\'', "''");
    let sql = format!("SELECT ('{escaped}'::regtype)::oid");
    Spi::get_one::<pg_sys::Oid>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error resolving regtype `{expr}`: {e}")))?
        .ok_or_else(|| PgWasmError::Internal(format!("failed to resolve regtype `{expr}`")))
}

fn run_sql(sql: &str) -> Result<()> {
    Spi::run(sql).map_err(|e| PgWasmError::Internal(format!("SPI error running `{sql}`: {e}")))
}

fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn quote_literal(s: &str) -> String {
    let escaped = s.replace('\'', "''");
    format!("'{escaped}'")
}

fn sql_qualified_type_name(module_id: i64, wit_name: &str, pg: &PgType) -> Result<String> {
    let base = type_sql_ident(module_id, wit_name, pg);
    Ok(format!("{CATALOG_SCHEMA}.{}", quote_ident(&base)))
}

fn type_sql_ident(module_id: i64, wit_name: &str, pg: &PgType) -> String {
    let suffix = match pg {
        PgType::Domain { name: Some(n), .. } => sanitize_sql_ident(n),
        _ => sanitize_sql_ident(wit_name),
    };
    format!("m{module_id}_{suffix}")
}

fn sanitize_sql_ident(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() { "t".to_string() } else { out }
}

fn ensure_domains_recursive(module_id: i64, pg: &PgType) -> Result<()> {
    match pg {
        PgType::Domain { .. } => {
            let fq = sql_qualified_type_name(module_id, "", pg)?;
            if regtype_exists(&fq)? {
                return Ok(());
            }
            let sql = build_create_domain_sql(module_id, pg)?;
            run_sql(&sql)?;
        }
        PgType::Composite(fields) => {
            for f in fields {
                ensure_domains_recursive(module_id, &f.ty)?;
            }
        }
        PgType::Variant(cases) => {
            for c in cases {
                if let Some(p) = &c.payload {
                    ensure_domains_recursive(module_id, p)?;
                }
            }
        }
        PgType::Array(inner) => ensure_domains_recursive(module_id, inner)?,
        PgType::Scalar(_) | PgType::Enum(_) => {}
    }
    Ok(())
}

fn regtype_exists(fq: &str) -> Result<bool> {
    let escaped = fq.replace('\'', "''");
    let sql = format!("SELECT to_regtype('{escaped}') IS NOT NULL");
    Spi::get_one::<bool>(&sql)
        .map_err(|e| PgWasmError::Internal(format!("SPI error checking regtype: {e}")))?
        .ok_or_else(|| PgWasmError::Internal("missing bool from regtype exists".to_string()))
}

fn build_create_domain_sql(module_id: i64, pg: &PgType) -> Result<String> {
    let PgType::Domain {
        base,
        check,
        flag_names,
        name: _,
    } = pg
    else {
        return Err(PgWasmError::Internal(
            "build_create_domain_sql expects domain".to_string(),
        ));
    };
    let fq = sql_qualified_type_name(module_id, "", pg)?;
    let mut s = format!("CREATE DOMAIN {fq} AS {base}");
    if let Some(flags) = flag_names {
        let max = (1_u64 << flags.len()).saturating_sub(1);
        s.push_str(&format!(" CHECK (VALUE >= 0 AND VALUE <= {max})"));
    } else if let Some(c) = check {
        s.push_str(&format!(" CHECK ({c})"));
    }
    Ok(s)
}

fn build_create_sql(module_id: i64, entry: &TypePlanEntry) -> Result<String> {
    let fq = sql_qualified_type_name(module_id, &entry.wit_name, &entry.pg_type)?;
    let sql = match &entry.pg_type {
        PgType::Scalar(_) => {
            return Err(PgWasmError::Internal(
                "build_create_sql called for scalar".to_string(),
            ));
        }
        PgType::Domain { .. } => build_create_domain_sql(module_id, &entry.pg_type)?,
        PgType::Composite(fields) => {
            let mut parts = Vec::new();
            for f in fields {
                parts.push(format!(
                    "{} {}",
                    quote_ident(&f.name),
                    pg_type_sql(module_id, &f.ty)?
                ));
            }
            format!("CREATE TYPE {fq} AS ({})", parts.join(", "))
        }
        PgType::Enum(cases) => {
            let labels = cases
                .iter()
                .map(|c| quote_literal(c))
                .collect::<Vec<_>>()
                .join(", ");
            format!("CREATE TYPE {fq} AS ENUM ({labels})")
        }
        PgType::Variant(_cases) => {
            // Only `discriminant` + `payload jsonb`: PostgreSQL rejects `void` in composite columns,
            // and `mapping::composite::val_to_datum` for variants supplies exactly two attributes.
            format!("CREATE TYPE {fq} AS (discriminant text, payload jsonb)")
        }
        PgType::Array(inner) => {
            let elem = pg_type_sql(module_id, inner)?;
            format!("CREATE DOMAIN {fq} AS {elem}[] NOT NULL")
        }
    };
    Ok(sql)
}

fn pg_type_sql(module_id: i64, pg: &PgType) -> Result<String> {
    Ok(match pg {
        PgType::Scalar(s) => (*s).to_string(),
        PgType::Domain {
            base: _,
            check: _,
            flag_names: _,
            name,
        } => {
            let wit_part = name
                .as_deref()
                .map(sanitize_sql_ident)
                .unwrap_or_else(|| "domain".to_string());
            format!(
                "{CATALOG_SCHEMA}.{}",
                quote_ident(&format!("m{module_id}_{wit_part}"))
            )
        }
        PgType::Composite(_) | PgType::Enum(_) | PgType::Variant(_) => {
            return Err(PgWasmError::Unsupported(
                "nested user-defined composite/enum/variant field types require load-orchestration wiring"
                    .to_string(),
            ));
        }
        PgType::Array(inner) => {
            let inner_sql = pg_type_sql(module_id, inner)?;
            format!("{inner_sql}[]")
        }
    })
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use pgrx::pg_sys::AsPgCStr;
    use pgrx::prelude::*;
    use pgrx::spi::Spi;
    use serde_json::json;
    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};

    use super::*;
    use crate::catalog::modules;
    use crate::wit::typing::{self, CompositeField};
    use crate::wit::world;

    fn extension_oid() -> pg_sys::Oid {
        unsafe { pg_sys::get_extension_oid("pgwasm".as_pg_cstr(), false) }
    }

    static STUB_COUNTER: AtomicU64 = AtomicU64::new(1);

    fn insert_stub_module() -> i64 {
        let n = STUB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let row = modules::insert(&modules::NewModule {
            abi: "core".to_string(),
            artifact_path: "/dev/null".to_string(),
            digest: vec![0u8; 32],
            generation: 0,
            limits: json!({}),
            name: format!("udt_stub_{n}"),
            origin: "test".to_string(),
            policy: json!({}),
            wasm_sha256: vec![0u8; 32],
            wit_world: "{}".to_string(),
        })
        .expect("stub module insert");
        row.module_id
    }

    fn fixture_component_bytes(wit_source: &str, world_name: &str) -> Vec<u8> {
        let mut module = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        let mut resolve = wit_parser::Resolve::default();
        let pkg = resolve
            .push_str("fixture.wit", wit_source)
            .expect("fixture wit parses");
        let world_id = resolve
            .select_world(&[pkg], Some(world_name))
            .expect("world exists");
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8)
            .expect("embed metadata");
        ComponentEncoder::default()
            .module(&module)
            .expect("module encodes")
            .validate(true)
            .encode()
            .expect("component encodes")
    }

    fn fixture_plan() -> TypePlan {
        let wit = r#"
            package test:udt;

            interface api {
                record person {
                    id: u32,
                    name: string,
                }

                enum color {
                    red,
                    green,
                }
            }

            world udt-world {
                export api;
            }
        "#;
        let decoded = world::decode(&fixture_component_bytes(wit, "udt-world"))
            .expect("fixture should decode");
        typing::plan_types("demo", &decoded).expect("plan should build")
    }

    #[pg_test]
    fn register_plan_creates_rows_and_types() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        let mid = insert_stub_module();
        let mid_u = u64::try_from(mid).unwrap();
        let plan = fixture_plan();
        let ext_oid = extension_oid();
        let registered = register_type_plan(&plan, mid_u, ext_oid).expect("register");

        assert!(registered.by_type_key.len() >= 2);

        let count: i64 = Spi::get_one(&format!(
            "SELECT count(*)::bigint FROM {CATALOG_SCHEMA}.wit_types WHERE module_id = {mid}"
        ))
        .unwrap()
        .unwrap();
        assert!(count >= 2);

        let person_oid = *registered
            .by_type_key
            .iter()
            .find(|(k, _)| k.contains("person"))
            .map(|(_, o)| o)
            .expect("person type");
        let attname: String = Spi::get_one(&format!(
            "SELECT a.attname::text
             FROM pg_attribute a
             WHERE a.attrelid = (SELECT typrelid FROM pg_catalog.pg_type WHERE oid = {person_oid})
               AND a.attnum > 0
               AND NOT a.attisdropped
             ORDER BY a.attnum
             LIMIT 1"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(attname, "id");

        let _ = modules::delete(mid);
    }

    #[pg_test]
    fn register_twice_is_idempotent() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        let mid = insert_stub_module();
        let plan = fixture_plan();
        let ext_oid = extension_oid();
        let mid_u = u64::try_from(mid).unwrap();
        let first = register_type_plan(&plan, mid_u, ext_oid).expect("first");
        let second = register_type_plan(&plan, mid_u, ext_oid).expect("second");

        for (k, oid) in &first.by_type_key {
            assert_eq!(second.by_type_key.get(k), Some(oid));
        }

        let _ = modules::delete(mid);
    }

    #[pg_test]
    fn alter_composite_adds_field() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        let mid = insert_stub_module();
        let ext_oid = extension_oid();
        let mid_u = u64::try_from(mid).unwrap();

        let v1 = TypePlan {
            entries: vec![TypePlanEntry {
                dependencies: vec![],
                pg_type: PgType::Composite(vec![CompositeField {
                    name: "a".to_string(),
                    ty: PgType::Scalar("int4"),
                }]),
                type_key: "k:demo/t".to_string(),
                wit_name: "t".to_string(),
            }],
        };
        register_type_plan(&v1, mid_u, ext_oid).unwrap();

        let v2 = TypePlan {
            entries: vec![TypePlanEntry {
                dependencies: vec![],
                pg_type: PgType::Composite(vec![
                    CompositeField {
                        name: "a".to_string(),
                        ty: PgType::Scalar("int4"),
                    },
                    CompositeField {
                        name: "b".to_string(),
                        ty: PgType::Scalar("text"),
                    },
                ]),
                type_key: "k:demo/t".to_string(),
                wit_name: "t".to_string(),
            }],
        };
        let reg = register_type_plan(&v2, mid_u, ext_oid).unwrap();
        let oid = *reg.by_type_key.get("k:demo/t").unwrap();
        let ncols: i64 = Spi::get_one(&format!(
            "SELECT count(*)::bigint FROM pg_attribute
             WHERE attrelid = (SELECT typrelid FROM pg_catalog.pg_type WHERE oid = {oid})
               AND attnum > 0 AND NOT attisdropped"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(ncols, 2);

        let _ = modules::delete(mid);
    }

    #[pg_test]
    fn breaking_change_errors() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        let mid = insert_stub_module();
        let ext_oid = extension_oid();
        let mid_u = u64::try_from(mid).unwrap();
        let v1 = TypePlan {
            entries: vec![TypePlanEntry {
                dependencies: vec![],
                pg_type: PgType::Enum(vec!["a".to_string(), "b".to_string()]),
                type_key: "k:demo/e".to_string(),
                wit_name: "e".to_string(),
            }],
        };
        register_type_plan(&v1, mid_u, ext_oid).unwrap();

        let v2 = TypePlan {
            entries: vec![TypePlanEntry {
                dependencies: vec![],
                pg_type: PgType::Enum(vec!["b".to_string(), "a".to_string()]),
                type_key: "k:demo/e".to_string(),
                wit_name: "e".to_string(),
            }],
        };
        let err = register_type_plan(&v2, mid_u, ext_oid).unwrap_err();
        assert!(matches!(err, PgWasmError::InvalidConfiguration(_)));

        let _ = modules::delete(mid);
    }

    #[pg_test]
    fn unregister_drops_types() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pgwasm").unwrap();
        let mid = insert_stub_module();
        let mid_u = u64::try_from(mid).unwrap();
        let plan = fixture_plan();
        let ext_oid = extension_oid();
        register_type_plan(&plan, mid_u, ext_oid).unwrap();
        unregister_module_types(mid_u, true).unwrap();

        let count: i64 = Spi::get_one(&format!(
            "SELECT count(*)::bigint FROM {CATALOG_SCHEMA}.wit_types WHERE module_id = {mid}"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);

        let _ = modules::delete(mid);
    }
}
