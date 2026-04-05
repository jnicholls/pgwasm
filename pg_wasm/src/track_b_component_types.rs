//! Track B: `SPI CREATE TYPE` for WIT `record` / `tuple` when `pg_wasm.auto_create_component_types` is on.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use pgrx::pg_sys;
use pgrx::spi::Spi;

use crate::composite_layout;
use crate::mapping::{
    ExportSignature, MarshalType, PgWasmArgDesc, PgWasmReturnDesc, PgWasmTypeKind,
    resolve_regtype_oid,
};
use crate::proc_reg;
use crate::registry::ModuleId;

fn shape_key(mt: &MarshalType) -> String {
    format!("{mt:?}")
}

fn stable_type_name(module_id: i64, mt: &MarshalType) -> String {
    let key = shape_key(mt);
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    let hx = h.finish();
    let s = format!("wct_m{module_id}_{hx:016x}");
    s.chars().take(63).collect()
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn marshal_leaf_sql_ddl(mt: &MarshalType) -> Result<&'static str, String> {
    match mt {
        MarshalType::Bool => Ok("boolean"),
        MarshalType::S8
        | MarshalType::U8
        | MarshalType::S16
        | MarshalType::U16
        | MarshalType::S32
        | MarshalType::U32 => Ok("integer"),
        MarshalType::S64 | MarshalType::U64 => Ok("bigint"),
        MarshalType::F32 => Ok("real"),
        MarshalType::F64 => Ok("double precision"),
        MarshalType::Char | MarshalType::String => Ok("text"),
        _ => Err(format!(
            "pg_wasm: Track B auto composite does not support WIT field shape {mt:?} inside a record/tuple (use jsonb or Track A)"
        )),
    }
}

fn sql_type_for_field(
    module_id: ModuleId,
    schema: &str,
    mt: &MarshalType,
    cache: &mut HashMap<String, String>,
    created: &mut Vec<(String, String)>,
) -> Result<String, String> {
    if composite_layout::marshal_type_uses_composite_surface(mt) {
        let fq_plain = ensure_composite_type(module_id, schema, mt, cache, created)?;
        let (sch, tn) = fq_plain.rsplit_once('.').ok_or_else(|| {
            format!("pg_wasm: internal error: malformed qualified type {fq_plain}")
        })?;
        return Ok(format!("{}.{}", quote_ident(sch), quote_ident(tn)));
    }
    if matches!(mt, MarshalType::List(_)) {
        return Err(
            "pg_wasm: Track B auto composite: list fields inside record/tuple are not supported"
                .into(),
        );
    }
    Ok(marshal_leaf_sql_ddl(mt)?.to_string())
}

/// Returns `schema.typename` for use with [`resolve_regtype_oid`].
fn ensure_composite_type(
    module_id: ModuleId,
    schema: &str,
    mt: &MarshalType,
    cache: &mut HashMap<String, String>,
    created: &mut Vec<(String, String)>,
) -> Result<String, String> {
    let key = shape_key(mt);
    if let Some(existing) = cache.get(&key).cloned() {
        return Ok(existing);
    }

    let tname = stable_type_name(module_id.0, mt);
    proc_reg::assert_sql_identifier(&tname).map_err(|e| e.to_string())?;

    let body = match mt {
        MarshalType::Record(fields) => {
            let mut cols = Vec::with_capacity(fields.len());
            for (fname, fty) in fields {
                proc_reg::assert_sql_identifier(fname).map_err(|e| e.to_string())?;
                let sql_ty = sql_type_for_field(module_id, schema, fty, cache, created)?;
                cols.push(format!("{} {}", quote_ident(fname), sql_ty));
            }
            cols.join(", ")
        }
        MarshalType::Tuple(elems) => {
            let mut cols = Vec::with_capacity(elems.len());
            for (i, fty) in elems.iter().enumerate() {
                let fname = format!("f{}", i + 1);
                let sql_ty = sql_type_for_field(module_id, schema, fty, cache, created)?;
                cols.push(format!("{} {}", quote_ident(&fname), sql_ty));
            }
            cols.join(", ")
        }
        _ => {
            return Err(
                "pg_wasm: internal error: ensure_composite_type on non-aggregate marshal type"
                    .into(),
            );
        }
    };

    let fq_plain = format!("{schema}.{tname}");
    let ddl = format!(
        "CREATE TYPE {}.{} AS ({})",
        quote_ident(schema),
        quote_ident(&tname),
        body
    );
    Spi::run(&ddl).map_err(|e| format!("pg_wasm: Track B CREATE TYPE {fq_plain}: {e}"))?;
    created.push((schema.to_string(), tname));

    let _verify = resolve_regtype_oid(&fq_plain)?;
    cache.insert(key, fq_plain.clone());
    Ok(fq_plain)
}

fn drop_created_types(created: &[(String, String)]) {
    for (schema, tname) in created.iter().rev() {
        let sql = format!(
            "DROP TYPE IF EXISTS {}.{} CASCADE",
            quote_ident(schema),
            quote_ident(tname)
        );
        let _ = Spi::run(&sql);
    }
}

fn rewrite_one_signature(
    module_id: ModuleId,
    ext_schema: &str,
    sig: &mut ExportSignature,
    cache: &mut HashMap<String, String>,
    created: &mut Vec<(String, String)>,
) -> Result<(), String> {
    let Some(plan) = sig.component_dynamic_plan.as_ref() else {
        return Ok(());
    };

    if plan.params.len() != sig.args.len() {
        return Err("pg_wasm: Track B: marshal plan vs signature arg length mismatch".into());
    }

    for (i, mt) in plan.params.iter().enumerate() {
        if !composite_layout::marshal_type_uses_composite_surface(mt) {
            continue;
        }
        if sig.args[i].kind != PgWasmTypeKind::Bytes || sig.args[i].pg_oid != pg_sys::JSONBOID {
            continue;
        }
        let reg = ensure_composite_type(module_id, ext_schema, mt, cache, created)?;
        let oid = resolve_regtype_oid(&reg)?;
        sig.args[i] = PgWasmArgDesc {
            pg_oid: oid,
            kind: PgWasmTypeKind::Composite,
        };
    }

    let ret_mt = &plan.result;
    if composite_layout::marshal_type_uses_composite_surface(ret_mt)
        && sig.ret.kind == PgWasmTypeKind::Bytes
        && sig.ret.pg_oid == pg_sys::JSONBOID
    {
        let reg = ensure_composite_type(module_id, ext_schema, ret_mt, cache, created)?;
        let oid = resolve_regtype_oid(&reg)?;
        sig.ret = PgWasmReturnDesc {
            pg_oid: oid,
            kind: PgWasmTypeKind::Composite,
        };
    }

    Ok(())
}

/// When the GUC is enabled, replace default `jsonb` record/tuple slots with auto-generated composites.
pub fn rewrite_signatures_with_auto_composites(
    module_id: ModuleId,
    ext_schema: &str,
    exports: &mut Vec<(String, ExportSignature)>,
) -> Result<(), String> {
    let mut cache = HashMap::<String, String>::new();
    let mut created: Vec<(String, String)> = Vec::new();

    let res = (|| {
        for (_name, sig) in exports.iter_mut() {
            rewrite_one_signature(module_id, ext_schema, sig, &mut cache, &mut created)?;
        }
        Ok::<(), String>(())
    })();

    if res.is_err() {
        drop_created_types(&created);
        return res;
    }

    crate::registry::record_module_track_b_types(module_id, created);
    res
}
