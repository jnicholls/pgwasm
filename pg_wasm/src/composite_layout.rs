//! Validate PostgreSQL composite types against WIT-derived [`MarshalType`] (Track A).

use pgrx::pg_sys::Oid;
use pgrx::spi::Spi;

use crate::mapping::{MarshalType, pg_type_oid_is_composite};

/// `record` / `tuple` may use SQL `composite` encoding instead of `jsonb`.
pub fn marshal_type_uses_composite_surface(mt: &MarshalType) -> bool {
    matches!(mt, MarshalType::Record(_) | MarshalType::Tuple(_))
}

/// Load composite attribute names and type OIDs in column order (`attnum`).
fn composite_attributes_ordered(typoid: Oid) -> Result<Vec<(String, Oid)>, String> {
    let sql = format!(
        "SELECT string_agg(a.attname || E'\\x1f' || a.atttypid::oid::text, E'\\x1e' ORDER BY a.attnum) \
         FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_type t ON t.oid = {} \
         WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped",
        u32::from(typoid)
    );
    let packed: Option<String> =
        Spi::get_one(&sql).map_err(|e| format!("pg_wasm: composite attribute query: {e}"))?;
    let Some(packed) = packed else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for part in packed.split('\x1e') {
        if part.is_empty() {
            continue;
        }
        let mut it = part.splitn(2, '\x1f');
        let name = it
            .next()
            .ok_or_else(|| "pg_wasm: malformed composite attribute row".to_string())?;
        let oid_s = it
            .next()
            .ok_or_else(|| "pg_wasm: malformed composite attribute row".to_string())?;
        let atttypid: u32 = oid_s
            .parse()
            .map_err(|_| format!("pg_wasm: invalid atttypid {oid_s:?}"))?;
        out.push((name.to_string(), Oid::from(atttypid)));
    }
    Ok(out)
}

fn attr_name_matches_wit(att: &str, wit: &str) -> bool {
    att.eq_ignore_ascii_case(wit)
}

fn validate_leaf_typoid(mt: &MarshalType, atttypid: Oid) -> Result<(), String> {
    let Some(expected) = crate::mapping::marshal_leaf_expected_pg_typoid(mt) else {
        return Err(format!(
            "pg_wasm: WIT field type {mt:?} cannot be used as a composite attribute (expected nested record/tuple)"
        ));
    };
    if atttypid != expected {
        return Err(format!(
            "pg_wasm: composite attribute type oid {} does not match WIT field {:?} (expected oid {})",
            u32::from(atttypid),
            mt,
            u32::from(expected)
        ));
    }
    Ok(())
}

fn validate_nested_or_leaf(mt: &MarshalType, atttypid: Oid) -> Result<(), String> {
    match mt {
        MarshalType::Record(_) | MarshalType::Tuple(_) => {
            if !pg_type_oid_is_composite(atttypid)? {
                return Err(format!(
                    "pg_wasm: WIT nested aggregate {:?} requires a composite PostgreSQL attribute (got oid {})",
                    mt,
                    u32::from(atttypid)
                ));
            }
            validate_composite_typoid_matches_marshal(atttypid, mt)
        }
        _ => validate_leaf_typoid(mt, atttypid),
    }
}

/// Ensure `typoid` is a composite type whose attributes match `mt` (`record` or `tuple` only).
pub fn validate_composite_typoid_matches_marshal(
    typoid: Oid,
    mt: &MarshalType,
) -> Result<(), String> {
    if !pg_type_oid_is_composite(typoid)? {
        return Err(format!(
            "pg_wasm: oid {} is not a composite type",
            u32::from(typoid)
        ));
    }
    let attrs = composite_attributes_ordered(typoid)?;
    match mt {
        MarshalType::Record(fields) => {
            if attrs.len() != fields.len() {
                return Err(format!(
                    "pg_wasm: composite type has {} attributes but WIT record has {} fields",
                    attrs.len(),
                    fields.len()
                ));
            }
            for ((wit_name, fty), (att_name, atttypid)) in fields.iter().zip(&attrs) {
                if !attr_name_matches_wit(att_name, wit_name) {
                    return Err(format!(
                        "pg_wasm: composite attribute {att_name:?} does not match WIT record field {wit_name:?}"
                    ));
                }
                validate_nested_or_leaf(fty, *atttypid)?;
            }
            Ok(())
        }
        MarshalType::Tuple(elems) => {
            if attrs.len() != elems.len() {
                return Err(format!(
                    "pg_wasm: composite type has {} attributes but WIT tuple has {} elements",
                    attrs.len(),
                    elems.len()
                ));
            }
            for ((_att_name, atttypid), elem_mt) in attrs.iter().zip(elems) {
                validate_nested_or_leaf(elem_mt, *atttypid)?;
            }
            Ok(())
        }
        _ => {
            Err("pg_wasm: composite SQL encoding applies only to WIT record and tuple types".into())
        }
    }
}
