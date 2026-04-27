use std::collections::HashSet;

use wit_parser::{Type, TypeDefKind, TypeOwner, WorldId, WorldItem};

use super::world::DecodedWorld;
use crate::errors::PgWasmError;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TypePlan {
    pub(crate) entries: Vec<TypePlanEntry>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TypePlanEntry {
    pub(crate) dependencies: Vec<String>,
    pub(crate) pg_type: PgType,
    pub(crate) type_key: String,
    pub(crate) wit_name: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum PgType {
    Scalar(&'static str),
    Domain {
        base: &'static str,
        check: Option<&'static str>,
        /// When this domain represents WIT `flags`, bit order matches this list (index 0 = LSB).
        flag_names: Option<Vec<String>>,
        name: Option<String>,
    },
    Array(Box<PgType>),
    Composite(Vec<CompositeField>),
    Enum(Vec<String>),
    Variant(Vec<VariantCasePlan>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CompositeField {
    pub(crate) name: String,
    pub(crate) ty: PgType,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct VariantCasePlan {
    pub(crate) name: String,
    pub(crate) payload: Option<PgType>,
}

/// Stable WIT type key for [`crate::catalog::wit_types`] rows (`wit_name` column), matching
/// [`TypePlanEntry::type_key`].
pub(crate) fn export_type_key_for_id(
    resolve: &wit_parser::Resolve,
    type_id: wit_parser::TypeId,
) -> Result<String, PgWasmError> {
    let typedef = resolve.types.get(type_id).ok_or_else(|| {
        PgWasmError::InvalidModule(format!("type id {type_id:?} was not present in resolve"))
    })?;
    let wit_name = type_name(typedef, type_id);
    Ok(build_type_key(resolve, type_id, wit_name.as_str()))
}

pub(crate) fn plan_types(
    module_prefix: &str,
    decoded: &DecodedWorld,
) -> Result<TypePlan, PgWasmError> {
    let mut reachable = Vec::new();
    let mut seen = HashSet::new();
    let mut visited_worlds = HashSet::new();

    collect_world_reachable_type_ids(
        decoded.world_id,
        &decoded.resolve,
        &mut visited_worlds,
        &mut seen,
        &mut reachable,
    )?;

    let mut visiting = HashSet::new();
    let mut planned = HashSet::new();
    let mut entries = Vec::new();

    for type_id in reachable {
        plan_type_id(
            type_id,
            module_prefix,
            &decoded.resolve,
            &mut visiting,
            &mut planned,
            &mut entries,
        )?;
    }

    Ok(TypePlan { entries })
}

fn collect_world_reachable_type_ids(
    world_id: WorldId,
    resolve: &wit_parser::Resolve,
    visited_worlds: &mut HashSet<WorldId>,
    seen: &mut HashSet<wit_parser::TypeId>,
    out: &mut Vec<wit_parser::TypeId>,
) -> Result<(), PgWasmError> {
    if !visited_worlds.insert(world_id) {
        return Ok(());
    }

    let world = resolve.worlds.get(world_id).ok_or_else(|| {
        PgWasmError::InvalidModule(format!(
            "world id {:?} was not present in resolve",
            world_id
        ))
    })?;

    for include in &world.includes {
        collect_world_reachable_type_ids(include.id, resolve, visited_worlds, seen, out)?;
    }

    for item in world.imports.values().chain(world.exports.values()) {
        match item {
            WorldItem::Type { id, .. } => {
                collect_type_ids(*id, resolve, seen, out)?;
            }
            WorldItem::Interface { id, .. } => {
                let interface = resolve.interfaces.get(*id).ok_or_else(|| {
                    PgWasmError::InvalidModule(format!(
                        "interface id {:?} was not present in resolve",
                        id
                    ))
                })?;
                for type_id in interface.types.values() {
                    collect_type_ids(*type_id, resolve, seen, out)?;
                }
            }
            WorldItem::Function(_) => {}
        }
    }

    Ok(())
}

fn collect_type_ids(
    type_id: wit_parser::TypeId,
    resolve: &wit_parser::Resolve,
    seen: &mut HashSet<wit_parser::TypeId>,
    out: &mut Vec<wit_parser::TypeId>,
) -> Result<(), PgWasmError> {
    if !seen.insert(type_id) {
        return Ok(());
    }

    let typedef = resolve.types.get(type_id).ok_or_else(|| {
        PgWasmError::InvalidModule(format!("type id {:?} was not present in resolve", type_id))
    })?;

    collect_nested_typedef_dependencies(&typedef.kind, resolve, seen, out)?;
    out.push(type_id);
    Ok(())
}

fn collect_nested_type_dependencies(
    ty: Type,
    resolve: &wit_parser::Resolve,
    seen: &mut HashSet<wit_parser::TypeId>,
    out: &mut Vec<wit_parser::TypeId>,
) -> Result<(), PgWasmError> {
    if let Type::Id(type_id) = ty {
        collect_type_ids(type_id, resolve, seen, out)?;
    }
    Ok(())
}

fn collect_nested_typedef_dependencies(
    kind: &TypeDefKind,
    resolve: &wit_parser::Resolve,
    seen: &mut HashSet<wit_parser::TypeId>,
    out: &mut Vec<wit_parser::TypeId>,
) -> Result<(), PgWasmError> {
    match kind {
        TypeDefKind::Record(record) => {
            for field in &record.fields {
                collect_nested_type_dependencies(field.ty, resolve, seen, out)?;
            }
        }
        TypeDefKind::Handle(handle) => match handle {
            wit_parser::Handle::Borrow(type_id) | wit_parser::Handle::Own(type_id) => {
                collect_type_ids(*type_id, resolve, seen, out)?;
            }
        },
        TypeDefKind::Tuple(tuple) => {
            for ty in &tuple.types {
                collect_nested_type_dependencies(*ty, resolve, seen, out)?;
            }
        }
        TypeDefKind::Variant(variant) => {
            for case in &variant.cases {
                if let Some(case_ty) = case.ty {
                    collect_nested_type_dependencies(case_ty, resolve, seen, out)?;
                }
            }
        }
        TypeDefKind::Option(ty)
        | TypeDefKind::List(ty)
        | TypeDefKind::Type(ty)
        | TypeDefKind::Future(Some(ty))
        | TypeDefKind::Stream(Some(ty)) => {
            collect_nested_type_dependencies(*ty, resolve, seen, out)?;
        }
        TypeDefKind::Result(result) => {
            if let Some(ok) = result.ok {
                collect_nested_type_dependencies(ok, resolve, seen, out)?;
            }
            if let Some(err) = result.err {
                collect_nested_type_dependencies(err, resolve, seen, out)?;
            }
        }
        TypeDefKind::Map(key, value) => {
            collect_nested_type_dependencies(*key, resolve, seen, out)?;
            collect_nested_type_dependencies(*value, resolve, seen, out)?;
        }
        TypeDefKind::FixedLengthList(ty, _) => {
            collect_nested_type_dependencies(*ty, resolve, seen, out)?;
        }
        TypeDefKind::Resource
        | TypeDefKind::Flags(_)
        | TypeDefKind::Enum(_)
        | TypeDefKind::Unknown
        | TypeDefKind::Future(None)
        | TypeDefKind::Stream(None) => {}
    }

    Ok(())
}

fn plan_type_id(
    type_id: wit_parser::TypeId,
    module_prefix: &str,
    resolve: &wit_parser::Resolve,
    visiting: &mut HashSet<wit_parser::TypeId>,
    planned: &mut HashSet<wit_parser::TypeId>,
    entries: &mut Vec<TypePlanEntry>,
) -> Result<(), PgWasmError> {
    if planned.contains(&type_id) {
        return Ok(());
    }

    if !visiting.insert(type_id) {
        return Err(PgWasmError::InvalidModule(format!(
            "recursive or mutually recursive WIT type involving {type_id:?}"
        )));
    }

    let typedef = resolve.types.get(type_id).ok_or_else(|| {
        PgWasmError::InvalidModule(format!("type id {:?} was not present in resolve", type_id))
    })?;

    let mut dependency_ids = Vec::new();
    collect_direct_dependencies(&typedef.kind, &mut dependency_ids);
    for dependency in &dependency_ids {
        plan_type_id(
            *dependency,
            module_prefix,
            resolve,
            visiting,
            planned,
            entries,
        )?;
    }

    let pg_type = wit_to_pg(module_prefix, resolve, Type::Id(type_id))?;
    let wit_name = typedef
        .name
        .clone()
        .unwrap_or_else(|| format!("anonymous_{:?}", type_id));
    let type_key = build_type_key(resolve, type_id, &wit_name);
    let dependencies = dependency_ids
        .iter()
        .map(|dependency_id| {
            let dep_def = resolve.types.get(*dependency_id).ok_or_else(|| {
                PgWasmError::InvalidModule(format!(
                    "type dependency {:?} missing from resolve",
                    dependency_id
                ))
            })?;
            let dep_name = dep_def
                .name
                .clone()
                .unwrap_or_else(|| format!("anonymous_{:?}", dependency_id));
            Ok(build_type_key(resolve, *dependency_id, dep_name.as_str()))
        })
        .collect::<Result<Vec<_>, PgWasmError>>()?;

    entries.push(TypePlanEntry {
        dependencies,
        pg_type,
        type_key,
        wit_name,
    });

    visiting.remove(&type_id);
    planned.insert(type_id);
    Ok(())
}

fn collect_direct_dependencies(kind: &TypeDefKind, dependencies: &mut Vec<wit_parser::TypeId>) {
    match kind {
        TypeDefKind::Record(record) => {
            for field in &record.fields {
                if let Type::Id(type_id) = field.ty {
                    dependencies.push(type_id);
                }
            }
        }
        TypeDefKind::Handle(handle) => match handle {
            wit_parser::Handle::Borrow(type_id) | wit_parser::Handle::Own(type_id) => {
                dependencies.push(*type_id);
            }
        },
        TypeDefKind::Tuple(tuple) => {
            for ty in &tuple.types {
                if let Type::Id(type_id) = ty {
                    dependencies.push(*type_id);
                }
            }
        }
        TypeDefKind::Variant(variant) => {
            for case in &variant.cases {
                if let Some(Type::Id(type_id)) = case.ty {
                    dependencies.push(type_id);
                }
            }
        }
        TypeDefKind::Option(Type::Id(type_id))
        | TypeDefKind::List(Type::Id(type_id))
        | TypeDefKind::Type(Type::Id(type_id)) => {
            dependencies.push(*type_id);
        }
        TypeDefKind::Future(Some(ty)) | TypeDefKind::Stream(Some(ty)) => {
            if let Type::Id(type_id) = ty {
                dependencies.push(*type_id);
            }
        }
        TypeDefKind::Result(result) => {
            if let Some(Type::Id(type_id)) = result.ok {
                dependencies.push(type_id);
            }
            if let Some(Type::Id(type_id)) = result.err {
                dependencies.push(type_id);
            }
        }
        TypeDefKind::Map(key, value) => {
            if let Type::Id(type_id) = key {
                dependencies.push(*type_id);
            }
            if let Type::Id(type_id) = value {
                dependencies.push(*type_id);
            }
        }
        TypeDefKind::FixedLengthList(Type::Id(type_id), _) => {
            dependencies.push(*type_id);
        }
        TypeDefKind::Resource
        | TypeDefKind::Flags(_)
        | TypeDefKind::Enum(_)
        | TypeDefKind::Unknown
        | TypeDefKind::Future(None)
        | TypeDefKind::Stream(None)
        | TypeDefKind::FixedLengthList(_, _)
        | TypeDefKind::Option(_)
        | TypeDefKind::List(_)
        | TypeDefKind::Type(_) => {}
    }
}

fn wit_to_pg(
    module_prefix: &str,
    resolve: &wit_parser::Resolve,
    ty: Type,
) -> Result<PgType, PgWasmError> {
    match ty {
        Type::Bool => Ok(PgType::Scalar("boolean")),
        Type::S8 | Type::S16 => Ok(PgType::Scalar("int2")),
        Type::S32 => Ok(PgType::Scalar("int4")),
        Type::S64 => Ok(PgType::Scalar("int8")),
        Type::U8 => Ok(PgType::Domain {
            base: "int2",
            check: Some("VALUE >= 0"),
            flag_names: None,
            name: Some(format!("{module_prefix}_u8")),
        }),
        Type::U16 => Ok(PgType::Domain {
            base: "int2",
            check: Some("VALUE >= 0"),
            flag_names: None,
            name: Some(format!("{module_prefix}_u16")),
        }),
        Type::U32 => Ok(PgType::Domain {
            base: "int8",
            check: Some("VALUE >= 0"),
            flag_names: None,
            name: Some(format!("{module_prefix}_u32")),
        }),
        Type::U64 => Ok(PgType::Domain {
            base: "numeric",
            check: Some("VALUE >= 0 AND VALUE <= 18446744073709551615"),
            flag_names: None,
            name: Some(format!("{module_prefix}_u64")),
        }),
        Type::F32 => Ok(PgType::Scalar("real")),
        Type::F64 => Ok(PgType::Scalar("double precision")),
        Type::Char => Ok(PgType::Scalar("\"char\"")),
        Type::String => Ok(PgType::Scalar("text")),
        Type::ErrorContext => Ok(PgType::Scalar("text")),
        Type::Id(type_id) => map_typedef(module_prefix, resolve, type_id),
    }
}

fn map_typedef(
    module_prefix: &str,
    resolve: &wit_parser::Resolve,
    type_id: wit_parser::TypeId,
) -> Result<PgType, PgWasmError> {
    let typedef = resolve.types.get(type_id).ok_or_else(|| {
        PgWasmError::InvalidModule(format!("type id {:?} was not present in resolve", type_id))
    })?;

    match &typedef.kind {
        TypeDefKind::Record(record) => {
            let mut fields = Vec::with_capacity(record.fields.len());
            for field in &record.fields {
                fields.push(CompositeField {
                    name: field.name.clone(),
                    ty: wit_to_pg(module_prefix, resolve, field.ty)?,
                });
            }
            Ok(PgType::Composite(fields))
        }
        TypeDefKind::Resource | TypeDefKind::Handle(_) => Ok(PgType::Scalar("int8")),
        TypeDefKind::Flags(flags) => {
            let flag_names = flags.flags.iter().map(|flag| flag.name.clone()).collect();
            Ok(PgType::Domain {
                base: "int4",
                check: None,
                flag_names: Some(flag_names),
                name: Some(format!(
                    "{}_flags_{}",
                    module_prefix,
                    sanitize_ident(&type_name(typedef, type_id))
                )),
            })
        }
        TypeDefKind::Tuple(tuple) => {
            let mut fields = Vec::with_capacity(tuple.types.len());
            for (index, tuple_ty) in tuple.types.iter().copied().enumerate() {
                fields.push(CompositeField {
                    name: format!("f{index}"),
                    ty: wit_to_pg(module_prefix, resolve, tuple_ty)?,
                });
            }
            Ok(PgType::Composite(fields))
        }
        TypeDefKind::Variant(variant) => {
            let mut cases = Vec::with_capacity(variant.cases.len());
            for case in &variant.cases {
                cases.push(VariantCasePlan {
                    name: case.name.clone(),
                    payload: case
                        .ty
                        .map(|case_ty| wit_to_pg(module_prefix, resolve, case_ty))
                        .transpose()?,
                });
            }
            Ok(PgType::Variant(cases))
        }
        TypeDefKind::Enum(enm) => Ok(PgType::Enum(
            enm.cases.iter().map(|case| case.name.clone()).collect(),
        )),
        TypeDefKind::Option(inner) => wit_to_pg(module_prefix, resolve, *inner),
        TypeDefKind::Result(result) => Ok(PgType::Composite(vec![
            CompositeField {
                name: "ok".to_string(),
                ty: result
                    .ok
                    .map(|ok_ty| wit_to_pg(module_prefix, resolve, ok_ty))
                    .transpose()?
                    .unwrap_or(PgType::Scalar("void")),
            },
            CompositeField {
                name: "err".to_string(),
                ty: result
                    .err
                    .map(|err_ty| wit_to_pg(module_prefix, resolve, err_ty))
                    .transpose()?
                    .unwrap_or(PgType::Scalar("void")),
            },
        ])),
        TypeDefKind::List(Type::U8) => Ok(PgType::Scalar("bytea")),
        TypeDefKind::List(inner) => Ok(PgType::Array(Box::new(wit_to_pg(
            module_prefix,
            resolve,
            *inner,
        )?))),
        TypeDefKind::Map(_, _)
        | TypeDefKind::FixedLengthList(_, _)
        | TypeDefKind::Future(_)
        | TypeDefKind::Stream(_) => Ok(PgType::Domain {
            base: "jsonb",
            check: None,
            flag_names: None,
            name: Some(format!(
                "{}_{}_json",
                module_prefix,
                sanitize_ident(&type_name(typedef, type_id))
            )),
        }),
        TypeDefKind::Type(alias) => wit_to_pg(module_prefix, resolve, *alias),
        TypeDefKind::Unknown => Err(PgWasmError::InvalidModule(format!(
            "encountered unknown WIT type definition for {:?}",
            type_id
        ))),
    }
}

fn type_name(typedef: &wit_parser::TypeDef, type_id: wit_parser::TypeId) -> String {
    typedef
        .name
        .clone()
        .unwrap_or_else(|| format!("anonymous_{:?}", type_id))
}

fn build_type_key(
    resolve: &wit_parser::Resolve,
    type_id: wit_parser::TypeId,
    type_name: &str,
) -> String {
    let Some(typedef) = resolve.types.get(type_id) else {
        return format!("unknown:unknown/{type_name}");
    };

    match typedef.owner {
        TypeOwner::Interface(interface_id) => {
            if let Some(interface) = resolve.interfaces.get(interface_id) {
                let interface_name = interface.name.as_deref().unwrap_or("interface");
                let package_name = interface
                    .package
                    .and_then(|package_id| resolve.packages.get(package_id))
                    .map(|pkg| pkg.name.to_string())
                    .unwrap_or_else(|| "package".to_string());
                format!("{package_name}:{interface_name}/{type_name}")
            } else {
                format!("interface:{:?}/{type_name}", interface_id)
            }
        }
        TypeOwner::World(world_id) => {
            if let Some(world) = resolve.worlds.get(world_id) {
                let package_name = world
                    .package
                    .and_then(|package_id| resolve.packages.get(package_id))
                    .map(|pkg| pkg.name.to_string())
                    .unwrap_or_else(|| "package".to_string());
                format!("{package_name}:world/{type_name}")
            } else {
                format!("world:{:?}/{type_name}", world_id)
            }
        }
        TypeOwner::None => format!("none:anonymous/{type_name}"),
    }
}

fn sanitize_ident(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut last_was_underscore = false;

    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            last_was_underscore = false;
        } else if !last_was_underscore {
            out.push('_');
            last_was_underscore = true;
        }
    }

    if out.is_empty() {
        "anon".to_string()
    } else {
        out.trim_matches('_').to_string()
    }
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use std::collections::HashMap;

    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};

    use super::*;
    use crate::wit::world;

    fn fixture_core_module() -> &'static [u8] {
        &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
    }

    fn fixture_component_bytes(wit_source: &str, world_name: &str) -> Vec<u8> {
        let mut module = fixture_core_module().to_vec();
        let mut resolve = wit_parser::Resolve::default();
        let pkg = resolve
            .push_str("fixture.wit", wit_source)
            .expect("fixture wit should parse");
        let world_id = resolve
            .select_world(&[pkg], Some(world_name))
            .expect("fixture world should exist");
        embed_component_metadata(&mut module, &resolve, world_id, StringEncoding::UTF8)
            .expect("fixture metadata should embed");

        let mut encoder = ComponentEncoder::default()
            .module(&module)
            .expect("fixture module should encode")
            .validate(true);
        encoder.encode().expect("component bytes should build")
    }

    #[test]
    fn empty_world_produces_empty_type_plan() {
        let decoded = world::decode(&fixture_component_bytes(
            "package test:fixture; world fixture {}",
            "fixture",
        ))
        .expect("fixture component should decode");
        let plan = plan_types("demo", &decoded).expect("planning should succeed");
        assert!(plan.entries.is_empty());
    }

    #[test]
    fn planning_is_deterministic_for_same_module_prefix() {
        let bytes = fixture_component_bytes(
            r#"
                package test:fixture;

                interface api {
                    record person {
                        id: u32,
                        name: string,
                    }

                    enum color {
                        red,
                        blue,
                    }
                }

                world fixture {
                    export api;
                }
            "#,
            "fixture",
        );
        let decoded_a = world::decode(&bytes).expect("fixture should decode");
        let decoded_b = world::decode(&bytes).expect("fixture should decode again");

        let plan_a = plan_types("demo", &decoded_a).expect("plan should build");
        let plan_b = plan_types("demo", &decoded_b).expect("plan should build");

        assert_eq!(plan_a.entries.len(), 2);
        assert_eq!(plan_a.entries[0].wit_name, "person");
        assert_eq!(plan_a.entries[1].wit_name, "color");
        assert_eq!(plan_a, plan_b);
    }

    #[test]
    fn plan_snapshot_record_and_enum() {
        let bytes = fixture_component_bytes(
            r#"
                package test:fixture;

                interface api {
                    record person {
                        id: u32,
                        name: string,
                    }

                    enum color {
                        red,
                        blue,
                    }
                }

                world fixture {
                    export api;
                }
            "#,
            "fixture",
        );
        let decoded = world::decode(&bytes).expect("fixture should decode");
        let plan = plan_types("demo", &decoded).expect("plan should build");

        assert_eq!(
            format!("{plan:?}"),
            concat!(
                "TypePlan { entries: [",
                "TypePlanEntry { dependencies: [], pg_type: Composite([",
                "CompositeField { name: \"id\", ty: Domain { base: \"int8\", check: Some(\"VALUE >= 0\"), flag_names: None, name: Some(\"demo_u32\") } }, ",
                "CompositeField { name: \"name\", ty: Scalar(\"text\") }",
                "]), type_key: \"test:fixture:api/person\", wit_name: \"person\" }, ",
                "TypePlanEntry { dependencies: [], pg_type: Enum([\"red\", \"blue\"]), type_key: \"test:fixture:api/color\", wit_name: \"color\" }",
                "] }"
            )
        );
    }

    #[test]
    fn included_world_exports_contribute_types() {
        let bytes = fixture_component_bytes(
            r#"
                package test:fixture;

                interface inner-i {
                    record inner-record {
                        x: u32,
                    }
                }

                world inner {
                    export inner-i;
                }

                world outer {
                    include inner;
                }
            "#,
            "outer",
        );
        let decoded = world::decode(&bytes).expect("outer world should decode");
        let plan = plan_types("demo", &decoded).expect("plan should include included types");
        assert_eq!(plan.entries.len(), 1);
        assert_eq!(plan.entries[0].wit_name, "inner-record");
        assert_eq!(
            plan.entries[0].type_key,
            "test:fixture:inner-i/inner-record"
        );
    }

    #[test]
    fn unsigned_scalars_map_to_domains() {
        let mut names = HashMap::new();
        names.insert(Type::U8, "int2");
        names.insert(Type::U16, "int2");
        names.insert(Type::U32, "int8");
        names.insert(Type::U64, "numeric");

        for (wit_ty, expected_base) in names {
            let mapped = wit_to_pg("demo", &wit_parser::Resolve::default(), wit_ty)
                .expect("unsigned scalar should map");
            match mapped {
                PgType::Domain { base, .. } => assert_eq!(base, expected_base),
                other => panic!("expected domain mapping, got {other:?}"),
            }
        }
    }
}
