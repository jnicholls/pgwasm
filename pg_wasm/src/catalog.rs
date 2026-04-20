//! Catalog schema and catalog access utilities.

use std::collections::BTreeSet;

use pgrx::prelude::*;
use pgrx::spi::{self, Spi, SpiHeapTupleData};
use serde_json::Value;

use crate::errors::{PgWasmError, Result};

fn default_json_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn map_spi_error(context: &str, error: spi::Error) -> PgWasmError {
    PgWasmError::Internal(format!("catalog SPI error while {context}: {error}"))
}

fn required_field<T>(row: &SpiHeapTupleData<'_>, field: &str) -> core::result::Result<T, spi::Error>
where
    T: FromDatum + IntoDatum,
{
    row.get_by_name::<T, _>(field)?
        .ok_or(spi::Error::InvalidPosition)
}

pub(crate) mod modules {
    use pgrx::JsonB;
    use pgrx::spi::SpiTupleTable;

    use super::*;

    const RETURNING_COLUMNS: &str = "module_id, name, abi, digest, wasm_sha256, origin, artifact_path, wit_world, policy, limits, created_at, updated_at, generation";

    #[derive(Clone, Debug)]
    pub(crate) struct ModuleRow {
        pub abi: String,
        pub artifact_path: String,
        pub created_at: TimestampWithTimeZone,
        pub digest: Vec<u8>,
        pub generation: i64,
        pub limits: Value,
        pub module_id: i64,
        pub name: String,
        pub origin: String,
        pub policy: Value,
        pub updated_at: TimestampWithTimeZone,
        pub wasm_sha256: Vec<u8>,
        pub wit_world: String,
    }

    #[derive(Clone, Debug)]
    pub(crate) struct NewModule {
        pub abi: String,
        pub artifact_path: String,
        pub digest: Vec<u8>,
        pub generation: i64,
        pub limits: Value,
        pub name: String,
        pub origin: String,
        pub policy: Value,
        pub wasm_sha256: Vec<u8>,
        pub wit_world: String,
    }

    pub(crate) fn insert(new_module: &NewModule) -> Result<ModuleRow> {
        let sql = format!(
            "INSERT INTO pg_wasm.modules (name, abi, digest, wasm_sha256, origin, artifact_path, wit_world, policy, limits, generation)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             RETURNING {RETURNING_COLUMNS}"
        );

        Spi::connect_mut(|client| {
            let args = vec![
                new_module.name.as_str().into(),
                new_module.abi.as_str().into(),
                new_module.digest.clone().into(),
                new_module.wasm_sha256.clone().into(),
                new_module.origin.as_str().into(),
                new_module.artifact_path.as_str().into(),
                new_module.wit_world.as_str().into(),
                JsonB(new_module.policy.clone()).into(),
                JsonB(new_module.limits.clone()).into(),
                new_module.generation.into(),
            ];

            client
                .update(sql.as_str(), Some(1), args.as_slice())
                .and_then(|rows| first_row(rows))
                .and_then(|row| module_from_row(&row))
        })
        .map_err(|error| map_spi_error("inserting module row", error))
    }

    pub(crate) fn get_by_id(module_id: i64) -> Result<Option<ModuleRow>> {
        get_one_by("module_id = $1", module_id.into())
    }

    pub(crate) fn get_by_name(name: &str) -> Result<Option<ModuleRow>> {
        get_one_by("name = $1", name.into())
    }

    pub(crate) fn list() -> Result<Vec<ModuleRow>> {
        Spi::connect(|client| {
            let rows = client.select(
                format!("SELECT {RETURNING_COLUMNS} FROM pg_wasm.modules ORDER BY module_id")
                    .as_str(),
                None,
                &[],
            )?;
            rows.into_iter().map(|row| module_from_row(&row)).collect()
        })
        .map_err(|error| map_spi_error("listing module rows", error))
    }

    pub(crate) fn update(module_id: i64, updated_module: &NewModule) -> Result<Option<ModuleRow>> {
        let sql = format!(
            "UPDATE pg_wasm.modules
             SET
                 name = $2,
                 abi = $3,
                 digest = $4,
                 wasm_sha256 = $5,
                 origin = $6,
                 artifact_path = $7,
                 wit_world = $8,
                 policy = $9,
                 limits = $10,
                 generation = $11,
                 updated_at = pg_catalog.clock_timestamp()
             WHERE module_id = $1
             RETURNING {RETURNING_COLUMNS}"
        );

        Spi::connect_mut(|client| {
            let args = vec![
                module_id.into(),
                updated_module.name.as_str().into(),
                updated_module.abi.as_str().into(),
                updated_module.digest.clone().into(),
                updated_module.wasm_sha256.clone().into(),
                updated_module.origin.as_str().into(),
                updated_module.artifact_path.as_str().into(),
                updated_module.wit_world.as_str().into(),
                JsonB(updated_module.policy.clone()).into(),
                JsonB(updated_module.limits.clone()).into(),
                updated_module.generation.into(),
            ];

            maybe_first(client.update(sql.as_str(), Some(1), args.as_slice())?)
                .map(|row| module_from_row(&row))
                .transpose()
        })
        .map_err(|error| map_spi_error("updating module row", error))
    }

    pub(crate) fn delete(module_id: i64) -> Result<bool> {
        Spi::connect_mut(|client| {
            let args = vec![module_id.into()];
            let deleted = client
                .update(
                    "DELETE FROM pg_wasm.modules WHERE module_id = $1",
                    None,
                    args.as_slice(),
                )?
                .len();
            Ok(deleted > 0)
        })
        .map_err(|error| map_spi_error("deleting module row", error))
    }

    fn get_one_by<'a>(
        predicate: &str,
        value: pgrx::datum::DatumWithOid<'a>,
    ) -> Result<Option<ModuleRow>> {
        let sql = format!(
            "SELECT {RETURNING_COLUMNS}
             FROM pg_wasm.modules
             WHERE {predicate}"
        );

        Spi::connect(|client| {
            let args = vec![value];
            maybe_first(client.select(sql.as_str(), Some(1), args.as_slice())?)
                .map(|row| module_from_row(&row))
                .transpose()
        })
        .map_err(|error| map_spi_error("reading module row", error))
    }

    fn module_from_row(row: &SpiHeapTupleData<'_>) -> core::result::Result<ModuleRow, spi::Error> {
        let policy = row
            .get_by_name::<JsonB, _>("policy")?
            .map(|json| json.0)
            .unwrap_or_else(default_json_object);
        let limits = row
            .get_by_name::<JsonB, _>("limits")?
            .map(|json| json.0)
            .unwrap_or_else(default_json_object);

        Ok(ModuleRow {
            abi: required_field::<String>(row, "abi")?,
            artifact_path: required_field::<String>(row, "artifact_path")?,
            created_at: required_field::<TimestampWithTimeZone>(row, "created_at")?,
            digest: required_field::<Vec<u8>>(row, "digest")?,
            generation: required_field::<i64>(row, "generation")?,
            limits,
            module_id: required_field::<i64>(row, "module_id")?,
            name: required_field::<String>(row, "name")?,
            origin: required_field::<String>(row, "origin")?,
            policy,
            updated_at: required_field::<TimestampWithTimeZone>(row, "updated_at")?,
            wasm_sha256: required_field::<Vec<u8>>(row, "wasm_sha256")?,
            wit_world: required_field::<String>(row, "wit_world")?,
        })
    }

    fn first_row(
        mut rows: SpiTupleTable<'_>,
    ) -> core::result::Result<SpiHeapTupleData<'_>, spi::Error> {
        rows.next().ok_or(spi::Error::InvalidPosition)
    }

    fn maybe_first(
        mut rows: SpiTupleTable<'_>,
    ) -> core::result::Result<Option<SpiHeapTupleData<'_>>, spi::Error> {
        Ok(rows.next())
    }
}

pub(crate) mod exports {
    use pgrx::JsonB;
    use pgrx::spi::SpiTupleTable;

    use super::*;

    const RETURNING_COLUMNS: &str =
        "export_id, module_id, wasm_name, sql_name, signature, arg_types, ret_type, fn_oid, kind";

    #[derive(Clone, Debug)]
    pub(crate) struct ExportRow {
        pub arg_types: Vec<pg_sys::Oid>,
        pub export_id: i64,
        pub fn_oid: Option<pg_sys::Oid>,
        pub kind: String,
        pub module_id: i64,
        pub ret_type: Option<pg_sys::Oid>,
        pub signature: Value,
        pub sql_name: String,
        pub wasm_name: String,
    }

    #[derive(Clone, Debug)]
    pub(crate) struct NewExport {
        pub arg_types: Vec<pg_sys::Oid>,
        pub fn_oid: Option<pg_sys::Oid>,
        pub kind: String,
        pub module_id: i64,
        pub ret_type: Option<pg_sys::Oid>,
        pub signature: Value,
        pub sql_name: String,
        pub wasm_name: String,
    }

    pub(crate) fn insert(new_export: &NewExport) -> Result<ExportRow> {
        let sql = format!(
            "INSERT INTO pg_wasm.exports (module_id, wasm_name, sql_name, signature, arg_types, ret_type, fn_oid, kind)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             RETURNING {RETURNING_COLUMNS}"
        );

        Spi::connect_mut(|client| {
            let args = vec![
                new_export.module_id.into(),
                new_export.wasm_name.as_str().into(),
                new_export.sql_name.as_str().into(),
                JsonB(new_export.signature.clone()).into(),
                new_export.arg_types.clone().into(),
                new_export.ret_type.into(),
                new_export.fn_oid.into(),
                new_export.kind.as_str().into(),
            ];

            client
                .update(sql.as_str(), Some(1), args.as_slice())
                .and_then(first_row)
                .and_then(|row| export_from_row(&row))
        })
        .map_err(|error| map_spi_error("inserting export row", error))
    }

    pub(crate) fn get_by_id(export_id: i64) -> Result<Option<ExportRow>> {
        get_one_by("export_id = $1", export_id.into())
    }

    pub(crate) fn get_by_fn_oid(fn_oid: pg_sys::Oid) -> Result<Option<ExportRow>> {
        get_one_by("fn_oid = $1", fn_oid.into())
    }

    pub(crate) fn list() -> Result<Vec<ExportRow>> {
        Spi::connect(|client| {
            let rows = client.select(
                format!("SELECT {RETURNING_COLUMNS} FROM pg_wasm.exports ORDER BY export_id")
                    .as_str(),
                None,
                &[],
            )?;
            rows.into_iter().map(|row| export_from_row(&row)).collect()
        })
        .map_err(|error| map_spi_error("listing export rows", error))
    }

    pub(crate) fn list_by_module(module_id: i64) -> Result<Vec<ExportRow>> {
        let sql = format!(
            "SELECT {RETURNING_COLUMNS}
             FROM pg_wasm.exports
             WHERE module_id = $1
             ORDER BY export_id"
        );

        Spi::connect(|client| {
            let args = vec![module_id.into()];
            let rows = client.select(sql.as_str(), None, args.as_slice())?;
            rows.into_iter().map(|row| export_from_row(&row)).collect()
        })
        .map_err(|error| map_spi_error("listing export rows by module", error))
    }

    pub(crate) fn update(export_id: i64, updated_export: &NewExport) -> Result<Option<ExportRow>> {
        let sql = format!(
            "UPDATE pg_wasm.exports
             SET
                 module_id = $2,
                 wasm_name = $3,
                 sql_name = $4,
                 signature = $5,
                 arg_types = $6,
                 ret_type = $7,
                 fn_oid = $8,
                 kind = $9
             WHERE export_id = $1
             RETURNING {RETURNING_COLUMNS}"
        );

        Spi::connect_mut(|client| {
            let args = vec![
                export_id.into(),
                updated_export.module_id.into(),
                updated_export.wasm_name.as_str().into(),
                updated_export.sql_name.as_str().into(),
                JsonB(updated_export.signature.clone()).into(),
                updated_export.arg_types.clone().into(),
                updated_export.ret_type.into(),
                updated_export.fn_oid.into(),
                updated_export.kind.as_str().into(),
            ];
            maybe_first(client.update(sql.as_str(), Some(1), args.as_slice())?)
                .map(|row| export_from_row(&row))
                .transpose()
        })
        .map_err(|error| map_spi_error("updating export row", error))
    }

    pub(crate) fn delete(export_id: i64) -> Result<bool> {
        Spi::connect_mut(|client| {
            let args = vec![export_id.into()];
            let deleted = client
                .update(
                    "DELETE FROM pg_wasm.exports WHERE export_id = $1",
                    None,
                    args.as_slice(),
                )?
                .len();
            Ok(deleted > 0)
        })
        .map_err(|error| map_spi_error("deleting export row", error))
    }

    fn get_one_by<'a>(
        predicate: &str,
        value: pgrx::datum::DatumWithOid<'a>,
    ) -> Result<Option<ExportRow>> {
        let sql = format!(
            "SELECT {RETURNING_COLUMNS}
             FROM pg_wasm.exports
             WHERE {predicate}"
        );

        Spi::connect(|client| {
            let args = vec![value];
            maybe_first(client.select(sql.as_str(), Some(1), args.as_slice())?)
                .map(|row| export_from_row(&row))
                .transpose()
        })
        .map_err(|error| map_spi_error("reading export row", error))
    }

    fn export_from_row(row: &SpiHeapTupleData<'_>) -> core::result::Result<ExportRow, spi::Error> {
        let signature = row
            .get_by_name::<JsonB, _>("signature")?
            .map(|json| json.0)
            .unwrap_or_else(default_json_object);
        let arg_types = row
            .get_by_name::<Vec<pg_sys::Oid>, _>("arg_types")?
            .unwrap_or_default();

        Ok(ExportRow {
            arg_types,
            export_id: required_field::<i64>(row, "export_id")?,
            fn_oid: row.get_by_name::<pg_sys::Oid, _>("fn_oid")?,
            kind: required_field::<String>(row, "kind")?,
            module_id: required_field::<i64>(row, "module_id")?,
            ret_type: row.get_by_name::<pg_sys::Oid, _>("ret_type")?,
            signature,
            sql_name: required_field::<String>(row, "sql_name")?,
            wasm_name: required_field::<String>(row, "wasm_name")?,
        })
    }

    fn first_row(
        mut rows: SpiTupleTable<'_>,
    ) -> core::result::Result<SpiHeapTupleData<'_>, spi::Error> {
        rows.next().ok_or(spi::Error::InvalidPosition)
    }

    fn maybe_first(
        mut rows: SpiTupleTable<'_>,
    ) -> core::result::Result<Option<SpiHeapTupleData<'_>>, spi::Error> {
        Ok(rows.next())
    }
}

pub(crate) mod wit_types {
    use pgrx::JsonB;
    use pgrx::spi::SpiTupleTable;

    use super::*;

    const RETURNING_COLUMNS: &str =
        "wit_type_id, module_id, wit_name, pg_type_oid, kind, definition";

    #[derive(Clone, Debug)]
    pub(crate) struct NewWitType {
        pub definition: Value,
        pub kind: String,
        pub module_id: i64,
        pub pg_type_oid: pg_sys::Oid,
        pub wit_name: String,
    }

    #[derive(Clone, Debug)]
    pub(crate) struct WitTypeRow {
        pub definition: Value,
        pub kind: String,
        pub module_id: i64,
        pub pg_type_oid: pg_sys::Oid,
        pub wit_name: String,
        pub wit_type_id: i64,
    }

    pub(crate) fn insert(new_wit_type: &NewWitType) -> Result<WitTypeRow> {
        let sql = format!(
            "INSERT INTO pg_wasm.wit_types (module_id, wit_name, pg_type_oid, kind, definition)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING {RETURNING_COLUMNS}"
        );

        Spi::connect_mut(|client| {
            let args = vec![
                new_wit_type.module_id.into(),
                new_wit_type.wit_name.as_str().into(),
                new_wit_type.pg_type_oid.into(),
                new_wit_type.kind.as_str().into(),
                JsonB(new_wit_type.definition.clone()).into(),
            ];

            client
                .update(sql.as_str(), Some(1), args.as_slice())
                .and_then(first_row)
                .and_then(|row| wit_type_from_row(&row))
        })
        .map_err(|error| map_spi_error("inserting WIT type row", error))
    }

    pub(crate) fn get_by_id(wit_type_id: i64) -> Result<Option<WitTypeRow>> {
        get_one_by("wit_type_id = $1", wit_type_id.into())
    }

    pub(crate) fn list() -> Result<Vec<WitTypeRow>> {
        Spi::connect(|client| {
            let rows = client.select(
                format!("SELECT {RETURNING_COLUMNS} FROM pg_wasm.wit_types ORDER BY wit_type_id")
                    .as_str(),
                None,
                &[],
            )?;
            rows.into_iter()
                .map(|row| wit_type_from_row(&row))
                .collect()
        })
        .map_err(|error| map_spi_error("listing WIT type rows", error))
    }

    pub(crate) fn list_by_module(module_id: i64) -> Result<Vec<WitTypeRow>> {
        let sql = format!(
            "SELECT {RETURNING_COLUMNS}
             FROM pg_wasm.wit_types
             WHERE module_id = $1
             ORDER BY wit_type_id"
        );

        Spi::connect(|client| {
            let args = vec![module_id.into()];
            let rows = client.select(sql.as_str(), None, args.as_slice())?;
            rows.into_iter()
                .map(|row| wit_type_from_row(&row))
                .collect()
        })
        .map_err(|error| map_spi_error("listing WIT type rows by module", error))
    }

    pub(crate) fn update(
        wit_type_id: i64,
        updated_wit_type: &NewWitType,
    ) -> Result<Option<WitTypeRow>> {
        let sql = format!(
            "UPDATE pg_wasm.wit_types
             SET
                 module_id = $2,
                 wit_name = $3,
                 pg_type_oid = $4,
                 kind = $5,
                 definition = $6
             WHERE wit_type_id = $1
             RETURNING {RETURNING_COLUMNS}"
        );

        Spi::connect_mut(|client| {
            let args = vec![
                wit_type_id.into(),
                updated_wit_type.module_id.into(),
                updated_wit_type.wit_name.as_str().into(),
                updated_wit_type.pg_type_oid.into(),
                updated_wit_type.kind.as_str().into(),
                JsonB(updated_wit_type.definition.clone()).into(),
            ];

            maybe_first(client.update(sql.as_str(), Some(1), args.as_slice())?)
                .map(|row| wit_type_from_row(&row))
                .transpose()
        })
        .map_err(|error| map_spi_error("updating WIT type row", error))
    }

    pub(crate) fn delete(wit_type_id: i64) -> Result<bool> {
        Spi::connect_mut(|client| {
            let args = vec![wit_type_id.into()];
            let deleted = client
                .update(
                    "DELETE FROM pg_wasm.wit_types WHERE wit_type_id = $1",
                    None,
                    args.as_slice(),
                )?
                .len();
            Ok(deleted > 0)
        })
        .map_err(|error| map_spi_error("deleting WIT type row", error))
    }

    fn get_one_by<'a>(
        predicate: &str,
        value: pgrx::datum::DatumWithOid<'a>,
    ) -> Result<Option<WitTypeRow>> {
        let sql = format!(
            "SELECT {RETURNING_COLUMNS}
             FROM pg_wasm.wit_types
             WHERE {predicate}"
        );

        Spi::connect(|client| {
            let args = vec![value];
            maybe_first(client.select(sql.as_str(), Some(1), args.as_slice())?)
                .map(|row| wit_type_from_row(&row))
                .transpose()
        })
        .map_err(|error| map_spi_error("reading WIT type row", error))
    }

    fn wit_type_from_row(
        row: &SpiHeapTupleData<'_>,
    ) -> core::result::Result<WitTypeRow, spi::Error> {
        let definition = row
            .get_by_name::<JsonB, _>("definition")?
            .map(|json| json.0)
            .unwrap_or_else(default_json_object);

        Ok(WitTypeRow {
            definition,
            kind: required_field::<String>(row, "kind")?,
            module_id: required_field::<i64>(row, "module_id")?,
            pg_type_oid: required_field::<pg_sys::Oid>(row, "pg_type_oid")?,
            wit_name: required_field::<String>(row, "wit_name")?,
            wit_type_id: required_field::<i64>(row, "wit_type_id")?,
        })
    }

    fn first_row(
        mut rows: SpiTupleTable<'_>,
    ) -> core::result::Result<SpiHeapTupleData<'_>, spi::Error> {
        rows.next().ok_or(spi::Error::InvalidPosition)
    }

    fn maybe_first(
        mut rows: SpiTupleTable<'_>,
    ) -> core::result::Result<Option<SpiHeapTupleData<'_>>, spi::Error> {
        Ok(rows.next())
    }
}

pub(crate) mod migrations {
    use super::*;

    pub(super) const TABLE_COLUMNS_SQL: &str = "
        SELECT a.attname
        FROM pg_catalog.pg_attribute AS a
        JOIN pg_catalog.pg_class AS c
            ON c.oid = a.attrelid
        JOIN pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
        WHERE
            n.nspname = 'pg_wasm'
            AND c.relkind = 'r'
            AND c.relname = $1
            AND a.attnum > 0
            AND NOT a.attisdropped
        ORDER BY a.attnum
    ";

    pub(super) const EXPECTED_TABLE_COLUMNS: &[(&str, &[&str])] = &[
        (
            "modules",
            &[
                "module_id",
                "name",
                "abi",
                "digest",
                "wasm_sha256",
                "origin",
                "artifact_path",
                "wit_world",
                "policy",
                "limits",
                "created_at",
                "updated_at",
                "generation",
            ],
        ),
        (
            "exports",
            &[
                "export_id",
                "module_id",
                "wasm_name",
                "sql_name",
                "signature",
                "arg_types",
                "ret_type",
                "fn_oid",
                "kind",
            ],
        ),
        (
            "wit_types",
            &[
                "wit_type_id",
                "module_id",
                "wit_name",
                "pg_type_oid",
                "kind",
                "definition",
            ],
        ),
        ("dependencies", &["module_id", "depends_on_module_id"]),
    ];

    pub(crate) fn validate_shape() {
        let installed = match extension_is_installed() {
            Ok(installed) => installed,
            Err(error) => fail_invalid_configuration(format!(
                "failed checking extension install state: {error}"
            )),
        };

        if !installed {
            return;
        }

        for (table_name, expected_columns) in EXPECTED_TABLE_COLUMNS {
            let actual_columns = match table_columns(table_name) {
                Ok(columns) => columns,
                Err(error) => fail_invalid_configuration(format!(
                    "failed validating table pg_wasm.{table_name}: {error}"
                )),
            };

            if actual_columns.is_empty() {
                fail_invalid_configuration(format!(
                    "catalog table pg_wasm.{table_name} is missing or has no visible columns"
                ));
            }

            let actual_set: BTreeSet<&str> = actual_columns.iter().map(String::as_str).collect();
            let expected_set: BTreeSet<&str> = expected_columns.iter().copied().collect();
            if actual_set != expected_set {
                fail_invalid_configuration(format!(
                    "catalog table pg_wasm.{table_name} has unexpected columns: actual={actual_columns:?}, expected={expected_columns:?}"
                ));
            }
        }
    }

    fn extension_is_installed() -> core::result::Result<bool, PgWasmError> {
        Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_wasm')",
        )
        .map(|maybe_exists| maybe_exists.unwrap_or(false))
        .map_err(|error| map_spi_error("checking pg_extension", error))
    }

    fn table_columns(table_name: &str) -> core::result::Result<Vec<String>, PgWasmError> {
        Spi::connect(|client| {
            let args = vec![table_name.into()];
            let rows = client.select(TABLE_COLUMNS_SQL, None, args.as_slice())?;
            rows.into_iter()
                .map(|row| required_field::<String>(&row, "attname"))
                .collect()
        })
        .map_err(|error| map_spi_error("reading catalog table columns", error))
    }

    fn fail_invalid_configuration(message: String) -> ! {
        let error = PgWasmError::InvalidConfiguration(message);
        ereport!(PgLogLevel::ERROR, error.sqlstate(), error.to_string());
    }
}

/// Called once from `_PG_init`. The catalog shape check is intentionally
/// lightweight and idempotent.
#[allow(dead_code)]
pub(crate) fn init() {
    migrations::validate_shape();
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::spi::Spi;

    use super::migrations::EXPECTED_TABLE_COLUMNS;

    #[pg_test]
    fn test_catalog_shape_and_role_grants() {
        Spi::run("CREATE EXTENSION IF NOT EXISTS pg_wasm").unwrap();
        super::migrations::validate_shape();

        for (table_name, expected_columns) in EXPECTED_TABLE_COLUMNS {
            let actual_columns = table_columns(table_name);
            assert_eq!(
                actual_columns, *expected_columns,
                "unexpected column shape for pg_wasm.{table_name}"
            );
        }

        assert!(role_exists("pg_wasm_reader"));
        assert!(role_exists("pg_wasm_loader"));
        assert!(has_schema_privilege("pg_wasm_reader", "USAGE"));
        assert!(has_schema_privilege("pg_wasm_loader", "USAGE"));

        for (table_name, _) in EXPECTED_TABLE_COLUMNS {
            let qualified_table_name = format!("pg_wasm.{table_name}");
            assert!(has_table_privilege(
                "pg_wasm_reader",
                qualified_table_name.as_str(),
                "SELECT"
            ));

            for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
                assert!(
                    has_table_privilege("pg_wasm_loader", qualified_table_name.as_str(), privilege),
                    "expected pg_wasm_loader to have {privilege} on {qualified_table_name}"
                );
            }
        }
    }

    fn table_columns(table_name: &str) -> Vec<String> {
        Spi::connect(|client| {
            let args = vec![table_name.into()];
            let rows = client
                .select(super::migrations::TABLE_COLUMNS_SQL, None, args.as_slice())
                .unwrap();
            rows.into_iter()
                .map(|row| row.get_by_name::<String, _>("attname").unwrap().unwrap())
                .collect()
        })
    }

    fn role_exists(role_name: &str) -> bool {
        Spi::get_one_with_args(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1)",
            &[role_name.into()],
        )
        .unwrap()
        .unwrap_or(false)
    }

    fn has_schema_privilege(role_name: &str, privilege: &str) -> bool {
        Spi::get_one_with_args(
            "SELECT pg_catalog.has_schema_privilege($1, 'pg_wasm', $2)",
            &[role_name.into(), privilege.into()],
        )
        .unwrap()
        .unwrap_or(false)
    }

    fn has_table_privilege(role_name: &str, table_name: &str, privilege: &str) -> bool {
        Spi::get_one_with_args(
            "SELECT pg_catalog.has_table_privilege($1, $2, $3)",
            &[role_name.into(), table_name.into(), privilege.into()],
        )
        .unwrap()
        .unwrap_or(false)
    }
}
