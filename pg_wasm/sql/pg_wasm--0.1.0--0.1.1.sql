-- Applied when the extension default version is bumped to 0.1.1 (or run manually on older
-- installs): several WIT type keys can map to the same PostgreSQL built-in OID, so the old
-- UNIQUE(module_id, pg_type_oid) must be removed.
ALTER TABLE @extschema@.wit_types
    DROP CONSTRAINT IF EXISTS wit_types_module_id_pg_type_oid_key;
