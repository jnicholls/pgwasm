CREATE TABLE IF NOT EXISTS @extschema@.modules (
    module_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    abi TEXT NOT NULL,
    digest BYTEA NOT NULL,
    wasm_sha256 BYTEA NOT NULL,
    origin TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    wit_world TEXT NOT NULL,
    policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    limits JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp(),
    generation BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS @extschema@.exports (
    export_id BIGSERIAL PRIMARY KEY,
    module_id BIGINT NOT NULL REFERENCES @extschema@.modules (module_id) ON DELETE CASCADE,
    wasm_name TEXT NOT NULL,
    sql_name TEXT NOT NULL,
    signature JSONB NOT NULL DEFAULT '{}'::jsonb,
    arg_types OID[] NOT NULL DEFAULT ARRAY[]::oid[],
    ret_type OID,
    fn_oid OID,
    kind TEXT NOT NULL,
    UNIQUE (module_id, wasm_name),
    UNIQUE (module_id, sql_name)
);

CREATE TABLE IF NOT EXISTS @extschema@.wit_types (
    wit_type_id BIGSERIAL PRIMARY KEY,
    module_id BIGINT NOT NULL REFERENCES @extschema@.modules (module_id) ON DELETE CASCADE,
    wit_name TEXT NOT NULL,
    pg_type_oid OID NOT NULL,
    kind TEXT NOT NULL,
    definition JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (module_id, wit_name)
);

CREATE TABLE IF NOT EXISTS @extschema@.dependencies (
    module_id BIGINT NOT NULL REFERENCES @extschema@.modules (module_id) ON DELETE CASCADE,
    depends_on_module_id BIGINT NOT NULL REFERENCES @extschema@.modules (module_id) ON DELETE CASCADE,
    PRIMARY KEY (module_id, depends_on_module_id),
    CHECK (module_id <> depends_on_module_id)
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'wasm_loader') THEN
        CREATE ROLE wasm_loader NOLOGIN;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'wasm_reader') THEN
        CREATE ROLE wasm_reader NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA @extschema@ TO wasm_loader;
GRANT USAGE ON SCHEMA @extschema@ TO wasm_reader;

GRANT SELECT ON TABLE
    @extschema@.dependencies,
    @extschema@.exports,
    @extschema@.modules,
    @extschema@.wit_types
TO wasm_reader;

GRANT DELETE, INSERT, SELECT, UPDATE ON TABLE
    @extschema@.dependencies,
    @extschema@.exports,
    @extschema@.modules,
    @extschema@.wit_types
TO wasm_loader;
