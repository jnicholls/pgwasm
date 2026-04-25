-- Observability SRFs: catalog-backed rows, shmem stats, reader grants.
-- Extension is created by tests/pg_regress/sql/setup.sql.

-- Clear stale module slots from prior regress / `#[pg_test]` runs (shmem survives `DROP DATABASE`).
SELECT wasm.test_scrub_shmem_slots(1, 20000) AS scrubbed_slots;

-- Seed a minimal module + export (superuser bypasses wasm_loader for INSERT).
INSERT INTO wasm.modules (
    module_id,
    name,
    abi,
    digest,
    wasm_sha256,
    origin,
    artifact_path,
    wit_world,
    policy,
    limits,
    generation
) VALUES (
    900000001,
    'views_test_mod',
    'component',
    decode('deadbeef', 'hex'),
    decode(repeat('ab', 16), 'hex'),
    'regress',
    '/tmp/views_test.wasm',
    'default',
    '{"allow_wasi_net": false}'::jsonb,
    '{"fuel_per_invocation": 42}'::jsonb,
    7
);

INSERT INTO wasm.exports (
    module_id,
    wasm_name,
    sql_name,
    signature,
    arg_types,
    ret_type,
    fn_oid,
    kind
)
SELECT
    m.module_id,
    'add',
    'views_test_add',
    '{}'::jsonb,
    ARRAY[]::oid[],
    NULL::oid,
    NULL::oid,
    'function'
FROM wasm.modules m
WHERE m.name = 'views_test_mod';

SELECT module_id, name, origin, digest, policy_json, limits_json, shared
FROM wasm.modules()
WHERE name = 'views_test_mod'
ORDER BY name;

SELECT module_name, export_name, fn_oid, arg_types, ret_type, abi, last_seen_generation
FROM wasm.functions()
WHERE module_name = 'views_test_mod'
ORDER BY export_name;

SELECT
    module_name,
    policy_json ->> 'allow_wasi_net' AS allow_wasi_net,
    limits_json ->> 'fuel_per_invocation' AS fuel_per_invocation
FROM wasm.policy_effective()
WHERE module_name = 'views_test_mod'
ORDER BY module_name;

-- Allocate shmem slots and bump invocations (superuser-only helper).
SELECT wasm.test_bump_export_counters(m.module_id, 0, 3) AS bumped
FROM wasm.modules m
WHERE m.name = 'views_test_mod';

SELECT module_name, export_name, invocations, traps, fuel_used_total, last_invocation_at, shared
FROM wasm.stats()
WHERE module_name = 'views_test_mod'
ORDER BY export_name;

SELECT wasm.test_bump_export_counters(m.module_id, 0, 2) AS bumped_again
FROM wasm.modules m
WHERE m.name = 'views_test_mod';

SELECT module_name, export_name, invocations, traps, fuel_used_total, last_invocation_at, shared
FROM wasm.stats()
WHERE module_name = 'views_test_mod'
ORDER BY export_name;

-- Monotonicity: cumulative invocations increase across bumps (5 = 3 + 2).
SELECT invocations > 3 AS invocations_monotonic
FROM wasm.stats()
WHERE module_name = 'views_test_mod' AND export_name = 'add';

-- Reader role: catalog SELECT + SRF EXECUTE + view SELECT.
DO $reader_block$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'views_test_reader') THEN
        CREATE ROLE views_test_reader NOLOGIN IN ROLE wasm_reader;
    END IF;
END
$reader_block$;

SET ROLE views_test_reader;

SELECT COUNT(*) AS n_modules FROM wasm.modules_view;
SELECT COUNT(*) AS n_functions FROM wasm.functions_view;
SELECT COUNT(*) AS n_wit FROM wasm.wit_types_view;
SELECT COUNT(*) AS n_policy FROM wasm.policy_effective_view;
SELECT COUNT(*) AS n_stats FROM wasm.stats_view;

RESET ROLE;

DELETE FROM wasm.exports WHERE module_id = (SELECT module_id FROM wasm.modules WHERE name = 'views_test_mod');
DELETE FROM wasm.modules WHERE name = 'views_test_mod';

DROP ROLE IF EXISTS views_test_reader;
