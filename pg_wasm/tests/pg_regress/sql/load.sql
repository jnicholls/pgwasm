-- pg_wasm.load: tiny component fixture and catalog / modules() visibility.
-- Extension is created by tests/pg_regress/sql/setup.sql.

SELECT wasm.test_scrub_shmem_slots(1, 20000) AS scrubbed_slots;

SELECT wasm.load(
    'load_regress_fixture',
    json_build_object(
        'bytes',
        to_json(
            ARRAY[
                0, 97, 115, 109, 13, 0, 1, 0, 1, 85, 0, 97, 115, 109, 1, 0, 0, 0, 1, 5, 1, 96, 0, 1, 127, 3, 2, 1, 0, 7, 7, 1, 3, 97, 100, 100, 0, 0, 10, 6, 1, 4, 0, 65, 42, 11, 0, 47, 9, 112, 114, 111, 100, 117, 99, 101, 114, 115, 1, 12, 112, 114, 111, 99, 101, 115, 115, 101, 100, 45, 98, 121, 1, 13, 119, 105, 116, 45, 99, 111, 109, 112, 111, 110, 101, 110, 116, 7, 48, 46, 50, 52, 55, 46, 48, 2, 4, 1, 0, 0, 0, 7, 5, 1, 64, 0, 0, 122, 6, 9, 1, 0, 0, 1, 0, 3, 97, 100, 100, 8, 6, 1, 0, 0, 0, 0, 0, 11, 9, 1, 0, 3, 97, 100, 100, 1, 0, 0, 0, 61, 14, 99, 111, 109, 112, 111, 110, 101, 110, 116, 45, 110, 97, 109, 101, 1, 8, 0, 0, 1, 0, 3, 97, 100, 100, 1, 9, 0, 17, 1, 0, 4, 109, 97, 105, 110, 1, 9, 0, 18, 1, 0, 4, 109, 97, 105, 110, 1, 12, 1, 2, 0, 3, 97, 100, 100, 1, 3, 97, 100, 100, 0, 47, 9, 112, 114, 111, 100, 117, 99, 101, 114, 115, 1, 12, 112, 114, 111, 99, 101, 115, 115, 101, 100, 45, 98, 121, 1, 13, 119, 105, 116, 45, 99, 111, 109, 112, 111, 110, 101, 110, 116, 7, 48, 46, 50, 52, 55, 46, 48
            ]::int[]
        )
    ),
    NULL
) AS loaded;

SELECT module_id, name, origin
FROM wasm.modules
WHERE name = 'load_regress_fixture'
ORDER BY name;

SELECT module_id, name, origin
FROM wasm.modules()
WHERE name = 'load_regress_fixture'
ORDER BY name;

SELECT wasm.unload('load_regress_fixture', false) AS unloaded;
