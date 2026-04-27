-- Core module scalar path: add_i32 fixture (binary matches pgwasm/fixtures/core/add_i32.wasm).
SELECT set_config('pgwasm.fuel_enabled', 'off', false);
SELECT set_config('pgwasm.invocation_deadline_ms', '0', false);

SELECT pgwasm.pgwasm_core_invoke_scalar(
    decode(
        '0061736d0100000001070160027f7f017f030201000707010361646400000a09010700200020016a0b000d046e616d650106010003616464',
        'hex'
    ),
    'add'::text,
    ARRAY[40, 2]::int[]
) AS add_result
ORDER BY 1;
