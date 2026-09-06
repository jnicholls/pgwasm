-- Exercise ordinary scalar and lifecycle APIs using the upstream add_i32 fixture.
DO $test$
DECLARE
    wasm_hex text := '0061736d0100000001070160027f7f017f030201000707010361646400000a09010700200020016a0b000d046e616d650106010003616464';
    options_list json[] := ARRAY[
        NULL::json,
        '{}'::json,
        '{"limits":{}}'::json,
        '{"limits":{"max_memory_pages":512}}'::json,
        '{"limits":{"fuel_per_invocation":null,"max_memory_pages":512}}'::json,
        '{"limits":{"fuel_per_invocation":100000,"instances_per_module":1,"invocation_deadline_ms":2000,"max_memory_pages":512}}'::json
    ];
    options json;
    v_module_name text;
    i integer := 0;
    answer integer;
    rejected boolean;
BEGIN
    RESET pgwasm.fuel_enabled;
    IF current_setting('pgwasm.fuel_enabled') <> 'off' THEN
        RAISE EXCEPTION 'regression requires the default fuel-disabled configuration';
    END IF;
    IF pgwasm.pgwasm_core_invoke_scalar(decode(wasm_hex, 'hex'), 'add', ARRAY[40,2]) <> 42 THEN
        RAISE EXCEPTION 'default scalar invocation failed';
    END IF;

    PERFORM set_config('pgwasm.fuel_enabled', 'on', true);
    PERFORM set_config('pgwasm.fuel_per_invocation', '100000', true);
    IF pgwasm.pgwasm_core_invoke_scalar(decode(wasm_hex, 'hex'), 'add', ARRAY[-7,7]) <> 0 THEN
        RAISE EXCEPTION 'finite-fuel scalar invocation failed';
    END IF;
    PERFORM set_config('pgwasm.fuel_per_invocation', '1', true);
    rejected := false;
    BEGIN
        PERFORM pgwasm.pgwasm_core_invoke_scalar(decode(wasm_hex, 'hex'), 'add', ARRAY[40,2]);
    EXCEPTION WHEN program_limit_exceeded THEN
        IF position('fuel exhausted' in SQLERRM) = 0 THEN
            RAISE;
        END IF;
        rejected := true;
    END;
    IF NOT rejected THEN
        RAISE EXCEPTION 'finite fuel budget was not enforced';
    END IF;
    PERFORM set_config('pgwasm.fuel_per_invocation', '100000000', true);

    FOREACH options IN ARRAY options_list LOOP
        i := i + 1;
        v_module_name := 'runtime_defaults_' || i;
        PERFORM set_config('pgwasm.fuel_enabled', CASE WHEN i <= 3 THEN 'off' ELSE 'on' END, true);
        IF pgwasm.pgwasm_load(v_module_name, json_build_object('bytes', wasm_hex), options) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'load failed for options %', options;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM pgwasm.pgwasm_modules() m WHERE m.name = v_module_name) THEN
            RAISE EXCEPTION 'loaded module is absent from catalog';
        END IF;
        EXECUTE 'SELECT pgwasm.core__add(20,22)' INTO answer;
        IF answer IS DISTINCT FROM 42 THEN
            RAISE EXCEPTION 'registered invocation failed for options %', options;
        END IF;
        SELECT (p.limits_json->>'max_memory_pages')::integer INTO answer
        FROM pgwasm.pgwasm_policy_effective() p WHERE p.module_name = v_module_name;
        IF answer IS DISTINCT FROM (CASE WHEN i <= 3 THEN 1024 ELSE 512 END) THEN
            RAISE EXCEPTION 'optional limits did not inherit or override the GUC ceiling';
        END IF;

        rejected := false;
        BEGIN
            PERFORM pgwasm.pgwasm_load(v_module_name, json_build_object('bytes', wasm_hex), options);
        EXCEPTION WHEN invalid_parameter_value THEN
            IF position('already exists in catalog' in SQLERRM) = 0 THEN
                RAISE;
            END IF;
            rejected := true;
        END;
        IF NOT rejected THEN
            RAISE EXCEPTION 'duplicate module load was accepted';
        END IF;
        IF pgwasm.pgwasm_reconfigure(v_module_name, NULL, '{"max_memory_pages":256}'::json) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'partial reconfiguration failed';
        END IF;
        SELECT (p.limits_json->>'max_memory_pages')::integer INTO answer
        FROM pgwasm.pgwasm_policy_effective() p WHERE p.module_name = v_module_name;
        IF answer IS DISTINCT FROM 256 THEN
            RAISE EXCEPTION 'reconfiguration did not apply the memory-page limit';
        END IF;
        EXECUTE 'SELECT pgwasm.core__add(40,2)' INTO answer;
        IF answer IS DISTINCT FROM 42 THEN
            RAISE EXCEPTION 'invocation after reconfiguration failed';
        END IF;
        IF pgwasm.pgwasm_reload(v_module_name, json_build_object('bytes', wasm_hex)) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'reload with inherited options failed';
        END IF;
        SELECT (p.limits_json->>'max_memory_pages')::integer INTO answer
        FROM pgwasm.pgwasm_policy_effective() p WHERE p.module_name = v_module_name;
        IF answer IS DISTINCT FROM 256 THEN
            RAISE EXCEPTION 'reload did not preserve the memory-page limit';
        END IF;
        EXECUTE 'SELECT pgwasm.core__add(42,0)' INTO answer;
        IF answer IS DISTINCT FROM 42 THEN
            RAISE EXCEPTION 'invocation after reload failed';
        END IF;
        IF pgwasm.pgwasm_unload(v_module_name, false) IS DISTINCT FROM true THEN
            RAISE EXCEPTION 'unload failed';
        END IF;
        IF EXISTS (SELECT 1 FROM pgwasm.pgwasm_modules() m WHERE m.name = v_module_name)
           OR to_regprocedure('pgwasm.core__add(integer,integer)') IS NOT NULL THEN
            RAISE EXCEPTION 'unload left a catalog row or registered function';
        END IF;
    END LOOP;
END
$test$;
