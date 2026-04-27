#!/usr/bin/env python3
"""Generate pg_regress SQL suites with embedded wasm byte payloads (int[] JSON style).

Run after `./build_all.sh`:

  python3 pgwasm/fixtures/components/generate_pg_regress_sql.py
"""

from __future__ import annotations

import pathlib
import textwrap


def wasm_to_pg_int_array_literal(data: bytes) -> str:
    parts = [str(b) for b in data]
    line: list[str] = []
    lines: list[str] = []
    width = 0
    for p in parts:
        chunk = p if not line else f", {p}"
        if width + len(chunk) > 100 and line:
            lines.append(", ".join(line))
            line = [p]
            width = len(p)
        else:
            line.append(p)
            width += len(chunk)
    if line:
        lines.append(", ".join(line))
    return ",\n                ".join(lines)


def read_wasm(name: str, root: pathlib.Path) -> bytes:
    p = root / name / "component.wasm"
    if not p.is_file():
        raise SystemExit(f"missing wasm: {p}")
    return p.read_bytes()


def main() -> None:
    root = pathlib.Path(__file__).resolve().parent
    out_sql = root.parent.parent / "tests" / "pg_regress" / "sql"

    arith = read_wasm("arith", root)
    hooks_wasm = read_wasm("hooks", root)
    policy_probe = read_wasm("policy_probe", root)
    resources = read_wasm("resources", root)
    trap_wasm = read_wasm("trap", root)
    wit_rt = read_wasm("wit_roundtrip", root)

    def payload(data: bytes) -> str:
        return textwrap.indent(
            "to_json(\n            ARRAY[\n                "
            + wasm_to_pg_int_array_literal(data)
            + "\n            ]::int[]\n        )",
            "        ",
        )

    # Trampoline reads module limits from catalog JSON; every key must be a JSON integer (not null).
    opt_limits_default = """json_build_object(
            'limits', json_build_object(
                'fuel_per_invocation', 100000000,
                'instances_per_module', 1,
                'invocation_deadline_ms', 5000,
                'max_memory_pages', 1024
            )
        )"""

    lifecycle_sql = textwrap.dedent(
        f"""\
        -- Lifecycle: load, unload, reload, reconfigure; transaction rollback on load.
        -- Extension is created by tests/pg_regress/sql/setup.sql.
        SELECT set_config('pgwasm.fuel_enabled', 'off', false);
        SELECT set_config('pgwasm.invocation_deadline_ms', '0', false);

        DO $lc$
        DECLARE
            n text;
        BEGIN
            FOREACH n IN ARRAY ARRAY[
                'lc_arith',
                'lc_hooks',
                'lc_policy',
                'lc_resources',
                'lc_reload_src',
                'lc_rb'
            ]
            LOOP
                BEGIN
                    IF EXISTS (SELECT 1 FROM wasm.modules WHERE name = n) THEN
                        PERFORM wasm.unload(n, true);
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        BEGIN
                            PERFORM wasm.test_force_cleanup_stuck_module(n, true);
                        EXCEPTION
                            WHEN OTHERS THEN
                                NULL;
                        END;
                END;
            END LOOP;
        END;
        $lc$;

        SELECT wasm.load(
            'lc_arith',
            json_build_object(
                'bytes',
        {payload(arith)}
            ),
            {opt_limits_default}
        ) AS loaded_arith;

        SELECT name, abi, origin
        FROM wasm.modules
        WHERE name = 'lc_arith'
        ORDER BY name;

        SELECT wasm.unload('lc_arith', false) AS unloaded_arith;

        SELECT wasm.load(
            'lc_reload_src',
            json_build_object(
                'bytes',
        {payload(arith)}
            ),
            {opt_limits_default}
        ) AS loaded_reload_src;

        CREATE TEMP TABLE lc_reload_baseline AS
        SELECT e.export_id, e.fn_oid, m.generation AS gen
        FROM wasm.exports e
        JOIN wasm.modules m ON m.module_id = e.module_id
        WHERE m.name = 'lc_reload_src';

        SELECT wasm.reload(
            'lc_reload_src',
            json_build_object(
                'bytes',
        {payload(arith)}
            ),
            {opt_limits_default}
        ) AS reloaded_same;

        SELECT
            cur.export_id = b.export_id AS export_id_preserved,
            cur.fn_oid = b.fn_oid AS fn_oid_preserved,
            cur.gen = b.gen + 1 AS generation_bumped_once
        FROM lc_reload_baseline b,
        LATERAL (
            SELECT e.export_id, e.fn_oid, m.generation AS gen
            FROM wasm.exports e
            JOIN wasm.modules m ON m.module_id = e.module_id
            WHERE m.name = 'lc_reload_src'
        ) cur;

        SELECT wasm.unload('lc_reload_src', false) AS unloaded_reload_src;

        BEGIN;
        SELECT wasm.load(
            'lc_rb',
            json_build_object(
                'bytes',
        {payload(policy_probe)}
            ),
            {opt_limits_default}
        ) AS loaded_rb;
        ROLLBACK;

        SELECT COUNT(*) AS n_after_rb
        FROM wasm.modules
        WHERE name = 'lc_rb';

        SELECT wasm.load(
            'lc_policy',
            json_build_object(
                'bytes',
        {payload(policy_probe)}
            ),
            json_build_object(
                'limits', json_build_object(
                    'fuel_per_invocation', 99999999,
                    'instances_per_module', 1,
                    'invocation_deadline_ms', 5000,
                    'max_memory_pages', 1024
                ),
                'overrides', json_build_object('allow_spi', false)
            )
        ) AS loaded_policy;

        SELECT wasm.reconfigure(
            'lc_policy',
            NULL,
            json_build_object('fuel_per_invocation', 5000)
        ) AS reconfigured_narrow;

        SELECT limits_json ->> 'fuel_per_invocation' AS fuel_after_narrow
        FROM wasm.modules
        WHERE name = 'lc_policy'
        ORDER BY name;

        DO $$
        BEGIN
            PERFORM wasm.reconfigure(
                'lc_policy',
                NULL,
                json_build_object('fuel_per_invocation', 99999999)
            );
            RAISE EXCEPTION 'expected reconfigure widen to fail';
        EXCEPTION
            WHEN insufficient_privilege THEN
                NULL;
        END
        $$;

        SELECT wasm.unload('lc_policy', false) AS unloaded_policy;

        SELECT wasm.load(
            'lc_hooks',
            json_build_object(
                'bytes',
        {payload(hooks_wasm)}
            ),
            {opt_limits_default}
        ) AS loaded_hooks;

        SELECT wasm.reconfigure('lc_hooks', NULL, NULL) AS reconfigured_hooks_ok;

        SELECT wasm.unload('lc_hooks', false) AS unloaded_hooks;

        SELECT wasm.load(
            'lc_resources',
            json_build_object(
                'bytes',
        {payload(resources)}
            ),
            {opt_limits_default}
        ) AS loaded_resources;

        SELECT export_name, fn_oid
        FROM wasm.functions()
        WHERE module_name = 'lc_resources'
        ORDER BY export_name;

        SELECT wasm.unload('lc_resources', false) AS unloaded_resources;
        """
    )

    wit_mapping_sql = textwrap.dedent(
        f"""\
        -- WIT -> SQL round-trip for exports supported by automatic registration today (bool, s32, s64, string).
        -- Additional corpus fixtures (records, enums, variants, list<u8>) live under fixtures/components/ for
        -- manual builds; extend wit_wasm_type_to_pg_oid before exercising them from regress.
        SELECT set_config('pgwasm.fuel_enabled', 'off', false);
        SELECT set_config('pgwasm.invocation_deadline_ms', '0', false);

        DO $wm$
        DECLARE
            n text;
        BEGIN
            FOREACH n IN ARRAY ARRAY['wm_wit']
            LOOP
                BEGIN
                    IF EXISTS (SELECT 1 FROM wasm.modules WHERE name = n) THEN
                        PERFORM wasm.unload(n, true);
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        BEGIN
                            PERFORM wasm.test_force_cleanup_stuck_module(n, true);
                        EXCEPTION
                            WHEN OTHERS THEN
                                NULL;
                        END;
                END;
            END LOOP;
        END;
        $wm$;

        SELECT wasm.load(
            'wm_wit',
            json_build_object(
                'bytes',
        {payload(wit_rt)}
            ),
            {opt_limits_default}
        ) AS loaded_wit;

        EXPLAIN (COSTS OFF, TIMING OFF)
        SELECT wasm.wm_wit__echo_bool(true) AS v_bool;

        SELECT wasm.wm_wit__echo_bool(false) AS v_bool_f
        ORDER BY 1;

        SELECT wasm.wm_wit__echo_s32(7) AS v_s32
        ORDER BY 1;

        SELECT wasm.wm_wit__echo_s64(9000000000::int8) AS v_s64
        ORDER BY 1;

        SELECT wasm.wm_wit__echo_string('hi'::text) AS v_string
        ORDER BY 1;

        SELECT export_name, fn_oid
        FROM wasm.functions()
        WHERE module_name = 'wm_wit'
        ORDER BY export_name;

        SELECT wasm.unload('wm_wit', false) AS unloaded_wit;
        """
    )

    policy_narrow_sql = textwrap.dedent(
        f"""\
        -- Policy narrowing permitted at load; widening denied on reconfigure.
        SELECT set_config('pgwasm.fuel_enabled', 'off', false);
        SELECT set_config('pgwasm.invocation_deadline_ms', '0', false);

        DO $pn$
        DECLARE
            n text := 'pn_mod';
        BEGIN
            IF EXISTS (SELECT 1 FROM wasm.modules WHERE name = n) THEN
                PERFORM wasm.unload(n, true);
            END IF;
        END;
        $pn$;

        SELECT wasm.load(
            'pn_mod',
            json_build_object(
                'bytes',
        {payload(policy_probe)}
            ),
            json_build_object(
                'overrides', json_build_object('allow_spi', false),
                'limits', json_build_object(
                    'fuel_per_invocation', 1000,
                    'instances_per_module', 1,
                    'invocation_deadline_ms', 5000,
                    'max_memory_pages', 1024
                )
            )
        ) AS loaded_narrow_ok;

        DO $$
        BEGIN
            PERFORM wasm.load(
                'pn_mod_wide',
                json_build_object(
                    'bytes',
        {payload(policy_probe)}
                ),
                json_build_object(
                    'overrides', json_build_object('allow_spi', true),
                    'limits', json_build_object(
                        'fuel_per_invocation', 100000000,
                        'instances_per_module', 1,
                        'invocation_deadline_ms', 5000,
                        'max_memory_pages', 1024
                    )
                )
            );
            RAISE EXCEPTION 'expected widen to fail';
        EXCEPTION
            WHEN insufficient_privilege THEN
                NULL;
        END
        $$;

        SELECT wasm.unload('pn_mod', false) AS unloaded_pn;
        """
    )

    error_classes_sql = textwrap.dedent(
        f"""\
        -- Representative SQLSTATE per pgwasm error class (see src/errors.rs).
        SELECT set_config('pgwasm.fuel_enabled', 'off', false);
        SELECT set_config('pgwasm.invocation_deadline_ms', '0', false);

        SET pgwasm.enabled = off;
        DO $$
        BEGIN
            PERFORM wasm.load(
                'ec_disabled',
                json_build_object(
                    'bytes',
        {payload(arith)}
                ),
                {opt_limits_default}
            );
            RAISE EXCEPTION 'expected disabled load';
        EXCEPTION
            WHEN object_not_in_prerequisite_state THEN
                NULL;
        END
        $$;
        RESET pgwasm.enabled;

        DO $$
        BEGIN
            PERFORM wasm.load(
                '',
                json_build_object(
                    'bytes',
        {payload(arith)}
                ),
                {opt_limits_default}
            );
            RAISE EXCEPTION 'expected invalid name';
        EXCEPTION
            WHEN invalid_parameter_value THEN
                NULL;
        END
        $$;

        DO $$
        BEGIN
            PERFORM wasm.load(
                'ec_perm',
                json_build_object('path', '/no/such/pgwasm_ec_path.wasm'),
                {opt_limits_default}
            );
            RAISE EXCEPTION 'expected perm';
        EXCEPTION
            WHEN insufficient_privilege THEN
                NULL;
        END
        $$;

        DO $$
        BEGIN
            PERFORM wasm.unload('ec_no_such_module___', false);
            RAISE EXCEPTION 'expected not found';
        EXCEPTION
            WHEN undefined_object THEN
                NULL;
        END
        $$;

        SET pgwasm.max_module_bytes = 1;
        DO $$
        BEGIN
            PERFORM wasm.load(
                'ec_limit',
                json_build_object(
                    'bytes',
        {payload(arith)}
                ),
                {opt_limits_default}
            );
            RAISE EXCEPTION 'expected limit';
        EXCEPTION
            WHEN program_limit_exceeded THEN
                NULL;
        END
        $$;
        RESET pgwasm.max_module_bytes;

        DO $$
        BEGIN
            PERFORM wasm.load(
                'ec_core',
                json_build_object('bytes', '0061736d01000000'),
                json_build_object('abi', 'core')
            );
            RAISE EXCEPTION 'expected unsupported';
        EXCEPTION
            WHEN feature_not_supported THEN
                NULL;
        END
        $$;

        DO $$
        BEGIN
            PERFORM wasm.load(
                'ec_invalid',
                json_build_object('bytes', 'ff00aa'),
                NULL
            );
            RAISE EXCEPTION 'expected invalid wasm';
        EXCEPTION
            WHEN invalid_binary_representation THEN
                NULL;
        END
        $$;

        DO $$
        BEGIN
            PERFORM wasm.load(
                'ec_policy',
                json_build_object(
                    'bytes',
        {payload(arith)}
                ),
                json_build_object(
                    'overrides', json_build_object('allow_spi', true),
                    'limits', json_build_object(
                        'fuel_per_invocation', 100000000,
                        'instances_per_module', 1,
                        'invocation_deadline_ms', 5000,
                        'max_memory_pages', 1024
                    )
                )
            );
            RAISE EXCEPTION 'expected policy deny';
        EXCEPTION
            WHEN insufficient_privilege THEN
                NULL;
        END
        $$;

        DO $trap$
        DECLARE
            n text := 'ec_trap_mod';
        BEGIN
            IF EXISTS (SELECT 1 FROM wasm.modules WHERE name = n) THEN
                PERFORM wasm.unload(n, true);
            END IF;
        END;
        $trap$;

        SELECT wasm.load(
            'ec_trap_mod',
            json_build_object(
                'bytes',
        {payload(trap_wasm)}
            ),
            {opt_limits_default}
        ) AS loaded_trap_mod;

        DO $$
        BEGIN
            PERFORM wasm.ec_trap_mod__boom();
            RAISE EXCEPTION 'expected trap';
        EXCEPTION
            WHEN external_routine_exception THEN
                NULL;
        END
        $$;

        SELECT wasm.unload('ec_trap_mod', false) AS unloaded_trap_mod;
        """
    )

    metrics_sql = textwrap.dedent(
        f"""\
        -- Monotone counters in wasm.stats() after wasm.test_bump_export_counters.
        SELECT set_config('pgwasm.fuel_enabled', 'off', false);
        SELECT set_config('pgwasm.invocation_deadline_ms', '0', false);

        DO $met$
        DECLARE
            n text := 'metrics_mod';
        BEGIN
            IF EXISTS (SELECT 1 FROM wasm.modules WHERE name = n) THEN
                PERFORM wasm.unload(n, true);
            END IF;
        END;
        $met$;

        SELECT wasm.load(
            'metrics_mod',
            json_build_object(
                'bytes',
        {payload(arith)}
            ),
            {opt_limits_default}
        ) AS loaded_metrics;

        SELECT wasm.test_bump_export_counters(m.module_id, 0, 2) AS bumped
        FROM wasm.modules m
        WHERE m.name = 'metrics_mod';

        SELECT wasm.test_bump_export_counters(m.module_id, 0, 3) AS bumped_again
        FROM wasm.modules m
        WHERE m.name = 'metrics_mod';

        SELECT export_name, invocations, traps, fuel_used_total
        FROM wasm.stats()
        WHERE module_name = 'metrics_mod'
        ORDER BY export_name;

        SELECT invocations >= 5 AS invocations_monotone
        FROM wasm.stats()
        WHERE module_name = 'metrics_mod' AND export_name = 'add';

        EXPLAIN (COSTS OFF, TIMING OFF)
        SELECT invocations FROM wasm.stats() WHERE module_name = 'metrics_mod';

        SELECT wasm.unload('metrics_mod', false) AS unloaded_metrics;
        """
    )

    out_sql.joinpath("lifecycle.sql").write_text(lifecycle_sql, encoding="utf-8")
    out_sql.joinpath("wit_mapping.sql").write_text(wit_mapping_sql, encoding="utf-8")
    out_sql.joinpath("policy_narrow.sql").write_text(policy_narrow_sql, encoding="utf-8")
    out_sql.joinpath("error_classes.sql").write_text(error_classes_sql, encoding="utf-8")
    out_sql.joinpath("metrics.sql").write_text(metrics_sql, encoding="utf-8")
    for name in ("lifecycle", "wit_mapping", "policy_narrow", "error_classes", "metrics"):
        print(f"wrote {out_sql / (name + '.sql')}")


if __name__ == "__main__":
    main()
