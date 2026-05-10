# Full bidirectional WIT <-> Postgres type conversion support

## Goal

Complete end-to-end conversion for all stable WIT value types between PostgreSQL SQL types and
`wasmtime::component::Val`, covering:

- WIT type planning and UDT registration.
- SQL export registration.
- SQL argument/result marshaling in both directions.
- Host-query value conversion where relevant.
- Regression, pg_test, and integration coverage for every supported shape.

The final state should make unsupported WIT types explicit and rare: only `unknown` should be
rejected as invalid input, and async/resource-lifecycle semantics should be documented if they
cannot be represented as plain SQL values.

## Current limitations to remove

- `wit::typing` maps many WIT types to `PgType`, but runtime marshaling supports only a subset.
- Automatic export registration accepts only direct `bool`, `s32`, `s64`, `string`, or registered
  named WIT types.
- The trampoline reconstructs marshal plans from PostgreSQL OIDs, losing WIT shape information for
  domains, arrays, composites, options, variants, results, and aliases.
- `mapping::composite` supports only a narrow scalar set and partial composite/list handling.
- `mapping::list` is currently specialized around `bytea` and `int4[]`; `int8[]` read support is
  present through an `int4[]` path and write support is missing.
- UDT registration does not wire nested composite/enum/variant fields.
- Variant UDT DDL uses `payload jsonb`, while runtime variant marshaling expects typed payloads.
- WIT `map`, `fixed-length-list`, `future`, and `stream` are mapped to JSONB domains but do not
  have `Val` marshaling.

## Type support matrix

### Scalars

Implement direct bidirectional support for:

- `bool` <-> `boolean`
- `s8` <-> checked `int2` domain or `int2`
- `u8` <-> checked `int2` domain
- `s16` <-> `int2`
- `u16` <-> checked `int4` domain, replacing the current lossy `int2` mapping
- `s32` <-> `int4`
- `u32` <-> checked `int8` domain
- `s64` <-> `int8`
- `u64` <-> checked `numeric` domain
- `f32` <-> `real`
- `f64` <-> `double precision`
- `char` <-> checked single-character `text` domain, not PostgreSQL internal `"char"`
- `string` and `error-context` <-> `text`

Add range checks on SQL->WIT and WIT->SQL for every narrowed or unsigned mapping.

### Named and structural types

Implement bidirectional support for:

- `type` aliases, preserving the aliased marshaling shape.
- `option<T>` as nullable SQL value for top-level and nested fields.
- `result<ok, err>` as a composite with mutually exclusive nullable `ok` and `err` fields.
- `record` as a registered composite.
- `tuple` as a registered composite with deterministic field names.
- `variant` as a registered composite with `discriminant text` plus a payload representation that
  can carry every supported payload shape.
- `enum` as a PostgreSQL enum.
- `flags` as an integer domain with bit validation.
- `list<T>` as arrays for scalar/composite/enum/domain element types where PostgreSQL supports an
  array type, and `list<u8>` as `bytea`.
- `fixed-length-list<T, N>` as an array plus length check, or as a fixed-field composite when the
  element type cannot be represented as a PostgreSQL array.
- `map<K, V>` as `jsonb` with typed conversion rules for keys and values.

### Resources, handles, futures, and streams

- Keep `resource`, `own<T>`, and `borrow<T>` as opaque `int8` handles only if handle lifecycle is
  intentionally outside SQL conversion. Add explicit validation and documentation.
- Represent `future<T>` and `stream<T>` as `jsonb` only if the project chooses snapshot semantics.
  Otherwise reject them at load time with a precise unsupported error instead of registering a type
  that cannot marshal.

## Architecture changes

### 1. Preserve WIT shape for exports

Store a normalized, machine-readable export type plan in `catalog.exports.signature`, not just
debug strings. The trampoline should reconstruct marshal plans from this WIT-derived signature and
the registered `wit_types` catalog rows instead of inferring shape only from PostgreSQL OIDs.

Deliverables:

- Add serializable `TypeShape` / `ExportShape` structs.
- Persist parameter and result shapes during load and reload.
- Version the signature JSON for future migrations.
- Keep compatibility checks based on normalized shape rather than `Debug` output.

### 2. Make `PgType` carry enough information

Extend `PgType` so every runtime decision has:

- The WIT kind.
- PostgreSQL type OID or lookup key.
- Domain base type and range constraints.
- Composite field/case/element shapes.
- Nullability semantics for options.

Avoid lossy conversions such as mapping `option<T>` directly to `T` without preserving option
metadata.

### 3. Complete UDT registration

Register UDTs in dependency order for nested:

- Composite fields referencing composites, enums, variants, domains, and arrays.
- Variant payloads.
- Result composites.
- Fixed-length list representations.

Deliverables:

- Resolve registered type OIDs by type key during DDL generation.
- Emit deterministic `CREATE TYPE`, `CREATE DOMAIN`, and `ALTER TYPE` statements.
- Decide and enforce migration rules for each WIT kind.
- Add explicit DDL rollback behavior for partial failures.

### 4. Replace ad hoc array/list conversion

Use PostgreSQL array APIs rather than CSV string conversion. Support:

- Empty arrays.
- NULL array value as `option<list<T>>` only, not as empty list.
- NULL elements only when the WIT element type is `option<T>`.
- `list<u8>` bytea fast path.
- All scalar, enum, domain, and registered composite element types with array OIDs.

### 5. Complete scalar/domain marshaling

Add scalar marshaling helpers for every WIT scalar and domain. Each helper should:

- Validate SQL->WIT range and shape.
- Validate WIT->SQL range and shape.
- Preserve signedness.
- Handle NaN/Infinity behavior intentionally for floats.
- Reject invalid Unicode scalar values for `char`.

### 6. Define JSONB conversion rules

If `map`, `future`, or `stream` remain JSONB-backed, define canonical JSON:

- Stable object encoding for maps.
- String encoding for non-string map keys.
- Explicit `future`/`stream` snapshot schema, or load-time rejection.
- Round-trip tests for nested values.

### 7. Host query conversions

Extend `HostQuerySpiParam` and host query result conversion only after UDF conversion semantics are
settled. Keep host query values aligned with the same scalar/list/json rules where possible.

## Implementation phases

1. Add shape persistence without changing behavior.
2. Expand scalar/domain marshaling and export registration for all scalar WIT types.
3. Fix options, null handling, and result composites.
4. Replace list/array conversion and complete list element support.
5. Wire nested UDT registration and composite/tuple/record marshaling through catalog OIDs.
6. Redesign variant payload storage and marshaling.
7. Decide JSONB-backed semantics or load-time rejection for map/fixed-list/future/stream.
8. Add host-query parity where the host interface exposes matching value kinds.
9. Update user docs with the final support matrix and examples.

Each phase should keep existing supported modules loadable unless a stored signature version bump
requires an explicit migration path.

## Test plan

Add tests in layers:

- Host unit tests for pure type planning, shape serialization, range checks, and migration
  compatibility checks.
- `#[pg_test]` tests for datum <-> `Val` round trips on every scalar, domain, list, composite,
  enum, flags, option, result, tuple, and variant shape.
- SQL regress tests for stable DDL emitted by UDT registration and catalog rows.
- Integration tests that load real components and call exported SQL functions for every WIT kind.
- Negative tests for out-of-range unsigned values, invalid `char`, invalid enum labels, unknown
  variant cases, nulls where WIT does not allow them, and unsupported async/resource cases.

Completion criteria:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace -- -D warnings`
- `cargo test --workspace`
- `cargo pgrx test pg17 -p pgwasm`
- `cargo pgrx regress pg17 --resetdb -p pgwasm --features pg_test`
- Integration suite for ignored component round-trip tests with PostgreSQL 17.

## Open decisions

- Whether `future<T>` and `stream<T>` should be load-time unsupported or represented as JSONB
  snapshots.
- Whether `resource` and handles should remain opaque `int8` values or require a catalog-backed
  handle table.
- Whether `char` should use a reusable module domain or a shared extension domain.
- Whether `list<T>` for composite elements should use PostgreSQL arrays of registered composites or
  JSONB arrays for simpler evolution.
- Whether variants should use one payload column per case, `jsonb`, or a generated union-like
  payload composite.
