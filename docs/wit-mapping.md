# WIT → PostgreSQL type mapping

This page is the canonical reference for how `pg_wasm` maps
[WIT](https://component-model.bytecodealliance.org/design/wit.html) types
to PostgreSQL types. The mapping is deterministic: the same WIT world
always produces the same PG type names (up to the `<module_prefix>`
derived from the module name), which lets `pg_wasm.reload` preserve OIDs
across code changes when the signatures are unchanged.

For the architectural rationale, see
[`docs/architecture.md` §8](architecture.md#8-type-mapping-and-udt-registration).
For the authoritative list of GUCs that gate loading and invocation, see
[`docs/guc.md`](guc.md).

All examples assume a module loaded with `name => 'ex'`, so registered
UDTs are named `ex_<kind>_<name>` and exports are `ex_<fn_name>`.

## Summary

| WIT type | PostgreSQL representation |
|----------|---------------------------|
| `bool` | `boolean` |
| `s8`, `s16`, `s32` | `smallint` / `integer` |
| `s64` | `bigint` |
| `u8`, `u16`, `u32` | `integer` (non-negative `CHECK` domain) or `bigint` when the range requires it |
| `u64` | `numeric` domain with `CHECK (x >= 0 AND x <= 18446744073709551615)` |
| `f32`, `f64` | `real`, `double precision` |
| `char` | `"char"` (single byte) when reachable, otherwise `text` |
| `string` | `text` (UTF-8) |
| `list<u8>` | `bytea` |
| `list<T>` | `T[]` when `T` is a simple scalar; otherwise a `jsonb` domain |
| `option<T>` | nullable column of the PG mapping of `T` |
| `result<T, E>` | composite `(ok T?, err E?)`, or tagged `jsonb` when `E` is a complex variant |
| `tuple<A, B, ...>` | anonymous composite registered as `pg_wasm_tuple_<hash>` |
| `record { ... }` | named composite type |
| `variant { Foo(A), Bar, ... }` | composite `(tag text, foo A, bar boolean default false)` or tagged `jsonb` if recursive |
| `enum { ... }` | PG enum |
| `flags { ... }` | `integer` domain with documented bit layout |
| `resource` | opaque `bigint` handle; borrowed vs owned enforced at marshal time |

The rest of this document expands every row with a concrete WIT fragment,
the DDL issued by `pg_wasm.load`, and a sample `SELECT`.

## 1. Primitives

### 1.1 `bool`

```wit
export is-even: func(n: s32) -> bool;
```

```sql
-- CREATE FUNCTION ex_is_even(n integer) RETURNS boolean ...
SELECT ex_is_even(4);  -- t
```

### 1.2 Signed integers

```wit
export add-s32: func(a: s32, b: s32) -> s32;
export add-s64: func(a: s64, b: s64) -> s64;
```

- `s8`, `s16` → `smallint`
- `s32` → `integer`
- `s64` → `bigint`

```sql
SELECT ex_add_s32(1::int, 2::int);     -- 3
SELECT ex_add_s64(1::bigint, 2::bigint); -- 3
```

### 1.3 Unsigned integers (domain wrappers)

Because PostgreSQL has no native unsigned integer types, `pg_wasm` wraps
unsigned WIT types in domains that enforce the non-negative range. The
domain names are derived from the module prefix:

```wit
export next: func(n: u32) -> u32;
```

```sql
-- CREATE DOMAIN ex_u32 AS integer
--   CHECK (VALUE >= 0);
-- CREATE FUNCTION ex_next(n ex_u32) RETURNS ex_u32 ...
SELECT ex_next(42::ex_u32);
```

- `u8` → `integer` domain `CHECK (VALUE BETWEEN 0 AND 255)`
- `u16` → `integer` domain `CHECK (VALUE BETWEEN 0 AND 65535)`
- `u32` → `integer` domain `CHECK (VALUE >= 0)` (widened storage)
- `u64` → `numeric` domain `CHECK (VALUE BETWEEN 0 AND 18446744073709551615)`

### 1.4 Floats

```wit
export hypot: func(x: f64, y: f64) -> f64;
```

- `f32` → `real`
- `f64` → `double precision`

```sql
SELECT ex_hypot(3::float8, 4::float8);  -- 5
```

### 1.5 `char`

`char` in WIT is a Unicode scalar value. When the reachable domain is
clearly a byte, `pg_wasm` uses PostgreSQL's `"char"` type; otherwise it
widens to `text` to preserve the full code-point range.

```wit
export initial: func(name: string) -> char;
```

```sql
SELECT ex_initial('Ada');   -- 'A'
```

### 1.6 `string` and `list<u8>`

```wit
export upper: func(s: string) -> string;
export hash:  func(bytes: list<u8>) -> list<u8>;
```

- `string` → `text` (always UTF-8)
- `list<u8>` → `bytea` (a typed byte list)

```sql
SELECT ex_upper('hello');             -- 'HELLO'
SELECT encode(ex_hash('\x00ff'), 'hex');
```

## 2. Composites

### 2.1 `record`

```wit
record point {
    x: f64,
    y: f64,
}

export midpoint: func(a: point, b: point) -> point;
```

```sql
-- CREATE TYPE ex_record_point AS (x double precision, y double precision);
-- CREATE FUNCTION ex_midpoint(a ex_record_point, b ex_record_point)
--   RETURNS ex_record_point ...;

SELECT ex_midpoint(ROW(0, 0)::ex_record_point,
                   ROW(2, 4)::ex_record_point);
--   (1,2)
```

### 2.2 `tuple`

Anonymous tuples get an auto-registered composite with a hashed name so
reload stays deterministic:

```wit
export split: func(s: string) -> tuple<string, string>;
```

```sql
-- CREATE TYPE pg_wasm_tuple_<hash> AS (field0 text, field1 text);
SELECT * FROM ex_split('a=b');
--   field0 | field1
--   -------+-------
--   a      | b
```

### 2.3 `variant`

```wit
variant shape {
    circle(f64),
    rectangle(tuple<f64, f64>),
    unit,
}

export area: func(s: shape) -> f64;
```

```sql
-- CREATE TYPE ex_variant_shape AS (
--   tag        text,
--   circle     double precision,
--   rectangle  pg_wasm_tuple_<hash>,
--   unit       boolean DEFAULT false
-- );

SELECT ex_area(ROW('circle', 3.0, NULL, false)::ex_variant_shape);
--   28.274...
```

Recursive variants (e.g. a linked-list shape) fall back to tagged
`jsonb` so PostgreSQL does not have to represent a cyclic composite.

### 2.4 `enum`

```wit
enum color { red, green, blue }

export name-of: func(c: color) -> string;
```

```sql
-- CREATE TYPE ex_enum_color AS ENUM ('red', 'green', 'blue');
SELECT ex_name_of('green'::ex_enum_color);  -- 'green'
```

### 2.5 `flags`

```wit
flags permissions { read, write, execute }

export mask: func(p: permissions) -> permissions;
```

```sql
-- CREATE DOMAIN ex_flags_permissions AS integer CHECK (VALUE >= 0);
--   bit 0 = read, bit 1 = write, bit 2 = execute

-- read | execute = 0b101 = 5
SELECT ex_mask(5::ex_flags_permissions);
```

## 3. Generics

### 3.1 `option<T>`

```wit
export find: func(key: string) -> option<s64>;
```

`option<T>` maps to a nullable column of the PG type for `T`:

```sql
-- CREATE FUNCTION ex_find(key text) RETURNS bigint ...;  -- nullable
SELECT ex_find('missing') IS NULL AS is_none;
```

### 3.2 `result<T, E>`

```wit
export parse-int: func(s: string) -> result<s64, string>;
```

```sql
-- CREATE TYPE ex_result_parse_int AS (ok bigint, err text);
SELECT ex_parse_int('42');    -- (42,)
SELECT ex_parse_int('oops');  -- (,"invalid digit")
```

When `E` is a complex variant the result is stored as tagged `jsonb`:
`{"ok": ...}` or `{"err": ...}`.

### 3.3 `list<T>`

Typed lists follow the scalar rules:

```wit
export sum-i32: func(xs: list<s32>) -> s32;
export names:   func() -> list<string>;
```

```sql
-- sum-i32: integer[] -> integer
SELECT ex_sum_i32(ARRAY[1, 2, 3]);   -- 6

-- names: text[]
SELECT unnest(ex_names());
```

For element types that are themselves composites or deep variants,
`pg_wasm` falls back to a domain over `jsonb` (documented on the module's
`pg_wasm.wit_types` row).

## 4. Resources and handles

```wit
resource counter {
    constructor(seed: s32);
    increment: func() -> s32;
    value: func() -> s32;
}

export make-counter: func(seed: s32) -> counter;
```

Handles are represented as opaque `bigint` identifiers. The distinction
between owned and borrowed handles is enforced at marshal time so
`borrow<counter>` parameters cannot outlive the call.

```sql
-- CREATE FUNCTION ex_make_counter(seed integer) RETURNS bigint ...;
-- Resource methods become free functions keyed on the handle:
-- CREATE FUNCTION ex_counter_increment(h bigint) RETURNS integer ...;
-- CREATE FUNCTION ex_counter_value(h bigint) RETURNS integer ...;

WITH c AS (SELECT ex_make_counter(10) AS h)
SELECT ex_counter_increment(h), ex_counter_increment(h), ex_counter_value(h)
FROM c;
--   11 | 12 | 12
```

Resource lifetimes follow WIT rules: the owning `bigint` is dropped when
its row is discarded (typically at statement end), which drops the
underlying resource in the component.

## 5. Deterministic naming

`pg_wasm` derives PG names as `<module_prefix>_<kind>_<wit_name>` where
`<module_prefix>` is the slugified module name (the `name` argument of
`pg_wasm.load`, or the slugified WIT world name when `name` is absent).
This guarantees:

- Two modules can register records with the same WIT name without
  colliding in the PG catalog.
- `pg_wasm.reload` can detect and preserve OIDs when the WIT definition
  is byte-for-byte identical.
- Administrators can identify the owning module from any `pg_type` row
  at a glance.

See `pg_wasm.wit_types()` for the live list of registered types.

## 6. Escape hatch: `pg_wasm:host/json`

For shapes that do not map cleanly — heavily recursive variants, open
sums, etc. — a component may import the `pg_wasm:host/json` interface
and exchange data as `jsonb`. This is what "record as JSON" meant in v1
and remains available as an explicit opt-in.
