# WIT → PostgreSQL type mapping

This page is the canonical reference for how `pgwasm` maps
[WIT](https://component-model.bytecodealliance.org/design/wit.html) types
to PostgreSQL types. The mapping is implemented in `pgwasm/src/wit/typing.rs`
and registered via `pgwasm/src/wit/udt.rs`.

For architecture context, see
[`docs/architecture.md` §8](architecture.md#8-type-mapping-and-udt-registration).
For GUCs, see [`docs/guc.md`](guc.md).

## Naming conventions

- **SQL functions** — For a module loaded with `module_name => 'ex'`, each export
  becomes `pgwasm."ex__<sanitized_key>"` where `<sanitized_key>` comes from the WIT
  export path (`/` and `-` become `_`). Example: export `add-s32` → `ex__add_s32`.
- **SQL types** — Registered types live in the extension schema as
  `pgwasm.m<module_id>_<suffix>` where `<suffix>` is a sanitized WIT name or domain
  alias (`wit::udt::type_sql_ident`). The catalog table `pgwasm.wit_types.wit_name`
  stores the stable **type key** (`package:interface/name`) used across reloads;
  `pgwasm.pgwasm_wit_types()` exposes it as `module_name::<wit_name>`.

## Summary

| WIT type | PostgreSQL representation |
|----------|---------------------------|
| `bool` | `boolean` |
| `s8`, `s16` | `smallint` |
| `s32` | `integer` |
| `s64` | `bigint` |
| `u8` | `smallint` domain with `CHECK (VALUE BETWEEN 0 AND 255)` |
| `u16` | `integer` domain with `CHECK (VALUE BETWEEN 0 AND 65535)` |
| `u32` | `bigint` domain with full `u32` range check |
| `u64` | `numeric` domain with `0 .. 2^64-1` check |
| `f32`, `f64` | `real`, `double precision` |
| `char` | `text` domain enforcing a single Unicode character (`char_length = 1`) |
| `string`, `error-context` | `text` |
| `list<u8>` | `bytea` |
| `list<T>` | `NOT NULL` array domain over the mapping of `T` |
| `option<T>` | PostgreSQL type for `T`, nullable at the function boundary |
| `result<T, E>` | Composite `(ok, err)`; missing arms use internal `void` typing rules in `wit::udt` |
| `tuple<…>` | Composite with fields `f0`, `f1`, … |
| `record { … }` | Composite with WIT field names |
| `variant { … }` | Composite `(discriminant text, payload jsonb)` |
| `enum { … }` | PostgreSQL `ENUM` |
| `flags { … }` | `integer` domain; `CHECK` bounds `0 .. (1 << n) - 1` for `n` flags |
| `resource`, `borrow<T>`, `own<T>` | `bigint` |
| `map`, fixed-size list, `future`, `stream` | `jsonb` domain (named `*_json`) |

**Limitation:** nested user-defined composites inside record fields are not yet
supported for DDL (`wit::udt::pg_type_sql` returns `Unsupported` for composite /
enum / variant field types). Prefer scalars, domains, and arrays of scalars inside
records until that path is wired.

## 1. Primitives

### 1.1 `bool`

```wit
export is-even: func(n: s32) -> bool;
```

```sql
-- CREATE FUNCTION ex__is_even(n integer) RETURNS boolean ...
SELECT ex__is_even(4);  -- t
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
SELECT ex__add_s32(1::int, 2::int);
SELECT ex__add_s64(1::bigint, 2::bigint);
```

### 1.3 Unsigned integers

Domains use the `m<module_id>_u8` / `_u16` / `_u32` / `_u64` suffix pattern.

```wit
export next: func(n: u32) -> u32;
```

```sql
-- CREATE DOMAIN pgwasm.m42_u32 AS bigint CHECK (VALUE >= 0 AND VALUE <= 4294967295);
-- CREATE FUNCTION ex__next(n pgwasm.m42_u32) RETURNS pgwasm.m42_u32 ...
```

### 1.4 Floats

```wit
export hypot: func(x: f64, y: f64) -> f64;
```

```sql
SELECT ex__hypot(3::float8, 4::float8);  -- 5
```

### 1.5 `char`

Mapped as a `text` domain with `char_length(VALUE) = 1`.

### 1.6 `string` and `list<u8>`

```wit
export upper: func(s: string) -> string;
export hash:  func(bytes: list<u8>) -> list<u8>;
```

```sql
SELECT ex__upper('hello');
SELECT encode(ex__hash('\x00ff'), 'hex');
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

Composite type name example: `pgwasm.m42_point` (exact suffix depends on WIT name
and module id).

```sql
SELECT ex__midpoint(ROW(0, 0)::pgwasm.m42_point,
                    ROW(2, 4)::pgwasm.m42_point);
```

### 2.2 `tuple`

Tuples use composite fields `f0`, `f1`, …

```wit
export split: func(s: string) -> tuple<string, string>;
```

```sql
-- CREATE TYPE pgwasm.m42_tuple_... AS (f0 text, f1 text);
SELECT * FROM ex__split('a=b');
--  f0 | f1
-- ----+----
--  a  | b
```

### 2.3 `variant`

Variants are stored as `(discriminant text, payload jsonb)` so PostgreSQL never
needs nullable per-case columns.

```wit
variant shape {
    circle(f64),
    rectangle(tuple<f64, f64>),
    unit,
}

export area: func(s: shape) -> f64;
```

```sql
-- CREATE TYPE pgwasm.m42_shape AS (discriminant text, payload jsonb);
-- Example row: discriminant 'circle', payload '3.0' (json number) for radius
SELECT ex__area(ROW('circle', '3.0'::jsonb)::pgwasm.m42_shape);
```

### 2.4 `enum`

```wit
enum color { red, green, blue }

export name-of: func(c: color) -> string;
```

```sql
-- CREATE TYPE pgwasm.m42_color AS ENUM ('red', 'green', 'blue');
SELECT ex__name_of('green'::pgwasm.m42_color);
```

### 2.5 `flags`

```wit
flags permissions { read, write, execute }

export mask: func(p: permissions) -> permissions;
```

```sql
-- CREATE DOMAIN pgwasm.m42_flags_permissions AS integer CHECK (VALUE >= 0 AND VALUE <= 7);
SELECT ex__mask(5::pgwasm.m42_flags_permissions);
```

## 3. Generics

### 3.1 `option<T>`

The return type is the PostgreSQL mapping of `T`, nullable.

```wit
export find: func(key: string) -> option<s64>;
```

```sql
-- RETURNS bigint (nullable)
SELECT ex__find('missing') IS NULL AS is_none;
```

### 3.2 `result<T, E>`

Composite with `ok` and `err` attributes.

```wit
export parse-int: func(s: string) -> result<s64, string>;
```

```sql
SELECT ex__parse_int('42');
SELECT ex__parse_int('oops');
```

### 3.3 `list<T>`

Homogeneous lists become PostgreSQL arrays (via a `NOT NULL` array domain).

```wit
export sum-i32: func(xs: list<s32>) -> s32;
export names:   func() -> list<string>;
```

```sql
SELECT ex__sum_i32(ARRAY[1, 2, 3]);
SELECT unnest(ex__names());
```

## 4. Resources and handles

WIT `resource` types and handles map to `bigint` in SQL. Constructor / method exports
use Wasm component naming (`Type#method`, etc.); the SQL identifier is still
`<module>__<sanitized export path>` — inspect `pgwasm.pgwasm_functions()` after load
for the exact `fn_oid` and argument lists.

## 5. Escape hatch: `pgwasm:host/json`

For shapes that are intentionally loose or not covered above, a component may import
`pgwasm:host/json` and exchange `jsonb` values directly.
