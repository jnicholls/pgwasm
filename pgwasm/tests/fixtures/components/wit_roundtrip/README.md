# `wit_roundtrip` fixture

World exports scalar round-trip functions: `echo-bool`, `echo-s32`, `echo-s64`, `echo-string`.

These match the subset of WIT types that `pgwasm` currently registers as SQL exports without extra scalar plumbing.

## Rebuild

```bash
cd "$(dirname "$0")/.." && ./build_all.sh
```
