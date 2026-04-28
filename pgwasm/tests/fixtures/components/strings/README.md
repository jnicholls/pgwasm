# `strings` fixture

`corpus:str/bytes` exports `cat-bytes` and `len-bytes` over `list<u8>` (maps to PostgreSQL `bytea`).

Built with **`wasm32-unknown-unknown`** to avoid WASI imports in the component.

## Rebuild

```bash
cd "$(dirname "$0")/.." && ./build_all.sh
```
