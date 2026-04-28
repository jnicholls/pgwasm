# `resources` fixture

`corpus:res/counter` defines a resource with `constructor`, `bump`, and `peek(borrow<counter>)`.

Built with **`wasm32-unknown-unknown`** to avoid WASI imports.

## Rebuild

```bash
cd "$(dirname "$0")/.." && ./build_all.sh
```
