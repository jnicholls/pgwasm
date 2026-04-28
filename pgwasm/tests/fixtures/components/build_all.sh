#!/usr/bin/env bash
# Build every component fixture (wasm32-wasip2 + release) and emit one-line hex
# files under this directory's `hex/` for embedding in pg_regress SQL.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT}"
rustup target add wasm32-wasip2 >/dev/null 2>&1 || true
for crate in arith strings records enums variants hooks policy_probe resources trap wit_roundtrip; do
  echo "building ${crate}..."
  if [[ "${crate}" == "strings" || "${crate}" == "resources" || "${crate}" == "trap" ]]; then
    target="wasm32-unknown-unknown"
  else
    target="wasm32-wasip2"
  fi
  (cd "${crate}" && cargo build --release --target "${target}")
  wasm="${crate}/target/${target}/release/${crate}.wasm"
  if [[ ! -f "${wasm}" ]]; then
    echo "error: wasm not found for ${crate} at ${wasm}" >&2
    exit 1
  fi
  wasm-tools validate "${wasm}"
  cp -f "${wasm}" "${crate}/component.wasm"
  echo "  -> ${crate}/component.wasm ($(wc -c <"${wasm}") bytes)"
done
