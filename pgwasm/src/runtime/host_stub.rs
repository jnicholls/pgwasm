//! Host-side stub for `runtime::host` used by plain `cargo test`.
//!
//! This module intentionally avoids importing Postgres/pgrx backend symbols so
//! host test binaries do not attempt to link against backend-only symbols.

use wasmtime::component::Linker;

use crate::errors::PgWasmError;
use crate::policy::EffectivePolicy;
use crate::runtime::component::StoreCtx;

pub(crate) fn add_to_linker(
    linker: &mut Linker<StoreCtx>,
    policy: &EffectivePolicy,
) -> Result<(), PgWasmError> {
    let _ = (linker, policy);
    Ok(())
}
