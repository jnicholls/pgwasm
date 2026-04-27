//! Workspace integration tests for `pgwasm` (tokio-postgres client, no pgrx).
//!
//! Tests are `#[ignore]` by default so `cargo test --workspace` succeeds without a live
//! Postgres. Run against pgrx-managed Postgres with `DATABASE_URL=... cargo test -p tests -- --ignored`
//! (see `tests/README.md`).

#![allow(clippy::unwrap_used)]

#[cfg(test)]
mod backend_restart;
#[cfg(test)]
mod common;
#[cfg(test)]
mod concurrent_generation;
#[cfg(test)]
mod fuel_exhaustion;
#[cfg(test)]
mod memory_pages;
#[cfg(test)]
mod query_cancellation;
#[cfg(test)]
mod wasi_http_search;
#[cfg(test)]
mod wasi_policy_denial;
