//! Error types and conversion helpers used across pg_wasm.

use std::io;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;
use wasmtime::Trap;

pub(crate) type Result<T> = core::result::Result<T, PgWasmError>;

pub(crate) fn map_wasmtime_err(e: wasmtime::Error) -> PgWasmError {
    if let Some(trap) = e.downcast_ref::<Trap>() {
        return match trap {
            Trap::Interrupt => {
                PgWasmError::Timeout("invocation interrupted by epoch deadline".to_string())
            }
            Trap::OutOfFuel => PgWasmError::ResourceLimitExceeded("fuel exhausted".to_string()),
            other => PgWasmError::Trap {
                kind: format!("{other}"),
            },
        };
    }
    PgWasmError::Internal(format!("{e:#}"))
}

#[derive(Debug, Error)]
pub(crate) enum PgWasmError {
    #[error("pg_wasm is disabled")]
    Disabled,
    #[error("reload rejected a breaking change")]
    BreakingChangeReload { detail: String, hint: String },
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid WebAssembly module: {0}")]
    InvalidModule(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
    #[error("invocation timed out: {0}")]
    Timeout(String),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("feature not supported: {0}")]
    Unsupported(String),
    #[error("internal pg_wasm error: {0}")]
    Internal(String),
    #[error("validation failed: {0}")]
    ValidationFailed(String),
    #[error("WebAssembly trap: {kind}")]
    Trap { kind: String },
}

impl PgWasmError {
    pub(crate) fn into_error_report(self) -> ErrorReport {
        match self {
            Self::BreakingChangeReload { detail, hint } => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!("invalid configuration: reload rejected: {detail} — {hint}"),
                "pg_wasm",
            ),
            Self::Trap { kind } => ErrorReport::new(
                PgSqlErrorCode::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,
                "WebAssembly trap".to_string(),
                "pg_wasm",
            )
            .set_detail(kind),
            other => ErrorReport::new(other.sqlstate(), other.to_string(), "pg_wasm"),
        }
    }

    pub(crate) const fn sqlstate(&self) -> PgSqlErrorCode {
        match self {
            Self::Disabled => PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
            Self::BreakingChangeReload { .. } => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            Self::PermissionDenied(_) => PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            Self::InvalidConfiguration(_) => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
            Self::InvalidModule(_) => PgSqlErrorCode::ERRCODE_INVALID_BINARY_REPRESENTATION,
            Self::NotFound(_) => PgSqlErrorCode::ERRCODE_UNDEFINED_OBJECT,
            Self::ResourceLimitExceeded(_) => PgSqlErrorCode::ERRCODE_PROGRAM_LIMIT_EXCEEDED,
            Self::Timeout(_) => PgSqlErrorCode::ERRCODE_QUERY_CANCELED,
            Self::Io(_) => PgSqlErrorCode::ERRCODE_IO_ERROR,
            Self::Unsupported(_) => PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
            Self::Internal(_) => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
            Self::ValidationFailed(_) => PgSqlErrorCode::ERRCODE_INVALID_BINARY_REPRESENTATION,
            Self::Trap { .. } => PgSqlErrorCode::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION,
        }
    }
}
