//! Error types and conversion helpers used across pg_wasm.

use std::io;

use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;

#[allow(dead_code)]
pub(crate) type Result<T> = core::result::Result<T, PgWasmError>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub(crate) enum PgWasmError {
    #[error("pg_wasm is disabled")]
    Disabled,
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
}

#[allow(dead_code)]
impl PgWasmError {
    pub(crate) const fn sqlstate(&self) -> PgSqlErrorCode {
        match self {
            Self::Disabled => PgSqlErrorCode::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
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
        }
    }
}
