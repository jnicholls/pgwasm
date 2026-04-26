//! Error types and conversion helpers used across pg_wasm.

use std::io;

use pgrx::pg_sys::panic::ErrorReport;
use pgrx::prelude::PgLogLevel;
use pgrx::prelude::PgSqlErrorCode;
use thiserror::Error;
use wasmtime::Trap;

/// Wasmtime version pinned by the workspace (see `Cargo.toml` / `AGENTS.md`).
pub(crate) const DEFAULT_WASMTIME_VERSION: &str = "43.0.0";

pub(crate) type Result<T> = core::result::Result<T, PgWasmError>;

/// Structured context attached to SQL `DETAIL` when reporting [`PgWasmError`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrorContext {
    pub export_index: Option<u32>,
    pub module_id: Option<u64>,
    pub wasmtime_version: &'static str,
}

impl Default for ErrorContext {
    fn default() -> Self {
        Self {
            export_index: None,
            module_id: None,
            wasmtime_version: DEFAULT_WASMTIME_VERSION,
        }
    }
}

impl ErrorContext {
    pub(crate) fn format_detail(self) -> String {
        let module_id = option_u64_display(self.module_id);
        let export_index = option_u32_display(self.export_index);
        format!(
            "module_id={module_id}, export_index={export_index}, wasmtime={}",
            self.wasmtime_version
        )
    }
}

fn option_u64_display(v: Option<u64>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "none".to_string(),
    }
}

fn option_u32_display(v: Option<u32>) -> String {
    match v {
        Some(n) => n.to_string(),
        None => "none".to_string(),
    }
}

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
    #[error("invalid configuration: reload rejected: {detail} — {hint}")]
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
    /// Catalog already contains this module name (see [`crate::lifecycle::load::load_impl`]).
    #[error("invalid configuration: {msg}")]
    ModuleAlreadyLoaded { module_id: i64, msg: String },
}

impl PgWasmError {
    /// Policy / operator hints for variants where a concrete GUC or option exists.
    pub(crate) fn hint(&self) -> Option<&'static str> {
        match self {
            Self::PermissionDenied(_) => Some(
                "Check extension GUCs (e.g. pg_wasm.allow_load_from_file, pg_wasm.allow_spi, pg_wasm.allow_wasi*) and module policy JSON; overrides may only narrow defaults.",
            ),
            _ => None,
        }
    }

    fn effective_context(&self, ctx: ErrorContext) -> ErrorContext {
        let mut out = ctx;
        if let Self::ModuleAlreadyLoaded { module_id, .. } = self
            && let Ok(u) = u64::try_from(*module_id)
        {
            out.module_id = Some(u);
        }
        out
    }

    fn detail_body(&self, ctx: &ErrorContext) -> String {
        let ctx = self.effective_context(*ctx);
        let structural = ctx.format_detail();
        match self {
            Self::BreakingChangeReload { .. } => structural,
            Self::Trap { kind } => format!("{structural}\ntrap: {kind}"),
            _ => structural,
        }
    }

    fn hint_body_owned(&self) -> Option<String> {
        match self {
            Self::BreakingChangeReload { hint, .. } => Some(hint.clone()),
            _ => self.hint().map(str::to_string),
        }
    }

    /// Convert to a pgrx [`ErrorReport`] for `#[pg_extern]` `Result<_, ErrorReport>` surfaces.
    pub(crate) fn into_error_report(self) -> ErrorReport {
        let ctx = ErrorContext::default();
        let sqlstate = self.sqlstate();
        let message = self.to_string();
        let detail = self.detail_body(&ctx);
        let mut report = ErrorReport::new(sqlstate, message, "pg_wasm").set_detail(detail);
        if let Some(h) = self.hint_body_owned() {
            report = report.set_hint(h);
        }
        report
    }

    /// Report this error at `ERROR` and do not return.
    pub(crate) fn report(self, ctx: ErrorContext) -> ! {
        let sqlstate = self.sqlstate();
        let message = self.to_string();
        let detail = self.detail_body(&ctx);
        let mut report = ErrorReport::new(sqlstate, message, "pg_wasm").set_detail(detail);
        if let Some(h) = self.hint_body_owned() {
            report = report.set_hint(h);
        }
        report.report(PgLogLevel::ERROR);
        // SAFETY: `report(ERROR)` does not return to the caller.
        unsafe { std::hint::unreachable_unchecked() }
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
            Self::ModuleAlreadyLoaded { .. } => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
        }
    }
}

/// Unwrap a [`Result`] or report [`PgWasmError`] to PostgreSQL and abort the call.
pub(crate) trait IntoReport {
    type Ok;

    fn or_report(self, ctx: ErrorContext) -> Self::Ok;
}

impl<T> IntoReport for Result<T> {
    type Ok = T;

    fn or_report(self, ctx: ErrorContext) -> T {
        match self {
            Ok(value) => value,
            Err(err) => err.report(ctx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ErrorContext, PgWasmError};

    #[test]
    fn hint_is_some_only_for_permission_denied_in_current_taxonomy() {
        let cases: Vec<(PgWasmError, bool)> = vec![
            (PgWasmError::Disabled, false),
            (
                PgWasmError::BreakingChangeReload {
                    detail: "d".to_string(),
                    hint: "h".to_string(),
                },
                false,
            ),
            (PgWasmError::PermissionDenied("no".to_string()), true),
            (PgWasmError::InvalidConfiguration("x".to_string()), false),
            (PgWasmError::InvalidModule("x".to_string()), false),
            (PgWasmError::NotFound("x".to_string()), false),
            (PgWasmError::ResourceLimitExceeded("x".to_string()), false),
            (PgWasmError::Timeout("x".to_string()), false),
            (PgWasmError::Unsupported("x".to_string()), false),
            (PgWasmError::Internal("x".to_string()), false),
            (PgWasmError::ValidationFailed("x".to_string()), false),
            (
                PgWasmError::Trap {
                    kind: "k".to_string(),
                },
                false,
            ),
            (
                PgWasmError::ModuleAlreadyLoaded {
                    module_id: 1,
                    msg: "x".to_string(),
                },
                false,
            ),
        ];
        for (err, expect_some) in cases {
            assert_eq!(
                err.hint().is_some(),
                expect_some,
                "hint presence mismatch for {err:?}"
            );
        }
    }

    #[test]
    fn error_context_detail_includes_module_id_when_set() {
        let ctx = ErrorContext {
            export_index: Some(2),
            module_id: Some(42),
            wasmtime_version: super::DEFAULT_WASMTIME_VERSION,
        };
        let s = ctx.format_detail();
        assert!(s.contains("module_id=42"), "{s}");
        assert!(s.contains("export_index=2"), "{s}");
        assert!(s.contains("wasmtime=43.0.0"), "{s}");
    }
}
