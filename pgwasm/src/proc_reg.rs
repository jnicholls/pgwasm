//! `pg_proc` registration and DDL generation support.

use std::ffi::{CStr, c_void};
use std::os::raw::c_char;

use pgrx::list::List;
use pgrx::memcx::current_context;
use pgrx::pg_sys;
use pgrx::pg_sys::{AsPgCStr, Datum};

use crate::errors::PgWasmError;

const EXTENSION_LIBRARY_PATH: &str = "$libdir/pgwasm";
const TRAMPOLINE_SYMBOL: &str = "pgwasm_udf_trampoline";

#[cfg(any(feature = "pg17", feature = "pg18"))]
type ProcSqlBody = *mut pg_sys::Node;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProcArgMode {
    In,
    Out,
    InOut,
    Variadic,
}

impl ProcArgMode {
    const fn to_pg_char(self) -> c_char {
        match self {
            Self::In => pg_sys::PROARGMODE_IN as c_char,
            Self::Out => pg_sys::PROARGMODE_OUT as c_char,
            Self::InOut => pg_sys::PROARGMODE_INOUT as c_char,
            Self::Variadic => pg_sys::PROARGMODE_VARIADIC as c_char,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Volatility {
    Immutable,
    Stable,
    Volatile,
}

impl Volatility {
    const fn to_pg_char(self) -> c_char {
        match self {
            Self::Immutable => pg_sys::PROVOLATILE_IMMUTABLE as c_char,
            Self::Stable => pg_sys::PROVOLATILE_STABLE as c_char,
            Self::Volatile => pg_sys::PROVOLATILE_VOLATILE as c_char,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Parallel {
    Safe,
    Restricted,
    Unsafe,
}

impl Parallel {
    const fn to_pg_char(self) -> c_char {
        match self {
            Self::Safe => pg_sys::PROPARALLEL_SAFE as c_char,
            Self::Restricted => pg_sys::PROPARALLEL_RESTRICTED as c_char,
            Self::Unsafe => pg_sys::PROPARALLEL_UNSAFE as c_char,
        }
    }
}

pub(crate) struct ProcSpec {
    pub schema: String,
    pub name: String,
    pub arg_types: Vec<pg_sys::Oid>,
    pub arg_names: Vec<String>,
    pub arg_modes: Vec<ProcArgMode>,
    pub ret_type: pg_sys::Oid,
    pub returns_set: bool,
    pub volatility: Volatility,
    pub strict: bool,
    pub parallel: Parallel,
    pub cost: Option<f32>,
}

pub(crate) fn register(
    spec: &ProcSpec,
    extension_oid: pg_sys::Oid,
    replace_exports: bool,
) -> Result<pg_sys::Oid, PgWasmError> {
    validate_spec(spec)?;

    if extension_oid == pg_sys::InvalidOid {
        return Err(PgWasmError::InvalidConfiguration(
            "extension oid must not be invalid".to_string(),
        ));
    }

    let schema_oid = schema_oid(spec)?;
    if !replace_exports {
        let existing = lookup_proc_oid(spec)?;
        if existing != pg_sys::InvalidOid {
            return Err(PgWasmError::InvalidConfiguration(collision_message(spec)));
        }
    }

    let argument_count = to_i32(spec.arg_types.len(), "argument count")?;
    let c_language_oid = c_language_oid()?;
    let all_arg_types = build_all_arg_types(spec)?;
    let arg_modes = build_arg_modes(spec)?;
    let arg_names = build_arg_names(spec)?;

    let arg_type_vector = unsafe {
        // SAFETY: `buildoidvector` copies the argument type OIDs immediately; pointer is valid
        // for this call because it either points into `spec.arg_types` or is null for zero args.
        pg_sys::buildoidvector(argument_types_ptr(spec), argument_count)
    };
    if arg_type_vector.is_null() {
        return Err(PgWasmError::Internal(
            "failed to build oidvector for procedure arguments".to_string(),
        ));
    }

    let procost = spec.cost.unwrap_or(1.0);
    let prorows = if spec.returns_set { 1000.0 } else { 0.0 };
    let procedure_address = unsafe {
        // SAFETY: every pointer argument is either a valid palloc-backed C string/list/array
        // for the duration of this call or a null pointer when PostgreSQL expects optional null.
        // Scalar arguments are validated and translated according to PostgreSQL's ProcedureCreate API.
        procedure_create(
            spec,
            schema_oid,
            replace_exports,
            c_language_oid,
            arg_type_vector,
            all_arg_types,
            arg_modes,
            arg_names,
            procost,
            prorows,
        )
    };

    if procedure_address.objectId == pg_sys::InvalidOid {
        return Err(PgWasmError::Internal(
            "ProcedureCreate returned an invalid object id".to_string(),
        ));
    }

    let extension_object = pg_sys::ObjectAddress {
        classId: pg_sys::ExtensionRelationId,
        objectId: extension_oid,
        objectSubId: 0,
    };
    unsafe {
        // SAFETY: both ObjectAddress values point to live stack values with valid class/object IDs.
        pg_sys::recordDependencyOn(
            &procedure_address,
            &extension_object,
            pg_sys::DependencyType::DEPENDENCY_EXTENSION,
        );
    }

    Ok(procedure_address.objectId)
}

pub(crate) fn unregister(fn_oid: pg_sys::Oid) -> Result<(), PgWasmError> {
    if fn_oid == pg_sys::InvalidOid {
        return Err(PgWasmError::NotFound(
            "function oid must not be invalid".to_string(),
        ));
    }

    unsafe {
        // SAFETY: caller provides a function OID within an active backend transaction context.
        pg_sys::RemoveFunctionById(fn_oid);
    }
    Ok(())
}

#[cfg(feature = "pg13")]
#[allow(clippy::too_many_arguments)]
unsafe fn procedure_create(
    spec: &ProcSpec,
    schema_oid: pg_sys::Oid,
    replace_exports: bool,
    c_language_oid: pg_sys::Oid,
    arg_type_vector: *mut pg_sys::oidvector,
    all_arg_types: Datum,
    arg_modes: Datum,
    arg_names: Datum,
    procost: f32,
    prorows: f32,
) -> pg_sys::ObjectAddress {
    unsafe {
        // SAFETY: caller validates arguments and pointer lifetimes for ProcedureCreate.
        pg_sys::ProcedureCreate(
            spec.name.as_pg_cstr(),
            schema_oid,
            replace_exports,
            spec.returns_set,
            spec.ret_type,
            pg_sys::GetUserId(),
            c_language_oid,
            pg_sys::Oid::from(pg_sys::F_FMGR_C_VALIDATOR),
            TRAMPOLINE_SYMBOL.as_pg_cstr(),
            EXTENSION_LIBRARY_PATH.as_pg_cstr(),
            pg_sys::PROKIND_FUNCTION as c_char,
            false,
            false,
            spec.strict,
            spec.volatility.to_pg_char(),
            spec.parallel.to_pg_char(),
            arg_type_vector,
            all_arg_types,
            arg_modes,
            arg_names,
            std::ptr::null_mut(),
            Datum::null(),
            Datum::null(),
            pg_sys::InvalidOid,
            procost,
            prorows,
        )
    }
}

#[cfg(any(
    feature = "pg14",
    feature = "pg15",
    feature = "pg16",
    feature = "pg17",
    feature = "pg18"
))]
#[allow(clippy::too_many_arguments)]
unsafe fn procedure_create(
    spec: &ProcSpec,
    schema_oid: pg_sys::Oid,
    replace_exports: bool,
    c_language_oid: pg_sys::Oid,
    arg_type_vector: *mut pg_sys::oidvector,
    all_arg_types: Datum,
    arg_modes: Datum,
    arg_names: Datum,
    procost: f32,
    prorows: f32,
) -> pg_sys::ObjectAddress {
    unsafe {
        // SAFETY: caller validates arguments and pointer lifetimes for ProcedureCreate.
        pg_sys::ProcedureCreate(
            spec.name.as_pg_cstr(),
            schema_oid,
            replace_exports,
            spec.returns_set,
            spec.ret_type,
            pg_sys::GetUserId(),
            c_language_oid,
            pg_sys::Oid::from(pg_sys::F_FMGR_C_VALIDATOR),
            TRAMPOLINE_SYMBOL.as_pg_cstr(),
            EXTENSION_LIBRARY_PATH.as_pg_cstr(),
            std::ptr::null_mut(),
            pg_sys::PROKIND_FUNCTION as c_char,
            false,
            false,
            spec.strict,
            spec.volatility.to_pg_char(),
            spec.parallel.to_pg_char(),
            arg_type_vector,
            all_arg_types,
            arg_modes,
            arg_names,
            std::ptr::null_mut(),
            Datum::null(),
            Datum::null(),
            pg_sys::InvalidOid,
            procost,
            prorows,
        )
    }
}

fn argument_types_ptr(spec: &ProcSpec) -> *const pg_sys::Oid {
    if spec.arg_types.is_empty() {
        std::ptr::null()
    } else {
        spec.arg_types.as_ptr()
    }
}

fn schema_oid(spec: &ProcSpec) -> Result<pg_sys::Oid, PgWasmError> {
    let oid = unsafe {
        // SAFETY: pgrx allocates a Postgres-owned C string and `get_namespace_oid` reads it.
        pg_sys::get_namespace_oid(spec.schema.as_pg_cstr(), true)
    };

    if oid == pg_sys::InvalidOid {
        return Err(PgWasmError::NotFound(format!(
            "schema '{}' does not exist",
            spec.schema
        )));
    }
    Ok(oid)
}

fn c_language_oid() -> Result<pg_sys::Oid, PgWasmError> {
    let oid = unsafe {
        // SAFETY: constant language name is converted into a valid Postgres C string.
        pg_sys::get_language_oid("c".as_pg_cstr(), true)
    };

    if oid == pg_sys::InvalidOid {
        return Err(PgWasmError::Internal(
            "language 'c' was not found in pg_language".to_string(),
        ));
    }
    Ok(oid)
}

fn validate_spec(spec: &ProcSpec) -> Result<(), PgWasmError> {
    if spec.schema.is_empty() {
        return Err(PgWasmError::InvalidConfiguration(
            "procedure schema must not be empty".to_string(),
        ));
    }
    if spec.name.is_empty() {
        return Err(PgWasmError::InvalidConfiguration(
            "procedure name must not be empty".to_string(),
        ));
    }
    if !spec.arg_names.is_empty() && spec.arg_names.len() != spec.arg_types.len() {
        return Err(PgWasmError::InvalidConfiguration(format!(
            "arg_names length ({}) must match arg_types length ({})",
            spec.arg_names.len(),
            spec.arg_types.len()
        )));
    }
    if !spec.arg_modes.is_empty() && spec.arg_modes.len() != spec.arg_types.len() {
        return Err(PgWasmError::InvalidConfiguration(format!(
            "arg_modes length ({}) must match arg_types length ({})",
            spec.arg_modes.len(),
            spec.arg_types.len()
        )));
    }
    Ok(())
}

fn to_i32(value: usize, description: &str) -> Result<i32, PgWasmError> {
    i32::try_from(value).map_err(|_| {
        PgWasmError::InvalidConfiguration(format!("{} exceeds i32::MAX ({value})", description))
    })
}

fn lookup_proc_oid(spec: &ProcSpec) -> Result<pg_sys::Oid, PgWasmError> {
    let nargs = to_i32(spec.arg_types.len(), "argument count")?;
    let oid = unsafe {
        // SAFETY: The temporary List nodes and strings are allocated in CurrentMemoryContext
        // and remain valid for the duration of LookupFuncName.
        current_context(|mcx| {
            let mut funcname = List::<*mut c_void>::Nil;
            funcname.unstable_push_in_context(
                pg_sys::makeString(spec.schema.as_pg_cstr()).cast::<c_void>(),
                mcx,
            );
            funcname.unstable_push_in_context(
                pg_sys::makeString(spec.name.as_pg_cstr()).cast::<c_void>(),
                mcx,
            );

            pg_sys::LookupFuncName(funcname.into_ptr(), nargs, argument_types_ptr(spec), true)
        })
    };
    Ok(oid)
}

fn build_all_arg_types(spec: &ProcSpec) -> Result<Datum, PgWasmError> {
    if spec.arg_modes.is_empty() {
        return Ok(Datum::null());
    }

    let mut datums: Vec<Datum> = spec
        .arg_types
        .iter()
        .copied()
        .map(|oid| Datum::from(u32::from(oid)))
        .collect();

    unsafe { construct_array_datum(&mut datums, pg_sys::OIDOID) }
}

fn build_arg_modes(spec: &ProcSpec) -> Result<Datum, PgWasmError> {
    if spec.arg_modes.is_empty() {
        return Ok(Datum::null());
    }

    let mut datums: Vec<Datum> = spec
        .arg_modes
        .iter()
        .copied()
        .map(|mode| Datum::from(mode.to_pg_char() as u8))
        .collect();

    unsafe { construct_array_datum(&mut datums, pg_sys::CHAROID) }
}

fn build_arg_names(spec: &ProcSpec) -> Result<Datum, PgWasmError> {
    if spec.arg_names.is_empty() {
        return Ok(Datum::null());
    }

    let mut datums: Vec<Datum> = spec
        .arg_names
        .iter()
        .map(|name| {
            let text_ptr = unsafe {
                // SAFETY: pgrx allocates a nul-terminated Postgres-owned C string for input.
                pg_sys::cstring_to_text(name.as_pg_cstr())
            };
            Datum::from(text_ptr as usize)
        })
        .collect();

    unsafe { construct_array_datum(&mut datums, pg_sys::TEXTOID) }
}

unsafe fn construct_array_datum(
    datums: &mut [Datum],
    element_type: pg_sys::Oid,
) -> Result<Datum, PgWasmError> {
    let mut element_len: i16 = 0;
    let mut element_byval = false;
    let mut element_align: c_char = 0;

    unsafe {
        // SAFETY: PostgreSQL fills out the provided scalar out-pointers for the given element type.
        pg_sys::get_typlenbyvalalign(
            element_type,
            &mut element_len,
            &mut element_byval,
            &mut element_align,
        );
    }

    let array_ptr = unsafe {
        // SAFETY: `datums` points to a contiguous initialized Datum buffer; PostgreSQL copies
        // these values into a freshly allocated ArrayType in the current memory context.
        pg_sys::construct_array(
            datums.as_mut_ptr(),
            to_i32(datums.len(), "array element count")?,
            element_type,
            i32::from(element_len),
            element_byval,
            element_align,
        )
    };

    if array_ptr.is_null() {
        return Err(PgWasmError::Internal(format!(
            "failed to build PostgreSQL array for element type {}",
            u32::from(element_type)
        )));
    }

    Ok(Datum::from(array_ptr as usize))
}

fn collision_message(spec: &ProcSpec) -> String {
    format!(
        "function {} already exists; set replace_exports := true to overwrite",
        render_function_signature(spec)
    )
}

fn render_function_signature(spec: &ProcSpec) -> String {
    let rendered_args = spec
        .arg_types
        .iter()
        .copied()
        .map(render_type_name)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}.{}({rendered_args})", spec.schema, spec.name)
}

fn render_type_name(type_oid: pg_sys::Oid) -> String {
    let type_name_ptr = unsafe {
        // SAFETY: PostgreSQL returns a palloc-allocated null-terminated string.
        pg_sys::format_type_be(type_oid)
    };
    if type_name_ptr.is_null() {
        return u32::from(type_oid).to_string();
    }

    unsafe {
        // SAFETY: `format_type_be` returns a valid NUL-terminated C string pointer.
        CStr::from_ptr(type_name_ptr).to_string_lossy().into_owned()
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use pgrx::pg_sys::{self, AsPgCStr};
    use pgrx::pg_test;
    use pgrx::prelude::{PgSqlErrorCode, Spi};

    use crate::errors::PgWasmError;

    use super::{Parallel, ProcSpec, Volatility, collision_message, register, unregister};

    static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(1);

    fn unique_name(prefix: &str) -> String {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{id}")
    }

    fn extension_oid() -> pg_sys::Oid {
        let oid = unsafe {
            // SAFETY: constant extension name is converted into a valid Postgres C string.
            pg_sys::get_extension_oid("pgwasm".as_pg_cstr(), false)
        };
        assert_ne!(
            oid,
            pg_sys::InvalidOid,
            "pgwasm extension must be installed during pg_test"
        );
        oid
    }

    fn base_spec(name: String) -> ProcSpec {
        ProcSpec {
            schema: "public".to_string(),
            name,
            arg_types: vec![pg_sys::INT4OID],
            arg_names: Vec::new(),
            arg_modes: Vec::new(),
            ret_type: pg_sys::INT4OID,
            returns_set: false,
            volatility: Volatility::Volatile,
            strict: false,
            parallel: Parallel::Unsafe,
            cost: Some(1.0),
        }
    }

    #[pg_test]
    fn register_creates_pg_proc_row_with_trampoline_metadata() {
        let spec = base_spec(unique_name("proc_reg_register"));
        let fn_oid = register(&spec, extension_oid(), false).expect("register should succeed");

        let created = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (\
                SELECT 1 \
                FROM pg_proc p \
                JOIN pg_language l ON l.oid = p.prolang \
                WHERE p.oid = {} \
                  AND p.prosrc = 'pgwasm_udf_trampoline' \
                  AND l.lanname = 'c'\
            )",
            u32::from(fn_oid)
        ))
        .expect("catalog verification query should run");

        assert_eq!(Some(true), created);
        unregister(fn_oid).expect("cleanup unregister should succeed");
    }

    #[pg_test]
    fn register_with_replace_false_returns_collision_error() {
        let spec = base_spec(unique_name("proc_reg_collision"));
        let extension_oid = extension_oid();
        let fn_oid =
            register(&spec, extension_oid, false).expect("initial register should succeed");

        let error = register(&spec, extension_oid, false).expect_err("collision should error");
        assert_eq!(
            error.sqlstate(),
            PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE
        );
        match error {
            PgWasmError::InvalidConfiguration(message) => {
                assert_eq!(message, collision_message(&spec));
            }
            unexpected => panic!("expected InvalidConfiguration, got: {unexpected:?}"),
        }

        unregister(fn_oid).expect("cleanup unregister should succeed");
    }

    #[pg_test]
    fn register_with_replace_true_overwrites_existing_definition() {
        let mut spec = base_spec(unique_name("proc_reg_replace"));
        spec.strict = false;
        spec.cost = Some(3.0);

        let extension_oid = extension_oid();
        let first_oid =
            register(&spec, extension_oid, false).expect("initial register should succeed");

        spec.strict = true;
        spec.cost = Some(42.0);
        let second_oid =
            register(&spec, extension_oid, true).expect("replace register should succeed");

        assert_eq!(first_oid, second_oid);

        let strict = Spi::get_one::<bool>(&format!(
            "SELECT proisstrict FROM pg_proc WHERE oid = {}",
            u32::from(second_oid)
        ))
        .expect("strict flag query should run");
        assert_eq!(Some(true), strict);

        let cost = Spi::get_one::<f32>(&format!(
            "SELECT procost::float4 FROM pg_proc WHERE oid = {}",
            u32::from(second_oid)
        ))
        .expect("cost query should run");
        assert_eq!(Some(42.0), cost);

        unregister(second_oid).expect("cleanup unregister should succeed");
    }

    #[pg_test]
    fn unregister_removes_pg_proc_row() {
        let spec = base_spec(unique_name("proc_reg_unregister"));
        let fn_oid = register(&spec, extension_oid(), false).expect("register should succeed");

        unregister(fn_oid).expect("unregister should succeed");

        let count = Spi::get_one::<i64>(&format!(
            "SELECT count(*) FROM pg_proc WHERE oid = {}",
            u32::from(fn_oid)
        ))
        .expect("count query should run");
        assert_eq!(Some(0), count);
    }

    #[pg_test]
    fn register_records_dependency_on_pgwasm_extension() {
        let spec = base_spec(unique_name("proc_reg_dependency"));
        let extension_oid = extension_oid();
        let fn_oid = register(&spec, extension_oid, false).expect("register should succeed");

        let has_dependency = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS (\
                SELECT 1 \
                FROM pg_depend \
                WHERE classid = 'pg_proc'::regclass \
                  AND objid = {} \
                  AND refclassid = 'pg_extension'::regclass \
                  AND refobjid = {} \
                  AND deptype = 'e'\
            )",
            u32::from(fn_oid),
            u32::from(extension_oid)
        ))
        .expect("dependency query should run");
        assert_eq!(Some(true), has_dependency);

        unregister(fn_oid).expect("cleanup unregister should succeed");
    }
}
