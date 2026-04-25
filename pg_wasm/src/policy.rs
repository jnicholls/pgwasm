//! Sandbox policy resolution and narrowing logic.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::{Limits, PolicyOverrides};
use crate::errors::{PgWasmError, Result};
use crate::guc;

/// Immutable snapshot of policy/limit GUC values used for one resolve call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GucSnapshot {
    pub(crate) allow_spi: bool,
    pub(crate) allow_wasi: bool,
    pub(crate) allow_wasi_env: bool,
    pub(crate) allow_wasi_fs: bool,
    pub(crate) allow_wasi_http: bool,
    pub(crate) allow_wasi_net: bool,
    pub(crate) allow_wasi_stdio: bool,
    pub(crate) allowed_hosts: Vec<String>,
    pub(crate) fuel_per_invocation: i32,
    pub(crate) instances_per_module: i32,
    pub(crate) invocation_deadline_ms: i32,
    pub(crate) max_memory_pages: i32,
    pub(crate) wasi_preopens: BTreeMap<String, String>,
}

impl GucSnapshot {
    pub(crate) fn from_gucs() -> Self {
        Self {
            allow_spi: guc::ALLOW_SPI.get(),
            allow_wasi: guc::ALLOW_WASI.get(),
            allow_wasi_env: guc::ALLOW_WASI_ENV.get(),
            allow_wasi_fs: guc::ALLOW_WASI_FS.get(),
            allow_wasi_http: guc::ALLOW_WASI_HTTP.get(),
            allow_wasi_net: guc::ALLOW_WASI_NET.get(),
            allow_wasi_stdio: guc::ALLOW_WASI_STDIO.get(),
            allowed_hosts: parse_allowed_hosts(guc::ALLOWED_HOSTS.get()),
            fuel_per_invocation: guc::FUEL_PER_INVOCATION.get(),
            instances_per_module: guc::INSTANCES_PER_MODULE.get(),
            invocation_deadline_ms: guc::INVOCATION_DEADLINE_MS.get(),
            max_memory_pages: guc::MAX_MEMORY_PAGES.get(),
            wasi_preopens: parse_wasi_preopens(guc::WASI_PREOPENS.get()),
        }
    }
}

#[cfg(test)]
impl GucSnapshot {
    pub(crate) fn new_for_test(
        allow_wasi: bool,
        allow_wasi_stdio: bool,
        allow_wasi_env: bool,
        allow_wasi_fs: bool,
        allow_wasi_net: bool,
        allow_wasi_http: bool,
        wasi_preopens: BTreeMap<String, String>,
        allowed_hosts: Vec<String>,
        allow_spi: bool,
        max_memory_pages: i32,
        instances_per_module: i32,
        fuel_per_invocation: i32,
        invocation_deadline_ms: i32,
    ) -> Self {
        Self {
            allow_spi,
            allow_wasi,
            allow_wasi_env,
            allow_wasi_fs,
            allow_wasi_http,
            allow_wasi_net,
            allow_wasi_stdio,
            allowed_hosts,
            fuel_per_invocation,
            instances_per_module,
            invocation_deadline_ms,
            max_memory_pages,
            wasi_preopens,
        }
    }
}

/// Fully resolved policy/limit bundle with no optional fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EffectivePolicy {
    pub(crate) allow_spi: bool,
    pub(crate) allow_wasi: bool,
    pub(crate) allow_wasi_env: bool,
    pub(crate) allow_wasi_fs: bool,
    pub(crate) allow_wasi_http: bool,
    pub(crate) allow_wasi_net: bool,
    pub(crate) allow_wasi_stdio: bool,
    pub(crate) allowed_hosts: Vec<String>,
    pub(crate) fuel_per_invocation: i32,
    pub(crate) instances_per_module: i32,
    pub(crate) invocation_deadline_ms: i32,
    pub(crate) max_memory_pages: i32,
    pub(crate) wasi_preopens: BTreeMap<String, String>,
}

pub(crate) fn resolve(
    guc_snapshot: &GucSnapshot,
    overrides: Option<&PolicyOverrides>,
    limits: Option<&Limits>,
) -> Result<EffectivePolicy> {
    let override_policy = overrides.cloned().unwrap_or_default();
    let override_limits = limits.cloned().unwrap_or_default();

    let allow_wasi = resolve_bool(
        "allow_wasi",
        guc_snapshot.allow_wasi,
        override_policy.allow_wasi,
    )?;
    let allow_wasi_stdio = resolve_bool(
        "allow_wasi_stdio",
        guc_snapshot.allow_wasi_stdio,
        override_policy.allow_wasi_stdio,
    )?;
    let allow_wasi_env = resolve_bool(
        "allow_wasi_env",
        guc_snapshot.allow_wasi_env,
        override_policy.allow_wasi_env,
    )?;
    let allow_wasi_fs = resolve_bool(
        "allow_wasi_fs",
        guc_snapshot.allow_wasi_fs,
        override_policy.allow_wasi_fs,
    )?;
    let allow_wasi_net = resolve_bool(
        "allow_wasi_net",
        guc_snapshot.allow_wasi_net,
        override_policy.allow_wasi_net,
    )?;
    let allow_wasi_http = resolve_bool(
        "allow_wasi_http",
        guc_snapshot.allow_wasi_http,
        override_policy.allow_wasi_http,
    )?;
    let allow_spi = resolve_bool(
        "allow_spi",
        guc_snapshot.allow_spi,
        override_policy.allow_spi,
    )?;

    let wasi_preopens = resolve_preopens(
        "wasi_preopens",
        &guc_snapshot.wasi_preopens,
        override_policy.wasi_preopens.as_ref(),
    )?;
    let allowed_hosts = resolve_allowed_hosts(
        "allowed_hosts",
        &guc_snapshot.allowed_hosts,
        override_policy.allowed_hosts.as_ref(),
    )?;

    let max_memory_pages = resolve_limit(
        "max_memory_pages",
        guc_snapshot.max_memory_pages,
        override_limits.max_memory_pages,
    )?;
    let instances_per_module = resolve_limit(
        "instances_per_module",
        guc_snapshot.instances_per_module,
        override_limits.instances_per_module,
    )?;
    let fuel_per_invocation = resolve_limit(
        "fuel_per_invocation",
        guc_snapshot.fuel_per_invocation,
        override_limits.fuel_per_invocation,
    )?;
    let invocation_deadline_ms = resolve_limit(
        "invocation_deadline_ms",
        guc_snapshot.invocation_deadline_ms,
        override_limits.invocation_deadline_ms,
    )?;

    Ok(EffectivePolicy {
        allow_spi,
        allow_wasi,
        allow_wasi_env,
        allow_wasi_fs,
        allow_wasi_http,
        allow_wasi_net,
        allow_wasi_stdio,
        allowed_hosts,
        fuel_per_invocation,
        instances_per_module,
        invocation_deadline_ms,
        max_memory_pages,
        wasi_preopens,
    })
}

fn parse_allowed_hosts(input: Option<std::ffi::CString>) -> Vec<String> {
    parse_csv_string(input)
}

fn parse_wasi_preopens(input: Option<std::ffi::CString>) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for pair in parse_csv_string(input) {
        if let Some((guest, host)) = pair.split_once('=') {
            let guest = guest.trim();
            let host = host.trim();
            if !guest.is_empty() && !host.is_empty() {
                map.insert(guest.to_string(), host.to_string());
            }
        }
    }
    map
}

fn parse_csv_string(input: Option<std::ffi::CString>) -> Vec<String> {
    input
        .as_ref()
        .map(|value| value.to_string_lossy())
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn resolve_bool(
    field: &'static str,
    guc_value: bool,
    override_value: Option<bool>,
) -> Result<bool> {
    if !guc_value && matches!(override_value, Some(true)) {
        return Err(PgWasmError::PermissionDenied(permission_denied_message(
            field,
        )));
    }

    Ok(guc_value && override_value.unwrap_or(guc_value))
}

fn resolve_limit(
    field: &'static str,
    guc_ceiling: i32,
    override_value: Option<i32>,
) -> Result<i32> {
    if let Some(value) = override_value {
        if value > guc_ceiling {
            return Err(PgWasmError::PermissionDenied(permission_denied_message(
                field,
            )));
        }

        return Ok(value.min(guc_ceiling));
    }

    Ok(guc_ceiling)
}

fn resolve_preopens(
    field: &'static str,
    guc_preopens: &BTreeMap<String, String>,
    override_preopens: Option<&BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>> {
    let Some(candidate) = override_preopens else {
        return Ok(guc_preopens.clone());
    };

    if candidate.iter().all(|(guest, host)| {
        guc_preopens
            .get(guest)
            .is_some_and(|allowed| allowed == host)
    }) {
        return Ok(candidate.clone());
    }

    Err(PgWasmError::PermissionDenied(permission_denied_message(
        field,
    )))
}

fn resolve_allowed_hosts(
    field: &'static str,
    guc_allowed_hosts: &[String],
    override_allowed_hosts: Option<&Vec<String>>,
) -> Result<Vec<String>> {
    let Some(candidate) = override_allowed_hosts else {
        return Ok(guc_allowed_hosts.to_vec());
    };

    let allowed_set: BTreeSet<&str> = guc_allowed_hosts.iter().map(String::as_str).collect();
    let candidate_set: BTreeSet<&str> = candidate.iter().map(String::as_str).collect();
    if candidate_set.is_subset(&allowed_set) {
        return Ok(candidate.clone());
    }

    Err(PgWasmError::PermissionDenied(permission_denied_message(
        field,
    )))
}

fn permission_denied_message(field: &str) -> String {
    format!("override attempts to widen `{field}`")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::config::{Limits, PolicyOverrides};
    use crate::errors::PgWasmError;
    use crate::policy::{EffectivePolicy, GucSnapshot, resolve};

    fn base_snapshot() -> GucSnapshot {
        GucSnapshot::new_for_test(
            true,
            true,
            true,
            true,
            true,
            true,
            BTreeMap::from([
                ("/etc".to_string(), "/srv/etc".to_string()),
                ("/tmp".to_string(), "/srv/tmp".to_string()),
            ]),
            vec![
                "db.example.com:443".to_string(),
                "api.example.com:443".to_string(),
                "cache.example.com:6379".to_string(),
            ],
            true,
            1_024,
            4,
            100_000_000,
            5_000,
        )
    }

    fn assert_permission_denied_with_field(error: PgWasmError, field: &str) {
        match error {
            PgWasmError::PermissionDenied(message) => {
                assert!(
                    message.contains(field),
                    "message `{message}` missing `{field}`"
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    struct BoolFieldCase {
        name: &'static str,
        set_snapshot: fn(&mut GucSnapshot, bool),
        set_override: fn(&mut PolicyOverrides, Option<bool>),
        get_effective: fn(&EffectivePolicy) -> bool,
    }

    fn bool_field_cases() -> [BoolFieldCase; 7] {
        [
            BoolFieldCase {
                name: "allow_wasi",
                set_snapshot: |snapshot, value| snapshot.allow_wasi = value,
                set_override: |overrides, value| overrides.allow_wasi = value,
                get_effective: |effective| effective.allow_wasi,
            },
            BoolFieldCase {
                name: "allow_wasi_stdio",
                set_snapshot: |snapshot, value| snapshot.allow_wasi_stdio = value,
                set_override: |overrides, value| overrides.allow_wasi_stdio = value,
                get_effective: |effective| effective.allow_wasi_stdio,
            },
            BoolFieldCase {
                name: "allow_wasi_env",
                set_snapshot: |snapshot, value| snapshot.allow_wasi_env = value,
                set_override: |overrides, value| overrides.allow_wasi_env = value,
                get_effective: |effective| effective.allow_wasi_env,
            },
            BoolFieldCase {
                name: "allow_wasi_fs",
                set_snapshot: |snapshot, value| snapshot.allow_wasi_fs = value,
                set_override: |overrides, value| overrides.allow_wasi_fs = value,
                get_effective: |effective| effective.allow_wasi_fs,
            },
            BoolFieldCase {
                name: "allow_wasi_net",
                set_snapshot: |snapshot, value| snapshot.allow_wasi_net = value,
                set_override: |overrides, value| overrides.allow_wasi_net = value,
                get_effective: |effective| effective.allow_wasi_net,
            },
            BoolFieldCase {
                name: "allow_wasi_http",
                set_snapshot: |snapshot, value| snapshot.allow_wasi_http = value,
                set_override: |overrides, value| overrides.allow_wasi_http = value,
                get_effective: |effective| effective.allow_wasi_http,
            },
            BoolFieldCase {
                name: "allow_spi",
                set_snapshot: |snapshot, value| snapshot.allow_spi = value,
                set_override: |overrides, value| overrides.allow_spi = value,
                get_effective: |effective| effective.allow_spi,
            },
        ]
    }

    #[test]
    fn boolean_fields_apply_narrowing_matrix_for_all_combinations() {
        for case in bool_field_cases() {
            let mut guc_permits = base_snapshot();
            (case.set_snapshot)(&mut guc_permits, true);

            let mut override_denies = PolicyOverrides::default();
            (case.set_override)(&mut override_denies, Some(false));
            let effective = resolve(&guc_permits, Some(&override_denies), None)
                .expect("guc permits + override denies should succeed");
            assert!(
                !(case.get_effective)(&effective),
                "expected denied effective value for field `{}`",
                case.name
            );

            let mut guc_denies = base_snapshot();
            (case.set_snapshot)(&mut guc_denies, false);

            let mut override_permits = PolicyOverrides::default();
            (case.set_override)(&mut override_permits, Some(true));
            let error = resolve(&guc_denies, Some(&override_permits), None)
                .expect_err("guc denies + override permits should fail");
            assert_permission_denied_with_field(error, case.name);

            let mut override_denies_again = PolicyOverrides::default();
            (case.set_override)(&mut override_denies_again, Some(false));
            let effective = resolve(&guc_denies, Some(&override_denies_again), None)
                .expect("guc denies + override denies should succeed");
            assert!(
                !(case.get_effective)(&effective),
                "expected denied effective value for field `{}`",
                case.name
            );

            let mut override_permits_again = PolicyOverrides::default();
            (case.set_override)(&mut override_permits_again, Some(true));
            let effective = resolve(&guc_permits, Some(&override_permits_again), None)
                .expect("guc permits + override permits should succeed");
            assert!(
                (case.get_effective)(&effective),
                "expected permitted effective value for field `{}`",
                case.name
            );
        }
    }

    #[test]
    fn wasi_preopens_subset_is_allowed() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            wasi_preopens: Some(BTreeMap::from([(
                "/tmp".to_string(),
                "/srv/tmp".to_string(),
            )])),
            ..PolicyOverrides::default()
        };

        let effective = resolve(&snapshot, Some(&overrides), None).expect("subset accepted");
        assert_eq!(
            effective.wasi_preopens,
            BTreeMap::from([("/tmp".to_string(), "/srv/tmp".to_string())])
        );
    }

    #[test]
    fn wasi_preopens_superset_is_denied() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            wasi_preopens: Some(BTreeMap::from([
                ("/etc".to_string(), "/srv/etc".to_string()),
                ("/tmp".to_string(), "/srv/tmp".to_string()),
                ("/new".to_string(), "/srv/new".to_string()),
            ])),
            ..PolicyOverrides::default()
        };

        let error = resolve(&snapshot, Some(&overrides), None).expect_err("superset denied");
        assert_permission_denied_with_field(error, "wasi_preopens");
    }

    #[test]
    fn wasi_preopens_equal_is_allowed() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            wasi_preopens: Some(snapshot.wasi_preopens.clone()),
            ..PolicyOverrides::default()
        };

        let effective = resolve(&snapshot, Some(&overrides), None).expect("equal set accepted");
        assert_eq!(effective.wasi_preopens, snapshot.wasi_preopens);
    }

    #[test]
    fn wasi_preopens_disjoint_is_denied() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            wasi_preopens: Some(BTreeMap::from([(
                "/var".to_string(),
                "/srv/var".to_string(),
            )])),
            ..PolicyOverrides::default()
        };

        let error = resolve(&snapshot, Some(&overrides), None).expect_err("disjoint denied");
        assert_permission_denied_with_field(error, "wasi_preopens");
    }

    #[test]
    fn allowed_hosts_subset_is_allowed() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            allowed_hosts: Some(vec!["db.example.com:443".to_string()]),
            ..PolicyOverrides::default()
        };

        let effective = resolve(&snapshot, Some(&overrides), None).expect("subset accepted");
        assert_eq!(
            effective.allowed_hosts,
            vec!["db.example.com:443".to_string()]
        );
    }

    #[test]
    fn allowed_hosts_superset_is_denied() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            allowed_hosts: Some(vec![
                "db.example.com:443".to_string(),
                "api.example.com:443".to_string(),
                "cache.example.com:6379".to_string(),
                "other.example.com:443".to_string(),
            ]),
            ..PolicyOverrides::default()
        };

        let error = resolve(&snapshot, Some(&overrides), None).expect_err("superset denied");
        assert_permission_denied_with_field(error, "allowed_hosts");
    }

    #[test]
    fn allowed_hosts_equal_is_allowed() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            allowed_hosts: Some(snapshot.allowed_hosts.clone()),
            ..PolicyOverrides::default()
        };

        let effective = resolve(&snapshot, Some(&overrides), None).expect("equal set accepted");
        assert_eq!(effective.allowed_hosts, snapshot.allowed_hosts);
    }

    #[test]
    fn allowed_hosts_disjoint_is_denied() {
        let snapshot = base_snapshot();
        let overrides = PolicyOverrides {
            allowed_hosts: Some(vec!["other.example.com:443".to_string()]),
            ..PolicyOverrides::default()
        };

        let error = resolve(&snapshot, Some(&overrides), None).expect_err("disjoint denied");
        assert_permission_denied_with_field(error, "allowed_hosts");
    }

    #[test]
    fn limits_override_above_ceiling_is_denied() {
        let snapshot = base_snapshot();
        let limits = Limits {
            max_memory_pages: Some(2_048),
            ..Limits::default()
        };

        let error = resolve(&snapshot, None, Some(&limits)).expect_err("widening limit denied");
        assert_permission_denied_with_field(error, "max_memory_pages");
    }

    #[test]
    fn limits_override_below_ceiling_uses_override() {
        let snapshot = base_snapshot();
        let limits = Limits {
            max_memory_pages: Some(512),
            instances_per_module: Some(2),
            fuel_per_invocation: Some(10_000),
            invocation_deadline_ms: Some(1_000),
        };

        let effective = resolve(&snapshot, None, Some(&limits)).expect("narrowing limits accepted");
        assert_eq!(effective.max_memory_pages, 512);
        assert_eq!(effective.instances_per_module, 2);
        assert_eq!(effective.fuel_per_invocation, 10_000);
        assert_eq!(effective.invocation_deadline_ms, 1_000);
    }

    #[test]
    fn limits_absent_use_guc_ceilings() {
        let snapshot = base_snapshot();
        let effective = resolve(&snapshot, None, None).expect("defaults accepted");

        assert_eq!(effective.max_memory_pages, snapshot.max_memory_pages);
        assert_eq!(
            effective.instances_per_module,
            snapshot.instances_per_module
        );
        assert_eq!(effective.fuel_per_invocation, snapshot.fuel_per_invocation);
        assert_eq!(
            effective.invocation_deadline_ms,
            snapshot.invocation_deadline_ms
        );
    }
}
