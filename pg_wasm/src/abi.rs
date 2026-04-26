//! ABI detection and invocation-shape metadata.

use wasmparser::{Encoding, Parser, Payload};

use crate::errors::PgWasmError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Abi {
    Component,
    Core,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbiOverride {
    Auto,
    ForceCore,
}

pub(crate) fn detect(bytes: &[u8], override_: AbiOverride) -> Result<Abi, PgWasmError> {
    let mut detected = None;

    for payload in Parser::new(0).parse_all(bytes) {
        match payload {
            Ok(Payload::Version { encoding, .. }) => {
                let abi = if encoding == Encoding::Component {
                    Abi::Component
                } else if encoding == Encoding::Module {
                    Abi::Core
                } else {
                    return Err(PgWasmError::ValidationFailed(format!(
                        "unsupported wasm encoding: {encoding:?}"
                    )));
                };
                detected = Some(abi);
                break;
            }
            Ok(_) => {}
            Err(err) => return Err(PgWasmError::ValidationFailed(format!("{err:#}"))),
        }
    }

    let detected = detected.ok_or_else(|| {
        PgWasmError::ValidationFailed("missing WebAssembly version payload".to_string())
    })?;

    validate(bytes)?;

    if detected == Abi::Component && override_ == AbiOverride::ForceCore {
        return Err(PgWasmError::ValidationFailed(
            "ABI override \"core\" cannot be used with a component binary".to_string(),
        ));
    }

    Ok(detected)
}

pub(crate) fn validate(bytes: &[u8]) -> Result<(), PgWasmError> {
    wasmparser::validate(bytes)
        .map(|_| ())
        .map_err(|err| PgWasmError::ValidationFailed(format!("{err:#}")))
}

#[cfg(all(test, not(feature = "pg_test")))]
mod host_tests {
    use super::{Abi, AbiOverride, detect};
    use crate::errors::PgWasmError;

    const MINIMAL_CORE_MODULE: &[u8] = &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
    const MINIMAL_COMPONENT: &[u8] = &[0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];
    const TRUNCATED_MAGIC: &[u8] = &[0x00, 0x61, 0x73, 0x6d];
    const INVALID_SECTION_LENGTH: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, // magic
        0x01, 0x00, 0x00, 0x00, // core module version
        0x01, // type section id
        0x05, // section length claims 5 bytes
        0x00, // but only one byte follows
    ];

    #[test]
    fn detects_core_module_header() {
        let detected = detect(MINIMAL_CORE_MODULE, AbiOverride::Auto).unwrap();
        assert_eq!(detected, Abi::Core);
    }

    #[test]
    fn detects_component_header() {
        let detected = detect(MINIMAL_COMPONENT, AbiOverride::Auto).unwrap();
        assert_eq!(detected, Abi::Component);
    }

    #[test]
    fn force_core_rejects_component_binary() {
        let result = detect(MINIMAL_COMPONENT, AbiOverride::ForceCore);
        assert!(matches!(result, Err(PgWasmError::ValidationFailed(_))));
    }

    #[test]
    fn truncated_magic_is_validation_failure() {
        let result = detect(TRUNCATED_MAGIC, AbiOverride::Auto);
        assert!(matches!(result, Err(PgWasmError::ValidationFailed(_))));
    }

    #[test]
    fn invalid_body_is_validation_failure() {
        let result = detect(INVALID_SECTION_LENGTH, AbiOverride::Auto);
        assert!(matches!(result, Err(PgWasmError::ValidationFailed(_))));
    }
}
