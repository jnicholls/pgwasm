use wit_component::{DecodedWasm, WitPrinter, decode as decode_wit_component};
use wit_parser::{Resolve, WorldId};

use crate::errors::PgWasmError;

#[derive(Debug)]
pub(crate) struct DecodedWorld {
    pub(crate) resolve: Resolve,
    pub(crate) world_id: WorldId,
    pub(crate) wit_text: String,
}

pub(crate) fn decode(bytes: &[u8]) -> Result<DecodedWorld, PgWasmError> {
    if is_core_module_binary(bytes) {
        return Err(PgWasmError::InvalidModule(
            "component did not embed a world".to_string(),
        ));
    }

    let decoded = decode_wit_component(bytes).map_err(|error| {
        PgWasmError::InvalidModule(format!("failed to decode component: {error}"))
    })?;

    let (resolve, world_id) = match decoded {
        DecodedWasm::Component(resolve, world_id) => (resolve, world_id),
        DecodedWasm::WitPackage(_, _) => {
            return Err(PgWasmError::InvalidModule(
                "component did not embed a world".to_string(),
            ));
        }
    };

    let wit_text = print_wit_text(&resolve, world_id)?;

    Ok(DecodedWorld {
        resolve,
        world_id,
        wit_text,
    })
}

fn is_core_module_binary(bytes: &[u8]) -> bool {
    const WASM_MAGIC_AND_CORE_VERSION: &[u8; 8] = b"\0asm\x01\0\0\0";
    bytes.len() >= WASM_MAGIC_AND_CORE_VERSION.len()
        && &bytes[..WASM_MAGIC_AND_CORE_VERSION.len()] == WASM_MAGIC_AND_CORE_VERSION
}

fn print_wit_text(resolve: &Resolve, world_id: WorldId) -> Result<String, PgWasmError> {
    let world = resolve.worlds.get(world_id).ok_or_else(|| {
        PgWasmError::InvalidModule("decoded component world was not present".to_string())
    })?;
    let pkg = world.package.ok_or_else(|| {
        PgWasmError::InvalidModule("decoded component world did not have a package".to_string())
    })?;

    let mut printer = WitPrinter::default();
    let nested_packages: Vec<_> = resolve
        .topological_packages()
        .into_iter()
        .filter(|package_id| *package_id != pkg)
        .collect();
    printer
        .print(resolve, pkg, &nested_packages)
        .map_err(|error| PgWasmError::InvalidModule(format!("failed to print world: {error}")))?;
    Ok(printer.output.to_string())
}

#[cfg(test)]
mod tests {
    use wit_component::{ComponentEncoder, StringEncoding, embed_component_metadata};

    use super::*;

    fn fixture_core_module() -> &'static [u8] {
        // (module)
        &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
    }

    fn fixture_component_bytes(wit_source: &str, world_name: &str) -> Result<Vec<u8>, PgWasmError> {
        let mut module = fixture_core_module().to_vec();
        let mut resolve = Resolve::default();
        let pkg = resolve
            .push_str("fixture.wit", wit_source)
            .map_err(|error| {
                PgWasmError::Internal(format!("failed to parse fixture wit: {error}"))
            })?;
        let world = resolve
            .select_world(&[pkg], Some(world_name))
            .map_err(|error| {
                PgWasmError::Internal(format!(
                    "failed to select fixture world `{world_name}`: {error}"
                ))
            })?;
        embed_component_metadata(&mut module, &resolve, world, StringEncoding::UTF8).map_err(
            |error| PgWasmError::Internal(format!("failed to embed fixture metadata: {error}")),
        )?;

        let mut encoder = ComponentEncoder::default()
            .module(&module)
            .map_err(|error| PgWasmError::Internal(format!("failed to set module: {error}")))?
            .validate(true);
        encoder
            .encode()
            .map_err(|error| PgWasmError::Internal(format!("failed to encode component: {error}")))
    }

    #[test]
    fn decode_rejects_core_module_bytes() {
        let error = decode(fixture_core_module()).expect_err("core wasm should be rejected");
        assert!(matches!(error, PgWasmError::InvalidModule(_)));
    }

    #[test]
    fn decode_rejects_corrupted_component_bytes() {
        let mut bytes =
            fixture_component_bytes("package test:fixture; world fixture {}", "fixture")
                .expect("fixture should encode");
        bytes.truncate(bytes.len() / 2);
        let error = decode(&bytes).expect_err("corrupted component should be rejected");
        assert!(matches!(error, PgWasmError::InvalidModule(_)));
    }

    #[test]
    fn decode_world_text_round_trips_stably() {
        let wit_source = r#"
            package test:fixture;

            interface api {
                record person {
                    id: u32,
                    name: string,
                }

                enum color {
                    red,
                    blue,
                }
            }

            world fixture {
                export api;
            }
        "#;
        let bytes = fixture_component_bytes(wit_source, "fixture").expect("fixture should encode");

        let decoded = decode(&bytes).expect("component should decode");
        let first = print_wit_text(&decoded.resolve, decoded.world_id).expect("first print works");
        let second =
            print_wit_text(&decoded.resolve, decoded.world_id).expect("second print works");

        assert_eq!(decoded.wit_text, first);
        assert_eq!(first, second);
    }

    #[test]
    fn wit_printer_output_stable_after_parse_round_trip() {
        let wit_source = r#"
            package test:fixture;

            interface api {
                record person {
                    id: u32,
                    name: string,
                }

                enum color {
                    red,
                    blue,
                }
            }

            world fixture {
                export api;
            }
        "#;
        let bytes = fixture_component_bytes(wit_source, "fixture").expect("fixture should encode");
        let decoded = decode(&bytes).expect("component should decode");
        let printed_once = decoded.wit_text.clone();

        let mut resolve = Resolve::default();
        resolve
            .push_str("reparse.wit", &printed_once)
            .expect("printed WIT should parse");
        let (world_id, _) = resolve
            .worlds
            .iter()
            .next()
            .expect("re-parsed WIT should define at least one world");

        let printed_twice =
            print_wit_text(&resolve, world_id).expect("second pass through WitPrinter should work");
        assert_eq!(printed_once, printed_twice);
    }
}
