//! Compiles `stub.c` into a static library for the test harness (macOS and Linux dev-dependency).

use std::env;
use std::path::Path;

fn main() {
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if os != "macos" && os != "linux" {
        return;
    }

    let stub = Path::new(env!("CARGO_MANIFEST_DIR")).join("stub.c");
    println!("cargo:rerun-if-changed={}", stub.display());

    cc::Build::new()
        .file(&stub)
        .warnings(false)
        .compile("pg_wasm_test_stub");

    if os == "macos" {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=IOKit");
    }
}
