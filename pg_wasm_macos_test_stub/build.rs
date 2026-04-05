//! Compiles `stub.c` into a static library for the test harness only (macOS dev-dependency).

use std::env;
use std::path::Path;

fn main() {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    let stub = Path::new(env!("CARGO_MANIFEST_DIR")).join("stub.c");
    println!("cargo:rerun-if-changed={}", stub.display());

    cc::Build::new()
        .file(&stub)
        .warnings(false)
        .compile("pg_wasm_macos_pg_stub");

    println!("cargo:rustc-link-lib=framework=CoreFoundation");
    println!("cargo:rustc-link-lib=framework=IOKit");
}
