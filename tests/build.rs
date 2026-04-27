//! Build integration-test WebAssembly components (`wasm-tools` CLI + `http_search_guest` crate).

use std::path::PathBuf;
use std::process::Command;

fn run(cmd: &mut Command) {
    let status = cmd.status().unwrap_or_else(|e| {
        panic!(
            "failed to spawn {:?}: {e} (is the binary on PATH?)",
            cmd.get_program()
        );
    });
    assert!(
        status.success(),
        "command failed with {status:?}: {:?}",
        cmd
    );
}

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let workspace_root: PathBuf = manifest_dir
        .parent()
        .expect("integration tests crate must reside at <workspace>/tests")
        .to_path_buf();
    let fixtures = manifest_dir.join("fixtures");
    let http_guest_dir = workspace_root.join("tests/http_search_guest");

    let core_wat = fixtures.join("core.wat");
    let itest_wit = fixtures.join("itest.wit");

    println!("cargo:rerun-if-changed={}", core_wat.display());
    println!("cargo:rerun-if-changed={}", itest_wit.display());
    println!(
        "cargo:rerun-if-changed={}",
        http_guest_dir.join("Cargo.toml").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        http_guest_dir.join("src/lib.rs").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        http_guest_dir.join("wit/http-search.wit").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        workspace_root.join("Cargo.toml").display()
    );

    for entry in std::fs::read_dir(http_guest_dir.join("wit/deps")).unwrap_or_else(|e| {
        panic!(
            "read {}: {e}; vendor WIT deps under tests/http_search_guest/wit/deps",
            http_guest_dir.join("wit/deps").display()
        )
    }) {
        let entry = entry.unwrap();
        println!("cargo:rerun-if-changed={}", entry.path().display());
    }

    let core_wasm = out_dir.join("itest_core.wasm");
    run(Command::new("wasm-tools")
        .arg("parse")
        .arg(&core_wat)
        .arg("-o")
        .arg(&core_wasm));

    let embedded = out_dir.join("itest_embedded.wasm");
    run(Command::new("wasm-tools")
        .arg("component")
        .arg("embed")
        .arg(&itest_wit)
        .arg(&core_wasm)
        .arg("-o")
        .arg(&embedded));

    let itest_component = out_dir.join("itest.component.wasm");
    run(Command::new("wasm-tools")
        .arg("component")
        .arg("new")
        .arg(&embedded)
        .arg("-o")
        .arg(&itest_component));

    run(Command::new("cargo").current_dir(&workspace_root).args([
        "build",
        "-p",
        "http_search_guest",
        "--target",
        "wasm32-wasip2",
        "--release",
    ]));
    let guest_wasm = workspace_root.join("target/wasm32-wasip2/release/http_search_guest.wasm");
    let http_out = out_dir.join("http_search.component.wasm");
    std::fs::copy(&guest_wasm, &http_out).unwrap_or_else(|e| {
        panic!(
            "copy {} -> {}: {e} (install wasm32-wasip2: rustup target add wasm32-wasip2)",
            guest_wasm.display(),
            http_out.display()
        );
    });
}
