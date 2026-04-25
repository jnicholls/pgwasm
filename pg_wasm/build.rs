//! Builds tiny Wasm guests under `test_guests/` for `#[pg_test]` host-interface checks.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=wit/host.wit");
    println!("cargo:rerun-if-changed=test_guests/log_guest/src/lib.rs");
    println!("cargo:rerun-if-changed=test_guests/log_guest/wit/guest.wit");
    println!("cargo:rerun-if-changed=test_guests/log_guest/wit/deps/pg-wasm-host/host.wit");
    println!("cargo:rerun-if-changed=test_guests/log_guest/Cargo.toml");
    println!("cargo:rerun-if-changed=test_guests/query_guest/src/lib.rs");
    println!("cargo:rerun-if-changed=test_guests/query_guest/wit/guest.wit");
    println!("cargo:rerun-if-changed=test_guests/query_guest/wit/deps/pg-wasm-host/host.wit");
    println!("cargo:rerun-if-changed=test_guests/query_guest/Cargo.toml");
    println!("cargo:rerun-if-changed=test_guests/write_query_guest/src/lib.rs");
    println!("cargo:rerun-if-changed=test_guests/write_query_guest/wit/guest.wit");
    println!("cargo:rerun-if-changed=test_guests/write_query_guest/wit/deps/pg-wasm-host/host.wit");
    println!("cargo:rerun-if-changed=test_guests/write_query_guest/Cargo.toml");

    let guest_target_dir = manifest_dir.join("target/guest_build");
    let _ = std::fs::create_dir_all(&guest_target_dir);

    for (name, rel_manifest, crate_name) in [
        ("log_guest", "test_guests/log_guest/Cargo.toml", "log_guest"),
        (
            "query_guest",
            "test_guests/query_guest/Cargo.toml",
            "query_guest",
        ),
        (
            "write_query_guest",
            "test_guests/write_query_guest/Cargo.toml",
            "write_query_guest",
        ),
    ] {
        let manifest_path = manifest_dir.join(rel_manifest);
        let status = Command::new("cargo")
            .current_dir(&manifest_dir)
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
                manifest_path.to_str().expect("utf8 manifest path"),
                "--target-dir",
                guest_target_dir.to_str().expect("utf8 target dir"),
            ])
            .status()
            .unwrap_or_else(|e| panic!("failed to spawn cargo build for {name}: {e}"));
        if !status.success() {
            panic!("cargo build for wasm guest `{name}` failed with {status}");
        }
        let wasm_src = guest_target_dir
            .join("wasm32-wasip2/release")
            .join(format!("{crate_name}.wasm"));
        if !wasm_src.is_file() {
            panic!("expected guest wasm at {}", wasm_src.display());
        }
        let wasm_dst = out_dir.join(format!("{name}.wasm"));
        std::fs::copy(&wasm_src, &wasm_dst).unwrap_or_else(|e| {
            panic!(
                "copy guest wasm from {} to {}: {e}",
                wasm_src.display(),
                wasm_dst.display()
            );
        });
    }
}
