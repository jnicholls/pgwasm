//! Builds tiny Wasm guests under `tests/guests/` for `#[pg_test]` host-interface checks.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    // macOS: Postgres symbols are resolved when the extension is loaded, not at link time.
    // Use a build-script link arg so this applies only to the `pgwasm` cdylib — unlike
    // `pgwasm/.cargo/config.toml`, it is not picked up by nested `cargo build` invocations
    // (e.g. wasm32-wasip2 guests), which would break `wasm-component-ld`.
    if let Ok(target) = std::env::var("TARGET")
        && target.contains("apple-darwin")
    {
        println!("cargo:rustc-link-arg=-Wl,-undefined,dynamic_lookup");
    }

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=wit/host.wit");
    println!("cargo:rerun-if-changed=tests/guests/log_guest/src/lib.rs");
    println!("cargo:rerun-if-changed=tests/guests/log_guest/wit/guest.wit");
    println!("cargo:rerun-if-changed=tests/guests/log_guest/wit/deps/pgwasm-host/host.wit");
    println!("cargo:rerun-if-changed=tests/guests/log_guest/Cargo.toml");
    println!("cargo:rerun-if-changed=tests/guests/query_guest/src/lib.rs");
    println!("cargo:rerun-if-changed=tests/guests/query_guest/wit/guest.wit");
    println!("cargo:rerun-if-changed=tests/guests/query_guest/wit/deps/pgwasm-host/host.wit");
    println!("cargo:rerun-if-changed=tests/guests/query_guest/Cargo.toml");
    println!("cargo:rerun-if-changed=tests/guests/write_query_guest/src/lib.rs");
    println!("cargo:rerun-if-changed=tests/guests/write_query_guest/wit/guest.wit");
    println!("cargo:rerun-if-changed=tests/guests/write_query_guest/wit/deps/pgwasm-host/host.wit");
    println!("cargo:rerun-if-changed=tests/guests/write_query_guest/Cargo.toml");

    let guest_target_dir = out_dir.join("guest_build");
    let _ = std::fs::create_dir_all(&guest_target_dir);

    for (name, rel_manifest, crate_name) in [
        (
            "log_guest",
            "tests/guests/log_guest/Cargo.toml",
            "log_guest",
        ),
        (
            "query_guest",
            "tests/guests/query_guest/Cargo.toml",
            "query_guest",
        ),
        (
            "write_query_guest",
            "tests/guests/write_query_guest/Cargo.toml",
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
