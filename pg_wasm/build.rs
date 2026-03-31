//! Compiles fixture `.wat` files to WebAssembly for tests and dev workflows.

fn main() {
    let out = std::env::var_os("OUT_DIR").expect("OUT_DIR");
    let out_dir = std::path::Path::new(&out);
    for name in [
        "test_add",
        "test_echo_mem",
        "test_hooks",
        "test_wasi_fd_write",
    ] {
        let path = format!("fixtures/{name}.wat");
        println!("cargo:rerun-if-changed={path}");
        let wat = std::fs::read_to_string(&path).expect("read wat");
        let wasm = wat::parse_str(&wat).expect("parse wat");
        std::fs::write(out_dir.join(format!("{name}.wasm")), wasm).expect("write wasm");
    }

    let comp = std::path::Path::new("fixtures/test_add.component.wasm");
    println!("cargo:rerun-if-changed={}", comp.display());
    let dst = out_dir.join("test_add.component.wasm");
    std::fs::copy(comp, &dst).unwrap_or_else(|e| panic!("copy {}: {e}", comp.display()));
}
