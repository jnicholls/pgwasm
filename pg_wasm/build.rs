//! Compiles fixture `.wat` files to WebAssembly for tests and dev workflows.

fn main() {
    let out = std::env::var_os("OUT_DIR").expect("OUT_DIR");
    let out_dir = std::path::Path::new(&out);
    for name in ["test_add", "test_echo_mem"] {
        let path = format!("fixtures/{name}.wat");
        println!("cargo:rerun-if-changed={path}");
        let wat = std::fs::read_to_string(&path).expect("read wat");
        let wasm = wat::parse_str(&wat).expect("parse wat");
        std::fs::write(out_dir.join(format!("{name}.wasm")), wasm).expect("write wasm");
    }
}
