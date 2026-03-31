//! Compiles `fixtures/test_add.wat` to WebAssembly for tests and dev workflows.

fn main() {
    println!("cargo:rerun-if-changed=fixtures/test_add.wat");
    let wat = std::fs::read_to_string("fixtures/test_add.wat").expect("read wat");
    let wasm = wat::parse_str(&wat).expect("parse wat");
    let out = std::path::Path::new(&std::env::var_os("OUT_DIR").expect("OUT_DIR"))
        .join("test_add.wasm");
    std::fs::write(&out, wasm).expect("write wasm");
}
