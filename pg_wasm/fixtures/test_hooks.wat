(module
  (func $hook_nop)
  (func $hook_rc)
  (func (export "add") (param i32 i32) (result i32)
    local.get 0
    local.get 1
    i32.add)
  (export "wasm_nop" (func $hook_nop))
  (export "wasm_rc" (func $hook_rc)))
