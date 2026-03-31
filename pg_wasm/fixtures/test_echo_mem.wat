(module
  (memory 1)
  (export "memory" (memory 0))
  (func (export "echo_mem") (param i32 i32) (result i32)
    (local $dst i32)
    local.get 0
    local.get 1
    i32.add
    i32.const 7
    i32.add
    i32.const -8
    i32.and
    local.set $dst
    local.get $dst
    local.get 0
    local.get 1
    memory.copy
    local.get 1))
