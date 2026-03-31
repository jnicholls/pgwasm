(module
  (memory 1)
  (func (export "add") (param i32 i32) (result i32)
    local.get 0
    local.get 1
    i32.add)
  (func (export "forty_two") (result i32)
    i32.const 42))
