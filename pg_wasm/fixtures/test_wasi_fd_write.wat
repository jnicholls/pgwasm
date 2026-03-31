;; Core module that imports WASI preview1 but never calls it; exports forty_two.
(module
  (type $t (func (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (type $t)))
  (memory 1)
  (export "memory" (memory 0))
  (func (export "forty_two") (result i32)
    (i32.const 42)
  )
)
