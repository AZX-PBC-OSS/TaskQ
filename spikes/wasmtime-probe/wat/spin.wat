;; spin.wat — infinite busy loop with a monotonic counter in a global.
;; `spin` never returns; `read` lets the host verify progress across
;; epoch-interrupted resumptions.
(module
  (global $g (mut i64) (i64.const 0))
  (func (export "spin")
    (loop $l
      (global.set $g (i64.add (global.get $g) (i64.const 1)))
      (br $l)))
  (func (export "read") (result i64) (global.get $g)))
