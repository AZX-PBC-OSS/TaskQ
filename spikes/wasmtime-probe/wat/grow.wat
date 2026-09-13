;; grow.wat — memory that the host can ask to grow, page by page.
;; memory.grow returns the previous page count, or -1 if growth was refused
;; (e.g. by the store's memory limiter). `grow_until_fail` grows until the
;; limiter refuses and reports total pages acquired.
(module
  (memory $m 1)
  (func (export "grow_by") (param $pages i32) (result i32)
    (memory.grow (local.get $pages)))
  (func (export "current_pages") (result i32) (memory.size))
  (func (export "grow_until_fail") (result i32)
    (local $total i32)
    (local $r i32)
    (block $done
      (loop $l
        (local.set $r (memory.grow (i32.const 1)))
        (br_if $done (i32.eq (local.get $r) (i32.const -1)))
        (local.set $total (i32.add (local.get $total) (i32.const 1)))
        (br $l)))
    (local.get $total)))
