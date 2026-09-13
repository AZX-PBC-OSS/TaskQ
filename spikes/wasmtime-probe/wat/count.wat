;; count.wat — bounded counting loop: n iterations, then returns i (== n).
;; Used for fuel-overhead measurement (1M-iteration workload).
(module
  (func (export "count") (param $n i64) (result i64)
    (local $i i64)
    (loop $l
      (local.set $i (i64.add (local.get $i) (i64.const 1)))
      (br_if $l (i64.lt_u (local.get $i) (local.get $n))))
    (local.get $i)))
