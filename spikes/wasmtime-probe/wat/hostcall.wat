;; hostcall.wat — wasm function that calls an imported host function
;; (simulating an actor making a DB-ish call) and returns the result.
(module
  (import "host" "db" (func $db (param i64) (result i64)))
  (func (export "call_db") (param i64) (result i64)
    (call $db (local.get 0))))
