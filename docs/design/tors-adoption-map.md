# tors adoption map: inventory, path mapping, measured verdicts

An inventory of the `tors` native text/scan/hash toolkit (v0.10.1,
maturin/Rust, GIL-free cores) mapped against TaskQ's text- and
document-heavy paths, with every adoption candidate measured on its real
call shape. The question this spike answers: which tors primitives earn a
place on a per-job hot path, and which are capability we have no consumer
for.

Evaluation scripts: `benchmarks/tors_ab_adoptables.py` (single-thread
A/B, 61 interleaved rounds), `benchmarks/tors_contended.py` (N-thread
contention, the GIL-free question); machine-readable runs land in
`benchmarks/results/` (gitignored history, the house convention).
Methodology follows the
`bench_hotspots.py` house rules: interleaved batches, correctness
asserted before any timing is trusted.

Related docs: [sql-hotpath-followups.md](sql-hotpath-followups.md) (the
measurement house style this follows), `src/taskq/_json.py` and
`src/taskq/backend/_terminal.py` (the NUL-guard surface — **reserved**,
see the coordination note), `src/taskq/obs/_redact_exc.py` (the
redaction surface — **reserved**, same note), `benchmarks/README.md`.

---

## 1. Inventory

The base wheel's `.pyi` surface (`python/tors/__init__.pyi`), by family:

| Family | Names | Contract in one line |
|---|---|---|
| Normalize | `normalize`, `finalize`, `strip_controls`, `nfc/nfd/nfkc/nfkd`, `html_unescape`, `dedent` | `str→str` transforms, identity-return when no work |
| Scrub | `scrub_pii`, `scrub_pii_report`, `scrub_log_text` | PII/credential/log-text redaction with span reports |
| Replace/search | `replace_many`, `replace_many_masked`, `find_patterns` (+`_iter`), `count_matches`, `CompiledPatterns` | Aho-Corasick, leftmost-longest, compiled handles |
| Segment/measure | `grapheme_count`, `word_bounds` (+`_iter`), `word_count`, `sentence_bounds` (+`_iter`), `sentence_count` | UAX #29 segmentation |
| Encode/decode | `decode_utf8`, `finalize_utf8`, `decode_utf16`, `utf8_is_valid`, `utf16_is_valid`, `b64_encode_bytes`, `b64_decode`, `detect_encoding`, `json_is_valid` | untrusted-bytes parsing and gates |
| Scan/size | `contains_unescaped`, `find_unescaped`, `utf8_byte_len`, `utf16_byte_len` | escape-parity byte scan; UTF-8/16 length without the copy |
| Diff/similarity | `diff_opcodes` (+`_lines`), `similarity_ratio`, `get_close_matches`, `levenshtein`, `jaro`, `jaro_winkler`, `is_grounded` | Myers diff and edit-distance metrics, all with `deadline_ms` |
| JSON repair | `repair_json`, `repair_json_loads`, `repair_json_diagnostics` | LLM-output repair, schema-aligned, `deadline_ms` |
| Truncate | `truncate_to_bounds`, `truncate_ellipsis` | grapheme-safe cuts (word/sentence-aware, or hard with `…`) |
| Hash | `content_hash`, `merkle_root`, `merkle_diff`, `md5/sha1/sha256/sha512` `_hex`/`_digest`, `hmac_sha256_hex/_digest` | canonical-object hash, RFC 6962 Merkle, one-shot digests |
| UUID | `uuid4`, `uuid7` (+`_bytes`), `uuid7_timestamp_ms`, `uuid_version`, `uuid_parse` | RFC 9562 layout, strict parse |
| Chunk | `chunk_cdc` (FastCDC), `chunk_text` (+`_iter`), `chunk_by_words/sentences/paragraphs/lines` (+`_iter`), `chunk_hierarchical` | context-window/RAG chunking, byte-level CDC |
| Near-dup | `simhash64`, `simhash128`, `minhash_signature` | near-duplicate fingerprints (precision/recall sides) |
| Rank | `tf_idf`, `bm25_rank`, `CompiledLemmaDict`, `apply_pipeline` | stateless reranking and batch preprocessing |
| Phonetic | `soundex`, `metaphone`, `double_metaphone`, `nysiis`, `daitch_mokotoff`, `refined_soundex` | name-matching codes |
| Random | `random_string`, `random_hex`, `random_b62`, `random_b64url` | CSPRNG tokens (Lemire, no modulo bias); `seed=` is the fixture lane, never secrets |
| Charset gate | `first_invalid_charset`, `first_invalid_offender`, `CHARSET_*` (5) | batch codepoint allow-list validation |
| URL | `quote`, `quote_plus`, `unquote`, `unquote_plus` | stdlib-parity quoting, GIL-free |

Counts: **108 public names** in the base module (106 callables plus the
two compiled-handle classes `CompiledPatterns`/`CompiledLemmaDict`; the
`.pyi` declares 110 `def`/`class` entries including the two
introspection artifacts), 6 module constants, **43 async twins**
(`tors.aio`, generated against the stub), and **15 names on the optional
`tors.documents` surface** (PDF/DOCX/XLSX → markdown/text extraction,
`sniff`, `pdf_classify`/`pdf_extract`/`pdf_page_count`/`pdf_link_uris`;
a separate payload wheel — out of scope here, TaskQ reads no documents).
Measured runs: the JSON records in `benchmarks/results/tors-*.json`
(gitignored per `benchmarks/results/.gitignore` — rerun the scripts to
regenerate).

### Already aligned: the ported scrub chain

tors's history ports TaskQ's scrub chain with a parity battery, and three
families are **semantically TaskQ's own** rather than new capability:

- `scrub_log_text` (the `pg_detail_lines` / `uri_userinfo` /
  `libpq_conninfo_creds` rules) ↔ the exception/PG-error text redaction
  in `src/taskq/obs/`.
- `scrub_pii` / `scrub_pii_report` (contact + credential families) ↔ the
  payload-safety scrubbing layer.
- `contains_unescaped` / `find_unescaped` ↔ the JSONB NUL guard
  (`taskq._json.dumps_jsonb_str`'s escape-or-refuse split and
  `backend/_terminal.py`'s `_encoded_has_nul`): the escape-parity scan is
  exactly the "is this `\u0000` spelling a real NUL or the literal six
  characters" question those sites answer today.

These three surfaces are **reserved** — another workstream is wiring them
now — so this map records them as ALIGNED-but-not-mine and measures
everything else. Verdicts below are for the *unreserved* surface only.

---

## 2. The map: TaskQ path → tors candidate

Walk of `src/taskq/` for text/document-heavy operations, with what the
walk actually found (several mission-listed candidate areas have **no
TaskQ consumer** — recorded so the next sweep doesn't re-walk them):

| TaskQ path | What runs today | tors candidate | Verdict |
|---|---|---|---|
| Terminal write, dict-result arm (`backend/_terminal.py:386`) | `len(serialized_result.encode("utf-8"))` — a full re-encode of the ≤64 KiB serialized result, per successful job | `utf8_byte_len` | **ADOPT (parked — reserved file)** |
| Enqueue pre-flight, idempotency caps (`client/_args.py:174,181`) | `len(key.encode())` / `len(scope.encode())`, ≤1024-byte strings, per enqueue | `utf8_byte_len` | SKIP (measured: slower on the common shape) |
| ID generation (`_ids.py` `new_base62`, `new_uuid`) | `os.urandom`+modulo+divmod loop; `uuid_utils.uuid7()` wrapped in `UUID(bytes=…)` | `random_b62`, `uuid7`/`uuid7_bytes` | SKIP (measured: `random_b62` 2.5x slower; uuid parity at 0.99x) |
| Payload-fingerprint log line (`obs/_structlog.py` `redact_payload`) | `orjson.dumps` + `hashlib.sha256().hexdigest()[:16]` | `sha256_hex`/`sha256_digest` | SKIP (measured: 1.03x — a wash against hashlib, same C core) |
| Schema checksum (`migrate.py:169`) | `hashlib.sha256(render.encode()).hexdigest()` | `sha256_hex` | SKIP (cold path, wash) |
| Event-detail render (`cli.py` `_format_event_detail`) | `" ".join(str(detail).split())`, CLI-only | `strip_controls`/`normalize` | SKIP (semantics not aligned; CLI-cold) |
| Admin blob/traceback display (`web/admin/jobs.py` `_truncate_traceback`) | slice + `"... (N more characters)"` suffix | `truncate_ellipsis` | SKIP (different marker contract; measured ~250x slower) |
| Admin redirect (one `quote_plus` in `web/admin/ops.py:437`) | stdlib | `quote_plus` | SKIP (one line, cold) |
| Tag/filter matching (`client/_args.py`, SQL `metadata @>`) | Python length checks; matching is SQL-side jsonb containment | `find_patterns`/`CompiledPatterns` | SKIP (no Python-side pattern scanning exists) |
| Event detail / progress jsonb (`_json.py`, `progress/`) | orjson dumps + the reserved NUL guard | `json_is_valid` | SKIP (a pre-gate double-parses in front of orjson) |
| Retry payloads / batch arg normalization | exact-key SQL dedup (`idempotency`/`unique_for`); no content hashing, no similarity anywhere | `simhash64/128`, `minhash_signature`, `is_grounded`, `get_close_matches` | SKIP (no near-dup consumer exists; `difflib`/`SequenceMatcher` appear nowhere in `src/`) |
| Migration ledger / event streams (`migrate.py`) | per-file SHA-256 checksums, drift flagged at runner start | `merkle_root`/`merkle_diff`, `chunk_cdc` | SKIP (chunk-level verification maps to no real need: the ledger verifies schema files once at boot, not byte streams at runtime) |
| Payload canonical hashing | — none; idempotency keys are caller-supplied strings, never derived from payload content | `content_hash` | SKIP (no canonical-hash call site) |
| Document chunking | — none; TaskQ is a queue, not a document pipeline | the `chunk_*` family, `documents` extraction | SKIP (no consumer) |
| Id/scope charset validation | length-only (no charset restriction by design) | `first_invalid_charset` | SKIP (no allow-list contract to port) |

## 3. Measurements

Environment: Linux x86_64, CPython 3.13, tors 0.10.1 (built from the
pinned tree, wheel `tors-0.10.1-cp310-abi3`), TaskQ editable install.
p50/p99 over 61 interleaved A/B rounds (`benchmarks/tors_ab_adoptables.py`);
contention cells run 400 ops/thread with a start barrier
(`benchmarks/tors_contended.py`). Correctness asserted for every row
(byte-length equality, hex-digest equality, alphabet/length pins) before
timing was read.

### 3.1 Single-thread A/B (p50/p99 ns/op, A = current, B = tors)

| bench | A p50 | A p99 | B p50 | B p99 | p50 speedup |
|---|---:|---:|---:|---:|---:|
| idempotency key, 36 B ASCII | 40 ns | 47 ns | 55 ns | 91 ns | **0.72x** |
| idempotency scope, 1024 cp CJK | 69 ns | 114 ns | 65 ns | 109 ns | 1.06x |
| result re-encode, 64 KiB | 687 ns | 879 ns | 58 ns | 108 ns | **11.80x** |
| `new_base62(8)` vs `tors.random_b62(8)` | 920 ns | 1.08 µs | 2.31 µs | 2.47 µs | **0.40x** |
| `new_uuid()` vs `UUID(bytes=tors.uuid7_bytes())` | 597 ns | 647 ns | 602 ns | 713 ns | 0.99x |
| bare `uuid_utils.uuid7().bytes` vs `tors.uuid7_bytes()` | 660 ns | 828 ns | 122 ns | 137 ns | 5.42x |
| sha256 4 KiB hex, hashlib vs tors | 1.98 µs | 2.16 µs | 1.91 µs | 2.08 µs | 1.03x |
| sha256 4 KiB `[:16]`, hashlib vs tors | 2.03 µs | 2.18 µs | 1.97 µs | 2.08 µs | 1.03x |
| detail whitespace collapse vs `strip_controls` | 217 ns | 1.02 µs | 189 ns | 993 ns | 1.15x |
| traceback cut vs `truncate_ellipsis` | 227 ns | 377 ns | 55.74 µs | 86.76 µs | **0.004x** |

Readings:

- **`utf8_byte_len`'s win is size-dependent and steep.** At 64 KiB it is
  11.8x at p50 (687 → 58 ns) with the p99 at 108 ns: tors reads the
  str's cached UTF-8 view (one field read after the first call) where
  `str.encode` copies the whole string. At 36 bytes the relationship
  inverts (0.72x) — the Python→Rust call boundary costs more than the
  copy for small strings. The crossover sits well below 1 KiB.
- **ID generation does not benefit.** TaskQ's `_random_base62` draws one
  `os.urandom` blob for all 8 characters and reduces once; tors's
  `random_b62` is 2.5x slower at that size (its sampling machinery does
  not beat the one-draw loop on 8 characters). `uuid_utils` is already a
  native
  uuid7; tors's is a 0.99x parity (the bare-bytes spelling is 5.4x
  faster, but TaskQ's contract is a `UUID` object, so the wrap dominates
  either way).
- **Hashing is a wash.** `hashlib.sha256` and `tors.sha256_hex` run the
  same OpenSSL core; tors's detach overhead and hashlib's C speed cancel
  (1.03x both spellings). No adoption case on any of TaskQ's hashing
  sites.

### 3.2 Contended (wall per op, T threads hammering the same primitive)

`len(s.encode())` vs `tors.utf8_byte_len`:

| size | 1 thr A/B | 2 thr A/B | 4 thr A/B | 8 thr A/B |
|---|---|---|---|---|
| 1 KiB | 171 / 131 ns (1.30x) | 114 / 97 ns (1.17x) | 98 / 132 ns (0.74x) | 115 / 105 ns (1.09x) |
| 64 KiB | 745 / 89 ns (**8.3x**) | 758 / 80 ns (**9.5x**) | 776 / 95 ns (**8.1x**) | 797 / 134 ns (**6.0x**) |
| 1 MiB | 19.6 / 0.18 µs (**108x**) | 18.2 / 0.11 µs (**168x**) | 17.3 / 0.16 µs (**107x**) | 17.6 / 0.15 µs (**115x**) |

`hashlib.sha256().hexdigest()` vs `tors.sha256_hex` (4 KiB): 0.95x at
1 thread, 1.09x at 2, 0.69x at 4, 0.89x at 8 — a wash that never clears
the noise floor. **No contended case for any hashing site.**

The 1 KiB contended cells are noise-bound (both sides sit at the
thread-switch floor; the speedup flips sign across runs) — no verdict is
read from them, matching the single-thread finding that small strings
have no win.

The GIL-free design shows exactly where it should: the contended win at
64 KiB (~6–9.5x across thread counts — the encode side serializes on
the GIL while tors's detach overlaps) and the 1 MiB band (107–168x,
where the GIL-held encode's ~18 µs wall stops moving while tors's does
not). TaskQ's shipped caps keep results at ≤64 KiB, so the 1 MiB band is
headroom evidence (operator-raised `result_max_bytes`), not a current
path. Both runs' cells reproduce directionally (the first run measured
9.4x at 64 KiB/1 thread and 76–110x at 1 MiB); the recorded JSON is the
second run.

---

## 4. Verdicts

### ADOPT — 1

**`tors.utf8_byte_len` at the terminal write's result-size check
(`backend/_terminal.py:386`).** Mapped hot path (every successful job
with a dict result pays a full re-encode of its serialized result to
learn a number tors keeps O(1)), measured win (11.8x p50 single-thread,
~6–9.5x contended at the real 64 KiB cap; 687 → 58 ns), and a clean
parity path: `tors.utf8_byte_len(s) == len(s.encode("utf-8"))` is pinned
byte-exact (same UnicodeEncodeError contract on lone surrogates, which
this site can never hit — the str is orjson's own decode).

**Parked on the reserved file.** `_terminal.py` is the NUL-scan
workstream's file; this swap lands with or after that work as a
follow-up, not in parallel with it. The site's `str.encode` stays as-is
until then. The same primitive should NOT be carried to the
enqueue pre-flight's key/scope caps — see the first SKIP.

### MAYBE — 1

**`tors.utf8_byte_len` (or `utf16_byte_len`) as the general
"size-cap-in-front-of-a-store" primitive, if a deployment profile grows
blob caps past `MAX_RESULT_BYTES`.** The 1 MiB contended band (107–168x)
is real, but no current TaskQ path carries MB-scale strs through a byte
cap per event; adopting for it today would be speculation. The design
decision that unlocks it: an operator raising `result_max_bytes` (or a
future blob column) turns the terminal-write ADOPT from a ~12x win into
a ~100x one. Re-measure if that setting's profile ever shows up.

### SKIP — the rest, with the numbers

- **Idempotency key/scope byte caps (`client/_args.py`)** — 0.72x on the
  common ASCII shape (40 → 55 ns): the boundary cost beats the copy.
  The 1.06x only appears at a 1024-codepoint CJK scope, which is not
  the shape keys actually have. A per-enqueue regression to save nothing.
- **ID generation** — `random_b62` is 2.5x slower than the urandom+
  modulo helper; uuid7 is 0.99x parity through the `UUID` wrapper the
  contract needs. TaskQ's IDs are not a bottleneck and tors doesn't
  improve them.
- **Hashing everywhere** (`redact_payload`, migrate checksums) — 1.03x
  single-thread; contended cells never clear the noise floor. hashlib
  is the same C; keep it.
- **`truncate_ellipsis` for admin display cuts** — ~250x slower (the
  grapheme backoff does real segmentation work) and the admin's
  "... (N more characters)" marker is a different contract than `…`.
- **`strip_controls`/`normalize` for the CLI detail render** — 1.15x but
  not semantics-aligned (all-whitespace collapse vs C0/DEL only), on a
  CLI-only cold path.
- **Diff/similarity family** — the admin renders no diffs; `difflib`
  and `SequenceMatcher` appear nowhere in `src/`. `deadline_ms`-bounded
  Myers is nice capability with no consumer here.
- **Near-dup family (`simhash`, `minhash`, `is_grounded`)** — TaskQ's
  dedup is exact-key SQL (idempotency composite unique, `unique_for`
  state checks); retry payloads are stored, never content-deduped; batch
  arg normalization compares identity keys as strings. There is no
  fuzzy-matching need to accelerate — adopting would be inventing a
  feature, not speeding one up.
- **`content_hash`** — no canonical-object-hash call site exists
  (idempotency keys are caller-supplied, not payload-derived).
- **`merkle_root`/`merkle_diff`/`chunk_cdc`** — the migration ledger
  verifies per-file SHA-256s once at runner start; event streams are
  consumed through SQL, not verified as chunk trees. No real need.
- **`json_is_valid`** — a validity gate in front of TaskQ's orjson
  parse-and-use paths would double-parse; the current code's
  try/except around `dumps`/`loads` is already the cheap answer.
- **`first_invalid_charset`** — TaskQ deliberately restricts id/scope
  charset nowhere (length-only), so there is no allow-list to port.
- **The `chunk_*` and `documents` surfaces** — TaskQ enqueues jobs; it
  does not chunk documents or parse PDFs. No consumer, now or planned.
- **`quote`/`quote_plus`** — one redirect-fragment call in the admin;
  cold, stdlib fine.

---

## 5. Coordination note

The reserved surfaces and their tors twins, for whichever agent lands
them: `scrub_log_text`/`scrub_pii`/`scrub_pii_report` (the redaction
workstream, `obs/`) and `contains_unescaped`/`find_unescaped` (the NUL
scan, `_json.py` + `_terminal.py`). The ADOPT row above touches
`_terminal.py` and must sequence behind the NUL-scan workstream's change
to that file. No code in this spike touches either surface: this branch
carries the two benchmark scripts, their results, and this document
only.
