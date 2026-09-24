// Node bench: the admin page's per-tick progress fingerprint (realtime.js).
// A: the shipped recursive canonicalize + stringify.
// B: variant - plain stringify compared first, canonical form only when the
//    plain form moved (the producer's key order is stable in practice; a
//    reordering costs one extra render, the dedup stays an optimization).
// C: variant - structural compare against the last rendered state without
//    building any JSON string (per-field shallow compare, recursion for
//    nested objects).
// Medians of interleaved batches, same conventions as the Python harness.

"use strict";

const FINGERPRINT_FIELDS = ["step", "percent", "detail", "data"];

function progressFingerprint(state) {
  const identity = {};
  for (const field of FINGERPRINT_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(state, field)) {
      identity[field] = state[field];
    }
  }
  if (Object.keys(identity).length === 0) return null;

  function canonicalize(value) {
    if (Array.isArray(value)) return value.map(canonicalize);
    if (value !== null && typeof value === "object") {
      return Object.fromEntries(
        Object.keys(value)
          .sort()
          .map((key) => [key, canonicalize(value[key])]),
      );
    }
    return value;
  }

  return JSON.stringify(canonicalize(identity));
}

function fingerprintB(state, last) {
  const identity = {};
  for (const field of FINGERPRINT_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(state, field)) {
      identity[field] = state[field];
    }
  }
  if (Object.keys(identity).length === 0) return null;
  const plain = JSON.stringify(identity);
  if (plain === last) return last;
  return progressFingerprint(identity);
}

function states(n, items) {
  const out = [];
  for (let i = 0; i < n; i++) {
    const s = { step: `step-${i % 5}`, percent: (i * 7) % 100, detail: "x".repeat(64), ts: `2026-09-23T12:00:${i % 60}Z` };
    if (items) {
      s.data = Array.from({ length: items }, (_, k) => ({
        sku: `SKU-${(i + k) % 50}`,
        qty: (i + k) % 5 + 1,
        price: ((i + k) % 17) * 1.5,
      }));
    }
    out.push(s);
  }
  return out;
}

function bench(name, fn, stateSeq, batches) {
  const last = null;
  fn(stateSeq[0], null); // warm
  const times = [];
  for (let b = 0; b < batches; b++) {
    const t0 = process.hrtime.bigint();
    let prev = null;
    for (const s of stateSeq) prev = fn(s, prev);
    times.push(Number(process.hrtime.bigint() - t0) / stateSeq.length);
  }
  times.sort((a, b) => a - b);
  const med = times[Math.floor(times.length / 2)];
  console.log(`${name.padEnd(52)} ${(med / 1000).toFixed(2)} us/tick`);
}

const small = states(2000, 0);
const big = states(2000, 24);
const identical = states(1, 24).concat(Array(1999).fill(states(1, 24)[0]));

bench("A canonicalize [small state, x2000 ticks]", progressFingerprint, small, 9);
bench("A canonicalize [data x24, x2000 ticks]", progressFingerprint, big, 9);
bench("A canonicalize [identical re-flush x2000]", (s) => progressFingerprint(s), identical, 9);
bench("B stringify-fast-path [data x24, x2000]", fingerprintB, big, 9);
