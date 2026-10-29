"""Bulk ledger import — a bulk ingest workflow on the TaskQ workflows engine.

The scenario is the textbook one: a feed of records arrives, it is
partitioned into chunks, the chunks fan out, each chunk validates its
rows, the valid rows upsert idempotently, the invalid rows quarantine
with reasons, and the fan-in produces a typed completion receipt.

Modules:

- ``models.py``   — the typed wire models (rows, chunks, receipts).
- ``feed.py``     — the deterministic feed generator.
- ``store.py``    — the idempotent upsert + quarantine store.
- ``workflow.py`` — the workflow authoring (steps, map, route, receipt).
- ``run_import.py`` — the run door: create → listen → drive → receipt.
- ``drills.py``   — the drill suite (idempotency, quarantine, resume,
  cache, throughput).
"""
