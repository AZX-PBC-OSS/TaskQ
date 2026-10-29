"""DocShelf — a document-intelligence pipeline built on TaskQ's workflow engine.

The adoption-sim product: documents arrive from an upload feed, each is
chunked (the map face), each chunk's extraction calls a mock extraction
API (flaky, slow, sometimes timing out), documents with many chunks spawn
child flows for the expensive sub-analyses (spawn + join), the per-document
results aggregate, documents branch by class (the typed route), and the
shelf publishes a digest. A nightly re-index runs on the durable timer with
a durable cursor (only NEW documents re-processed).

Every external system is mocked at the boundary; the TaskQ machinery is
real (real Postgres, real rows, real workers).
"""

__version__ = "1.0.0"
