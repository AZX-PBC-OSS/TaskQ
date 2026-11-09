"""The workflows feature demo (ledger E1): a runnable ``WorkflowApp``
that exercises the branch's two feature families end to end — the
capacity surface (``max_concurrent`` /
``reservations`` / ``result_ttl``) and the retention arms — with the
map's failing-item collect, a HITL gate, a durable timer and a typed
sink, all in one graph.

Start at ``README.md`` (the run commands + the WHAT-TO-OBSERVE list),
or run the story directly:

    uv run python -m examples.flows_demo.run_demo
"""
