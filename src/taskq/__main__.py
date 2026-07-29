"""``python -m taskq`` entry point.

Required for any code path that spawns the CLI via ``sys.executable -m
taskq`` — the workgroup supervisor's ``_spawn_child`` does exactly that
(``src/taskq/worker/workgroup.py``), so without this module every spawned
worker dies with ``No module named taskq.__main__``.
"""

from taskq.cli import main

if __name__ == "__main__":
    main()
