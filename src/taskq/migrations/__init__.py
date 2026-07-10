"""Forward-only SQL migrations.

Naming convention:

    {major.minor.patch}_{nn}_{pre|post}_{description}.sql

* ``pre``  — runs before new code is deployed (additive changes, new tables).
* ``post`` — runs after new code is deployed (drops, backfills the new code wrote).

Files contain literal ``{schema}`` placeholders that are substituted at apply
time with the configured schema name. Within a SQL file, escape literal curly
braces by doubling them (``{{`` → ``{``) — the runner uses ``str.format``.
"""
