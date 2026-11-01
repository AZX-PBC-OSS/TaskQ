"""The scenario grounds: seven runnable examples, one per production
behavior. Every script drives real flows against a real Postgres (and a
real Redis where a worker is involved), prints the states it exercised
and the rows it verified, and ends green only when every row told the
truth it promised. See the README in this folder and
``docs/guides/scenario-grounds.md`` for the tour."""
