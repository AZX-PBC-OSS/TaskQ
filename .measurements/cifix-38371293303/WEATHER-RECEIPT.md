# run 38371293303's first-attempt weather red (test 3.12) + the local repro
- the leg red: 2 failed — test_batch_reset_hotspot_pg.py::test_hook_per_job_wall_is_terminal_update_class_under_contention (the contended ratio pin: hook 6.813ms vs bare 1.613ms = 4.2x vs the 4x budget — a wall-clock RATIO under contention) and test_timescale_retention_interplay.py::test_chunk_drop_does_not_advance_the_event_watermark (asyncpg InternalServerError: relkind for chunk is invalid — the timescale container's async drop race).
- the same run's test 3.13 + test 3.14 legs: GREEN, same suite, same commit.
- the diff 8b8a6e76c..30c45fbde: measurement-only (the two tests untouched by the lane).
- the local repro, solo x2: GREEN x2 (11.14s / 10.90s).
- the verdict: the pre-existing flake class (the door-edges report's documented lottery), not a regression. No weakening made; no timeout widened.
- the re-run: test 3.12 GREEN in 48m46s; build GREEN; the run completed SUCCESS.
