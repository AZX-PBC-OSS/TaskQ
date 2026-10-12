# THE 3AM PINS — the red-first receipts (`tests/test_wf_3am_pins.py`)

The observability lane's drills (stuck run / failing run / wedged fan-in /
poison / lost worker / audit / the machinery's own face), run at the base
tip `0e2f5cc09` against a live PG 18.6. Each pin below was collected RED
against the convicted surface, then flipped GREEN by its cure (the
cured surface named). Evidence tier: the drills' captured CLI output.

## 1. `test_healthy_join_wait_gets_the_join_wait_arm_not_the_retry_remedy`

RED (captured, base tip):

```
>       assert "JOIN-WAIT" in joined
E       assert 'JOIN-WAIT' in "  collect: BLOCKED — join (source: the row's
        metadata.blocking_reason stamp)\n       remedy: retry the failed
        upstream node — taskq flows status run-1 names it"
```

The convicted face: the blocking-reason arm caught the bare `'join'`
stamp (every emitted join carries it — the HEALTHY shape) and printed
the retry-the-failed-upstream remedy for a join whose parents are alive.
CURED: `workflows/_cli.py`'s `stuck_lines` routes reason `'join'` to the
honest join-wait arm (`remedy: none — the join fires when its parents
finalize`).

## 2. `test_failed_parent_line_names_the_dead_parent`

RED: `TypeError: FlowNodeRow.__init__() got an unexpected keyword
argument 'failed_parent'` — the CLI's row type could not even CARRY the
stamp the T06 cascade writes (`metadata.failed_parent`). CURED: the
field rides the CLI's supplement read; the line prints the dead parent's
id.

## 3. `test_flow_dead_names_the_fence_and_the_dead_path`

RED: the flow_dead line was the generic else-arm — "remedy: retry the
failed upstream node" (a dead path's remedy reads as if a retry revives
it). CURED: the flow_dead arm — the fence's record, the path is dead,
`taskq flows cancel` named beside the retry.

## 4. `test_running_node_age_is_named_with_the_lease_check_remedy` /
`test_running_face_is_silent_for_a_fresh_row`

RED: `TypeError: ... unexpected keyword argument 'started_at'` — no
surface carried a running row's age; a hung body read as a bare
"1 running" count (the stuck run's drill: time-to-diagnosis was
infinite). CURED: `format_flow_status`'s RUNNING face — each running row
older than a minute names its age (source: `started_at`) + the lease
check; a fresh row renders no line.

## 5. `test_job_show_names_the_lapsed_lease` / `test_job_show_names_a_fresh_lease_as_held` / `test_job_show_omits_the_lease_face_for_terminal_rows`

RED: `job show` printed no lease columns — "the worker died" vs "the
work is slow" required raw SQL on `jobs.lock_expires_at` (the lost
worker's drill: the row held the truth, the CLI refused to say it).
CURED: the lease face — `locked_by_worker` / `last_heartbeat_at` /
`lock_expires_at` + the LAPSED verdict (the reclaim sweep's 30 s bound,
the no-leader caveat, `taskq job events` as the death's record), rendered
for live rows only.

## 6. `test_job_show_renders_the_cache_hit_receipt` /
`test_node_panel_renders_the_cache_hit_receipt`

RED: the cross-run cache's receipt (`metadata.wf_cache_hit` — the
address + the producing run + the store instant) appeared on NO surface;
a cache-hit step read exactly like an executed one (the audit drill's
provenance gap). CURED: `job show`'s `cache_hit:` block + the explorer's
node panel `cache_hit` field (decoded from the jsonb string in
`_wf_actions.py`, rendered in `workflows.js`).

## 7. `test_a_crashed_terminal_node_renders_its_story`

RED: a `crashed` terminal row produced NO why-stuck line — the run read
"1 crashed" with no class, no message, no remedy (the terminal-crash
fold's face was missing from the why-stuck arm). CURED: the failed arm
folds `crashed`/`abandoned` with the derivation (the §17.5 row-0's own
fold, now on the CLI's face).
