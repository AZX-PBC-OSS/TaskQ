# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.2...v0.3.0) (2026-09-04)


### ⚠ BREAKING CHANGES

* page every job-list ordering, by ordering id with the sort column
* **obs:** drop identity values from metric dimensions before they throttle ingestion
* **web:** admin pages could be framed and UI-redressed; CSRF cookie lost Secure behind TLS termination
* **consumer:** validate payload before acquire; pass the validated model
* **api:** export the sanitized validate_actor_payload from the public API
* **ratelimit:** refund the store that actually paid, not the configured one
* EnqueueArgs.scheduled_at is now None-able and no longer defaults to a client-side now() stamp — "immediate" enqueue passes None and the server stamps it; Backend implementations that require a non-None datetime now fail loudly. The raw schedule_to_close datetime form is deprecated in favor of schedule_to_close_interval. Rate-limit Redis scripts derive time from redis.call('TIME') (caller-supplied now ARGV removed). Full suite 5122 passed; pyright, ruff, mkdocs, uv lock --check all clean.
* **deps:** dotenvmodel 0.6.3 -> 1.0.0. Real environment variables now take precedence over .env file values (previously .env files overwrote os.environ); set DOTENV_OVERRIDE=true or pass override=True to restore files-beat-env-vars. TaskQSettings.load() no longer mutates os.environ. An invalid TaskQ(redis_url=...) now raises TypeCoercionError at open() (previously a late ValueError from redis-py) and an empty or whitespace redis_url raises ValueError at construction. AppSettings subclasses whose string field defaults contain ${VAR} now interpolate them at load time (unset references resolve to "").
* **testing:** make_integration_settings/make_integration_settings_dict no longer default to the deterministic per-worker schema tq_<worker>; the default is a unique per-call name (explicit schema_name= unchanged).
* **consumer:** always pass validated_payload to acquire_for_actor
* **ratelimit:** make payload_type required, add .typed() to both keyed refs
* **actor-config:** sync_actor_config (and therefore worker startup) no longer raises ActorConfigDriftList for a max_concurrent, max_pending, or result_ttl mismatch between the registered actor and the stored row — only queue and metadata mismatches still raise. ActorConfigDriftError.field narrows from Literal["max_concurrent", "max_pending", "queue", "result_ttl", "metadata"] to Literal["queue", "metadata"]. --force-update-actor-config / TASKQ_FORCE_UPDATE_ACTOR_CONFIG no longer affect capacity fields at all — they are unconditionally preserved by the UPSERT once a row exists. Consumers who relied on a differing @actor(...) literal fatally refusing worker startup for capacity fields must now use `taskq actor-config set` to change stored capacity going forward; a stale literal is only logged.
* **actor-config:** sync_actor_config (and therefore worker startup) no longer raises ActorConfigDriftList for a max_concurrent, max_pending, or result_ttl mismatch between the registered actor and the stored row — only queue and metadata mismatches still raise. ActorConfigDriftError.field narrows from Literal["max_concurrent", "max_pending", "queue", "result_ttl", "metadata"] to Literal["queue", "metadata"]. --force-update-actor-config / TASKQ_FORCE_UPDATE_ACTOR_CONFIG no longer affect capacity fields at all — they are unconditionally preserved by the UPSERT once a row exists. Consumers who relied on a differing @actor(...) literal fatally refusing worker startup for capacity fields must now use `taskq actor-config set` to change stored capacity going forward; a stale literal is only logged.

### Features

* accept init= per-connection hook in make_pg_pool_factory ([#31](https://github.com/AZX-PBC-OSS/TaskQ/issues/31)) ([#34](https://github.com/AZX-PBC-OSS/TaskQ/issues/34)) ([c6fd05a](https://github.com/AZX-PBC-OSS/TaskQ/commit/c6fd05aba0d5a017651c7fe8306d2c1a80bc35a6))
* accept primitive instances in actor rate_limits/reservations declarations ([3503b82](https://github.com/AZX-PBC-OSS/TaskQ/commit/3503b823c1e29033126ea93ef1a9c6bee865715f))
* acquire_for_actor normalizes primitive instances to .name ([599107e](https://github.com/AZX-PBC-OSS/TaskQ/commit/599107ed8d0c8a92b01626e8225e8fcca6f4f48a))
* **actor-config:** make capacity fields operator-owned, not code-asserted ([abc38d1](https://github.com/AZX-PBC-OSS/TaskQ/commit/abc38d104f369d0f0bbabd3a96c9acc00bf28cd2))
* **actor-config:** make capacity fields operator-owned, not code-asserted ([#33](https://github.com/AZX-PBC-OSS/TaskQ/issues/33)) ([07a6cfe](https://github.com/AZX-PBC-OSS/TaskQ/commit/07a6cfe70033dc1354c40f8c63a30dc732a3df10))
* **actor-config:** make max_pending genuinely operator-owned at enqueue time ([76e084a](https://github.com/AZX-PBC-OSS/TaskQ/commit/76e084aa73a523ae08882d954f9f6cb60abbcbaa))
* add 'taskq actor-config deregister' CLI command ([fc7af6e](https://github.com/AZX-PBC-OSS/TaskQ/commit/fc7af6e6b28b16bab506c68b33ed926bfc101a36))
* add actor deregistration exception hierarchy ([66b6c41](https://github.com/AZX-PBC-OSS/TaskQ/commit/66b6c41c707aa03a12d9ecaa12145aa1ac4a2ef2))
* add ActorsClient pool-wrapping facade ([4b2def9](https://github.com/AZX-PBC-OSS/TaskQ/commit/4b2def9444b1009b8556acf15d4e980fc3453942))
* add admin UI actors page with deregister button ([982e098](https://github.com/AZX-PBC-OSS/TaskQ/commit/982e098c6e2e7481955fe24bf311a39a3b369895))
* add BulkCancelResult type and EmptyFilterError exception ([b0c8dca](https://github.com/AZX-PBC-OSS/TaskQ/commit/b0c8dca50c217eab7358e6c6652f7b466bf9a66b))
* add cancel_where to Backend protocol ([a7df046](https://github.com/AZX-PBC-OSS/TaskQ/commit/a7df046e3fa1331ca56d2020f03ffdce1b70c846))
* add deregister_actor with force=False/force=True safety checks and purge_queue ([b06c6ea](https://github.com/AZX-PBC-OSS/TaskQ/commit/b06c6ea7cab8109e14b6b989b72f1d44ae4c02c5))
* add RateLimitRegistry.clear() test-isolation aid ([78dcc23](https://github.com/AZX-PBC-OSS/TaskQ/commit/78dcc230d6c4f6de56822c4e772184ac894975e2))
* add tags and missing fields to SubJobEnqueuer.enqueue() ([dfca461](https://github.com/AZX-PBC-OSS/TaskQ/commit/dfca461d8008d5cde6e6ec48b5f6207e89dc398c))
* add TaskQ.actors property and export ActorsClient, DeregisterResult from public API ([59d9d5c](https://github.com/AZX-PBC-OSS/TaskQ/commit/59d9d5c697e01dfb1bde8190ee82ba609ccb8cf1))
* allow a leading digit in queue names, and prove the rule is enforced ([370ac12](https://github.com/AZX-PBC-OSS/TaskQ/commit/370ac126c9584bd8ea2a1a8fd9e558af5bc1e7d5))
* **api:** give consumers public paths where they were monkey-patching internals ([5ee5b35](https://github.com/AZX-PBC-OSS/TaskQ/commit/5ee5b357a716aa2c1cc764fe28c91f4252f21f2e))
* **backend:** add count_active_jobs to Backend protocol and implementations ([429d497](https://github.com/AZX-PBC-OSS/TaskQ/commit/429d497cfafef3ade5280b07a7b9860e3f6a0bed))
* **batch:** add BatchAbortedError and EmptyBatchError exceptions ([fae8b9f](https://github.com/AZX-PBC-OSS/TaskQ/commit/fae8b9fdadc908ff6bb348b7231edd4db91746d5))
* **batch:** add batches table migration ([f75762b](https://github.com/AZX-PBC-OSS/TaskQ/commit/f75762b527ab54e6c0d946e1f03caa5ec6738713))
* **batch:** add BatchFailurePolicy and AbortBatchAfter types ([1501eff](https://github.com/AZX-PBC-OSS/TaskQ/commit/1501effa1f39c4c19a0a201bfde4fc0393489a67))
* **batch:** add BatchRow, BatchCounts, BatchFilter, Backend protocol batch methods ([e2fed4c](https://github.com/AZX-PBC-OSS/TaskQ/commit/e2fed4cecd860f6040abca73cd96573dbb9e0502))
* **batch:** add failure_policy, finalizer, streaming, list_batches to client + facade ([3b111f9](https://github.com/AZX-PBC-OSS/TaskQ/commit/3b111f9b84e999094a9d6860b19e1e8a609661dc))
* **batch:** add InMemoryBackend batch operations + PostgresBackend protocol stubs ([296b1b5](https://github.com/AZX-PBC-OSS/TaskQ/commit/296b1b50328a521d6c58c6e382c554ce93c045ad))
* **batch:** add post-terminal-write batch hook + wait_for_batch safety ([d14ce84](https://github.com/AZX-PBC-OSS/TaskQ/commit/d14ce84f9786f6e0b60df363dd21fc348a6cc25b))
* **batch:** add PostgresBackend batch SQL and methods ([e723026](https://github.com/AZX-PBC-OSS/TaskQ/commit/e72302665ff154264b97e9b98f72c699733a6853))
* **batch:** keyset pagination for list_batches, and validate the queue-name write path ([0725feb](https://github.com/AZX-PBC-OSS/TaskQ/commit/0725feb932026dd67cad21c86feef2049c81a768))
* **batch:** stale-batch completion sweep + prune old batches in leader ([4b1e34f](https://github.com/AZX-PBC-OSS/TaskQ/commit/4b1e34fcaec7d5fceea3744729792e0867982d4f))
* **cli:** resolve credential providers from the CLI so rotation actually rotates ([5a803f4](https://github.com/AZX-PBC-OSS/TaskQ/commit/5a803f4e25dd67219cf8b0ecbeca8d17a170a6e9))
* **config:** route credential providers through dotenvmodel, dedupe literals ([573dc3f](https://github.com/AZX-PBC-OSS/TaskQ/commit/573dc3f45e9c95e8a0c8f60951008c1d77f6dd73))
* export missing public API types for 1.0 surface ([e41f49f](https://github.com/AZX-PBC-OSS/TaskQ/commit/e41f49f248ab158836a780f364c73f40e71559dc))
* export OIDCSettings and SAMLSettings from public API surface ([6ab5aac](https://github.com/AZX-PBC-OSS/TaskQ/commit/6ab5aac5007d96bbc60dab229be28ef15da9e0b6))
* fleet-wide queue concurrency cap and KeyedRateLimitRef ([#30](https://github.com/AZX-PBC-OSS/TaskQ/issues/30)) ([b042028](https://github.com/AZX-PBC-OSS/TaskQ/commit/b042028759d171bec96608740fabf9fd6c59cff8))
* forward setup, server_settings, connection_class in pool/conn factories ([2eb7fe4](https://github.com/AZX-PBC-OSS/TaskQ/commit/2eb7fe42c2f159eb2b47a107bdc9a2490959c775))
* **health:** serve probes over HTTP, because ACA has no exec probe type ([ce103e9](https://github.com/AZX-PBC-OSS/TaskQ/commit/ce103e913673f131f4a97f17f807798de3e3cd3a))
* implement cancel_where for InMemoryBackend ([166b53b](https://github.com/AZX-PBC-OSS/TaskQ/commit/166b53bf58bc9a575793327af850961314d5c867))
* implement cancel_where for PostgresBackend + JobsClient/TaskQ ([8c26d77](https://github.com/AZX-PBC-OSS/TaskQ/commit/8c26d77444db33abbe865e0de9e32f950e0af34a))
* implement job handle fetching to avoid multiple calls [#95](https://github.com/AZX-PBC-OSS/TaskQ/issues/95) ([947a0f5](https://github.com/AZX-PBC-OSS/TaskQ/commit/947a0f5b17e6a7364c7d8afa899054506742b99c))
* injectable rate_limit_registry at worker bootstrap with actor-declared collection pass ([6333eae](https://github.com/AZX-PBC-OSS/TaskQ/commit/6333eaea764e63d73299093e1048e130f4843ca9))
* injectable rate_limit_registry for admin app; examples use owned instance ([309c0bf](https://github.com/AZX-PBC-OSS/TaskQ/commit/309c0bf43b963a566b78a07547b332f14eba1a85))
* make crash-reclamation observable via a fleet-wide event cursor ([#25](https://github.com/AZX-PBC-OSS/TaskQ/issues/25)) ([db16364](https://github.com/AZX-PBC-OSS/TaskQ/commit/db163640e453b2c5c13b7ead270574f660b426de))
* **migrate:** let migrations opt out of the transaction wrapper ([879813b](https://github.com/AZX-PBC-OSS/TaskQ/commit/879813b7fbef3b36b460be11c94bce2562c58ffb)), closes [#29](https://github.com/AZX-PBC-OSS/TaskQ/issues/29)
* per-worker rate-limit registry in leader sweeps; de-gate keyed eviction ([4244b00](https://github.com/AZX-PBC-OSS/TaskQ/commit/4244b0031625c072cee60f0df63be676fa363a47))
* PG-backed multi-status pagination parity test, docstring clarifications ([04df2bd](https://github.com/AZX-PBC-OSS/TaskQ/commit/04df2bd116e21343bf8aadb3f036bf3286deee52))
* **queues:** make queue mode configurable so fairness_key is not a no-op ([0e4d36a](https://github.com/AZX-PBC-OSS/TaskQ/commit/0e4d36ad98555a7773cad9492bac7aa0cdb308ba))
* **ratelimit:** make payload_type required, add .typed() to both keyed refs ([a3013fb](https://github.com/AZX-PBC-OSS/TaskQ/commit/a3013fbfaa03b00f975504e27760cd7519a85719))
* **ratelimit:** validate payloads to BaseModel before key_fn in registry ([88c4c08](https://github.com/AZX-PBC-OSS/TaskQ/commit/88c4c08bf7b4cda8cc6d13eeba5b344aa132de6d))
* scope idempotency_key uniqueness via idempotency_scope ([#27](https://github.com/AZX-PBC-OSS/TaskQ/issues/27)) ([c27e702](https://github.com/AZX-PBC-OSS/TaskQ/commit/c27e70295b99e2314bc8e46b4cbafe6b73d36def))
* **settings:** add idle drain mode fields to WorkerSettings ([28285a4](https://github.com/AZX-PBC-OSS/TaskQ/commit/28285a4f3fbd0c15efaf066859f47b021422bf49))
* **testing:** hermetic dotfiles, strict-mode fixtures, health-sock helper, bind retries ([801095c](https://github.com/AZX-PBC-OSS/TaskQ/commit/801095c504b519f5b98e7d5a2d19551a0e52ae96))
* two-tier loop-lag watchdog, lease invariant, merge fallout fixes ([a48a17b](https://github.com/AZX-PBC-OSS/TaskQ/commit/a48a17b3b33ca516852fa0ff5c2125e7053ad4eb))
* unify time on the database clock across enqueue, cron, ratelimit ([1ab2340](https://github.com/AZX-PBC-OSS/TaskQ/commit/1ab234030de478f512a1a6dd962c9261c85b617f))
* widen JobFilter.status to accept multiple statuses, add active meta-filter ([e863850](https://github.com/AZX-PBC-OSS/TaskQ/commit/e863850c47fed6a9822a4e1b45b5544f6cee6fef)), closes [#22](https://github.com/AZX-PBC-OSS/TaskQ/issues/22)
* widen JobFilter.status to accept multiple statuses, add active meta-filter ([#28](https://github.com/AZX-PBC-OSS/TaskQ/issues/28)) ([c5ea971](https://github.com/AZX-PBC-OSS/TaskQ/commit/c5ea9716991fbf081f3fd6d41df46508a45510a9))
* **worker:** add drain_failures counter to WorkerDeps ([dbda072](https://github.com/AZX-PBC-OSS/TaskQ/commit/dbda072c80ec5ee7dd0a97ad4d62823a02a4c08b))
* **worker:** implement drain monitor loop in drain.py ([83bd758](https://github.com/AZX-PBC-OSS/TaskQ/commit/83bd7589eec1badb149dc66c251f69d34e0fd7a0))
* **worker:** return AttemptOutcome from dispatch_one_job, increment drain_failures ([9cf20e7](https://github.com/AZX-PBC-OSS/TaskQ/commit/9cf20e72fbcd5fd41db0645ab63006a398c2bc5e))
* **worker:** wire until_idle through _main/worker_main + CLI --until-idle ([ac569e0](https://github.com/AZX-PBC-OSS/TaskQ/commit/ac569e0668d1ea0bc5382ca11646aabc59f3a5bf))


### Bug Fixes

* actor existence check first, job_events on cancel, canonical status sets, force-aware error message ([80c6ff3](https://github.com/AZX-PBC-OSS/TaskQ/commit/80c6ff32ff202d13431aff1e9f0a3e2a5db3e1e9))
* **actor-config:** address review — result_ttl completion fallback, validation guards, cache hardening ([9d917df](https://github.com/AZX-PBC-OSS/TaskQ/commit/9d917dfc405a17cd6f7cf920d79ffed3d8cbf531))
* address adversarial review findings ([a9d6ffe](https://github.com/AZX-PBC-OSS/TaskQ/commit/a9d6ffe68f2f6a3e2a90f774a051205a1de0342c))
* address adversarial review findings ([47e9eed](https://github.com/AZX-PBC-OSS/TaskQ/commit/47e9eed8b32853ee2bdcc5b5e3ee93781d663a70))
* address adversarial review findings ([a479656](https://github.com/AZX-PBC-OSS/TaskQ/commit/a4796560364281785fde23795b5bc33bcf3cff62))
* address adversarial review findings — code quality, DRY, test gaps ([8706290](https://github.com/AZX-PBC-OSS/TaskQ/commit/8706290da86288a6eb9aa95e7115f36ec3abfcf6))
* address all review findings (C1-C2, H1, M1-M11, Low) ([20245b1](https://github.com/AZX-PBC-OSS/TaskQ/commit/20245b1b608a9481b4f6686dab5b258c70c08f1a))
* address approved-review follow-ups — clock_timestamp, read_timeout validation, hasattr gap, stale docs ([b72b790](https://github.com/AZX-PBC-OSS/TaskQ/commit/b72b790d8f494fbc0566917ffd33f637f20185e8))
* address PR [#37](https://github.com/AZX-PBC-OSS/TaskQ/issues/37) review — bound all remaining closes, fix shutdown race ([87474a2](https://github.com/AZX-PBC-OSS/TaskQ/commit/87474a2dde78c675b838da780972a1ea710b1b0e))
* **admin:** age timestamps in the database clock domain, not the app's ([123acfd](https://github.com/AZX-PBC-OSS/TaskQ/commit/123acfd8d5d804843c2599decc8ab1a5a91517d7))
* **admin:** answer a pg-only bucket reset with an explanatory 404, not a 500 ([6a27c5f](https://github.com/AZX-PBC-OSS/TaskQ/commit/6a27c5f0dcfde148b8cf7b0167659cd39cbc37cb))
* **admin:** bound the job list's status and tags filter inputs ([e7c00ef](https://github.com/AZX-PBC-OSS/TaskQ/commit/e7c00efff62221bbbbd5ed6ded3f91fcc332fd01))
* **admin:** compute schedule skip with the row's stored dst_strategy ([ed4209c](https://github.com/AZX-PBC-OSS/TaskQ/commit/ed4209cffa5441e3262d17a0bf18ce1149093abc))
* **admin:** gate schedule enable/disable/skip on admin_actions_enabled ([1a3a4c9](https://github.com/AZX-PBC-OSS/TaskQ/commit/1a3a4c900cb3f9f4afc463cb4600da51fd1dea41))
* **admin:** keyset pagination must follow the sort direction, and bind the cursor at its column type ([2569da5](https://github.com/AZX-PBC-OSS/TaskQ/commit/2569da538056c37a586047db7c488af702963eeb))
* **admin:** reject NUL in text filters with a clean 400 ([6423aa8](https://github.com/AZX-PBC-OSS/TaskQ/commit/6423aa850e6c0bc7fdb7fbfed64a6b0d68e6b5f0))
* **admin:** warn when the admin UI is served with no authentication ([2e162ef](https://github.com/AZX-PBC-OSS/TaskQ/commit/2e162ef96358e6499948e7abe94b4035c7dc1e60))
* **admin:** warn when the admin UI is served with no authentication ([07a6c8f](https://github.com/AZX-PBC-OSS/TaskQ/commit/07a6c8ff99719945f5e57d729fd3acacb14a1496))
* all remaining L findings — CSRF test, notice whitelist, exit codes, schema assertion, edge-case tests ([048da63](https://github.com/AZX-PBC-OSS/TaskQ/commit/048da631196c2c5545bb1578b5098332f445329c))
* **api:** export the sanitized validate_actor_payload from the public API ([a24f697](https://github.com/AZX-PBC-OSS/TaskQ/commit/a24f697154265be1a1b0d1a59148cf4613986ad6))
* **attempts:** measure duration_ms on the database clock, drop the freed params ([dbb19eb](https://github.com/AZX-PBC-OSS/TaskQ/commit/dbb19eb9ae5d3958f38b212998bf23ba85e0fe5c))
* attestations gate via explicit input, stale now() in docs, misleading test docstring ([328600f](https://github.com/AZX-PBC-OSS/TaskQ/commit/328600f0a89a5bff8c6a483481a64b3a7b2699c0))
* **auth:** refresh Postgres credentials per physical connection ([9111d9b](https://github.com/AZX-PBC-OSS/TaskQ/commit/9111d9b2945ccbd335ba6ed6304bca95a7360943))
* **backend:** bound BatchFilter.limit at 500 ([68c8236](https://github.com/AZX-PBC-OSS/TaskQ/commit/68c8236686e43363201499ef4676d170bf1ac7c1))
* **backend:** reject a NUL in JobFilter's text predicates ([8ac867a](https://github.com/AZX-PBC-OSS/TaskQ/commit/8ac867a9721222eab0bfdd22f256831081442bc4))
* **backend:** reject a NUL in ScheduleCreateArgs caller text ([7572073](https://github.com/AZX-PBC-OSS/TaskQ/commit/75720732c5b910b0c193fe7e21a8e67e5983b741))
* **backend:** validate dst_strategy and DRY the strategy set ([9c8710c](https://github.com/AZX-PBC-OSS/TaskQ/commit/9c8710c79712c74aa30bcb6be087de983e8f66d4))
* **batch:** address adversarial review findings — docs, bugs, tests, types ([e066ce0](https://github.com/AZX-PBC-OSS/TaskQ/commit/e066ce03804d2fdd0984aea44d77a2f1d978b556))
* **batch:** resolve all PR [#62](https://github.com/AZX-PBC-OSS/TaskQ/issues/62) review findings — C1-C3, H1-H6, M1-M19 ([28c665b](https://github.com/AZX-PBC-OSS/TaskQ/commit/28c665bcc0f73f68d93133bb8624b93125fbde24))
* bound add_listener calls with wait_for timeout ([2f19188](https://github.com/AZX-PBC-OSS/TaskQ/commit/2f191882264196c72b4b5918efe39257d860e029))
* bound mid-run error-path closes ([#38](https://github.com/AZX-PBC-OSS/TaskQ/issues/38)) ([4768920](https://github.com/AZX-PBC-OSS/TaskQ/commit/47689205f75ee2edd9cbb71c6fa3bdb2014f7e25))
* bound worker teardown closes with wait_for + terminate-on-timeout ([fc5559a](https://github.com/AZX-PBC-OSS/TaskQ/commit/fc5559a08c3578a7a833cc9743e665905d9d214f))
* bound worker teardown closes with wait_for + terminate-on-timeout ([#37](https://github.com/AZX-PBC-OSS/TaskQ/issues/37)) ([5d65b42](https://github.com/AZX-PBC-OSS/TaskQ/commit/5d65b422d986ea5e204169c999de530d4d02b83c))
* bump BACKEND_PROTOCOL_VERSION to 3 for the list_jobs contract change ([8a1896b](https://github.com/AZX-PBC-OSS/TaskQ/commit/8a1896b34700889802ef9021d94b9cbe2383b2dd))
* **cancel:** clear cancel state on every retry arm, both backends ([c06ba0e](https://github.com/AZX-PBC-OSS/TaskQ/commit/c06ba0eac09f383695a02d0ba6d5dd6e676546b2))
* **client:** bound the schedule seed clock read on the pool ([3e724cf](https://github.com/AZX-PBC-OSS/TaskQ/commit/3e724cf2d0d8d678b9d311b69a784248a84a6d28))
* **client:** cap sub-job batches, and bound two unindexed admin filters ([a9ebcaf](https://github.com/AZX-PBC-OSS/TaskQ/commit/a9ebcaf7360b6afc5c416416f28bc574be6181cc))
* **client:** hand back a cursor for every ordering, and finish two dedupes ([a01ddb9](https://github.com/AZX-PBC-OSS/TaskQ/commit/a01ddb9e08dc41904973a58f4b1ee78bddecf293))
* **client:** honour an explicit trace_id/span_id on JobsClient.enqueue ([79ef633](https://github.com/AZX-PBC-OSS/TaskQ/commit/79ef63362f13e4c7773009556ce99c33a6a7adc8))
* **client:** report capacity has_snapshot from refresh success, not row count ([af22922](https://github.com/AZX-PBC-OSS/TaskQ/commit/af22922defc602fba2d220415aa8317b5951aa66))
* **cli:** reject queue max_concurrent=0 before it hits the DB CHECK ([c50205b](https://github.com/AZX-PBC-OSS/TaskQ/commit/c50205bf198400eb75bc18beb56f14878e230385))
* close redis client on initialize() failure + tighten bounded-close tests ([cd059d8](https://github.com/AZX-PBC-OSS/TaskQ/commit/cd059d88b7091314e6c205f51eac38f76067c177))
* close remaining unbounded closes and shutdown double-close race ([2247801](https://github.com/AZX-PBC-OSS/TaskQ/commit/22478013286b449510a861367db2e8326fe06741))
* code review findings L1-L3 ([8223898](https://github.com/AZX-PBC-OSS/TaskQ/commit/8223898cdd4cb3a2b4fa045e7bf03f002e7a48f2))
* complete the clock_timestamp() database-clock invariant ([bc5c395](https://github.com/AZX-PBC-OSS/TaskQ/commit/bc5c395310d178928005e3e83d969b8daa1631cd))
* complete three half-applied invariants (dispatcher acquire timeout, CLI close bound, single-job tags NUL guard) ([dfb41f4](https://github.com/AZX-PBC-OSS/TaskQ/commit/dfb41f4ea520b812099ae36dfafcd4000cf9beb9))
* complete two half-applied invariants (schema re-validation, jsonb NUL guard) ([0690e51](https://github.com/AZX-PBC-OSS/TaskQ/commit/0690e5155f84b8ea9688ae6e37c1b5cc472afeca))
* concurrent deregistration safety, 404 for unknown actor, RETURNING for purge, spec doc fixes ([8bf4c13](https://github.com/AZX-PBC-OSS/TaskQ/commit/8bf4c13ab1cc028cd9fc52d96318d180991c53e3))
* concurrent test with real interleaving, audit trail assertions, fixture convergence, template/doc fixes ([39a5767](https://github.com/AZX-PBC-OSS/TaskQ/commit/39a576704537092ca4df23f19836cc2a1ace4837))
* **constants:** anchor _IDENT_RE with \A/\Z so a trailing newline is rejected ([e222be2](https://github.com/AZX-PBC-OSS/TaskQ/commit/e222be2cf7c6db67b29fcfb7979dcc7ffe40a996))
* **consumer:** always pass validated_payload to acquire_for_actor ([12fd571](https://github.com/AZX-PBC-OSS/TaskQ/commit/12fd5717efa87af8bf06f5b2571f54db645b9e39))
* **consumer:** validate payload before acquire; pass the validated model ([b3f0bd4](https://github.com/AZX-PBC-OSS/TaskQ/commit/b3f0bd4085070884f0a919f815e425c84184b2ad))
* **cron:** detect and warn on cron schedule drift at startup ([474600a](https://github.com/AZX-PBC-OSS/TaskQ/commit/474600af01f757268b14d9cd19f7e856f88a273e))
* **cron:** make a contended cron tick observable ([eee9265](https://github.com/AZX-PBC-OSS/TaskQ/commit/eee926595be9f1f7a8848a5262215823f3a38806))
* **cron:** read dst_strategy back, so 'allof' and 'firstof' actually apply ([7d7e01c](https://github.com/AZX-PBC-OSS/TaskQ/commit/7d7e01cb710b5d74975c42a0f5768dd9f1b959a0))
* **deps:** clear PYSEC-2026-3552 and PYSEC-2026-3721 from pip-audit ([308478b](https://github.com/AZX-PBC-OSS/TaskQ/commit/308478b51ee81e1c800b06f465bceb42eaadc178))
* **deps:** lower opentelemetry-api floor to 1.30.0 for Azure Monitor ([7064035](https://github.com/AZX-PBC-OSS/TaskQ/commit/7064035f13176009bb4bfe8ec446983fd086415c))
* **deps:** move the uv required-version pin to the 0.12.x line ([9959123](https://github.com/AZX-PBC-OSS/TaskQ/commit/9959123528016b1889f2d9c04039ee3a5d4801f2))
* **deps:** raise floors for two CVEs the security lane is failing on ([6b19690](https://github.com/AZX-PBC-OSS/TaskQ/commit/6b19690373dc3d0c39aaed5b0a0c2c3fb7a5d6f4))
* **deps:** widen uv floor to 0.12.7 for Dependabot, track compose with docker-compose ([4426288](https://github.com/AZX-PBC-OSS/TaskQ/commit/44262887bdb629bc972ed666709edbe1f3dd862b))
* **dispatch:** bound the producer's pool acquire and re-validate its schema ([61cd656](https://github.com/AZX-PBC-OSS/TaskQ/commit/61cd656dc4bf8a853814461496c41a62329fc55e))
* **e2e:** align finalizer snooze_interval with test settings and fix gather anti-pattern ([4888b10](https://github.com/AZX-PBC-OSS/TaskQ/commit/4888b10b5c7cb0adcb03900745bd66af687a4698))
* **e2e:** correct stage-2 tag count in test_sub_job_explicit_tags_merge_with_parent ([2d310f1](https://github.com/AZX-PBC-OSS/TaskQ/commit/2d310f197b90f6695e1f4721ea4e23064ce91501))
* **e2e:** poll for cron job succeeded status instead of one-shot check ([bcd1584](https://github.com/AZX-PBC-OSS/TaskQ/commit/bcd1584a39a06d8460da85697c92f2940b5cf8f9))
* **enqueue:** enforce NUL rejection at the EnqueueArgs chokepoint ([9a4178a](https://github.com/AZX-PBC-OSS/TaskQ/commit/9a4178a34e12fb00b528b8de991eca59b4d3b723))
* **enqueue:** make unique_for + identity_key actually single-flight ([92d1d93](https://github.com/AZX-PBC-OSS/TaskQ/commit/92d1d933d2367a31c7665d8c58f7493ab9c6477f))
* **enqueuer:** explicit empty tags suppress inheritance instead of inheriting ([5f01da5](https://github.com/AZX-PBC-OSS/TaskQ/commit/5f01da51c6c1e9a23d0b68806d906740104172be))
* **enqueuer:** reject an empty sub-job batch before any connection use ([97ef4af](https://github.com/AZX-PBC-OSS/TaskQ/commit/97ef4afdd838c4310a06b8606a2bb9b0fcd975cd))
* fail fast on ambiguous rate_limit_registry explicit+DI co-presence ([3898267](https://github.com/AZX-PBC-OSS/TaskQ/commit/389826784976e381dc053b65b040462ec1a10017))
* H1+M1+M5+M6 — wrap registry ValidationError, extract shared helper, fix runner ([d018398](https://github.com/AZX-PBC-OSS/TaskQ/commit/d018398e784ab066b110d3875aff44d494c62cb1))
* hardcoded terminal statuses, missing actor registration, count_batch_non_terminal connection param ([e297a3f](https://github.com/AZX-PBC-OSS/TaskQ/commit/e297a3fae05681d6f359f5073c7ad578589ba898))
* harden JobFilter list path — schema re-validation, sort tie-break parity, input validation ([7b15ed6](https://github.com/AZX-PBC-OSS/TaskQ/commit/7b15ed657ca179cc46ba4e8a0b8540a5ba665129))
* harden queue-stranding warning, top-level status exports, in-memory expiry pins ([96884d8](https://github.com/AZX-PBC-OSS/TaskQ/commit/96884d80aac4abb56fd93e4807e0839f5f6d81d0))
* **heartbeat:** always run run_post_tx, and heal a rolled-back phase-2 write ([329acad](https://github.com/AZX-PBC-OSS/TaskQ/commit/329acad2084596d7033949634bdc4b374b36eb88))
* idempotency_scope migration test handles 01.00.02_01 ([#40](https://github.com/AZX-PBC-OSS/TaskQ/issues/40)) ([b39f5cb](https://github.com/AZX-PBC-OSS/TaskQ/commit/b39f5cb4c783cb1e2bfa5cc7328f72f0f0f47f7f))
* in-memory result expiry, queue-stranding warning, grace default 75s ([d0fcd79](https://github.com/AZX-PBC-OSS/TaskQ/commit/d0fcd7907718a5d3bf5c7b14ec1508ad7fb9c36c))
* job handle fixes ([9744c96](https://github.com/AZX-PBC-OSS/TaskQ/commit/9744c96ff4828045493e073253eef1ddc4c3177f))
* JobsClient.list never emits an unusable cursor for non-default order_by ([041c9c4](https://github.com/AZX-PBC-OSS/TaskQ/commit/041c9c4e6d2b6107636c1b21887293eb542ebf41))
* **jobs:** emit batch-streaming-enqueued in kebab case ([6fe38ce](https://github.com/AZX-PBC-OSS/TaskQ/commit/6fe38ce1837d005729bd96dc3a62878929e9b18b))
* **jobs:** seed schedule next_fire_at from the due-check's clock domain ([7183318](https://github.com/AZX-PBC-OSS/TaskQ/commit/7183318e19ea5071b4174fb24583cfd383e885ef))
* keep every persisted/compared timestamp in the database clock domain ([7565d3f](https://github.com/AZX-PBC-OSS/TaskQ/commit/7565d3f3dfd398e5724f879828f4d9759c18efc3))
* **leader:** clear leader-only gauges on demotion ([a742d13](https://github.com/AZX-PBC-OSS/TaskQ/commit/a742d135ef15e51d6a6290dee90d2421434b760c))
* **leader:** keep the stranded-jobs detector reporting after the first tick ([d9df857](https://github.com/AZX-PBC-OSS/TaskQ/commit/d9df857db133d3434eec3768de7781c7d114a920))
* **leader:** log a schema-muted sampler as an error-level *-disabled event ([36fd840](https://github.com/AZX-PBC-OSS/TaskQ/commit/36fd8407d3b2094999e277c9e13cb5cfac6ab670))
* **leader:** run the stale-batch sweep leader-gated, not under keyed-RL state ([db1e28a](https://github.com/AZX-PBC-OSS/TaskQ/commit/db1e28a598af2e8d677e2e48d158598f72ceefcb))
* M4+M7+M8+H2+L1-L8 — DRY helpers, docs, changelog, test hardening ([a5120da](https://github.com/AZX-PBC-OSS/TaskQ/commit/a5120da04805c4c059d951f52847769cc3ceb4c8))
* make every in-memory get() return an isolated row copy ([0b2b20a](https://github.com/AZX-PBC-OSS/TaskQ/commit/0b2b20a62c3751b3d2c9343809990b8ffd2e9399))
* **migrate:** address PR [#35](https://github.com/AZX-PBC-OSS/TaskQ/issues/35) review feedback ([caa3695](https://github.com/AZX-PBC-OSS/TaskQ/commit/caa3695c77598e857bc1053ca136ed5ab3eea791))
* **migrate:** lock `migrate up` and bound the advisory-lock wait ([b9d080b](https://github.com/AZX-PBC-OSS/TaskQ/commit/b9d080bb8831aa46ef3002cbd62c2190b0426e67))
* **migrate:** report the truth for transaction-control guard rejections ([c6fb4db](https://github.com/AZX-PBC-OSS/TaskQ/commit/c6fb4dbbd7d45eda6040facb377802c9d5a36cfa))
* **migrate:** tag the phase-ordering guard's exception with the offending migration ([c722c26](https://github.com/AZX-PBC-OSS/TaskQ/commit/c722c2601d72778287444ec8ec4ee842880f877a))
* **migrate:** warn when the lock_timeout reset fails on a caller-owned conn ([3d43fc3](https://github.com/AZX-PBC-OSS/TaskQ/commit/3d43fc3a05c828f41609fb07adbf4f57a03eaf2b))
* None-safe sweep fallback; drop dead store; stale provider module docstring ([e8279b4](https://github.com/AZX-PBC-OSS/TaskQ/commit/e8279b4f21268e36a1e0aa2620908d01bffa00f8))
* **obs:** drop identity values from metric dimensions before they throttle ingestion ([6a7368d](https://github.com/AZX-PBC-OSS/TaskQ/commit/6a7368d6d6d9bdec76cb20689c3bb893cb929666))
* **obs:** finalize kebab-case event names and job-failed docstrings ([0c3132f](https://github.com/AZX-PBC-OSS/TaskQ/commit/0c3132f41b220b8992d53ded4d798b4db91fc187))
* **obs:** log the worker/workgroup identity that cron and supervisor events dropped ([bb5a2cf](https://github.com/AZX-PBC-OSS/TaskQ/commit/bb5a2cf9069c829c893915c68d7ac17ffa57fcbb))
* **obs:** one event name for a state change, whichever backend logged it ([bf3cfc7](https://github.com/AZX-PBC-OSS/TaskQ/commit/bf3cfc7e01b514be04cceae996143b24377186d6))
* **obs:** redact last_error in cron auto-disable span event ([fe425dc](https://github.com/AZX-PBC-OSS/TaskQ/commit/fe425dcfd1b5e38f0db5d14e5d77bce061bf99e3))
* **obs:** render redacted exception detail on the JSON log channel ([6f3aa16](https://github.com/AZX-PBC-OSS/TaskQ/commit/6f3aa162fe264b489af6a739e989f081077ba7e9))
* **obs:** replace an arbitrary 512-char message cap with the bound already in the tree ([349c51b](https://github.com/AZX-PBC-OSS/TaskQ/commit/349c51b301c4ffdfddf6084fe1cb5d3d925a480c))
* **obs:** scrub DETAIL/HINT/CONTEXT lines flattened by repr() ([91412b5](https://github.com/AZX-PBC-OSS/TaskQ/commit/91412b5145232f40530be19de6616c9e514cf96e))
* **obs:** scrub exception text before the LogRecord reaches ANY root handler ([010a60a](https://github.com/AZX-PBC-OSS/TaskQ/commit/010a60aef37de1065d6848b54c2b568286e49b8d))
* **obs:** scrub exception-bearing log fields through the redaction helpers ([067ab80](https://github.com/AZX-PBC-OSS/TaskQ/commit/067ab80983bb0390894894ec5566553254ad7b5b))
* **obs:** scrub the terminal-write log's job/infra error fields ([5c745ea](https://github.com/AZX-PBC-OSS/TaskQ/commit/5c745eae865c98062fbbffe80fa9b0ae0dd80f3d))
* **obs:** stop deleting HINT and CONTEXT, and let an operator turn redaction off ([76a10cc](https://github.com/AZX-PBC-OSS/TaskQ/commit/76a10cc633cc3df3015e85a7a2bb16156db1429d))
* **obs:** stop the OTel SDK re-emitting the exception text spans just scrubbed ([87a47ab](https://github.com/AZX-PBC-OSS/TaskQ/commit/87a47abaf4fd4d45033f9dfc96fda0044fad76de))
* **obs:** surface capacity-cache fail-open as a metric, not just a log ([cdef3ee](https://github.com/AZX-PBC-OSS/TaskQ/commit/cdef3ee853d9f5d5d882098e0d5bc16d0c5e9927))
* **ops:** keep the queues row while non-terminal jobs still reference it ([ebd61c3](https://github.com/AZX-PBC-OSS/TaskQ/commit/ebd61c3aaafaf25f1d4f803ae60adfc627d2e537))
* **ops:** record each force-cancelled job's real prev status in its event ([b594fdc](https://github.com/AZX-PBC-OSS/TaskQ/commit/b594fdc5d6dbedde9347e50957d6144d66509725))
* page every job-list ordering, by ordering id with the sort column ([d8e9b20](https://github.com/AZX-PBC-OSS/TaskQ/commit/d8e9b20a747f82a3dcc2709cc41be4f234128e75))
* pass resolved_rl_registry to _served_redis_rate_limits instead of module singleton ([758ac78](https://github.com/AZX-PBC-OSS/TaskQ/commit/758ac7880e495d98458c76b444484a6d5110b916))
* **ratelimit:** bound the unbounded 1/refill quantities in TokenBucket ([87a3df1](https://github.com/AZX-PBC-OSS/TaskQ/commit/87a3df1c1fd1d5d89f10f32c944b50f73ed55921))
* **ratelimit:** drop payload embedding from key_fn ValueError messages ([6b54fd5](https://github.com/AZX-PBC-OSS/TaskQ/commit/6b54fd565878e3c192039d97820b98136efcfd80))
* **ratelimit:** refund the store that actually paid, not the configured one ([1e860fb](https://github.com/AZX-PBC-OSS/TaskQ/commit/1e860fb1b3badb1f241b83810043302e7644c6fd))
* read contracted columns by subscript, not by defaulting .get() ([baaeec0](https://github.com/AZX-PBC-OSS/TaskQ/commit/baaeec0f2381d5736165248ecb14c275e15aa8ee))
* redteam findings — clock completeness, concurrency guards, test robustness ([246dc46](https://github.com/AZX-PBC-OSS/TaskQ/commit/246dc463807bf7eed1c871cda2caf29a27c6ab2c))
* register batch_abort_finalizer actor in e2e worker entry ([af2ee48](https://github.com/AZX-PBC-OSS/TaskQ/commit/af2ee483106b351ab6c1f6bd9e2fe8ea5f6938ea))
* reject a NUL in a job payload at enqueue instead of looping forever ([6dd167c](https://github.com/AZX-PBC-OSS/TaskQ/commit/6dd167c8f9e014a4c45348a3f38ae782e90c13b5))
* reject a NUL in a job payload at enqueue instead of looping forever ([a394047](https://github.com/AZX-PBC-OSS/TaskQ/commit/a39404793729224afe615a0a488ad4a994060f7f))
* reject non-LOOP RateLimitRegistry value provider at bootstrap ([fa5f038](https://github.com/AZX-PBC-OSS/TaskQ/commit/fa5f03895825cf827ffa00d2819c5516f4ea6f12))
* remove _revive_uuids from JSON deserialization — Pydantic owns type coercion ([1e8aeaf](https://github.com/AZX-PBC-OSS/TaskQ/commit/1e8aeaf69cf490bac306db2fd21896b9311a5420))
* remove dead `now` parameter from PG sweep functions ([94b85e4](https://github.com/AZX-PBC-OSS/TaskQ/commit/94b85e4e31e3359b8c7c00d09ea5a8e1295cd780))
* remove invented caps, and two bugs they were hiding ([40fdcd9](https://github.com/AZX-PBC-OSS/TaskQ/commit/40fdcd92a5e27027f339c90ee0655d709daac2e9))
* **reservation:** fence slot release to the lease that acquired it ([7230a1e](https://github.com/AZX-PBC-OSS/TaskQ/commit/7230a1e92690cdee95100c0dfad5a93d2095f9a3))
* reset-handler owned-registry round-trip test; None-safe setup_admin_state fallback ([b2c3417](https://github.com/AZX-PBC-OSS/TaskQ/commit/b2c3417015110d381b2099434797cfcd64fbe593))
* resolve integration merge conflicts — lint, typecheck, and test fixes ([4eea47a](https://github.com/AZX-PBC-OSS/TaskQ/commit/4eea47a0ef52425a128abb14e27f4175642b9d18))
* resolve pyright type error in test_batch_enqueue result attribute access ([21c3cac](https://github.com/AZX-PBC-OSS/TaskQ/commit/21c3cac6e46072442a5f3c2ab949297054bcc6ff))
* **retry:** saturate exponential backoff instead of raising OverflowError ([88726c7](https://github.com/AZX-PBC-OSS/TaskQ/commit/88726c7ae2468b077cd86eee31dc3f0c5ed7169f))
* review findings + add full-stack client integration tests ([f924c39](https://github.com/AZX-PBC-OSS/TaskQ/commit/f924c399b0b7d26c5356aab92ed9f0d29245454b))
* ruff __all__ sort in actor_config_ops.py ([de3ebc6](https://github.com/AZX-PBC-OSS/TaskQ/commit/de3ebc687d7bb745965f960ec4cb2395b651b397))
* ruff format ([76abb7f](https://github.com/AZX-PBC-OSS/TaskQ/commit/76abb7f151eff70b7e80c4c9fa70051573a8c168))
* ruff format on test files ([63c25ed](https://github.com/AZX-PBC-OSS/TaskQ/commit/63c25ed0faf828b830f9d5b8c9fce8d18150589d))
* **settings:** cap schema_name at Postgres' 63-byte identifier limit ([7905fea](https://github.com/AZX-PBC-OSS/TaskQ/commit/7905fea7e058635a74884a87f2e91113f2f4be10))
* **settings:** validate schema_name via a hook so validate=False cannot skip it ([2538325](https://github.com/AZX-PBC-OSS/TaskQ/commit/2538325752a16e9ff060e879981db5b5b3824fbf))
* **settings:** validate worker identity fields at load time ([deddb16](https://github.com/AZX-PBC-OSS/TaskQ/commit/deddb1645a95ea735b0064b590628e5b3ab458c9))
* shared actor summaries, export ActorConfigRow, CLI schema test, e2e purge assertion, doc gaps ([e609b16](https://github.com/AZX-PBC-OSS/TaskQ/commit/e609b1613ddc664f0d2903ec8767b5b670c8ac8c))
* **shutdown:** model the bounded-close tail in the shutdown budget ([270f169](https://github.com/AZX-PBC-OSS/TaskQ/commit/270f1697b65841854e60999759510548a47a86da))
* **sso:** thread session_max_age_seconds into the SAML config ([1125953](https://github.com/AZX-PBC-OSS/TaskQ/commit/112595399d0003e6b0dc9a7875de5e850c122549))
* standardize 3 remaining non-kebab-case event names in _leader_sweeps.py ([1ead8f1](https://github.com/AZX-PBC-OSS/TaskQ/commit/1ead8f1c4da603efa5089b7994bfde890a3e6a04))
* standardize leader connection close/keepalive labels ([aaae997](https://github.com/AZX-PBC-OSS/TaskQ/commit/aaae99753fa62448f49291a811b5038b4533f0ff))
* standardize structured log event names to kebab-case ([80dbf30](https://github.com/AZX-PBC-OSS/TaskQ/commit/80dbf30d7a5b458a60f8ba46b6b98f9d73e2c11a))
* **test:** clear the SSE slot registry on BOTH sides of every test ([8dd2aec](https://github.com/AZX-PBC-OSS/TaskQ/commit/8dd2aec98d6639ef5d7d042328a66ad18d34c55b))
* **test:** drop the shadowed _ACQUIRE_TIMEOUT the watchdog commit left behind ([b21dcdf](https://github.com/AZX-PBC-OSS/TaskQ/commit/b21dcdfa47a10a0c31d7d6d458afc9382c39e382))
* **testing:** make ChaosPool honour acquire(timeout=) with real asyncpg semantics ([235f32e](https://github.com/AZX-PBC-OSS/TaskQ/commit/235f32ebc9ac9ea9f18665da368ce0599298649d))
* **testing:** mirror PG's bind-time NUL guard for payload/metadata on InMemory enqueue ([f7b3b23](https://github.com/AZX-PBC-OSS/TaskQ/commit/f7b3b239a290f33edc2cd4a8f804b32c9c28959a))
* **testing:** scope the shared test pair and holder registry per invocation ([350b0f0](https://github.com/AZX-PBC-OSS/TaskQ/commit/350b0f0501abae28d780710b146a6818138c04ed))
* **testing:** track shared-pair references by live holder pid, not creator labels ([c6ee1cc](https://github.com/AZX-PBC-OSS/TaskQ/commit/c6ee1cc424ca4b02dd3ce610a437ad7b8ad3f470))
* **test:** pass validated_payload in queue cap saturation test ([950502b](https://github.com/AZX-PBC-OSS/TaskQ/commit/950502ba0dc0935d74453b74a959c979ae01ddf2))
* **test:** use None instead of MagicMock for finalizer_handle (type-safe) ([5a4c869](https://github.com/AZX-PBC-OSS/TaskQ/commit/5a4c869ea27b7be9bec4a84011a8a5cd5e09a9f6))
* **types:** satisfy pyright strict on the hardening changes ([8ea5677](https://github.com/AZX-PBC-OSS/TaskQ/commit/8ea5677b28ceb5f003111f0ef3b6954efa5b41b8))
* use gt=0 instead of ge=0.1 for interval settings ([f4a1bf2](https://github.com/AZX-PBC-OSS/TaskQ/commit/f4a1bf2565a238f5ac95b1b94812e30072fc7648))
* validate settings that crash at runtime instead of load time ([1b19d01](https://github.com/AZX-PBC-OSS/TaskQ/commit/1b19d0181ea79b8386d8b907a003f0bb2abbbced))
* **validation:** re-anchor the queue/tag/keyed-key regexes with \A...\Z ([83ab3da](https://github.com/AZX-PBC-OSS/TaskQ/commit/83ab3dab05a20f239764c65c2ecd97f5daa7a77f))
* **validation:** run the queue-name validator at the enqueue and actor chokepoints ([2470b2f](https://github.com/AZX-PBC-OSS/TaskQ/commit/2470b2f6a925a81fdc134e823a777fccbe5fa5ef))
* **web:** admin pages could be framed and UI-redressed; CSRF cookie lost Secure behind TLS termination ([6d1b615](https://github.com/AZX-PBC-OSS/TaskQ/commit/6d1b615c124dd49bfa4b31bca2814d4950ab0bde))
* **web:** admin session cookie was sent to the whole host origin, and an empty group allowlist was silent ([b137be8](https://github.com/AZX-PBC-OSS/TaskQ/commit/b137be888833feb532b0d45239af10d1e73bb706))
* **web:** cap concurrent SSE connections on both uncapped streams ([1195d61](https://github.com/AZX-PBC-OSS/TaskQ/commit/1195d61779ab0998683237b821af86ee9c6c8d76))
* **worker:** address PR [#39](https://github.com/AZX-PBC-OSS/TaskQ/issues/39) follow-up findings + parallel review fixes ([c906187](https://github.com/AZX-PBC-OSS/TaskQ/commit/c906187251a32c41ad90efb7e1c0fbecd8ab822e))
* **worker:** address XBeg9's PR [#39](https://github.com/AZX-PBC-OSS/TaskQ/issues/39) follow-up findings ([7d27029](https://github.com/AZX-PBC-OSS/TaskQ/commit/7d27029224926bf2d3d1f99898c67cb6f3a71c00))
* **worker:** bound the Redis drain retry instead of hanging on a wedged socket ([188bcb2](https://github.com/AZX-PBC-OSS/TaskQ/commit/188bcb2981019112486c118cb0d665bad2700cd0))
* **worker:** exit-code contract, CLI rename, handler return values, validation ([1348597](https://github.com/AZX-PBC-OSS/TaskQ/commit/13485977a6fd6685492ee990155067771a83f9ba))
* **worker:** forget drain monitor liveness registration on exit ([09e3774](https://github.com/AZX-PBC-OSS/TaskQ/commit/09e3774c0fcd62a0f727a9cabf64a61dc11afce4))
* **worker:** register the drain monitor's worst-case tick gap ([a3fe701](https://github.com/AZX-PBC-OSS/TaskQ/commit/a3fe7014a3ec931ab688deb9a2a349985bee372c))
* **worker:** warn instead of silently ignoring TASKQ_MIGRATE_ON_START ([4304558](https://github.com/AZX-PBC-OSS/TaskQ/commit/43045585e190552b001cedeab0d8d340c3f6708a))


### Performance Improvements

* **dispatch:** write state-change events in one statement, not one per job ([f66b53e](https://github.com/AZX-PBC-OSS/TaskQ/commit/f66b53e38a264ccc2c75019265560413e7ad094a))


### Reverts

* **admin:** _time_ago's int/float branches are unreachable and unguessable ([1dcf9f9](https://github.com/AZX-PBC-OSS/TaskQ/commit/1dcf9f92f1f63973899ab85853aaee8bf76bb9e5))
* **admin:** the status-count cap only ever fired where the dedup already had ([08dffc5](https://github.com/AZX-PBC-OSS/TaskQ/commit/08dffc5e54f71bdbef759e035ab14d9f99531fda))
* **backend:** a limit cap on a listing with no cursor hides rows for good ([e48c5ba](https://github.com/AZX-PBC-OSS/TaskQ/commit/e48c5bacd4eb98e5038846c37a35ecfee380467d))
* **enqueuer:** keep tags=[] unioning with the parent's tags ([e7652f9](https://github.com/AZX-PBC-OSS/TaskQ/commit/e7652f9d85c225abbc740193c12f3e8e7178a34c))
* **obs:** schedule_id is a bounded operator-created set, not a per-process UUID ([9bb30bc](https://github.com/AZX-PBC-OSS/TaskQ/commit/9bb30bcd54aeb35b34419a9d68984519787a7bf1))


### Documentation

* add actor deregistration documentation ([1042fcb](https://github.com/AZX-PBC-OSS/TaskQ/commit/1042fcbcbf2f82598358fbc36d0bc030382b2971))
* add configuration and troubleshooting guides to index Next Steps ([2b977cd](https://github.com/AZX-PBC-OSS/TaskQ/commit/2b977cd4a4c9c60fdb9cf23d3d9bce6bfac68ad4))
* add missing settings to configuration guide ([6f8cdd1](https://github.com/AZX-PBC-OSS/TaskQ/commit/6f8cdd137db409d32b62ca00a5fd84c68117eb54))
* align keyed rate-limit growth phrasing with eviction scheduling ([829c02e](https://github.com/AZX-PBC-OSS/TaskQ/commit/829c02ea8e0dcf4f1c3ff1a2fbc812917bf0c4a1))
* architecture rewrap + uv pin comment accuracy ([15e8ee1](https://github.com/AZX-PBC-OSS/TaskQ/commit/15e8ee1d16de39df293315679e083a68041b87ed))
* **close:** caveat the teardown-tail model for the sibling-crash path ([e1cc11e](https://github.com/AZX-PBC-OSS/TaskQ/commit/e1cc11ee65c52fff26e1da8a25e23089387a0df3))
* **dispatch:** stop documenting max_concurrent as a hard fleet-wide cap ([4b9414b](https://github.com/AZX-PBC-OSS/TaskQ/commit/4b9414b4ffdea8209107bf5ac53a2ea909707d7d))
* document cancel_where, sub-job tags, and tag inheritance ([fea6db3](https://github.com/AZX-PBC-OSS/TaskQ/commit/fea6db3df8d7da89332b54836743481b4846cbfd))
* document leader sweep interval settings added in [#39](https://github.com/AZX-PBC-OSS/TaskQ/issues/39) ([b01dd7b](https://github.com/AZX-PBC-OSS/TaskQ/commit/b01dd7b9a826e634ba703f2a15d53ec09d0657ba))
* document taskq.worker.actor_config → taskq.actor_config migration ([f0d6320](https://github.com/AZX-PBC-OSS/TaskQ/commit/f0d6320ba8515b93063b46b119560a9b9851725a))
* document watchdog subsystem, transient errors, and SIGUSR2 dump in architecture guide ([9d17a7d](https://github.com/AZX-PBC-OSS/TaskQ/commit/9d17a7d0e84299860dbc27300fe1a1cd73b260ac))
* drop the state-change breaking entry -- the rename never touched production ([b9ec371](https://github.com/AZX-PBC-OSS/TaskQ/commit/b9ec3714f81daf9605ea3717cbb48629637ba910))
* **e2e:** reference the surviving chaos modules in the infra-free guard ([89783fe](https://github.com/AZX-PBC-OSS/TaskQ/commit/89783fe22f7672f9cdfa242fbd5762a66a655e00))
* fix doc-vs-code drift across extras, protocol listing, and guides ([8018981](https://github.com/AZX-PBC-OSS/TaskQ/commit/8018981da90e257f47c4425c6d76c48ec64f7464))
* fix stale keyed-eviction scheduling claim; robustify register snippet ([bb96b97](https://github.com/AZX-PBC-OSS/TaskQ/commit/bb96b9765ca609e6712ed6005204ef7c3affc6ec))
* **jobs:** disclose that enqueue_batch_streaming never enforces max_pending ([dcadc79](https://github.com/AZX-PBC-OSS/TaskQ/commit/dcadc7932cfac7c3877ddc4fbd6fabea286ffb57))
* **jobs:** state the caller-connection batch-row ordering and its counting gap ([f6345d7](https://github.com/AZX-PBC-OSS/TaskQ/commit/f6345d70dfc518578e40a5f7b1db8b2a73000df8))
* note rate_limit_registry ambiguity TypeError and collection-log semantics in bootstrap ([5b9cfb4](https://github.com/AZX-PBC-OSS/TaskQ/commit/5b9cfb40d504b3b91a1285e14948b8fa264a178d))
* **obs:** correct two claims the code does not support ([130ed81](https://github.com/AZX-PBC-OSS/TaskQ/commit/130ed81e1a48257cfa7761e487c4cce424d794e3))
* **ops:** say what actually strands a deregistered actor's running job ([a81beb7](https://github.com/AZX-PBC-OSS/TaskQ/commit/a81beb7d8a545e2ef75cd1352465cc2137d7e13a))
* **pg:** note that verify-ca/verify-full need an explicit sslrootcert ([55dec89](https://github.com/AZX-PBC-OSS/TaskQ/commit/55dec89aa3a45b39e1405742ac16660cf879d2ac))
* rate-limit registry ownership model, testing patterns, architecture paragraph ([d325f84](https://github.com/AZX-PBC-OSS/TaskQ/commit/d325f8410ba136f2a34faf7763af3199e932db4b))
* record every consumer-visible change this consolidation ships ([8563283](https://github.com/AZX-PBC-OSS/TaskQ/commit/8563283171aa015a3f6ae4b2210b696b648a20c7))
* record the seven consumer-visible changes 8563283 left out ([fb5ce38](https://github.com/AZX-PBC-OSS/TaskQ/commit/fb5ce381d13e56e9022c4fd0141302bd29cdfd73))
* **settings:** correct the environment field description to match shipped behavior ([20a2dce](https://github.com/AZX-PBC-OSS/TaskQ/commit/20a2dce0129ad1c80a40a225ba12a962a7071aee))
* soften prerequisites registration claim for actor-declared instances ([6154b26](https://github.com/AZX-PBC-OSS/TaskQ/commit/6154b2637d1239247c4b42c88a423e804f7da659))
* state the server-anchored next_fire_at seeding truth in ScheduleCreateArgs ([fedef48](https://github.com/AZX-PBC-OSS/TaskQ/commit/fedef48c3cdbaeea5d74ee2b52c2fb6a088f1d8c))
* **test:** module docstring describes the inventory audit that replaced the grep ([92a4e44](https://github.com/AZX-PBC-OSS/TaskQ/commit/92a4e44ecd9241a837cd643decd779ab5722a93f))
* **test:** record the triage of the static guards that survive the sweep ([962fc29](https://github.com/AZX-PBC-OSS/TaskQ/commit/962fc2927d7778d13ae2d9a62298dfb86e02a81c))
* update Backend protocol listing in architecture guide ([f6ec7c9](https://github.com/AZX-PBC-OSS/TaskQ/commit/f6ec7c92dcef51f8272aa140d869391e2e126150))
* update rate-limiting guide and architecture for typed key_fn ([ebfb75c](https://github.com/AZX-PBC-OSS/TaskQ/commit/ebfb75c9e0a9ed1812b2b8670dd6ea2427b8cd9f))
* update workers.md, cli.md, architecture.md for until-idle mode ([4f27c44](https://github.com/AZX-PBC-OSS/TaskQ/commit/4f27c44a951274191effd36240d8fb0c080359a5))
* **upgrading:** warn that capacity literals no longer win, and pin the asymmetry ([043fa98](https://github.com/AZX-PBC-OSS/TaskQ/commit/043fa98a90ac714432840e3736913b306fe8bffe))
* **worker:** failure-log contract names the events that are actually emitted ([ffbc79c](https://github.com/AZX-PBC-OSS/TaskQ/commit/ffbc79c24293d7fffd03361d2e8c30e107a42be7))


### Miscellaneous Chores

* **deps:** bump dotenvmodel to &gt;=1.0.0,&lt;2, adopt 1.0 defaults, drop workarounds ([ea22a66](https://github.com/AZX-PBC-OSS/TaskQ/commit/ea22a66526054f4bb894392ece0e198a1349cdea))


### Code Refactoring

* conftest unit-test isolation uses public RateLimitRegistry.clear() ([da64077](https://github.com/AZX-PBC-OSS/TaskQ/commit/da64077bfadaa5ae84d6316f3f510c665774fef9))
* drop the clock parameters left dead by the DB-clock unification ([b888823](https://github.com/AZX-PBC-OSS/TaskQ/commit/b88882327b8be43ce7925b25c7fb792bdb688160))
* extract filter→SQL WHERE builder into _filter_sql.py ([4262eeb](https://github.com/AZX-PBC-OSS/TaskQ/commit/4262eeb859d406287089b502853c2733de92bbee))
* extract filter→SQL WHERE builder into _filter_sql.py ([3ca37a3](https://github.com/AZX-PBC-OSS/TaskQ/commit/3ca37a3de0d0e35122b8ff881e0abb33caae3a20))
* move actor_config and actor_config_ops to top-level package ([c2b1674](https://github.com/AZX-PBC-OSS/TaskQ/commit/c2b1674cbe96ec70fdc0d44823e3e96e2aef53ef))
* **obs:** type the exc_info boundary honestly for pyright strict ([7cbf553](https://github.com/AZX-PBC-OSS/TaskQ/commit/7cbf55306b487d4760f3a2f7f4d77bc31f58d653))
* remove sub_job_inherit_tags kill switch ([aa1fdfc](https://github.com/AZX-PBC-OSS/TaskQ/commit/aa1fdfc714f59c57ae042805eba670b9ea5b6ab9))
* resolve every remaining accepted-and-ignored parameter ([4b9fc12](https://github.com/AZX-PBC-OSS/TaskQ/commit/4b9fc12030346d9d92ae38c2acbd80496e48a874))
* style and architecture improvements in batch subsystem ([dcd1643](https://github.com/AZX-PBC-OSS/TaskQ/commit/dcd164367ec374905244783ab1d80782255f5650))


### Continuous Integration

* make uv usage locked, no-sync and interpreter-pinned ([a19b732](https://github.com/AZX-PBC-OSS/TaskQ/commit/a19b7327e5f48a11912d75aa2bb296baf27be0df))
* make uv usage locked, no-sync and interpreter-pinned ([81d18ba](https://github.com/AZX-PBC-OSS/TaskQ/commit/81d18baf7f34997dcdfa3fe9af922b631fe797cf))

## [Unreleased]

### Changed (breaking)

* **`KeyedRateLimitRef` and `KeyedReservationRef`: `payload_type` is now required and `key_fn` receives the validated Pydantic model, not the raw dict.** Every existing keyed-ref declaration must be updated:

  ```python
  # BEFORE (broken):
  KeyedRateLimitRef(
      base_name="api-per-tenant", key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0
  )

  # AFTER:
  KeyedRateLimitRef.typed(
      MyPayload,
      base_name="api-per-tenant",
      key_fn=lambda p: p.tenant_id,
      capacity=10,
      refill_per_second=1.0,
  )
  ```

  Use `.typed()` for compile-time type checking of `key_fn` against the payload model.

* **Dispatch-path malformed payloads now fail immediately as `PayloadValidationError` (non-retryable)** instead of being retried as a generic `ValidationError`. In-flight legacy rows with invalid payloads will fail on first dispatch instead of exhausting the retry budget.

* **Per-key budget reset hazard during deploys:** if defaults or validators change a key-deriving field's value vs the raw row, new concrete names materialize fresh full-capacity buckets alongside old ones (temporary over-admission window). Drain affected queues before deploying payload model changes that affect key derivation.

* **`taskq.validate_actor_payload` now resolves to the sanitized implementation, and its third parameter's keyword name is `actor`, not `actor_name`.** The public `taskq.validate_actor_payload` export previously resolved to a duplicate in `taskq.exceptions` that embedded the raw payload in its message (`Raw payload: {...}`) and attached pydantic errors with `include_input`. That message is persisted to the job row's `error_message` and rendered in the web admin, so attacker-controlled payload values could leak into both — this is an information-disclosure fix, not a refactor. The one remaining implementation lives in `taskq._validation` (`include_url=False`, `include_input=False`, no raw-payload embedding); `taskq.exceptions.validate_actor_payload` re-exports it lazily. **Call-site break:** the keyword `actor_name=` is now `actor=` (and is now optional, defaulting to `None`); the third *positional* argument is unaffected. Two further observable changes: the raised message is now `Payload validation failed for actor '<name>': <model title>` with no pydantic detail and no payload dump, and `validation_errors` entries no longer carry `input` or `url` keys — anything parsing `error_message` or reading those keys must be updated. The `raw_payload` parameter also now accepts an existing `BaseModel` as well as a `dict`. See [docs/guides/upgrading.md](docs/guides/upgrading.md).

* **Payload validation now runs before rate-limit acquisition, and `acquire_for_actor` receives the validated model instead of the raw row dict.** On the dispatch path nothing changes: `dispatch_one_job` already validates and passes `validated_payload`, so workers behave identically. The change is visible only to **direct callers of `consume_one_job`** that pass `validated_payload=None`. For those, an invalid payload now raises `PayloadValidationError` *before* any token is acquired; previously the consumer acquired first and burned a **non-refunded** token (`release_for_actor` sets `refund_on_release=False`) for an actor body that could never run. The error escapes to the caller, which owns the terminal write — as `dispatch_one_job`'s outer handler and the in-memory test runner already do. This also fixes a real defect in keyed refs: the registry re-validated the *raw* dict against the ref's `payload_type`, dropping the actor model's defaults, so an actor defaulting `tenant_id="unattributed"` against a ref model requiring `tenant_id` failed the job non-retryably on a payload that was perfectly valid for the actor. Same-model refs now hit the registry's `isinstance` fast path; a stricter cross-model ref re-validates the model's `model_dump()`, which carries the actor model's applied defaults and aliases.

* **`WorkerSettings` now rejects at load time several values it previously accepted.** A deployment whose configuration contains any of these stops starting on upgrade, with a settings-load error rather than the opaque mid-startup failure it used to produce. Audit before rolling out: `schema_name` longer than 63 characters (Postgres' `NAMEDATALEN` silently truncated it, while Redis channel templates interpolated the full string — the two stores quietly diverged); `workgroup_instance` that is not a valid UUID (previously a raw `ValueError` mid-registration); `worker_label` containing a NUL (previously an opaque asyncpg `22021 CharacterNotInRepertoireError` at startup); and any item of `queues` that does not match the canonical queue-name charset (letters, digits, `_`, `.`, `-`, first character a letter or `_`). Two new cross-field watchdog invariants also apply, but **only when `watchdog_enabled=True`**: `watchdog_loop_lag_budget + heartbeat_interval` must be `< lock_lease` (a stalled loop must die before its leases expire, or the leader sweep reclaims live jobs' locks mid-stall), and `watchdog_loop_lag_budget` must be `> watchdog_check_interval` (a budget at or below the sampling period trips on a healthy idle loop — measured: a 1.0 budget against the 1.0 s default check interval force-exits an idle worker on its first armed poll). These raise `ValidationError` / `MultipleValidationErrors`, not `ValueError` — see the dotenvmodel note below.


### Added

* `JobsClient.cancel_where(filter, reason)` — bulk cancel all jobs matching a
  `JobFilter` in a single set-based operation. Pending/scheduled jobs go straight
  to terminal `cancelled`; running jobs get cooperative cancel (`cancel_phase=1`).
  Returns `BulkCancelResult` with counts and affected IDs. Empty filters are
  rejected with `EmptyFilterError` unless `allow_empty_filter=True` is passed.
* `SubJobEnqueuer.enqueue()` now accepts `tags`, `inherit_tags`,
  `schedule_to_close`, `start_to_close`, and `heartbeat_timeout` parameters.
  Sub-jobs inherit the parent job's tags by default (`inherit_tags=True`); pass
  `inherit_tags=False` to suppress inheritance for a specific sub-job.
* `BulkCancelResult` and `EmptyFilterError` exported from `taskq` top-level.


- **Batch failure policies (`AbortBatchAfter`)** — #55. An opt-in
  `failure_policy` parameter on `enqueue_batch()` /
  `enqueue_batch_streaming()` creates a `batches` row and drives
  abort-on-consecutive-failure semantics via the
  `apply_batch_terminal_outcome` hook. When the threshold is reached the
  batch is aborted: pending/scheduled child jobs are cancelled and the
  batch row is set to `aborted`.
- **Batch finalizer (transactional enqueue with batch)** — #58. A
  `finalizer` parameter on `enqueue_batch()` /
  `enqueue_batch_streaming()` enqueues a finalizer job alongside the
  batch in the same transaction. The finalizer is NOT stamped with
  `batch_id` (deadlock prevention); `wait_for_batch` automatically
  excludes it from counts via the batch row's `finalizer_job_id`.
- **Batch discovery (`list_batches`, `BatchSummary`)** — #59.
  `JobsClient.list_batches(BatchFilter)` returns `BatchSummary` objects
  with live job-count aggregates. `BatchFilter` carries only
  batch-relevant fields (`queue`, `active`, `batch_id`, `limit`).
- **`enqueue_batch_streaming` for unbounded iterables** — accepts an
  `Iterable[EnqueueItem]` (including generators) and inserts in chunks
  of `chunk_size` (1–1000). All items share the same `batch_id`.
- **`wait_for_batch` with `expect_at_least`, `on_empty`,
  `exclude_job_id`** — `expect_at_least` raises `EmptyBatchError` when
  fewer than the expected number of jobs are present; `on_empty`
  controls behaviour when zero jobs and no `batches` row exist
  (`"error"` raises, `"ok"` returns empty status); `exclude_job_id`
  omits a specific job from counts (defaults to the batch row's
  `finalizer_job_id`).
- **Backend protocol batch methods (10 new methods)** —
  `enqueue_batch_atomic`, `create_batch`, `increment_batch_failures`,
  `reset_batch_failures`, `abort_batch`, `complete_batch`, `get_batch`,
  `list_batches`, `count_batch_non_terminal`, `prune_old_batches`.
- **Batches table migration (01.00.05_01)** — adds the `batches` table
  with columns for status tracking, failure counters, finalizer linkage,
  and batch-level metadata.
- **Connection hook points for managed-identity / BYO connections** —
  `WorkerConnections` dataclass with per-role pre-constructed resources
  (caller-owned) or zero-arg async factories (TaskQ-owned) for the worker's
  three PG pools, notify/leader dedicated connections, and Redis client.
  `worker_main(..., connections=...)` and `open_worker_deps(...,
  connections=...)` accept it; fields left `None` fall back to DSN
  construction. `PoolFactory`, `ConnFactory`, `RedisFactory` type aliases
  exported from `taskq` top-level.
- **Vendor-neutral credential provider abstraction** (`taskq.auth`) —
  `PgCredentialProvider` and `RedisCredentialProvider` async Protocols
  with reusable `make_pg_pool_factory`, `make_dedicated_conn_factory`,
  `make_redis_client_factory` builders. Any provider implementing the
  Protocols gets all factory builders for free. The PG factories pass the
  credential to asyncpg as `user=` / `password=` keyword arguments
  (which take precedence over both DSN userinfo and DSN query
  parameters), so the token never appears in the DSN string;
  `enrich_pg_dsn` remains as the string-helper variant (writes the
  credential into DSN userinfo; adds `sslmode=require` only when the DSN
  has no explicit sslmode — `verify-full` is never downgraded). All four
  helpers are exported from the `taskq` top level as well as
  `taskq.auth`.
- **`taskq[aad]` extra** — `taskq.aad` module with Microsoft Entra ID
  providers (`EntraIdProvider`, `EntraIdPgProvider`, `EntraIdRedisProvider`)
  backed by `azure.identity.aio` (the extra includes `aiohttp`, required
  by the async credentials). Providers constructed with `credential=None`
  lazily create one `DefaultAzureCredential` and reuse it; sync
  `azure.identity` credentials are supported and offloaded to a thread.
  See `docs/guides/managed-identities.md`.
- **`taskq[aws]` extra** — `taskq.aws` module with `RdsIamProvider` for
  AWS IAM RDS Postgres authentication, backed by `boto3`.
- **`taskq[vault]` extra** — `taskq.vault` module with
  `VaultDynamicDbProvider` for HashiCorp Vault database secrets engine
  dynamic credentials, backed by `hvac`.
- **`TaskQ` stream hooks** — `pg_conn_factory` and `listen_conn`
  parameters for the LISTEN/NOTIFY transport in `TaskQ.stream()`, so
  pool-only / AAD deployments can stream without a DSN. `stream()` now
  uses `contextlib.aclosing` to ensure the inner generator's `finally`
  (conn close) runs promptly on early return.
- **`migrate.apply_pending_locked` hooks** — `conn` (caller-owned) and
  `conn_factory` (TaskQ-owned) parameters replace the DSN-only path.
- **Credential hot-reload (SIGHUP / interval / programmatic)** —
  hot-swaps every factory-backed PG pool, dedicated connection, and
  Redis client with freshly-built replacements (each factory fetches a
  fresh credential). Triggers: SIGHUP; `TASKQ_RELOAD_INTERVAL`
  (seconds, unset by default) for periodic reloads with no external
  signal — the only rotation path on Windows; and
  `WorkerDeps.request_reload()` / `reload_credentials(deps)` for
  embedders. Each factory call is bounded by
  `TASKQ_RELOAD_FACTORY_TIMEOUT` (default 30 s). The swap is atomic: the
  old pool stops serving new acquisitions immediately and is closed in
  the background with a bounded drain (default 5 s), then terminated —
  an in-flight actor that outlives the drain sees its next acquire fail
  and the job retries on the new pool. DI-injected `db: asyncpg.Pool`
  actors resolve the new pool (LOOP-scope cache refresh) and progress
  flushing follows the swap. A SIGHUP arriving mid-reload (success or
  failure) triggers exactly one follow-up reload; reloads are skipped
  while shutdown is in progress. Each resource reloads independently —
  one factory failure is logged and does not abort the rest; the
  `credentials-reloaded` log line's `failed` field reports any resource
  that didn't rotate. Caller-owned resources are not swapped.
- **NOTIFY listener resilience** — the reconnect loop rebuilds a dropped
  LISTEN connection through the user-supplied `notify_conn_factory` (or
  the DSN closure it was opened with) instead of a stale/absent DSN. A
  caller-owned `notify_conn` that drops disables the listener
  (poll-based dispatch fallback) instead of crashing the worker.
- **Ownership-contract enforcement** — caller-owned pools/connections/
  Redis clients are never closed by TaskQ (including shutdown paths). A
  caller-owned `leader_conn` with no `leader_conn_factory` and no
  `pg_dsn_direct` is a startup `ValueError` (no rebuild path).
  TaskQ-owned dedicated connections (DSN- or factory-built) get TCP
  keepalive.
- `taskq.worker` re-exports `WorkerConnections` and `reload_credentials`
  (lazy, alongside the existing `WorkerDeps` / `open_worker_deps`).
- `ErrorReporter` Protocol for vendor-neutral terminal failure routing (Sentry, Datadog, DLQ) with `NullErrorReporter` default and `taskq.error_reporter.failures` OTel counter
- `retry_classifier` hook on `@actor` for exception-instance-level retry classification (inspect attributes like HTTP status codes, return `RetryOverride` to refine kind/delay per occurrence)
- `RetryOverride` and `RetryClassifierHook` types exported from `taskq` top-level
- `on_success` hook on `@actor` for success callbacks (mirrors `on_retry_exhausted` with timeout guard)
- `start_to_close` per-attempt execution timeout with precedence chain: per-enqueue > `@actor(start_to_close=...)` > `TASKQ_DEFAULT_START_TO_CLOSE` worker fallback
- `KeyedReservationRef` for dynamic per-key (session/tenant) concurrency caps computed from job payload at dispatch time
- `name` and `identity_key` fields on `CronScheduleSpec` for per-property cron schedules and cron↔on-demand dedup
- `JobSortField` enum and `JobFilter.order_by` for "latest run by business key" queries
- `admin_actions_enabled` and `admin_ui_require_auth` security settings for admin UI
- `max_keyed_reservations` setting to guard against unbounded keyed reservation growth
- Consolidated testing guide (`docs/guides/testing.md`)
- **SSO / SAML auth for admin UI**
  - OIDC backend (`taskq[oidc]`): PKCE flow, JWKS validation, signed-cookie sessions
  - SAML backend (`taskq[saml]`): python3-saml, SP metadata, attribute extraction
  - Shared `AuthBundle`/`IdentityClaims` abstraction — both backends use the same
    session handling and group/role allowlist
  - `token_auth()` helper for machine-to-machine bearer-token auth
  - `TASKQ_SSO_BACKEND=none/oidc/saml` CLI integration for standalone `taskq ui serve`
  - Health/metrics endpoints wired into `taskq ui serve` with fail-closed
    `TASKQ_HEALTH_TOKEN`/`TASKQ_HEALTH_REQUIRE_TOKEN` pattern
  - `OIDCSettings`/`SAMLSettings` as separate DotEnvConfig classes with prefix scoping
- **`TASKQ_ADMIN_UI_SECURE_COOKIES` (`admin_ui_secure_cookies`, default `True`)** — sets the `Secure` flag on the admin UI's CSRF cookie. The flag was previously derived from `request.url.scheme`, so behind a TLS-terminating edge (Azure Application Gateway, App Service) the app saw plain `http` and silently dropped `Secure` on exactly the deployments that need it — while the session cookie, which already used a configured flag, kept it. Set it to `False` only for local http dev, where a `Secure` cookie is rejected by the browser and the UI stops working. A one-shot `admin-ui-cookie-scheme-mismatch` warning fires when the configured value contradicts the observed scheme; run uvicorn with `--proxy-headers` so `X-Forwarded-Proto` is honoured.
- **`TASKQ_ADMIN_UI_FRAME_ANCESTORS` (`admin_ui_frame_ancestors`, default `none`)** — who may frame admin pages. Every admin response now carries `Content-Security-Policy: frame-ancestors '<value>'` and the legacy `X-Frame-Options` (`DENY` for `none`, `SAMEORIGIN` for `self`). **Admin pages can no longer be iframed**: a host application that embeds the admin UI in its own dashboard must set `TASKQ_ADMIN_UI_FRAME_ANCESTORS=self` or the frame renders blank. Only `none` and `self` are accepted; anything else fails at settings construction rather than silently emitting no header. CSRF is no defence against UI redress — the framed page is the real, authenticated, same-origin page, so a tricked click carries a valid token.


### Changed

* **Sub-jobs now inherit parent tags by default.** Every `ctx.jobs.enqueue()`
  call inside an actor body now propagates the parent job's tags to the sub-job,
  making sub-jobs findable by `JobFilter(tags=...)` and cancellable by
  `cancel_where`. Pass `inherit_tags=False` per-call to opt out. This is a
  behavior change for any code that relied on sub-job tags being empty —
  inherited tags make sub-jobs visible to tag-based filters and bulk cancels.


- **dotenvmodel bumped 0.3.0 → 0.5.0.** `WorkerSettings` now uses dotenvmodel's native `post_load()` hook (added in 0.5.0) instead of a manual `_post_load` method called from `load()`/`load_from_dict()` overrides. The base `DotEnvConfig._load_fields` invokes `post_load` automatically on every load path — `load()`, `load_from_dict()`, and `reload()` — including under `validate=False`. The redundant `WorkerSettings.load`/`load_from_dict` overrides have been removed.
- **Breaking: cross-field invariant exceptions changed type.** `WorkerSettings.load()`/`load_from_dict()` cross-field invariants (`lock_lease >= 4 * heartbeat_interval`, grace-budget checks) previously raised `ValueError`; they now raise `ValidationError` (single failure) or `MultipleValidationErrors` (several at once). `ConstraintViolationError` (field validators) was already not a `ValueError`. **Callers that catch `ValueError` around `WorkerSettings.load*()` will no longer catch these** — catch `DotEnvModelError` (the common base) to cover both single and aggregate cases, or `ValidationError` when at most one invariant can fire. Field-level validation (`prune_retention_*`, `default_start_to_close`, `log_format`, etc.) already raised `ConstraintViolationError` and is unaffected.
- **`reload()` now enforces cross-field invariants and applies DSN fallback.** Previously `reload()` did not run `_post_load` (it was only called from the `load()`/`load_from_dict()` overrides), so a reload that produced invariant-violating values would silently succeed. This is now fixed by the native `post_load` hook.
- **`log_format` validation moved from `choices=` to a `validator` hook.** `choices=` is a built-in constraint that `load_from_dict(..., validate=False)` skips, so an invalid `TASKQ_LOG_FORMAT` could previously load silently under `validate=False`. The validator hook runs regardless of `validate=`, closing the hole. Error message changed from `log_format must be 'json' or 'console'` to `log_format must be one of ['console', 'json'], got <value>`.
- **Breaking: `wait_for_batch` default `on_empty="error"` raises
  `EmptyBatchError` instead of silent return.** Previously, calling
  `wait_for_batch` on a batch_id with zero jobs and no `batches` row
  returned an empty `BatchCompletionStatus` silently. The default is now
  `on_empty="error"`, which raises `EmptyBatchError`. Pass
  `on_empty="ok"` to preserve the old silent-return behaviour.
- **Breaking: structured-log field rename in sub-enqueue failure events.**
  `sub_enqueue_re_enqueue_error` and `sub_enqueue_flush_error` now carry
  `error_class` + `error_message` instead of the single `message` field,
  matching the `error_class`/`error_message` convention used by every
  other error event (`job_timeout`, `job_exception`, `job_failed`,
  `rate_limit_release_failed`, `savepoint_rollback_failed`,
  `stranded_jobs_query_failed`, and the `failed_details` payload of
  `sub_enqueue_flush_failed`). Log pipelines querying `fields.message`
  on these two events must switch to `error_message`.
- **Breaking: `taskq.worker.actor_config` moved to `taskq.actor_config`.**
  The `ActorConfig` dataclass (released in v0.2.0–v0.2.2 at
  `taskq.worker.actor_config`) has moved to the top-level
  `taskq.actor_config` module. It is shared by the client, CLI, and admin
  UI, not worker-internal. The old import path raises `ImportError`. See
  [docs/guides/upgrading.md](docs/guides/upgrading.md) for the full
  migration mapping. The companion `actor_config_ops` module (listing,
  inspecting, tuning, and deregistering actors) has likewise moved from
  `taskq.worker.actor_config_ops` to `taskq.actor_config_ops`; it was
  never released under the `worker.*` path.
- **Breaking: dotenvmodel bumped to 1.x (`>=1.1.0,<2`), adopting its 1.0 defaults.** Environment-variable precedence flips: the process environment now beats `.env` files by default (previously `.env` values overwrote `os.environ`); restore the files-beat-env-vars behaviour with `DOTENV_OVERRIDE=true` or `TaskQSettings.load(override=True)`. `load()` no longer mutates `os.environ` — read `TASKQ_*` values from the settings instance, not the process environment, after a load. `TaskQSettings.load()` now forwards dotenvmodel's full parameter surface (`env`, `override`, `env_dir`, `read_dotfiles`, `read_environ`, `load_local`). Subclass string-field defaults containing `${VAR}` references are interpolated at load time (unset references resolve to `""`).
- **Breaking: time is unified on the database clock — the enqueue and rate-limit surfaces changed shape.** `EnqueueArgs.scheduled_at` is now nullable: "immediate" enqueue passes `None` and the server stamps it (no more client-side `now()` default), and `Backend` implementations that require a non-`None` datetime fail loudly. The raw `schedule_to_close` datetime form is deprecated in favour of `schedule_to_close_interval` (or declaring `retry.time_budget` on the actor — absolute datetimes cross clock domains and can misbehave under skew); every enqueue arm writes the deadline from one domain (server clock + interval). The rate-limit Redis Lua scripts derive `now` from `redis.call('TIME')` — the caller-supplied `now` ARGV is removed.
- **Every mixed-clock decision is now single-arbiter on the store's clock.** The application process and the database server keep separate clocks that can diverge or step (VM pause/resume, NTP drift); every place that mixed the two domains in one decision is anchored to the database clock: workgroup supervisor freshness is computed server-side (a skewed supervisor host can no longer kill healthy children); cron ticks read the server clock inside the leader transaction, with the catch-up cutoff and beyond-window recompute server-anchored (no fire-loops or silently skipped backlog under leader-clock skew); rate limiting runs on the store's clock (PG window predicates and GCRA/token-bucket epoch math are server-side; peeks measure against the store clock too); prune/archive cutoffs and enqueue-pinned result TTLs are stamped server-side; the batch COPY path is server-stamped via an in-transaction fixup (`status`, `created_at`, `scheduled_at`, `schedule_to_close`, `result_expires_at`), so dedup windows hold under skew.
- **`taskq[oidc]` no longer installs `httpx`; its `authlib` floor is now `>=1.8.0`.** authlib 1.8.0's `httpx_client` integration is httpx2-first (httpx is only a deprecated fallback), the direct OIDC calls (discovery, JWKS fetch) use `httpx2`, and nothing under `src/taskq` imports `httpx` — the extra's `httpx` entry was redundant (authlib 1.7.x, which imported `httpx` unconditionally, is excluded by the new floor).
- **`SubJobEnqueuer.enqueue_batch([])` now raises `ValueError` instead of returning `[]`.** Without a connection the fallback loop iterated zero items and returned an empty list silently, while `JobsClient.enqueue_batch` already raised pre-I/O and the streaming path raised on the first peek. An empty fan-out is now an error at every layer — guard the call site if your item list can legitimately be empty.
- **`taskq queues set-max-concurrent --max-concurrent` now requires `>= 1`; `0` is rejected.** The typer option's minimum moved from `0` to `1`, and the underlying `taskq.worker.queue_ops.set_queue_max_concurrent` now raises `ValueError` below `1` (previously below `0`). `0` was accepted by both layers and then died on the table's `CHECK (max_concurrent IS NULL OR max_concurrent >= 1)`, handing the operator a raw asyncpg `CheckViolationError` traceback — so this trades a crash for a clean error, but it is still a contract change for scripted callers. `NULL` (via `--clear`) remains the uncapped state; an emergency drain to `0` belongs to the per-actor `actor-config set --max-concurrent 0`, which still allows it.
- **Admin job-list filter inputs are now bounded and return 400 past their caps.** On `/jobs` (and `/jobs/count`, `/history`) the `status` list is deduplicated in first-occurrence order (values outside the closed status set are still a 400). The `/jobs` `tags` filter is stripped and deduplicated, rejected with a 400 above 16 items, and rejected with a 400 for any item longer than 255 characters (the enqueue-side tag length limit, so a longer filter term could never match a stored tag anyway). Deduplicated, otherwise-valid requests are unchanged; a client that previously sent repeated statuses or an oversized tag list now gets a 400 where it used to get a 200.
- **Queue names are now validated at the enqueue and actor-declaration chokepoints.** `JobsClient.enqueue()` (per-call `queue=` override and the actor-declared default alike) and `@actor(queue=...)` now run the backend's canonical `_validate_queue_name`, raising `ValueError` — at decoration time, which is import time in the common case. The `QueueName` annotation is inert at runtime (its `AfterValidator` only fires inside pydantic model validation), so a typo'd queue name previously sailed through and stranded every job on a queue no worker's `queue = ANY($1)` ever matched. **A typo that used to fail silently now fails loudly at import.**
- **The queue-name, tag, and keyed-key regexes are re-anchored `\A...\Z` instead of `^...$`.** Python's `$` also matches immediately before a trailing newline, so `"default\n"`, `"mytag\n"`, and `"key\n"` all satisfied the old patterns. Such values are now rejected. If any queue name, tag, or keyed rate-limit key in your system has a trailing newline — most plausibly from a shell `$(...)`, a file read, or an unstripped environment variable — it will start raising `ValueError`.
- **NUL bytes are rejected in caller-supplied text instead of surfacing as a database error.** `JobFilter` (`queue`, `actor`, `identity_key`, `tags`) and `ScheduleCreateArgs` (`actor`, `name`, `timezone`, `payload_factory`, `identity_key`) now raise `ValueError` in `__post_init__`; the admin UI's text filters (`actor`, `queue`, `search`, `identity_key`, `fairness_key`, `tags`) now return a clean 400. Previously these reached Postgres and came back as an opaque asyncpg `22021` — a 500 from the admin routes.
- **`ScheduleCreateArgs.dst_strategy` is now validated in `__post_init__`** and raises `ValueError` for a value outside the known set, which is newly exported as `taskq.cron.DST_STRATEGIES`. An unrecognized strategy previously constructed fine and took the default branch at cron-tick time.
- **Breaking: `firstof` and `allof` `dst_strategy` become live for schedules that already exist.** The cron tick's `SELECT` never listed the `dst_strategy` column and `fire_schedule` read it as `row.get("dst_strategy", "skip")`, so the stored value was **always** `skip` in production whatever the schedule said — the branch that enqueues the second job for a DST fall-back overlap was unreachable. The column is now selected and read, so a schedule configured `allof` or `firstof` years ago changes behaviour on upgrade, with no configuration change and nothing raised: on the autumn fall-back night an `allof` schedule enqueues **two** jobs for the repeated local hour where it used to enqueue one. Audit for non-`skip` schedules before rolling out, and make sure their actors are idempotent.
- **Breaking: `worker_id` is no longer a dimension on six metric instruments.** `taskq.lock.expires_in_seconds`, `taskq.heartbeat.misses`, `taskq.leader.election_attempts`, `taskq.leader.election_failures`, `taskq.cron.lock_contention` and the `taskq.heartbeat.consecutive_failures` gauge now emit a single undimensioned series each. **Dashboards, queries and alert rules that group or filter by `worker_id` on these metrics will break** — they return one series where they used to return one per worker. `worker_id` is a fresh UUID per worker *process*, so every deploy, restart and autoscale event minted new time series without bound; Azure Monitor counts each unique (metric, dimension key, dimension value) seen in 12 hours as an active series, caps a subscription at 50,000 per region, and throttles ingestion for *every* custom metric once the cap is passed, with no backfill of what was dropped. Per-worker attribution is unchanged on the channels where cardinality is free: `worker_id` is bound onto every log line via contextvars, and `taskq.worker_id` is a cron-fire span attribute. The `record_*` helpers still accept their `worker_id` argument — only the dimension is gone. `taskq.cron.consecutive_failures` keeps its `schedule_id` dimension: schedules are a bounded, operator-created set, and `cron_auto_disable_threshold` is evaluated per schedule.
- **The admin session cookie is now scoped to the admin mount path.** `taskq_session` carried no `path=`, so it defaulted to `/` and the browser attached it to every request to the host application that mounts the admin UI — including routes with no reason to see an admin session. Both SSO backends now set `path` to their `base_path`, and logout clears it on the same path (a delete on a different path clears nothing, which would have left a live session behind). **A stale `path=/` cookie written by a previous version is not replaced by the new one** — the browser keeps both and sends both, and the broader one can shadow the narrower until it expires. Operators upgrading should clear the `taskq_session` cookie, or expect one session lifetime (`session_max_age_seconds`, default 8h) of overlap.


### Fixed

- SQL injection in `batch.py` `BatchHandle.status()` and `wait_for_batch()` — `schema` parameter now validated against `_IDENT_RE` before SQL interpolation
- Fire-and-forget progress publish — `ctx.progress()` no longer blocks the actor on a synchronous Redis round-trip; publishes via background tasks with drain-on-shutdown
- Stale `[web]` extra references in README and CI — replaced with `[fastapi]`
- `ErrorReporter.report()` now has a timeout guard (`error_reporter_timeout`, default 3s) matching `on_retry_exhausted` convention
- `ErrorReporter.report()` argument order aligned with `OnRetryExhausted`: `(job, exception)` not `(error, job)`
- `retry_classifier` hook return value validated — non-`RetryOverride` returns caught and logged, not crash
- `retry_classifier` hook skipped for `non_retryable_exceptions` and `PayloadValidationError` — matches documented contract
- `on_retry_exhausted` now uses `inspect.isawaitable()` instead of `inspect.iscoroutine()` — handles non-coroutine Awaitables
- Rate-limit `refund()` for memory and Postgres log-style sliding window — was silent no-op, now properly frees slots
- Token-bucket `refund()` on Postgres backend — was silent no-op, now properly refunds tokens (capped at capacity) via `FOR UPDATE` on `rate_limit_buckets`
- `_di/solver.py` debug log now reports real `cache_hit` value instead of hardcoded `False`
- `worker/_leader_sweeps.py` logs warning on invalid schema and includes error detail in exception handlers
- `testing/pg.py` validates schema against `_IDENT_RE` before SQL interpolation
- `worker/notify.py` logs debug on NOTIFY payload parse failures
- Admin UI "run schedule now" endpoint checks `enabled` flag and has cooldown rate limiting
- Admin UI cron payload_factory error redirect uses generic error code instead of reflecting exception text
- Admin UI fails closed by default in non-dev environments when no `auth_dependency` is configured
- `humanize` moved from core to `[fastapi]` extra (was bloating core install)
- `starlette` and `prometheus_client` declared as direct dependencies (were transitive-reliance)
- Dependency upper bounds added to `asyncpg`, `redis`, `pydantic`, `fastapi`, `typer`, `dotenvmodel`, `uuid-utils`, `uvicorn`, `structlog`, `opentelemetry-instrumentation`, `prometheus-client`
- Worker exception handlers no longer swallow failure diagnostics. Timeout
  and generic-exception attempts log `job_timeout` / `job_exception`
  WARNING events carrying `error_class` / `error_message` /
  `error_traceback`; every terminal (non-retryable) failure across all
  five handlers emits exactly one `job_failed` ERROR event (`job_id`,
  `actor`, `attempt`, `cause`, `error_class`, plus handler context such as
  `snooze_count` / `consume_budget` / `bucket_name`) — one alertable event
  per dead job, and per-attempt diagnostics at WARNING so retryable
  attempts produce zero ERROR noise. Tracebacks are formatted from the
  explicit exception object rather than the ambient `sys.exception()`, so
  handler invocations outside an `except` block no longer record
  `'NoneType: None'`. The `terminal-write-failed` event now includes
  `job_error_traceback` and `infra_error_traceback`. Timeout spans
  (`lifecycle.scheduled` / `lifecycle.failed`) now report the concrete
  exception class instead of hardcoded `TimeoutError`, agreeing with the
  log fields. Snooze / RetryAfter / ReservationUnavailable terminal
  outcomes and the stranded-jobs leader sweep also log their failure
  details instead of continuing silently.
- `TaskQ(redis_url=...)` validation: the URL routes through `load_from_dict`, so the `RedisDsn` field type coerces and validates it — an invalid URL now raises `TypeCoercionError` fail-fast at `open()` (previously a late `ValueError` from redis-py), and an empty or whitespace-only `redis_url` raises `ValueError` at construction instead of silently disabling Redis.
- The `.env`-not-found warning suppression is narrowed to exactly that one warning — a `logging.Filter` matched on message prefix, instead of raising the whole `dotenvmodel` logger to ERROR — so real misconfiguration warnings (e.g. an invalid `DOTENV_*` value) stay visible.
- Docs corrected: `configuration.md` claimed `TASKQ_ENVIRONMENT` selects `.env.{env}` files — `ENV` does; `TASKQ_ENVIRONMENT` is a deployment label that gates the unauthenticated-admin warning.
- Rate-limit `refund()` credited the *configured* store rather than the store
  that actually paid. With `backend="redis"` and `rate_limit_pg_fallback_enabled`
  (the default), an acquire during a Redis outage falls through to Postgres and
  consumes the token there, but `TokenBucket.refund` and `SlidingWindow.refund`
  both dispatched on the primitive's static `self._backend`, so the refund went
  to Redis. One failed job therefore cost twice: Postgres, which paid, was never
  repaid — and for a fixed-quota bucket (`refill_per_second == 0`) nothing ever
  puts that token back, so the quota is permanently smaller — while Redis was
  credited a token it never spent. Silently, because by refund time the outage is
  usually over. Both primitives now dispatch on `decision.backend`, the store the
  acquire actually used. For the GCRA sliding-window style, whose
  `previous_state` shape differs per backend, the mismatch also raised a
  `KeyError` straight out of the release path. `ConcurrencyReservation` was
  checked and does not have this shape — it has no Redis path at all.
  Deployments running Redis rate limits with the Postgres fallback enabled
  should expect quota accounting to change (correct itself) after upgrading.
- `start_to_close` now actually cancels the running actor on the transactional
  path. `_run_actor_in_tx` wrapped the actor in `asyncio.shield()` *inside* the
  `wait_for` enforcing the deadline, and a shield keeps the shielded awaitable
  running when its waiter is cancelled — so the timeout applied to the wait and
  never to the actor. The attempt was marked timed out and became eligible for
  retry on another worker while the original body kept executing, running every
  side effect past the timeout point twice. **An actor that previously ran past
  its `start_to_close` deadline will now see `CancelledError` at that deadline**,
  so any cleanup it needs on interruption belongs in a `finally`. The autonomous
  path already used a bare `wait_for` and is unchanged; the outer
  `shield(_run_actor_in_tx())`, which decouples *external* cancellation from an
  in-flight commit, is untouched.
- Cancel state is cleared on every retry arm, in both backends. A cancel that
  escalated to `cancel_phase=2` in the same instant the actor raised an ordinary
  retryable exception survived the retry write: `mark_retry`, `mark_snoozed` and
  both `mark_retry_after` variants rewrote `status`/`scheduled_at` but left
  `cancel_phase` and `cancel_requested_at` on the row, and a retry reuses the
  *same* row. The next attempt was therefore dispatched already at FORCED, so the
  cancel controller's PG-observation fast-advance jumped straight to FORCED
  without ever calling `task.cancel()` — the job could never be cancelled again,
  only abandoned while its coroutine kept running. Terminal arms still keep both
  columns: they are the audit trail, and `mark_abandoned`'s `cancel_phase=2`
  guard reads them. `InMemoryBackend`'s three retry arms were copying the old
  values forward explicitly and are fixed identically, so the backends stay
  observably equivalent.


### Security

- SQL injection in `batch.py` public API (`BatchHandle.status()`, `wait_for_batch()`) — schema parameter was interpolated without validation
- Admin UI unauthenticated business-flow trigger — `POST /schedules/{id}/run` now requires `admin_actions_enabled=True` and has cooldown rate limiting
- Admin UI fail-closed defaults: `admin_ui_require_auth=True` raises `RuntimeError` in non-dev when no `auth_dependency`; `health_require_token=True` raises `RuntimeError` in non-dev when `health_token` is empty. Both have explicit opt-out env vars (`TASKQ_ADMIN_UI_REQUIRE_AUTH=false`, `TASKQ_HEALTH_REQUIRE_TOKEN=false`).
- Admin UI destructive actions (run-schedule, retry-job, cancel-job) gated behind `admin_actions_enabled` (default False). Run-schedule has per-process cooldown.
- Keyed rate-limit `key_fn` errors no longer embed the payload. The `RateLimitRegistry` "key_fn returned an empty key" `ValueError` interpolated the whole payload (`for payload {payload!r}`); that exception propagates into the persisted `error_message` and the web admin through generic exception handling, and payload values are attacker-controlled. The message now names only the ref, matching the sanitization contract `PayloadValidationError` follows in `taskq._validation`.


### Internal

- Test containers are shared singletons: one Postgres and one Dragonfly container per pytest invocation, shared across all xdist workers (filelock refcount, stale-leftover sweep) with per-module database and per-test schema isolation preserved — full suite ~152 s vs the ~226–240 s baseline.
- Docker/testcontainers calls in tests run off the event loop (`asyncio.to_thread`) — docker-py's blocking HTTP round-trips no longer stall the event loop mid-test.
- Behavioral timing tests assert in a single clock domain (one statement reads the server clock and the row together), so application/database clock divergence cannot corrupt an assertion; liveness freshness is bounded by the missed-at-most-one-tick contract.


## [0.2.2](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.1...v0.2.2) (2026-07-22)


### Continuous Integration

* local self-contained publish workflow with attestations off ([#15](https://github.com/AZX-PBC-OSS/TaskQ/issues/15)) ([07fcfce](https://github.com/AZX-PBC-OSS/TaskQ/commit/07fcfced7d4cf8de26859d24b9282ba16a0a25f8))

## [0.2.1](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.0...v0.2.1) (2026-07-22)


### Continuous Integration

* fix reusable-workflow publish — conditional attestations, manual republish dispatch ([#12](https://github.com/AZX-PBC-OSS/TaskQ/issues/12)) ([8798fdd](https://github.com/AZX-PBC-OSS/TaskQ/commit/8798fddb7b6005b15879c0121055726aa465d26d))

## [0.2.0](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.1.0...v0.2.0) (2026-07-22)


### Features

* managed-identity connections, credential hot-reload, BYO pools ([df9d7c3](https://github.com/AZX-PBC-OSS/TaskQ/commit/df9d7c35ad00f267a6cffc0460a0a0a2cd0ec922))
* managed-identity connections, credential hot-reload, BYO pools ([a754fd7](https://github.com/AZX-PBC-OSS/TaskQ/commit/a754fd730e21f939b2ad2e6e1acd0ebca78c1eb5))


### Bug Fixes

* handle ENOTSOCK in stale socket cleanup, add session backstop fixture ([dc87254](https://github.com/AZX-PBC-OSS/TaskQ/commit/dc8725424fe1c788234d51e9d73dc38b0b92facf))
* log traceback on generic job exceptions ([63d18ca](https://github.com/AZX-PBC-OSS/TaskQ/commit/63d18caafffbcfc9b7fd390cde16a8b2f083b701))
* PR review correctness fixes, reload hardening, isolated test infra ([5cb6483](https://github.com/AZX-PBC-OSS/TaskQ/commit/5cb64837a28a514c4f2c13ee520af6aa2c5681c8))
* stop swallowing exceptions in worker exception handlers ([4ff0065](https://github.com/AZX-PBC-OSS/TaskQ/commit/4ff0065f242a45a34ee27f9325640114124c0540))
* stop swallowing exceptions in worker exception handlers ([fc7786b](https://github.com/AZX-PBC-OSS/TaskQ/commit/fc7786b3ba6565f6cb4c17879dbf05f71689120d))
* stringify job ids ([8dbf369](https://github.com/AZX-PBC-OSS/TaskQ/commit/8dbf369353fd649997dcf65013b927cd9b263396))


### Documentation

* improve examples, add real-world actors, deployment/troubleshooting/tutorial guides ([1d02b34](https://github.com/AZX-PBC-OSS/TaskQ/commit/1d02b34743014a1edf5574f961853a766761e637))


### Continuous Integration

* add release-please for automated release PRs, tags, and PyPI publish ([#10](https://github.com/AZX-PBC-OSS/TaskQ/issues/10)) ([ab86d7d](https://github.com/AZX-PBC-OSS/TaskQ/commit/ab86d7d371faf550aad8fdceb5f95b9d5da37b48))
* only deploy docs on push to main, not on PRs ([afaffc7](https://github.com/AZX-PBC-OSS/TaskQ/commit/afaffc79c7690db6c2947f61a0e71cf7778bc3d9))

## 0.1.0 - 2026-07-08

### Added

- **Core Job System**
  - `@actor` decorator with typed `ActorRef` references
  - `TaskQ` facade for enqueueing and managing jobs
  - `JobsClient` for job queries, cancellation, and inspection
  - `JobHandle` for awaiting individual job results
  - Batch enqueue with `wait_for_batch` and `BatchHandle`

- **Worker System**
  - Multi-queue worker with configurable concurrency
  - Leader election for singleton job dispatch
  - Graceful shutdown with drain semantics
  - Heartbeat-based lease management
  - Workgroup orchestration for multi-replica deployments

- **Rate Limiting**
  - Sliding window (GCRA) algorithm
  - Token bucket algorithm
  - Composable rate limit groups
  - PostgreSQL and Redis backends

- **Scheduling**
  - Cron-based recurring schedules via `cron()`
  - Delayed job execution

- **Reliability**
  - Configurable retry policies with exponential backoff
  - Job cancellation with phase tracking
  - Idempotency keys and identity-based deduplication
  - Max pending and backpressure controls

- **Observability**
  - Vendor-neutral OpenTelemetry integration
  - Structured logging via structlog
  - Prometheus metrics exporter (optional extra)

- **Admin UI**
  - FastAPI-based web dashboard with htmx
  - Real-time SSE updates
  - Job inspection, queue management, worker monitoring

- **Progress Tracking**
  - Progress event streaming
  - Optional Redis fanout for real-time updates

- **Dependency Injection**
  - Scoped DI container with provider registry
  - Singleton and request scopes

- **Developer Experience**
  - `taskq` CLI (Typer) for migrations, health checks, admin UI, and workgroup management
  - Forward-only SQL migration runner
  - `taskq.testing` module with in-memory backend, fixtures, and assertions
  - Full type safety with py.typed marker

### Changed

- N/A (initial release)

### Security

- No known security issues
