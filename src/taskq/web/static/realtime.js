(function () {
    "use strict";

    const BASE = window.TASKQ_BASE_PATH || "";

    const MODE_LABELS = {
        realtime: "real-time mode",
        polling: "polling mode",
        "polling-degraded": "polling mode (Redis unavailable)",
    };

    let eventSource = null;
    let pollingActive = false;
    let pollingInterval = null;
    // The progress render state: the last sequence either feed accepted,
    // the fingerprint of the progress the timeline currently renders
    // (seeded from the server-rendered snapshot at boot), and the entry
    // node the render patches in place. The sequence cursor is the EXACT
    // decimal string, not a Number: JS numbers are IEEE doubles, exact
    // only up to 2^53, and a cursor that lost digits lets a seq the page
    // never rendered compare equal to the cursor and be dropped as a
    // duplicate (a frozen timeline, at a scale the progress_seq column's
    // bigint domain admits). Every comparison goes through BigInt; the
    // wire sources (the SSE Last-Event-ID, the poll state endpoint's
    // ETag header) already carry the exact decimal digits as strings.
    let lastSeenSeq = "0";
    let haveSeenSeq = false;
    let lastRenderedFingerprint = null;
    let renderedEntry = null;
    // True while a poll fetch is outstanding. A background tab that the
    // browser throttles (or a server that answers slowly) must never
    // stack overlapping conditional GETs: the tick that finds one in
    // flight skips itself, and the next interval fire - a suspended
    // tab's overdue fires are coalesced to one by the browser - catches
    // up with a single request.
    let pollInFlight = false;

    function getBadgeEl() {
        return document.querySelector(".taskq-badge");
    }

    function getProgressSection() {
        return document.getElementById("progress-section");
    }

    function currentMode() {
        const badge = getBadgeEl();
        return badge ? badge.getAttribute("data-mode") : null;
    }

    function setModeBadge(mode) {
        const badge = getBadgeEl();
        if (!badge) return;
        badge.setAttribute("data-mode", mode);
        badge.textContent = MODE_LABELS[mode] ?? mode;
    }

    // ---------------------------------------------------------------------------
    // Progress timeline rendering
    //
    // The render is update-in-place: the entry's nodes are built once and
    // every later tick patches only the nodes whose value changed, so a
    // repeated identical poll performs ZERO DOM writes. Rebuilding the
    // entry (or appending a fresh one) per tick is what made a
    // polling-degraded page flicker: each poll repainted the whole
    // section.
    // ---------------------------------------------------------------------------

    // The fields a progress snapshot carries. ``ts`` is rendered on the
    // meta line but excluded from the fingerprint below: the worker
    // re-flushes unchanged snapshots with a fresh timestamp, so
    // fingerprinting ts would re-render identical progress forever (the
    // polling flicker this machinery exists to stop).
    const PROGRESS_FIELDS = ["step", "percent", "detail", "data", "ts"];
    const FINGERPRINT_FIELDS = ["step", "percent", "detail", "data"];

    function progressState(evt) {
        const state = {};
        for (const field of PROGRESS_FIELDS) {
            if (Object.prototype.hasOwnProperty.call(evt, field) && evt[field] != null) {
                state[field] = evt[field];
            }
        }
        return state;
    }

    // Canonical JSON of the identity fields: key-order independent, ts
    // excluded. Equal fingerprints mean the timeline already shows this
    // progress and the tick must write nothing.
    //
    // The recursion is depth-capped: a state nested past the cap collapses
    // to a stable sentinel, so neither the canonicalize nor the
    // JSON.stringify below it can ever overflow the JS stack (RangeError)
    // on a poisoned state - the fingerprint machine must be the one part
    // of the page that cannot crash on its input. Past the cap, states
    // compare equal whatever their deeper content: dedup stays sound (a
    // repeated tick still writes nothing), and a changed deep field is
    // rendered by the next event that also changes anything above the
    // cap. The cap is far above what the server accepts: the worker's
    // own encoder refuses progress data nested past its recursion limit,
    // so a depth this side of it only ever fires on a foreign writer.
    const FINGERPRINT_MAX_DEPTH = 64;

    function progressFingerprint(state) {
        const identity = {};
        for (const field of FINGERPRINT_FIELDS) {
            if (Object.prototype.hasOwnProperty.call(state, field)) {
                identity[field] = state[field];
            }
        }
        if (Object.keys(identity).length === 0) return null;

        function canonicalize(value, depth) {
            if (depth > FINGERPRINT_MAX_DEPTH) return "…";
            if (Array.isArray(value)) return value.map((v) => canonicalize(v, depth + 1));
            if (value !== null && typeof value === "object") {
                return Object.fromEntries(
                    Object.keys(value)
                        .sort()
                        .map((key) => [key, canonicalize(value[key], depth + 1)]),
                );
            }
            return value;
        }

        return JSON.stringify(canonicalize(identity, 0));
    }

    // The single gate both feeds render through (the SSE stream and the
    // poll tick): a sequence that does not advance the cursor, an empty
    // state, and a state whose fingerprint the timeline already renders
    // are dropped BEFORE any DOM work, so a repeated identical poll
    // performs zero DOM writes.
    // The exact decimal-sequence gate: the cursor advances only on a
    // strictly greater sequence, compared as BigInt over the exact
    // digits (see the cursor declaration at the top). Non-decimal and
    // zero cursors advance nothing, the same drop a malformed frame got
    // under the Number-based gate.
    function parseSeq(raw) {
        let text = typeof raw === "string" ? raw : String(raw ?? "");
        if (/^".*"$/.test(text)) text = text.slice(1, -1);
        if (!/^\d+$/.test(text)) return null;
        const normalized = text.replace(/^0+(?=\d)/, "");
        return normalized === "0" ? null : normalized;
    }

    function acceptProgress(seqRaw, rawState) {
        const seq = parseSeq(seqRaw);
        if (seq === null) return;
        if (haveSeenSeq && BigInt(seq) <= BigInt(lastSeenSeq)) return;
        lastSeenSeq = seq;
        haveSeenSeq = true;

        // Fail open on a fingerprint computation that itself fails: a
        // tick whose state cannot be fingerprinted must RENDER (the
        // timeline shows the latest state) rather than freeze the page
        // behind a dropped exception. With the depth-capped canonicalize
        // above this is unreachable for JSON-parsed state; it exists so
        // the gate's own defect can never again take the timeline down.
        let fingerprint = null;
        try {
            fingerprint = progressFingerprint(progressState(rawState));
        } catch {
            fingerprint = null;
        }
        if (fingerprint === null || fingerprint === lastRenderedFingerprint) return;
        lastRenderedFingerprint = fingerprint;
        renderProgressEvent(progressState(rawState));
    }

    function progressMetaText(state) {
        const percent = typeof state.percent === "number" ? state.percent : 0;
        const stepText = state.step ? `${state.step}` : "";
        const tsText = state.ts ? new Date(state.ts).toLocaleTimeString() : "";
        const percentText = `${percent}%`;
        return [percentText, stepText, tsText].filter(Boolean).join(" · ");
    }

    function buildProgressEntry() {
        const entry = document.createElement("div");
        entry.className = "progress-event";

        const barWrap = document.createElement("div");
        barWrap.className = "progress-bar-wrap";

        const bar = document.createElement("div");
        bar.className = "progress-bar";
        barWrap.appendChild(bar);

        const detail = document.createElement("div");
        detail.className = "progress-detail";

        const meta = document.createElement("div");
        meta.className = "progress-meta";

        entry.appendChild(barWrap);
        entry.appendChild(detail);
        entry.appendChild(meta);

        return { entry, bar, detail, meta, dataWrap: null, dataPre: null };
    }

    // The data <pre>'s text: pretty-printed JSON, bounded by the same
    // depth cap the fingerprint uses - stringify on a poisoned deep state
    // would RangeError exactly where the old canonicalize could, so the
    // render degrades to a notice instead of throwing out of the tick.
    function stringifyDataBounded(data) {
        try {
            return JSON.stringify(data, null, 2);
        } catch {
            return "[data too deeply nested to render]";
        }
    }

    function renderProgressEvent(state) {
        const timeline = document.getElementById("progress-timeline");
        if (!timeline) return;

        if (!renderedEntry) {
            // First render: build the entry's nodes once and append it.
            // Nothing is rebuilt afterwards; later ticks patch the nodes.
            // No scrollIntoView, here or on any patch: the driver never
            // steals the viewport (a poll tick landing while the operator
            // reads the page must not yank the scroll position).
            renderedEntry = buildProgressEntry();
            timeline.appendChild(renderedEntry.entry);
        }

        // Update-in-place: assign a node only when its value changed, so a
        // changed field patches exactly its own node and an unchanged
        // field is never written at all.
        const percent = typeof state.percent === "number" ? state.percent : 0;
        const width = `${Math.min(100, Math.max(0, percent))}%`;
        if (renderedEntry.bar.style.width !== width) {
            renderedEntry.bar.style.width = width;
        }

        const detailText = state.detail ?? state.step ?? "";
        if (renderedEntry.detail.textContent !== detailText) {
            renderedEntry.detail.textContent = detailText;
        }

        const metaText = progressMetaText(state);
        if (renderedEntry.meta.textContent !== metaText) {
            renderedEntry.meta.textContent = metaText;
        }

        const dataText = state.data == null
            ? null
            : typeof state.data === "string"
                ? state.data
                : stringifyDataBounded(state.data);
        if (dataText !== null && !renderedEntry.dataWrap) {
            const details = document.createElement("details");
            const summary = document.createElement("summary");
            summary.className = "progress-meta";
            summary.textContent = "data";
            const pre = document.createElement("pre");
            pre.className = "progress-meta";
            pre.style.whiteSpace = "pre-wrap";
            pre.style.wordBreak = "break-all";
            details.appendChild(summary);
            details.appendChild(pre);
            renderedEntry.entry.appendChild(details);
            renderedEntry.dataWrap = details;
            renderedEntry.dataPre = pre;
        }
        if (renderedEntry.dataWrap && renderedEntry.dataPre) {
            if (dataText === null) {
                renderedEntry.dataWrap.remove();
                renderedEntry.dataWrap = null;
                renderedEntry.dataPre = null;
            } else if (renderedEntry.dataPre.textContent !== dataText) {
                renderedEntry.dataPre.textContent = dataText;
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Polling helpers (fetch-based; no HTMX involvement)
    // ---------------------------------------------------------------------------

    const TERMINAL_STATUSES = new Set([
        "succeeded", "failed", "cancelled", "crashed", "abandoned",
    ]);

    function startPolling() {
        if (pollingActive) return;
        const section = getProgressSection();
        if (!section) return;

        const jobId = section.getAttribute("data-job-id");
        if (!jobId) return;

        pollingInterval = setInterval(function () {
            if (!pollingActive) return;
            if (pollInFlight) return;
            pollInFlight = true;
            // Conditional GET: the request carries the progress sequence
            // the page already rendered, so a tick whose data is
            // unchanged is answered 304 and downloads no state at all.
            // The fingerprint gate inside acceptProgress is the second
            // half of the same contract server-side: an identical
            // snapshot (a re-flushed state with a bumped seq and a fresh
            // ts) renders nothing; a changed snapshot patches the
            // rendered entry in place.
            fetch(
                `${BASE}/jobs/api/job/${jobId}/state`,
                haveSeenSeq
                    ? { headers: { "If-None-Match": `"${lastSeenSeq}"` } }
                    : {},
            )
                .then(function (res) {
                    pollInFlight = false;
                    if (res.status === 304) return null;
                    return res.json().then(function (body) {
                        // The sequence cursor's exact source: the ETag
                        // header carries the same decimal digits the
                        // server compares, quoted, at any magnitude.
                        // body.progress_seq is a JS number (a double):
                        // above 2^53 it loses digits, so it is the
                        // FALLBACK only, for a response with no ETag -
                        // precision-limited there, documented at the
                        // cursor declaration.
                        const etag = res.headers && typeof res.headers.get === "function"
                            ? res.headers.get("ETag")
                            : null;
                        return { body: body, seqRaw: etag ?? body.progress_seq };
                    });
                })
                .then(function (tick) {
                    pollInFlight = false;
                    if (!pollingActive || tick === null) return;
                    acceptProgress(tick.seqRaw, tick.body.progress_state ?? {});
                    if (TERMINAL_STATUSES.has(tick.body.status)) {
                        stopPolling();
                    }
                })
                .catch(function () {
                    pollInFlight = false;
                });
        }, POLL_INTERVAL_MS);

        pollingActive = true;
        // A restart (the stream dropped) must not inherit a wedged guard
        // from a request the browser abandoned while the tab was
        // suspended: the first catch-up poll always fires, and the seq
        // gate absorbs any overlap with a genuinely stale response.
        pollInFlight = false;
    }

    function stopPolling() {
        if (pollingInterval !== null) {
            clearInterval(pollingInterval);
            pollingInterval = null;
        }
        pollingActive = false;
        pollInFlight = false;
    }

    // ---------------------------------------------------------------------------
    // EventSource (SSE) management
    // ---------------------------------------------------------------------------

    function openEventSource(jobId) {
        if (eventSource) {
            eventSource.close();
            eventSource = null;
        }

        const es = new EventSource(`${BASE}/jobs/api/job/${jobId}/progress/stream`);
        eventSource = es;

        function handleProgressMessage(rawEvent) {
            let evt;
            try {
                evt = JSON.parse(rawEvent.data);
            } catch {
                return;
            }
            // Same gate the poll uses: a reconnect replay with an
            // already-rendered fingerprint writes nothing. The
            // Last-Event-ID string is the exact cursor source (no Number
            // precision loss).
            acceptProgress(rawEvent.lastEventId, evt);
            if (evt.terminal) {
                es.close();
                eventSource = null;
                stopPolling();
            }
        }

        es.addEventListener("progress", handleProgressMessage);
        es.addEventListener("terminal", handleProgressMessage);

        es.addEventListener("done", function () {
            es.close();
            eventSource = null;
            stopPolling();
        });

        es.addEventListener("open", function () {
            // The stream is live again after a reconnect: the poll that
            // bridged the gap stands down.
            stopPolling();
        });

        es.addEventListener("error", function () {
            // Polling is a first-class mode, and the badge stays calm: a
            // dropped stream is not an outage announcement, so the badge
            // does not flip here. EventSource reconnects with
            // Last-Event-ID on its own; polling keeps the data fresh in
            // the meantime; the periodic probe (/sse/mode, the only
            // authority on Redis health) owns every badge transition.
            startPolling();
        });
    }

    // ---------------------------------------------------------------------------
    // Periodic Redis health check
    // ---------------------------------------------------------------------------

    function checkRedisHealth() {
        fetch(`${BASE}/sse/mode`)
            .then(function (res) {
                return res.json();
            })
            .then(function (body) {
                const wantRealtime = Boolean(body.realtime);
                const mode = currentMode();

                if (mode === "realtime" && !wantRealtime) {
                    setModeBadge("polling-degraded");
                    if (eventSource) {
                        eventSource.close();
                        eventSource = null;
                    }
                    startPolling();
                    return;
                }

                if (mode !== "realtime" && wantRealtime) {
                    setModeBadge("realtime");
                    stopPolling();
                    const section = getProgressSection();
                    const jobId = section
                        ? section.getAttribute("data-job-id")
                        : null;
                    if (jobId) {
                        openEventSource(jobId);
                    }
                }
            })
            .catch(function () {
                // Health check failure is non-fatal; remain in current mode.
            });
    }

    // ---------------------------------------------------------------------------
    // Bootstrap
    // ---------------------------------------------------------------------------

    document.addEventListener("DOMContentLoaded", function () {
        const badge = getBadgeEl();
        const section = getProgressSection();

        if (!badge || !section) return;

        const mode = badge.getAttribute("data-mode");
        const jobId = section.getAttribute("data-job-id");

        // Seed the dedup cursor from the server-rendered snapshot: the
        // template already shows this progress, so a poll that returns
        // the same state (the common idle case) must write nothing at
        // all, not even once. The attribute is the exact decimal string.
        const initialSeq = parseSeq(section.getAttribute("data-progress-seq"));
        if (initialSeq !== null) {
            lastSeenSeq = initialSeq;
            haveSeenSeq = true;
        }
        try {
            lastRenderedFingerprint = progressFingerprint(
                progressState(JSON.parse(section.getAttribute("data-progress-state") ?? "{}")),
            );
        } catch {
            lastRenderedFingerprint = null;
        }

        if (mode === "realtime" && jobId) {
            openEventSource(jobId);
        } else {
            startPolling();
        }

        setInterval(checkRedisHealth, 30000);
    });
})();
