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
    // node the render patches in place.
    let lastSeenSeq = 0;
    let lastRenderedFingerprint = null;
    let renderedEntry = null;

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
    function progressFingerprint(state) {
        const identity = {};
        for (const field of FINGERPRINT_FIELDS) {
            if (Object.prototype.hasOwnProperty.call(state, field)) {
                identity[field] = state[field];
            }
        }
        if (Object.keys(identity).length === 0) return null;

        function canonicalize(value) {
            if (Array.isArray(value)) return value.map(canonicalize);
            if (value !== null && typeof value === "object") {
                return Object.fromEntries(
                    Object.keys(value)
                        .sort()
                        .map((key) => [key, canonicalize(value[key])]),
                );
            }
            return value;
        }

        return JSON.stringify(canonicalize(identity));
    }

    // The single gate both feeds render through (the SSE stream and the
    // poll tick): a sequence that does not advance the cursor, an empty
    // state, and a state whose fingerprint the timeline already renders
    // are dropped BEFORE any DOM work, so a repeated identical poll
    // performs zero DOM writes.
    function acceptProgress(seq, rawState) {
        if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;
        lastSeenSeq = seq;

        const fingerprint = progressFingerprint(progressState(rawState));
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
                : JSON.stringify(state.data, null, 2);
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
                lastSeenSeq > 0
                    ? { headers: { "If-None-Match": `"${lastSeenSeq}"` } }
                    : {},
            )
                .then(function (res) {
                    if (res.status === 304) return null;
                    return res.json();
                })
                .then(function (body) {
                    if (!pollingActive || body === null) return;
                    acceptProgress(body.progress_seq, body.progress_state ?? {});
                    if (TERMINAL_STATUSES.has(body.status)) {
                        stopPolling();
                    }
                })
                .catch(function () {});
        }, POLL_INTERVAL_MS);

        pollingActive = true;
    }

    function stopPolling() {
        if (pollingInterval !== null) {
            clearInterval(pollingInterval);
            pollingInterval = null;
        }
        pollingActive = false;
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
            // already-rendered fingerprint writes nothing.
            acceptProgress(Number(rawEvent.lastEventId), evt);
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
        // all, not even once.
        const initialSeq = Number(section.getAttribute("data-progress-seq"));
        if (Number.isInteger(initialSeq) && initialSeq >= 0) {
            lastSeenSeq = initialSeq;
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
