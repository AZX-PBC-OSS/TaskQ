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
    let lastSeenSeq = 0;
    let accumulatedProgress = {};
    let lastRenderedProgress = null;

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
    // ---------------------------------------------------------------------------

    const PROGRESS_FIELDS = ["step", "percent", "detail", "data", "ts"];
    const PROGRESS_FINGERPRINT_FIELDS = ["step", "percent", "detail", "data"];

    function progressState(evt) {
        const state = {};
        for (const field of PROGRESS_FIELDS) {
            if (Object.prototype.hasOwnProperty.call(evt, field) && evt[field] != null) {
                state[field] = evt[field];
            }
        }
        return state;
    }

    function progressFingerprint(state) {
        const progress = Object.fromEntries(
            PROGRESS_FINGERPRINT_FIELDS
                .filter((field) => Object.prototype.hasOwnProperty.call(state, field))
                .map((field) => [field, state[field]]),
        );
        if (Object.keys(progress).length === 0) return null;

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

        return JSON.stringify(canonicalize(progress));
    }

    function acceptProgress(seq, rawState, merge) {
        if (!Number.isInteger(seq) || seq <= lastSeenSeq) return;
        lastSeenSeq = seq;

        const state = progressState(rawState);
        accumulatedProgress = merge
            ? { ...accumulatedProgress, ...state }
            : state;
        const fingerprint = progressFingerprint(accumulatedProgress);
        const actorProgress = rawState.kind === "progress";
        if (fingerprint === null || (fingerprint === lastRenderedProgress && !actorProgress)) return;

        lastRenderedProgress = fingerprint;
        renderProgressEvent(state);
    }

    function acceptTerminal(rawState, merge) {
        // A terminal delivery is exempt from the seq discard, the client
        // mirror of the stream's own terminal rule. The cursor can
        // legitimately sit ABOVE the terminal's seq: publishes ride Redis
        // out as progress calls land while the coalesced flush lands the
        // seq up to half a second later, so a crash in between leaves the
        // durable row (and the redispatched attempt's re-seeded seqs)
        // behind what this page already consumed. Skipping the terminal on
        // that cursor discards the one frame that ends the job - the job
        // is durably over, no later seq ever recovers the page, and both
        // drivers then stop on the very delivery they dropped. Render it
        // whatever the cursor says, still fingerprint-deduped so a
        // redundant re-delivery of already-shown state adds no entry -
        // unless it is empty, which is the empty-snapshot skip this module
        // exists for.
        const state = progressState(rawState);
        // The empty-snapshot skip comes first: a lifecycle-only terminal
        // carries no progress content and must not become a synthetic
        // entry, even merged into state that has some.
        if (progressFingerprint(state) === null) return;
        accumulatedProgress = merge
            ? { ...accumulatedProgress, ...state }
            : state;
        // Deduped on the accumulated state the timeline actually reflects,
        // so a terminal replaying already-shown content adds no entry.
        const fingerprint = progressFingerprint(accumulatedProgress);
        if (fingerprint === lastRenderedProgress) return;
        lastRenderedProgress = fingerprint;
        renderProgressEvent(state);
    }

    function renderProgressEvent(evt) {
        const timeline = document.getElementById("progress-timeline");
        if (!timeline) return;

        const entry = document.createElement("div");
        entry.className = "progress-event";

        const percent = typeof evt.percent === "number" ? evt.percent : 0;

        const barWrap = document.createElement("div");
        barWrap.className = "progress-bar-wrap";

        const bar = document.createElement("div");
        bar.className = "progress-bar";
        bar.style.width = `${Math.min(100, Math.max(0, percent))}%`;
        barWrap.appendChild(bar);

        const detail = document.createElement("div");
        detail.className = "progress-detail";
        detail.textContent = evt.detail ?? evt.step ?? "";

        const meta = document.createElement("div");
        meta.className = "progress-meta";
        const stepText = evt.step ? `${evt.step}` : "";
        const tsText = evt.ts ? new Date(evt.ts).toLocaleTimeString() : "";
        const percentText = `${percent}%`;
        meta.textContent = [percentText, stepText, tsText].filter(Boolean).join(" · ");

        entry.appendChild(barWrap);
        entry.appendChild(detail);
        entry.appendChild(meta);

        if (evt.data != null) {
            const details = document.createElement("details");
            const summary = document.createElement("summary");
            summary.className = "progress-meta";
            summary.textContent = "data";
            const pre = document.createElement("pre");
            pre.className = "progress-meta";
            pre.style.whiteSpace = "pre-wrap";
            pre.style.wordBreak = "break-all";
            pre.textContent =
                typeof evt.data === "string"
                    ? evt.data
                    : JSON.stringify(evt.data, null, 2);
            details.appendChild(summary);
            details.appendChild(pre);
            entry.appendChild(details);
        }

        timeline.appendChild(entry);
        entry.scrollIntoView({ behavior: "smooth", block: "nearest" });
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
            fetch(`${BASE}/jobs/api/job/${jobId}/state`)
                .then(function (res) { return res.json(); })
                .then(function (body) {
                    if (!pollingActive) return;
                    acceptProgress(body.progress_seq, body.progress_state ?? {}, false);
                    if (TERMINAL_STATUSES.has(body.status)) {
                        // The durable terminal row is the delivery: accept it
                        // exempt from the cursor, the same rule the terminal
                        // SSE envelope follows. A cursor inflated by wire
                        // seqs whose flush a crash ate must not discard the
                        // row that ends the job.
                        acceptTerminal(body.progress_state ?? {}, false);
                        stopPolling();
                        if (eventSource) {
                            eventSource.close();
                            eventSource = null;
                        }
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

        function handleProgressMessage(rawEvent, render, terminalEvent) {
            let evt;
            try {
                evt = JSON.parse(rawEvent.data);
            } catch {
                return;
            }
            const seq = Number(rawEvent.lastEventId);
            if (terminalEvent) {
                // The terminal envelope is exempt from the seq cursor (and
                // from the fingerprint's actor carve-out): the stream's own
                // close signal must land even when a lost flush left its seq
                // below what this page already consumed, and even when a
                // reconnect snapshot replays state the page has seen.
                acceptTerminal(evt, evt.kind != null);
            } else if (render) {
                // Redis envelopes are call-level deltas; initial PG snapshots
                // have no kind and replace the accumulated client state.
                acceptProgress(seq, evt, evt.kind != null);
            } else if (Number.isInteger(seq) && seq > lastSeenSeq) {
                lastSeenSeq = seq;
            }
            stopPolling();
            if (terminalEvent || evt.terminal) {
                es.close();
                eventSource = null;
            }
        }

        es.addEventListener("progress", function (event) {
            handleProgressMessage(event, true, false);
        });
        es.addEventListener("terminal", function (event) {
            handleProgressMessage(event, true, true);
        });

        es.addEventListener("done", function () {
            es.close();
            eventSource = null;
            stopPolling();
        });

        es.addEventListener("open", function () {
            stopPolling();
        });

        es.addEventListener("error", function () {
            // EventSource reconnects with Last-Event-ID. The periodic Redis
            // health probe still owns the mode badge, while polling provides
            // durable progress if the stream endpoint itself stays broken.
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
        const initialSeq = Number(section.getAttribute("data-progress-seq"));
        if (Number.isInteger(initialSeq) && initialSeq >= 0) {
            lastSeenSeq = initialSeq;
        }
        try {
            const initialState = JSON.parse(section.getAttribute("data-progress-state") ?? "{}");
            accumulatedProgress = progressState(initialState);
            lastRenderedProgress = progressFingerprint(accumulatedProgress);
        } catch {
            accumulatedProgress = {};
            lastRenderedProgress = null;
        }

        if (mode === "realtime" && jobId) {
            openEventSource(jobId);
        } else {
            startPolling();
        }

        setInterval(checkRedisHealth, 30000);
    });
})();
