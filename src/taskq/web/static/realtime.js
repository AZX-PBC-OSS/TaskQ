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

        let lastRenderedSeq = -1;

        pollingInterval = setInterval(function () {
            if (!pollingActive) return;
            fetch(`${BASE}/jobs/api/job/${jobId}/state`)
                .then(function (res) { return res.json(); })
                .then(function (body) {
                    if (!pollingActive) return;
                    if (body.progress_state && body.progress_seq > lastRenderedSeq) {
                        lastRenderedSeq = body.progress_seq;
                        renderProgressEvent(body.progress_state);
                    }
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
            renderProgressEvent(evt);
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

        es.addEventListener("error", function () {
            es.close();
            eventSource = null;
            setModeBadge("polling-degraded");
            startPolling();
        });
    }

    // ---------------------------------------------------------------------------
    // Periodic Redis health check
    // ---------------------------------------------------------------------------

    function checkRedisHealth() {
        fetch(`${BASE}/jobs/health/ready`)
            .then(function (res) {
                return res.json();
            })
            .then(function (body) {
                const mode = currentMode();
                const redisOk = Boolean(body.redis_configured);

                if (mode === "realtime" && !redisOk) {
                    setModeBadge("polling-degraded");
                    if (eventSource) {
                        eventSource.close();
                        eventSource = null;
                    }
                    startPolling();
                    return;
                }

                if (mode === "polling-degraded" && redisOk) {
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

        if (mode === "realtime" && jobId) {
            openEventSource(jobId);
        } else {
            startPolling();
        }

        setInterval(checkRedisHealth, 30000);
    });
})();
