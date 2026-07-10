/**
 * TaskQ Admin — Alpine.js components.
 * Uses Alpine.data() registrations; config passed via window.__taskqJobConfig.
 */
(function () {
    "use strict";

    var ACTIVE_STATUSES = ["pending", "scheduled", "running"];
    var TERMINAL_STATUSES = ["succeeded", "failed", "cancelled", "crashed", "abandoned"];
    var ALL_STATUSES = ACTIVE_STATUSES.concat(TERMINAL_STATUSES);

    var STATUS_COLORS = {
        pending: "text-gray-600 dark:text-gray-400",
        scheduled: "text-purple-600 dark:text-purple-400",
        running: "text-yellow-600 dark:text-yellow-400",
        succeeded: "text-green-600 dark:text-green-400",
        failed: "text-red-600 dark:text-red-400",
        cancelled: "text-orange-600 dark:text-orange-400",
        crashed: "text-red-600 dark:text-red-400",
        abandoned: "text-gray-500 dark:text-gray-500",
    };

    var CHIP_COLORS = {
        pending: "bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300",
        scheduled: "bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300",
        running: "bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300",
        succeeded: "bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300",
        failed: "bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300",
        cancelled: "bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300",
        crashed: "bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300",
        abandoned: "bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400",
    };

    var BADGE_CLASSES = {
        pending: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300",
        scheduled: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-700 dark:bg-purple-900 dark:text-purple-300",
        running: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300",
        succeeded: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300",
        failed: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300",
        cancelled: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300",
        crashed: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300",
        abandoned: "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-500 dark:bg-gray-800 dark:text-gray-400",
    };

    document.addEventListener("alpine:init", function () {

        // ── jobsPage component ──────────────────────────────────────────
        Alpine.data("jobsPage", function () {
            var cfg = window.__taskqJobConfig || {};
            return {
                tab: cfg.tab || "live",
                basePath: cfg.basePath || "",
                realtimeMode: cfg.realtimeMode || "polling",
                pollIntervalMs: cfg.pollIntervalMs || 5000,
                actor: cfg.actor || "",
                queue: cfg.queue || "",
                timeRange: cfg.timeRange || "",
                timeFrom: cfg.timeFrom || "",
                timeTo: cfg.timeTo || "",
                identityKey: cfg.identityKey || "",
                fairnessKey: cfg.fairnessKey || "",
                search: cfg.search || "",
                liveOn: cfg.liveOn !== false,
                selectedStatuses: cfg.selectedStatuses || [],
                allStatuses: cfg.allStatuses || [],
                totalRows: cfg.totalRows || 0,
                pendingCount: 0,
                eventSource: null,
                pollTimer: null,

                init: function () {
                    if (this.tab === "live" && this.liveOn) {
                        this.connectSSE();
                    } else if (this.tab === "live") {
                        this.startPolling();
                    }
                },

                switchTab: function (t) {
                    if (this.tab === t) return;
                    this.tab = t;
                    var form = document.getElementById("job-filters");
                    if (form) form.requestSubmit();
                },

                toggleLive: function () {
                    this.liveOn = !this.liveOn;
                    if (this.liveOn) {
                        this.pendingCount = 0;
                        this.connectSSE();
                        var form = document.getElementById("job-filters");
                        if (form) form.requestSubmit();
                    } else {
                        this.disconnectSSE();
                        this.startPolling();
                    }
                },

                showPending: function () {
                    this.pendingCount = 0;
                    var form = document.getElementById("job-filters");
                    if (form) {
                        var ca = form.querySelector('input[name="cursor_at"]');
                        var ci = form.querySelector('input[name="cursor_id"]');
                        if (ca) ca.value = "";
                        if (ci) ci.value = "";
                        form.requestSubmit();
                    }
                },

                connectSSE: function () {
                    if (this.eventSource) return;
                    var self = this;
                    var es = new EventSource(this.basePath + "/sse/jobs");
                    this.eventSource = es;
                    es.addEventListener("state_change", function (evt) {
                        try { self.handleStateChange(JSON.parse(evt.data)); } catch (e) {}
                    });
                    es.addEventListener("error", function () {
                        es.close();
                        self.eventSource = null;
                        self.startPolling();
                    });
                    if (this.pollTimer) {
                        clearInterval(this.pollTimer);
                        this.pollTimer = null;
                    }
                },

                disconnectSSE: function () {
                    if (this.eventSource) { this.eventSource.close(); this.eventSource = null; }
                },

                startPolling: function () {
                    if (this.pollTimer || this.eventSource) return;
                    var self = this;
                    this.pollTimer = setInterval(function () { self.refreshTable(); }, this.pollIntervalMs);
                },

                handleStateChange: function (evt) {
                    if (!this.liveOn) { this.pendingCount++; return; }
                    var jobId = evt.job_id;
                    var row = document.querySelector('tr[data-job-id="' + jobId + '"]');
                    if (row && evt.status) {
                        row.setAttribute("data-status", evt.status);
                        var badge = row.querySelector('[data-status-badge]');
                        if (badge) {
                            badge.textContent = evt.status;
                            badge.className = BADGE_CLASSES[evt.status] || "";
                        }
                    } else if (evt.status && TERMINAL_STATUSES.indexOf(evt.status) === -1) {
                        this.pendingCount++;
                        this.refreshTable();
                    }
                },

                refreshTable: function () {
                    var self = this;
                    var container = document.getElementById("job-table-container");
                    if (!container) return;
                    var form = document.getElementById("job-filters");
                    if (!form) return;
                    var fd = new FormData(form);
                    var params = new URLSearchParams(fd);
                    params.delete("cursor_at");
                    params.delete("cursor_id");
                    params.set("tab", this.tab);
                    fetch(this.basePath + "/jobs?" + params.toString(), { headers: { "HX-Request": "true" } })
                        .then(function (r) { return r.text(); })
                        .then(function (html) {
                            var tmp = document.createElement("div");
                            tmp.innerHTML = html;
                            var el = tmp.querySelector("#job-table-container");
                            if (el) { container.outerHTML = el.outerHTML; }
                            if (window.lucide) lucide.createIcons();
                        })
                        .catch(function () {});
                },

                destroy: function () {
                    this.disconnectSSE();
                    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
                }
            };
        });

        // ── statusCombobox component ────────────────────────────────────
        Alpine.data("statusCombobox", function () {
            var cfg = window.__taskqJobConfig || {};
            return {
                open: false,
                selected: (cfg.selectedStatuses || []).slice(),
                allStatuses: ALL_STATUSES,

                displayText: function () {
                    if (this.selected.length === 0) return "All statuses";
                    if (this.selected.length >= ALL_STATUSES.length) return "All statuses";
                    if (this.selected.length <= 2) return this.selected.join(", ");
                    return this.selected.length + " selected";
                },

                toggle: function (s) {
                    var idx = this.selected.indexOf(s);
                    if (idx >= 0) this.selected.splice(idx, 1);
                    else this.selected.push(s);
                },

                selectAll: function () { this.selected = ALL_STATUSES.slice(); },
                selectActive: function () { this.selected = ACTIVE_STATUSES.slice(); },
                selectTerminal: function () { this.selected = TERMINAL_STATUSES.slice(); },
                selectNone: function () { this.selected = []; },

                statusColor: function (s) { return STATUS_COLORS[s] || ""; },
                chipColor: function (s) { return CHIP_COLORS[s] || ""; },
            };
        });
    });
})();
