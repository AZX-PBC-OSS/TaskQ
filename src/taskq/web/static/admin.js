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
                eventSource: null,
                pollTimer: null,
                // The page the operator is on, as the cursor of the last
                // pagination click; null on the unpaged first page.
                cursor: null,

                init: function () {
                    this.trackPagination();
                    if (this.tab !== "live") return;
                    // Live refresh is the poll plus the SSE accelerator; paused
                    // is neither, so the table stays exactly as the operator
                    // left it until they resume or act on it themselves.
                    if (this.liveOn) this.startLive();
                },

                startLive: function () {
                    // Polling is the source of truth: the events channel carries
                    // only the cancel fast-path (terminal writes and dispatch
                    // never NOTIFY it), so SSE can only bring a refresh forward,
                    // never replace the poll.
                    this.startPolling();
                    this.connectSSE();
                },

                stopLive: function () {
                    this.disconnectSSE();
                    this.stopPolling();
                },

                trackPagination: function () {
                    // htmx drives every request that can move the operator's
                    // page: a pagination link's path carries the cursor it
                    // follows, while a form submit (filters, tab switch, live
                    // toggle) never carries one and re-pages from the start.
                    // Syncing the cursor from each request path keeps the poll
                    // and the SSE refresh forward pointing at the page the
                    // operator is on. Guarded: htmx is absent on pages (and
                    // test harnesses) that never paginate.
                    if (!window.htmx) return;
                    var self = this;
                    this._onHtmxRequest = function (evt) {
                        var cfg = evt.detail && evt.detail.requestConfig;
                        var path = (evt.detail && evt.detail.path) || (cfg && cfg.path) || "";
                        var qs = new URLSearchParams(path.split("?")[1] || "");
                        var at = qs.get("cursor_at") || "";
                        var id = qs.get("cursor_id") || "";
                        if (at && id) {
                            self.cursor = { at: at, id: id, dir: qs.get("cursor_dir") || "next" };
                        } else {
                            self.cursor = null;
                        }
                    };
                    document.body.addEventListener("htmx:beforeRequest", this._onHtmxRequest);
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
                        // Resuming reloads the table: whatever changed while
                        // it was frozen is fetched now rather than on the
                        // next poll tick.
                        this.startLive();
                        var form = document.getElementById("job-filters");
                        if (form) form.requestSubmit();
                    } else {
                        this.stopLive();
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
                    // An error is left to EventSource itself, which reconnects
                    // with the server's retry interval: closing it here made a
                    // dropped connection (a proxy idle timeout, a server
                    // restart) permanent, with polling carrying the page alone
                    // for the rest of the visit. Polling stays the source of
                    // truth throughout, so the page never depends on the
                    // reconnect succeeding.
                    es.addEventListener("error", function () {});
                },

                disconnectSSE: function () {
                    if (this.eventSource) { this.eventSource.close(); this.eventSource = null; }
                },

                startPolling: function () {
                    if (this.pollTimer) return;
                    var self = this;
                    this.pollTimer = setInterval(function () { self.refreshTable(); }, this.pollIntervalMs);
                },

                stopPolling: function () {
                    if (this.pollTimer) { clearInterval(this.pollTimer); this.pollTimer = null; }
                },

                handleStateChange: function (evt) {
                    // An event in flight when the operator paused must not
                    // touch the frozen table.
                    if (!this.liveOn) return;
                    var jobId = evt.job_id;
                    var row = document.querySelector('tr[data-job-id="' + jobId + '"]');
                    if (row && evt.status) {
                        row.setAttribute("data-status", evt.status);
                        var badge = row.querySelector('[data-status-badge]');
                        if (badge) {
                            badge.textContent = evt.status;
                            badge.className = BADGE_CLASSES[evt.status] || "";
                        }
                        return;
                    }
                    // Anything the client cannot apply locally - a payload with
                    // no status (the cancel NOTIFY names the job only), or a
                    // transition for a job the table does not show (its listing
                    // membership just changed) - is answered by fetching the
                    // server's view, which is the only source of truth. On a
                    // cursor page that refresh is left to the poll, which
                    // refetches the operator's page in place: a cursor-less
                    // refresh from here would swap page one under the reader.
                    if (!this.cursor) this.refreshTable();
                },

                refreshTable: function () {
                    var self = this;
                    var container = document.getElementById("job-table-container");
                    if (!container) return;
                    var form = document.getElementById("job-filters");
                    if (!form) return;
                    var fd = new FormData(form);
                    var params = new URLSearchParams(fd);
                    params.set("tab", this.tab);
                    // The poll refetches the page the operator is on: the
                    // cursor synced from the last pagination click rides
                    // along, so live mode never yanks a reader back to page
                    // one. A submit without a cursor (filters, tab switch,
                    // live toggle) cleared it, which is the explicit re-page.
                    var cursor = this.cursor;
                    if (cursor) {
                        params.set("cursor_at", cursor.at);
                        params.set("cursor_id", cursor.id);
                        params.set("cursor_dir", cursor.dir);
                    }
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
                    this.stopLive();
                    if (this._onHtmxRequest) {
                        document.body.removeEventListener("htmx:beforeRequest", this._onHtmxRequest);
                        this._onHtmxRequest = null;
                    }
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
