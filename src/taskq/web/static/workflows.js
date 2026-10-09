/* The workflow-run page's live machinery (T11): render-once SVG +
 * data-node-key patches + the SSE revisioned-snapshot feed + the node
 * panel + the Resolve form. The conventions are the admin's own: no
 * bundler, no React, no CDN — mermaid is vendored beside
 * alpine/htmx/lucide for the same air-gapped-ops reason.
 *
 * The state contract (pinned by the tests):
 * - every frame is a revisioned FULL snapshot; a frame with seq <= the
 *   cursor is DROPPED (stale never overwrites fresh);
 * - the reconnect (EventSource auto-reconnect sends Last-Event-ID)
 *   resumes the cursor; the next frame carries the whole state, so a
 *   killed connection cannot strand the page;
 * - every async surface has a DEFINED loading, empty, AND error state —
 *   a blank region that reads as a healthy zero is a defect (#673).
 */
(function () {
  "use strict";

  var bootEl = document.getElementById("wf-boot");
  var graphHost = document.getElementById("wf-graph-host");
  if (!bootEl || !graphHost) return;
  var boot = JSON.parse(bootEl.textContent);

  // ── the state classes (the legend declares them; the CSS fills them) ──
  var STATUS_CLASSES = [
    "wf-st-pending", "wf-st-scheduled", "wf-st-running", "wf-st-succeeded",
    "wf-st-failed", "wf-st-cancelled", "wf-st-crashed", "wf-st-abandoned",
    "wf-st-skipped"
  ];

  // ── render once, patch forever ────────────────────────────────────────
  // mermaid's DOM ids RENUMBER on any topology change (the dragon P2
  // pinned: probe1_id_stability) — bindSvg parses the ids ONCE, stamps
  // data-node-key, and nothing ever re-renders.
  function bindSvg(svg) {
    var byKey = {};
    svg.querySelectorAll("g.node").forEach(function (g) {
      var id = g.id || "";
      // mermaid v11 ids: flowchart-<KEY>-<n> (the number shifts on
      // topology change; the KEY is the stable half).
      var m = id.match(/^flowchart-(.+?)-\d+$/);
      if (!m) return;
      var key = m[1];
      g.setAttribute("data-node-key", key);
      g.setAttribute("tabindex", "0");
      g.setAttribute("role", "button");
      g.setAttribute("aria-label", "node " + key + " — open detail");
      byKey[key] = g;
    });
    // THE BARE-<path> HEXAGON (P2's pin 4): the collapsed map node
    // renders its shape as a bare path — the state fill must target
    // path too, not only rect/polygon.
    Object.keys(byKey).forEach(function (key) {
      paintNode(key, keyState(key));
    });
    return byKey;
  }

  function keyState(key) {
    var state = boot.state || { nodes: [] };
    var nodes = state.nodes || [];
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].key === key) return nodes[i];
    }
    return null;
  }

  function paintNode(key, node) {
    // THE ESCAPED SELECTOR (the poisoned-row defense, nit 1 of 2): the
    // row's step key is ATTACKER-CONTROLLED (a poisoned row key with a
    // quote or backslash in it made this querySelector THROW — a
    // SyntaxError inside applySnapshot's patch loop that blinded every
    // later frame of the run's live updates). CSS.escape makes any key
    // a legal attribute-selector value; no key can throw here.
    var g = graphHost.querySelector('[data-node-key="' + CSS.escape(key) + '"]');
    if (!g || !node) return;
    STATUS_CLASSES.forEach(function (c) { g.classList.remove(c); });
    // THE SANITIZED STATUS CLASS (the poisoned-row defense, nit 2 of 2):
    // a space-bearing status (a hostile row) made classList.add THROW
    // (InvalidCharacterError) — the same live-update blinding. The
    // class is the LEGEND's closed vocabulary: anything outside it
    // paints as pending, exactly the server's own status_class default.
    var cls = node.hold ? "wf-st-held" : "wf-st-" + node.status;
    if (cls !== "wf-st-held" && STATUS_CLASSES.indexOf(cls) < 0) cls = "wf-st-pending";
    g.classList.add(cls);
    var counter = g.querySelector(".wf-counter");
    if (node.map_children > 0) {
      if (!counter) {
        counter = document.createElement("span");
        counter.className = "wf-counter";
        g.appendChild(counter);
      }
      counter.textContent = node.map_done + "/" + node.map_children;
    } else if (counter) {
      counter.remove();
    }
  }

  // ── the SSE feed ──────────────────────────────────────────────────────
  var streamStateEl = document.querySelector('[data-wf="stream-state"]');
  var derivedEl = document.querySelector('[data-wf="derived-status"]');

  function setStreamState(state, text) {
    if (!streamStateEl) return;
    streamStateEl.setAttribute("data-stream-state", state);
    streamStateEl.textContent = text;
  }

  function applySnapshot(state) {
    // THE SEQ-CURSOR: a stale frame never overwrites a fresh one.
    var cursor = window.__wfSeq || 0;
    if (state.seq <= cursor) return;
    window.__wfSeq = state.seq;
    boot.state = state;
    if (derivedEl) derivedEl.textContent = state.status;
    // THE ROOT'S MAP COUNTER (the collapse's run-level face): the frame
    // carries the SUM across sources; the header's font-mono span
    // patches with it (absent element or absent field = no-op).
    var mapEl = document.querySelector('[data-wf="map-counter"]');
    if (mapEl && typeof state.map_children === "number") {
      mapEl.textContent = (state.map_done || 0) + "/" + state.map_children;
    }
    (state.nodes || []).forEach(function (node) { paintNode(node.key, node); });
  }

  function connect() {
    if (!window.EventSource) {
      // The no-JS-live story, stated where it lands: the snapshot above
      // is the whole no-JS surface; polling is not invented here.
      setStreamState("static", "live stream unavailable — this snapshot is from page load");
      return;
    }
    var es = new EventSource(boot.streamUrl);
    es.addEventListener("run_state", function (ev) {
      var state = JSON.parse(ev.data);
      if (state.status === "uninstalled") {
        setStreamState("error", state.detail);
        es.close();
        return;
      }
      setStreamState("live", "live (seq " + state.seq + ")");
      applySnapshot(state);
    });
    es.onerror = function () {
      // The DEFINED error state: say the stream is down; EventSource
      // reconnects itself and the cursor rides Last-Event-ID.
      setStreamState("reconnecting", "reconnecting…");
    };
  }

  // ── the node panel (one delegated listener; loading/empty/error) ─────
  var panel = document.getElementById("wf-panel");
  var panelLoading = panel ? panel.querySelector('[data-panel="loading"]') : null;
  var panelError = panel ? panel.querySelector('[data-panel="error"]') : null;
  var panelBody = panel ? panel.querySelector('[data-panel="body"]') : null;

  function openPanel(key) {
    if (!panel) return;
    panel.classList.remove("hidden");
    panelLoading.classList.remove("hidden");
    panelError.classList.add("hidden");
    panelBody.classList.add("hidden");
    var state = keyState(key);
    if (state === null) {
      // THE EMPTY STATE: a node the rows do not carry (e.g. a fired map
      // child clicked in a stale view) — say it, never a blank panel.
      showPanelError("No row for node " + key + " — the run's node rows are the panel's source (source: jobs by metadata.flow_id).");
      return;
    }
    var t0 = performance.now();
    fetch(boot.streamUrl.replace(/stream$/, "") + "nodes/" + encodeURIComponent(key))
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (data) {
        window.__wfPanelLatencyMs = performance.now() - t0; // the latency band's probe
        renderPanel(data);
      })
      .catch(function (err) {
        showPanelError("The panel's read failed: " + err.message + " — retry by clicking the node again.");
      });
  }

  function renderPanel(data) {
    panelLoading.classList.add("hidden");
    panelBody.classList.remove("hidden");
    panelBody.querySelector('[data-panel="key"]').textContent = data.key;
    var lines = [
      "status: " + data.status,
      "attempt: " + data.attempt + "/" + data.max_attempts + (data.retry_kind ? " (" + data.retry_kind + ")" : ""),
      data.trace_id ? "trace: " + data.trace_id : null,
      data.error_class ? "error: " + data.error_class + " — " + (data.error_message || "") : null,
      data.parent ? "upstream hop: " + data.parent.step_key + " (" + data.parent.status + ")" : null,
      "created: " + (data.created_at || "—"),
      "started: " + (data.started_at || "—"),
      "finished: " + (data.finished_at || "—")
    ].filter(Boolean);
    var timeline = (data.timeline || []).map(function (t) {
      return t.created_at + "  " + t.status + (t.error_class ? "  " + t.error_class : "");
    });
    panelBody.innerHTML = "";
    lines.forEach(function (line) {
      var div = document.createElement("div");
      div.textContent = line;
      panelBody.appendChild(div);
    });
    if (timeline.length) {
      var h = document.createElement("div");
      h.className = "font-semibold mt-2";
      h.textContent = "attempt ledger";
      panelBody.appendChild(h);
      timeline.forEach(function (line) {
        var div = document.createElement("div");
        div.className = "font-mono text-xs";
        div.textContent = line;
        panelBody.appendChild(div);
      });
    }
  }

  function showPanelError(text) {
    panelLoading.classList.add("hidden");
    panelBody.classList.add("hidden");
    panelError.textContent = text;
    panelError.classList.remove("hidden");
  }

  if (panel) {
    // ONE delegated listener (the ticket's shape) — click + keyboard
    // (Enter/Space on a focused node) both land here.
    graphHost.addEventListener("click", function (ev) {
      var g = ev.target.closest("[data-node-key]");
      if (g) openPanel(g.getAttribute("data-node-key"));
    });
    graphHost.addEventListener("keydown", function (ev) {
      if (ev.key !== "Enter" && ev.key !== " ") return;
      var g = ev.target.closest("[data-node-key]");
      if (g) {
        ev.preventDefault();
        openPanel(g.getAttribute("data-node-key"));
      }
    });
    panel.querySelector('[data-panel="close"]').addEventListener("click", function () {
      panel.classList.add("hidden");
    });
  }

  // ── the Resolve form (the typed door's guarded POST: FORM-encoded —
  // the CSRF token is a form field, the admin's own convention) ─────────
  document.querySelectorAll('[data-wf-form="resolve"]').forEach(function (form) {
    form.addEventListener("submit", function (ev) {
      ev.preventDefault();
      var decisionEl = form.querySelector('[name="decision"]');
      var holdId = form.getAttribute("data-hold-id");
      var fd = new FormData();
      fd.set("hold_id", holdId);
      fd.set("decision", decisionEl.value);
      fd.set("reason", "resolved from the admin workflow page");
      fd.set("csrf_token", form.querySelector('[name="csrf_token"]').value);
      fetch("api/runs/" + (boot.state ? boot.state.run_id : boot.runId) + "/resolve", {
        method: "POST",
        body: fd
      })
        .then(function (r) { return r.json().then(function (b) { return { ok: r.ok, body: b }; }); })
        .then(function (res) {
          var note = document.createElement("div");
          note.className = res.ok && res.body.status === "delivered"
            ? "text-xs text-green-700 dark:text-green-400 mt-1"
            : "text-xs text-red-600 dark:text-red-400 mt-1";
          note.textContent = res.ok && res.body.status === "delivered"
            ? "delivered — the node resumes on its next drive"
            : "refused: " + (res.body.detail || res.body.reason || "unknown");
          form.appendChild(note);
        })
        .catch(function (err) {
          var note = document.createElement("div");
          note.className = "text-xs text-red-600 dark:text-red-400 mt-1";
          note.textContent = "the resolve failed: " + err.message;
          form.appendChild(note);
        });
    });
  });

  // ── boot: render the mermaid once, then connect ──────────────────────
  if (window.mermaid) {
    window.mermaid.initialize({ startOnLoad: false, securityLevel: "strict" });
    window.mermaid.render("wf-graph-render", boot.mermaid).then(function (res) {
      graphHost.innerHTML = res.svg;
      var svg = graphHost.querySelector("svg");
      if (svg) bindSvg(svg);
      connect();
    }).catch(function (err) {
      // THE ERROR STATE for the graph surface: the render failed — say
      // why (the rows are still readable below), never a blank box.
      graphHost.innerHTML = "";
      var div = document.createElement("p");
      div.className = "text-sm text-red-600 dark:text-red-400 p-4";
      div.textContent = "The graph render failed: " + err.message +
        " — the node table below carries the same state.";
      graphHost.appendChild(div);
      connect();
    });
  } else {
    // THE DEFINED DEGRADE: mermaid did not load (an air-gapped asset
    // loss) — the node list is the same state's second renderer.
    setStreamState("static", "graph renderer unavailable — the node list below is the state");
    connect();
  }
})();
