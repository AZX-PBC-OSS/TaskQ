/* The workflow-run page's live machinery (T11): render-once SVG +
 * data-node-key patches + the SSE revisioned-snapshot feed + the node
 * panel (with the node I/O) + the Resolve form + the rerun forms'
 * confirm dialogs + the node table's live patch. The conventions are
 * the admin's own: no bundler, no React, no CDN — mermaid is vendored
 * beside alpine/htmx/lucide for the same air-gapped-ops reason.
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
  // THE BASE_PATH LAW (the relative-URL 404 bug's conviction): every
  // URL this script builds rides the mounted prefix the server stamped
  // into the boot JSON — a relative "api/runs/…" from a page at
  // {base_path}/workflows/{id} resolves to
  // {base_path}/workflows/api/runs/… and 404s.
  var BASE = boot.basePath || "";

  // ── the status chips' source (the ONE map — _base.html emits it; the
  // node table's live patch derives its classes from the same blob) ──
  var statusClasses = window.__taskqStatusClasses || null;

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
    paintRow(key, node);
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

  // ── the retry affordance's LIVE form (H-C9: the affordance tracks the
  // row's state, never the page load — a node that ARRIVES at failed
  // while you watch gains the form; one that leaves it loses the form).
  // The form mirrors the server-rendered one exactly: native POST, the
  // stamped CSRF field, the reason, the confirm guard. ─────────────────
  function retryForm(key) {
    var table = document.querySelector("[data-wf-node-table]");
    var token = table ? table.getAttribute("data-csrf") || "" : "";
    var runId = boot.state ? boot.state.run_id : boot.runId;
    var form = document.createElement("form");
    form.method = "post";
    form.className = "m-0";
    form.setAttribute("data-wf-form", "rerun-node");
    form.setAttribute(
      "data-confirm",
      "Retry node " + key +
        "? Its failed attempt re-opens (the attempt ordinal continues) and its blocked downstream clears; the audit trail records the retry."
    );
    form.action =
      BASE + "/api/runs/" + runId + "/nodes/" + encodeURIComponent(key) + "/retry";
    var csrf = document.createElement("input");
    csrf.type = "hidden";
    csrf.name = "csrf_token";
    csrf.value = token;
    var reason = document.createElement("input");
    reason.type = "hidden";
    reason.name = "reason";
    reason.value = "node retry from the admin run page";
    var btn = document.createElement("button");
    btn.type = "submit";
    btn.className = "px-2 py-1 rounded bg-blue-600 text-white text-xs hover:bg-blue-700";
    btn.textContent = "Retry";
    form.appendChild(csrf);
    form.appendChild(reason);
    form.appendChild(btn);
    // THE CONFIRM GUARD (the same contract the load-time forms carry —
    // the submit stays NATIVE, the 303 redirect + the refusal banner
    // land the defined state with or without this script).
    form.addEventListener("submit", function (ev) {
      if (!window.confirm(form.getAttribute("data-confirm"))) ev.preventDefault();
    });
    return form;
  }

  // ── the node table's live patch (the graph's second renderer — the
  // same snapshot patches both, so they cannot disagree) ────────────────
  function paintRow(key, node) {
    // THE ESCAPED SELECTOR (the same poisoned-row defense as the
    // graph's): the row key is attacker-controlled — CSS.escape before
    // it touches a selector.
    var row = document.querySelector('[data-wf-node-row="' + CSS.escape(key) + '"]');
    if (!row || !node) return;
    var chip = row.querySelector('[data-wf-node-chip]');
    if (chip && statusClasses) {
      // THE CLOSED VOCABULARY: an unknown status paints as pending —
      // the server's own status_class default (and the chip map's).
      var cls = statusClasses.chip[node.status] || statusClasses.chip.pending;
      chip.className = statusClasses.badgeBase + cls;
      chip.textContent = node.status;
    }
    var mapCell = row.querySelector('[data-wf-node-map]');
    if (mapCell && typeof node.map_children === "number") {
      mapCell.textContent = node.map_children > 0 ? (node.map_done || 0) + "/" + node.map_children : "—";
    }
    var holdCell = row.querySelector('[data-wf-node-hold]');
    if (holdCell) {
      if (node.hold) {
        holdCell.textContent = "⏳ held";
        holdCell.setAttribute("data-wf-held", "1");
      } else if (holdCell.getAttribute("data-wf-held") === "1") {
        // A hold we painted clearing: back to the plain dash. A static
        // join-wait text is NEVER overwritten (the frame carries no
        // join-wait fact — the snapshot's row said it already).
        holdCell.textContent = "—";
        holdCell.removeAttribute("data-wf-held");
      }
    }
    // THE ERROR COLUMN (H-C9): the frame carries the row's bounded
    // error class — the cell patches live (the red: a node that failed
    // while you watched showed its class only after a reload).
    var errCell = row.querySelector('[data-wf-node-error]');
    if (errCell) {
      if (node.error_class) {
        errCell.textContent = node.error_class;
        errCell.classList.add("text-red-600", "dark:text-red-400");
      } else {
        errCell.textContent = "—";
        errCell.classList.remove("text-red-600", "dark:text-red-400");
      }
    }
    // THE RETRY AFFORDANCE'S LIVE ADD/REMOVE (H-C9): a failed row
    // carries the retry form; a row that leaves failed reverts to the
    // dash. Only rows the ACTIONS column renders participate (the
    // cell hook exists only on an actions-enabled deployment).
    var actionsCell = row.querySelector('[data-wf-node-actions]');
    if (actionsCell) {
      var staleForm = actionsCell.querySelector('[data-wf-form="rerun-node"]');
      if (node.status === "failed" && !staleForm && tableHasCsrf()) {
        actionsCell.textContent = "";
        actionsCell.appendChild(retryForm(key));
      } else if (node.status !== "failed" && staleForm) {
        actionsCell.textContent = "—";
      }
    }
  }

  function tableHasCsrf() {
    var table = document.querySelector("[data-wf-node-table]");
    return !!(table && table.getAttribute("data-csrf"));
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

  function openPanel(key, mapIndex) {
    if (!panel) return;
    panel.classList.remove("hidden");
    panelLoading.classList.remove("hidden");
    panelError.classList.add("hidden");
    panelBody.classList.add("hidden");
    var ioHost = panel.querySelector('[data-panel="io"]');
    if (ioHost) ioHost.classList.add("hidden");
    var state = keyState(key);
    if (state === null) {
      // THE EMPTY STATE: a node the rows do not carry (e.g. a fired map
      // child clicked in a stale view) — say it, never a blank panel.
      showPanelError("No row for node " + key + " — the run's node rows are the panel's source (source: jobs by metadata.flow_id).");
      return;
    }
    var url = boot.streamUrl.replace(/stream$/, "") + "nodes/" + encodeURIComponent(key);
    if (mapIndex !== undefined && mapIndex !== null) {
      // THE MAP-INDEX ADDRESSING: a map's children share one step key —
      // the child is addressed by (step, map_index).
      url += "?map_index=" + encodeURIComponent(mapIndex);
    }
    var t0 = performance.now();
    fetch(url)
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
    renderIo(data.io);
    panelBody.querySelector('[data-panel="key"]').textContent =
      data.key + (data.map_index !== null && data.map_index !== undefined ? " #" + data.map_index : "");
    var lines = [
      "status: " + data.status,
      "attempt: " + data.attempt + "/" + data.max_attempts + (data.retry_kind ? " (" + data.retry_kind + ")" : ""),
      data.trace_id ? "trace: " + data.trace_id : null,
      data.cache_hit ? "cache_hit: the body did NOT run — an earlier run's cached result was delivered (producing run " + (data.cache_hit.run_id || "?") + ")" : null,
      data.error_class ? "error: " + data.error_class + " — " + (data.error_message || "") : null,
      data.parent ? "upstream hop: " + data.parent.step_key + " (" + data.parent.status + ")" : null,
      "created: " + (data.created_at || "—"),
      "started: " + (data.started_at || "—"),
      "finished: " + (data.finished_at || "—")
    ].filter(Boolean);
    panelBody.innerHTML = "";
    lines.forEach(function (line) {
      var div = document.createElement("div");
      div.textContent = line;
      panelBody.appendChild(div);
    });
    var children = data.children || [];
    if (children.length) {
      // THE MAP'S CHILDREN CENSUS: one clickable row per child, in map
      // order — the operator watches ONE child's story (its attempt
      // ladder) by (step, map_index), never a step-key-arbitrary read.
      // THE BOUNDED RENDER (H-C9): the server caps the rows and carries
      // the total — the "N more" arm names the rest, never a 10k DOM.
      var h = document.createElement("div");
      h.className = "font-semibold mt-2";
      var total = typeof data.children_total === "number" ? data.children_total : children.length;
      h.textContent = "map children (" + total + ")";
      panelBody.appendChild(h);
      children.forEach(function (c) {
        var div = document.createElement("div");
        div.className = "font-mono text-xs cursor-pointer hover:underline text-blue-600 dark:text-blue-400";
        div.textContent = "#" + c.map_index + "  " + c.status + "  attempt " + c.attempt + "/" + c.max_attempts + (c.error_class ? "  " + c.error_class : "");
        div.addEventListener("click", function () { openPanel(data.key, c.map_index); });
        panelBody.appendChild(div);
      });
      var more = total - children.length;
      if (more > 0) {
        var moreDiv = document.createElement("div");
        moreDiv.className = "text-xs text-slate-500 dark:text-slate-400 mt-1";
        moreDiv.textContent = "… " + more + " more children not shown (total " + total +
          ") — open a child by its map index: ?map_index=<n>";
        panelBody.appendChild(moreDiv);
      }
    }
    var timeline = (data.timeline || []).map(function (t) {
      return t.created_at + "  " + t.status + (t.error_class ? "  " + t.error_class : "");
    });
    if (timeline.length) {
      var h2 = document.createElement("div");
      h2.className = "font-semibold mt-2";
      h2.textContent = "attempt ledger";
      panelBody.appendChild(h2);
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

  // ── the node I/O section (the wired input + the decoded output —
  // every value lands via textContent: the escaping law's DOM form) ────
  function renderIo(io) {
    var host = panel ? panel.querySelector('[data-panel="io"]') : null;
    if (!host) return;
    host.innerHTML = "";
    var blocks = [];
    io = io || {};
    var input = io.input;
    if (input && Object.keys(input).length) {
      blocks.push(["input — the wired args (display-redacted through the engine's own pipeline)", JSON.stringify(input, null, 2)]);
    } else {
      blocks.push(["input — the wired args", "none recorded on the row (the entry input and the wiring's data args ride the node's payload; a parent-result arg resolves at run time)"]);
    }
    var capture = io.capture;
    if (capture) {
      var captureText = JSON.stringify(capture, null, 2);
      if (capture.__truncated__ !== undefined) {
        captureText += "\n(the engine cut " + capture.__truncated__ + " characters at the write — the __truncated__ marker)";
      }
      // THE CAPTURE'S LABEL (H-C6): WHICH attempt's capture this is —
      // the read prefers the LATEST FAILED attempt, so a later
      // success's NULL capture never hides the failure's.
      var which = io.capture_attempt !== undefined && io.capture_attempt !== null
        ? " — attempt " + io.capture_attempt + " (" + (io.capture_status || "failed") + ")"
        : "";
      blocks.push(["failure capture" + which + " — as the engine wrote it (redacted + truncated at persistence)", captureText]);
    } else if (io.capture_pruned) {
      // THE PRUNED ARM (H-C7): the run's step history was deleted by
      // the retention arms — the honest state, never the suppression
      // lie (the capture was not policy-suppressed; it is GONE).
      blocks.push(["failure capture", "the flow history was pruned by retention — this run's step ledger (and its failure captures) is gone; the capture was not policy-suppressed"]);
    } else if (io.capture_suppressed) {
      blocks.push(["failure capture", "captured nothing — the workflow's capture policy suppressed it (this panel never substitutes raw rows for a suppressed capture)"]);
    }
    var result = io.result;
    if (result && result.text) {
      blocks.push(["output — the decoded result (display-redacted)" + (result.truncated ? " — TRUNCATED for display; the full value stays in the row" : ""), result.text]);
      // THE WALK-BUDGET ARM (the bounded walk's honesty): the server
      // pre-trims a huge result BEFORE its redaction walk and NAMES the
      // cut — the arm rides beside the text, never a silent discard.
      if (result.walk_note) {
        blocks.push(["output — walk budget", result.walk_note]);
      }
    } else if (!capture && !io.capture_suppressed) {
      blocks.push(["output — the decoded result", "no result captured yet"]);
    }
    blocks.forEach(function (pair) {
      var h = document.createElement("div");
      h.className = "font-semibold mt-2";
      h.textContent = pair[0];
      host.appendChild(h);
      var pre = document.createElement("pre");
      pre.className = "text-xs bg-slate-50 dark:bg-slate-800 p-2 rounded overflow-x-auto";
      pre.textContent = pair[1];
      host.appendChild(pre);
    });
    host.classList.remove("hidden");
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
      fetch(BASE + "/api/runs/" + (boot.state ? boot.state.run_id : boot.runId) + "/resolve", {
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

  // ── the confirm dialogs (the rerun forms' native-POST guard): a form
  // that declares data-confirm asks once before the browser submits —
  // the submit stays NATIVE (no fetch), so the 303 redirect + the
  // refusal banner land the defined state with or without this script. ──
  document.querySelectorAll("form[data-confirm]").forEach(function (form) {
    form.addEventListener("submit", function (ev) {
      if (!window.confirm(form.getAttribute("data-confirm"))) ev.preventDefault();
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
