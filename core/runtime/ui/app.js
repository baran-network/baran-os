(function () {
  "use strict";

  var decisions = {};
  var listEl = document.getElementById("decision-list");
  var emptyEl = document.getElementById("empty");
  var statusDot = document.getElementById("sse-status");
  var statusLabel = document.getElementById("sse-label");

  // --- Rendering ---

  function render() {
    var ids = Object.keys(decisions);
    if (ids.length === 0) {
      listEl.innerHTML = "";
      emptyEl.style.display = "";
      return;
    }
    emptyEl.style.display = "none";
    // Sort newest first.
    ids.sort(function (a, b) {
      return (decisions[b].requested_at || "").localeCompare(
        decisions[a].requested_at || ""
      );
    });
    listEl.innerHTML = ids.map(renderCard).join("");
  }

  function renderCard(id) {
    var d = decisions[id];
    var hasConflict = d.conflict_ids && d.conflict_ids.length > 0;
    var cardClass = "decision-card" + (hasConflict ? " conflict" : "");
    var badge = hasConflict
      ? '<span class="badge badge-conflict">CONFLICT</span>'
      : '<span class="badge badge-pending">PENDING</span>';

    var resources = "";
    if (d.resource_ids && d.resource_ids.length > 0) {
      resources =
        '<div class="resources">' +
        d.resource_ids
          .map(function (r) {
            return '<span class="resource-tag">' + esc(r) + "</span>";
          })
          .join("") +
        "</div>";
    }

    var conflictInfo = "";
    if (hasConflict) {
      conflictInfo =
        '<div class="conflict-info">Conflicts with decisions: ' +
        d.conflict_ids.map(function (c) { return esc(c.slice(0, 8)); }).join(", ") +
        "</div>";
    }

    var ts = d.requested_at ? new Date(d.requested_at).toLocaleString() : "";

    return (
      '<div class="' + cardClass + '" data-id="' + esc(id) + '">' +
        '<div class="card-header">' +
          "<h3>" + esc(d.step_name || "Step " + d.step_index) + "</h3>" +
          badge +
        "</div>" +
        '<div class="prompt">' + esc(d.prompt) + "</div>" +
        '<div class="meta">' +
          "<span>Workflow: " + esc((d.workflow_id || "").slice(0, 8)) + "...</span>" +
          "<span>Step: " + d.step_index + "</span>" +
          "<span>" + esc(ts) + "</span>" +
        "</div>" +
        resources +
        conflictInfo +
        '<div class="actions">' +
          '<input type="text" placeholder="Comment (optional)" id="comment-' + esc(id) + '">' +
          '<button class="btn btn-approve" onclick="window._respond(\'' + esc(id) + '\',\'approve\')">Approve</button>' +
          '<button class="btn btn-reject" onclick="window._respond(\'' + esc(id) + '\',\'reject\')">Reject</button>' +
        "</div>" +
      "</div>"
    );
  }

  function esc(s) {
    if (!s) return "";
    var el = document.createElement("span");
    el.textContent = s;
    return el.innerHTML;
  }

  // --- API ---

  function fetchDecisions() {
    fetch("/api/decisions")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        decisions = {};
        var list = data.decisions || data || [];
        list.forEach(function (d) {
          decisions[d.decision_id] = d;
        });
        render();
      })
      .catch(function (err) {
        console.error("fetch decisions:", err);
      });
  }

  window._respond = function (decisionId, action) {
    var commentEl = document.getElementById("comment-" + decisionId);
    var comment = commentEl ? commentEl.value : "";

    // Disable buttons during request.
    var card = document.querySelector('[data-id="' + decisionId + '"]');
    if (card) {
      var btns = card.querySelectorAll("button");
      btns.forEach(function (b) { b.disabled = true; });
    }

    fetch("/api/decisions/" + decisionId + "/respond", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        action: action,
        operator_id: "operator-ui",
        comment: comment,
      }),
    })
      .then(function (r) {
        if (r.ok) {
          delete decisions[decisionId];
          render();
        } else {
          return r.json().then(function (err) {
            alert("Error: " + (err.error || r.statusText));
            if (card) {
              card.querySelectorAll("button").forEach(function (b) { b.disabled = false; });
            }
          });
        }
      })
      .catch(function (err) {
        alert("Network error: " + err.message);
        if (card) {
          card.querySelectorAll("button").forEach(function (b) { b.disabled = false; });
        }
      });
  };

  // --- SSE ---

  function connectSSE() {
    var es = new EventSource("/api/decisions/stream");

    es.onopen = function () {
      statusDot.className = "status-dot";
      statusLabel.textContent = "Connected";
    };

    es.onerror = function () {
      statusDot.className = "status-dot disconnected";
      statusLabel.textContent = "Reconnecting...";
    };

    es.addEventListener("decision.new", function (e) {
      var d = JSON.parse(e.data);
      decisions[d.decision_id] = d;
      render();
    });

    es.addEventListener("decision.conflict", function (e) {
      var data = JSON.parse(e.data);
      // Refresh to get updated conflict_ids on all decisions.
      if (data.decision_ids) {
        fetchDecisions();
      }
    });

    es.addEventListener("decision.resolved", function (e) {
      var data = JSON.parse(e.data);
      delete decisions[data.decision_id];
      render();
    });
  }

  // --- Init ---

  fetchDecisions();
  connectSSE();
})();
