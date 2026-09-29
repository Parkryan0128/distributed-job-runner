const $ = (selector) => document.querySelector(selector);
const labels = {
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  dead: "Dead letter",
  canceled: "Canceled",
  failed: "Failed",
  expired: "Expired",
};
const state = {
  token: "",
  session: new AbortController(),
  selected: null,
  before: 0,
  next: 0,
  generation: 0,
  timer: null,
  controller: null,
  rows: "",
  pending: null,
  submitting: false,
  canceling: false,
};
const samples = {
  demo: { work_ms: 1500, fail_until: 0 },
  checksum: { text: "Hello from the job runner" },
  statistics: { values: [12, 18, 7, 24, 19] },
};

async function api(path, options = {}) {
  const signals = [state.session.signal, AbortSignal.timeout(10000)];
  if (options.signal) signals.push(options.signal);
  const signal = AbortSignal.any(signals);
  const response = await fetch(path, {
    ...options,
    signal,
    headers: {
      Authorization: `Bearer ${state.token}`,
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });
  const body = await response.json().catch(() => {
    signal.throwIfAborted();
    return null;
  });
  if (!response.ok) {
    const error = new Error(
      body?.error || `Request failed (${response.status})`,
    );
    error.status = response.status;
    throw error;
  }
  if (!body || typeof body !== "object")
    throw new Error("Server returned an invalid response");
  return body;
}

function badge(status) {
  const span = document.createElement("span");
  span.className = `badge ${status}`;
  span.textContent = labels[status] || status;
  return span;
}

function text(tag, value, className = "") {
  const el = document.createElement(tag);
  el.textContent = value;
  el.className = className;
  return el;
}

function time(value) {
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function renderJobs(page) {
  state.next = page.next_cursor || 0;
  $("#older").disabled = !state.next;
  $("#newest").disabled = !state.before;
  $("#empty").hidden = page.jobs.length > 0;
  $("#job-count").textContent =
    `${page.jobs.length} jobs${state.before ? " · older page" : " · newest first"}`;
  const fingerprint = JSON.stringify([
    state.selected,
    page.jobs.map(
      ({ id, kind, queue, status, attempt, max_attempts, created_at }) => ({
        id,
        kind,
        queue,
        status,
        attempt,
        max_attempts,
        created_at,
      }),
    ),
  ]);
  if (fingerprint === state.rows) return;
  state.rows = fingerprint;
  const focused = document.activeElement?.id;
  const rows = page.jobs.map((job) => {
    const row = document.createElement("tr");
    row.dataset.jobId = job.id;
    if (job.id === state.selected) row.className = "selected";
    const name = document.createElement("td");
    const open = document.createElement("button");
    open.id = `open-${job.id}`;
    open.setAttribute("aria-label", `Open job ${job.id.slice(0, 8)}`);
    open.append(
      text("span", job.kind),
      text("span", job.id.slice(0, 8), "job-id"),
    );
    open.addEventListener("click", () => selectJob(job.id));
    name.append(open);
    const status = document.createElement("td");
    status.append(badge(job.status));
    row.append(
      name,
      status,
      text("td", job.queue),
      text("td", `${job.attempt} / ${job.max_attempts}`),
      text("td", time(job.created_at)),
    );
    return row;
  });
  $("#jobs").replaceChildren(...rows);
  if (focused?.startsWith("open-"))
    document.getElementById(focused)?.focus({ preventScroll: true });
}

function renderDetail(job) {
  $("#detail-empty").hidden = true;
  $("#detail").hidden = false;
  $("#detail-status").replaceChildren(badge(job.status));
  $("#detail-kind").textContent = job.kind;
  $("#detail-id").textContent = job.id;
  $("#detail-queue").textContent = job.queue;
  $("#detail-attempts").textContent = `${job.attempt} / ${job.max_attempts}`;
  $("#detail-worker").textContent = job.worker_id || "—";
  $("#detail-available").textContent = time(job.available_at);
  $("#cancel-job").disabled =
    state.canceling || !["queued", "running"].includes(job.status);
  $("#payload").textContent = JSON.stringify(job.payload, null, 2);
  $("#result-section").hidden = job.result === null;
  $("#result").textContent =
    job.result === null ? "" : JSON.stringify(job.result, null, 2);
  $("#failure-section").hidden = !job.error;
  $("#failure").textContent = job.error;
  $("#no-attempts").hidden = job.attempts.length > 0;
  $("#attempts").replaceChildren(
    ...job.attempts.map((attempt) => {
      const li = document.createElement("li");
      const heading = text("div", "", "attempt-title");
      heading.append(
        text("strong", `Attempt ${attempt.number}`),
        badge(attempt.status),
      );
      const duration = attempt.finished_at
        ? ` · ${((new Date(attempt.finished_at) - new Date(attempt.started_at)) / 1000).toFixed(2)}s`
        : " · in progress";
      li.append(
        heading,
        text(
          "p",
          `${attempt.worker_id} · ${time(attempt.started_at)}${duration}`,
        ),
      );
      if (attempt.error) li.append(text("p", attempt.error, "attempt-error"));
      return li;
    }),
  );
}

async function refresh() {
  if (!state.token) return;
  clearTimeout(state.timer);
  state.controller?.abort();
  const controller = new AbortController();
  state.controller = controller;
  const generation = ++state.generation;
  const selected = state.selected;
  const query = new URLSearchParams({ limit: "20" });
  if ($("#status-filter").value) query.set("status", $("#status-filter").value);
  if ($("#queue-filter").value) query.set("queue", $("#queue-filter").value);
  if (state.before) query.set("before", String(state.before));
  try {
    const [page, stats, detail] = await Promise.all([
      api(`/api/jobs?${query}`, { signal: controller.signal }),
      api("/api/stats", { signal: controller.signal }),
      selected
        ? api(`/api/jobs/${selected}`, { signal: controller.signal })
        : null,
    ]);
    if (generation !== state.generation) return;
    renderJobs(page);
    for (const key of Object.keys(labels))
      if ($(`#count-${key}`)) $(`#count-${key}`).textContent = stats[key];
    if (detail) renderDetail(detail);
    $("#error").textContent = "";
    $("#connection").textContent = "Live";
    $("#connection").classList.add("live");
  } catch (error) {
    if (generation !== state.generation || error.name === "AbortError") return;
    if (error.status === 401) {
      disconnect();
      $("#login-error").textContent =
        "Your token is no longer valid. Connect again with the current token.";
      return;
    }
    $("#error").textContent = `${error.message}. Retrying…`;
    $("#connection").textContent = "Reconnecting";
    $("#connection").classList.remove("live");
  } finally {
    if (generation === state.generation && state.token)
      state.timer = setTimeout(refresh, 1500);
  }
}

function selectJob(id) {
  state.selected = id;
  $("#detail").hidden = true;
  $("#detail-status").replaceChildren();
  $("#detail-empty").hidden = false;
  refresh();
}

$("#login-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = event.submitter;
  button.disabled = true;
  state.token = $("#token").value;
  try {
    await api("/api/stats");
    $("#token").value = "";
    $("#login-error").textContent = "";
    $("#login").hidden = true;
    $("#console").hidden = false;
    $("#disconnect").hidden = false;
    await refresh();
  } catch (error) {
    state.token = "";
    $("#login-error").textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

function disconnect() {
  state.session.abort();
  state.session = new AbortController();
  state.token = "";
  state.pending = null;
  state.submitting = false;
  state.canceling = false;
  $("#submit-job").disabled = false;
  $("#job-form").reset();
  state.selected = null;
  state.before = 0;
  state.rows = "";
  state.generation++;
  state.controller?.abort();
  clearTimeout(state.timer);
  $("#composer").close();
  $("#console").hidden = true;
  $("#disconnect").hidden = true;
  $("#login").hidden = false;
  $("#connection").textContent = "Not connected";
  $("#connection").classList.remove("live");
  $("#jobs").replaceChildren();
  $("#detail").hidden = true;
  $("#detail-empty").hidden = false;
  $("#detail-status").replaceChildren();
  $("#notice").textContent = "";
  $("#token").focus();
}

$("#disconnect").addEventListener("click", disconnect);

for (const id of ["#status-filter", "#queue-filter"])
  $(id).addEventListener("change", () => {
    state.before = 0;
    refresh();
  });
for (const button of document.querySelectorAll(".stat"))
  button.addEventListener("click", () => {
    $("#status-filter").value = button.dataset.status;
    state.before = 0;
    refresh();
  });
$("#newest").addEventListener("click", () => {
  state.before = 0;
  refresh();
});
$("#older").addEventListener("click", () => {
  state.before = state.next;
  refresh();
});

$("#new-job").addEventListener("click", () => {
  $("#submit-error").textContent = "";
  $("#composer").showModal();
});
$("#close-composer").addEventListener("click", () => $("#composer").close());
$("#kind").addEventListener("change", () => {
  $("#job-payload").value = JSON.stringify(samples[$("#kind").value], null, 2);
});
for (const button of document.querySelectorAll("[data-preset]"))
  button.addEventListener("click", () => {
    const preset = button.dataset.preset;
    $("#kind").value = "demo";
    $("#job-payload").value = JSON.stringify(
      {
        work_ms: preset === "long" ? 20000 : 1000,
        fail_until: preset === "retry" ? 2 : preset === "dead" ? 10 : 0,
      },
      null,
      2,
    );
    const form = $("#job-form");
    form.elements.max_attempts.value = 3;
    form.elements.timeout_seconds.value = 30;
    form.elements.delay_seconds.value = 0;
  });

$("#job-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (state.submitting) return;
  $("#submit-error").textContent = "";
  const session = state.session;
  const fields = new FormData(event.currentTarget);
  let body;
  try {
    body = JSON.stringify({
      kind: fields.get("kind"),
      queue: fields.get("queue"),
      payload: JSON.parse(fields.get("payload")),
      priority: Number(fields.get("priority")),
      max_attempts: Number(fields.get("max_attempts")),
      timeout_seconds: Number(fields.get("timeout_seconds")),
      delay_seconds: Number(fields.get("delay_seconds")),
    });
  } catch {
    $("#submit-error").textContent = "Payload must be valid JSON.";
    return;
  }
  if (state.pending?.body !== body)
    state.pending = { body, key: crypto.randomUUID() };
  state.submitting = true;
  $("#submit-job").disabled = true;
  try {
    const job = await api("/api/jobs", {
      method: "POST",
      body,
      headers: { "Idempotency-Key": state.pending.key },
    });
    if (session !== state.session) return;
    state.pending = null;
    $("#composer").close();
    $("#status-filter").value = "";
    $("#queue-filter").value = "";
    state.before = 0;
    $("#notice").textContent =
      `Job ${job.id.slice(0, 8)} added to ${job.queue}.`;
    selectJob(job.id);
  } catch (error) {
    if (session !== state.session) return;
    $("#submit-error").textContent =
      `${error.message}. You can retry this submission safely.`;
  } finally {
    if (session === state.session) {
      state.submitting = false;
      $("#submit-job").disabled = false;
    }
  }
});

$("#cancel-job").addEventListener("click", async () => {
  const id = state.selected;
  if (!id || state.canceling) return;
  const session = state.session;
  state.canceling = true;
  $("#cancel-job").disabled = true;
  try {
    await api(`/api/jobs/${id}/cancel`, { method: "POST" });
    if (session !== state.session) return;
    $("#notice").textContent = `Job ${id.slice(0, 8)} canceled.`;
  } catch (error) {
    if (session !== state.session) return;
    $("#error").textContent = error.message;
  } finally {
    if (session === state.session) {
      state.canceling = false;
      refresh();
    }
  }
});
