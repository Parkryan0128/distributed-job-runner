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
  selected: null,
  before: 0,
  next: 0,
  generation: 0,
  controller: null,
  rows: "",
  pending: null,
  submitting: false,
  canceling: false,
};

async function api(path, options = {}) {
  const signals = [AbortSignal.timeout(10000)];
  if (options.signal) signals.push(options.signal);
  const signal = AbortSignal.any(signals);
  const response = await fetch(path, {
    ...options,
    signal,
    headers: {
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...options.headers,
    },
  });
  const body = await response.json().catch(() => {
    signal.throwIfAborted();
    return null;
  });
  if (!response.ok) {
    throw new Error(body?.error || `Request failed (${response.status})`);
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
      ({
        id,
        kind,
        queue,
        status,
        attempt,
        max_attempts,
        created_at,
        worker_id,
      }) => ({
        id,
        kind,
        queue,
        status,
        attempt,
        max_attempts,
        created_at,
        worker_id,
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
    open.textContent = job.id.slice(0, 8);
    open.addEventListener("click", () => selectJob(job.id));
    name.append(open);
    const status = document.createElement("td");
    status.append(badge(job.status));
    row.append(
      name,
      status,
      text("td", `${job.attempt} / ${job.max_attempts}`),
      text("td", time(job.created_at)),
      text("td", job.worker_id || "—"),
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
  $("#detail-id").textContent = job.id;
  $("#detail-attempts").textContent = `${job.attempt} / ${job.max_attempts}`;
  $("#detail-worker").textContent = job.worker_id || "—";
  $("#detail-available").textContent = time(job.available_at);
  $("#cancel-job").disabled =
    !control.mine ||
    !connected ||
    state.canceling ||
    !["queued", "running"].includes(job.status);
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
  state.controller?.abort();
  const controller = new AbortController();
  state.controller = controller;
  const generation = ++state.generation;
  const selected = state.selected;
  const query = new URLSearchParams({ limit: "20" });
  if ($("#status-filter").value) query.set("status", $("#status-filter").value);
  if (state.before) query.set("before", String(state.before));
  try {
    const [page, detail] = await Promise.all([
      api(`/api/jobs?${query}`, { signal: controller.signal }),
      selected
        ? api(`/api/jobs/${selected}`, { signal: controller.signal })
        : null,
    ]);
    if (generation !== state.generation) return;
    renderJobs(page);
    if (detail) renderDetail(detail);
    $("#error").textContent = "";
  } catch (error) {
    if (generation !== state.generation || error.name === "AbortError") return;
    $("#error").textContent = `${error.message}.`;
  }
}

function selectJob(id) {
  state.selected = id;
  $("#detail").hidden = true;
  $("#detail-status").replaceChildren();
  $("#detail-empty").hidden = false;
  refresh();
}

for (const id of ["#status-filter"])
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

$("#new-job").addEventListener("click", async () => {
  try {
    if (!control.mine) await changeControl("claim");
    $("#submit-error").textContent = "";
    $("#composer").showModal();
  } catch (error) {
    $("#action-error").textContent = error.message;
  }
});
$("#close-composer").addEventListener("click", () => $("#composer").close());
for (const button of document.querySelectorAll("[data-preset]"))
  button.addEventListener("click", () => {
    const preset = button.dataset.preset;
    submitJob(
      JSON.stringify({
        kind: "demo",
        payload: {
          work_ms: preset === "long" ? 20000 : 1000,
          fail_until: preset === "retry" ? 2 : preset === "dead" ? 10 : 0,
        },
        priority: 0,
        max_attempts: 3,
        timeout_seconds: 30,
        delay_seconds: 0,
      }),
    );
  });

$("#job-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (state.submitting) return;
  $("#submit-error").textContent = "";
  const fields = new FormData(event.currentTarget);
  let body;
  try {
    const payload = {
      work_ms: Math.round(Number($("#task-time").value) * 1000),
      fail_until: Number($("#fail-until").value),
    };
    body = JSON.stringify({
      kind: "demo",
      payload,
      priority: Number(fields.get("priority")),
      max_attempts: Number(fields.get("max_attempts")),
      timeout_seconds: Number(fields.get("timeout_seconds")),
      delay_seconds: Number(fields.get("delay_seconds")),
    });
  } catch (error) {
    $("#submit-error").textContent = error.message;
    return;
  }
  await submitJob(body);
});

async function submitJob(body) {
  if (state.submitting || !control.mine || !connected) return;
  $("#submit-error").textContent = "";
  if (state.pending?.body !== body)
    state.pending = { body, key: crypto.randomUUID() };
  state.submitting = true;
  renderWorkload();
  try {
    const job = await api("/api/jobs", {
      method: "POST",
      body,
      headers: { "Idempotency-Key": state.pending.key },
    });
    state.pending = null;
    $("#composer").close();
    $("#status-filter").value = "";
    state.before = 0;
    selectJob(job.id);
  } catch (error) {
    $("#submit-error").textContent =
      `${error.message}. You can retry this submission safely.`;
  } finally {
    state.submitting = false;
    renderWorkload();
  }
}

$("#cancel-job").addEventListener("click", async () => {
  const id = state.selected;
  if (!id || state.canceling) return;
  state.canceling = true;
  $("#action-error").textContent = "";
  $("#cancel-job").disabled = true;
  try {
    await api(`/api/jobs/${id}/cancel`, { method: "POST" });
  } catch (error) {
    $("#action-error").textContent = error.message;
  } finally {
    state.canceling = false;
    refresh();
  }
});

const profiles = {
  low: {
    rate: 0.3,
    every: 20,
    note: "A steady trickle of work. Most slots remain available.",
  },
  medium: {
    rate: 1.2,
    every: 10,
    note: "Steady activity, with occasional failures and recovery.",
  },
  high: {
    rate: 3,
    every: 5,
    note: "At capacity. Incoming work queues while failed attempts retry.",
  },
};
const workload = { level: "low" };
let control = {
  available: false,
  mine: false,
  running: false,
  level: "low",
  remaining_ms: 0,
};
let connected = false;
let stream;
let epoch = 0;
let cursor = 0;
let workers = [];
const activity = new Map();
const transitions = new Map();
let tableTimer;

function jobBlock(job, status) {
  const button = text(
    "button",
    "",
    "job-block" +
      ((job.attempt > 0 && status !== "Running") || job.attempt > 1
        ? " retry-job"
        : ""),
  );
  if (status === "Succeeded") button.classList.add("completed-job");
  button.type = "button";
  button.id = `activity-${job.id}`;
  button.dataset.activityJob = job.id;
  button.setAttribute(
    "aria-label",
    `${job.kind} ${job.id.slice(0, 8)} · ${status}`,
  );
  button.append(text("code", job.id.slice(0, 8)));
  if (status.includes("Retry"))
    button.append(
      text(
        "small",
        status === "Retrying" ? `↻ ${job.attempt}/${job.max_attempts}` : "↻",
      ),
    );
  button.title = `${job.id} · ${status} · attempt ${job.attempt}/${job.max_attempts}`;
  if (job.id === state.selected) button.classList.add("selected");
  button.addEventListener("click", () => selectJob(job.id));
  return button;
}

// Keep existing blocks and panels mounted so unrelated events cannot restart animations.
function syncActivity(parent, desired, existingJobs) {
  const old = [...parent.childNodes];
  const used = new Set();
  desired.forEach((next, index) => {
    const jobID = next.dataset?.activityJob;
    const workerID = next.dataset?.workerId;
    let current = jobID
      ? existingJobs.get(jobID)
      : workerID
        ? old.find((node) => node.dataset?.workerId === workerID)
        : old[index];
    if (
      !current ||
      used.has(current) ||
      current.nodeName !== next.nodeName ||
      (!jobID && current.dataset?.activityJob) ||
      (!workerID && current.dataset?.workerId)
    )
      current = next;
    if (current !== next) {
      if (current.nodeType === Node.TEXT_NODE) {
        if (current.data !== next.data) current.data = next.data;
      } else {
        for (const attr of [...current.attributes])
          if (!next.hasAttribute(attr.name)) current.removeAttribute(attr.name);
        for (const attr of next.attributes)
          if (current.getAttribute(attr.name) !== attr.value)
            current.setAttribute(attr.name, attr.value);
        syncActivity(current, [...next.childNodes], existingJobs);
      }
    }
    used.add(current);
    if (parent.childNodes[index] !== current)
      parent.insertBefore(current, parent.childNodes[index] || null);
  });
  for (const child of [...parent.childNodes])
    if (!used.has(child)) child.remove();
}

const blockMotion = new WeakMap();
function layoutPosition(element) {
  let x = 0,
    y = 0;
  for (let node = element; node; node = node.offsetParent) {
    x += node.offsetLeft;
    y += node.offsetTop;
  }
  return { x, y };
}
function renderWorkers(workers, pending, waitingCount) {
  const existingJobs = new Map(
    [...document.querySelectorAll("[data-activity-job]")].map((el) => [
      el.dataset.activityJob,
      el,
    ]),
  );
  const positions = new Map(
    [...existingJobs].map(([id, el]) => [
      id,
      {
        layout: layoutPosition(el),
        visual: el.getBoundingClientRect(),
      },
    ]),
  );
  const focused = document.activeElement?.id;
  const online = workers.filter((w) => w.online);
  const capacity = online.reduce((n, w) => n + w.concurrency, 0);
  $("#worker-summary").textContent =
    `${online.length} worker${online.length === 1 ? "" : "s"} online · ${capacity} concurrent slots`;
  $("#slot-summary").textContent =
    `${state.stats.running} running · ${capacity} slots online`;
  // A job appears in one activity row at a time.
  const active = new Set(workers.flatMap((w) => w.jobs.map((j) => j.id)));
  const available = pending.filter((j) => !active.has(j.id));
  const width = $("#pending-jobs").clientWidth;
  const slots = Math.max(0, Math.floor((width + 8) / 108));
  const visible =
    waitingCount > slots ? Math.max(0, Math.floor((width - 76) / 108)) : slots;
  const waiting = available.slice(0, visible);
  $("#pending-count").textContent = `· ${waitingCount} waiting`;
  const pendingBlocks = waiting.length
    ? waiting.map((j) =>
        jobBlock(
          j,
          j.attempt > 0
            ? "Retry waiting"
            : new Date(j.available_at) > new Date()
              ? "Scheduled"
              : "Queued",
        ),
      )
    : waitingCount
      ? []
      : [text("p", "No waiting jobs", "muted")];
  if (waitingCount > waiting.length) {
    const more = text(
      "span",
      `+${waitingCount - waiting.length} more`,
      "queue-overflow",
    );
    pendingBlocks.push(more);
  }
  syncActivity($("#pending-jobs"), pendingBlocks, existingJobs);
  $("#pending-note").textContent = "";
  const panels = workers.map((worker) => {
    const panel = text("section", "", "worker-panel");
    panel.dataset.workerId = worker.id;
    panel.setAttribute("aria-label", worker.id);
    const heading = text("div", "", "activity-heading");
    heading.append(
      text("h3", worker.id),
      text(
        "span",
        worker.online
          ? `${worker.jobs.filter((j) => j.status === "running").length} / ${worker.concurrency} running`
          : "Offline · waiting for lease recovery",
      ),
    );
    const blocks = text("div", "", "job-blocks");
    blocks.append(
      ...worker.jobs.map((j) =>
        jobBlock(
          j,
          j.status === "succeeded"
            ? "Succeeded"
            : !worker.online
              ? "Lease recovery pending"
              : j.attempt > 1
                ? "Retrying"
                : "Running",
        ),
      ),
    );
    for (let i = worker.jobs.length; i < worker.concurrency; i++)
      blocks.append(text("div", worker.online ? "—" : "Offline", "empty-slot"));
    panel.append(heading, blocks);
    return panel;
  });
  syncActivity(
    $("#workers"),
    panels.length
      ? panels
      : [
          text(
            "p",
            "No workers detected. Queued jobs will wait for a worker.",
            "muted",
          ),
        ],
    existingJobs,
  );
  if (
    !document.hidden &&
    !window.matchMedia("(prefers-reduced-motion: reduce)").matches
  ) {
    document.querySelectorAll("[data-activity-job]").forEach((el) => {
      const before = positions.get(el.dataset.activityJob);
      const after = layoutPosition(el);
      if (
        before &&
        (Math.abs(before.layout.x - after.x) > 1 ||
          Math.abs(before.layout.y - after.y) > 1)
      ) {
        // Retarget from the visible position only when the layout really changed.
        blockMotion.get(el)?.cancel();
        const target = el.getBoundingClientRect();
        blockMotion.set(
          el,
          el.animate(
            [
              {
                transform: `translate(${before.visual.left - target.left}px, ${before.visual.top - target.top}px)`,
              },
              { transform: "translate(0, 0)" },
            ],
            { duration: 250, easing: "ease-out" },
          ),
        );
      } else if (!before) {
        blockMotion.set(
          el,
          el.animate(
            [
              { opacity: 0, transform: "scale(.9)" },
              { opacity: 1, transform: "scale(1)" },
            ],
            { duration: 200, easing: "ease-out" },
          ),
        );
      }
    });
  }
  if (focused?.startsWith("activity-"))
    document.getElementById(focused)?.focus({ preventScroll: true });
}

function renderWorkload() {
  document.querySelectorAll("[data-preset]").forEach((b) => {
    b.disabled = state.submitting || !control.mine || !connected;
  });
  const p = profiles[workload.level];
  document.querySelectorAll("[data-level]").forEach((b) => {
    b.setAttribute("aria-pressed", String(b.dataset.level === workload.level));
    b.disabled = !connected || (!control.available && !control.mine);
  });
  $("#arrival-rate").textContent = `${p.rate.toFixed(1)} jobs/s`;
  $("#retry-rate").textContent = `${100 / p.every}% of jobs`;
  $("#workload-note").textContent = p.note;
  $("#workload-state").textContent = !connected
    ? "Reconnecting…"
    : control.available
      ? "Ready · 30s per turn"
      : `${control.mine ? "Your turn" : "Another visitor is controlling"} · ${Math.ceil(control.remaining_ms / 1000)}s`;
  $("#toggle-workload").textContent =
    control.mine && control.running ? "Stop" : "Start";
  $("#toggle-workload").disabled =
    !connected || (!control.available && !control.mine);
  $("#new-job").disabled = !connected || (!control.available && !control.mine);
  $("#release-control").hidden = !control.mine || control.running;
  $("#cancel-job").disabled =
    !control.mine ||
    !connected ||
    state.canceling ||
    !["Queued", "Running"].includes($("#detail-status").textContent);
  $("#submit-job").disabled = state.submitting || !control.mine || !connected;
}
function setControl(value) {
  if (control.mine && !value.mine && $("#composer").open)
    $("#composer").close();
  control = value;
  if (!control.available) workload.level = control.level;
  renderWorkload();
}
async function changeControl(action, level = workload.level) {
  const value = await api("/api/demo", {
    method: "POST",
    body: JSON.stringify({ action, level }),
  });
  setControl(value);
  $("#action-error").textContent = "";
  return value;
}
for (const b of document.querySelectorAll("[data-level]"))
  b.addEventListener("click", async () => {
    if (control.available) {
      workload.level = b.dataset.level;
      renderWorkload();
      return;
    }
    try {
      await changeControl("level", b.dataset.level);
    } catch (e) {
      $("#action-error").textContent = e.message;
    }
  });
$("#toggle-workload").addEventListener("click", async () => {
  try {
    await changeControl(control.mine && control.running ? "stop" : "start");
  } catch (e) {
    $("#action-error").textContent = e.message;
  }
});
$("#release-control").addEventListener("click", async () => {
  try {
    await changeControl("stop");
  } catch (e) {
    $("#action-error").textContent = e.message;
  }
});
function paintActivity() {
  const assigned = workers.map((w) => ({
    ...w,
    jobs: [...activity.values()].filter(
      (j) =>
        ["running", "succeeded"].includes(j.status) && j.worker_id === w.id,
    ),
  }));
  const pending = [...activity.values()]
    .filter((j) => j.status === "queued")
    .sort(
      (a, b) =>
        Number(new Date(a.available_at) > Date.now()) -
          Number(new Date(b.available_at) > Date.now()) ||
        b.priority - a.priority ||
        new Date(a.available_at) - new Date(b.available_at),
    )
    .slice(0, 12);
  if (state.stats)
    renderWorkers(
      assigned,
      pending,
      [...activity.values()].filter((j) => j.status === "queued").length,
    );
}
function updateStats() {
  for (const key of ["queued", "running", "succeeded", "dead", "canceled"])
    $("#count-" + key).textContent = state.stats[key];
}
function tableRefresh() {
  if (!tableTimer)
    tableTimer = setTimeout(() => {
      tableTimer = null;
      refresh();
    }, 300);
}
async function animateJob(id, list, version) {
  while (list.length && version === epoch) {
    const job = list.shift();
    if (["queued", "running"].includes(job.status)) activity.set(id, job);
    else if (job.status === "succeeded" && activity.get(id)?.worker_id) {
      activity.set(id, { ...job, worker_id: activity.get(id).worker_id });
      paintActivity();
      await new Promise((resolve) => setTimeout(resolve, 400));
      if (version !== epoch) return;
      activity.delete(id);
    } else activity.delete(id);
    paintActivity();
    // Give every actual transition a brief visible phase, even for a zero-duration job.
    await new Promise((resolve) => setTimeout(resolve, 300));
  }
  if (version === epoch) transitions.delete(id);
}
function connectEvents() {
  stream?.close();
  stream = new EventSource("/api/events");
  stream.addEventListener("reset", () => connectEvents());
  stream.addEventListener("snapshot", (e) => {
    const snap = JSON.parse(e.data);
    epoch++;
    transitions.clear();
    activity.clear();
    cursor = snap.cursor;
    snap.jobs.forEach((j) => activity.set(j.id, j));
    state.stats = snap.stats;
    workers = snap.workers;
    connected = true;
    $("#connection").textContent = "Live";
    $("#error").textContent = "";
    updateStats();
    paintActivity();
    renderWorkload();
    refresh();
  });
  stream.addEventListener("job", (e) => {
    const event = JSON.parse(e.data);
    if (event.id <= cursor) return;
    cursor = event.id;
    if (event.previous && state.stats[event.previous] !== undefined)
      state.stats[event.previous] = Math.max(
        0,
        state.stats[event.previous] - 1,
      );
    if (state.stats[event.job.status] !== undefined)
      state.stats[event.job.status]++;
    updateStats();
    tableRefresh();
    const list = transitions.get(event.job.id);
    if (list) list.push(event.job);
    else {
      const next = [event.job];
      transitions.set(event.job.id, next);
      animateJob(event.job.id, next, epoch);
    }
    if (transitions.size > 128) {
      epoch++;
      transitions.clear();
      connectEvents();
    }
  });
  stream.addEventListener("workers", (e) => {
    workers = JSON.parse(e.data);
    paintActivity();
  });
  stream.addEventListener("demo", (e) => setControl(JSON.parse(e.data)));
  stream.onerror = connectionLost;
}
function connectionLost() {
  connected = false;
  $("#connection").textContent = "Reconnecting";
  $("#error").textContent = "Connection lost. Reconnecting…";
  renderWorkload();
}
window.addEventListener("offline", () => {
  stream?.close();
  connectionLost();
});
window.addEventListener("online", () => initialize());
async function initialize() {
  try {
    setControl(await api("/api/demo"));
    connectEvents();
  } catch (e) {
    $("#error").textContent = e.message;
    setTimeout(initialize, 2000);
  }
}
window.addEventListener("pagehide", () => stream?.close());
window.addEventListener("pageshow", (e) => {
  if (e.persisted) initialize();
});
new ResizeObserver(() => paintActivity()).observe($("#pending-jobs"));
renderWorkload();
initialize();
