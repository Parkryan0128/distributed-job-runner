import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { test, before, after } from "node:test";

const base = process.env.BASE_URL || "http://127.0.0.1:8080";
let cookie;
before(async () => {
  const r = await fetch(`${base}/api/demo`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "claim" }),
  });
  cookie = r.headers.get("set-cookie").split(";")[0];
  assert.equal(r.status, 200);
});
after(async () => {
  await fetch(`${base}/api/demo`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify({ action: "stop" }),
  });
});

async function api(path, body) {
  const response = await fetch(`${base}${path}`, {
    method: body ? "POST" : "GET",
    headers: {
      "Content-Type": "application/json",
      Cookie: cookie,
    },
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(5000),
  });
  assert.ok(
    response.ok,
    `HTTP ${response.status}: ${await response.clone().text()}`,
  );
  return response.json();
}

async function until(id, predicate, timeout = 45000) {
  const end = Date.now() + timeout;
  let job;
  while (Date.now() < end) {
    job = await api(`/api/jobs/${id}`);
    if (predicate(job)) return job;
    await delay(150);
  }
  assert.fail(`Job did not reach expected state: ${JSON.stringify(job)}`);
}

function compose(...args) {
  return execFileSync("docker", ["compose", ...args], {
    encoding: "utf8",
    timeout: 20000,
  });
}

test("only demo jobs enter the shared queue", async () => {
  const response = await fetch(`${base}/api/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify({ kind: "checksum", payload: { text: "abc" } }),
  });
  assert.equal(response.status, 400);
  const job = await api("/api/jobs", { kind: "demo", payload: { work_ms: 0 } });
  assert.equal(
    (await until(job.id, (j) => j.status === "succeeded")).attempt,
    1,
  );
});

test(
  "two worker processes share a batch without duplicate attempts",
  { timeout: 45000 },
  async () => {
    const jobs = await Promise.all(
      Array.from({ length: 12 }, () =>
        api("/api/jobs", {
          kind: "demo",
          payload: { work_ms: 400 },
          max_attempts: 3,
        }),
      ),
    );
    const done = await Promise.all(
      jobs.map((j) => until(j.id, (j) => j.status === "succeeded")),
    );
    assert.equal(new Set(done.map((j) => j.attempts[0].worker_id)).size, 2);
    for (const job of done) {
      assert.equal(job.attempt, 1);
      assert.equal(job.attempts.length, 1);
      assert.equal(job.result.completed, true);
    }
  },
);

test(
  "a surviving process reclaims work after SIGKILL",
  { timeout: 60000 },
  async () => {
    const created = await api("/api/jobs", {
      kind: "demo",
      payload: { work_ms: 8000 },
      max_attempts: 3,
      timeout_seconds: 30,
    });
    const running = await until(created.id, (j) => j.status === "running");
    const owner = running.worker_id;
    assert.ok(["worker-a", "worker-b"].includes(owner));
    const container = compose("ps", "-q", owner).trim();
    assert.ok(container);
    execFileSync("docker", ["update", "--restart=no", container], {
      timeout: 10000,
    });
    try {
      compose("kill", "-s", "SIGKILL", owner);
      const recovered = await until(
        created.id,
        (j) => j.status === "succeeded",
      );
      const workers = await api("/api/workers");
      assert.equal(workers.find((w) => w.id === owner).online, false);
      assert.equal(workers.find((w) => w.id !== owner).online, true);
      assert.equal(recovered.attempt, 2);
      assert.equal(recovered.attempts[0].status, "expired");
      assert.equal(recovered.attempts[1].status, "succeeded");
      assert.notEqual(recovered.attempts[1].worker_id, owner);
    } finally {
      execFileSync(
        "docker",
        ["update", "--restart=unless-stopped", container],
        { timeout: 10000 },
      );
      execFileSync("docker", ["start", container], { timeout: 20000 });
    }
  },
);

test(
  "API restart preserves completed job and attempt history",
  { timeout: 45000 },
  async () => {
    const created = await api("/api/jobs", {
      kind: "demo",
      payload: { work_ms: 0 },
    });
    const finished = await until(created.id, (j) => j.status === "succeeded");
    compose("restart", "api");
    const end = Date.now() + 15000;
    let ready = false;
    while (Date.now() < end) {
      try {
        const r = await fetch(`${base}/readyz`, {
          signal: AbortSignal.timeout(1000),
        });
        if (r.ok) {
          ready = true;
          break;
        }
      } catch {}
      await delay(200);
    }
    assert.ok(ready, "API did not become ready after restart");
    await api("/api/demo", { action: "claim" });
    const restored = await api(`/api/jobs/${created.id}`);
    assert.deepEqual(restored, finished);
  },
);

test(
  "database outage stops active work and workers recover after reconnecting",
  { timeout: 60000 },
  async () => {
    const created = await api("/api/jobs", {
      kind: "demo",
      payload: { work_ms: 12000 },
      max_attempts: 3,
      timeout_seconds: 30,
    });
    await until(created.id, (j) => j.status === "running");
    compose("pause", "postgres");
    try {
      const responses = await Promise.all([
        fetch(`${base}/readyz`, { signal: AbortSignal.timeout(8000) }),
        fetch(`${base}/api/jobs`, {
          signal: AbortSignal.timeout(8000),
        }),
      ]);
      for (const response of responses) assert.equal(response.status, 503);
      assert.deepEqual(await responses[0].json(), {
        error: "database unavailable",
      });
      assert.deepEqual(await responses[1].json(), {
        error: "service temporarily unavailable",
      });
      await delay(2000);
    } finally {
      compose("unpause", "postgres");
    }
    const recovered = await until(created.id, (j) => j.status === "succeeded");
    assert.equal(recovered.attempt, 2);
    assert.equal(recovered.attempts[0].status, "expired");
    assert.equal(recovered.attempts[1].status, "succeeded");
  },
);
