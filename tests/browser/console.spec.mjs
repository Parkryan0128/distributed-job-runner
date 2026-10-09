import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page, request }) => {
  const r = await request.post("/api/demo", { data: { action: "claim" } });
  expect(r.status()).toBe(200);
  await page.context().addCookies((await request.storageState()).cookies);
});
test.afterEach(async ({ request }) => {
  await request.post("/api/demo", { data: { action: "stop" } });
});

async function openConsole(page) {
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Queue monitor" }),
  ).toBeVisible();
  await expect(page.getByRole("button", { name: "+ New job" })).toBeEnabled();
}

async function submit(page, preset) {
  await page.getByRole("button", { name: "+ New job" }).click();
  await page.getByRole("button", { name: preset, exact: true }).click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(page.getByTestId("job-id")).toBeVisible();
  return page.getByTestId("job-id").textContent();
}

async function create(request, payload) {
  const response = await request.post("/api/jobs", {
    data: { kind: "demo", payload },
  });
  expect(response.status()).toBe(201);
  return response.json();
}

test("console opens directly and shows retry history and filters", async ({
  page,
}, testInfo) => {
  await openConsole(page);
  const id = await submit(page, "Retry twice");
  await expect(page.locator("#detail-status")).toHaveText("Succeeded");
  await expect(page.locator("#attempts li")).toHaveCount(3);
  await expect(page.locator("#attempts li").nth(0)).toContainText("Failed");
  await expect(page.locator("#attempts li").nth(2)).toContainText("Succeeded");
  await expect(page.locator("#result")).toContainText('"attempt": 3');
  await page.getByLabel("Status", { exact: true }).selectOption("succeeded");
  await expect(page.locator(`[data-job-id="${id}"]`)).toBeVisible();
  await page.screenshot({
    path: testInfo.outputPath("overview.png"),
    fullPage: true,
  });
});

test("running work can be canceled and exhausted jobs show their errors", async ({
  page,
}) => {
  await openConsole(page);
  await submit(page, "Long job");
  await expect(page.locator("#detail-status")).toHaveText("Running");
  await page.getByRole("button", { name: "Cancel job" }).click();
  await expect(page.locator("#detail-status")).toHaveText("Canceled");
  await expect(page.getByRole("button", { name: "Cancel job" })).toBeDisabled();
  await expect(page.locator("#attempts li")).toContainText("Canceled");
  await submit(page, "Exhaust retries");
  await expect(page.locator("#detail-status")).toHaveText("Dead letter");
  await expect(page.locator("#attempts li")).toHaveCount(3);
  await expect(page.locator("#failure")).toContainText(
    "simulated failure on attempt 3",
  );
});

test("only demo task inputs are exposed", async ({ page, request }) => {
  await openConsole(page);
  await page.getByRole("button", { name: "+ New job" }).click();
  await expect(page.getByLabel("Task time (s)")).toBeVisible();
  await expect(page.locator("#kind, #queue, #job-payload")).toHaveCount(0);
  const invalid = await request.post("/api/jobs", {
    data: { kind: "checksum", payload: { text: "abc" } },
  });
  expect(invalid.status()).toBe(400);
});

test("a failed cancellation stays visible after polling and can be retried", async ({
  page,
}) => {
  await openConsole(page);
  await submit(page, "Long job");
  await expect(page.locator("#detail-status")).toHaveText("Running");
  await page.route("**/cancel", (route) =>
    route.fulfill({
      status: 503,
      contentType: "application/json",
      body: '{"error":"service temporarily unavailable"}',
    }),
  );
  await page.getByRole("button", { name: "Cancel job" }).click();
  await expect(page.getByRole("button", { name: "Cancel job" })).toBeEnabled();
  await expect(page.getByRole("button", { name: "+ New job" })).toBeEnabled();
  await expect(page.locator("#action-error")).toHaveText(
    "service temporarily unavailable",
  );
  await page.unroute("**/cancel");
  await page.getByRole("button", { name: "Cancel job" }).click();
  await expect(page.locator("#detail-status")).toHaveText("Canceled");
  await expect(page.locator("#action-error")).toBeEmpty();
});

test("retrying a lost submission response reuses the same job", async ({
  page,
}) => {
  await openConsole(page);
  let committed;
  const keys = [];
  await page.route("**/api/jobs", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    keys.push(route.request().headers()["idempotency-key"]);
    const response = await route.fetch();
    if (!committed) {
      expect(response.status()).toBe(201);
      committed = await response.json();
      await route.abort("failed");
    } else {
      expect(response.status()).toBe(200);
      await route.fulfill({ response });
    }
  });
  await page.getByRole("button", { name: "+ New job" }).click();
  await page
    .getByRole("button", { name: "Quick success", exact: true })
    .click();
  await expect(page.locator("#submit-error")).toContainText(
    "retry this submission safely",
  );
  await page
    .getByRole("button", { name: "Quick success", exact: true })
    .click();
  await expect(page.getByTestId("job-id")).toHaveText(committed.id);
  expect(keys).toHaveLength(2);
  expect(keys[0]).toBe(keys[1]);
});

test("a delayed previous selection cannot overwrite the current job", async ({
  page,
  request,
}) => {
  const old = await create(request, { work_ms: 0 });
  const current = await create(request, { work_ms: 0 });
  await openConsole(page);
  let release;
  let intercepted;
  const blocked = new Promise((resolve) => {
    intercepted = resolve;
  });
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  await page.route(`**/api/jobs/${old.id}`, async (route) => {
    const response = await route.fetch();
    intercepted();
    await gate;
    await route.fulfill({ response }).catch(() => {});
  });
  await page
    .getByRole("button", { name: `Open job ${old.id.slice(0, 8)}` })
    .click();
  await blocked;
  await page
    .getByRole("button", { name: `Open job ${current.id.slice(0, 8)}` })
    .click();
  await expect(page.getByTestId("job-id")).toHaveText(current.id);
  release();
  await expect(page.getByTestId("job-id")).toHaveText(current.id);
});

test("mobile console and composer fit the viewport", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openConsole(page);
  await page.getByRole("button", { name: "+ New job" }).click();
  await expect(page.getByRole("dialog")).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  const dialog = await page.getByRole("dialog").boundingBox();
  expect(dialog.x).toBeGreaterThanOrEqual(0);
  expect(dialog.x + dialog.width).toBeLessThanOrEqual(390);
  await page.screenshot({
    path: testInfo.outputPath("mobile.png"),
    fullPage: true,
  });
});

test("SSE reconnect restores the selected job", async ({ page, context }) => {
  await openConsole(page);
  const id = await submit(page, "Quick success");
  await expect(page.locator("#detail-status")).toHaveText("Succeeded");
  await context.setOffline(true);
  await expect(page.getByRole("button", { name: "+ New job" })).toBeDisabled();
  await expect(page.locator("#workload-state")).toHaveText("Reconnecting…");
  await context.setOffline(false);
  await expect(page.getByRole("button", { name: "+ New job" })).toBeEnabled();
  await expect(page.getByTestId("job-id")).toHaveText(id);
});

test("the console shows four jobs running concurrently", async ({
  page,
  request,
}) => {
  const jobs = await Promise.all(
    Array.from({ length: 4 }, () => create(request, { work_ms: 6000 })),
  );
  await openConsole(page);
  for (const job of jobs) {
    await expect(page.locator(`[data-job-id="${job.id}"]`)).toContainText(
      "Running",
    );
  }
  await expect(page.locator("#count-running")).toHaveText("4");
  for (const job of jobs) {
    await expect(page.locator(`[data-job-id="${job.id}"]`)).toContainText(
      "Succeeded",
    );
  }
});

test("server workload generates real retries and stops arrivals", async ({
  page,
  request,
}) => {
  await openConsole(page);
  const total = async () =>
    Object.values(await (await request.get("/api/stats")).json()).reduce(
      (a, b) => a + b,
      0,
    );
  const initial = await total();
  await page.getByRole("button", { name: "High", exact: true }).click();
  await page.getByRole("button", { name: "Start", exact: true }).click();
  await expect.poll(total).toBeGreaterThanOrEqual(initial + 6);
  await page.getByRole("button", { name: "Stop", exact: true }).click();
  const count = await total();
  await page.waitForTimeout(1400);
  expect(await total()).toBe(count);
  const jobs = (await (await request.get("/api/jobs?limit=10")).json()).jobs;
  const retry = jobs.find((j) => j.payload.fail_until === 1);
  expect(retry).toBeTruthy();
  await expect
    .poll(
      async () =>
        (await (await request.get(`/api/jobs/${retry.id}`)).json()).status,
    )
    .toBe("succeeded");
});

test("a job moves from shared queue to its worker and leaves on completion", async ({
  page,
  request,
}, testInfo) => {
  const response = await request.post("/api/jobs", {
    data: { kind: "demo", payload: { work_ms: 5000 }, delay_seconds: 4 },
  });
  const job = await response.json();
  await openConsole(page);
  const block = `[data-activity-job="${job.id}"]`;
  await expect(page.locator(`#pending-jobs ${block}`)).toBeVisible();
  await page.locator(`#pending-jobs ${block}`).click();
  await expect(page.getByTestId("job-id")).toHaveText(job.id);
  await expect(page.locator(`#workers ${block}`)).toBeVisible();
  await expect(page.locator(`#pending-jobs ${block}`)).toHaveCount(0);
  await page.screenshot({
    path: testInfo.outputPath("activity.png"),
    fullPage: true,
  });
  await expect(page.locator("#detail-status")).toHaveText("Succeeded");
  await expect(page.locator(block)).toHaveCount(0);
  await expect(page.locator(`[data-job-id="${job.id}"]`)).toContainText(
    "Succeeded",
  );
});

test("custom task duration and failure inputs are submitted correctly", async ({
  page,
}) => {
  await openConsole(page);
  await page.getByRole("button", { name: "+ New job" }).click();
  await page.getByLabel("Task time (s)").fill("1.2");
  await page.getByLabel("Fail first attempts").fill("1");
  await page.getByRole("button", { name: "Submit job" }).click();
  await expect(page.locator("#detail-status")).toHaveText("Succeeded");
  await expect(page.locator("#attempts li")).toHaveCount(2);
  await expect(page.locator("#result")).toContainText('"work_ms": 1200');
  await expect(page.locator("#payload")).toHaveCount(0);
});

test("another visitor observes but cannot control or submit; ownership transfers on release", async ({
  page,
  request,
  browser,
}) => {
  await openConsole(page);
  const spectator = await browser.newContext();
  const second = await spectator.newPage();
  try {
    await second.goto("/");
    await expect(second.locator("#workload-state")).toContainText(
      "Another visitor",
    );
    await expect(second.locator("#toggle-workload")).toBeDisabled();
    await expect(second.locator("#new-job")).toBeDisabled();
    expect(
      (
        await spectator.request.post("/api/demo", {
          data: { action: "start", level: "high" },
        })
      ).status(),
    ).toBe(409);
    expect(
      (
        await spectator.request.post("/api/jobs", {
          data: { kind: "demo", payload: {} },
        })
      ).status(),
    ).toBe(409);
    const job = await create(request, { work_ms: 0 });
    await expect(second.locator(`[data-job-id="${job.id}"]`)).toContainText(
      "Succeeded",
    );
    await page.getByRole("button", { name: "Release", exact: true }).click();
    await expect(second.locator("#toggle-workload")).toBeEnabled();
    await second.locator("#toggle-workload").click();
    await expect(page.locator("#toggle-workload")).toBeDisabled();
    await second.getByRole("button", { name: "Stop", exact: true }).click();
  } finally {
    await spectator.request.post("/api/demo", { data: { action: "stop" } });
    await spectator.close();
  }
});

test("zero-duration jobs still animate through queue and worker", async ({
  page,
  request,
}) => {
  await openConsole(page);
  const job = await create(request, { work_ms: 0 });
  const block = `[data-activity-job="${job.id}"]`;
  await expect(page.locator(`#pending-jobs ${block}`)).toBeVisible();
  await expect(page.locator(`#workers ${block}`)).toBeVisible();
  await expect(page.locator(`${block}.completed-job`)).toBeVisible();
  await expect(page.locator(block)).toHaveCount(0);
});

test("presets submit immediately without changing custom inputs", async ({
  page,
  request,
}) => {
  await openConsole(page);
  const lease = await (await request.get("/api/demo")).json();
  expect(lease.remaining_ms).toBeLessThanOrEqual(30000);
  await page.locator("#new-job").click();
  await page.getByLabel("Task time (s)").fill("7");
  await page.getByLabel("Fail first attempts").fill("4");
  await page
    .getByRole("button", { name: "Quick success", exact: true })
    .click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(page.getByTestId("job-id")).toBeVisible();
  const id = await page.getByTestId("job-id").textContent();
  const job = await (await request.get(`/api/jobs/${id}`)).json();
  expect(job.payload).toEqual({ work_ms: 1000, fail_until: 0 });
  await page.locator("#new-job").click();
  await expect(
    page.getByRole("heading", { name: "Custom task" }),
  ).toBeVisible();
  await expect(page.getByLabel("Task time (s)")).toHaveValue("7");
  await expect(page.getByLabel("Fail first attempts")).toHaveValue("4");
});

test("shared queue stays on one row and counts hidden jobs at different widths", async ({
  page,
  request,
}) => {
  const ids = [];
  try {
    for (let i = 0; i < 15; i++) {
      const r = await request.post("/api/jobs", {
        data: { kind: "demo", payload: { work_ms: 0 }, delay_seconds: 120 },
      });
      expect(r.status()).toBe(201);
      ids.push((await r.json()).id);
    }
    await openConsole(page);
    for (const width of [1440, 390]) {
      await page.setViewportSize({ width, height: 1000 });
      await expect(page.locator(".queue-overflow")).toBeVisible();
      await expect
        .poll(async () =>
          page.locator("#pending-jobs").evaluate((el) => {
            const blocks = [...el.children];
            return (
              el.scrollWidth <= el.clientWidth &&
              Math.max(...blocks.map((b) => b.getBoundingClientRect().bottom)) -
                Math.min(...blocks.map((b) => b.getBoundingClientRect().top)) <=
                36
            );
          }),
        )
        .toBe(true);
      await expect
        .poll(() =>
          page.locator("#pending-jobs").evaluate((el) => {
            const visible = el.querySelectorAll(".job-block").length;
            return (
              el.querySelector(".queue-overflow")?.textContent ===
              `+${15 - visible} more`
            );
          }),
        )
        .toBe(true);
    }
  } finally {
    for (const id of ids) await request.post(`/api/jobs/${id}/cancel`);
  }
});

test("worker updates and other jobs preserve the running block", async ({
  page,
  request,
}) => {
  await page.addInitScript(() => {
    window.moves = [];
    const animate = Element.prototype.animate;
    Element.prototype.animate = function (frames, options) {
      if (
        this.dataset.activityJob &&
        frames[0]?.transform?.startsWith("translate(")
      )
        window.moves.push({
          id: this.dataset.activityJob,
          from: frames[0].transform,
        });
      return animate.call(this, frames, options);
    };
  });
  await openConsole(page);
  const job = await create(request, { work_ms: 6000 });
  const selector = `#workers [data-activity-job="${job.id}"]`;
  await expect(page.locator(selector)).toBeVisible();
  const original = await page.locator(selector).elementHandle();
  await create(request, { work_ms: 0 });
  await page.waitForTimeout(1200);
  expect(
    await original.evaluate(
      (el) => el.isConnected && document.getElementById(el.id) === el,
    ),
  ).toBe(true);
  const moves = await page.evaluate(
    (id) => window.moves.filter((m) => m.id === id),
    job.id,
  );
  expect(moves.length).toBeGreaterThanOrEqual(1);
  // Other jobs leaving can legitimately shift this block. Once settled, idle
  // worker heartbeats must preserve the same animation count and DOM node.
  await page.waitForTimeout(1200);
  const afterHeartbeat = await page.evaluate(
    (id) => window.moves.filter((m) => m.id === id).length,
    job.id,
  );
  expect(afterHeartbeat).toBe(moves.length);
  expect(moves[0].from).not.toBe("translate(0px, 0px)");
  await expect
    .poll(() => page.locator(`${selector}.completed-job`).count(), {
      intervals: [20],
    })
    .toBe(1);
  await expect(page.locator(`${selector}.completed-job`)).toHaveCSS(
    "animation-duration",
    "0.4s",
  );
  await expect
    .poll(() => page.locator(selector).count(), {
      intervals: [20],
      timeout: 1000,
    })
    .toBe(0);
});
