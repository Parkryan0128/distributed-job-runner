import { expect, test } from "@playwright/test";

const token = process.env.API_TOKEN || "local-runner-token-change-me";

async function login(page) {
  await page.goto("/");
  await page.getByLabel("API token").fill(token);
  await page.getByRole("button", { name: "Connect to runner" }).click();
  await expect(
    page.getByRole("heading", { name: "Execution overview" }),
  ).toBeVisible();
  await expect(page.locator("#connection")).toHaveText("Live");
}

async function submit(page, preset) {
  await page.getByRole("button", { name: "+ New job" }).click();
  await page.getByRole("button", { name: preset, exact: true }).click();
  await page.getByRole("button", { name: "Submit job" }).click();
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await expect(page.getByTestId("job-id")).toBeVisible();
  return page.getByTestId("job-id").textContent();
}

async function create(request, payload) {
  const response = await request.post("/api/jobs", {
    headers: { Authorization: `Bearer ${token}` },
    data: { kind: "demo", payload },
  });
  expect(response.status()).toBe(201);
  return response.json();
}

test("authentication, retry history, filters and disconnect", async ({
  page,
}, testInfo) => {
  await page.goto("/");
  await page.getByLabel("API token").fill("this-is-the-wrong-token");
  await page.getByRole("button", { name: "Connect to runner" }).click();
  await expect(page.locator("#login-error")).toHaveText(
    "valid bearer token required",
  );
  await login(page);
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
  await page.getByRole("button", { name: "Disconnect" }).click();
  await expect(page.getByLabel("API token")).toHaveValue("");
  await expect(page.getByTestId("job-id")).not.toBeVisible();
});

test("running work can be canceled and exhausted jobs show their errors", async ({
  page,
}) => {
  await login(page);
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

test("checksum task displays its actual result and rejects malformed JSON", async ({
  page,
}) => {
  await login(page);
  await page.getByRole("button", { name: "+ New job" }).click();
  await page.getByLabel("Task", { exact: true }).selectOption("checksum");
  await page.getByLabel("Payload", { exact: true }).fill("{");
  await page.getByRole("button", { name: "Submit job" }).click();
  await expect(page.locator("#submit-error")).toHaveText(
    "Payload must be valid JSON.",
  );
  await page.getByLabel("Payload", { exact: true }).fill('{"text":"abc"}');
  await page.getByRole("button", { name: "Submit job" }).click();
  await expect(page.locator("#detail-status")).toHaveText("Succeeded");
  await expect(page.locator("#result")).toContainText(
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  );
});

test("retrying a lost submission response reuses the same job", async ({
  page,
}) => {
  await login(page);
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
  await page.getByRole("button", { name: "Submit job" }).click();
  await expect(page.locator("#submit-error")).toContainText(
    "retry this submission safely",
  );
  await page.getByRole("button", { name: "Submit job" }).click();
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
  await login(page);
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
  await login(page);
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

test("temporary API failure recovers without losing the selected job", async ({
  page,
}) => {
  await login(page);
  const id = await submit(page, "Quick success");
  await expect(page.locator("#detail-status")).toHaveText("Succeeded");
  let unavailable = true;
  await page.route("**/api/stats", async (route) => {
    if (unavailable)
      return route.fulfill({
        status: 503,
        contentType: "text/html",
        body: "<h1>Service unavailable</h1>",
      });
    return route.continue();
  });
  await expect(page.locator("#connection")).toHaveText("Reconnecting");
  await expect(page.locator("#error")).toContainText("Request failed (503)");
  unavailable = false;
  await expect(page.locator("#connection")).toHaveText("Live");
  await expect(page.locator("#error")).toBeEmpty();
  await expect(page.getByTestId("job-id")).toHaveText(id);
});

test("disconnect discards an in-flight submission response", async ({
  page,
}) => {
  await login(page);
  let release;
  let committed;
  let intercepted;
  const blocked = new Promise((resolve) => {
    intercepted = resolve;
  });
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  let delivered;
  const finished = new Promise((resolve) => {
    delivered = resolve;
  });
  await page.route("**/api/jobs", async (route) => {
    if (route.request().method() !== "POST") return route.continue();
    const response = await route.fetch();
    committed = await response.json();
    intercepted();
    await gate;
    await route.fulfill({ response }).catch(() => {});
    delivered();
  });
  await page.getByRole("button", { name: "+ New job" }).click();
  await page
    .getByRole("button", { name: "Quick success", exact: true })
    .click();
  await page.getByRole("button", { name: "Submit job" }).click();
  await blocked;
  await page.getByRole("button", { name: "Close new job" }).click();
  await page.getByRole("button", { name: "Disconnect" }).click();
  release();
  await finished;
  await page.getByLabel("API token").fill(token);
  await page.getByRole("button", { name: "Connect to runner" }).click();
  await expect(page.locator("#connection")).toHaveText("Live");
  await expect(page.getByTestId("job-id")).not.toBeVisible();
  await expect(page.locator("#notice")).toBeEmpty();
  await expect(page.locator(`[data-job-id="${committed.id}"]`)).toBeVisible();
  await page.unroute("**/api/jobs");
  const next = await submit(page, "Quick success");
  expect(next).not.toBe(committed.id);
});

test("revoked API credentials return the console to login", async ({
  page,
}) => {
  await login(page);
  await submit(page, "Quick success");
  await page.route("**/api/stats", (route) =>
    route.fulfill({
      status: 401,
      contentType: "application/json",
      body: '{"error":"valid bearer token required"}',
    }),
  );
  await expect(page.getByLabel("API token")).toBeVisible();
  await expect(page.locator("#login-error")).toContainText(
    "token is no longer valid",
  );
  await expect(page.getByTestId("job-id")).not.toBeVisible();
  await page.unroute("**/api/stats");
  await page.getByLabel("API token").fill(token);
  await page.getByRole("button", { name: "Connect to runner" }).click();
  await expect(page.locator("#connection")).toHaveText("Live");
});
