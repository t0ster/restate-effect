// End to end: serve the example under Node and under Bun, register it with a real Restate server,
// and run the example client against it. Restate runs in Docker on the host network.
import { type ChildProcess, spawn, spawnSync } from "node:child_process"
import { after, before, describe, test } from "node:test"
import assert from "node:assert/strict"

const admin = "http://127.0.0.1:29070"
const ingress = "http://127.0.0.1:28080"
const restateEnv = {
  RESTATE_CLUSTER_NAME: "restate-effect-e2e",
  RESTATE_BIND_ADDRESS: "127.0.0.1:25122",
  RESTATE_ADVERTISED_ADDRESS: "http://127.0.0.1:25122/",
  RESTATE_ADMIN__BIND_ADDRESS: "127.0.0.1:29070",
  RESTATE_INGRESS__BIND_ADDRESS: "127.0.0.1:28080"
}

/** A child process whose output is kept for assertions and failure reports. */
const start = (command: string, args: ReadonlyArray<string>, env: Record<string, string> = {}, cwd?: string) => {
  const child = spawn(command, args, { cwd, env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] })
  let output = ""
  child.stdout.on("data", chunk => { output += chunk })
  child.stderr.on("data", chunk => { output += chunk })
  const exited = new Promise<number | null>(resolve => child.once("exit", code => resolve(code)))
  return { child, exited, output: () => output }
}

const stop = async (process: { child: ChildProcess; exited: Promise<number | null> }) => {
  if (process.child.exitCode === null) process.child.kill("SIGINT")
  await Promise.race([process.exited, new Promise(resolve => setTimeout(resolve, 5000))])
  if (process.child.exitCode === null && process.child.signalCode === null) {
    process.child.kill("SIGKILL")
    await process.exited
  }
}

const waitFor = async (what: string, ready: () => Promise<boolean>, seconds = 60) => {
  for (let i = 0; i < seconds * 4; i++) {
    if (await ready().catch(() => false)) return
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  throw new Error(`Timed out waiting for ${what}`)
}

const container = `restate-effect-e2e-${process.pid}`
/** A JSON call through Restate ingress. */
const call = async (path: string, body: unknown) => {
  const response = await fetch(`${ingress}/${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) })
  assert.ok(response.ok, `${path}: ${response.status} ${await response.clone().text()}`)
  return response.json() as Promise<unknown>
}
const albumsOf = async (owner: string) => await call(`Profile/${owner}/albums`, null) as ReadonlyArray<string>
/** Registers the endpoint on this port with Restate. */
const register = async (port: number) => {
  const registration = await fetch(`${admin}/deployments`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ uri: `http://127.0.0.1:${port}`, force: true })
  })
  // 201 for a new deployment; 200 when it replaces the one at this URI.
  assert.ok(registration.status === 201 || registration.status === 200, `${registration.status}: ${await registration.text()}`)
}

let restate: ReturnType<typeof start>

before(async () => {
  restate = start("docker", [
    "run", "--rm", "--name", container, "--network", "host",
    ...Object.entries(restateEnv).flatMap(([k, v]) => ["-e", `${k}=${v}`]),
    "restatedev/restate:1.8.1"
  ])
  await waitFor("Restate", async () => (await fetch(`${admin}/health`)).ok, 120)
})

after(async () => {
  spawnSync("docker", ["rm", "--force", container], { stdio: "ignore" })
  await stop(restate)
})

for (const [runtime, port] of [["node", 29180], ["bun", 29181]] as const) {
  void test(`the example runs on ${runtime}`, { timeout: 120_000 }, async () => {
    const server = start(runtime, ["example/server.ts"], { PORT: String(port) })
    try {
      await waitFor(`the ${runtime} endpoint`, async () => server.output().includes("Listening on"), 30)
      await register(port)

      const client = start(runtime, ["example/client.ts"], { RESTATE_INGRESS_URL: ingress })
      const code = await client.exited
      const output = client.output()
      assert.equal(code, 0, `client failed:\n${output}\n--- endpoint:\n${server.output()}`)
      for (const line of [
        /ada: welcomed \(msg-\d+\)/,          // workflow run, then its shared status handler
        /Name taken: admin/,                   // declared errors arrive typed
        /mail rejected: blocked@example\.com/,
        /bug: RestateClientError: Signup\/run failed/, // a defect stays opaque
        /offline: failed/,                     // runRetry exhausted: the signup fails, onError ran
        /not published: dog\.bad/,             // nested durable concurrency
        /album: \{/,                           // one-way start, then attach by invocation ID
        /ada's albums: \[ ["']album-\d+/            // console.log quotes differ: Node ', Bun "
      ]) assert.match(output, line)

      // Cancellation: an album with a RAW photo is still developing it when cancelled. Its
      // Restate.onError compensation (durable) takes the album off the profile before it ends.
      const albumId = `album-cancelled-on-${runtime}`
      const id = await call("Albums/create", { albumId, owner: "ada", photos: ["night.raw"] }) as string
      await waitFor("the album to start", async () => (await albumsOf("ada")).includes(albumId), 30)
      const cancel = await fetch(`${admin}/invocations/${id}/cancel`, { method: "PATCH" })
      assert.equal(cancel.status, 202, await cancel.text())
      const attached = await fetch(`${ingress}/restate/invocation/${id}/attach`)
      assert.equal(attached.status, 409)
      assert.equal((await attached.json() as { message: string }).message, "Cancelled")
      assert.ok(!(await albumsOf("ada")).includes(albumId), "onError removed the cancelled album")
    } finally {
      await stop(server)
    }
  })
}

// Restate.run retry.delay: a declared failure sets the wait before the next attempt (fixture: test/retries.ts).
void test("a retried failure's delay overrides the backoff", { timeout: 120_000 }, async () => {
  const port = 29182
  const server = start("node", ["test/retries.ts"], { PORT: String(port) })
  try {
    await waitFor("the retries endpoint", async () => server.output().includes("Listening on"), 30)
    await register(port)
    const attempts = async (id: string) => {
      const started = await call("Retries/attempts", id) as ReadonlyArray<number>
      return started.slice(1).map((time, i) => time - started[i]!)
    }
    const backoff = 2000   // the fixture's runRetry: initialInterval "2 seconds", factor 1

    // Delays of 300ms, then none (the backoff): the third attempt succeeds.
    assert.equal(await call("Retries/call", { id: "delay-then-backoff", retryAfter: [300, null], maxAttempts: 5 }), 3)
    const [delayed, backedOff] = await attempts("delay-then-backoff")
    assert.ok(delayed! >= 300 && delayed! < backoff, `delayed retry after ${delayed}ms`)
    assert.ok(backedOff! >= backoff, `backoff retry after ${backedOff}ms`)
    assert.match(server.output(), /Restate\.run 'call' failed; retrying in 300ms/)   // the SDK logs only backoff retries

    // maxAttempts still bounds delayed retries; the last failure is returned as the declared error.
    const response = await fetch(`${ingress}/Retries/call`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: "exhausted", retryAfter: [100, 100, 100, 100, 100], maxAttempts: 3 })
    })
    const body = await response.json() as { message: string; metadata?: { effectError?: string } }
    assert.equal(response.ok, false)
    assert.equal(body.message, "RateLimited", `${JSON.stringify(body)}\n--- endpoint:\n${server.output()}`)
    const failure = JSON.parse(body.metadata!.effectError!) as { _tag: string; attempt: number; retryAfter: unknown }
    assert.equal(failure._tag, "RateLimited")
    assert.equal(failure.attempt, 3)
    assert.ok(failure.retryAfter !== undefined, "the failure keeps its retryAfter")
    const gaps = await attempts("exhausted")
    assert.equal(gaps.length, 2, "three attempts")
    for (const gap of gaps) assert.ok(gap >= 100 && gap < backoff, `delayed retry after ${gap}ms`)
  } finally {
    await stop(server)
  }
})

// Restate.ensuring, Restate.addFinalizer, one-way sends and Restate.cancel (fixture: test/cleanup.ts).
void describe("durable cleanup", () => {
  const port = 29185
  let server: ReturnType<typeof start>
  before(async () => {
    server = start("node", ["test/cleanup.ts"], { PORT: String(port) })
    await waitFor("the cleanup endpoint", async () => server.output().includes("Listening on"), 30)
    await register(port)
  })
  after(() => stop(server))

  const recorded = async (id: string) => await call("Cleanup/events", id) as ReadonlyArray<string>
  /** What ran, without the attempts. */
  const ran = async (id: string) => (await recorded(id)).filter(e => e !== "attempt")
  const scenario = async (id: string, scenario: string, handler = "run") => {
    const response = await fetch(`${ingress}/Cleanup/${handler}`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ id, scenario })
    })
    return { status: response.status, body: await response.json() as unknown }
  }
  const started = async (id: string, scenario: string) => {
    const response = await fetch(`${ingress}/Cleanup/run/send`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ id, scenario })
    })
    assert.ok(response.ok)
    return (await response.json() as { invocationId: string }).invocationId
  }

  void test("success: the cleanup runs after the effect, the finalizer at the end", { timeout: 60_000 }, async () => {
    assert.deepEqual(await scenario("success", "success"), { status: 200, body: "ok" })
    assert.deepEqual(await ran("success"), ["body", "cleanup", "final:success"])
  })

  void test("a declared failure runs both", { timeout: 60_000 }, async () => {
    const { status, body } = await scenario("failure", "failure")
    assert.equal(status, 500)
    assert.equal((body as { message: string }).message, "Refused")
    assert.deepEqual(await ran("failure"), ["body", "cleanup", "final:failure"])
  })

  for (const how of ["admin API", "Restate.cancel"] as const) {
    void test(`cancellation (${how}) runs both`, { timeout: 60_000 }, async () => {
      const id = `cancelled-${how === "admin API" ? "admin" : "api"}`
      const invocation = await started(id, "cancelled")
      await waitFor("the body", async () => (await ran(id)).includes("body"), 30)
      if (how === "admin API") {
        const response = await fetch(`${admin}/invocations/${invocation}/cancel`, { method: "PATCH" })
        assert.equal(response.status, 202)
      } else {
        await call("Cleanup/cancel", invocation)
      }
      const attached = await fetch(`${ingress}/restate/invocation/${invocation}/attach`)
      assert.equal(attached.status, 409)
      assert.deepEqual(await ran(id), ["body", "cleanup", "final:interrupted"])
    })
  }

  void test("after a caught defect, the cleanup is deferred to the end of the invocation", { timeout: 60_000 }, async () => {
    assert.deepEqual(await scenario("caught", "caught"), { status: 200, body: "caught" })
    assert.deepEqual(await ran("caught"), ["body", "after", "cleanup", "final:success"])
  })

  void test("when only the attempt ends, nothing runs; the retry runs them once", { timeout: 60_000 }, async () => {
    assert.deepEqual(await scenario("retried", "retried"), { status: 200, body: "retried" })
    const events = await recorded("retried")
    assert.ok(events.filter(e => e === "attempt").length >= 2, `retried: ${events.join(", ")}`)
    assert.deepEqual(events.filter(e => e !== "attempt"), ["body", "cleanup", "final:success"])
    // Nothing between the two attempts: the first attempt's deferred cleanup never ran.
    assert.deepEqual(events.slice(0, 3), ["attempt", "body", "attempt"])
  })

  void test("a ClientError with a non-terminal cause ends only the attempt: nothing runs until the retry ends", { timeout: 60_000 }, async () => {
    assert.deepEqual(await scenario("transient", "transient"), { status: 200, body: "transient" })
    const events = await recorded("transient")
    assert.deepEqual(events.filter(e => e !== "attempt"), ["body", "cleanup", "final:success"])
    assert.deepEqual(events.slice(0, 3), ["attempt", "body", "attempt"])
  })

  // The 0.7.1 bug: cleanup must run on the final end, never on an attempt Restate retries.
  void test("after a retried attempt, a final failure or a cancellation runs the cleanup once", { timeout: 60_000 }, async () => {
    const failed = await scenario("retried-failure", "retried-failure")
    assert.equal(failed.status, 500)
    assert.equal((failed.body as { message: string }).message, "Refused")
    const failure = await recorded("retried-failure")
    assert.deepEqual(failure.slice(0, 3), ["attempt", "body", "attempt"], "nothing ran after the retried attempt")
    assert.deepEqual(failure.filter(e => e !== "attempt"), ["body", "cleanup", "final:failure"])

    const invocation = await started("retried-cancelled", "retried-cancelled")
    await waitFor("the retry", async () => (await recorded("retried-cancelled")).filter(e => e === "attempt").length >= 2, 30)
    assert.deepEqual(await ran("retried-cancelled"), ["body"], "nothing ran after the retried attempt")
    await call("Cleanup/cancel", invocation)
    assert.equal((await fetch(`${ingress}/restate/invocation/${invocation}/attach`)).status, 409)
    assert.deepEqual(await ran("retried-cancelled"), ["body", "cleanup", "final:interrupted"])
  })

  void test("a defect with onDefect: \"fail\" runs both at once", { timeout: 60_000 }, async () => {
    assert.equal((await scenario("crash", "crash", "crash")).status, 500)
    assert.deepEqual(await ran("crash"), ["body", "cleanup", "final:failure"])
  })

  void test("a later ensuring with the same key runs the deferred cleanup before its body", { timeout: 60_000 }, async () => {
    assert.deepEqual(await scenario("keyed", "keyed"), { status: 200, body: "keyed" })
    assert.deepEqual(await ran("keyed"), ["first", "between", "cleanup:first", "second", "cleanup:second", "final:success"])
  })

  void test("a one-way send through Restate.fork goes out after Effect.timeout", { timeout: 60_000 }, async () => {
    assert.deepEqual(await scenario("timeout", "timeout"), { status: 200, body: "timed out" })
    await waitFor("the send", async () => (await ran("timeout")).includes("sent"), 30)
    assert.deepEqual(await ran("timeout"), ["sent"])
  })
})
