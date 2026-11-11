// End to end: serve the example under Node and under Bun, register it with a real Restate server,
// and run the example client against it. Restate runs in Docker on the host network.
import { type ChildProcess, spawn, spawnSync } from "node:child_process"
import { after, before, describe, test } from "node:test"
import assert from "node:assert/strict"

const admin = "http://127.0.0.1:29070"
const ingress = "http://127.0.0.1:28080"
const restateEnv = {
  RESTATE_BIND_ADDRESS: "127.0.0.1:25122",
  RESTATE_ADVERTISED_ADDRESS: "http://127.0.0.1:25122/",
  RESTATE_ADMIN__BIND_ADDRESS: "127.0.0.1:29070",
  RESTATE_INGRESS__BIND_ADDRESS: "127.0.0.1:28080"
}

/** A child process whose output is kept for assertions and failure reports. */
const start = (command: string, args: ReadonlyArray<string>, env: Record<string, string> = {}) => {
  const child = spawn(command, args, { env: { ...process.env, ...env }, stdio: ["ignore", "pipe", "pipe"] })
  let output = ""
  child.stdout.on("data", chunk => { output += chunk })
  child.stderr.on("data", chunk => { output += chunk })
  const exited = new Promise<number | null>(resolve => child.once("exit", code => resolve(code)))
  return { child, exited, output: () => output }
}

const stop = async (process: { child: ChildProcess; exited: Promise<number | null> }) => {
  if (process.child.exitCode === null) process.child.kill("SIGINT")
  await Promise.race([process.exited, new Promise(resolve => setTimeout(resolve, 5000))])
  if (process.child.exitCode === null) process.child.kill("SIGKILL")
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
  assert.equal(registration.status, 201, await registration.text())
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

// Restate.Semaphore (fixture: test/semaphore.ts). Holders hold a permit until the test opens their
// gate (an awakeable); the fixture records when each one entered and left its permit.
// On Node, every test; on Bun, the tests marked "(also on Bun)". Each runtime has services of its own.
const semaphoreTests = (runtime: "node" | "bun", port: number) => describe(`Restate.Semaphore on ${runtime}`, () => {
  const prefix = `e2e-${runtime}`
  const holders = `${prefix}.Holders`
  const it = (name: string, options: { timeout: number }, fn: () => Promise<void>) => {
    if (runtime === "node" || name.includes("(also on Bun)")) void test(name, options, fn)
  }
  let server: ReturnType<typeof start>
  before(async () => {
    server = start(runtime, ["test/semaphore.ts"], { PORT: String(port), PREFIX: prefix })
    await waitFor("the semaphore endpoint", async () => server.output().includes("Listening on"), 30)
    await register(port)
  })
  after(() => stop(server))

  type Event = { readonly id: string; readonly event: "enter" | "exit" | "died"; readonly at: number }
  const events = async (ids: ReadonlyArray<string>) =>
    (await call(`${holders}/events`, null) as ReadonlyArray<Event>).filter(e => ids.includes(e.id))
  const entered = async (id: string) => (await events([id])).some(e => e.event === "enter")
  /** Starts a holder without waiting for it; returns its invocation ID, which is its holder ID. */
  const hold = async (semaphore: string, id: string, options: { until?: "opened" | number; fail?: boolean; flaky?: boolean; nested?: boolean; die?: "caught" | "uncaught" | "fatal"; timeout?: number } = {}, handler: "hold" | "crash" = "hold") => {
    const response = await fetch(`${ingress}/${holders}/${handler}/send`, {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ semaphore, id, until: "opened", ...options })
    })
    assert.ok(response.ok, `hold ${id}: ${response.status} ${await response.clone().text()}`)
    return (await response.json() as { invocationId: string }).invocationId
  }
  const open = async (id: string) => assert.equal(await call(`${holders}/open`, id), true, `${id} has a gate`)
  /** How an invocation ended: its HTTP status and body, once it has. */
  const outcome = async (invocationId: string) => {
    const response = await fetch(`${ingress}/restate/invocation/${invocationId}/attach`)
    return { status: response.status, body: await response.json() as unknown }
  }
  const manage = async (invocationId: string, action: "cancel" | "kill") => {
    const response = await fetch(`${admin}/invocations/${invocationId}/${action}`, { method: "PATCH" })
    assert.ok(response.ok, `${action}: ${response.status} ${await response.text()}`)
  }
  const query = async (sql: string) => {
    const response = await fetch(`${admin}/query`, {
      method: "POST", headers: { "content-type": "application/json", accept: "application/json" }, body: JSON.stringify({ query: sql })
    })
    assert.ok(response.ok, `query: ${response.status} ${await response.clone().text()}`)
    return (await response.json() as { rows: ReadonlyArray<Record<string, unknown>> }).rows
  }
  /** A semaphore's state in Restate: who holds a permit, grants so far, and who waits, in order. */
  const semaphore = async (name: string, service = `${prefix}.Semaphores`) => {
    const rows = await query(`SELECT key, value_utf8 FROM state WHERE service_name = '${service}' AND service_key = '${name}'`)
    const values = new Map(rows.map(row => [row.key as string, JSON.parse(row.value_utf8 as string)]))
    const ledger = values.get("ledger") as { holders: ReadonlyArray<{ holder: string }>; grants: number } | undefined
    const waiting = [...values.entries()]
      .filter(([key]) => key.startsWith("waiter:"))
      .sort(([a], [b]) => Number(a.slice("waiter:".length)) - Number(b.slice("waiter:".length)))
      .map(([, waiter]) => (waiter as { holder: string }).holder)
    return { holders: ledger?.holders.map(h => h.holder) ?? [], grants: ledger?.grants ?? 0, waiting }
  }
  const waits = (name: string, holder: string) => waitFor(`${holder} to wait`, async () => (await semaphore(name)).waiting.includes(holder), 30)
  /** Waits until nobody holds or waits, once the last release (a one-way send) has arrived; returns the grants so far. */
  const idle = async (name: string) => {
    await waitFor(`${name} to be idle`, async () => {
      const { holders, waiting } = await semaphore(name)
      return holders.length === 0 && waiting.length === 0
    }, 30)
    return (await semaphore(name)).grants
  }
  /** How many lease expiries of a semaphore Restate still has scheduled. */
  const scheduledExpiries = async (name: string) => (await query(
    `SELECT count(*) AS n FROM sys_invocation WHERE target_service_name = '${prefix}.Semaphores' AND target_service_key = '${name}' AND target_handler_name = 'expire' AND status = 'scheduled'`
  ))[0]!.n as number
  /** The semaphore handlers an invocation sent to, with how often. */
  const sentBy = async (invocationId: string) => Object.fromEntries((await query(
    `SELECT target_handler_name AS handler, count(*) AS n FROM sys_invocation WHERE target_service_name = '${prefix}.Semaphores' AND invoked_by_id = '${invocationId}' GROUP BY target_handler_name`
  )).map(row => [row.handler as string, row.n as number]))

  it("at most `permits` holders at once", { timeout: 60_000 }, async () => {
    const ids = Array.from({ length: 6 }, (_, i) => `cap-${i}`)
    const holders = await Promise.all(ids.map(id => hold("cap", id, { until: 300 })))   // all at once, 2 permits
    for (const [i, holder] of holders.entries()) assert.deepEqual(await outcome(holder), { status: 200, body: ids[i] })
    // Replayed in time order, the events never have more than 2 inside at once.
    let inside = 0
    let most = 0
    for (const e of [...await events(ids)].sort((a, b) => a.at - b.at || (a.event === "exit" ? -1 : 1))) {
      inside += e.event === "enter" ? 1 : -1
      most = Math.max(most, inside)
    }
    assert.equal(most, 2)
    assert.equal(await idle("e2e-cap"), 6)
    // Each release cancels its grant's lease expiry.
    await waitFor("no expiry scheduled", async () => await scheduledExpiries("e2e-cap") === 0, 30)
  })

  it("waiters get the permit first in, first out", { timeout: 60_000 }, async () => {
    const ids = ["fifo-a", "fifo-b", "fifo-c", "fifo-d"]
    const holders = [await hold("fifo", ids[0]!)]
    await waitFor("fifo-a to hold", () => entered("fifo-a"), 30)
    // One at a time, so the queue's order is known.
    for (const id of ids.slice(1)) {
      holders.push(await hold("fifo", id))
      await waits("e2e-fifo", holders.at(-1)!)
    }
    for (const [i, id] of ids.entries()) {
      await open(id)   // succeeds, releasing the permit to the next waiter
      if (i + 1 < ids.length) await waitFor(`a holder after ${id}`, async () => (await events(ids)).filter(e => e.event === "enter").length > i + 1, 30)
    }
    for (const holder of holders) assert.equal((await outcome(holder)).status, 200)
    assert.deepEqual((await events(ids)).filter(e => e.event === "enter").map(e => e.id), ids)
    assert.equal(await idle("e2e-fifo"), 4)
  })

  it("a holder that fails or is cancelled releases its permit", { timeout: 60_000 }, async () => {
    const failing = await hold("release", "release-a", { fail: true })
    await waitFor("release-a to hold", () => entered("release-a"), 30)
    const cancelled = await hold("release", "release-b")
    await waits("e2e-release", cancelled)

    // A declared failure is final: the permit goes to the waiter.
    await open("release-a")
    const failure = await outcome(failing)
    assert.equal(failure.status, 500, JSON.stringify(failure))
    assert.equal((failure.body as { message: string }).message, "Refused")
    await waitFor("release-b to hold", () => entered("release-b"), 30)

    // Cancelled while holding the permit: it goes to the next waiter.
    const last = await hold("release", "release-c")
    await waits("e2e-release", last)
    await manage(cancelled, "cancel")
    const cancellation = await outcome(cancelled)
    assert.equal(cancellation.status, 409)
    assert.equal((cancellation.body as { message: string }).message, "Cancelled")
    await waitFor("release-c to hold", () => entered("release-c"), 30)
    await open("release-c")
    assert.equal((await outcome(last)).status, 200)
    assert.equal(await idle("e2e-release"), 3)
  })

  it("a waiter cancelled in the queue leaves it: no permit goes to it", { timeout: 60_000 }, async () => {
    const first = await hold("queue", "queue-a")
    await waitFor("queue-a to hold", () => entered("queue-a"), 30)
    const cancelled = await hold("queue", "queue-b")
    await waits("e2e-queue", cancelled)
    const last = await hold("queue", "queue-c")
    await waits("e2e-queue", last)

    await manage(cancelled, "cancel")
    assert.equal((await outcome(cancelled)).status, 409)
    await waitFor("queue-b to leave the queue", async () => !(await semaphore("e2e-queue")).waiting.includes(cancelled), 30)
    assert.deepEqual((await semaphore("e2e-queue")).waiting, [last])

    // The permit goes to queue-c at once, not to queue-b (it would keep it for its 15-minute lease).
    await open("queue-a")
    await waitFor("queue-c to hold", () => entered("queue-c"), 30)
    await open("queue-c")
    for (const holder of [first, last]) assert.equal((await outcome(holder)).status, 200)
    assert.equal(await entered("queue-b"), false)
    assert.equal(await idle("e2e-queue"), 2)
  })

  it("the lease reclaims the permit of a killed holder, and of a killed waiter", { timeout: 60_000 }, async () => {
    const holder = await hold("lease", "lease-a")   // a 3-second lease
    await waitFor("lease-a to hold", () => entered("lease-a"), 30)
    const killedWaiter = await hold("lease", "lease-b")
    await waits("e2e-lease", killedWaiter)
    const waiter = await hold("lease", "lease-c")
    await waits("e2e-lease", waiter)

    // Killed, no cleanup runs: lease-a keeps its permit until its lease passes; then lease-b, still
    // in the queue, gets the permit, which it keeps for a lease too.
    for (const killed of [holder, killedWaiter]) {
      await manage(killed, "kill")
      assert.equal((await outcome(killed)).status, 409)
    }
    await waitFor("lease-c to hold", () => entered("lease-c"), 30)
    const [a, c] = await Promise.all(["lease-a", "lease-c"].map(async id => (await events([id])).find(e => e.event === "enter")!.at))
    assert.ok(c! - a! >= 5000 && c! - a! < 20_000, `lease-c got the permit ${c! - a!}ms after lease-a: two leases`)
    assert.equal(await entered("lease-b"), false)
    await open("lease-c")
    assert.equal((await outcome(waiter)).status, 200)
    assert.equal(await idle("e2e-lease"), 3)
  })

  it("a holder's retries and replays keep its permit", { timeout: 60_000 }, async () => {
    // Under the permit, its step fails twice, then its attempt dies: Restate replays it, three times.
    const flaky = await hold("retry", "retry-a", { flaky: true })
    await waitFor("retry-a to die once", async () => (await events(["retry-a"])).some(e => e.event === "died"), 30)
    const waiter = await hold("retry", "retry-b")
    await waits("e2e-retry", waiter)
    assert.deepEqual((await semaphore("e2e-retry")).holders, [flaky])   // still the holder, under its first grant

    await open("retry-a")
    await waitFor("retry-b to hold", () => entered("retry-b"), 30)
    await open("retry-b")
    for (const holder of [flaky, waiter]) assert.equal((await outcome(holder)).status, 200)
    assert.equal(await idle("e2e-retry"), 2)
    // However often it ran, it acquired once and released once.
    assert.deepEqual(await sentBy(flaky), { acquire: 1, release: 1 })
  })

  it("a nested withPermit of the same semaphore runs under the permit held", { timeout: 60_000 }, async () => {
    const nested = await hold("nested", "nested-a", { nested: true })
    await waitFor("nested-a to hold", () => entered("nested-a"), 30)
    const waiter = await hold("nested", "nested-b")
    await waits("e2e-nested", waiter)
    await open("nested-a")
    await waitFor("nested-b to hold", () => entered("nested-b"), 30)
    await open("nested-b")
    for (const holder of [nested, waiter]) assert.equal((await outcome(holder)).status, 200)
    assert.equal(await idle("e2e-nested"), 2)
    // The nested withPermit sent nothing: it ran under the permit already held.
    assert.deepEqual(await sentBy(nested), { acquire: 1, release: 1 })
  })

  it("a defect caught around withPermit releases the permit when the handler ends", { timeout: 60_000 }, async () => {
    // Caught, then a 3-second durable sleep: it holds the permit until the handler ends.
    const caught = await hold("defect", "defect-a", { die: "caught", until: 3000 })
    await waitFor("defect-a to hold", () => entered("defect-a"), 30)
    await new Promise(resolve => setTimeout(resolve, 1000))
    assert.deepEqual((await semaphore("e2e-defect")).holders, [caught], "held during the sleep after the catch")
    assert.deepEqual(await outcome(caught), { status: 200, body: "caught" })
    assert.equal(await idle("e2e-defect"), 1)
    assert.deepEqual(await sentBy(caught), { acquire: 1, release: 1 })
  })

  it("an uncaught defect keeps the permit across retries, even with a durable finalizer", { timeout: 60_000 }, async () => {
    // Dies in every attempt; a durable step in an Effect.ensuring around withPermit runs each time.
    const dying = await hold("ensure", "ensure-a", { die: "uncaught" })
    await waitFor("ensure-a to hold", () => entered("ensure-a"), 30)
    const waiter = await hold("ensure", "ensure-b")
    await waits("e2e-ensure", waiter)
    await new Promise(resolve => setTimeout(resolve, 2000))   // retries meanwhile
    assert.deepEqual((await semaphore("e2e-ensure")).holders, [dying], "still the holder")
    assert.equal(await entered("ensure-b"), false)
    assert.deepEqual(await sentBy(dying), { acquire: 1 })
    // Killed, it never releases: its lease (4 seconds) frees the permit for ensure-b.
    await manage(dying, "kill")
    await waitFor("ensure-b to hold", () => entered("ensure-b"), 30)
    await open("ensure-b")
    assert.equal((await outcome(waiter)).status, 200)
    assert.equal(await idle("e2e-ensure"), 2)
  })

  it("an Effect.timeout around withPermit releases the permit, also while waiting", { timeout: 60_000 }, async () => {
    const first = await hold("timeout", "timeout-a")
    await waitFor("timeout-a to hold", () => entered("timeout-a"), 30)
    // Times out in the queue: it leaves it, so no permit goes to it.
    const waiter = await hold("timeout", "timeout-b", { timeout: 1000 })
    assert.deepEqual(await outcome(waiter), { status: 200, body: "timed out" })
    await waitFor("timeout-b to leave the queue", async () => (await semaphore("e2e-timeout")).waiting.length === 0, 30)
    await open("timeout-a")
    assert.equal((await outcome(first)).status, 200)
    assert.equal(await idle("e2e-timeout"), 1)
    // Times out holding the permit, inside a durable operation: it releases it.
    const holder = await hold("timeout", "timeout-c", { timeout: 1000 })
    assert.deepEqual(await outcome(holder), { status: 200, body: "timed out" })
    assert.equal(await idle("e2e-timeout"), 2)
    for (const id of [waiter, holder]) assert.deepEqual(await sentBy(id), { acquire: 1, release: 1 })
    await waitFor("no expiry scheduled", async () => await scheduledExpiries("e2e-timeout") === 0, 30)
  })

  it("a semaphore object with an app-chosen name", { timeout: 60_000 }, async () => {
    const first = await hold("named", "named-a")
    await waitFor("named-a to hold", () => entered("named-a"), 30)
    const waiter = await hold("named", "named-b")
    await waitFor("named-b to wait", async () => (await semaphore("e2e-named", `${prefix}.app.Limits`)).waiting.includes(waiter), 30)
    assert.deepEqual((await semaphore("e2e-named", `${prefix}.app.Limits`)).holders, [first])
    assert.deepEqual(await semaphore("e2e-named"), { holders: [], grants: 0, waiting: [] }, "nothing in the other object")
    await open("named-a")
    await waitFor("named-b to hold", () => entered("named-b"), 30)
    await open("named-b")
    for (const id of [first, waiter]) assert.equal((await outcome(id)).status, 200)
    await waitFor("e2e-named to be idle", async () => (await semaphore("e2e-named", `${prefix}.app.Limits`)).holders.length === 0, 30)
  })

  it("withPermit against an object that no deployment registers (layerRemote) fails its attempts with RT0018", { timeout: 60_000 }, async () => {
    const holder = await hold("missing", "missing-a")
    // Restate rejects the send to the unknown object, so the attempt fails, with a clear error, and retries.
    await waitFor("the RT0018 failure", async () => {
      const [row] = await query(`SELECT last_failure_error_code AS code, last_failure AS failure FROM sys_invocation WHERE id = '${holder}'`)
      return row?.code === "RT0018" && String(row.failure).includes(`${prefix}.Unregistered/acquire was not found`)
    }, 30)
    assert.equal(await entered("missing-a"), false)
    await manage(holder, "kill")
  })

  it("a crash under the permit, with onDefect: \"fail\", releases it to the next waiter (also on Bun)", { timeout: 60_000 }, async () => {
    const crashing = await hold("crash", `crash-a`, { die: "fatal" }, "crash")
    await waitFor("crash-a to hold", () => entered("crash-a"), 30)
    const waiter = await hold("crash", "crash-b")
    await waits("e2e-crash", waiter)
    await open("crash-a")   // then it dies: the invocation fails at once
    assert.equal((await outcome(crashing)).status, 500)
    await waitFor("crash-b to hold", () => entered("crash-b"), 30)
    await open("crash-b")
    assert.equal((await outcome(waiter)).status, 200)
    assert.equal(await idle("e2e-crash"), 2)
    assert.deepEqual(await sentBy(crashing), { acquire: 1, release: 1 })
  })
})
void semaphoreTests("node", 29183)
void semaphoreTests("bun", 29184)
