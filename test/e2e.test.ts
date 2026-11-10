// End to end: serve the example under Node and under Bun, register it with a real Restate server,
// and run the example client against it. Restate runs in Docker on the host network.
import { type ChildProcess, spawn, spawnSync } from "node:child_process"
import { after, before, test } from "node:test"
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
