# Restate × Effect

Write [Restate](https://restate.dev) services with [Effect](https://effect.website). Handlers are Effects, callers share typed contracts, and declared errors arrive as their original classes.

## Install

```sh
npm install @t0ster/restate-effect effect @restatedev/restate-sdk @restatedev/restate-sdk-clients
```

Requires Effect 4.1, Restate TypeScript SDK 1.18 and Restate Server 1.8+. Older servers lose typed errors when returning stored results.

To host a service on Node, also install `@effect/platform-node`.

## Usage

Define a contract, then implement it. Callers only need the contract, so it can live in a shared package.

```ts
import { Effect, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"

class NameTaken extends Schema.Error<NameTaken>("NameTaken")({
  _tag: Schema.tag("NameTaken"),
  name: Schema.String
}) {}

const Greeter = Restate.service("Greeter", {
  greet: Restate.handler({ input: Schema.String, success: Schema.String, error: NameTaken })
})

const GreeterLive = Greeter.toLayer({
  greet: name => name === "admin"
    ? Effect.fail(new NameTaken({ name }))
    : Effect.succeed(`Hello, ${name}`)
})
```

Serve `GreeterLive` with `Restate.layerHttp` on an Effect HTTP/2 server; [example/server.ts](example/server.ts) shows the wiring. Once the endpoint is registered with Restate, call it through ingress:

```ts
const greeting = await Effect.runPromise(Restate.client(Greeter).greet("admin").pipe(
  Effect.catchTag("NameTaken", e => Effect.succeed(`${e.name} is taken`)),
  Effect.provide(Restate.layerIngress({ url: "http://127.0.0.1:8080" }))
))

console.log(greeting) // "admin is taken"
```

Inside handlers, the same client makes durable calls automatically—no ingress Layer needed.

## Run the example

The [photo-sharing example](example/app.ts) covers services, keyed objects, workflows, external I/O and concurrent calls.

From this repository, with [Bun](https://bun.sh) 1.3+ and [Restate Server](https://docs.restate.dev/develop/local_dev) running on its default ports:

```sh
bun install
bun example/server.ts
# in another terminal: register the endpoint with Restate, then run the client
curl http://127.0.0.1:9070/deployments -H 'content-type: application/json' \
  -d '{"uri":"http://127.0.0.1:9080"}'
bun example/client.ts
```

The server binds to loopback. For containers or remote deployments, use a reachable hostname and configure Restate's endpoint authentication.

## Services, objects and workflows

Define typed contracts for Restate services, virtual objects and workflows with `Restate.service`, `Restate.object` and `Restate.workflow`.

Use `Restate.client(contract).key(id)` to call an object or workflow. Inside their handlers, `Restate.key` gives the key and `Restate.state` reads and writes state.

Handler dependencies become requirements of the implementation Layer, provided once at startup. Server options such as `retryPolicy`, `runRetry` and `onDefect` go on `toLayer`, for the service or per handler.

## Durability and errors

- **External I/O belongs in `Restate.run(name, effect, { success, error })`.** Restate stores the result and reuses it on replay. Don't call Restate operations inside the step.
- **Set a default step retry with `runRetry`.** `toLayer(handlers, { runRetry: { maxAttempts: 3, initialInterval: "2 seconds" } })` applies to every `Restate.run` in the Layer's handlers; set it per handler under `handlers: { name: { runRetry } }`. A step's own `retry` overrides the fields it sets, and `retry.while` (which declared errors to retry) is per step only. When a step's retries run out on a defect, the handler fails with a defect: with `onDefect: "fail"`, the invocation fails and `Restate.onError` runs.
- **Let a failure set its retry delay with `retry.delay`.** `retry: { while: e => e._tag === "RateLimited", delay: e => e.retryAfter }` waits the failure's own delay (such as an HTTP 429's Retry-After) before the next attempt; `undefined` falls back to the backoff (`initialInterval`, `exponentiationFactor`, `maxInterval`). Like `while`, it is per step only. `maxAttempts` and `maxDuration` still bound the retries, and exhausted retries still return the last declared failure. Restate adds no jitter to the delay, so add it in `delay`; it runs inside the failed attempt and only tells Restate when to retry, so randomness there is replay-safe. `maxDuration` is checked when an attempt fails, so a long delay can start the next attempt after it. Bound delayed retries with `maxAttempts` or `maxDuration`: without them, a delay of 0 retries at once, forever. If `delay` (or `while`) throws, the step retries as a defect and ends as one, not as the declared failure. Delayed retries are logged as warnings.
- **External writes still need idempotency keys.** A crash before a step's result is stored can cause it to run again.
- Use `Restate.now` and `Restate.sleep` for replay-safe time. Ordinary clocks, randomness and I/O outside `run` are not replay-safe.
- Use `Restate.all`, `Restate.raceAll` and `Restate.fork` for durable concurrency. Ordinary Effect concurrency over durable operations (`Effect.all` with concurrency, `Effect.forkChild`) fails at runtime with a defect.
- **For a durable timeout, race against `Restate.sleep`:** `Restate.raceAll([operation, Restate.map(() => null)(Restate.sleep("30 seconds"))])`. `Effect.timeout`, `Effect.race` and other Effect interruption of a pending durable operation run on wall-clock time, so they are not replay-safe: the interrupted Effect ends and its finalizers run, and one-way commands still work (so `withPermit` still releases its permit), but awaiting any other durable operation fails with a defect until the interrupted one settles.
- Declared handler errors are final, typed failures; Restate does not retry them. By default, defects are retried, then the invocation pauses for an operator to fix and resume. Set `onDefect: "fail"` to fail immediately instead.
- Use `Restate.onError` for durable cleanup on final failure or cancellation. Ordinary Effect finalizers run whenever an attempt ends, including retries.

The [example](example/app.ts) shows these patterns, including step retries, delayed calls and waiting for workflow results. Signals, workflow durable promises and explicit cancellation APIs are not wrapped; the raw SDK context is available as `Restate.Invocation`.

## Concurrency limits

`Restate.Semaphore` caps how many invocations do something at once, across every handler and process, such as calls to an API that allows four in flight:

```ts
const gateway = Restate.Semaphore.make("llm-gateway", 4)

// Inside handlers: at most 4 of these steps run at once.
const complete = (prompt: string) =>
  Restate.run("complete", callGateway(prompt), { success: Schema.String, error: GatewayError }).pipe(
    Restate.Semaphore.withPermit(gateway)
  )
```

- **A semaphore is a description; its permits live in Restate.** Its name is unique in the Restate cluster: everything that uses the name shares its permits. The virtual object `_restate-effect.Semaphores` keeps them; `Restate.layerHttp` serves it on every endpoint, so registering a deployment adds it.
- **Waiting is durable and first in, first out.** A waiting invocation suspends, holding nothing.
- **The permit is released when the effect succeeds, fails for good, is cancelled or interrupted**, as `Restate.onError` runs, including while it still waits. After a defect caught around it (`Effect.catchDefect`, `Effect.exit`, …), the permit is released when the handler ends with success or a final failure, or at the next `withPermit` of the same semaphore; an uncaught defect only ends the attempt, so the retry keeps the permit.
- **Do not bound `withPermit` with `Effect.timeout` or `Effect.race`.** The permit is released, but the invocation then cannot await further durable operations (they fail with a defect) until the interrupted one settles. For a durable timeout, race the durable operation against `Restate.sleep` with `Restate.raceAll`. The invocation holds the permit, so its retries and replays keep it, and a nested `withPermit` of the same semaphore runs under it.
- **A lease bounds a lost permit.** A killed invocation never releases its permit: the semaphore reclaims a permit held longer than the lease, 15 minutes unless `make(name, permits, { lease })` sets it. A holder still running then loses its permit without knowing, so set the lease above the longest hold, retries and suspensions included. The lease cannot be renewed yet; a release cancels its lease's scheduled expiry.
- **Limits.** Each semaphore is one virtual-object key, so its acquires and releases run one at a time: a few hundred per second at most. Every deployment serves `_restate-effect.Semaphores`, and Restate routes it to the latest registered one, so keep deployments on compatible library versions.

The semaphore waits with an awakeable, which is available on its own: `Restate.awakeable(schema)` returns an `id` and a `promise` that suspends until another handler calls `Restate.resolveAwakeable(id, schema, value)`. Rejecting an awakeable is not wrapped yet.

## Development

```sh
bun install --frozen-lockfile
bun run check
bun run lint
bun run build
bun run test:e2e
```

End-to-end tests require Node 24+ and Docker with host networking. They run the example against Restate on both Node and Bun.

## License

MIT
