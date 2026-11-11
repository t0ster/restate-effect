# Restate × Effect

Write [Restate](https://restate.dev) services with [Effect](https://effect.website). Handlers are Effects, callers share typed contracts, and declared errors arrive as their original classes.

## Install

```sh
npm install @t0ster/restate-effect effect @restatedev/restate-sdk @restatedev/restate-sdk-clients
```

Requires Effect 4.1, Restate TypeScript SDK 1.18 and Restate Server 1.8+. Older servers lose typed errors when returning stored results.

To host a service on Node, also install `@effect/platform-node`.

`import { Restate } from "@t0ster/restate-effect"` and `import * as Restate from "@t0ster/restate-effect/Restate"` (a flat module, like `effect/Effect`) give the same API.

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
- **For a durable timeout, race against `Restate.sleep`:** `Restate.raceAll([operation, Restate.map(() => null)(Restate.sleep("30 seconds"))])`. `Effect.timeout`, `Effect.race` and other Effect interruption of a pending durable operation run on wall-clock time, so they are not replay-safe: the interrupted Effect ends and its finalizers run, and commands that are not awaited still work (one-way sends through `Restate.fork`, state writes, `Restate.resolveAwakeable`, `Restate.cancel`), but awaiting any other durable operation fails with a defect until the interrupted one settles.
- Declared handler errors are final, typed failures; Restate does not retry them. By default, defects are retried, then the invocation pauses for an operator to fix and resume. Set `onDefect: "fail"` to fail immediately instead.
- Ordinary Effect finalizers run whenever an attempt ends, including retries and suspensions. For durable cleanup, use `Restate.ensuring`, `Restate.addFinalizer` or `Restate.onError` (see below).
- `client.method(input, { discard: true })` starts a handler one way. Inside a handler it is a durable operation: `yield*` it for the invocation ID, or issue it with `Restate.fork` without awaiting anything. `Restate.cancel(invocationId)` cancels an invocation.

The [example](example/app.ts) shows these patterns, including step retries, delayed calls and waiting for workflow results. Signals and workflow durable promises are not wrapped; `yield* Restate.Invocation` gives the raw SDK context as `ctx`.

## Durable cleanup

An attempt ends more often than the invocation does: a suspension, a retry after a defect or a pause end only the attempt, and the next attempt replays the handler from its journal. Effect's own finalizers (`Effect.ensuring`, `onExit`, `acquireRelease`) run at the end of every attempt, so durable steps or calls in them are journaled by some attempts but not others; a retry that takes another path then fails with a journal mismatch (570). Three combinators run cleanup only when it is final:

```ts
// Like Effect.ensuring: release when the work succeeds, fails for good, or is interrupted or cancelled.
const booked = reserve(seat).pipe(
  Restate.ensuring(Restate.fork(Seats.client.key(seat).unlock(undefined, { discard: true })))
)

// When the invocation ends for good, with its Exit.
yield* Restate.addFinalizer(exit => notifyDone(exit))

// Only when the invocation fails for good or is cancelled, with its Cause.
const order = placeOrder.pipe(Restate.onError(cause => refund(cause)))
```

- **`Restate.ensuring(self, cleanup, options?)`** runs `cleanup` when `self` succeeds, fails for good (a declared failure, or a defect with `onDefect: "fail"`), or is interrupted (`Effect.timeout`) or cancelled. After a defect that may only end the attempt (the default `onDefect: "pause"`), the cleanup waits for the end of the invocation, so it runs if the defect is caught around it, and not at all if only the attempt ends: the retry runs `self` again and the cleanup after it. With `{ key }`, a later `ensuring` with the same key runs such a waiting cleanup before its own `self`, as a lock released before it is taken again.
- **`Restate.addFinalizer(exit => cleanup)`** runs when the invocation ends for good: success, final failure, cancellation. Never when only the attempt ends; each attempt registers it again as it replays. Like `Workflow.addFinalizer` in Effect's workflow module.
- **`Restate.onError(cleanup)`** runs only on a final failure or cancellation of the Effect it wraps.
- Cleanup runs uninterruptibly and can make durable calls and steps. A defect in it fails the handler as a defect. Cleanups that wait for the end of the invocation run after the handler's own scope has closed, with the services of where they were registered: do not use the handler's scoped resources in them.
- After `Effect.timeout` interrupted a durable operation, that operation stays pending, so cleanup cannot await another until it settles: issue one-way sends with `Restate.fork(client.method(input, { discard: true }))`.
- A killed invocation runs no cleanup at all.

## Concurrency limits

`Semaphore`, from the `@t0ster/restate-effect/semaphore` subpath, caps how many invocations do something at once, across every handler and process, such as calls to an API that allows four in flight. It is built only on the public API above (`Restate.ensuring`, awakeables, one-way sends, `Restate.cancel`, state). Its permits live in a virtual object that your app names and registers, like any other service:

```ts
import { Semaphore } from "@t0ster/restate-effect/semaphore"

// Shared, like a contract: the object that keeps the app's semaphores.
export const Semaphores = Semaphore.service("my-app.Semaphores")

// The endpoint: register it once, provided to the implementation Layers whose handlers use permits.
GreeterLive.pipe(Layer.provideMerge(Semaphores.layer), Layer.provide(Restate.layerHttp) /* , … */)

const gateway = Semaphore.make(Semaphores, "llm-gateway", 4)

// Inside handlers: at most 4 of these steps run at once.
const complete = (prompt: string) =>
  Restate.run("complete", callGateway(prompt), { success: Schema.String, error: GatewayError }).pipe(
    Semaphore.withPermit(gateway)
  )
```

- **A semaphore is a description; its permits live in Restate.** `Semaphore.service(name)` describes the virtual object that keeps the permits, with its implementation as `.layer`. Its handlers are internal (and ingress-private): only `withPermit` calls them. One object holds any number of semaphores, one key each; within an object, a semaphore's name is unique: everything that uses the name shares its permits.
- **A forgotten registration is a type error.** `withPermit` requires `Registered<"my-app.Semaphores">`, which only `Semaphores.layer` provides, so handlers that use permits leave it in their Layer's requirements until it is provided. A deployment that uses an object another deployment registers provides `Semaphores.layerRemote` instead. If no deployment registers it after all, Restate rejects `withPermit`'s first send: the attempt fails with `RT0018` ("the service handler … was not found") and is retried per the handler's retry policy, then paused or failed once its attempts run out (`onDefect`).
- **Waiting is durable and first in, first out.** A waiting invocation suspends, holding nothing.
- **The permit is released as `Restate.ensuring` runs its cleanup** (`withPermit` is built on it, keyed by the semaphore): when the effect succeeds, fails for good, is cancelled or interrupted, including while it still waits. After a defect caught around it (`Effect.catchDefect`, `Effect.exit`, …), the permit is released when the invocation ends, or at the next `withPermit` of the same semaphore; an uncaught defect only ends the attempt, so the retry keeps the permit.
- **Durable cleanup around `withPermit` belongs in `Restate.ensuring` or `Restate.onError`**, not `Effect.ensuring`, `onExit` or `acquireRelease` (see [Durable cleanup](#durable-cleanup)).
- **Do not bound `withPermit` with `Effect.timeout` or `Effect.race`.** The permit is released, but the invocation then cannot await further durable operations (they fail with a defect) until the interrupted one settles. For a durable timeout, race the durable operation against `Restate.sleep` with `Restate.raceAll`. The invocation holds the permit, so its retries and replays keep it, and a nested `withPermit` of the same semaphore runs under it.
- **A lease bounds a lost permit.** A killed invocation never releases its permit: the semaphore reclaims a permit held longer than the lease, 15 minutes unless `make(service, name, permits, { lease })` sets it. A holder still running then loses its permit without knowing, so set the lease above the longest hold, retries and suspensions included. The lease cannot be renewed yet; a release cancels its lease's scheduled expiry.
- **Wide types.** `Semaphore.Semaphore` and `Semaphore.Service` without a type argument accept any semaphore or object, for fields and helpers; `withPermit` of such a semaphore is satisfied by any object's Layer, so the check of which one is lost.
- **Limits.** Each semaphore is one virtual-object key, so its acquires and releases run one at a time: a few hundred per second at most. The app registers the object once; if several deployments register it, Restate routes it to the latest, so keep them on compatible library versions.

### Migrating from 0.6

0.7.0 moves the semaphore out of the root API, to the `@t0ster/restate-effect/semaphore` subpath, and rebuilds it on the public API. Its shape is unchanged:

```diff
-import { Restate } from "@t0ster/restate-effect"
-export const Semaphores = Restate.Semaphore.service("my-app.Semaphores")
-const gateway = Restate.Semaphore.make(Semaphores, "llm-gateway", 4)
-effect.pipe(Restate.Semaphore.withPermit(gateway))
+import { Semaphore } from "@t0ster/restate-effect/semaphore"
+export const Semaphores = Semaphore.service("my-app.Semaphores")
+const gateway = Semaphore.make(Semaphores, "llm-gateway", 4)
+effect.pipe(Semaphore.withPermit(gateway))
```

The type `Restate.Semaphore.Semaphore<N>` becomes `Semaphore.Semaphore<N>`. The object's name, handlers, schemas and state, and the commands `withPermit` journals, are unchanged, so permits carry over and invocations that hold or wait for a permit under 0.6 resume under 0.7 (an end-to-end test upgrades an endpoint under them). One journal difference: when an invocation ends with releases still waiting after caught defects in several semaphores, 0.6 sent them in the order they were deferred and 0.7 sends them in reverse (scope order). It only matters if that last attempt, started under 0.6, fails after sending some of them and is retried under 0.7.

Other changes in 0.7.0:

- `Restate.Invocation` now gives only `{ ctx }`, the raw SDK context; the adapter's internals are no longer reachable.
- `client.method(input, { discard: true })` returns a `Durable<InvocationId>` instead of an `Effect`: `yield*` still gives the invocation ID; `Restate.fork` and the other combinators accept it.
- New: `Restate.ensuring`, `Restate.addFinalizer`, `Restate.cancel`.

### Migrating from 0.5

0.6.0 makes a forgotten registration a type error. `withPermit` requires `Registered<name>`, which `Semaphores.layer` provides, so provide the Layer to the implementation Layers that use permits instead of merging it beside them. A deployment that uses an object served by another deployment provides `Semaphores.layerRemote`. `Service` no longer exposes the object's contract (`toLayer`, handlers, `Restate.client`).

```diff
-Layer.mergeAll(GreeterLive, Semaphores.layer).pipe(Layer.provide(Restate.layerHttp))
+GreeterLive.pipe(Layer.provideMerge(Semaphores.layer), Layer.provide(Restate.layerHttp))
```

The object's name, keys and state are unchanged, so permits carry over.

### Migrating from 0.4

0.5.0 removed the built-in `_restate-effect.Semaphores` object, which `Restate.layerHttp` served on every endpoint. Make the object yourself, provide its Layer to the implementation Layers that use permits, and pass it to `make`:

```diff
+const Semaphores = Restate.Semaphore.service("my-app.Semaphores")
-const gateway = Restate.Semaphore.make("llm-gateway", 4)
+const gateway = Restate.Semaphore.make(Semaphores, "llm-gateway", 4)
-GreeterLive.pipe(Layer.provide(Restate.layerHttp))
+GreeterLive.pipe(Layer.provideMerge(Semaphores.layer), Layer.provide(Restate.layerHttp))
```

Permits held or awaited in `_restate-effect.Semaphores` are not moved to a new name. Either:

- **Keep the old name:** `Restate.Semaphore.service("_restate-effect.Semaphores")` reads the existing permit state, so holders and waiters carry over; or
- **Stop traffic and drain first:** let every invocation that holds or awaits a permit finish before deploying the new name.

Otherwise, during a rolling or blue-green upgrade, invocations on the old deployment use the old object and new ones the new object: two permit pools, so up to twice the limit runs at once until the old invocations finish. Keep the old deployment registered until its leases have passed (15 minutes by default): 0.4.0 left each grant's lease expiry scheduled, and those calls still go to the old object. Restate removes whole deployments, not single services, so the old object goes away when the last deployment that serves it is removed.

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
