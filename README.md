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

Awakeables: `Restate.awakeable(schema)` returns an `id` and a `promise` that suspends until another handler calls `Restate.resolveAwakeable(id, schema, value)`. Rejecting an awakeable is not wrapped yet.

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
