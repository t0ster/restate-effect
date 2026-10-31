# Restate × Effect

`@t0ster/restate-effect`: [Restate](https://restate.dev) services for [Effect](https://effect.website) 4.1 and the Restate TypeScript SDK 1.18. Handlers are Effects, errors are typed across the wire, and durable operations compose like Effects.

## Install

```sh
npm install @t0ster/restate-effect effect @restatedev/restate-sdk @restatedev/restate-sdk-clients
```

Serving also needs an Effect HTTP server, such as `@effect/platform-node`.

Requires Restate Server 1.8+: older servers drop error metadata from stored results, so idempotent retries, attaching to a finished invocation and joining a completed workflow lose typed errors.

## Usage

```ts
import { createServer } from "node:http2"
import { Effect, Layer, Schema } from "effect"
import { HttpRouter } from "effect/http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { Restate } from "@t0ster/restate-effect"

class NameTaken extends Schema.Error<NameTaken>("NameTaken")({ _tag: Schema.tag("NameTaken"), name: Schema.String }) {}

// The contract: shared by the server and its callers.
const Greeter = Restate.service("Greeter", {
  greet: Restate.handler({ input: Schema.String, success: Schema.String, error: NameTaken })
})

// Server: implement the contract as a Layer; it registers with the Restate endpoint.
const GreeterLive = Greeter.toLayer({
  greet: name => name === "admin" ? Effect.fail(new NameTaken({ name })) : Effect.succeed(`Hello, ${name}`)
})
GreeterLive.pipe(
  Layer.provide(Restate.layerHttp),
  layer => HttpRouter.serve(layer, { disableLogger: true }),
  Layer.provide(NodeHttpServer.layer(() => createServer(), { port: 9080, host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)

// Client: call it through Restate ingress; declared errors arrive typed.
const greeting = Restate.client(Greeter).greet("admin").pipe(
  Effect.catchTag("NameTaken", e => Effect.succeed(`${e.name} is taken`)),
  Effect.provide(Restate.layerIngress({ url: "http://127.0.0.1:8080" }))
)
```

`greeting` is an Effect: run it once the endpoint is registered with Restate, as the example below shows. It returns `"admin is taken"`.

## Run the example

`example/app.ts` shows the whole API as one small photo-sharing app, read top to bottom: typed errors, contracts for a service, an object and workflows, their handlers, an Effect service over the clients, and the wiring. `example/server.ts` serves it; `example/client.ts` calls it through Restate ingress.

Requires [Bun](https://bun.sh) 1.3+ and a [Restate Server](https://docs.restate.dev/develop/local_dev) running locally on its default ports.

```sh
bun install
bun example/server.ts
# in another terminal: register the endpoint with Restate, then run the client
curl http://127.0.0.1:9070/deployments -H 'content-type: application/json' \
  -d '{"uri":"http://127.0.0.1:9080"}'
bun example/client.ts
```

The example imports `@t0ster/restate-effect`, mapped to `src/` by `tsconfig.json`.

## Contracts and handlers

```ts
import { Context, Effect, Layer, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"

class NameTaken extends Schema.Error<NameTaken>("NameTaken")({ _tag: Schema.tag("NameTaken"), name: Schema.String }) {}

const Users = Restate.service("Users", {
  register: Restate.handler({ input: Schema.String, success: Schema.String, error: NameTaken })
})

const UsersLive = Users.toLayer({
  register: name => name === "admin" ? Effect.fail(new NameTaken({ name })) : Effect.succeed(`hello ${name}`)
})
```

A contract names handlers with their input, success and optional error schemas; TypeScript infers the rest. It has no implementation, so callers can import it from a shared package. `toLayer` implements it: it checks handler names, results, declared errors and capabilities, and the services handlers need become the Layer's requirements, captured once at startup. The Layer provides nothing: it registers the service with `Restate.Endpoint`, as Effect Cluster's `Entity.toLayer` registers with `Sharding`.

- `Restate.service`: stateless.
- `Restate.object`: keyed state. Handlers are exclusive by default; `Restate.shared(handler)` can only read.
- `Restate.workflow`: keyed, one `run` per key, plus shared handlers that read its state meanwhile.

Objects and workflows read state with `Restate.state.get(key, schema)` and write it with `state.set`/`state.clear` (exclusive handlers and workflow `run` only). `Restate.key` is their key. Services cannot use either; `toLayer` rejects it.

### Options

Options that describe the interface go on the contract or a handler: `ingressPrivate` (only other handlers can call it), `description`, `metadata`.

```ts
Restate.service("Photos", handlers, { ingressPrivate: true })
Restate.handler({ input, success, options: { ingressPrivate: true } })
```

Options for how this server runs it go on `toLayer`, for the whole contract or per handler: `retryPolicy`, `onDefect`, `journalRetention`, `idempotencyRetention`, `inactivityTimeout`, `abortTimeout`, `onJournalMismatchErrors`, `enableLazyState` (objects, workflows), `workflowRetention` (workflows). Durations take Effect's `Duration.Input`.

```ts
Photos.toLayer(handlers, {
  journalRetention: "0 seconds",
  retryPolicy: { maxAttempts: 5, initialInterval: "1 second" },
  handlers: { upload: { onDefect: "fail" } }
})
```

### Serving

`Restate.layerHttp` is the endpoint: it provides `Restate.Endpoint` to the implementation Layers and serves every service they register as one route on Effect's `HttpRouter`. Serve it with `HttpRouter.serve` on an HTTP/2 server (Restate streams both ways over one request; Bun: see below), next to other routes if you like, and run it with `Layer.launch` and your platform's `runMain`, which stops it on Ctrl-C. Two service names that collide fail at startup.

Pass `disableLogger: true` to `HttpRouter.serve`, or it logs every Restate request: each invocation attempt is one.

Bun's server has no cleartext HTTP/2, so with `BunHttpServer` register the deployment over HTTP/1.1, which still streams both ways:

```ts
Layer.provide(BunHttpServer.layer({ port: 9080, hostname: "127.0.0.1" })), Layer.launch, BunRuntime.runMain
```

```sh
restate deployments register http://127.0.0.1:9080 --use-http1.1
# or: curl http://127.0.0.1:9070/deployments -H 'content-type: application/json' -d '{"uri":"http://127.0.0.1:9080","use_http_11":true}'
```

Inside handlers, `Effect.log*` goes to Restate's logger, which tags the invocation and stays quiet during replays. Each handler runs in a server span named `Service/handler`, continuing Restate's trace context. Every handler's schemas are advertised to Restate as JSON Schema.

## Clients

```ts
const users = Restate.client(Users)                       // a pure value: build it once
const greeting = yield* users.register("ada")             // waits for the result
const id = yield* users.register("ada", { discard: true }) // one-way: returns the InvocationId
const later = yield* users.attach.register(id)            // waits for that invocation
const counter = Restate.client(Counter).key("signups")    // objects and workflows are keyed
```

Client calls require `Restate.Transport`, resolved when they run. Inside a handler the adapter provides a durable transport, so calls are journaled. Outside Restate, provide `Restate.layerIngress({ url })`. A client can therefore live inside an ordinary Effect service and be used both inside and outside Restate.

- One-way calls take `{ discard: true }`, plus optional `delay` and `idempotencyKey`, and return a `Restate.InvocationId`: a branded string schema, so IDs can be stored, sent and returned.
- `client.attach.<handler>(id)` waits for an invocation, decoded with that handler's schemas. Restate keeps a result to attach to only for workflows and calls made with an `idempotencyKey`.
- Workflows: `client.key(k).run(input)` starts the workflow and waits; running a key that already started joins it. `run(input, { discard: true })` only starts it.

### Clients in Effect services

`Restate.makeClient(contract)` resolves `Transport` once, like `HttpApiClient.make`: its calls require nothing. Build it in a service's Layer, and the Layer requires `Transport` instead of every method. Its calls go through the transport it was made with, so made from ingress they are not journaled, even when the service is used inside a handler. Inside handlers, call `Restate.client`.

`Restate.Client.orDie(client)` turns transport errors into defects, like `HttpClient.catchTag(client, "…", Effect.die)`: calls then fail only with the handler's declared errors, and the defect is the original `ClientError`, with its cause. It works on clients from `client` too; their calls still resolve `Transport` per call and stay durable inside handlers.

```ts
class Profiles extends Context.Service<Profiles, {
  readonly register: (name: string, requestId: string) => Effect.Effect<string, NameTaken>
}>()("Profiles") {
  static readonly layer = Layer.effect(Profiles)(Effect.gen(function* () {
    const users = Restate.Client.orDie(yield* Restate.makeClient(Users))
    return { register: (name, requestId) => users.register(name, { idempotencyKey: requestId }) }
  }))
}

Profiles.layer.pipe(Layer.provide(Restate.layerIngress({ url: "http://127.0.0.1:8080" })))
```

The type is `Restate.Client<C, E, R>`: `E` is the transport error (`ClientError`, or `never` after `orDie`) and `R` what calls require (`Transport` from `client`, `never` from `makeClient`).

## Errors

Declared errors are expected failures: they cross the wire as Restate `TerminalError`s, with the error's `message` (or its tag) as the message and the encoded error in `effectError` metadata, and every caller decodes them back into their classes. They are final: Restate never retries them.

A defect (any other failure: a bug) is handled by `onDefect`:

- `"pause"` (default, Restate's own model): Restate retries per `retryPolicy`, then pauses the invocation, so an operator can deploy a fix and resume it.
- `"fail"`: the full cause is logged and the invocation fails at once with an opaque `"Internal error"`.

A callee's undeclared failure dies in a calling handler, as a remote defect does in Effect RPC. Through ingress it is a `Restate.ClientError`, as are transport failures; the original cause is kept.

`Restate.onError(cleanup)` is `Effect.onError` that runs only when the invocation fails for good: a declared failure, a defect with `onDefect: "fail"`, or cancellation; never when an attempt merely ends (suspension, retry, pause). Its cleanup can make durable calls and steps. Local cleanup (`Effect.ensuring`, `acquireRelease`) runs on every exit, as usual.

Cancellation is interruption: a cancelled handler is interrupted at its next durable operation, its `onError` runs, and it ends as cancelled. A caller waiting on a cancelled invocation is interrupted too, as joining an interrupted fiber is.

## Durable operations

`Restate.run(name, effect, { success, error, retry? })` runs external I/O as one journaled step: a replay returns the journaled result instead of running it again. The effect can use application services, local concurrency and scopes, but no Restate operations. Defects are retried; declared failures are final unless `retry.while` matches them. Once retries are exhausted, the last declared failure is returned, and a defect dies.

```ts
yield* Restate.run("send-mail", mailer.send(to), {
  success: Schema.String,
  error: Schema.Union([MailRejected, MailUnavailable]),
  retry: { while: e => e._tag === "MailUnavailable", maxAttempts: 3, initialInterval: "1 second" }
})
```

Also: `Restate.sleep(duration)` (a durable timer), `Restate.now` (journaled `DateTime.Utc`), `Restate.currentInvocationId`.

### Concurrency

Client calls, `run` and `sleep` return a `Restate.Durable`: an Effect you can `yield*`, which Restate's combinators can also run concurrently. Restate journals the order in which they settle, so replays take the same path.

- `Restate.all(durables)`: like `Effect.all`, concurrent; results in input order; fails on the first failure.
- `Restate.result(durable)`: like `Effect.result`; a failure becomes a `Result`.
- `Restate.raceAll(durables)`: like `Effect.raceAll`, the first success; the others keep running.
- `Restate.fork(durable)` starts one now; `Restate.join(fiber)` is its result.
- `Restate.map`, `asVoid`, `mapError`, `filterOrFail`: transform a Durable like their Effect namesakes and keep it durable. Their functions return plain values, so they cannot start operations; there is no `flatMap`.

Combinators return Durables, so groups nest. Inside a combinator a workflow's `run` does not join a run that already started. Ordinary `Effect.all`, `Effect.race` or forks over durable operations are rejected at runtime: they would make the journal order depend on timing.

## Execution notes

- `run` is at-least-once across crashes before its result is journaled: external writes still need idempotency keys.
- Ordinary Effect clocks, randomness and I/O outside `run` are not replay-safe; use `Restate.now`, `Restate.sleep` and `Restate.run`.
- Each attempt gets a fresh context and scope; the end of an attempt interrupts the handler and its `run` actions. Application Layers live as long as the server Layer.
- Not covered: signals and awakeables, workflow durable promises, explicit cancellation APIs. The raw SDK context is `Restate.Invocation`.

## Development

```sh
bun install --frozen-lockfile
bun run check
bun run lint
bun run build
bun run test:e2e
```

`test:e2e` serves the example on Node 24+ and on Bun, registers it with Restate 1.8.1 in Docker (host network, ports 28080, 29070 and 25122), and runs the example client against each. Node runs the example's TypeScript directly and imports the package from `dist/`, so it also tests the build.

For containers or remote deployments, configure a reachable endpoint hostname and Restate's endpoint authentication; the example defaults to loopback.

## License

MIT
