// A fixture for the end-to-end tests of Restate.Semaphore: `hold` takes a permit of one of the
// semaphores below and holds it until the test opens its gate (an awakeable), or for some
// milliseconds; then it succeeds, or fails as asked. The endpoint records when each holder entered
// and left its permit, which `events` returns.
import { createServer } from "node:http2"
import { Effect, Layer, Schema } from "effect"
import { HttpRouter } from "effect/http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { Restate } from "@t0ster/restate-effect"

export class Refused extends Schema.Error<Refused>("Refused")({ _tag: Schema.tag("Refused") }) {}

// One semaphore per test, so that each starts empty.
const semaphores = {
  cap: Restate.Semaphore.make("e2e-cap", 2),
  fifo: Restate.Semaphore.make("e2e-fifo", 1),
  release: Restate.Semaphore.make("e2e-release", 1),
  queue: Restate.Semaphore.make("e2e-queue", 1),
  lease: Restate.Semaphore.make("e2e-lease", 1, { lease: "3 seconds" }),
  retry: Restate.Semaphore.make("e2e-retry", 1),
  nested: Restate.Semaphore.make("e2e-nested", 1)
}

const Event = Schema.Struct({ id: Schema.String, event: Schema.Literals(["enter", "exit", "died"]), at: Schema.Number })

export const Holders = Restate.service("Holders", {
  hold: Restate.handler({
    input: Schema.Struct({
      semaphore: Schema.Literals(["cap", "fifo", "release", "queue", "lease", "retry", "nested"]),
      /** Unique: names the holder in events, and its gate. */
      id: Schema.String,
      /** Holds the permit until `open(id)`, or for this many milliseconds. */
      until: Schema.Union([Schema.Literal("opened"), Schema.Number]),
      /** Then fails with Refused. */
      fail: Schema.optional(Schema.Boolean),
      /** Under the permit, a step fails twice (step retries), then the first attempt dies (a retry that replays). */
      flaky: Schema.optional(Schema.Boolean),
      /** Takes the permit again inside, with a nested withPermit. */
      nested: Schema.optional(Schema.Boolean)
    }),
    success: Schema.String,
    error: Refused
  }),
  /** Opens the gate of the holder `id`; false if it has none yet. */
  open: Restate.handler({ input: Schema.String, success: Schema.Boolean }),
  events: Restate.handler({ input: Schema.Void, success: Schema.Array(Event) })
})

const events: Array<typeof Event.Type> = []
const gates = new Map<string, string>()
const stepFailures = new Map<string, number>()
const died = new Set<string>()

/** A journaled step: a replay does not run it again. */
const step = (name: string, action: () => void) => Restate.run(name, Effect.sync(action), { success: Schema.Void, error: Schema.Never })

const HoldersLive = Holders.toLayer({
  hold: ({ semaphore, id, until, fail, flaky, nested }) => Effect.gen(function* () {
    const gate = yield* Restate.awakeable(Schema.Void)
    yield* step("enter", () => {
      gates.set(id, gate.id)
      events.push({ id, event: "enter", at: Date.now() })
    })
    if (flaky) {
      const flakyStep = Effect.suspend(() => {
        const failures = stepFailures.get(id) ?? 0
        if (failures >= 2) return Effect.void
        stepFailures.set(id, failures + 1)
        return Effect.die(new Error(`flaky step failure ${failures + 1}`))
      })
      yield* Restate.run("flaky", flakyStep, { success: Schema.Void, error: Schema.Never, retry: { maxAttempts: 5, initialInterval: "100 millis" } })
      if (!died.has(id)) {
        died.add(id)
        events.push({ id, event: "died", at: Date.now() })
        return yield* Effect.die(new Error("the first attempt dies after the step"))
      }
    }
    yield* until === "opened" ? gate.promise : Restate.sleep(until)
    yield* step("exit", () => {
      events.push({ id, event: "exit", at: Date.now() })
    })
    if (fail) return yield* new Refused()
    return id
  }).pipe(
    effect => nested ? Restate.Semaphore.withPermit(semaphores[semaphore], effect) : effect,
    Restate.Semaphore.withPermit(semaphores[semaphore])
  ),
  open: id => Effect.gen(function* () {
    const gate = yield* Restate.run("gate", Effect.sync(() => gates.get(id) ?? null), { success: Schema.NullOr(Schema.String), error: Schema.Never })
    if (gate === null) return false
    yield* Restate.resolveAwakeable(gate, Schema.Void, undefined)
    return true
  }),
  events: () => Effect.sync(() => [...events])
}, {
  // A dead attempt is retried at once.
  handlers: { hold: { retryPolicy: { initialInterval: "100 millis" } } }
})

if (import.meta.main) HoldersLive.pipe(
  Layer.provide(Restate.layerHttp),
  layer => HttpRouter.serve(layer, { disableLogger: true }),
  Layer.provide(NodeHttpServer.layer(() => createServer(), { port: Number(process.env.PORT ?? 9080), host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)
