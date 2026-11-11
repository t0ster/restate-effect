// A fixture for the end-to-end tests of Restate.ensuring, Restate.addFinalizer, one-way sends
// and Restate.cancel: `run` plays a scenario and records what ran, in order, under its `id`;
// `events` returns the record. Steps are journaled, so a replay does not record them again;
// "attempt" is recorded by every attempt.
import { createServer } from "node:http2"
import { Cause, Effect, Exit, Layer, Schema } from "effect"
import { HttpRouter } from "effect/http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { Restate } from "@t0ster/restate-effect"

export class Refused extends Schema.Error<Refused>("Refused")({ _tag: Schema.tag("Refused") }) {}

const Scenario = Schema.Literals(["success", "failure", "cancelled", "caught", "retried", "crash", "keyed", "timeout", "transient"])
const Run = Restate.handler({ input: Schema.Struct({ id: Schema.String, scenario: Scenario }), success: Schema.String, error: Refused })

export const Cleanup = Restate.service("Cleanup", {
  run: Run,
  /** `run` with `onDefect: "fail"`. */
  crash: Run,
  /** Records `event` under `id`: the target of one-way sends. */
  record: Restate.handler({ input: Schema.Struct({ id: Schema.String, event: Schema.String }), success: Schema.Void }),
  /** Cancels an invocation with Restate.cancel. */
  cancel: Restate.handler({ input: Restate.InvocationId, success: Schema.Void }),
  events: Restate.handler({ input: Schema.String, success: Schema.Array(Schema.String) })
})

const events = new Map<string, Array<string>>()
const push = (id: string, event: string) => { events.set(id, [...events.get(id) ?? [], event]) }
const failedOnce = new Set<string>()
/** A journaled step that records `event`. */
const step = (id: string, event: string) => Restate.run(event, Effect.sync(() => push(id, event)), { success: Schema.Void, error: Schema.Never })
const exitOf = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isSuccess(exit) ? "success" : Cause.hasInterruptsOnly(exit.cause) ? "interrupted" : "failure"

const run = ({ id, scenario }: typeof Run.input.Type) => Effect.gen(function* () {
  push(id, "attempt")
  // Not with "timeout": its interrupted sleep stays pending, so the finalizer's step could not be awaited.
  if (scenario !== "timeout") yield* Restate.addFinalizer(exit => step(id, `final:${exitOf(exit)}`))
  const cleanup = step(id, "cleanup")
  switch (scenario) {
    case "success":
      return yield* Restate.ensuring(Effect.as(step(id, "body"), "ok"), cleanup)
    case "failure":
      return yield* Restate.ensuring(Effect.andThen(step(id, "body"), new Refused()), cleanup)
    case "cancelled": {
      const never = yield* Restate.awakeable(Schema.Void)
      return yield* Restate.ensuring(Effect.andThen(step(id, "body"), Effect.as(never.promise, "woken")), cleanup)
    }
    case "crash":
      return yield* Restate.ensuring(Effect.andThen(step(id, "body"), Effect.die(new Error("crash"))), cleanup)
    case "caught": {
      // The defect is caught: the cleanup waits for the end of the invocation, after "after".
      const caught = yield* Restate.ensuring(Effect.andThen(step(id, "body"), Effect.die(new Error("caught"))), cleanup).pipe(
        Effect.catchDefect(() => Effect.succeed("caught"))
      )
      yield* step(id, "after")
      yield* Restate.sleep(100)   // a suspension: the attempt may end here, the invocation does not
      return caught
    }
    case "retried": {
      // The first attempt dies, uncaught: only the attempt ends, so nothing runs; the retry succeeds.
      return yield* Restate.ensuring(Effect.andThen(step(id, "body"), Effect.suspend(() => {
        if (failedOnce.has(id)) return Effect.succeed("retried")
        failedOnce.add(id)
        return Effect.die(new Error("the first attempt dies"))
      })), cleanup)
    }
    case "transient": {
      // The first attempt fails with a ClientError whose cause is not a TerminalError: Restate
      // retries it, so only the attempt ends and nothing runs; the retry succeeds.
      return yield* Restate.ensuring(Effect.andThen(step(id, "body"), Effect.suspend(() => {
        if (failedOnce.has(id)) return Effect.succeed("transient")
        failedOnce.add(id)
        return Effect.fail(new Restate.ClientError({ service: "Cleanup", method: "run", cause: new Error("transient") }))
      })), cleanup)
    }
    case "keyed": {
      // A caught defect defers the first cleanup; the next ensuring with the key runs it before its body.
      yield* Restate.ensuring(Effect.andThen(step(id, "first"), Effect.die(new Error("caught"))), step(id, "cleanup:first"), { key: "k" }).pipe(
        Effect.catchDefect(() => Effect.void)
      )
      yield* step(id, "between")
      return yield* Restate.ensuring(Effect.as(step(id, "second"), "keyed"), step(id, "cleanup:second"), { key: "k" })
    }
    case "timeout": {
      // Effect.timeout interrupts a durable sleep, which stays pending: only a one-way send can
      // go out until it settles, issued with Restate.fork.
      const notify = Restate.fork(Restate.client(Cleanup).record({ id, event: "sent" }, { discard: true }))
      return yield* Restate.ensuring(Restate.sleep("1 minute"), notify).pipe(
        Effect.timeout("500 millis"),
        Effect.as("slept"),
        Effect.catchTag("TimeoutError", () => Effect.succeed("timed out"))
      )
    }
  }
})

const CleanupLive = Cleanup.toLayer({
  run,
  crash: run,
  record: ({ id, event }) => Effect.sync(() => push(id, event)),
  cancel: invocationId => Restate.cancel(invocationId),
  events: id => Effect.sync(() => events.get(id) ?? [])
}, {
  handlers: {
    run: { retryPolicy: { initialInterval: "100 millis" } },
    crash: { onDefect: "fail" }
  }
})

if (import.meta.main) CleanupLive.pipe(
  Layer.provide(Restate.layerHttp),
  layer => HttpRouter.serve(layer, { disableLogger: true }),
  Layer.provide(NodeHttpServer.layer(() => createServer(), { port: Number(process.env.PORT ?? 9080), host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)
