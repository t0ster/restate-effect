// A fixture for the end-to-end test of Restate.run retries: `call` fails with RateLimited until it
// has failed once per `retryAfter` entry, each failure asking for that delay (null: none). The
// endpoint keeps the start time of every attempt, which `attempts` returns.
import { createServer } from "node:http2"
import { Duration, Effect, Layer, Schema } from "effect"
import { HttpRouter } from "effect/http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { Restate } from "@t0ster/restate-effect"

export class RateLimited extends Schema.Error<RateLimited>("RateLimited")({
  _tag: Schema.tag("RateLimited"), attempt: Schema.Number, retryAfter: Schema.optional(Schema.Duration)
}) {}

export const Retries = Restate.service("Retries", {
  call: Restate.handler({
    input: Schema.Struct({ id: Schema.String, retryAfter: Schema.Array(Schema.NullOr(Schema.Number)), maxAttempts: Schema.Number }),
    success: Schema.Number,
    error: RateLimited
  }),
  attempts: Restate.handler({ input: Schema.String, success: Schema.Array(Schema.Number) })
})

const attempts = new Map<string, Array<number>>()

const RetriesLive = Retries.toLayer({
  call: ({ id, retryAfter, maxAttempts }) => {
    const attempt = Effect.suspend(() => {
      const started = attempts.get(id) ?? []
      attempts.set(id, [...started, Date.now()])
      const millis = retryAfter[started.length]
      return millis === undefined
        ? Effect.succeed(started.length + 1)
        : Effect.fail(new RateLimited({ attempt: started.length + 1, retryAfter: millis === null ? undefined : Duration.millis(millis) }))
    })
    return Restate.run("call", attempt, {
      success: Schema.Number,
      error: RateLimited,
      retry: { while: () => true, delay: error => error.retryAfter, maxAttempts }
    })
  },
  attempts: id => Effect.succeed(attempts.get(id) ?? [])
}, {
  // The backoff when a failure asks for no delay: 2 seconds, every time.
  runRetry: { initialInterval: "2 seconds", exponentiationFactor: 1 }
})

if (import.meta.main) RetriesLive.pipe(
  Layer.provide(Restate.layerHttp),
  layer => HttpRouter.serve(layer, { disableLogger: true }),
  Layer.provide(NodeHttpServer.layer(() => createServer(), { port: Number(process.env.PORT ?? 9080), host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)
