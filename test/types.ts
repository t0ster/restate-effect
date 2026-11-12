// Type-level tests for the core API, checked by `bun run check`; never run.
import { Effect, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"

const C = Restate.service("types.C", { h: Restate.handler({ input: Schema.Void, success: Schema.Void }) })

// Restate.Invocation gives the raw context only.
export const raw = Effect.map(Restate.Invocation, invocation => invocation.ctx)
// @ts-expect-error: the adapter's internals are hidden
export const internals = Effect.map(Restate.Invocation, invocation => invocation.execute)

// Restate.ensuring is dual and keeps self's types; it requires Invocation.
declare const work: Effect.Effect<number, "E", "R">
export const ensured: Effect.Effect<number, "E", "R" | Restate.Invocation> = Restate.ensuring(work, Effect.void)
export const ensuredPiped: Effect.Effect<number, "E", "R" | Restate.Invocation> = work.pipe(Restate.ensuring(Effect.void, { key: "k" }))
// @ts-expect-error: the cleanup cannot fail
export const failingCleanup = Restate.ensuring(work, Effect.fail("x"))
export const finalizer: Effect.Effect<void, never, Restate.Invocation> = Restate.addFinalizer(_exit => Effect.void)

// A one-way send is a Durable: fork issues it without awaiting; yield* gives the invocation ID.
const sent = Restate.client(C).h(undefined, { discard: true })
export const forked: Effect.Effect<Restate.Fiber<Restate.InvocationId>, never, Restate.Invocation> = Restate.fork(sent)
export const awaited: Effect.Effect<Restate.InvocationId, Restate.ClientError, Restate.Transport> = sent
export const cancelled: Effect.Effect<void, never, Restate.Invocation> = Restate.cancel(undefined as unknown as Restate.InvocationId)
