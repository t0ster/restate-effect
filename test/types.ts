// Type-level tests for the semaphore subpath and the core API, checked by `bun run check`; never run.
import { Effect, Layer, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"
import { Semaphore } from "@t0ster/restate-effect/semaphore"

const Semaphores = Semaphore.service("types.Semaphores")
const Others = Semaphore.service("types.Others")
const gateway = Semaphore.make(Semaphores, "gateway", 2)
const other = Semaphore.make(Semaphores, "other", 1)
const C = Restate.service("types.C", { h: Restate.handler({ input: Schema.Void, success: Schema.Void }) })
type Endpointed = Layer.Layer<never, never, Restate.Endpoint>

// withPermit requires the object's Layer.
const Live = C.toLayer({ h: () => Effect.void.pipe(Semaphore.withPermit(gateway)) })
export const provided: Endpointed = Live.pipe(Layer.provide(Semaphores.layer(gateway)))
export const providedWithOthers: Endpointed = Live.pipe(Layer.provide(Semaphores.layer(other, gateway)))
export const remote: Endpointed = Live.pipe(Layer.provide(Semaphores.layerRemote(gateway)))
// @ts-expect-error: the object is served without this semaphore
export const notGiven: Endpointed = Live.pipe(Layer.provide(Semaphores.layer(other)))
// @ts-expect-error: not provided
export const forgotten: Endpointed = Live
// @ts-expect-error: another object's Layer
export const wrong: Endpointed = Live.pipe(Layer.provide(Others.layer()))

// The types without an argument accept any semaphore and object, and withPermit of them is usable.
const wide: Semaphore.Semaphore = gateway
const wideService: Semaphore.Service = Semaphores
const limited = <A, E, R>(semaphore: Semaphore.Semaphore, effect: Effect.Effect<A, E, R>) =>
  Semaphore.withPermit(semaphore, effect)
const WideLive = C.toLayer({ h: () => limited(wide, Effect.void) })
export const wideProvided: Endpointed = WideLive.pipe(Layer.provide(wideService.layer(wide)))
export const madeFromWide: Semaphore.Semaphore = Semaphore.make(wideService, "x", 1)

// The object's internals are hidden.
// @ts-expect-error: no toLayer
export const noToLayer = Semaphores.toLayer
// @ts-expect-error: no handlers to call
export const noClient = Restate.client(Semaphores)

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
