// Type-level tests for Restate.Semaphore, checked by `bun run check`; never run.
import { Effect, Layer, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"

const Semaphores = Restate.Semaphore.service("types.Semaphores")
const Others = Restate.Semaphore.service("types.Others")
const gateway = Restate.Semaphore.make(Semaphores, "gateway", 2)
const C = Restate.service("types.C", { h: Restate.handler({ input: Schema.Void, success: Schema.Void }) })
type Endpointed = Layer.Layer<never, never, Restate.Endpoint>

// withPermit requires the object's Layer.
const Live = C.toLayer({ h: () => Effect.void.pipe(Restate.Semaphore.withPermit(gateway)) })
export const provided: Endpointed = Live.pipe(Layer.provide(Semaphores.layer))
export const remote: Endpointed = Live.pipe(Layer.provide(Semaphores.layerRemote))
// @ts-expect-error: not provided
export const forgotten: Endpointed = Live
// @ts-expect-error: another object's Layer
export const wrong: Endpointed = Live.pipe(Layer.provide(Others.layer))

// The types without an argument accept any semaphore and object, and withPermit of them is usable.
const wide: Restate.Semaphore.Semaphore = gateway
const wideService: Restate.Semaphore.Service = Semaphores
const limited = <A, E, R>(semaphore: Restate.Semaphore.Semaphore, effect: Effect.Effect<A, E, R>) =>
  Restate.Semaphore.withPermit(semaphore, effect)
const WideLive = C.toLayer({ h: () => limited(wide, Effect.void) })
export const wideProvided: Endpointed = WideLive.pipe(Layer.provide(wideService.layer))
export const madeFromWide: Restate.Semaphore.Semaphore = Restate.Semaphore.make(wideService, "x", 1)

// The object's internals are hidden.
// @ts-expect-error: no toLayer
export const noToLayer = Semaphores.toLayer
// @ts-expect-error: no handlers to call
export const noClient = Restate.client(Semaphores)
