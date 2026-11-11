import { Effect, Schema } from "effect"
import type * as sdk from "@restatedev/restate-sdk"
import { awaitPromise } from "./durable.js"
import { cachedSerde } from "./codec.js"
import { handler, object, type Implementations } from "./contract.js"
import { Invocation, type InvocationState } from "./invocation.js"
import { key, resolveAwakeable, state } from "./operations.js"

/*
 * The virtual object behind `Restate.Semaphore`, named and registered by the app (see `service`):
 * one object per semaphore, keyed by its name, so its exclusive handlers update one permit ledger at a time.
 * Holders only send to it, one way, never call it: a send leaves the sender's call tree, so
 * cancelling or killing a holder never stops a handler halfway, and Restate runs the sends from
 * one invocation to one key in the order they were sent.
 */

const Acquire = Schema.Struct({
  /** The invocation ID. */
  holder: Schema.String,
  /** Resolved once the holder has its permit. */
  awakeable: Schema.String,
  permits: Schema.Number,
  /** Milliseconds. */
  lease: Schema.Number
})
const Release = Schema.Struct({ holder: Schema.String })
const Expire = Schema.Struct({ holder: Schema.String, grant: Schema.Number })

const methods = {
  /** Grants the holder a permit, now or once one is free, by resolving its awakeable. */
  acquire: handler({ input: Acquire, success: Schema.Void }),
  /** Frees the holder's permit, or takes it out of the queue. */
  release: handler({ input: Release, success: Schema.Void }),
  /** The lease: reclaims the permit if the holder still holds it under this grant. */
  expire: handler({ input: Expire, success: Schema.Void })
}

type Methods = typeof methods
/** The contract of a semaphore object. */
export type Contract<N extends string = string> = ReturnType<typeof contract<N>>
const contract = <const N extends string>(name: N) =>
  object(name, methods, { ingressPrivate: true, description: "Restate.Semaphore permits (restate-effect), keyed by semaphore name" })
/**
 * Sends `input` to the semaphore `name`, one way, without waiting for Restate to accept it (see
 * above). With `delay` (milliseconds), Restate delivers it that much later. Returns the handle.
 */
export const sendNow = <M extends keyof Methods>(invocation: InvocationState, service: string, name: string, method: M, input: Methods[M]["input"]["Type"], delay?: number): sdk.InvocationHandle => {
  invocation.assertReady()
  return issueSend(invocation, service, name, method, input, delay)
}
/** `sendNow` without the readiness check, for inside `execute`, which made it. */
const issueSend = <M extends keyof Methods>(invocation: InvocationState, service: string, name: string, method: M, input: Methods[M]["input"]["Type"], delay?: number): sdk.InvocationHandle =>
  invocation.ctx.genericSend({
    service, method, key: name, parameter: input,
    inputSerde: cachedSerde(methods[method].input as Methods[M]["input"]), delay
  })
export const send = <M extends keyof Methods>(service: string, name: string, method: M, input: Methods[M]["input"]["Type"]): Effect.Effect<void, never, Invocation> =>
  Effect.flatMap(Invocation, invocation => Effect.sync(() => { sendNow(invocation, service, name, method, input) }))

/*
 * State: the ledger, small and rewritten by every change, and the queue's waiters, one key each,
 * so joining or leaving even a long queue writes only that waiter's keys: `waiter:<place>`, and
 * `place:<holder>` to find a waiter that leaves.
 */
const Holder = Schema.Struct({
  holder: Schema.String,
  grant: Schema.Number,
  /** The invocation ID of this grant's `expire`, cancelled once the grant ends (absent in 0.4.0 ledgers). */
  expire: Schema.optional(Schema.String)
})
const Ledger = Schema.Struct({
  /** The latest acquire's. */
  permits: Schema.Number,
  holders: Schema.Array(Holder),
  /** Grants so far; each grant's number. */
  grants: Schema.Number,
  /** The queue: places `head` to `tail - 1`, without those whose waiter left. */
  head: Schema.Number,
  tail: Schema.Number
})
interface Ledger {
  permits: number
  holders: Array<typeof Holder.Type>
  grants: number
  head: number
  tail: number
}
const Waiter = Schema.Struct({ holder: Schema.String, awakeable: Schema.String, lease: Schema.Number })
type Waiter = typeof Waiter.Type
const waiterKey = (place: number) => `waiter:${place}`
const placeKey = (holder: string) => `place:${holder}`

const load = Effect.map(state.get("ledger", Ledger), (saved): Ledger =>
  saved === null ? { permits: 0, holders: [], grants: 0, head: 0, tail: 0 } : { ...saved, holders: [...saved.holders] })
const save = (ledger: Ledger) => state.set("ledger", Ledger, ledger)

const grantTo = Effect.fnUntraced(function* (service: string, ledger: Ledger, waiter: Waiter) {
  const grant = ++ledger.grants
  // Unless this grant is released by then, `expire` reclaims it.
  const name = yield* key
  const expire = yield* Effect.flatMap(Invocation, invocation => awaitPromise<string, never>(signal =>
    invocation.execute(() => issueSend(invocation, service, name, "expire", { holder: waiter.holder, grant }, waiter.lease).invocationId, signal)))
  ledger.holders.push({ holder: waiter.holder, grant, expire })
  yield* resolveAwakeable(waiter.awakeable, Schema.Void, undefined)
})

/** Ends a grant other than by its `expire`: cancels that, so it does not linger, scheduled, for the lease. */
const cancelExpire = (holder: typeof Holder.Type) => Effect.flatMap(Invocation, invocation => Effect.sync(() => {
  if (holder.expire === undefined) return
  invocation.assertReady()
  invocation.ctx.cancel(holder.expire as sdk.InvocationId)
}))

/** Grants free permits to the queue, first in, first out. */
const fill = Effect.fnUntraced(function* (service: string, ledger: Ledger) {
  while (ledger.holders.length < ledger.permits && ledger.head < ledger.tail) {
    const place = ledger.head++
    const waiter = yield* state.get(waiterKey(place), Waiter)
    if (waiter === null) continue   // it left the queue
    yield* state.clear(waiterKey(place))
    yield* state.clear(placeKey(waiter.holder))
    yield* grantTo(service, ledger, waiter)
  }
})

/** The contract named `name`, and its implementation. */
export const make = <const N extends string>(name: N) => {
  const self = contract(name)
  return { contract: self, layer: self.toLayer(implementation(name)) }
}

const implementation = (service: string): Implementations<Methods> => ({
  acquire: ({ holder, awakeable, permits, lease }) => Effect.gen(function* () {
    const ledger = yield* load
    ledger.permits = permits
    // Permits that a larger count frees go to earlier waiters first.
    yield* fill(service, ledger)
    const waiter = { holder, awakeable, lease }
    if (ledger.holders.some(h => h.holder === holder)) {
      // Acquired again by a holder: it keeps the permit it has.
      yield* resolveAwakeable(awakeable, Schema.Void, undefined)
    } else {
      const place = yield* state.get(placeKey(holder), Schema.Number)
      if (place !== null) {
        // Acquired again while waiting: wake its latest awakeable.
        yield* state.set(waiterKey(place), Waiter, waiter)
      } else if (ledger.holders.length < ledger.permits) {
        // A free permit after `fill` means nobody waits.
        yield* grantTo(service, ledger, waiter)
      } else {
        const last = ledger.tail++
        yield* state.set(waiterKey(last), Waiter, waiter)
        yield* state.set(placeKey(holder), Schema.Number, last)
      }
    }
    yield* save(ledger)
  }),
  release: ({ holder }) => Effect.gen(function* () {
    const ledger = yield* load
    const index = ledger.holders.findIndex(h => h.holder === holder)
    if (index >= 0) {
      const [released] = ledger.holders.splice(index, 1)
      yield* cancelExpire(released!)
      yield* fill(service, ledger)
      return yield* save(ledger)
    }
    // Still waiting, as when cancelled in the queue: leave it, so no permit goes to it.
    const place = yield* state.get(placeKey(holder), Schema.Number)
    if (place !== null) {
      yield* state.clear(waiterKey(place))
      yield* state.clear(placeKey(holder))
    }
  }),
  expire: ({ holder, grant }) => Effect.gen(function* () {
    const ledger = yield* load
    const index = ledger.holders.findIndex(h => h.holder === holder && h.grant === grant)
    if (index < 0) return   // released, or held under a later grant
    ledger.holders.splice(index, 1)
    yield* Effect.logWarning(`Restate.Semaphore ${yield* key}: ${holder} held its permit past the lease; reclaimed it`)
    yield* fill(service, ledger)
    yield* save(ledger)
  })
})
