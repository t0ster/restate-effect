import { Duration, Effect, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"

/*
 * The virtual object behind a semaphore service, named and registered by the app (see `service`):
 * one key per semaphore, its name, so its exclusive handlers update one permit ledger at a time.
 * Holders only send to it, one way, never call it: a send leaves the sender's call tree, so
 * cancelling or killing a holder never stops a handler halfway, and Restate runs the sends from
 * one invocation to one key in the order they were sent.
 *
 * Built only on the package's public API. The object owns the limits (`Limits`, per key), given
 * to its layer by the serving deployment; each handler journals the limits it uses (a `Restate.run`
 * first), so one replayed across a deploy that changed them stays consistent. State keys are those
 * of 0.6 and 0.7: their ledgers' `permits` is ignored, their waiters keep their stored lease.
 */

/** A semaphore's limits: `lease` in milliseconds. */
export interface Limits {
  readonly permits: number
  readonly lease: number
}
const LimitsSchema = Schema.NullOr(Schema.Struct({ permits: Schema.Number, lease: Schema.Number }))

const Acquire = Schema.Struct({
  /** The invocation ID. */
  holder: Schema.String,
  /** Resolved once the holder has its permit. */
  awakeable: Schema.String
})
const Release = Schema.Struct({ holder: Schema.String })
const Expire = Schema.Struct({ holder: Schema.String, grant: Schema.Number })

const methods = {
  /** Grants the holder a permit, now or once one is free, by resolving its awakeable. */
  acquire: Restate.handler({ input: Acquire, success: Schema.Void }),
  /** Frees the holder's permit, or takes it out of the queue. */
  release: Restate.handler({ input: Release, success: Schema.Void }),
  /** The lease: reclaims the permit if the holder still holds it under this grant. */
  expire: Restate.handler({ input: Expire, success: Schema.Void })
}

/** The contract of the semaphore object `name`. */
export const contract = <const N extends string>(name: N) =>
  Restate.object(name, methods, { ingressPrivate: true, description: "Restate.Semaphore permits (restate-effect), keyed by semaphore name" })

/** Its client: holders send to it one way; its transport errors are defects. */
export const clientOf = (service: string) => Restate.Client.orDie(Restate.client(contract(service)))

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
  /** The limit last applied; informational (0.7 ledgers' is ignored): the configuration's applies. */
  permits: Schema.optional(Schema.Number),
  holders: Schema.Array(Holder),
  /** Grants so far; each grant's number. */
  grants: Schema.Number,
  /** The queue: places `head` to `tail - 1`, without those whose waiter left. */
  head: Schema.Number,
  tail: Schema.Number
})
interface Ledger {
  permits?: number | undefined
  holders: Array<typeof Holder.Type>
  grants: number
  head: number
  tail: number
}
/** `lease`: milliseconds, stored by 0.7 waiters, which keep it; others get the configuration's at their grant. */
const Waiter = Schema.Struct({ holder: Schema.String, awakeable: Schema.String, lease: Schema.optional(Schema.Number) })
type Waiter = typeof Waiter.Type
const waiterKey = (place: number) => `waiter:${place}`
const placeKey = (holder: string) => `place:${holder}`

const load = Effect.map(Restate.state.get("ledger", Ledger), (saved): Ledger =>
  saved === null ? { holders: [], grants: 0, head: 0, tail: 0 } : { ...saved, holders: [...saved.holders] })
const save = (ledger: Ledger) => Restate.state.set("ledger", Ledger, ledger)

/** The limits of this key, journaled, so a replay across a deploy that changed them sees the same. */
const limitsOf = (service: string, config: ReadonlyMap<string, Limits>) => Effect.gen(function* () {
  const name = yield* Restate.key
  const limits = yield* Restate.run("limits", Effect.sync(() => config.get(name) ?? null), { success: LimitsSchema, error: Schema.Never })
  if (limits === null) return yield* Effect.die(new Error(`Restate.Semaphore ${service}/${name}: unknown semaphore; give it to ${service}'s layer in the deployment that serves the object`))
  return limits
})

const grantTo = Effect.fnUntraced(function* (service: string, ledger: Ledger, limits: Limits, waiter: Waiter) {
  const grant = ++ledger.grants
  // Unless this grant is released by then, `expire` reclaims it.
  const name = yield* Restate.key
  const expire = yield* clientOf(service).key(name).expire({ holder: waiter.holder, grant }, { discard: true, delay: Duration.millis(waiter.lease ?? limits.lease) })
  ledger.holders.push({ holder: waiter.holder, grant, expire })
  yield* Restate.resolveAwakeable(waiter.awakeable, Schema.Void, undefined)
})

/** Ends a grant other than by its `expire`: cancels that, so it does not linger, scheduled, for the lease. */
const cancelExpire = (holder: typeof Holder.Type) =>
  holder.expire === undefined ? Effect.void : Restate.cancel(holder.expire as Restate.InvocationId)

/** Grants free permits to the queue, first in, first out. */
const fill = Effect.fnUntraced(function* (service: string, ledger: Ledger, limits: Limits) {
  while (ledger.holders.length < limits.permits && ledger.head < ledger.tail) {
    const place = ledger.head++
    const waiter = yield* Restate.state.get(waiterKey(place), Waiter)
    if (waiter === null) continue   // it left the queue
    yield* Restate.state.clear(waiterKey(place))
    yield* Restate.state.clear(placeKey(waiter.holder))
    yield* grantTo(service, ledger, limits, waiter)
  }
})

/** The object named `service`, implemented. */
export const layer = (service: string, config: ReadonlyMap<string, Limits>) => contract(service).toLayer({
  acquire: ({ holder, awakeable }) => Effect.gen(function* () {
    const limits = yield* limitsOf(service, config)
    const ledger = yield* load
    ledger.permits = limits.permits
    // Permits that a raised limit frees go to earlier waiters first.
    yield* fill(service, ledger, limits)
    const waiter: Waiter = { holder, awakeable }
    if (ledger.holders.some(h => h.holder === holder)) {
      // Acquired again by a holder: it keeps the permit it has.
      yield* Restate.resolveAwakeable(awakeable, Schema.Void, undefined)
    } else {
      const place = yield* Restate.state.get(placeKey(holder), Schema.Number)
      if (place !== null) {
        // Acquired again while waiting: wake its latest awakeable.
        yield* Restate.state.set(waiterKey(place), Waiter, waiter)
      } else if (ledger.holders.length < limits.permits) {
        // A free permit after `fill` means nobody waits.
        yield* grantTo(service, ledger, limits, waiter)
      } else {
        const last = ledger.tail++
        yield* Restate.state.set(waiterKey(last), Waiter, waiter)
        yield* Restate.state.set(placeKey(holder), Schema.Number, last)
      }
    }
    yield* save(ledger)
  }),
  release: ({ holder }) => Effect.gen(function* () {
    const limits = yield* limitsOf(service, config)
    const ledger = yield* load
    const index = ledger.holders.findIndex(h => h.holder === holder)
    if (index >= 0) {
      const [released] = ledger.holders.splice(index, 1)
      yield* cancelExpire(released!)
      yield* fill(service, ledger, limits)
      return yield* save(ledger)
    }
    // Still waiting, as when cancelled in the queue: leave it, so no permit goes to it.
    const place = yield* Restate.state.get(placeKey(holder), Schema.Number)
    if (place !== null) {
      yield* Restate.state.clear(waiterKey(place))
      yield* Restate.state.clear(placeKey(holder))
    }
  }),
  expire: ({ holder, grant }) => Effect.gen(function* () {
    const limits = yield* limitsOf(service, config)
    const ledger = yield* load
    const index = ledger.holders.findIndex(h => h.holder === holder && h.grant === grant)
    if (index < 0) return   // released, or held under a later grant
    ledger.holders.splice(index, 1)
    yield* Effect.logWarning(`Restate.Semaphore ${yield* Restate.key}: ${holder} held its permit past the lease; reclaimed it`)
    yield* fill(service, ledger, limits)
    yield* save(ledger)
  })
})
