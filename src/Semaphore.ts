/**
 * Durable semaphores, shaped like Effect's `Semaphore` module: at most `permits` invocations hold
 * a permit at once, across every handler, process and restart, such as callers of an API with a
 * concurrency limit. A semaphore is a description; its permits live in Restate, in the object
 * `_restate-effect.Semaphores` that `Restate.layerHttp` serves.
 */
import { Duration, Effect, Exit, Schema } from "effect"
import { isFinal } from "./durable.js"
import { Invocation, type InvocationState } from "./invocation.js"
import { awakeable } from "./operations.js"
import { send, sendNow } from "./semaphores.js"

/** A durable counting semaphore. Make one with `make`; use it with `withPermit`. */
export interface Semaphore {
  /** Unique in the Restate cluster: every description with this name shares its permits. */
  readonly name: string
  /** How many invocations hold a permit at once, at most. */
  readonly permits: number
  /** How long a holder keeps its permit, at most: then the semaphore reclaims it. */
  readonly lease: Duration.Duration
}

/**
 * Describes a semaphore. Its state lives in Restate, so this is a plain value, like a contract.
 * Descriptions with one name should agree; if they do not, each acquire's `permits` applies from
 * then on.
 *
 * `lease` (default 15 minutes) bounds how long a holder keeps its permit, so a killed holder,
 * which never releases it, frees it eventually. A holder that is still running when its lease
 * passes loses the permit without being told, so set it above the longest hold, retries and
 * suspensions included. The default is above Restate's default inactivity and abort timeouts
 * (1 and 10 minutes): a stuck attempt is retried, keeping its permit, before the lease passes.
 */
export const make = (name: string, permits: number, options?: { readonly lease?: Duration.Input }): Semaphore => {
  if (!name) throw new Error("A Restate semaphore needs a nonempty name")
  if (!Number.isSafeInteger(permits) || permits < 1) throw new Error(`Semaphore ${name}: permits must be a positive integer, got ${permits}`)
  const lease = Duration.fromInputUnsafe(options?.lease ?? "15 minutes")
  if (!Duration.isFinite(lease) || !Duration.isPositive(lease)) throw new Error(`Semaphore ${name}: the lease must be positive and finite`)
  return Object.freeze({ name, permits, lease })
}

/** The semaphores each attempt holds a permit of, so that a nested `withPermit` runs under it. */
const holding = new WeakMap<InvocationState, Set<string>>()

const permit = <A, E, R>(self: Semaphore, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Invocation> =>
  Effect.flatMap(Invocation, invocation => {
    let held = holding.get(invocation)
    if (held === undefined) holding.set(invocation, held = new Set())
    if (held.has(self.name)) return effect
    const names = held
    const holder = invocation.ctx.request().id
    const acquire = Effect.gen(function* () {
      // A caught defect's release, still deferred, goes first.
      invocation.flush(`semaphore:${self.name}`)
      const granted = yield* awakeable(Schema.Void)
      yield* send(self.name, "acquire", {
        holder, awakeable: granted.id, permits: self.permits, lease: Math.ceil(Duration.toMillis(self.lease))
      })
      yield* granted.promise
      names.add(self.name)
    })
    const release = send(self.name, "release", { holder }).pipe(
      // A release that cannot be sent leaves the permit held until the lease passes: never silently.
      Effect.tapCause(cause => Effect.logError(`Restate.Semaphore ${self.name}: could not release the permit`, cause))
    )
    return Effect.onExit(Effect.andThen(acquire, effect), exit => Effect.suspend(() => {
      names.delete(self.name)
      if (Exit.isSuccess(exit) || isFinal(invocation, exit.cause)) return release
      // A defect that only ends the attempt when it ends the handler: it may yet be caught, as by
      // Effect.catchDefect. Release when the handler ends for good (or before taking the permit
      // again); if only the attempt ends, its retry keeps the permit.
      if (!invocation.ctx.request().attemptCompletedSignal.aborted) {
        invocation.defer(`semaphore:${self.name}`, () => { sendNow(invocation, self.name, "release", { holder }) })
      }
      return Effect.void
    }))
  })

/**
 * Runs `effect` holding one permit, inside a handler: waits for a permit durably, first in, first
 * out (a waiting invocation suspends, holding nothing), runs `effect`, then releases the permit
 * when it succeeds, fails for good, is cancelled or interrupted (as by `Effect.timeout`), also
 * while still waiting, as `Restate.onError` runs; after a caught defect, when the handler ends
 * with success or a final failure (or at the next `withPermit` of this semaphore). Not when only
 * an attempt ends: the invocation is the holder, so its retries and replays keep the permit, and a
 * nested `withPermit` of the same semaphore runs under it. A killed invocation keeps its permit
 * until the lease passes (see `make`).
 *
 * Do not bound it with `Effect.timeout` or `Effect.race`: the permit is released, but the
 * interrupted durable operation stays pending, so the invocation cannot await another durable
 * operation until it settles (such awaits fail with a defect), and wall-clock interruption is not
 * replay-safe. For a durable timeout, race the durable operation inside it against
 * `Restate.sleep` with `Restate.raceAll`.
 */
export const withPermit: {
  (self: Semaphore): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R | Invocation>
  <A, E, R>(self: Semaphore, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Invocation>
} = ((self: Semaphore, effect?: Effect.Effect<any, any, any>) =>
  effect === undefined ? (effect: Effect.Effect<any, any, any>) => permit(self, effect) : permit(self, effect)) as any
