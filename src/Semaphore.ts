/**
 * Durable semaphores, shaped like Effect's `Semaphore` module: at most `permits` invocations hold
 * a permit at once, across every handler, process and restart, such as callers of an API with a
 * concurrency limit. A semaphore is a description; its permits live in Restate, in a virtual object
 * that the app names and registers, made by `service`.
 *
 * Known limitation: durable cleanup around `withPermit` (`Effect.ensuring`, `Effect.onExit`,
 * `Effect.acquireRelease`) is not replay-safe after a defect that may not repeat: the retry's
 * journal no longer matches (error 570). Use `Restate.onError` for durable cleanup.
 */
import { Context, Duration, Effect, Exit, Layer, Schema } from "effect"
import { isFinal } from "./durable.js"
import { Invocation, type InvocationState } from "./invocation.js"
import { awakeable } from "./operations.js"
import { make as makeService, send, sendNow } from "./semaphores.js"
import type { Endpoint } from "./server.js"

declare const RegisteredId: unique symbol
/**
 * Proof that the endpoint serves (or, with `layerRemote`, that another deployment serves) the
 * semaphore object `N`: `withPermit` requires it, so a forgotten registration is a type error.
 */
export interface Registered<N extends string> {
  readonly [RegisteredId]: (name: N) => N
}
/** What `withPermit` of `S`, or of each semaphore in a union `S`, requires. */
export type RegisteredOf<S extends Semaphore<any>> = S extends Semaphore<infer N> ? Registered<N> : never

/**
 * The virtual object that keeps semaphores' permits, one key per semaphore, with its
 * implementation as `layer`. Register `layer` in the app's endpoint, once in the cluster. Its
 * handlers are internal: only `withPermit` calls them.
 *
 * `Service` without a type argument accepts any semaphore object.
 */
export interface Service<N extends string = any> {
  /** The object's name in Restate. */
  readonly name: N
  /** Provided by `layer` and `layerRemote`; required by `withPermit`. */
  readonly registered: Context.Service<Registered<N>, N>
  /**
   * Implements the object: registers it with the endpoint, like a `toLayer` Layer, and provides
   * `Registered<N>`. Provide it to the implementation Layers whose handlers use `withPermit`.
   */
  readonly layer: Layer.Layer<Registered<N>, never, Endpoint>
  /**
   * Provides `Registered<N>` without serving the object, for a deployment that uses permits of an
   * object that another deployment of the cluster registers.
   */
  readonly layerRemote: Layer.Layer<Registered<N>>
}

/**
 * Makes the semaphore object named `name`, unique in the Restate cluster. Every semaphore made
 * with it (see `make`) keeps its permits there.
 *
 * ```ts
 * export const Semaphores = Restate.Semaphore.service("app.Semaphores")
 * // in the endpoint: handlers that use withPermit require it
 * GreeterLive.pipe(Layer.provideMerge(Semaphores.layer), Layer.provide(Restate.layerHttp))
 * ```
 */
export const service = <const N extends string>(name: N): Service<N> => {
  const { contract, layer } = makeService(name)
  const registered = Context.Service<Registered<N>, N>(`restate-effect/Semaphore/Registered/${name}`)
  const layerRemote = Layer.succeed(registered, name)
  return Object.freeze({ name: contract.name, registered, layer: Layer.merge(layer, layerRemote), layerRemote })
}

/**
 * A durable counting semaphore. Make one with `make`; use it with `withPermit`. `Semaphore`
 * without a type argument accepts any semaphore; `withPermit` of one then requires any
 * `Registered`, so it no longer checks which object's Layer is provided.
 */
export interface Semaphore<N extends string = any> {
  /** The object that keeps its permits. */
  readonly service: Service<N>
  /** Unique in its service: every description with this name and service shares its permits. */
  readonly name: string
  /** How many invocations hold a permit at once, at most. */
  readonly permits: number
  /** How long a holder keeps its permit, at most: then the semaphore reclaims it. */
  readonly lease: Duration.Duration
}

/**
 * Describes a semaphore, kept by `service` (see `service`). Its state lives in Restate, so this is a plain value, like a contract.
 * Descriptions with one name should agree; if they do not, each acquire's `permits` applies from
 * then on.
 *
 * `lease` (default 15 minutes) bounds how long a holder keeps its permit, so a killed holder,
 * which never releases it, frees it eventually. A holder that is still running when its lease
 * passes loses the permit without being told, so set it above the longest hold, retries and
 * suspensions included. The default is above Restate's default inactivity and abort timeouts
 * (1 and 10 minutes): a stuck attempt is retried, keeping its permit, before the lease passes.
 */
export const make = <N extends string>(service: Service<N>, name: string, permits: number, options?: { readonly lease?: Duration.Input }): Semaphore<N> => {
  if (typeof service?.name !== "string" || !Context.isKey((service as Partial<Service>).registered)) throw new Error("Restate.Semaphore.make needs a service from Restate.Semaphore.service")
  if (!name) throw new Error("A Restate semaphore needs a nonempty name")
  if (!Number.isSafeInteger(permits) || permits < 1) throw new Error(`Semaphore ${name}: permits must be a positive integer, got ${permits}`)
  const lease = Duration.fromInputUnsafe(options?.lease ?? "15 minutes")
  if (!Duration.isFinite(lease) || !Duration.isPositive(lease)) throw new Error(`Semaphore ${name}: the lease must be positive and finite`)
  return Object.freeze({ service, name, permits, lease })
}

/** The semaphores each attempt holds a permit of, so that a nested `withPermit` runs under it. */
const holding = new WeakMap<InvocationState, Set<string>>()

const permit = <A, E, R>(self: Semaphore<any>, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Invocation | Registered<any>> =>
  Effect.andThen(self.service.registered, Effect.flatMap(Invocation, invocation => {
    let held = holding.get(invocation)
    if (held === undefined) holding.set(invocation, held = new Set())
    const id = `${self.service.name}/${self.name}`
    if (held.has(id)) return effect
    const names = held
    const service = self.service.name
    const holder = invocation.ctx.request().id
    const acquire = Effect.gen(function* () {
      // A caught defect's release, still deferred, goes first.
      invocation.flush(`semaphore:${id}`)
      const granted = yield* awakeable(Schema.Void)
      yield* send(service, self.name, "acquire", {
        holder, awakeable: granted.id, permits: self.permits, lease: Math.ceil(Duration.toMillis(self.lease))
      })
      yield* granted.promise
      names.add(id)
    })
    const release = send(service, self.name, "release", { holder }).pipe(
      // A release that cannot be sent leaves the permit held until the lease passes: never silently.
      Effect.tapCause(cause => Effect.logError(`Restate.Semaphore ${self.name}: could not release the permit`, cause))
    )
    return Effect.onExit(Effect.andThen(acquire, effect), exit => Effect.suspend(() => {
      names.delete(id)
      if (Exit.isSuccess(exit) || isFinal(invocation, exit.cause)) return release
      // A defect that only ends the attempt when it ends the handler: it may yet be caught, as by
      // Effect.catchDefect. Release when the handler ends for good (or before taking the permit
      // again); if only the attempt ends, its retry keeps the permit.
      if (!invocation.ctx.request().attemptCompletedSignal.aborted) {
        invocation.defer(`semaphore:${id}`, () => { sendNow(invocation, service, self.name, "release", { holder }) })
      }
      return Effect.void
    }))
  }))

/**
 * Runs `effect` holding one permit, inside a handler of an implementation Layer that is provided
 * the semaphore's `service.layer` (or `layerRemote`), which the types require: waits for a permit durably, first in, first
 * out (a waiting invocation suspends, holding nothing), runs `effect`, then releases the permit
 * when it succeeds, fails for good, is cancelled or interrupted (as by `Effect.timeout`), also
 * while still waiting, as `Restate.onError` runs; after a caught defect, when the handler ends
 * with success or a final failure (or at the next `withPermit` of this semaphore). Not when only
 * an attempt ends: the invocation is the holder, so its retries and replays keep the permit, and a
 * nested `withPermit` of the same semaphore runs under it. A killed invocation keeps its permit
 * until the lease passes (see `make`).
 *
 * Durable cleanup around it (`Effect.ensuring`, `Effect.onExit`, `Effect.acquireRelease`) is not
 * replay-safe after a defect that may not repeat: use `Restate.onError`.
 *
 * Do not bound it with `Effect.timeout` or `Effect.race`: the permit is released, but the
 * interrupted durable operation stays pending, so the invocation cannot await another durable
 * operation until it settles (such awaits fail with a defect), and wall-clock interruption is not
 * replay-safe. For a durable timeout, race the durable operation inside it against
 * `Restate.sleep` with `Restate.raceAll`.
 */
export const withPermit: {
  <S extends Semaphore<any>>(self: S): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R | Invocation | RegisteredOf<S>>
  <S extends Semaphore<any>, A, E, R>(self: S, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Invocation | RegisteredOf<S>>
} = ((self: Semaphore<any>, effect?: Effect.Effect<any, any, any>) =>
  effect === undefined ? (effect: Effect.Effect<any, any, any>) => permit(self, effect) : permit(self, effect)) as any
