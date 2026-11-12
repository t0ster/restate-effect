/**
 * Durable semaphores, shaped like Effect's `Semaphore` module: at most `permits` invocations hold
 * a permit at once, across every handler, process and restart, such as callers of an API with a
 * concurrency limit. A semaphore is a description; its permits live in Restate, in a virtual object
 * that the app names and registers, made by `service`.
 *
 * Built only on the public API of `@t0ster/restate-effect` (`Restate.ensuring`, awakeables,
 * one-way sends, `Restate.cancel`, state).
 */
import { Context, Duration, Effect, Layer, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"
import { clientOf, layer as objectLayer, type Limits } from "./object.js"

declare const RegisteredId: unique symbol
/**
 * Proof that the endpoint serves (or, with `layerRemote`, that another deployment serves) the
 * semaphore `K` of the object `N`: `withPermit` requires it, so a forgotten registration is a type error.
 */
export interface Registered<N extends string, K extends string = any> {
  readonly [RegisteredId]: (name: N, key: K) => [N, K]
}
/** What `withPermit` of `S`, or of each semaphore in a union `S`, requires. */
export type RegisteredOf<S extends Semaphore<any, any>> = S extends Semaphore<infer N, infer K> ? Registered<N, K> : never

/**
 * The virtual object that keeps semaphores' permits, one key per semaphore, and owns their limits.
 * Register `layer(...semaphores)` in the app's endpoint, once in the cluster. Its handlers are
 * internal: only `withPermit` calls them.
 *
 * `Service` without a type argument accepts any semaphore object.
 */
export interface Service<N extends string = any> {
  /** The object's name in Restate. */
  readonly name: N
  /**
   * Implements the object for these semaphores, with their limits (`permits`, `lease`): registers
   * it with the endpoint, like a `toLayer` Layer, and provides `Registered` for each. Provide it to
   * the implementation Layers whose handlers use `withPermit`. A limit changed here applies on
   * deploy, from the object's next operation.
   */
  readonly layer: <const S extends ReadonlyArray<Semaphore<N, any>>>(...semaphores: S) => Layer.Layer<RegisteredOf<S[number]>, never, Restate.Endpoint>
  /**
   * Provides `Registered` for these semaphores without serving the object, for a deployment that
   * uses permits of an object that another deployment of the cluster registers. Their limits are
   * not used here: the serving deployment's apply.
   */
  readonly layerRemote: <const S extends ReadonlyArray<Semaphore<N, any>>>(...semaphores: S) => Layer.Layer<RegisteredOf<S[number]>>
}

const registeredKey = (service: string, name: string) =>
  Context.Service<Registered<any, any>, string>(`restate-effect/Semaphore/Registered/${service}/${name}`)

/**
 * Makes the semaphore object named `name`, unique in the Restate cluster. Every semaphore made
 * with it (see `make`) keeps its permits there.
 *
 * ```ts
 * export const Semaphores = Semaphore.service("app.Semaphores")
 * export const gateway = Semaphore.make(Semaphores, "llm-gateway", 4)
 * // in the endpoint: handlers that use withPermit(gateway) require it
 * GreeterLive.pipe(Layer.provideMerge(Semaphores.layer(gateway)), Layer.provide(Restate.layerHttp))
 * ```
 */
export const service = <const N extends string>(name: N): Service<N> => {
  const check = (semaphores: ReadonlyArray<Semaphore<any, any>>) => {
    const limits = new Map<string, Limits>()
    for (const semaphore of semaphores) {
      if (semaphore?.service?.name !== name) throw new Error(`${name}.layer: semaphore ${semaphore?.name} belongs to ${semaphore?.service?.name}`)
      const lease = Math.ceil(Duration.toMillis(semaphore.lease))
      const known = limits.get(semaphore.name)
      if (known !== undefined && (known.permits !== semaphore.permits || known.lease !== lease))
        throw new Error(`${name}.layer: semaphore ${semaphore.name} is given twice, with different limits`)
      limits.set(semaphore.name, { permits: semaphore.permits, lease })
    }
    return limits
  }
  const proof = (limits: ReadonlyMap<string, Limits>): Layer.Layer<any> =>
    Layer.succeedContext([...limits.keys()].reduce((context, key) => Context.add(context, registeredKey(name, key), key), Context.empty() as Context.Context<any>))
  const layerRemote = (...semaphores: ReadonlyArray<Semaphore<any, any>>) => proof(check(semaphores))
  const layer = (...semaphores: ReadonlyArray<Semaphore<any, any>>) => {
    const limits = check(semaphores)
    return Layer.merge(objectLayer(name, limits), proof(limits))
  }
  return Object.freeze({ name, layer, layerRemote }) as Service<N>
}

/**
 * A durable counting semaphore. Make one with `make`; use it with `withPermit`. `Semaphore`
 * without type arguments accepts any semaphore; `withPermit` of one then requires any
 * `Registered`, so it no longer checks which object's Layer is provided.
 */
export interface Semaphore<N extends string = any, K extends string = any> {
  /** The object that keeps its permits. */
  readonly service: Service<N>
  /** Unique in its service: every description with this name and service shares its permits. */
  readonly name: K
  /** How many invocations hold a permit at once, at most: applied by the deployment that serves the object. */
  readonly permits: number
  /** How long a holder keeps its permit, at most: then the semaphore reclaims it. Applied by the serving deployment. */
  readonly lease: Duration.Duration
}

/**
 * Describes a semaphore, kept by `service` (see `service`). Its state lives in Restate, so this is
 * a plain value, like a contract. Its limits apply where it is given to `service.layer`: the
 * object owns them, so changing them is safe on a live server. A raise frees slots at the object's
 * next operation; a cut does not revoke current holders.
 *
 * `lease` (default 15 minutes) bounds how long a holder keeps its permit, so a killed holder,
 * which never releases it, frees it eventually. A holder that is still running when its lease
 * passes loses the permit without being told, so set it above the longest hold, retries and
 * suspensions included. The default is above Restate's default inactivity and abort timeouts
 * (1 and 10 minutes): a stuck attempt is retried, keeping its permit, before the lease passes.
 */
export const make = <N extends string, const K extends string>(service: Service<N>, name: K, permits: number, options?: { readonly lease?: Duration.Input }): Semaphore<N, K> => {
  if (typeof service?.name !== "string" || typeof (service as Partial<Service>).layer !== "function") throw new Error("Semaphore.make needs a service from Semaphore.service")
  if (!name) throw new Error("A Restate semaphore needs a nonempty name")
  if (!Number.isSafeInteger(permits) || permits < 1) throw new Error(`Semaphore ${name}: permits must be a positive integer, got ${permits}`)
  const lease = Duration.fromInputUnsafe(options?.lease ?? "15 minutes")
  if (!Duration.isFinite(lease) || !Duration.isPositive(lease)) throw new Error(`Semaphore ${name}: the lease must be positive and finite`)
  return Object.freeze({ service, name, permits, lease })
}

/** The semaphores whose permit the current fiber runs under, so that a nested `withPermit` runs under it. */
const Held = Context.Reference<ReadonlySet<string>>("@t0ster/restate-effect/semaphore/Held", { defaultValue: () => new Set() })

const permit = <A, E, R>(self: Semaphore<any, any>, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Restate.Invocation | Registered<any, any>> =>
  Effect.gen(function* () {
    yield* registeredKey(self.service.name, self.name)
    const id = `${self.service.name}/${self.name}`
    const held = yield* Held
    if (held.has(id)) return yield* effect
    const semaphore = clientOf(self.service.name).key(self.name)
    const holder = yield* Restate.currentInvocationId
    const acquire = Effect.gen(function* () {
      const granted = yield* Restate.awakeable(Schema.Void)
      // One way, without awaiting the send's invocation ID.
      yield* Restate.fork(semaphore.acquire({
        holder, awakeable: granted.id }, { discard: true }))
      yield* granted.promise
    })
    // Not awaited, so it works after Effect.timeout interrupted a durable operation that is still pending.
    const release = Restate.fork(semaphore.release({ holder }, { discard: true })).pipe(
      // A release that cannot be sent leaves the permit held until the lease passes: never silently.
      Effect.tapCause(cause => Effect.logError(`Restate.Semaphore ${self.name}: could not release the permit`, cause))
    )
    // The key: after a caught defect, the deferred release goes before the next acquire of this semaphore.
    return yield* Effect.andThen(acquire, Effect.provideService(effect, Held, new Set([...held, id]))).pipe(
      Restate.ensuring(release, { key: `semaphore:${id}` })
    )
  })

/**
 * Runs `effect` holding one permit, inside a handler of an implementation Layer that is provided
 * the semaphore's `service.layer(semaphore, …)` (or `layerRemote`), which the types require: waits for a permit durably, first in, first
 * out (a waiting invocation suspends, holding nothing), runs `effect`, then releases the permit
 * when it succeeds, fails for good, is cancelled or interrupted (as by `Effect.timeout`), also
 * while still waiting, as `Restate.onError` runs; after a caught defect, when the handler ends
 * with success or a final failure (or at the next `withPermit` of this semaphore). Not when only
 * an attempt ends: the invocation is the holder, so its retries and replays keep the permit, and a
 * nested `withPermit` of the same semaphore runs under it. A killed invocation keeps its permit
 * until the lease passes (see `make`).
 *
 * Durable cleanup around it with `Effect.ensuring`, `Effect.onExit` or `Effect.acquireRelease` is
 * not replay-safe after a defect that may not repeat: use `Restate.ensuring` or `Restate.onError`.
 *
 * Do not bound it with `Effect.timeout` or `Effect.race`: the permit is released, but the
 * interrupted durable operation stays pending, so the invocation cannot await another durable
 * operation until it settles (such awaits fail with a defect), and wall-clock interruption is not
 * replay-safe. For a durable timeout, race the durable operation inside it against
 * `Restate.sleep` with `Restate.raceAll`.
 */
export const withPermit: {
  <S extends Semaphore<any, any>>(self: S): <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R | Restate.Invocation | RegisteredOf<S>>
  <S extends Semaphore<any, any>, A, E, R>(self: S, effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R | Restate.Invocation | RegisteredOf<S>>
} = ((self: Semaphore<any, any>, effect?: Effect.Effect<any, any, any>) =>
  effect === undefined ? (effect: Effect.Effect<any, any, any>) => permit(self, effect) : permit(self, effect)) as any
