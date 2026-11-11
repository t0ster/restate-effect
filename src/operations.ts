import { Cause, Context, DateTime, Duration, Effect, Exit, Scope } from "effect"
import { dual } from "effect/Function"
import * as sdk from "@restatedev/restate-sdk"
import { cachedSerde, decodeFailure, exhaustedFailure, retryableFailure, singleFailure, unwrapExit } from "./codec.js"
import type { Codec } from "./codec.js"
import type { InvocationId } from "./client.js"
import { millis } from "./duration.js"
import { Durable, awaitPromise, decodeRejection, isCancellation, isFinal } from "./durable.js"
import { ClientError } from "./errors.js"
import { Invocation, Keyed, StateReader, StateWriter, current, stateOf, type InvocationState } from "./invocation.js"
import { defined, omitInvocationServices, type InvocationServices } from "./server.js"
import type { RunRetryPolicy } from "./contract.js"

/** A durable timer: survives restarts and replays. Cancellation interrupts it. */
export const sleep = (duration: Duration.Input): Durable<void, never, Invocation> =>
  new Durable(invocation => invocation.ctx.sleep(Duration.toMillis(duration)))

/** The current time, journaled: a replay sees the same value. */
export const now: Effect.Effect<DateTime.Utc, never, Invocation> = Effect.flatMap(current, invocation =>
  awaitPromise<number, never>(signal => invocation.execute(() => invocation.ctx.date.now(), signal))
).pipe(Effect.map(millis => DateTime.makeUnsafe(millis)))

/** The key of the current object or workflow. */
export const key: Effect.Effect<string, never, Keyed> = Effect.map(Keyed, invocation => (invocation.ctx as sdk.ObjectSharedContext).key)

/** The current invocation's ID. */
export const currentInvocationId: Effect.Effect<InvocationId, never, Invocation> =
  Effect.map(Invocation, invocation => invocation.ctx.request().id as unknown as InvocationId)

/**
 * Like `Effect.onError`, but runs only when the invocation fails for good: a declared failure,
 * a defect with `onDefect: "fail"`, or cancellation. Never when an attempt merely ends (suspension,
 * retry, pause). The cleanup can make durable calls and steps.
 */
export const onError: {
  <E, X, R2>(cleanup: (cause: Cause.Cause<E>) => Effect.Effect<X, never, R2>): <A, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R | R2 | Invocation>
  <A, E, R, X, R2>(self: Effect.Effect<A, E, R>, cleanup: (cause: Cause.Cause<E>) => Effect.Effect<X, never, R2>): Effect.Effect<A, E, R | R2 | Invocation>
} = dual(2, <A, E, R, X, R2>(self: Effect.Effect<A, E, R>, cleanup: (cause: Cause.Cause<E>) => Effect.Effect<X, never, R2>) =>
  Effect.onError(self, cause => Effect.flatMap(current, invocation => isFinal(invocation, cause) ? Effect.asVoid(cleanup(cause)) : Effect.void)))

/**
 * Registers `finalizer` to run when the invocation ends for good: it succeeds, fails for good (a
 * declared failure, or a defect with `onDefect: "fail"`), or is cancelled. Never when only the
 * attempt ends (suspension, retry, pause): a retry replays the handler, which registers it again.
 * Like `Workflow.addFinalizer`. It runs after the handler's own scope has closed, with the
 * services of where it was registered, so it can make durable calls and steps, but not use scoped
 * resources of the handler. A defect in it fails the handler as one.
 */
export const addFinalizer = <X, R>(finalizer: (exit: Exit.Exit<unknown, unknown>) => Effect.Effect<X, never, R>): Effect.Effect<void, never, R | Invocation> =>
  Effect.flatMap(current, invocation => Effect.flatMap(Effect.context<R>(), context =>
    Scope.addFinalizerExit(invocation.scope, exit => Effect.provideContext(finalizer(exit), context))))

/** Runs the cleanups of `ensuring` deferred under `key` now. */
const flushDeferred = (invocation: InvocationState, key: string): Effect.Effect<void> => Effect.suspend(() => {
  const scopes = invocation.deferred.get(key) ?? []
  invocation.deferred.delete(key)
  return Effect.forEach(scopes, scope => Scope.close(scope, Exit.void), { discard: true })
})

const ensure = <A, E, R, X, R2>(self: Effect.Effect<A, E, R>, cleanup: Effect.Effect<X, never, R2>, options?: EnsuringOptions): Effect.Effect<A, E, R | R2 | Invocation> =>
  Effect.uninterruptibleMask(restore => Effect.gen(function* () {
    const invocation = yield* current
    if (options?.key !== undefined) yield* flushDeferred(invocation, options.key)
    const run = Effect.provideContext(Effect.asVoid(cleanup), yield* Effect.context<R2>())
    const exit = yield* Effect.exit(restore(self))
    if (Exit.isSuccess(exit) || isFinal(invocation, exit.cause)) {
      yield* run
    } else if (!invocation.ctx.request().attemptCompletedSignal.aborted) {
      // A defect that ends only the attempt when it ends the handler: it may yet be caught. Run
      // the cleanup when the invocation ends for good, or at the next `ensuring` with this key.
      const scope = Scope.forkUnsafe(invocation.scope)
      yield* Scope.addFinalizer(scope, run)
      if (options?.key !== undefined) invocation.deferred.set(options.key, [...invocation.deferred.get(options.key) ?? [], scope])
    }
    return yield* exit
  }))

export interface EnsuringOptions {
  /**
   * A later `ensuring` with the same key, in the same attempt, runs the cleanup this one deferred
   * after a defect before it runs its own effect, instead of at the end of the invocation: as a
   * lock released before it is taken again.
   */
  readonly key?: string
}

/**
 * A replay-safe `Effect.ensuring`: runs `cleanup` when `self` succeeds, fails for good (a declared
 * failure, or a defect with `onDefect: "fail"`), or is interrupted or cancelled. After a defect
 * that may only end the attempt, `cleanup` is deferred until the invocation ends for good, so it
 * runs if the defect is caught. Never when only the attempt ends (suspension, retry, pause): its
 * retry replays `self` and runs `cleanup` then. The cleanup runs uninterruptibly and can make
 * durable calls and steps; a defect in it fails the handler as one.
 *
 * After an interruption by Effect (as by `Effect.timeout`), the interrupted durable operation
 * stays pending, so the cleanup cannot await another until it settles: issue one-way sends with
 * `Restate.fork(client.method(input, { discard: true }))`.
 */
export const ensuring: {
  <X, R2>(cleanup: Effect.Effect<X, never, R2>, options?: EnsuringOptions): <A, E, R>(self: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R | R2 | Invocation>
  <A, E, R, X, R2>(self: Effect.Effect<A, E, R>, cleanup: Effect.Effect<X, never, R2>, options?: EnsuringOptions): Effect.Effect<A, E, R | R2 | Invocation>
} = dual(args => Effect.isEffect(args[0]) && Effect.isEffect(args[1]), ensure)

/** Cancels the invocation `invocationId`, one way. Journaled: a replay does not cancel it again. */
export const cancel = (invocationId: InvocationId): Effect.Effect<void, never, Invocation> =>
  Effect.flatMap(current, invocation => Effect.sync(() => {
    invocation.assertReady()
    invocation.ctx.invocation(invocationId as unknown as sdk.InvocationId).cancel()
  }))

/**
 * How `Restate.run` retries: declared failures matching `while`, and every defect. Fields left out
 * come from the handler's `runRetry` server option; `while` and `delay` are per step only.
 */
export interface RunRetry<E> extends RunRetryPolicy {
  /** Retry this declared failure; others are final at once. Defects are always retried. */
  readonly while?: (error: E) => boolean
  /**
   * How long to wait before the next attempt after this retried declared failure, such as a rate
   * limit's Retry-After; undefined (or an infinite duration) uses the backoff of `initialInterval`,
   * `exponentiationFactor` and `maxInterval`. Restate adds no jitter to it: add your own here. It
   * runs inside the failed attempt and only tells Restate when to retry, so it may be random.
   * `maxAttempts` and `maxDuration` still bound the retries; `maxDuration` is checked when an
   * attempt fails, so a delay can start the next attempt after it. Without either, a delay of 0
   * retries at once, forever: bound delayed retries. If `delay` (or `while`) throws, the step
   * retries as a defect and ends as one, not as the declared failure.
   */
  readonly delay?: (error: E) => Duration.Input | undefined
}

/**
 * Runs external I/O as one journaled step: a replay returns the journaled result instead of
 * running it again. Defects are retried, and die once retries are exhausted; declared failures are
 * final unless `retry.while` matches, and once retries are exhausted the last one is returned;
 * `retry.delay` can set the wait before the next attempt from the failure. The action must not
 * depend on Restate operations or durable clients.
 */
export function run<A extends Codec, E extends Codec, R>(
  name: string,
  action: Effect.Effect<A["Type"], E["Type"], R> & (Extract<R, InvocationServices> extends never ? unknown : never),
  options: { readonly success: A; readonly error: E; readonly retry?: RunRetry<E["Type"]> }
): Durable<A["Type"], E["Type"], R | Invocation> {
  return new Durable((invocation, context) => {
    const retry: RunRetry<E["Type"]> = { ...stateOf(invocation).runRetry, ...defined(options.retry ?? {}) }
    // The public signature excludes these services from R; also remove them at runtime.
    const isolated = omitInvocationServices(context as Context.Context<any>) as Context.Context<R>
    const step = invocation.ctx.run(name, async () => {
      const exit = await Effect.runPromiseExitWith(isolated)(Effect.scoped(action), {
        signal: invocation.ctx.request().attemptCompletedSignal
      })
      const declared = Exit.isFailure(exit) ? singleFailure(exit.cause) : undefined
      if (declared && retry.while?.(declared.error)) {
        const retryAfter = retryDelay(retry.delay?.(declared.error))
        // The SDK logs a retried step only when it uses the backoff: log a delayed retry here.
        if (retryAfter !== undefined) {
          Effect.runSyncWith(isolated)(Effect.logWarning(`Restate.run '${name}' failed; retrying in ${retryAfter}ms`, declared.error))
        }
        throw retryableFailure(declared.error, options.error, retryAfter)
      }
      return unwrapExit(exit, options.error)
    }, {
      serde: cachedSerde(options.success),
      maxRetryAttempts: retry.maxAttempts,
      initialRetryInterval: millis(retry.initialInterval),
      maxRetryInterval: millis(retry.maxInterval),
      retryIntervalFactor: retry.exponentiationFactor,
      maxRetryDuration: millis(retry.maxDuration)
    })
    // Exhausted retries of a declared failure carry it in the message; of a defect, still a defect.
    return decodeRejection(step, failure => decodeFailure(failure, options.error) ?? exhaustedFailure(failure, options.error))
  })
}

/** A step's retry delay in whole milliseconds, as Restate takes it; undefined when infinite, so the backoff applies. */
const retryDelay = (delay: Duration.Input | undefined): number | undefined => {
  const millis = delay === undefined ? undefined : Duration.toMillis(delay)
  return millis === undefined || !Number.isFinite(millis) ? undefined : Math.max(0, Math.ceil(millis))
}

/** A durable promise that another invocation resolves by `id`, with `Restate.resolveAwakeable`. */
export interface Awakeable<A> {
  /** Pass it to whoever resolves the awakeable. */
  readonly id: string
  /**
   * The value it is resolved with. Awaiting it suspends the invocation, holding nothing, until then;
   * cancellation interrupts it. A Durable, so `Restate.raceAll` can race it against a timeout.
   */
  readonly promise: Durable<A, never, Invocation>
}

/**
 * Creates an awakeable: a durable promise another invocation resolves by its `id`. Its value is
 * decoded with `schema`. A replay creates the same awakeable, with the same ID.
 */
export const awakeable = <S extends Codec>(schema: S): Effect.Effect<Awakeable<S["Type"]>, never, Invocation> =>
  Effect.flatMap(current, invocation => Effect.sync(() => {
    invocation.assertReady()
    const { id, promise } = invocation.ctx.awakeable(cachedSerde(schema))
    return { id, promise: new Durable<S["Type"], never, Invocation>(() => promise) }
  }))

/** Resolves the awakeable `id` with `value`, encoded with `schema`, waking the invocation awaiting it. Journaled: a replay does not resolve it again. */
export const resolveAwakeable = <S extends Codec>(id: string, schema: S, value: S["Type"]): Effect.Effect<void, never, Invocation> =>
  Effect.flatMap(current, invocation => Effect.sync(() => {
    invocation.assertReady()
    // Encoded here: the SDK would send an undefined value (Schema.Void's) as an empty body, not as the schema encodes it.
    invocation.ctx.resolveAwakeable(id, cachedSerde(schema).serialize(value), sdk.serde.binary)
  }))

/** A state write: only exclusive object handlers and workflow `run` can write. */
const writeState = (write: (ctx: sdk.ObjectContext) => void): Effect.Effect<void, never, StateWriter> =>
  Effect.flatMap(Effect.map(StateWriter, stateOf), invocation => Effect.sync(() => {
    if (!invocation.writable) throw new Error("State writes require an exclusive object handler or workflow run")
    invocation.assertReady()
    write(invocation.ctx as sdk.ObjectContext)
  }))

export const state = {
  get: <S extends Codec>(key: string, schema: S): Effect.Effect<S["Type"] | null, ClientError, StateReader> =>
    Effect.flatMap(Effect.map(StateReader, stateOf), invocation => Effect.tryPromise({
      try: signal => invocation.execute(() => {
        if (!invocation.stateful) throw new Error("State is only available on objects and workflows")
        return (invocation.ctx as sdk.ObjectSharedContext).get(key, cachedSerde(schema))
      }, signal),
      catch: cause => new ClientError({ service: "<invocation>", method: "get", cause })
    }).pipe(
      // Its errors stay in the typed channel; cancellation interrupts.
      Effect.catch(error => isCancellation(error.cause) ? Effect.interrupt : Effect.fail(error))
    )),
  set: <S extends Codec>(key: string, schema: S, value: S["Type"]): Effect.Effect<void, never, StateWriter> =>
    writeState(ctx => ctx.set(key, value, cachedSerde(schema))),
  clear: (key: string): Effect.Effect<void, never, StateWriter> =>
    writeState(ctx => ctx.clear(key))
}
