import { Cause, Context, DateTime, Duration, Effect, Exit } from "effect"
import { dual } from "effect/Function"
import * as sdk from "@restatedev/restate-sdk"
import { decodeFailure, exhaustedFailure, retryableFailure, serde, unwrapExit } from "./codec.js"
import type { Codec } from "./codec.js"
import { Transport, type InvocationId } from "./client.js"
import { DeclaredFailure, Durable, awaitPromise, isCancellation, isFinal } from "./durable.js"
import { ClientError } from "./errors.js"
import { Invocation, Keyed, StateReader, StateWriter } from "./invocation.js"

const millis = (duration: Duration.Input | undefined) => duration === undefined ? undefined : Duration.toMillis(duration)
const invocationError = (method: string, cause: unknown) => new ClientError({ service: "<invocation>", method, cause })

/** A durable timer: survives restarts and replays. Cancellation interrupts it. */
export const sleep = (duration: Duration.Input): Durable<void, never, Invocation> =>
  new Durable(invocation => invocation.ctx.sleep(Duration.toMillis(duration)))

/** The current time, journaled: a replay sees the same value. */
export const now: Effect.Effect<DateTime.Utc, never, Invocation> = Effect.flatMap(Invocation, invocation =>
  awaitPromise<number, never>(() => invocation.execute(() => invocation.ctx.date.now()))
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
  Effect.onError(self, cause => Effect.flatMap(Invocation, invocation => isFinal(invocation, cause) ? Effect.asVoid(cleanup(cause)) : Effect.void)))

/** How `Restate.run` retries: declared failures matching `while`, and every defect. */
export interface RunRetry<E> {
  /** Retry this declared failure; others are final at once. Defects are always retried. */
  readonly while?: (error: E) => boolean
  readonly maxAttempts?: number
  readonly initialInterval?: Duration.Input
  readonly maxInterval?: Duration.Input
  readonly exponentiationFactor?: number
  readonly maxDuration?: Duration.Input
}

/**
 * Runs external I/O as one journaled step: a replay returns the journaled result instead of
 * running it again. Defects are retried, and die once retries are exhausted; declared failures are
 * final unless `retry.while` matches, and once retries are exhausted the last one is returned. The action must not depend on Restate
 * operations or durable clients.
 */
export function run<A extends Codec, E extends Codec, R>(
  name: string,
  action: Effect.Effect<A["Type"], E["Type"], R> & (Extract<R, Invocation | Transport | StateReader | StateWriter | Keyed> extends never ? unknown : never),
  options: { readonly success: A; readonly error: E; readonly retry?: RunRetry<E["Type"]> }
): Durable<A["Type"], E["Type"], R | Invocation> {
  const retry = options.retry
  return new Durable((invocation, context) => {
    // The public signature excludes these services from R; also remove them at runtime.
    const isolated = Context.omit(Invocation, Transport, StateReader, StateWriter, Keyed)(context as Context.Context<any>) as Context.Context<R>
    return invocation.ctx.run(name, async () => {
      const exit = await Effect.runPromiseExitWith(isolated)(Effect.scoped(action), {
        signal: invocation.ctx.request().attemptCompletedSignal
      })
      if (Exit.isFailure(exit) && exit.cause.reasons.length === 1 && Cause.isFailReason(exit.cause.reasons[0])
        && retry?.while?.(exit.cause.reasons[0].error)) {
        throw retryableFailure(exit.cause.reasons[0].error, options.error)
      }
      return unwrapExit(exit, options.error)
    }, {
      serde: serde(options.success),
      maxRetryAttempts: retry?.maxAttempts,
      initialRetryInterval: millis(retry?.initialInterval),
      maxRetryInterval: millis(retry?.maxInterval),
      retryIntervalFactor: retry?.exponentiationFactor,
      maxRetryDuration: millis(retry?.maxDuration)
    }).map((value, failure) => {
      if (failure === undefined) return value
      if (isCancellation(failure)) throw failure
      const declared = decodeFailure(failure, options.error) ?? exhaustedFailure(failure, options.error)
      if (declared) throw new DeclaredFailure(declared.value)
      // Retries of a defect exhausted: still a defect.
      throw failure
    })
  })
}

/** A state operation; its errors stay in the typed channel, cancellation interrupts. */
const stateOperation = <A>(method: string, f: () => PromiseLike<A>, execute: (f: () => PromiseLike<A>) => Promise<A>) =>
  Effect.tryPromise({ try: () => execute(f), catch: cause => invocationError(method, cause) }).pipe(
    Effect.catch(error => isCancellation(error.cause) ? Effect.interrupt : Effect.fail(error))
  )

export const state = {
  get: <S extends Codec>(key: string, schema: S): Effect.Effect<S["Type"] | null, ClientError, StateReader> =>
    Effect.flatMap(StateReader, invocation => stateOperation("get", () => {
      if (!invocation.stateful) throw new Error("State is only available on objects and workflows")
      return (invocation.ctx as sdk.ObjectSharedContext).get(key, serde(schema))
    }, invocation.execute)),
  set: <S extends Codec>(key: string, schema: S, value: S["Type"]): Effect.Effect<void, never, StateWriter> =>
    Effect.flatMap(StateWriter, invocation => Effect.sync(() => {
      if (!invocation.writable) throw new Error("State writes require an exclusive object handler or workflow run")
      invocation.assertReady()
      ;(invocation.ctx as sdk.ObjectContext).set(key, value, serde(schema))
    })),
  clear: (key: string): Effect.Effect<void, never, StateWriter> =>
    Effect.flatMap(StateWriter, invocation => Effect.sync(() => {
      if (!invocation.writable) throw new Error("State writes require an exclusive object handler or workflow run")
      invocation.assertReady()
      ;(invocation.ctx as sdk.ObjectContext).clear(key)
    }))
}
