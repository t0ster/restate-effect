import { Cause, Context, Effect, Effectable, Result } from "effect"
import type { Pipeable } from "effect"
import * as sdk from "@restatedev/restate-sdk"
import { singleFailure } from "./codec.js"
import { Invocation, stateOf, type InvocationApi, type InvocationState } from "./invocation.js"
import type { ClientError } from "./errors.js"
import type { Transport } from "./client.js"

/**
 * A declared failure travelling through a RestatePromise chain, so Restate's own combinators
 * apply (`all` fails on the first one, `any` waits for a success). It never leaves the handler.
 */
export class DeclaredFailure extends sdk.TerminalError {
  constructor(readonly value: unknown) { super("Declared failure") }
}

/** Restate's code for a cancelled invocation; a caller sees it as a plain TerminalError. */
export const isCancellation = (error: unknown) =>
  error instanceof sdk.CancelledError || (error instanceof sdk.TerminalError && error.code === 409 && error.message === "Cancelled")

/**
 * Rejects with a DeclaredFailure when `decode` finds one in the promise's TerminalError; any other
 * failure (cancellation, a remote defect) rejects as is.
 */
export const decodeRejection = <A>(
  promise: sdk.RestatePromise<A>,
  decode: (failure: sdk.TerminalError) => { readonly value: unknown } | undefined
): sdk.RestatePromise<A> =>
  promise.map((value, failure) => {
    if (failure === undefined) return value as A
    if (isCancellation(failure)) throw failure
    const declared = decode(failure)
    if (declared) throw new DeclaredFailure(declared.value)
    throw failure
  })

/** How a rejected durable operation continues in Effect: typed failure, interruption, or defect. */
export function fromRejection<E>(error: unknown): Effect.Effect<never, E> {
  if (error instanceof DeclaredFailure) return Effect.fail(error.value as E)
  if (isCancellation(error)) return Effect.interrupt
  return Effect.die(error)
}

/**
 * Awaits a RestatePromise (or any promise) as an Effect, following the rejection protocol. The
 * signal aborts if the Effect is interrupted, as by `Effect.timeout`; the promise keeps running.
 */
export const awaitPromise = <A, E>(promise: (signal: AbortSignal) => PromiseLike<A>): Effect.Effect<A, E> =>
  Effect.callback<A, E>((resume, signal) => {
    promise(signal).then(value => resume(Effect.succeed(value)), error => resume(fromRejection<E>(error)))
  })

/**
 * A durable operation: an Effect you can `yield*`, which Restate's combinators can also start
 * without awaiting. `issue` starts it inside a handler and returns its RestatePromise; it must be
 * pure apart from issuing its command, so replays issue the same commands in the same order.
 */
export class Durable<A, E = never, R = Invocation> extends Effectable.Class<A, E, R> implements Pipeable.Pipeable {
  constructor(
    readonly issue: (invocation: InvocationApi, context: Context.Context<R>) => sdk.RestatePromise<A>,
    /** How `yield*` runs it; by default it is issued and awaited inside a handler. */
    private readonly effect?: Effect.Effect<A, E, R>
  ) { super() }
  asEffect(): Effect.Effect<A, E, R> {
    return this.effect ?? Effect.flatMap(
      Effect.context<R | Invocation>(),
      context => {
        const invocation = Context.get(context, Invocation)
        return awaitPromise<A, E>(signal => stateOf(invocation).execute(() => this.issue(invocation, context as Context.Context<R>), signal))
      }
    ) as Effect.Effect<A, E, R>
  }
}

/*
 * Combinators run inside a handler and issue commands directly, so they need no Transport, and
 * their results carry no ClientError: there, a callee's undeclared failure is a defect (it dies),
 * as a remote defect is in Effect RPC.
 */
type Values<T extends ReadonlyArray<Durable<any, any, any>>> = { -readonly [K in keyof T]: T[K] extends Durable<infer A, any, any> ? A : never }
type Errors<T extends ReadonlyArray<Durable<any, any, any>>> = T[number] extends Durable<any, infer E, any> ? Exclude<E, ClientError> : never
type Services<T extends ReadonlyArray<Durable<any, any, any>>> = T[number] extends Durable<any, any, infer R> ? Exclude<R, Transport> : never
type Inside<R> = Exclude<R, Transport> | Invocation

/** Like `Effect.all`, but concurrent and durable: results in input order; fails on the first failure. */
export const all = <const T extends ReadonlyArray<Durable<any, any, any>>>(durables: T): Durable<Values<T>, Errors<T>, Services<T> | Invocation> =>
  new Durable((invocation, context) => sdk.RestatePromise.all(durables.map(d => d.issue(invocation, context as never))) as sdk.RestatePromise<Values<T>>)

/** Like `Effect.raceAll`: the first success, or the last failure if all fail. The others keep running. */
export const raceAll = <const T extends ReadonlyArray<Durable<any, any, any>>>(durables: T): Durable<Values<T>[number], Errors<T>, Services<T> | Invocation> =>
  new Durable((invocation, context) => sdk.RestatePromise.any(durables.map(d => d.issue(invocation, context as never))).map((value, failure) => {
    if (failure === undefined) return value as Values<T>[number]
    const errors = (failure as unknown as { readonly errors?: ReadonlyArray<unknown> }).errors
    throw errors?.at(-1) ?? failure
  }))

/** Like `Effect.result`: a declared failure becomes a `Result` value. */
export const result = <A, E, R>(self: Durable<A, E, R>): Durable<Result.Result<A, Exclude<E, ClientError>>, never, Inside<R>> =>
  new Durable((invocation, context) => self.issue(invocation, context as never).map((value, failure) => {
    if (failure === undefined) return Result.succeed(value as A)
    if (failure instanceof DeclaredFailure) return Result.fail(failure.value as Exclude<E, ClientError>)
    throw failure
  }))

/** Like `Effect.map`; `f` must be pure: it cannot start durable operations. */
export const map = <A, B>(f: (a: A) => B) => <E, R>(self: Durable<A, E, R>): Durable<B, Exclude<E, ClientError>, Inside<R>> =>
  new Durable((invocation, context) => self.issue(invocation, context as never).map((value, failure) => {
    if (failure !== undefined) throw failure
    return f(value as A)
  }))

/** Like `Effect.asVoid`. */
export const asVoid = <A, E, R>(self: Durable<A, E, R>): Durable<void, Exclude<E, ClientError>, Inside<R>> => map<A, void>(() => undefined)(self)

/** Like `Effect.mapError`, for declared failures. */
export const mapError = <E, E2>(f: (e: Exclude<E, ClientError>) => E2) => <A, R>(self: Durable<A, E, R>): Durable<A, E2, Inside<R>> =>
  new Durable((invocation, context) => self.issue(invocation, context as never).map((value, failure) => {
    if (failure === undefined) return value as A
    throw failure instanceof DeclaredFailure ? new DeclaredFailure(f(failure.value as Exclude<E, ClientError>)) : failure
  }))

/** Like `Effect.filterOrFail`: fails with `orFail(value)` when `predicate` rejects the value. */
export const filterOrFail = <A, E2>(predicate: (a: A) => boolean, orFail: (a: A) => E2) => <E, R>(self: Durable<A, E, R>): Durable<A, Exclude<E, ClientError> | E2, Inside<R>> =>
  map((value: A) => {
    if (!predicate(value)) throw new DeclaredFailure(orFail(value))
    return value
  })(self)

/** A durable operation already started by `fork`; joining it starts nothing new. */
export interface Fiber<A, E = never> {
  readonly promise: sdk.RestatePromise<A>
  /** Type-level only. */
  readonly _E?: (_: never) => E
}

/** Starts a durable operation now, without awaiting it. */
export const fork = <A, E, R>(self: Durable<A, E, R>): Effect.Effect<Fiber<A, Exclude<E, ClientError>>, never, Inside<R>> =>
  Effect.flatMap(Effect.context<Inside<R>>(), context => Effect.sync(() => {
    const invocation = Context.get(context, Invocation)
    stateOf(invocation).assertReady()
    return { promise: self.issue(invocation, context as unknown as Context.Context<R>) }
  }))

/** Like `Fiber.join`: the forked operation's result. */
export const join = <A, E>(fiber: Fiber<A, E>): Durable<A, E, Invocation> => new Durable(() => fiber.promise)

/** Whether a Cause ends the invocation for good, so `Restate.onError` should run. */
export const isFinal = (invocation: InvocationState, cause: Cause.Cause<unknown>): boolean => {
  if (invocation.ctx.request().attemptCompletedSignal.aborted) return false
  if (Cause.hasInterruptsOnly(cause)) return true                                    // cancellation
  if (singleFailure(cause)) return true                                              // declared failure
  return invocation.onDefect === "fail"                                              // defect
}
