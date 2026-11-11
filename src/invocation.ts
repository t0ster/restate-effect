import { Context, Effect, Scope } from "effect"
import type * as sdk from "@restatedev/restate-sdk"
import type { RunRetryPolicy } from "./contract.js"

/**
 * The current handler attempt, as handlers see it: the raw Restate SDK context, for Restate
 * features this library does not wrap. Commands issued on it bypass the library's checks.
 */
export interface InvocationApi {
  readonly ctx: sdk.Context
}

/** The adapter's own state of an attempt; never exported from the package. */
export interface InvocationState {
  readonly ctx: sdk.Context
  readonly writable: boolean
  readonly stateful: boolean
  /** What a defect in this handler does; decides whether `Restate.onError` runs for one. */
  readonly onDefect: "pause" | "fail"
  /** The handler's default `Restate.run` retry. */
  readonly runRetry: RunRetryPolicy
  /** Throws if no durable operation can start now. */
  readonly assertReady: () => void
  /**
   * Runs one awaited durable operation; rejects a second one while the first is pending. When
   * `signal` aborts (the awaiting Effect was interrupted, as by `Effect.timeout`), commands that
   * are issued without being awaited (one-way sends through `Restate.fork`, state writes,
   * resolving awakeables, cancellation) can go on, so cleanup runs; awaiting another durable
   * operation fails loudly until the abandoned one settles, as the SDK awaits one at a time.
   */
  readonly execute: <A>(operation: () => PromiseLike<A>, signal?: AbortSignal) => Promise<A>
  /**
   * Closed (by the adapter) only when the invocation ends for good; never when only the attempt
   * ends, so a retry, which makes a new one, replays without its finalizers.
   */
  readonly scope: Scope.Closeable
  /** Cleanups of `Restate.ensuring` deferred under a key: child scopes of `scope`. */
  readonly deferred: Map<string, Array<Scope.Closeable>>
}

const states = new WeakMap<InvocationApi, InvocationState>()
/** The adapter's state behind the public view of an attempt. */
export const stateOf = (invocation: InvocationApi): InvocationState => {
  const state = states.get(invocation)
  if (state === undefined) throw new Error("Not a restate-effect invocation")
  return state
}

/** Attempt-local service. Never put this in an application-wide Layer. */
export class Invocation extends Context.Service<Invocation, InvocationApi>()("restate-effect/Invocation") {}
/** Provided to object and workflow handlers: they have a key. */
export class Keyed extends Context.Service<Keyed, InvocationApi>()("restate-effect/Keyed") {}
/** Provided to object and workflow handlers: they have a key and state. */
export class StateReader extends Context.Service<StateReader, InvocationApi>()("restate-effect/StateReader") {}
/** Provided to exclusive object handlers and workflow `run`: they can write state. */
export class StateWriter extends Context.Service<StateWriter, InvocationApi>()("restate-effect/StateWriter") {}

/** The current attempt's adapter state. */
export const current: Effect.Effect<InvocationState, never, Invocation> = Effect.map(Invocation, stateOf)

export function invocationState(ctx: sdk.Context, stateful: boolean, writable: boolean, onDefect: "pause" | "fail", runRetry: RunRetryPolicy): { readonly api: InvocationApi; readonly state: InvocationState } {
  /** The pending operation, if any; and one whose awaiting Effect was interrupted, until it settles. */
  let pending: object | undefined
  let abandoned: object | undefined
  const assertReady = () => {
    if (ctx.request().attemptCompletedSignal.aborted) throw new Error("Restate attempt has ended")
    if (pending) throw new Error("Concurrent durable operations are unsupported outside Restate combinators: sequence them with yield*, or use Restate.all, Restate.fork or Restate.raceAll. Use concurrency inside Restate.run for external I/O.")
  }
  const state: InvocationState = {
    ctx, stateful, writable, onDefect, runRetry, assertReady,
    scope: Scope.makeUnsafe(),
    deferred: new Map(),
    async execute(operation, signal) {
      assertReady()
      if (abandoned) throw new Error("A durable operation interrupted by Effect (Effect.timeout, race or interrupt) is still pending, so no other can be awaited until it settles. Interrupting durable operations is not replay-safe: for a durable timeout, use Restate.raceAll with Restate.sleep.")
      const self = {}
      const abandon = () => { if (pending === self) [pending, abandoned] = [undefined, self] }
      pending = self
      signal?.addEventListener("abort", abandon, { once: true })
      try { return await operation() }
      finally {
        signal?.removeEventListener("abort", abandon)
        if (pending === self) pending = undefined
        if (abandoned === self) abandoned = undefined
      }
    }
  }
  const api: InvocationApi = Object.freeze({ ctx })
  states.set(api, state)
  return { api, state }
}
