import { Context } from "effect"
import type * as sdk from "@restatedev/restate-sdk"
import type { RunRetryPolicy } from "./contract.js"

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
   * `signal` aborts (the awaiting Effect was interrupted, as by `Effect.timeout`), one-way commands
   * (sends, state writes, resolving awakeables) can go on, so cleanup such as a semaphore release
   * runs; awaiting another durable operation fails loudly until the abandoned one settles, as the
   * SDK awaits one at a time.
   */
  readonly execute: <A>(operation: () => PromiseLike<A>, signal?: AbortSignal) => Promise<A>
  /**
   * Runs `work` (which issues commands) when the handler ends with success or a final failure, or
   * earlier at `flush(key)`; never if only the attempt ends, so a retry replays without it. Not at
   * the next durable operation: a durable finalizer of an uncaught defect would run it too.
   */
  readonly defer: (key: string, work: () => void) => void
  /** Runs the work deferred under `key` now, or all of it. */
  readonly flush: (key?: string) => void
}
/** Attempt-local service. Never put this in an application-wide Layer. */
export class Invocation extends Context.Service<Invocation, InvocationState>()("restate-effect/Invocation") {}
/** Provided to object and workflow handlers: they have a key. */
export class Keyed extends Context.Service<Keyed, InvocationState>()("restate-effect/Keyed") {}
/** Provided to object and workflow handlers: they have a key and state. */
export class StateReader extends Context.Service<StateReader, InvocationState>()("restate-effect/StateReader") {}
/** Provided to exclusive object handlers and workflow `run`: they can write state. */
export class StateWriter extends Context.Service<StateWriter, InvocationState>()("restate-effect/StateWriter") {}

export function invocationState(ctx: sdk.Context, stateful: boolean, writable: boolean, onDefect: "pause" | "fail", runRetry: RunRetryPolicy): InvocationState {
  /** The pending operation, if any; and one whose awaiting Effect was interrupted, until it settles. */
  let pending: object | undefined
  let abandoned: object | undefined
  const deferred = new Map<string, Array<() => void>>()
  const flush = (key?: string) => {
    for (const k of key === undefined ? [...deferred.keys()] : [key]) {
      const work = deferred.get(k) ?? []
      deferred.delete(k)
      for (const run of work) run()
    }
  }
  const assertReady = () => {
    if (ctx.request().attemptCompletedSignal.aborted) throw new Error("Restate attempt has ended")
    if (pending) throw new Error("Concurrent durable operations are unsupported outside Restate combinators: sequence them with yield*, or use Restate.all, Restate.fork or Restate.raceAll. Use concurrency inside Restate.run for external I/O.")
  }
  return {
    ctx, stateful, writable, onDefect, runRetry, assertReady, flush,
    defer: (key, work) => { deferred.set(key, [...deferred.get(key) ?? [], work]) },
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
}
