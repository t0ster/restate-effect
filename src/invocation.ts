import { Context } from "effect"
import type * as sdk from "@restatedev/restate-sdk"

export interface InvocationState {
  readonly ctx: sdk.Context
  readonly writable: boolean
  readonly stateful: boolean
  /** What a defect in this handler does; decides whether `Restate.onError` runs for one. */
  readonly onDefect: "pause" | "fail"
  readonly assertReady: () => void
  /** Runs one awaited durable operation; rejects a second one while the first is pending. */
  readonly execute: <A>(operation: () => PromiseLike<A>) => Promise<A>
}
/** Attempt-local service. Never put this in an application-wide Layer. */
export class Invocation extends Context.Service<Invocation, InvocationState>()("restate-effect/Invocation") {}
/** Provided to object and workflow handlers: they have a key and state. */
export class StateReader extends Context.Service<StateReader, InvocationState>()("restate-effect/StateReader") {}
/** Provided to exclusive object handlers and workflow `run`: they can write state. */
export class StateWriter extends Context.Service<StateWriter, InvocationState>()("restate-effect/StateWriter") {}

export function invocationState(ctx: sdk.Context, stateful: boolean, writable: boolean, onDefect: "pause" | "fail" = "pause"): InvocationState {
  let pending = false
  const assertReady = () => {
    if (ctx.request().attemptCompletedSignal.aborted) throw new Error("Restate attempt has ended")
    if (pending) throw new Error("Concurrent durable operations are unsupported outside Restate combinators: sequence them with yield*, or use Restate.all, Restate.fork or Restate.raceAll. Use concurrency inside Restate.run for external I/O.")
  }
  return {
    ctx, stateful, writable, onDefect, assertReady,
    async execute(operation) {
      assertReady()
      pending = true
      try { return await operation() }
      finally { pending = false }
    }
  }
}
/** Provided to object and workflow handlers: they have a key. */
export class Keyed extends Context.Service<Keyed, InvocationState>()("restate-effect/Keyed") {}
