import { Context, Effect, Layer, Schema } from "effect"
import { Endpoint, definition, type InvocationServices } from "./server.js"
import type { Duration, Scope } from "effect"
import type { ClientError } from "./errors.js"
import type { Codec } from "./codec.js"
import type { Keyed, StateReader, StateWriter } from "./invocation.js"

export type Kind = "service" | "object" | "workflow"
export type Mode = "default" | "shared"
export interface Handler<I extends Codec = Codec, A extends Codec = Codec, E extends Codec = Codec, M extends Mode = Mode> {
  readonly input: I
  readonly success: A
  readonly error: E
  readonly mode: M
  readonly options?: InterfaceOptions
}
/** Options that describe the interface, so they live on the contract that callers share. */
export interface InterfaceOptions {
  /** Only other handlers can call it, not callers outside Restate. */
  readonly ingressPrivate?: boolean
  readonly description?: string
  readonly metadata?: Readonly<Record<string, string>>
}
type HandlerOptions = { readonly options?: InterfaceOptions }
export function handler<I extends Codec, A extends Codec>(options: HandlerOptions & { readonly input: I; readonly success: A }): Handler<I, A, typeof Schema.Never, "default">
export function handler<I extends Codec, A extends Codec, E extends Codec>(options: HandlerOptions & { readonly input: I; readonly success: A; readonly error: E }): Handler<I, A, E, "default">
export function handler(options: HandlerOptions & { readonly input: Codec; readonly success: Codec; readonly error?: Codec }): Handler<Codec, Codec, Codec, "default"> {
  return Object.freeze({ ...options, error: options.error ?? Schema.Never, mode: "default" })
}
export function shared<I extends Codec, A extends Codec, E extends Codec, M extends Mode>(
  definition: Handler<I, A, E, M>
): Handler<I, A, E, "shared"> {
  return Object.freeze({ ...definition, mode: "shared" })
}
export type Handlers = Readonly<Record<string, Handler>>
export interface AnyContract {
  readonly name: string
  readonly kind: Kind
  readonly handlers: Handlers
  readonly options: InterfaceOptions
}

/** Restate's invocation retry policy; `onMaxAttempts` is decided by `onDefect`. */
export interface RetryPolicy {
  readonly maxAttempts?: number
  readonly initialInterval?: Duration.Input
  readonly maxInterval?: Duration.Input
  readonly exponentiationFactor?: number
}
/** How `Restate.run` retries a step: each field a step's own `retry` sets overrides this. No `while`: that stays per step. */
export interface RunRetryPolicy {
  readonly maxAttempts?: number
  readonly initialInterval?: Duration.Input
  readonly maxInterval?: Duration.Input
  readonly exponentiationFactor?: number
  readonly maxDuration?: Duration.Input
}
/** How this server runs a handler: passed to `toLayer`, not part of the shared contract. */
export interface HandlerServerOptions {
  /**
   * What a defect (an unexpected failure, a bug) in the handler does. "pause" (default): Restate
   * retries per `retryPolicy`, then pauses the invocation for an operator to resume. "fail": the
   * invocation fails for good with an opaque internal error; the cause is logged.
   */
  readonly onDefect?: "pause" | "fail"
  readonly retryPolicy?: RetryPolicy
  /**
   * The default retry of every `Restate.run` in the handler; a handler's fields override the
   * service's. Exhausted retries of a defect are a defect of the handler (see `onDefect`).
   */
  readonly runRetry?: RunRetryPolicy
  readonly journalRetention?: Duration.Input
  readonly idempotencyRetention?: Duration.Input
  readonly inactivityTimeout?: Duration.Input
  readonly abortTimeout?: Duration.Input
  readonly onJournalMismatchErrors?: "retry" | "pause" | "fail"
  /** Objects and workflows only. */
  readonly enableLazyState?: boolean
}
export interface ServerOptions<H extends Handlers = Handlers> extends HandlerServerOptions {
  /** Workflows only. */
  readonly workflowRetention?: Duration.Input
  /** Overrides for single handlers. */
  readonly handlers?: { readonly [K in keyof H]?: HandlerServerOptions }
}
export type Implementations<H extends Handlers> = {
  readonly [K in keyof H]: (input: H[K]["input"]["Type"]) => Effect.Effect<H[K]["success"]["Type"], H[K]["error"]["Type"] | ClientError, any>
}
type Requirements<F> = F extends (...args: any[]) => Effect.Effect<any, any, infer R> ? R : never
type Provided = InvocationServices | Scope.Scope
type CheckCapabilities<H extends Handlers, K extends Kind, F extends Implementations<H>> = {
  [M in keyof H]: Extract<Requirements<F[M]>, K extends "service" ? StateReader | StateWriter | Keyed : H[M]["mode"] extends "shared" ? StateWriter : never> extends never
    ? unknown : never
}
export interface BoundHandlers<H extends Handlers> {
  readonly handlers: Implementations<H>
  readonly context: Context.Context<any>
  readonly options: ServerOptions<H>
}
export interface Contract<N extends string = string, H extends Handlers = Handlers, K extends Kind = Kind> {
  readonly name: N
  readonly kind: K
  readonly handlers: H
  readonly options: InterfaceOptions
  /** Implements the contract: a Layer that registers it with `Restate.Endpoint` and provides nothing. */
  readonly toLayer: <F extends Implementations<H>>(
    handlers: F & Record<Exclude<keyof F, keyof H>, never> & CheckCapabilities<H, K, F>,
    options?: ServerOptions<H>
  ) => Layer.Layer<never, never, Endpoint | Exclude<Requirements<F[keyof F]>, Provided>>
}
function contract<N extends string, H extends Handlers, K extends Kind>(name: N, kind: K, handlers: H, options: InterfaceOptions = {}): Contract<N, H, K> {
  if (!name) throw new Error("A Restate contract needs a nonempty name")
  for (const [method, spec] of Object.entries(handlers)) {
    if (["then", "__proto__", "constructor", "key", "attach", "pipe"].includes(method)) {
      throw new Error(`Reserved handler name: ${method}`)
    }
    if (kind === "service" && spec.mode !== "default") throw new Error("Services cannot have shared handlers")
    if (kind === "workflow" && ((method === "run") !== (spec.mode === "default"))) {
      throw new Error("A workflow requires a default run handler and shared auxiliary handlers")
    }
  }
  if (kind === "workflow" && !Object.hasOwn(handlers, "run")) throw new Error("Workflow requires run")
  const self: Contract<N, H, K> = Object.freeze({
    name, kind, handlers: Object.freeze({ ...handlers }), options: Object.freeze({ ...options }),
    toLayer: <F extends Implementations<H>>(implementations: F, serverOptions: ServerOptions<H> = {}) => {
      const expected = Object.keys(handlers).sort().join("\0")
      if (Object.keys(implementations).sort().join("\0") !== expected) throw new Error("Handler implementation keys do not match contract")
      for (const method of Object.keys(serverOptions.handlers ?? {})) {
        if (!Object.hasOwn(handlers, method)) throw new Error(`Options for unknown handler: ${method}`)
      }
      return Layer.effectDiscard(Effect.gen(function* () {
        const context = yield* Effect.context<Exclude<Requirements<F[keyof F]>, Provided>>()
        const endpoint = yield* Endpoint
        yield* endpoint.register(definition(self, { handlers: implementations, context, options: serverOptions }))
      }))
    }
  }) as Contract<N, H, K>
  return self
}
export const service = <const N extends string, const H extends Record<string, Handler<Codec, Codec, Codec, "default">>>(name: N, handlers: H, options?: InterfaceOptions) => contract(name, "service", handlers, options)
export const object = <const N extends string, const H extends Handlers>(name: N, handlers: H, options?: InterfaceOptions) => contract(name, "object", handlers, options)
export const workflow = <const N extends string, const H extends Handlers & { readonly run: Handler<Codec, Codec, Codec, "default"> }>(name: N, handlers: H, options?: InterfaceOptions) => contract(name, "workflow", handlers, options)
