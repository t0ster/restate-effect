import { Context, Duration, Effect, Layer, Schema } from "effect"
import { Headers, HttpTraceContext } from "effect/http"
import * as ingressSdk from "@restatedev/restate-sdk-clients"
import * as sdk from "@restatedev/restate-sdk"
import { decodeFailure, serde } from "./codec.js"
import type { AnyContract, Handler, Handlers } from "./contract.js"
import { DeclaredFailure, Durable, isCancellation } from "./durable.js"
import type { InvocationState } from "./invocation.js"
import { ClientError } from "./errors.js"
export { ClientError } from "./errors.js"

/** A Restate invocation ID: a branded string, so it can be stored, sent and decoded like any value. */
export const InvocationId = Schema.String.pipe(Schema.brand("restate-effect/InvocationId"))
export type InvocationId = typeof InvocationId.Type

export interface CallOptions {
  readonly idempotencyKey?: string
  readonly headers?: Record<string, string>
}
/** A one-way call: returns the invocation ID once Restate accepted it, without waiting for the result. */
export interface SendOptions extends CallOptions {
  readonly discard: true
  /** Restate delivers the call after this delay. */
  readonly delay?: Duration.Input
}
export interface Call {
  readonly contract: AnyContract
  readonly method: string
  readonly handler: Handler
  readonly input: unknown
  readonly key?: string
  readonly options?: CallOptions & { readonly delay?: Duration.Input }
  /** "call" waits for the result (a workflow's run joins a run already started); "send" does not; "attach" waits for `invocationId`. */
  readonly operation: "call" | "send" | "attach"
  readonly invocationId?: InvocationId
}
/** Transport errors stay in the typed channel; the original cause remains available. */
export class Transport extends Context.Service<Transport, {
  readonly call: (call: Call) => Effect.Effect<unknown, ClientError>
}>()("restate-effect/Transport") {}

type In<H extends Handler> = H["input"]["Type"]
type Out<H extends Handler> = H["success"]["Type"]
type Err<H extends Handler> = H["error"]["Type"]
/**
 * Call a handler: `yield*` waits for its result; with `{ discard: true }` it only starts it. Fails
 * with the handler's declared errors or `E`, the client's transport error.
 */
export interface Method<H extends Handler, E = ClientError, R = Transport> {
  (input: In<H>, options: SendOptions): Effect.Effect<InvocationId, E, R>
  (input: In<H>, options?: CallOptions): Durable<Out<H>, Err<H> | E, R>
}
type Methods<H extends Handlers, E, R> = { readonly [K in keyof H]: Method<H[K], E, R> }
/** Wait for an invocation started with `{ discard: true }`, decoded with that handler's schemas. */
type Attach<H extends Handlers, E, R> = {
  readonly [K in keyof H]: (invocationId: InvocationId) => Durable<Out<H[K]>, Err<H[K]> | E, R>
}
/**
 * A contract's client. `E` is its transport error (`ClientError`, or `never` after
 * `Client.orDie`); `R` what its calls require (`Transport` from `client`, nothing from `makeClient`).
 */
export type Client<C extends AnyContract, E = ClientError, R = Transport> = ClientTypeId<C, E, R> & (C["kind"] extends "service"
  ? Methods<C["handlers"], E, R> & { readonly attach: Attach<C["handlers"], E, R> }
  : { readonly key: (key: string) => Methods<C["handlers"], E, R>; readonly attach: Attach<C["handlers"], E, R> })

const TypeId: unique symbol = Symbol.for("restate-effect/Client")
/** Carries a client's contract and how it runs calls; `Client.orDie` rebuilds the client from it. */
interface ClientTypeId<C extends AnyContract, E, R> {
  readonly [TypeId]: { readonly contract: C; readonly run: Run; readonly _E: (_: never) => E; readonly _R: (_: never) => R }
}
type AnyClient = ClientTypeId<AnyContract, any, any>

const clientError = (call: Call, cause: unknown) => new ClientError({ service: call.contract.name, method: call.method, cause })

/** Restate reports a cancelled callee to ingress callers as HTTP 409 "Cancelled". */
const isIngressCancellation = (error: unknown) => {
  if (!(error instanceof ingressSdk.HttpCallError) || error.status !== 409) return false
  try { return JSON.parse(error.responseText)?.message === "Cancelled" } catch { return false }
}

/** The yield* path: through a Transport, so it works inside handlers and through ingress. */
function perform(transport: Context.Service.Shape<typeof Transport>, call: Call): Effect.Effect<unknown, unknown> {
  return transport.call(call).pipe(
    Effect.catch(error => {
      // A cancelled callee interrupts its caller, as joining an interrupted fiber does.
      if (isCancellation(error.cause) || isIngressCancellation(error.cause)) return Effect.interrupt
      let decoded: { readonly value: unknown } | undefined
      try { decoded = decodeFailure(error.cause, call.handler.error) }
      catch (cause) { return Effect.fail(clientError(call, cause)) }
      if (decoded) return Effect.fail(decoded.value)
      // Inside a handler, a callee's undeclared failure is a remote defect: it dies here too.
      if (error.cause instanceof sdk.TerminalError) return Effect.die(error.cause)
      return Effect.fail(error)
    })
  )
}

/**
 * The combinator path, inside a handler: issues the command; rejections follow the Durable
 * protocol. A callee's undeclared failure is a remote defect, so it rejects as is and dies.
 */
function issue(invocation: InvocationState, call: Call): sdk.RestatePromise<unknown> {
  return command(invocation.ctx, call).map((value, failure) => {
    if (failure === undefined) return value
    if (isCancellation(failure)) throw failure
    const decoded = decodeFailure(failure, call.handler.error)
    if (decoded) throw new DeclaredFailure(decoded.value)
    throw failure
  })
}

/** The SDK command for a call or attach. A workflow's run is called directly here: it does not join. */
function command(ctx: sdk.Context, call: Call): sdk.RestatePromise<unknown> {
  if (call.operation === "attach") return ctx.invocation(call.invocationId as unknown as sdk.InvocationId).attach(serde(call.handler.success))
  return ctx.genericCall({
    service: call.contract.name, method: call.method, key: call.key, parameter: call.input,
    inputSerde: serde(call.handler.input), outputSerde: serde(call.handler.success),
    idempotencyKey: call.options?.idempotencyKey, headers: call.options?.headers
  })
}

/** How a client runs a call outside the combinators. */
type Run = (call: Call) => Effect.Effect<unknown, unknown, any>

function build(contract: AnyContract, run: Run): any {
  const methods = (key?: string) => Object.fromEntries(Object.entries(contract.handlers).map(([method, handler]) => [
    method,
    (input: unknown, options?: CallOptions & { readonly discard?: boolean; readonly delay?: Duration.Input }) => {
      if (options?.discard) {
        const { discard: _, ...rest } = options
        return Effect.map(run({ contract, method, handler, input, key, options: rest, operation: "send" }), id => id as InvocationId)
      }
      const call: Call = { contract, method, handler, input, key, options, operation: "call" }
      return new Durable<unknown, unknown, any>(invocation => issue(invocation, call), run(call))
    }
  ]))
  const attach = Object.fromEntries(Object.entries(contract.handlers).map(([method, handler]) => [
    method,
    (invocationId: InvocationId) => {
      const call: Call = { contract, method, handler, input: undefined, operation: "attach", invocationId }
      return new Durable<unknown, unknown, any>(invocation => issue(invocation, call), run(call))
    }
  ]))
  const self = contract.kind === "service" ? { ...methods(), attach } : { key: (key: string) => methods(key), attach }
  return Object.defineProperty(self, TypeId, { value: { contract, run } })
}

/**
 * A pure, reusable client: each call resolves `Transport` when it runs, so inside a handler its calls
 * are durable (journaled) and work with the combinators; outside, provide `layerIngress`.
 */
export const client = <C extends AnyContract>(contract: C): Client<C> =>
  build(contract, call => Effect.flatMap(Transport, transport => perform(transport, call)))

/**
 * Resolves `Transport` once and returns a client whose calls require nothing, like
 * `HttpApiClient.make`: for services built in a Layer. Its calls go through the transport it was
 * made with: ingress, or the handler's durable transport if made inside a handler.
 */
export const makeClient = <C extends AnyContract>(contract: C): Effect.Effect<Client<C, ClientError, never>, never, Transport> =>
  Effect.map(Transport, transport => build(contract, call => perform(transport, call)))

/** Operations on clients; `Client<C, E, R>` is the type. */
export const Client = {
  /**
   * Turns the client's transport errors into defects, like `HttpClient.catchTag(client, tag, Effect.die)`:
   * calls fail only with the handler's declared errors. The defect is the original `ClientError`.
   * Works on clients from `client` and `makeClient`; durable calls inside handlers are unchanged.
   */
  orDie: <Self extends AnyClient>(self: Self): Self extends ClientTypeId<infer C, infer E, infer R> ? Client<C, Exclude<E, ClientError>, R> : never => {
    const { contract, run } = self[TypeId]
    return build(contract, call => Effect.catchIf(run(call), error => error instanceof ClientError, Effect.die))
  }
}

const isWorkflowRun = (call: Call) => call.contract.kind === "workflow" && call.method === "run"
const millis = (delay: Duration.Input | undefined) => delay === undefined ? undefined : Duration.toMillis(delay)

export const ingressTransport = (ingress: ingressSdk.Ingress): Context.Service.Shape<typeof Transport> => ({
  call: call => Effect.useSpan(`${call.contract.name}/${call.method}`, { kind: "client" }, span => Effect.tryPromise({
    try: async signal => {
      // W3C trace context, so Restate (and the handler's span) continue this trace.
      const headers = { ...Object.fromEntries(Object.entries(HttpTraceContext.toHeaders(span))), ...call.options?.headers }
      const target = { service: call.contract.name, handler: call.method, key: call.key }
      const send = () => ingress.send({ ...target, parameter: call.input, opts: ingressSdk.rpc.sendOpts({
        idempotencyKey: call.options?.idempotencyKey, headers, delay: millis(call.options?.delay), input: serde(call.handler.input)
      }) })
      const result = (invocationId: string) => ingress.result({ invocationId, attachable: true } as ingressSdk.Send, serde(call.handler.success))
      if (call.operation === "send") return (await send()).invocationId
      if (call.operation === "attach") return result(call.invocationId!)
      // A workflow runs once per key: send (which joins a started run), then wait for it.
      if (isWorkflowRun(call)) return result((await send()).invocationId)
      return ingress.call({ ...target, parameter: call.input, opts: ingressSdk.rpc.opts({
        idempotencyKey: call.options?.idempotencyKey, headers, signal, input: serde(call.handler.input), output: serde(call.handler.success)
      }) })
    },
    catch: cause => clientError(call, cause)
  }))
})
export const layerIngress = (options: Parameters<typeof ingressSdk.connect>[0]) =>
  Layer.succeed(Transport)(ingressTransport(ingressSdk.connect(options)))

/** Used only by the handler adapter, with a fresh context for each invocation. */
export const invocationTransport = (
  ctx: sdk.Context,
  execute: <A>(operation: () => PromiseLike<A>) => Promise<A>
): Context.Service.Shape<typeof Transport> => ({
  call: call => Effect.tryPromise({
    try: () => execute(async () => {
      const send = () => ctx.genericSend({
        service: call.contract.name, method: call.method, key: call.key, parameter: call.input,
        inputSerde: serde(call.handler.input), delay: millis(call.options?.delay),
        idempotencyKey: call.options?.idempotencyKey, headers: call.options?.headers
      }).invocationId
      if (call.operation === "send") return send()
      // A workflow runs once per key: send (which joins a started run), then attach to it.
      if (isWorkflowRun(call)) return command(ctx, { ...call, operation: "attach", invocationId: await send() as unknown as InvocationId })
      return command(ctx, call)
    }),
    catch: cause => clientError(call, cause)
  })
})

/** Header map of the attempt, for trace context. */
export const attemptHeaders = (ctx: sdk.Context) => Headers.fromInput(Object.fromEntries(ctx.request().attemptHeaders ?? []))
export const parentSpan = (ctx: sdk.Context) => HttpTraceContext.fromHeaders(attemptHeaders(ctx))
