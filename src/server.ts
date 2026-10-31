import { Cause, Context, Effect, Exit, Layer, Logger, Option } from "effect"
import { Headers, HttpEffect, HttpRouter, HttpTraceContext } from "effect/http"
import * as sdk from "@restatedev/restate-sdk"
import * as fetchSdk from "@restatedev/restate-sdk/fetch"
import { cachedSerde, singleFailure, unwrapExit } from "./codec.js"
import type { BoundHandlers, Contract, HandlerServerOptions, Handlers, Kind, RetryPolicy } from "./contract.js"
import { millis } from "./duration.js"
import { Invocation, Keyed, StateReader, StateWriter, invocationState } from "./invocation.js"
import { Transport, invocationTransport } from "./client.js"

type Definition = sdk.ServiceDefinition<string, unknown> | sdk.VirtualObjectDefinition<string, unknown> | sdk.WorkflowDefinition<string, unknown>

/** The services the adapter provides to each handler attempt: never taken from application Layers. */
export type InvocationServices = Invocation | Transport | StateReader | StateWriter | Keyed
/** Removes `InvocationServices` from a context. */
export const omitInvocationServices = Context.omit(Invocation, Transport, StateReader, StateWriter, Keyed)

/** The SDK definition of an implemented contract. Its handlers run with the services captured in `bound`. */
export function definition<N extends string, H extends Handlers, K extends Kind>(contract: Contract<N, H, K>, bound: BoundHandlers<H>): Definition {
    const { handlers: overrides = {}, workflowRetention, ...shared } = bound.options
    const stateful = contract.kind !== "service"
    const services = omitInvocationServices(bound.context)
    const handlers: Record<string, any> = {}
    for (const [method, handler] of Object.entries(contract.handlers)) {
      const own: HandlerServerOptions = (overrides as Record<string, HandlerServerOptions | undefined>)[method] ?? {}
      const onDefect = own.onDefect ?? shared.onDefect ?? "pause"
      const execute = async (ctx: sdk.Context, input: unknown) => {
        const invocation = invocationState(ctx, stateful, stateful && handler.mode !== "shared", onDefect)
        let context = Context.add(
          Context.add(services, Invocation, invocation),
          Transport, invocationTransport(ctx, invocation.execute)
        )
        if (invocation.stateful) context = Context.add(Context.add(context, StateReader, invocation), Keyed, invocation)
        if (invocation.writable) context = Context.add(context, StateWriter, invocation)
        context = Context.add(context, Logger.CurrentLoggers, new Set([restateLogger(ctx)]))
        const request = ctx.request()
        // A server span per handler, continuing Restate's trace when it sends one.
        const program = Effect.scoped(Effect.suspend(() => bound.handlers[method](input))).pipe(
          Effect.withSpan(`${contract.name}/${method}`, {
            kind: "server",
            parent: parentSpan(request),
            attributes: request.id ? { "restate.invocation.id": request.id } : {}
          })
        )
        const signal = request.attemptCompletedSignal
        const exit = await Effect.runPromiseExitWith(context)(program, { signal })
        if (Exit.isFailure(exit) && !signal.aborted) {
          // Interrupted, not by the end of the attempt: cancelled, so the invocation ends as cancelled.
          if (Cause.hasInterruptsOnly(exit.cause)) throw new sdk.CancelledError()
          // A defect: anything but a single declared failure (or a client error).
          if (onDefect === "fail" && !singleFailure(exit.cause)) {
            ctx.console.error(`${contract.name}/${method} failed with a defect`, Cause.pretty(exit.cause))
            throw new sdk.TerminalError("Internal error")
          }
        }
        // Defects stay retryable: with onDefect "pause", Restate retries, then pauses.
        return unwrapExit(exit, handler.error)
      }
      const options = defined({
        ...handler.options,
        ...sdkOptions(own),
        retryPolicy: onDefect === "pause" ? { ...retryPolicy(own.retryPolicy), onMaxAttempts: "pause" as const } : retryPolicy(own.retryPolicy),
        enableLazyState: stateful ? own.enableLazyState : undefined,
        input: cachedSerde(handler.input),
        output: cachedSerde(handler.success)
      })
      handlers[method] = contract.kind === "service"
        ? sdk.handlers.handler(options, execute)
        : contract.kind === "object"
          ? handler.mode === "shared" ? sdk.handlers.object.shared(options, execute) : sdk.handlers.object.exclusive(options, execute)
          : method === "run" ? sdk.handlers.workflow.workflow(options, execute) : sdk.handlers.workflow.shared(options, execute)
    }
    const { description, metadata, ingressPrivate } = contract.options
    const options = defined({
      ingressPrivate,
      ...sdkOptions(shared),
      enableLazyState: stateful ? shared.enableLazyState : undefined,
      workflowRetention: contract.kind === "workflow" ? millis(workflowRetention) : undefined
    })
    const definition = defined({ name: contract.name, description, metadata, options })
    if (contract.kind === "service") return sdk.service({ ...definition, handlers })
    if (contract.kind === "object") return sdk.object({ ...definition, handlers })
    return sdk.workflow({ ...definition, handlers: handlers as { run: (ctx: sdk.WorkflowContext, input: any) => Promise<any> } })
}

/** The span in Restate's trace context for this attempt, if it sent one: the handler span continues it. */
const parentSpan = (request: sdk.Request) =>
  Option.getOrUndefined(HttpTraceContext.fromHeaders(Headers.fromInput(Object.fromEntries(request.attemptHeaders ?? []))))

const retryPolicy = (policy: RetryPolicy | undefined) => policy && defined({
  maxAttempts: policy.maxAttempts,
  initialInterval: millis(policy.initialInterval),
  maxInterval: millis(policy.maxInterval),
  exponentiationFactor: policy.exponentiationFactor
})

/** The SDK options a service and a handler share; durations become milliseconds. */
const sdkOptions = (options: HandlerServerOptions) => ({
  journalRetention: millis(options.journalRetention),
  idempotencyRetention: millis(options.idempotencyRetention),
  inactivityTimeout: millis(options.inactivityTimeout),
  abortTimeout: millis(options.abortTimeout),
  onJournalMismatchErrors: options.onJournalMismatchErrors,
  retryPolicy: retryPolicy(options.retryPolicy)
})

/** Drops undefined fields, so they do not override defaults set elsewhere. */
function defined<T extends object>(value: T): T {
  return Object.fromEntries(Object.entries(value).filter(([, v]) => v !== undefined)) as T
}

/** Effect logs go to Restate's console, which adds the invocation and stays quiet during replay. */
const restateLogger = (ctx: sdk.Context) => Logger.make(({ logLevel, message, cause }) => {
  const parts = Array.isArray(message) ? [...message] : [message]
  if (cause.reasons.length > 0) parts.push(Cause.pretty(cause))
  switch (logLevel) {
    case "Fatal": case "Error": return ctx.console.error(...parts)
    case "Warn": return ctx.console.warn(...parts)
    case "Debug": return ctx.console.debug(...parts)
    case "Trace": return ctx.console.trace(...parts)
    default: return ctx.console.info(...parts)
  }
})
/** Collects the services that `toLayer` Layers register; `layerHttp` provides it. */
export class Endpoint extends Context.Service<Endpoint, {
  readonly register: (definition: Definition) => Effect.Effect<void>
}>()("restate-effect/Endpoint") {}

/**
 * Serves every registered service as one Restate endpoint, mounted on `HttpRouter`. Provide it to
 * the implementation Layers, then serve them with `HttpRouter.serve` on an HTTP/2 server.
 */
export const layerHttp: Layer.Layer<Endpoint, never, HttpRouter.HttpRouter> = Layer.effect(Endpoint)(Effect.gen(function* () {
  const router = yield* HttpRouter.HttpRouter
  const definitions = new Map<string, Definition>()
  let handler: ((request: Request) => Promise<Response>) | undefined
  // Built on the first request, Restate's discovery: HttpRouter.serve builds every Layer before serving.
  const serve = () => {
    if (!handler) {
      const endpoint = [...definitions.values()].reduce((e, d) => e.bind(d), fetchSdk.endpoint().bidirectional()).handler()
      handler = request => endpoint.fetch(request)
    }
    return handler
  }
  // The SDK answers protocol errors itself; a rejected fetch is a fault in this process.
  yield* router.add("*", "/*", Effect.orDie(HttpEffect.fromWebHandler(request => serve()(request))))
  return Endpoint.of({
    register: definition => Effect.suspend(() => {
      if (handler) return Effect.die(new Error(`Restate service ${definition.name} registered after the endpoint started serving`))
      if (definitions.has(definition.name)) return Effect.die(new Error(`Duplicate Restate service name: ${definition.name}`))
      definitions.set(definition.name, definition)
      return Effect.void
    })
  })
}))
