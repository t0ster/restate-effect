import { Layer } from "effect"
import type { HttpRouter } from "effect/http"
import { layerEndpoint, type Endpoint } from "./server.js"
import { SemaphoresLive } from "./semaphores.js"

/**
 * Serves every registered service as one Restate endpoint, mounted on `HttpRouter`. Provide it to
 * the implementation Layers, then serve them with `HttpRouter.serve` on an HTTP/2 server. Every
 * endpoint also serves `_restate-effect.Semaphores`, the object behind `Restate.Semaphore`.
 */
export const layerHttp: Layer.Layer<Endpoint, never, HttpRouter.HttpRouter> = Layer.provideMerge(SemaphoresLive, layerEndpoint)
