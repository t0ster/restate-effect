import type { Layer } from "effect"
import type { HttpRouter } from "effect/http"
import { layerEndpoint, type Endpoint } from "./server.js"

/**
 * Serves every registered service as one Restate endpoint, mounted on `HttpRouter`. Provide it to
 * the implementation Layers, then serve them with `HttpRouter.serve` on an HTTP/2 server.
 */
export const layerHttp: Layer.Layer<Endpoint, never, HttpRouter.HttpRouter> = layerEndpoint
