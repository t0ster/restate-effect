import { createServer } from "node:http2"
import { Layer } from "effect"
import { HttpRouter } from "effect/http"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"
import { Restate } from "@t0ster/restate-effect"
import { AppLive } from "./app.ts"

// Each implementation Layer registers its service with the Restate endpoint, one route on an
// HTTP/2 server. Runs until interrupted (Ctrl-C); the app's Layers live as long as the server.
AppLive.pipe(
  Layer.provide(Restate.layerHttp),
  layer => HttpRouter.serve(layer, { disableLogger: true }), // else it logs every Restate request
  Layer.provide(NodeHttpServer.layer(() => createServer(), { port: Number(process.env.PORT ?? 9080), host: "127.0.0.1" })),
  Layer.launch,
  NodeRuntime.runMain
)
