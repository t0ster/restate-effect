import { Cause, Effect, Layer } from "effect"
import { Restate } from "@t0ster/restate-effect"
import { AlbumService, Gallery, Profile, Signup } from "./app.ts"

const signups = Restate.client(Signup)

const signup = (name: string, email: string) => Effect.gen(function* () {
  // A workflow's `run` starts it and waits. One run per key: running "ada" again joins it.
  const messageId = yield* signups.key(name).run({ email })
  console.log(`${name}: welcomed (${messageId}), status: ${yield* signups.key(name).status()}`)
}).pipe(
  // Declared errors arrive as their own classes.
  Effect.catchTags({
    NameTaken: e => Effect.log(e.message),
    MailRejected: e => Effect.log(`mail rejected: ${e.to}`)
  })
)

const program = Effect.gen(function* () {
  // Signups: success, then each declared error, then a bug (an opaque defect).
  yield* signup("ada", "ada@example.com")
  yield* signup("admin", "admin@example.com")
  yield* signup("mallory", "blocked@example.com")
  yield* signup(" ", "nobody@example.com").pipe(Effect.catchCause(cause => Effect.sync(() => console.log("bug:", Cause.pretty(cause)))))
  // An outage outlasting the step's retries: the signup fails, and its onError cleanup ran.
  yield* signup("offline", "offline@example.com").pipe(Effect.catchCause(() => Effect.gen(function* () {
    console.log(`offline: ${yield* signups.key("offline").status()}`)
  })))

  // Photos: many at once, nested groups, and a one-way call.
  const names = ["cat.jpg", "dog.bad", "sun.jpg"]
  const gallery = Restate.client(Gallery)
  console.log("published:", yield* gallery.publishAll(names))
  yield* gallery.publishWithCover({ cover: "cover.jpg", photos: names }).pipe(
    Effect.catchTag("PublishIncomplete", e => Effect.log(`not published: ${e.failed.join(", ")}`))
  )
  // One-way: returns once Restate accepted it; retrying with the same key starts it once.
  yield* gallery.publishAll(names, { discard: true, idempotencyKey: "publish-demo" })

  // Albums, through an ordinary Effect service.
  const albums = yield* AlbumService
  const albumId = `album-${Date.now()}`
  const run = yield* albums.create(albumId, "ada", names)   // returns at once, with the run's ID
  console.log("progress:", yield* albums.progress(albumId))
  console.log("album:", yield* albums.wait(run))
  console.log("ada's albums:", yield* Restate.client(Profile).key("ada").albums())
})

await Effect.runPromise(program.pipe(
  Effect.provide(AlbumService.layer.pipe(
    Layer.provideMerge(Restate.layerIngress({ url: process.env.RESTATE_INGRESS_URL ?? "http://127.0.0.1:8080" }))
  ))
))
