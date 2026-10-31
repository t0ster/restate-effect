/**
 * @t0ster/restate-effect by example: a small photo-sharing app.
 *
 * Read top to bottom:
 *   1. Errors       what can go wrong, as typed values that cross the wire
 *   2. Contracts    the app's Restate API: services, objects and workflows
 *   3. Handlers     one implementation Layer per contract
 *   4. Client       an ordinary Effect service other code calls
 *   5. Wiring       demo dependencies and the Layer the server runs
 *
 * `server.ts` serves this app; `client.ts` calls it from outside Restate.
 */
import { Array, Context, Data, DateTime, Effect, Layer, Result, Schema } from "effect"
import { Restate } from "@t0ster/restate-effect"

// ─── 1. Errors ───────────────────────────────────────────────────────────────────────────────────
//
// Declared errors are part of a contract: Schema errors, encoded into Restate's TerminalError and
// decoded back into these classes by every caller. Declared errors are final: Restate never
// retries them. Anything else that crashes a handler is a defect (see Signup and Album below).

export class NameTaken extends Schema.Error<NameTaken>("NameTaken")({
  _tag: Schema.tag("NameTaken"), name: Schema.String
}) {
  // Shown in the Restate UI and to HTTP callers; the fields travel separately.
  override get message() { return `Name taken: ${this.name}` }
}
export class MailRejected extends Schema.Error<MailRejected>("MailRejected")({
  _tag: Schema.tag("MailRejected"), to: Schema.String
}) {}
export class MailUnavailable extends Schema.Error<MailUnavailable>("MailUnavailable")({
  _tag: Schema.tag("MailUnavailable")
}) {}
export class UploadFailed extends Schema.Error<UploadFailed>("UploadFailed")({
  _tag: Schema.tag("UploadFailed"), photo: Schema.String
}) {}
export class PublishIncomplete extends Schema.Error<PublishIncomplete>("PublishIncomplete")({
  _tag: Schema.tag("PublishIncomplete"), failed: Schema.Array(Schema.String)
}) {}

// ─── 2. Contracts ────────────────────────────────────────────────────────────────────────────────
//
// A contract names handlers with their input, success and error schemas. It has no implementation,
// so it can live in a shared package that callers import. Options here describe the interface:
// `ingressPrivate` hides a contract or handler from callers outside Restate.

const Published = Schema.Struct({ photo: Schema.String, thumbnail: Schema.String })
const PublishReport = Schema.Struct({ thumbnails: Schema.Array(Schema.String), failed: Schema.Array(Schema.String) })
const AlbumInput = Schema.Struct({ owner: Schema.String, photos: Schema.Array(Schema.String) })
const AlbumIds = Schema.Array(Schema.String)

/** Service: stateless steps. Internal: only other handlers call it. */
export const Photos = Restate.service("Photos", {
  upload: Restate.handler({ input: Schema.String, success: Schema.String, error: UploadFailed }),
  thumbnail: Restate.handler({ input: Schema.String, success: Schema.String })
}, { ingressPrivate: true })

/** Service: publishes photos, alone or many at once. */
export const Gallery = Restate.service("Gallery", {
  publish: Restate.handler({ input: Schema.String, success: Published, error: UploadFailed }),
  publishAll: Restate.handler({ input: Schema.Array(Schema.String), success: PublishReport }),
  publishWithCover: Restate.handler({
    input: Schema.Struct({ cover: Schema.String, photos: Schema.Array(Schema.String) }),
    success: Schema.String,
    error: Schema.Union([UploadFailed, PublishIncomplete])
  })
})

/** Virtual object, keyed by user: state with exclusive writes and shared reads. */
export const Profile = Restate.object("Profile", {
  addAlbum: Restate.handler({ input: Schema.String, success: Schema.Void }),
  albums: Restate.shared(Restate.handler({ input: Schema.Void, success: AlbumIds })),
  forgetAlbum: Restate.handler({ input: Schema.String, success: Schema.Void, options: { ingressPrivate: true } })
})

/** Workflow, keyed by user name: runs once per key, with shared handlers to query it meanwhile. */
export const Signup = Restate.workflow("Signup", {
  run: Restate.handler({
    input: Schema.Struct({ email: Schema.String }),
    success: Schema.String,
    error: Schema.Union([NameTaken, MailRejected, MailUnavailable])
  }),
  status: Restate.shared(Restate.handler({ input: Schema.Void, success: Schema.String }))
})

/** Workflow, keyed by album ID: publishes an album's photos, reporting progress as it goes. */
export const Album = Restate.workflow("Album", {
  run: Restate.handler({ input: AlbumInput, success: PublishReport }),
  progress: Restate.shared(Restate.handler({ input: Schema.Void, success: PublishReport }))
})

/** Service: starts an album and returns at once, with the ID of its run. */
export const Albums = Restate.service("Albums", {
  create: Restate.handler({
    input: Schema.Struct({ albumId: Schema.String, ...AlbumInput.fields }),
    // A branded string schema, like any other: invocation IDs can be stored and returned.
    success: Restate.InvocationId
  })
})

// Clients are pure values: build them once. Inside a handler their calls are durable (journaled);
// outside Restate they go through ingress. Every call returns a `Restate.Durable`: an Effect that
// Restate's combinators can also run concurrently. With `{ discard: true }` a call only starts
// the handler and returns its `Restate.InvocationId`; `client.attach.<handler>(id)` waits for it.
const photos = Restate.client(Photos)
const gallery = Restate.client(Gallery)
const profile = Restate.client(Profile)
const album = Restate.client(Album)

// ─── 3. Handlers ─────────────────────────────────────────────────────────────────────────────────
//
// `Contract.toLayer(handlers, options)` implements a contract. Handlers are Effects; the services
// they need (UserStore, Mailer) become requirements of the Layer, provided once at startup.
// Options here describe how this server runs it: `journalRetention`, `retryPolicy`, `onDefect`.

// ── Photos: the simplest handlers ──

export const PhotosLive = Photos.toLayer({
  upload: photo => photo.endsWith(".bad")
    ? Effect.fail(new UploadFailed({ photo }))
    // RAW photos take a minute to develop: a durable timer, so a restart does not start it over.
    : Effect.as(photo.endsWith(".raw") ? Restate.sleep("1 minute") : Effect.void, `https://cdn.example/${photo}`),
  thumbnail: url => Effect.succeed(`${url}?width=200`)
}, {
  // Photos may be private: Restate keeps no journal once an invocation completes.
  journalRetention: "0 seconds"
})

// ── Gallery: calling other handlers, sequentially and concurrently ──
//
// `Restate.all` runs durable calls at once (fails on the first failure); `Restate.result` turns a
// failure into a `Result` value. Both return a Durable, so groups nest. `Restate.map`,
// `asVoid`, `mapError` and `filterOrFail` transform a Durable like their Effect namesakes; their
// functions return plain values, so they cannot start calls. Plain `Effect.all`/`Effect.map` also
// work, but the result is an ordinary Effect that cannot run concurrently with other calls.

export const GalleryLive = Gallery.toLayer({
  // Sequential: each `yield*` is one durable call, replayed from the journal after a crash.
  publish: name => Effect.gen(function* () {
    const url = yield* photos.upload(name)
    return { photo: name, thumbnail: yield* photos.thumbnail(url) }
  }),

  // Concurrent: one invocation per photo; a failed photo does not stop the others.
  publishAll: names => Effect.gen(function* () {
    const results = yield* Restate.all(names.map(name => Restate.result(gallery.publish(name))))
    const [published, failures] = Array.separate(results)
    return { thumbnails: published.map(p => p.thumbnail), failed: failures.map(f => f.photo) }
  }),

  // Nested: the cover and a whole group run at once.
  publishWithCover: ({ cover, photos: names }) => Effect.gen(function* () {
    const [thumbnail] = yield* Restate.all([
      gallery.publish(cover).pipe(Restate.map(published => published.thumbnail)),
      publishEvery(names)
    ])
    return thumbnail
  })
})

/** Every photo at once, as one Durable that fails with PublishIncomplete if any upload failed. */
const publishEvery = (names: ReadonlyArray<string>): Restate.Durable<void, PublishIncomplete> =>
  Restate.all(names.map(name => Restate.result(gallery.publish(name)))).pipe(
    Restate.filterOrFail(
      results => results.every(Result.isSuccess),
      results => new PublishIncomplete({ failed: Array.getFailures(results).map(f => f.photo) })
    ),
    Restate.asVoid
  )

// ── Profile: object state, its key, and a delayed send ──

export const ProfileLive = Profile.toLayer({
  addAlbum: albumId => Effect.gen(function* () {
    const albums = (yield* Restate.state.get("albums", AlbumIds)) ?? []
    yield* Restate.state.set("albums", AlbumIds, [albumId, ...albums])
    // A one-way send to itself, delivered by Restate a day later.
    yield* profile.key(yield* Restate.key).forgetAlbum(albumId, { discard: true, delay: "1 day" })
  }),
  albums: () => Effect.map(Restate.state.get("albums", AlbumIds), albums => albums ?? []),
  forgetAlbum: albumId => Effect.gen(function* () {
    const albums = (yield* Restate.state.get("albums", AlbumIds)) ?? []
    yield* Restate.state.set("albums", AlbumIds, albums.filter(id => id !== albumId))
  })
})

// ── Signup: external side effects, retries, errors and cleanup ──
//
// External I/O goes in `Restate.run`: its result is journaled, so a replay does not repeat it.
// Inside `run`, defects are retried, and so are declared errors matching `retry.while`.

export const SignupLive = Signup.toLayer({
  run: ({ email }) => Effect.gen(function* () {
    const name = yield* Restate.key
    // A bug, not an expected failure: callers validate names. See onDefect below.
    if (name.trim() === "") return yield* Effect.die(new Error("empty user name"))

    yield* Restate.state.set("status", Schema.String, "registering")
    const store = yield* UserStore
    const register = store.insert(name).pipe(
      // Translate internal errors at the boundary: the one a caller can act on becomes declared...
      Effect.catchTag("UniqueViolation", () => Effect.fail(new NameTaken({ name }))),
      // ...the rest is a defect, which run retries: an outage is temporary.
      Effect.catchTag("ConnectionLost", Effect.die)
    )
    yield* Restate.run("register", register, { success: Schema.Void, error: NameTaken })

    yield* Restate.sleep("1 second")   // a durable timer: survives restarts

    const mailer = yield* Mailer
    const messageId = yield* Restate.run("welcome-mail", mailer.send(email, `Welcome, ${name}!`), {
      success: Schema.String,
      error: Schema.Union([MailRejected, MailUnavailable]),
      // Retry MailUnavailable; MailRejected, or the last MailUnavailable, fails the signup.
      retry: { while: error => error._tag === "MailUnavailable", maxAttempts: 3, initialInterval: "1 second" }
    })

    const welcomedAt = yield* Restate.now   // journaled: a replay sees the same time
    yield* Restate.state.set("status", Schema.String, `welcomed at ${DateTime.formatIso(welcomedAt)}`)
    return messageId
  }).pipe(
    // Runs only when the signup fails for good; never on a retry, suspension or pause.
    Restate.onError(cause => Effect.gen(function* () {
      yield* Effect.logError("signup failed", yield* Restate.currentInvocationId, cause)   // Restate's logger
      yield* Restate.state.set("status", Schema.String, "failed")
      const mailer = yield* Mailer
      // Cleanup can use durable steps too; they are journaled like the rest of the handler.
      yield* Restate.run("alert-ops", mailer.send("ops@example.com", "A signup failed"), {
        success: Schema.String,
        error: Schema.Union([MailRejected, MailUnavailable]),
        retry: { while: error => error._tag === "MailUnavailable", maxAttempts: 3, initialInterval: "1 second" }
      })
    }).pipe(
      // Cleanup cannot fail with a typed error: if it fails, it is a defect.
      Effect.orDie
    ))
  ),
  status: () => Effect.map(Restate.state.get("status", Schema.String), status => status ?? "pending")
}, {
  // A defect ends a signup at once: callers get an opaque internal error instead of waiting.
  // The default, "pause", retries and then pauses the invocation for an operator to resume.
  onDefect: "fail"
})

// ── Album: react to each result as it arrives ──
//
// `Restate.fork` starts a durable call now; `Restate.raceAll` returns the first to succeed, and
// the others keep running. Restate journals which one won, so a replay takes the same branch.

export const AlbumLive = Album.toLayer({
  run: ({ owner, photos: names }) => Effect.gen(function* () {
    // One-way: the album does not wait for the owner's profile to update.
    yield* profile.key(owner).addAlbum(yield* Restate.key, { discard: true })

    const pending = new Map(yield* Effect.forEach(names, name =>
      Effect.map(Restate.fork(Restate.result(gallery.publish(name))), fiber => [name, fiber] as const)))
    const thumbnails: string[] = []
    const failed: string[] = []
    while (pending.size > 0) {
      const result = yield* Restate.raceAll([...pending.values()].map(Restate.join))
      if (Result.isSuccess(result)) {
        pending.delete(result.success.photo)
        thumbnails.push(result.success.thumbnail)
      } else {
        pending.delete(result.failure.photo)
        failed.push(result.failure.photo)
      }
      yield* Restate.state.set("progress", PublishReport, { thumbnails, failed })
    }
    return { thumbnails, failed }
  }).pipe(
    // Cancelled (or failed for good): take the album off its owner's profile again. Only on the
    // final failure, never when an attempt merely ends, and the cleanup itself is durable.
    Restate.onError(() => Effect.flatMap(Restate.key, albumId => profile.key(owner).forgetAlbum(albumId)).pipe(Effect.orDie))
  ),
  progress: () => Effect.map(
    Restate.state.get("progress", PublishReport),
    report => report ?? { thumbnails: [], failed: [] }
  )
}, {
  // Default onDefect ("pause"): a bug pauses a long album instead of losing its progress; deploy a
  // fix, then resume it. retryPolicy only tunes the attempts before the pause.
  retryPolicy: { maxAttempts: 5, initialInterval: "1 second" }
})

// ── Albums: start a workflow without waiting ──

export const AlbumsLive = Albums.toLayer({
  // `run` waits for the result; with `discard` it only starts the run and returns its invocation
  // ID, like any one-way call. The album ID is the workflow key: one run per key, so a retry joins
  // the same run and returns the same ID.
  create: ({ albumId, owner, photos }) => album.key(albumId).run({ owner, photos }, { discard: true })
})

// ─── 4. Client ───────────────────────────────────────────────────────────────────────────────────
//
// An ordinary Effect service over the contracts: its callers need not know about Restate.
// `makeClient` takes the Transport once, when the Layer is built, so the methods require nothing;
// `Client.orDie` makes transport failures defects, so they fail only with declared errors.
export class AlbumService extends Context.Service<AlbumService, {
  readonly create: (albumId: string, owner: string, photos: ReadonlyArray<string>) => Effect.Effect<Restate.InvocationId>
  readonly wait: (run: Restate.InvocationId) => Effect.Effect<typeof PublishReport.Type>
  readonly progress: (albumId: string) => Effect.Effect<typeof PublishReport.Type>
}>()("example/AlbumService") {
  static readonly layer = Layer.effect(AlbumService)(Effect.gen(function* () {
    const albums = Restate.Client.orDie(yield* Restate.makeClient(Albums))
    const album = Restate.Client.orDie(yield* Restate.makeClient(Album))
    return {
      create: Effect.fn("AlbumService.create")((albumId: string, owner: string, photos: ReadonlyArray<string>) =>
        albums.create({ albumId, owner, photos })),
      // Attaches to the run that create started, by its invocation ID: waits without starting
      // another. `attach.<handler>` knows that handler's schemas, so the result is typed.
      wait: Effect.fn("AlbumService.wait")((run: Restate.InvocationId) => album.attach.run(run)),
      progress: Effect.fn("AlbumService.progress")((albumId: string) => album.key(albumId).progress())
    }
  }))
}

// ─── 5. Wiring ───────────────────────────────────────────────────────────────────────────────────
//
// Handlers depend on ordinary Effect services. These demo versions stand in for a database and a
// mail provider; their internal errors never cross the wire, so they are plain tagged errors.

class UniqueViolation extends Data.TaggedError("UniqueViolation") {}
class ConnectionLost extends Data.TaggedError("ConnectionLost") {}

export class UserStore extends Context.Service<UserStore, {
  readonly insert: (name: string) => Effect.Effect<void, UniqueViolation | ConnectionLost>
}>()("example/UserStore") {}
const takenNames = new Set(["admin"])
const DemoUserStore = Layer.succeed(UserStore)({
  insert: name => takenNames.has(name) ? Effect.fail(new UniqueViolation()) : Effect.sync(() => { takenNames.add(name) })
})

export class Mailer extends Context.Service<Mailer, {
  readonly send: (to: string, body: string) => Effect.Effect<string, MailRejected | MailUnavailable>
}>()("example/Mailer") {}
let mailAttempts = 0
const DemoMailer = Layer.succeed(Mailer)({
  send: (to, body) => to.startsWith("blocked")
    ? Effect.fail(new MailRejected({ to }))
    // Every other attempt is unavailable, to show retries.
    : mailAttempts++ % 2 === 0
      ? Effect.fail(new MailUnavailable())
      : Effect.sync(() => {
        console.log(`mail to ${to}: ${body}`)
        return `msg-${Date.now()}`
      })
})

/** Every handler, with its dependencies: what `server.ts` serves. */
export const AppLive = Layer.mergeAll(PhotosLive, GalleryLive, ProfileLive, SignupLive, AlbumLive, AlbumsLive).pipe(
  Layer.provide(Layer.mergeAll(DemoUserStore, DemoMailer))
)
