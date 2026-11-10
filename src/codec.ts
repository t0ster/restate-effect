import { Cause, Exit, Schema } from "effect"
import * as sdk from "@restatedev/restate-sdk"
import { HttpCallError } from "@restatedev/restate-sdk-clients"
import { ClientError } from "./errors.js"

/** Restate serdes are synchronous; schemas requiring services are unsupported. */
export type Codec = Schema.ConstraintCodec<any, any, never, never>
const encoder = new TextEncoder()
const decoder = new TextDecoder()
/** TerminalError metadata key holding the encoded declared failure. */
const errorKey = "effectError"

export function serde<S extends Codec>(schema: S): sdk.Serde<S["Type"]> {
  const json = Schema.toCodecJson(schema)
  const encode = Schema.encodeSync(json)
  const decode = Schema.decodeUnknownSync(json)
  return {
    contentType: "application/json",
    jsonSchema: jsonSchema(schema),
    serialize(value) {
      const encoded = encode(value)
      return encoded === undefined ? new Uint8Array() : encoder.encode(JSON.stringify(encoded))
    },
    deserialize(bytes) {
      return decode(bytes.length === 0 ? undefined : JSON.parse(decoder.decode(bytes)))
    }
  }
}

const serdes = new WeakMap<Codec, sdk.Serde<any>>()
/** `serde`, built once per schema: the adapters serialize on every call, and building one derives its JSON Schema. */
export function cachedSerde<S extends Codec>(schema: S): sdk.Serde<S["Type"]> {
  let cached = serdes.get(schema)
  if (cached === undefined) serdes.set(schema, cached = serde(schema))
  return cached
}

/** The schema's JSON form as JSON Schema, which Restate shows in its UI and OpenAPI; undefined if it has none. */
function jsonSchema(schema: Codec): object | undefined {
  try {
    const document = Schema.toJsonSchemaDocument(schema)
    return Object.keys(document.definitions).length > 0 ? { ...document.schema, $defs: document.definitions } : document.schema
  } catch {
    return undefined
  }
}

/** The cause's only reason when it is a typed failure; undefined for defects, interruptions and several reasons. */
export const singleFailure = <E>(cause: Cause.Cause<E>): Cause.Fail<E> | undefined =>
  cause.reasons.length === 1 && Cause.isFailReason(cause.reasons[0]) ? cause.reasons[0] : undefined

/** Domain failures are terminal. Defects and interruption are never encoded as domain errors. */
export function unwrapExit<A, E>(exit: Exit.Exit<A, E>, error: Codec): A {
  if (Exit.isSuccess(exit)) return exit.value
  const failure = singleFailure(exit.cause)
  if (failure) {
    if (failure.error instanceof ClientError) {
      const cause = failure.error.cause
      if (cause instanceof Error) throw cause
      throw new Error("Restate operation failed", { cause })
    }
    throw declaredTerminalError(failure.error, error)
  }
  const cause = Cause.squash(exit.cause)
  // A defect containing TerminalError must not silently become a terminal domain failure.
  if (cause instanceof Error && !(cause instanceof sdk.TerminalError)) throw cause
  throw new Error("Effect handler failed", { cause })
}

/** The error's own message, else its tag: a Schema.Error without a message has an empty one. */
function failureMessage(failure: unknown): string {
  if (failure instanceof Error && failure.message) return failure.message
  const tag = (failure as { readonly _tag?: unknown } | null)?._tag
  return typeof tag === "string" ? tag : "Restate handler failed"
}

/** A TerminalError's metadata, or the metadata in an ingress error body; undefined for other errors. */
function failureMetadata(error: unknown): Readonly<Record<string, unknown>> | undefined {
  if (error instanceof sdk.TerminalError) return error.metadata
  if (!(error instanceof HttpCallError)) return undefined
  let body: unknown
  try { body = JSON.parse(error.responseText) } catch { return undefined }
  if (typeof body !== "object" || body === null || !("metadata" in body)) return undefined
  return typeof body.metadata === "object" && body.metadata !== null ? body.metadata as Record<string, unknown> : undefined
}

/** A declared failure as JSON text: how it travels in error metadata and in retryable error messages. */
const encodeDeclared = (failure: unknown, schema: Codec): string =>
  JSON.stringify(Schema.encodeSync(Schema.toCodecJson(schema))(failure))
/** The declared failure in `encodeDeclared` text. Throws when it does not decode. */
const decodeDeclared = (encoded: string, schema: Codec): { readonly value: unknown } =>
  ({ value: Schema.decodeUnknownSync(Schema.toCodecJson(schema))(JSON.parse(encoded)) })

/** The declared failure a Restate error carries; undefined when it carries none. Throws when it does not decode. */
export function decodeFailure(error: unknown, schema: Codec): { readonly value: unknown } | undefined {
  const encoded = failureMetadata(error)?.[errorKey]
  if (encoded === undefined) return undefined
  if (typeof encoded !== "string") throw new Error("Invalid declared failure metadata")
  return decodeDeclared(encoded, schema)
}

const retryablePrefix = "restate-effect/retryable:"
/**
 * A declared failure that `Restate.run` retries: Restate keeps only a retryable error's message,
 * so the encoded failure travels in it, and is restored once retries are exhausted.
 * With `retryAfter` (milliseconds), Restate waits that long before the next attempt instead of
 * the step's backoff.
 */
export function retryableFailure(failure: unknown, error: Codec, retryAfter?: number): Error {
  const message = retryablePrefix + encodeDeclared(failure, error)
  return retryAfter === undefined ? new Error(message) : new sdk.RetryableError(message, { retryAfter })
}
/** The declared failure a terminal error from exhausted `Restate.run` retries carries, if any. */
export function exhaustedFailure(error: sdk.TerminalError, schema: Codec): { readonly value: unknown } | undefined {
  if (!error.message.startsWith(retryablePrefix)) return undefined
  return decodeDeclared(error.message.slice(retryablePrefix.length), schema)
}
/** A declared failure as a TerminalError: a readable message for people, the encoded error for decoding. */
export function declaredTerminalError(failure: unknown, error: Codec): sdk.TerminalError {
  const encoded = encodeDeclared(failure, error)
  return new sdk.TerminalError(failureMessage(failure), { metadata: { [errorKey]: encoded } })
}
