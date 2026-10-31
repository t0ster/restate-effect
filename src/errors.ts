import { Data } from "effect"

export class ClientError extends Data.TaggedError("RestateClientError")<{
  readonly service: string
  readonly method: string
  readonly cause: unknown
}> {
  override get message() { return `${this.service}/${this.method} failed` }
}
