import { Duration } from "effect"

/** A duration in milliseconds, as the Restate SDK takes it; undefined stays undefined, so the SDK default applies. */
export const millis = (duration: Duration.Input | undefined): number | undefined =>
  duration === undefined ? undefined : Duration.toMillis(duration)
