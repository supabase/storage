/**
 * The common fields every storage event payload carries (the v1 `BasePayload`, minus the
 * transport-level `singletonKey`/`scheduleAt`, which are produce options here:
 * `idempotencyKey`/`delayMs`).
 */
export interface BasePayload {
  $version?: string
  reqId?: string
  sbReqId?: string
  tenant: {
    ref: string
    host: string
  }
}

/** A storage event payload as it travels on the wire: base fields plus `region`, both
 * injected at produce time. */
export type WirePayload<T extends BasePayload = BasePayload> = T & { region: string }

/**
 * The static shape the queue's middlewares read directly off a topic's bound message class —
 * `eventType`/`allowSync`/`disableKeys` are declared once, on the event class itself (see
 * `storageEvent` in `storage/events/base.ts`), with sensible defaults, instead of a
 * free-standing metadata object hand-maintained separately from the class. The queue layer
 * never knows concrete events beyond this structural contract.
 */
export interface QueueEventOptions {
  /** The v1 `eventName()` — the message-type header, webhook `event.type`, and the per-tenant
   * disable key when `disableKeys` isn't overridden. */
  readonly eventType: string
  /**
   * Whether this event may run synchronously in-process (v1 `allowSync`): `false` ⇒ the
   * produce-failure fallback rethrows, and the env-disabled sync wave skips it with a warning.
   * Defaults to `true`.
   */
  readonly allowSync?: boolean
  /**
   * All per-tenant disable keys this concrete message matches, derived from its payload.
   * Defaults to `[eventType]`; the webhook event overrides this to add `Webhook:{type}` and
   * `Webhook:{type}:{bucket}/{name}` scoping. Method-shorthand (not an arrow-typed property) so
   * a subtype (`StorageEventClass<T>`) can narrow `data` to its own payload type — TS checks
   * method parameters bivariantly, arrow-typed properties strictly.
   */
  disableKeys?(data: unknown): string[]
}

/**
 * Runtime guard for `BasePayload` — validates the `tenant` shape before extracting fields from
 * a produce message's `data: unknown`, instead of trusting a bare `as BasePayload` cast at the
 * point of use.
 */
export function isBasePayload(data: unknown): data is BasePayload {
  if (typeof data !== 'object' || data === null) return false
  const tenant = (data as { tenant?: unknown }).tenant
  return (
    typeof tenant === 'object' &&
    tenant !== null &&
    typeof (tenant as { ref?: unknown }).ref === 'string' &&
    typeof (tenant as { host?: unknown }).host === 'string'
  )
}
