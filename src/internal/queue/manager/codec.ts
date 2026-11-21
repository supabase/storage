/**
 * The wire-format seam, generic over the adapter's wire type `W`. The manager only encodes
 * and decodes; each adapter owns its codec.
 */
export interface Codec<W = unknown> {
  /** MIME type of the encoded payload (e.g. `application/json`). */
  readonly contentType: string
  /** Encode a payload into the adapter's wire type (produce path). */
  encode(value: unknown): W
  /** Decode a stored value back into the payload (consume path). */
  decode(wire: W): unknown
}

/** Structured JSON stored natively (e.g. Postgres `jsonb`). */
export function json<W = unknown>(): Codec<W> {
  return {
    contentType: 'application/json',
    encode: (value) => value as W,
    decode: (wire) => wire,
  }
}

/** JSON text, for a backend whose body is a string. */
export function jsonText(): Codec<string> {
  return {
    contentType: 'application/json',
    encode: (value) => JSON.stringify(value),
    decode: (wire) => JSON.parse(wire) as unknown,
  }
}
