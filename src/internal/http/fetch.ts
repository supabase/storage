/**
 * Discard an unread fetch response body while keeping the connection reusable.
 *
 * `body.cancel()` aborts the in-flight request when the body hasn't fully
 * arrived yet, which makes undici destroy the socket instead of returning it
 * to the dispatcher's keep-alive pool (and the server sees a connection
 * reset). Draining the stream to completion releases the socket for reuse.
 *
 * Only use this when the response body is expected to be small (the caller's
 * request timeout still bounds how long the drain can take). For large bodies
 * that won't be read, prefer `body.cancel()` and pay the reconnect.
 */
export async function drainResponseBody(response: Response): Promise<void> {
  if (!response.body) {
    return
  }

  try {
    for await (const _ of response.body) {
      // discard
    }
  } catch {
    // the connection is already lost; nothing to preserve
  }
}
