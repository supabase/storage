import fs from 'node:fs'
import path from 'node:path'
import FormData from 'form-data'
import { FastifyInstance, LightMyRequestResponse } from 'fastify'
import { serviceKey } from './auth'

/**
 * Absolute path to the shared JPEG test asset. Reused by every test that
 * needs to push real bytes through the upload pipeline so we exercise the
 * real S3 backend rather than a mock.
 */
export const SADCAT_PATH = path.resolve(__dirname, '../../../test/assets/sadcat.jpg')
export const SADCAT_SIZE = fs.statSync(SADCAT_PATH).size

export interface UploadOptions {
  /** Optional bearer token; defaults to the service-role key. */
  token?: string
  /** Extra headers merged on top of defaults. */
  headers?: Record<string, string | number>
  /** `x-upsert: true` shortcut. */
  upsert?: boolean
  /** Override the default 'image/jpeg' content-type (binary only). */
  contentType?: string
  /** Explicit payload file path; defaults to SADCAT_PATH. */
  payloadPath?: string
}

export interface MultipartOptions extends UploadOptions {
  /** Extra form fields such as `metadata` stringified JSON. */
  fields?: Record<string, string>
  /** Buffer to send instead of reading from disk (e.g. Buffer.alloc(0)). */
  payloadBuffer?: Buffer
}

/**
 * POST/PUT an object via multipart form upload. Default auth is service-role
 * so happy-path tests stay one-liners; pass a different `token` to exercise
 * anon / user RLS paths.
 */
export async function multipartUpload(
  app: FastifyInstance,
  method: 'POST' | 'PUT',
  url: string,
  options: MultipartOptions = {}
): Promise<LightMyRequestResponse> {
  const form = new FormData()
  if (options.payloadBuffer) {
    form.append('file', options.payloadBuffer)
  } else {
    form.append('file', fs.createReadStream(options.payloadPath ?? SADCAT_PATH))
  }
  for (const [k, v] of Object.entries(options.fields ?? {})) {
    form.append(k, v)
  }

  const token = options.token ?? (await serviceKey())
  const headers: Record<string, string | number> = {
    ...form.getHeaders(),
    authorization: `Bearer ${token}`,
    ...(options.upsert ? { 'x-upsert': 'true' } : {}),
    ...(options.headers ?? {}),
  }

  return app.inject({ method, url, headers, payload: form })
}

/**
 * POST/PUT an object via a raw binary stream (no multipart envelope). Content
 * length is read from the payload file so the route's early size checks work.
 */
export async function binaryUpload(
  app: FastifyInstance,
  method: 'POST' | 'PUT',
  url: string,
  options: UploadOptions = {}
): Promise<LightMyRequestResponse> {
  const payloadPath = options.payloadPath ?? SADCAT_PATH
  const { size } = fs.statSync(payloadPath)
  const token = options.token ?? (await serviceKey())

  const headers: Record<string, string | number> = {
    authorization: `Bearer ${token}`,
    'Content-Length': size,
    'Content-Type': options.contentType ?? 'image/jpeg',
    ...(options.upsert ? { 'x-upsert': 'true' } : {}),
    ...(options.headers ?? {}),
  }

  return app.inject({ method, url, headers, payload: fs.createReadStream(payloadPath) })
}
