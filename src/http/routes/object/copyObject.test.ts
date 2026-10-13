import fastify from 'fastify'
import { onTestFinished, vi } from 'vitest'
import { setErrorHandler } from '../../error-handler'
import { withFiniteAjv } from '../../finite'
import { authSchema } from '../../schemas/auth'
import { errorSchema } from '../../schemas/error'
import copyObject from './copyObject'

async function createApp() {
  const copy = vi.fn().mockResolvedValue({
    destObject: { id: 'destination-id', name: 'destination.txt' },
  })
  const from = vi.fn(() => ({ copyObject: copy }))
  const app = fastify(withFiniteAjv({}))
  onTestFinished(() => app.close())
  app.addSchema(authSchema)
  app.addSchema(errorSchema)
  setErrorHandler(app)
  app.decorateRequest('storage')
  app.addHook('preHandler', async (request) => {
    request.storage = { from } as never
  })
  app.register(copyObject, { prefix: '/object' })
  await app.ready()
  return { app, copy, from }
}

const headers = { authorization: 'Bearer route-test' }
const payload = {
  bucketId: 'source-bucket',
  sourceKey: 'source.txt',
  destinationKey: 'destination.txt',
}

describe('REST copy metadata validation', () => {
  it.each([
    { name: 'absent', value: undefined, expected: true },
    { name: 'true', value: true, expected: true },
    { name: 'false', value: false, expected: false },
    { name: "'true'", value: 'true', expected: true },
    { name: "'false'", value: 'false', expected: false },
    { name: '0', value: 0, expected: false },
    { name: '1', value: 1, expected: true },
  ])('copies with copyMetadata $name', async ({ value, expected }) => {
    const { app, copy } = await createApp()
    const response = await app.inject({
      method: 'POST',
      url: '/object/copy',
      headers,
      payload: { ...payload, ...(value === undefined ? {} : { copyMetadata: value }) },
    })

    expect(response.statusCode).toBe(200)
    expect(copy).toHaveBeenCalledOnce()
    expect(copy).toHaveBeenCalledWith(
      expect.objectContaining({ copyMetadata: expected, preserveUnspecifiedFileMetadata: true })
    )
  })

  it.each(
    [null, 'COPY', 'REPLACE', 'UNKNOWN', '', [true], [false], [], {}].map((value) => ({ value }))
  )('rejects non-boolean copyMetadata $value before copying', async ({ value }) => {
    const { app, copy, from } = await createApp()
    const response = await app.inject({
      method: 'POST',
      url: '/object/copy',
      headers,
      payload: { ...payload, copyMetadata: value },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({
      code: 'InvalidRequest',
      message: 'body/copyMetadata must be boolean',
    })
    expect(from).not.toHaveBeenCalled()
    expect(copy).not.toHaveBeenCalled()
  })

  it('retains schema coercion for other request fields', async () => {
    const { app, copy } = await createApp()
    const response = await app.inject({
      method: 'POST',
      url: '/object/copy',
      headers,
      payload: { ...payload, sourceKey: 42, copyMetadata: false },
    })

    expect(response.statusCode).toBe(200)
    expect(copy).toHaveBeenCalledWith(
      expect.objectContaining({ sourceKey: '42', copyMetadata: false })
    )
  })
})
