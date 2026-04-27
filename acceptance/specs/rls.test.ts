import { randomUUID } from 'node:crypto'
import {
  describeAcceptance,
  encodePathSegments,
  getAcceptanceConfig,
  requireConfigValue,
} from '../support/config'
import { createRestClient } from '../support/http'
import { cleanupRestObjects } from '../support/resources'

describeAcceptance(
  'RLS authorization contract',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['rlsSetup'],
  },
  () => {
    it('allows authenticated access and rejects anon access for configured RLS resources', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const bucketName = requireConfigValue(config.rlsBucket, 'ACCEPTANCE_RLS_BUCKET')
      const readObject = requireConfigValue(config.rlsReadObject, 'ACCEPTANCE_RLS_READ_OBJECT')
      const writePrefix = requireConfigValue(
        config.rlsWritePrefix,
        'ACCEPTANCE_RLS_WRITE_PREFIX'
      ).replace(/\/+$/, '')
      const authenticatedKey = requireConfigValue(
        config.authenticatedKey,
        'ACCEPTANCE_AUTHENTICATED_KEY'
      )
      const anonKey = requireConfigValue(config.anonKey, 'ACCEPTANCE_ANON_KEY')
      const writeKey = `${writePrefix}/acceptance-${config.runId}-${randomUUID()
        .replace(/-/g, '')
        .slice(0, 12)}.txt`
      const deniedWriteKey = `${writePrefix}/acceptance-denied-${config.runId}-${randomUUID()
        .replace(/-/g, '')
        .slice(0, 12)}.txt`

      try {
        await client.request(
          'GET',
          `/object/authenticated/${bucketName}/${encodePathSegments(readObject)}`,
          {
            expectedStatus: 200,
            token: authenticatedKey,
          }
        )

        const deniedRead = await client.request(
          'GET',
          `/object/authenticated/${bucketName}/${encodePathSegments(readObject)}`,
          {
            token: anonKey,
          }
        )
        expect(deniedRead.status).toBeGreaterThanOrEqual(400)

        await client.request('POST', `/object/${bucketName}/${encodePathSegments(writeKey)}`, {
          body: `acceptance-rls-${config.runId}`,
          expectedStatus: 200,
          headers: {
            'content-type': 'text/plain',
            'x-upsert': 'true',
          },
          token: authenticatedKey,
        })

        const deniedWrite = await client.request(
          'POST',
          `/object/${bucketName}/${encodePathSegments(deniedWriteKey)}`,
          {
            body: `acceptance-rls-denied-${config.runId}`,
            headers: {
              'content-type': 'text/plain',
              'x-upsert': 'true',
            },
            token: anonKey,
          }
        )
        expect(deniedWrite.status).toBeGreaterThanOrEqual(400)
      } finally {
        await cleanupRestObjects(bucketName, [writeKey, deniedWriteKey], client)
      }
    })
  }
)
