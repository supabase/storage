import { describeAcceptance, getAcceptanceConfig, requireConfigValue } from '../support/config'
import { AcceptanceHttpClient, createAdminClient } from '../support/http'

interface TenantSummary {
  id: string
}

interface MetricsConfigResponse {
  metrics: Array<{
    enabled: boolean
    name: string
  }>
}

interface TenantDetailResponse {
  capabilities?: Record<string, unknown>
  features?: Record<string, unknown>
}

interface S3CredentialResponse {
  access_key: string
  description: string
  id: string
  secret_key: string
}

interface S3CredentialListItem {
  description?: string
  id: string
}

describeAcceptance(
  'extended admin API contract',
  {
    destructive: true,
    profiles: ['full'],
    requires: ['admin'],
  },
  () => {
    it('covers tenant reads, migration and queue validation, metrics config, JWKS validation, orphan validation, and S3 credential CRUD', async () => {
      const config = getAcceptanceConfig()
      const client = createAdminClient()
      const headers = {
        apikey: requireConfigValue(config.adminApiKey, 'ACCEPTANCE_ADMIN_API_KEY'),
      }
      const tenantId = await resolveTenantId(client, headers)
      const credentialDescription = `acceptance-${config.runId}`
      const credentialIds = new Set<string>()

      try {
        const tenant = await client.request<TenantDetailResponse>('GET', `/tenants/${tenantId}`, {
          expectedStatus: 200,
          headers,
        })
        expect(tenant.json?.features).toBeTruthy()
        expect(tenant.json?.capabilities).toBeTruthy()

        const migrations = await client.request('GET', `/tenants/${tenantId}/migrations`, {
          expectedStatus: 200,
          headers,
        })
        expect(migrations.json).toBeTruthy()

        const metrics = await client.request<MetricsConfigResponse>('GET', '/metrics/config', {
          expectedStatus: 200,
          headers,
        })
        expect(Array.isArray(metrics.json?.metrics)).toBe(true)

        await client.request('GET', '/migrations/failed?cursor=not-a-number', {
          expectedStatus: 400,
          headers,
        })

        await client.request('POST', '/migrations/reset/fleet', {
          body: {
            untilMigration: 'not-a-migration',
          },
          expectedStatus: 400,
          headers,
        })

        await client.request('POST', '/queue/move', {
          body: {},
          expectedStatus: 400,
          headers,
        })

        await client.request('POST', `/tenants/${tenantId}/jwks`, {
          body: {
            jwk: {
              kty: 'unsupported',
            },
            kind: 'acceptance_invalid',
          },
          expectedStatus: 400,
          headers,
        })

        const jwksStatus = await client.request('GET', '/tenants/jwks/generate-all-missing', {
          expectedStatus: 200,
          headers,
        })
        expect(jwksStatus.json).toBeTruthy()

        await client.request(
          'GET',
          `/tenants/${tenantId}/buckets/acceptance-nonexistent/orphan-objects?before=not-a-date`,
          {
            expectedStatus: 400,
            headers,
          }
        )

        const credential = await client.request<S3CredentialResponse>(
          'POST',
          `/s3/${tenantId}/credentials`,
          {
            body: {
              claims: {
                role: 'service_role',
                sub: `acceptance-${config.runId}`,
              },
              description: credentialDescription,
            },
            expectedStatus: 201,
            headers,
          }
        )
        const credentialId = credential.json?.id
        expect(credential.json?.access_key).toBeTruthy()
        expect(credential.json?.secret_key).toBeTruthy()
        expect(credentialId).toBeTruthy()
        if (credentialId) {
          credentialIds.add(credentialId)
        }

        const credentials = await client.request<S3CredentialListItem[]>(
          'GET',
          `/s3/${tenantId}/credentials`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(credentials.json?.map((item) => item.id)).toContain(credentialId)
      } finally {
        await cleanupS3Credentials(client, tenantId, headers, credentialIds, credentialDescription)
      }
    })
  }
)

async function resolveTenantId(
  client: AcceptanceHttpClient,
  headers: Record<string, string>
): Promise<string> {
  const configuredTenantId = getAcceptanceConfig().tenantId
  if (configuredTenantId) {
    return configuredTenantId
  }

  const tenants = await client.request<TenantSummary[]>('GET', '/tenants', {
    expectedStatus: 200,
    headers,
  })
  const tenantId = tenants.json?.[0]?.id
  if (!tenantId) {
    throw new Error('Admin acceptance tests require ACCEPTANCE_TENANT_ID or at least one tenant')
  }

  return tenantId
}

async function cleanupS3Credentials(
  client: AcceptanceHttpClient,
  tenantId: string,
  headers: Record<string, string>,
  credentialIds: Set<string>,
  credentialDescription: string
) {
  await client
    .request<S3CredentialListItem[]>('GET', `/s3/${tenantId}/credentials`, {
      expectedStatus: 200,
      headers,
    })
    .then((credentials) => {
      for (const credential of credentials.json ?? []) {
        if (credential.description === credentialDescription) {
          credentialIds.add(credential.id)
        }
      }
    })
    .catch(() => undefined)

  for (const credentialId of credentialIds) {
    await client
      .request('DELETE', `/s3/${tenantId}/credentials`, {
        body: {
          id: credentialId,
        },
        expectedStatus: [204, 400, 404],
        headers,
      })
      .catch(() => undefined)
  }
}
