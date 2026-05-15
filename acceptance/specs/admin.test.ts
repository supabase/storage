import { randomUUID } from 'node:crypto'
import { CreateBucketCommand, ListBucketsCommand, PutObjectCommand } from '@aws-sdk/client-s3'
import { describeAcceptance, getAcceptanceConfig, requireConfigValue } from '../support/config'
import { AcceptanceHttpClient, createAdminClient } from '../support/http'
import { uniqueBucketName, uniqueObjectKey } from '../support/resources'
import { cleanupS3Bucket, createAcceptanceS3Client } from '../support/s3'

interface TenantSummary {
  fileSizeLimit?: number
  id: string
}

interface MetricConfigEntry {
  enabled: boolean
  name: string
}

interface MetricsConfigResponse {
  metrics: MetricConfigEntry[]
}

interface TenantFeatures {
  icebergCatalog?: {
    enabled?: boolean
    maxCatalogs?: number | null
    maxNamespaces?: number | null
    maxTables?: number | null
  }
  imageTransformation?: {
    enabled?: boolean
    maxResolution?: number | null
  }
  purgeCache?: {
    enabled?: boolean
  }
  s3Protocol?: {
    enabled?: boolean
  }
  vectorBuckets?: {
    enabled?: boolean
    maxBuckets?: number | null
    maxIndexes?: number | null
  }
}

interface TenantDetailResponse {
  anonKey?: string
  capabilities?: Record<string, unknown>
  databasePoolMode?: string | null
  databasePoolUrl?: string | null
  databaseUrl?: string
  fileSizeLimit?: number
  features?: TenantFeatures
  jwtSecret?: string
  maxConnections?: number
  migrationStatus?: string | null
  migrationVersion?: string | null
  serviceKey?: string
}

interface TenantMigrationResponse {
  isLatest?: boolean
  migrationsStatus?: string | null
  migrationsVersion?: string | null
}

interface TenantHealthResponse {
  healthy: boolean
}

interface MessageResponse {
  message?: string
}

interface GenerateSignaturesResponse extends MessageResponse {
  jobId?: string
}

interface TenantMigrationRunResponse {
  migrated?: boolean
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

interface JwksAddResponse {
  kid: string
}

interface JwksToggleResponse {
  result: boolean
}

describeAcceptance(
  'admin API contract',
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
      const jwkKind = `acceptance-${config.runId}`.slice(0, 50)
      const jwkKids = new Set<string>()

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

        const jwk = await client.request<JwksAddResponse>('POST', `/tenants/${tenantId}/jwks`, {
          body: {
            jwk: {
              kty: 'oct',
              k: 'nrRW40eXW1wEzzqhsyIRieFZNrUA59sowrTTWLzPJks',
            },
            kind: jwkKind,
          },
          expectedStatus: 201,
          headers,
        })
        const jwkKid = jwk.json?.kid
        if (typeof jwkKid !== 'string' || jwkKid.length === 0) {
          throw new Error('Admin JWKS create response did not include a non-empty kid')
        }
        expect(jwkKid.startsWith(`${jwkKind}_`)).toBe(true)
        jwkKids.add(jwkKid)

        const disabledJwk = await client.request<JwksToggleResponse>(
          'PUT',
          `/tenants/${tenantId}/jwks/${jwkKid}`,
          {
            body: {
              active: false,
            },
            expectedStatus: 200,
            headers,
          }
        )
        expect(disabledJwk.json?.result).toBe(true)

        await client.request(
          'GET',
          `/tenants/${tenantId}/buckets/acceptance-nonexistent/orphan-objects?before=not-a-date`,
          {
            expectedStatus: 400,
            headers,
          }
        )

        await client.request(
          'DELETE',
          `/tenants/${tenantId}/buckets/acceptance-nonexistent/orphan-objects`,
          {
            body: {
              deleteDbKeys: false,
              deleteS3Keys: false,
            },
            expectedStatus: 400,
            headers,
          }
        )

        const orphanSync = await client.request(
          'DELETE',
          `/tenants/${tenantId}/buckets/acceptance-nonexistent/orphan-objects`,
          {
            body: {
              before: new Date(Date.now() + 1_000).toISOString(),
              deleteS3Keys: true,
            },
            expectedStatus: 200,
            headers,
          }
        )
        expect(orphanSync.headers.get('content-type') ?? 'application/x-ndjson').toContain(
          'application/x-ndjson'
        )
        expect(orphanSync.body).not.toContain('"event":"error"')

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
        if (!credentialId) {
          throw new Error('Admin S3 credential response did not include an id')
        }
        credentialIds.add(credentialId)

        const accessKeyId = credential.json?.access_key
        const secretAccessKey = credential.json?.secret_key
        if (!accessKeyId || !secretAccessKey) {
          throw new Error('Admin S3 credential response did not include access and secret keys')
        }

        const credentialClient = createAcceptanceS3Client({
          accessKeyId,
          secretAccessKey,
        })
        try {
          await expect(credentialClient.send(new ListBucketsCommand({}))).resolves.toBeTruthy()
        } finally {
          credentialClient.destroy()
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

        await client.request('DELETE', `/s3/${tenantId}/credentials`, {
          body: {
            id: credentialId,
          },
          expectedStatus: 204,
          headers,
        })
        credentialIds.delete(credentialId)

        const credentialsAfterDelete = await client.request<S3CredentialListItem[]>(
          'GET',
          `/s3/${tenantId}/credentials`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(credentialsAfterDelete.json?.map((item) => item.id)).not.toContain(credentialId)
      } finally {
        await deactivateJwks(client, tenantId, headers, jwkKids)
        await cleanupS3Credentials(client, tenantId, headers, credentialIds, credentialDescription)
      }
    })

    it('covers tenant lifecycle, migration operations, health, metrics mutation, and queue admin contracts', async () => {
      const config = getAcceptanceConfig()
      const client = createAdminClient()
      const headers = {
        apikey: requireConfigValue(config.adminApiKey, 'ACCEPTANCE_ADMIN_API_KEY'),
      }
      const sourceTenantId = await resolveTenantId(client, headers)
      const sourceTenant = await client.request<TenantDetailResponse>(
        'GET',
        `/tenants/${sourceTenantId}`,
        {
          expectedStatus: 200,
          headers,
        }
      )

      const createdTenantId = uniqueTenantId()
      const upsertedTenantId = uniqueTenantId()
      const tenantIdsToCleanup = new Set<string>()

      try {
        await client.request('POST', `/tenants/${createdTenantId}`, {
          body: buildTenantProvisionBody(sourceTenant.json, 1_048_576),
          expectedStatus: 201,
          headers,
        })
        tenantIdsToCleanup.add(createdTenantId)

        const tenants = await client.request<TenantSummary[]>('GET', '/tenants', {
          expectedStatus: 200,
          headers,
        })
        expect(tenants.json?.map((tenant) => tenant.id)).toContain(createdTenantId)

        await client.request('PATCH', `/tenants/${createdTenantId}`, {
          body: {
            fileSizeLimit: 2_097_152,
          },
          expectedStatus: 204,
          headers,
        })

        const patchedTenant = await client.request<TenantDetailResponse>(
          'GET',
          `/tenants/${createdTenantId}`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(patchedTenant.json?.fileSizeLimit).toBe(2_097_152)

        const health = await client.request<TenantHealthResponse>(
          'GET',
          `/tenants/${createdTenantId}/health`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(health.json?.healthy).toBe(true)

        const migrated = await client.request<TenantMigrationRunResponse>(
          'POST',
          `/tenants/${createdTenantId}/migrations`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(migrated.json?.migrated).toBe(true)

        const migrationState = await client.request<TenantMigrationResponse>(
          'GET',
          `/tenants/${createdTenantId}/migrations`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(migrationState.json?.isLatest).toBe(true)

        const resetTarget = migrationState.json?.migrationsVersion
        if (resetTarget) {
          const reset = await client.request<MessageResponse>(
            'POST',
            `/tenants/${createdTenantId}/migrations/reset`,
            {
              body: {
                untilMigration: resetTarget,
              },
              expectedStatus: 200,
              headers,
            }
          )
          expect(reset.json?.message).toBe('Migrations reset')
        }

        await client.request('PUT', `/tenants/${upsertedTenantId}`, {
          body: buildTenantProvisionBody(sourceTenant.json, 3_145_728),
          expectedStatus: 204,
          headers,
        })
        tenantIdsToCleanup.add(upsertedTenantId)

        await client.request('PUT', `/tenants/${upsertedTenantId}`, {
          body: buildTenantProvisionBody(sourceTenant.json, 4_194_304),
          expectedStatus: 204,
          headers,
        })

        const upsertedTenant = await client.request<TenantDetailResponse>(
          'GET',
          `/tenants/${upsertedTenantId}`,
          {
            expectedStatus: 200,
            headers,
          }
        )
        expect(upsertedTenant.json?.fileSizeLimit).toBe(4_194_304)

        const migrationJobs = await client.request<unknown[] | MessageResponse>(
          'GET',
          `/tenants/${createdTenantId}/migrations/jobs`,
          {
            expectedStatus: [200, 400],
            headers,
          }
        )
        if (migrationJobs.status === 200) {
          expect(Array.isArray(migrationJobs.json)).toBe(true)
        } else {
          expect((migrationJobs.json as MessageResponse | undefined)?.message).toBe(
            'Queue is not enabled'
          )
        }

        const deletedMigrationJobs = await client.request<number | MessageResponse>(
          'DELETE',
          `/tenants/${createdTenantId}/migrations/jobs`,
          {
            expectedStatus: [200, 400],
            headers,
          }
        )
        if (deletedMigrationJobs.status === 200) {
          expect(typeof deletedMigrationJobs.json).toBe('number')
        } else {
          expect((deletedMigrationJobs.json as MessageResponse | undefined)?.message).toBe(
            'Queue is not enabled'
          )
        }

        await expectMetricsConfigMutation(client, headers)

        const queueMigration = await client.request<MessageResponse>(
          'POST',
          '/queue/migrate/pgboss-v10',
          {
            expectedStatus: [200, 400],
            headers,
          }
        )
        expect(queueMigration.json?.message).toBe(
          queueMigration.status === 200 ? 'Migration scheduled' : 'Queue is not enabled'
        )

        await client.request('DELETE', `/tenants/${createdTenantId}`, {
          expectedStatus: 204,
          headers,
        })
        tenantIdsToCleanup.delete(createdTenantId)

        await client.request('GET', `/tenants/${createdTenantId}`, {
          expectedStatus: 404,
          headers,
        })
      } finally {
        for (const tenantId of tenantIdsToCleanup) {
          await client
            .request('DELETE', `/tenants/${tenantId}`, {
              expectedStatus: [204, 404],
              headers,
            })
            .catch(() => undefined)
        }
      }
    })

    it('covers object signature generation scheduling for an uploaded object', async () => {
      const config = getAcceptanceConfig()
      const client = createAdminClient()
      const headers = {
        apikey: requireConfigValue(config.adminApiKey, 'ACCEPTANCE_ADMIN_API_KEY'),
      }
      const tenantId = await resolveTenantId(client, headers)
      const credentialDescription = `acceptance-signature-${config.runId}`
      const credentialIds = new Set<string>()
      const bucketName = uniqueBucketName('sig')
      const objectKey = uniqueObjectKey('sig')
      const expectScheduling = config.target === 'local' && process.env.PG_QUEUE_ENABLE === 'true'

      let credentialClient: ReturnType<typeof createAcceptanceS3Client> | undefined
      let bucketCreated = false

      try {
        const credential = await client.request<S3CredentialResponse>(
          'POST',
          `/s3/${tenantId}/credentials`,
          {
            body: {
              claims: {
                role: 'service_role',
                sub: `acceptance-signature-${config.runId}`,
              },
              description: credentialDescription,
            },
            expectedStatus: 201,
            headers,
          }
        )
        const credentialId = credential.json?.id
        expect(credentialId).toBeTruthy()
        if (!credentialId) {
          throw new Error('Admin S3 credential response did not include an id')
        }
        credentialIds.add(credentialId)

        const accessKeyId = credential.json?.access_key
        const secretAccessKey = credential.json?.secret_key
        if (!accessKeyId || !secretAccessKey) {
          throw new Error('Admin S3 credential response did not include access and secret keys')
        }

        credentialClient = createAcceptanceS3Client({
          accessKeyId,
          secretAccessKey,
        })

        await credentialClient.send(new CreateBucketCommand({ Bucket: bucketName }))
        bucketCreated = true
        await credentialClient.send(
          new PutObjectCommand({
            Body: `signature acceptance ${config.runId}`,
            Bucket: bucketName,
            ContentType: 'text/plain',
            Key: objectKey,
          })
        )

        const scheduled = await client.request<GenerateSignaturesResponse>(
          'POST',
          `/tenants/${tenantId}/storage/generate-signatures`,
          {
            body: {
              bucketId: bucketName,
              force: true,
              objectNames: [objectKey],
            },
            expectedStatus: expectScheduling ? 200 : [200, 400],
            headers,
          }
        )

        if (scheduled.status === 200) {
          expect(scheduled.json?.message).toBe('Object signature generation scheduled')
          expect(typeof scheduled.json?.jobId).toBe('string')
          expect(scheduled.json?.jobId?.length).toBeGreaterThan(0)
        } else {
          expect(expectScheduling).toBe(false)
          expect(scheduled.json?.message).toMatch(
            /^(Queue is not enabled|Tenant migrations must include add-objects-signature before generating signatures)$/
          )
        }
      } finally {
        if (credentialClient) {
          if (bucketCreated) {
            await cleanupS3Bucket(credentialClient, bucketName)
          }
          credentialClient.destroy()
        }
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

async function expectMetricsConfigMutation(
  client: AcceptanceHttpClient,
  headers: Record<string, string>
) {
  const metrics = await client.request<MetricsConfigResponse>('GET', '/metrics/config', {
    expectedStatus: 200,
    headers,
  })
  const metric = metrics.json?.metrics[0]
  expect(metric).toBeTruthy()
  if (!metric) {
    throw new Error('Admin metrics config did not include any registered metrics')
  }

  const metricToRestore = {
    enabled: metric.enabled,
    name: metric.name,
  }
  const toggledMetric = {
    enabled: !metric.enabled,
    name: metric.name,
  }

  try {
    const toggledMetrics = await client.request<MetricsConfigResponse>('PUT', '/metrics/config', {
      body: {
        metrics: [toggledMetric],
      },
      expectedStatus: 200,
      headers,
    })
    expect(
      toggledMetrics.json?.metrics.find((candidate) => candidate.name === metric.name)?.enabled
    ).toBe(toggledMetric.enabled)

    const persistedMetrics = await client.request<MetricsConfigResponse>('GET', '/metrics/config', {
      expectedStatus: 200,
      headers,
    })
    expect(
      persistedMetrics.json?.metrics.find((candidate) => candidate.name === metric.name)?.enabled
    ).toBe(toggledMetric.enabled)
  } finally {
    const restoredMetrics = await client.request<MetricsConfigResponse>('PUT', '/metrics/config', {
      body: {
        metrics: [metricToRestore],
      },
      expectedStatus: 200,
      headers,
    })
    expect(
      restoredMetrics.json?.metrics.find((candidate) => candidate.name === metric.name)?.enabled
    ).toBe(metricToRestore.enabled)

    const persistedRestore = await client.request<MetricsConfigResponse>('GET', '/metrics/config', {
      expectedStatus: 200,
      headers,
    })
    expect(
      persistedRestore.json?.metrics.find((candidate) => candidate.name === metric.name)?.enabled
    ).toBe(metricToRestore.enabled)
  }
}

async function deactivateJwks(
  client: AcceptanceHttpClient,
  tenantId: string,
  headers: Record<string, string>,
  jwkKids: Set<string>
) {
  for (const kid of jwkKids) {
    await client
      .request('PUT', `/tenants/${tenantId}/jwks/${kid}`, {
        body: {
          active: false,
        },
        expectedStatus: [200, 400, 404],
        headers,
      })
      .catch(() => undefined)
  }
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

function uniqueTenantId(): string {
  const letters = randomUUID()
    .replace(/-/g, '')
    .split('')
    .map((char) => String.fromCharCode(97 + (Number.parseInt(char, 16) % 26)))
    .join('')

  return `acc${letters}`.slice(0, 20)
}

function buildTenantProvisionBody(
  sourceTenant: TenantDetailResponse | undefined,
  fileSizeLimit: number
): Record<string, unknown> {
  const body: Record<string, unknown> = {
    anonKey: requireString(sourceTenant?.anonKey, 'anonKey'),
    databaseUrl: requireString(sourceTenant?.databaseUrl, 'databaseUrl'),
    features: normalizeTenantFeatures(sourceTenant?.features),
    fileSizeLimit,
    jwtSecret: requireString(sourceTenant?.jwtSecret, 'jwtSecret'),
    serviceKey: requireString(sourceTenant?.serviceKey, 'serviceKey'),
  }

  if (typeof sourceTenant?.databasePoolMode === 'string') {
    body.databasePoolMode = sourceTenant.databasePoolMode
  }

  if (typeof sourceTenant?.databasePoolUrl === 'string') {
    body.databasePoolUrl = sourceTenant.databasePoolUrl
  }

  if (typeof sourceTenant?.maxConnections === 'number') {
    body.maxConnections = sourceTenant.maxConnections
  }

  return body
}

function normalizeTenantFeatures(features: TenantFeatures | undefined): TenantFeatures {
  return {
    icebergCatalog: {
      enabled: Boolean(features?.icebergCatalog?.enabled),
      maxCatalogs: features?.icebergCatalog?.maxCatalogs,
      maxNamespaces: features?.icebergCatalog?.maxNamespaces,
      maxTables: features?.icebergCatalog?.maxTables,
    },
    imageTransformation: {
      enabled: Boolean(features?.imageTransformation?.enabled),
      maxResolution: features?.imageTransformation?.maxResolution ?? null,
    },
    purgeCache: {
      enabled: Boolean(features?.purgeCache?.enabled),
    },
    s3Protocol: {
      enabled: features?.s3Protocol?.enabled ?? true,
    },
    vectorBuckets: {
      enabled: Boolean(features?.vectorBuckets?.enabled),
      maxBuckets: features?.vectorBuckets?.maxBuckets,
      maxIndexes: features?.vectorBuckets?.maxIndexes,
    },
  }
}

function requireString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`Admin tenant response did not include ${name}`)
  }

  return value
}
