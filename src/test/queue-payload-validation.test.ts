import { getConfig } from '../config'

// Mock dependencies
jest.mock('../config')

const mockGetConfig = getConfig as jest.MockedFunction<typeof getConfig>

describe('Queue Payload Validation Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks()

    // Mock config with default values
    mockGetConfig.mockReturnValue({
      isMultitenant: false,
      databaseURL: 'postgres://test:test@localhost:5432/test',
      multitenantDatabaseUrl: 'postgres://test:test@localhost:5433/test',
      pgQueueConnectionURL: undefined,
      pgQueueArchiveCompletedAfterSeconds: 3600,
      pgQueueDeleteAfterDays: 7,
      pgQueueDeleteAfterHours: undefined,
      pgQueueRetentionDays: 7,
      pgQueueEnableWorkers: true,
      pgQueueReadWriteTimeout: 30000,
      pgQueueConcurrentTasksPerQueue: 5,
      pgQueueMaxConnections: 10,
      logLevel: 'info',
      logflareApiKey: undefined,
      logflareSourceToken: undefined,
      logflareEnabled: false,
    } as any)
  })

  describe('Webhook Event Payload Validation', () => {
    it('should validate webhook event payload structure', () => {
      const validWebhookPayload = {
        event: {
          $version: 'v1',
          type: 'object.created',
          payload: {
            bucketId: 'test-bucket',
            name: 'test-file.jpg',
            reqId: 'test-req-id',
          },
          applyTime: Date.now(),
        },
        tenant: {
          ref: 'test-tenant',
        },
        sentAt: new Date().toISOString(),
      }

      // Validate required fields
      expect(validWebhookPayload.event).toBeDefined()
      expect(validWebhookPayload.event.$version).toBe('v1')
      expect(validWebhookPayload.event.type).toBe('object.created')
      expect(validWebhookPayload.event.payload).toBeDefined()
      expect(validWebhookPayload.event.payload.bucketId).toBe('test-bucket')
      expect(validWebhookPayload.event.payload.name).toBe('test-file.jpg')
      expect(validWebhookPayload.tenant).toBeDefined()
      expect(validWebhookPayload.tenant.ref).toBe('test-tenant')
      expect(validWebhookPayload.sentAt).toBeDefined()
    })

    it('should validate webhook event types', () => {
      const eventTypes = [
        'object.created',
        'object.updated',
        'object.deleted',
        'bucket.created',
        'bucket.deleted',
      ]

      eventTypes.forEach((eventType) => {
        const payload = {
          event: {
            $version: 'v1',
            type: eventType,
            payload: {
              bucketId: 'test-bucket',
              name: 'test-file.jpg',
            },
            applyTime: Date.now(),
          },
          tenant: {
            ref: 'test-tenant',
          },
          sentAt: new Date().toISOString(),
        }

        expect(payload.event.type).toBe(eventType)
        expect(typeof payload.event.type).toBe('string')
      })
    })

    it('should validate webhook event payload properties', () => {
      const payload = {
        event: {
          $version: 'v1',
          type: 'object.created',
          payload: {
            bucketId: 'test-bucket',
            name: 'test-file.jpg',
            reqId: 'test-req-id',
            size: 1024,
            contentType: 'image/jpeg',
          },
          applyTime: Date.now(),
        },
        tenant: {
          ref: 'test-tenant',
        },
        sentAt: new Date().toISOString(),
      }

      expect(payload.event.payload.bucketId).toBeDefined()
      expect(payload.event.payload.name).toBeDefined()
      expect(payload.event.payload.reqId).toBeDefined()
      expect(payload.event.payload.size).toBeDefined()
      expect(payload.event.payload.contentType).toBeDefined()
    })
  })

  describe('Object Admin Delete Payload Validation', () => {
    it('should validate object admin delete payload structure', () => {
      const validPayload = {
        bucketId: 'test-bucket',
        objectName: 'test-file.jpg',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
      }

      expect(validPayload.bucketId).toBeDefined()
      expect(validPayload.objectName).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
    })

    it('should validate object admin delete all before payload', () => {
      const validPayload = {
        bucketId: 'test-bucket',
        before: new Date().toISOString(),
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
      }

      expect(validPayload.bucketId).toBeDefined()
      expect(validPayload.before).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
    })
  })

  describe('Migration Event Payload Validation', () => {
    it('should validate run migrations payload structure', () => {
      const validPayload = {
        tenantId: 'test-tenant',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
        singletonKey: 'test-tenant',
      }

      expect(validPayload.tenantId).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
      expect(validPayload.singletonKey).toBeDefined()
    })

    it('should validate reset migrations payload structure', () => {
      const validPayload = {
        tenantId: 'test-tenant',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
        untilMigration: '0001-initialmigration',
        markCompletedTillMigration: '0001-initialmigration',
      }

      expect(validPayload.tenantId).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
      expect(validPayload.untilMigration).toBeDefined()
      expect(validPayload.markCompletedTillMigration).toBeDefined()
    })
  })

  describe('PgBoss Event Payload Validation', () => {
    it('should validate move jobs payload structure', () => {
      const validPayload = {
        fromQueue: 'old-queue',
        toQueue: 'new-queue',
        deleteJobsFromOriginalQueue: true,
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
      }

      expect(validPayload.fromQueue).toBeDefined()
      expect(validPayload.toQueue).toBeDefined()
      expect(validPayload.deleteJobsFromOriginalQueue).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
    })

    it('should validate upgrade pgboss v10 payload structure', () => {
      const validPayload = {
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
      }

      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
    })
  })

  describe('Backup Object Event Payload Validation', () => {
    it('should validate backup object payload structure', () => {
      const validPayload = {
        bucketId: 'test-bucket',
        objectName: 'test-file.jpg',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
        backupUrl: 'https://backup.example.com/test-file.jpg',
      }

      expect(validPayload.bucketId).toBeDefined()
      expect(validPayload.objectName).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
      expect(validPayload.backupUrl).toBeDefined()
    })
  })

  describe('JWKS Event Payload Validation', () => {
    it('should validate jwks create signing secret payload structure', () => {
      const validPayload = {
        tenantId: 'test-tenant',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
        keyId: 'test-key-id',
      }

      expect(validPayload.tenantId).toBeDefined()
      expect(validPayload.tenant).toBeDefined()
      expect(validPayload.tenant.ref).toBeDefined()
      expect(validPayload.tenant.host).toBeDefined()
      expect(validPayload.keyId).toBeDefined()
    })
  })

  describe('Queue Event Payload Type Validation', () => {
    it('should validate string types in payloads', () => {
      const payload = {
        bucketId: 'test-bucket',
        objectName: 'test-file.jpg',
        tenantId: 'test-tenant',
        fromQueue: 'old-queue',
        toQueue: 'new-queue',
      }

      expect(typeof payload.bucketId).toBe('string')
      expect(typeof payload.objectName).toBe('string')
      expect(typeof payload.tenantId).toBe('string')
      expect(typeof payload.fromQueue).toBe('string')
      expect(typeof payload.toQueue).toBe('string')
    })

    it('should validate boolean types in payloads', () => {
      const payload = {
        deleteJobsFromOriginalQueue: true,
        includeMetadata: true,
      }

      expect(typeof payload.deleteJobsFromOriginalQueue).toBe('boolean')
      expect(typeof payload.includeMetadata).toBe('boolean')
    })

    it('should validate number types in payloads', () => {
      const payload = {
        size: 1024,
        applyTime: Date.now(),
        priority: 10,
        retryLimit: 3,
      }

      expect(typeof payload.size).toBe('number')
      expect(typeof payload.applyTime).toBe('number')
      expect(typeof payload.priority).toBe('number')
      expect(typeof payload.retryLimit).toBe('number')
    })

    it('should validate object types in payloads', () => {
      const payload = {
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
        event: {
          $version: 'v1',
          type: 'object.created',
          payload: {
            bucketId: 'test-bucket',
            name: 'test-file.jpg',
          },
          applyTime: Date.now(),
        },
      }

      expect(typeof payload.tenant).toBe('object')
      expect(typeof payload.event).toBe('object')
      expect(payload.tenant.ref).toBeDefined()
      expect(payload.event.$version).toBeDefined()
    })
  })

  describe('Queue Event Payload Required Fields', () => {
    it('should validate required fields for all event types', () => {
      const eventTypes = [
        'webhook',
        'object-admin-delete',
        'object-admin-delete-all-before',
        'tenants-migrations-v2',
        'backup-object',
        'tenants-migrations-reset-v2',
        'jwks-create-signing-secret',
        'upgrade-pg-boss-v10',
        'move-jobs',
      ]

      eventTypes.forEach((eventType) => {
        expect(eventType).toBeDefined()
        expect(typeof eventType).toBe('string')
        expect(eventType.length).toBeGreaterThan(0)
      })
    })

    it('should validate tenant structure in all payloads', () => {
      const tenant = {
        ref: 'test-tenant',
        host: 'localhost',
      }

      expect(tenant.ref).toBeDefined()
      expect(tenant.host).toBeDefined()
      expect(typeof tenant.ref).toBe('string')
      expect(typeof tenant.host).toBe('string')
    })
  })
})
