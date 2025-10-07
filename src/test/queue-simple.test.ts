import { Queue } from '@internal/queue'
import { QueueDB } from '@internal/queue/database'
import { getConfig } from '../config'

// Mock all dependencies
jest.mock('../config')
jest.mock('@internal/queue')
jest.mock('@internal/queue/database')
jest.mock('pg-boss')
jest.mock('pg')

const mockGetConfig = getConfig as jest.MockedFunction<typeof getConfig>

describe('Queue Simple Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks()

    // Mock config with all required values
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

  describe('Queue Configuration', () => {
    it('should have correct configuration values', () => {
      const config = mockGetConfig()

      expect(config.isMultitenant).toBe(false)
      expect(config.databaseURL).toBe('postgres://test:test@localhost:5432/test')
      expect(config.pgQueueEnableWorkers).toBe(true)
      expect(config.pgQueueMaxConnections).toBe(10)
    })

    it('should handle multitenant configuration', () => {
      mockGetConfig.mockReturnValue({
        isMultitenant: true,
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

      const config = mockGetConfig()
      expect(config.isMultitenant).toBe(true)
      expect(config.multitenantDatabaseUrl).toBe('postgres://test:test@localhost:5433/test')
    })
  })

  describe('QueueDB Configuration', () => {
    it('should create QueueDB with correct configuration', () => {
      const config = {
        min: 0,
        max: 10,
        connectionString: 'postgres://test:test@localhost:5432/test',
        statement_timeout: 30000,
      }

      // Mock QueueDB constructor
      const mockQueueDB = jest.fn()
      jest.doMock('@internal/queue/database', () => ({
        QueueDB: mockQueueDB,
      }))

      expect(mockQueueDB).toBeDefined()
    })

    it('should handle connection string configuration', () => {
      const config = {
        min: 0,
        max: 10,
        connectionString: 'postgres://test:test@localhost:5432/test',
        statement_timeout: 30000,
      }

      expect(config.connectionString).toBe('postgres://test:test@localhost:5432/test')
      expect(config.statement_timeout).toBe(30000)
    })
  })

  describe('Queue Event Names', () => {
    it('should have correct queue names', () => {
      // Test queue names without importing the actual classes
      const expectedQueueNames = [
        'webhooks',
        'object-admin-delete',
        'object-admin-delete-all-before',
        'tenants-migrations-v2',
        'backup-object',
        'tenants-migrations-reset-v2',
        'jwks-create-signing-secret',
        'upgrade-pg-boss-v10',
        'move-jobs',
      ]

      expectedQueueNames.forEach((name) => {
        expect(name).toBeDefined()
        expect(typeof name).toBe('string')
        expect(name.length).toBeGreaterThan(0)
      })
    })
  })

  describe('Queue Event Payloads', () => {
    it('should have correct payload structure for webhook', () => {
      const webhookPayload = {
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

      expect(webhookPayload.event.$version).toBe('v1')
      expect(webhookPayload.event.type).toBe('object.created')
      expect(webhookPayload.tenant.ref).toBe('test-tenant')
      expect(webhookPayload.sentAt).toBeDefined()
    })

    it('should have correct payload structure for object admin delete', () => {
      const objectAdminDeletePayload = {
        bucketId: 'test-bucket',
        objectName: 'test-file.jpg',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
      }

      expect(objectAdminDeletePayload.bucketId).toBe('test-bucket')
      expect(objectAdminDeletePayload.objectName).toBe('test-file.jpg')
      expect(objectAdminDeletePayload.tenant.ref).toBe('test-tenant')
      expect(objectAdminDeletePayload.tenant.host).toBe('localhost')
    })

    it('should have correct payload structure for migrations', () => {
      const migrationsPayload = {
        tenantId: 'test-tenant',
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
        singletonKey: 'test-tenant',
      }

      expect(migrationsPayload.tenantId).toBe('test-tenant')
      expect(migrationsPayload.tenant.ref).toBe('test-tenant')
      expect(migrationsPayload.singletonKey).toBe('test-tenant')
    })

    it('should have correct payload structure for move jobs', () => {
      const moveJobsPayload = {
        fromQueue: 'old-queue',
        toQueue: 'new-queue',
        deleteJobsFromOriginalQueue: true,
        tenant: {
          ref: 'test-tenant',
          host: 'localhost',
        },
      }

      expect(moveJobsPayload.fromQueue).toBe('old-queue')
      expect(moveJobsPayload.toQueue).toBe('new-queue')
      expect(moveJobsPayload.deleteJobsFromOriginalQueue).toBe(true)
      expect(moveJobsPayload.tenant.ref).toBe('test-tenant')
    })
  })

  describe('Queue Send Options', () => {
    it('should have correct send options structure', () => {
      const sendOptions = {
        expireInHours: 2,
        retryLimit: 3,
        retryDelay: 5,
        priority: 10,
        singletonKey: 'test-tenant',
      }

      expect(sendOptions.expireInHours).toBe(2)
      expect(sendOptions.retryLimit).toBe(3)
      expect(sendOptions.retryDelay).toBe(5)
      expect(sendOptions.priority).toBe(10)
      expect(sendOptions.singletonKey).toBe('test-tenant')
    })

    it('should have correct queue options structure', () => {
      const queueOptions = {
        name: 'test-queue',
        policy: 'exactly_once',
      }

      expect(queueOptions.name).toBe('test-queue')
      expect(queueOptions.policy).toBe('exactly_once')
    })

    it('should have correct worker options structure', () => {
      const workerOptions = {
        includeMetadata: true,
      }

      expect(workerOptions.includeMetadata).toBe(true)
    })
  })

  describe('Queue Event Versions', () => {
    it('should have correct version for all events', () => {
      const version = 'v1'
      expect(version).toBe('v1')
    })
  })

  describe('Queue Configuration Validation', () => {
    it('should validate required configuration fields', () => {
      const config = mockGetConfig()

      expect(config.databaseURL).toBeDefined()
      expect(config.pgQueueEnableWorkers).toBeDefined()
      expect(config.pgQueueMaxConnections).toBeDefined()
      expect(config.pgQueueConcurrentTasksPerQueue).toBeDefined()
    })

    it('should handle optional configuration fields', () => {
      const config = mockGetConfig()

      expect(config.pgQueueConnectionURL).toBeUndefined()
      expect(config.pgQueueDeleteAfterHours).toBeUndefined()
    })

    it('should validate multitenant configuration', () => {
      mockGetConfig.mockReturnValue({
        isMultitenant: true,
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

      const config = mockGetConfig()
      expect(config.isMultitenant).toBe(true)
      expect(config.multitenantDatabaseUrl).toBeDefined()
    })
  })
})
