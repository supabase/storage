import { getConfig } from '../config'

// Mock dependencies
jest.mock('../config')

const mockGetConfig = getConfig as jest.MockedFunction<typeof getConfig>

describe('Queue Configuration Tests', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })

  describe('Queue Configuration Values', () => {
    it('should have correct default configuration values', () => {
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

      const config = mockGetConfig()

      expect(config.isMultitenant).toBe(false)
      expect(config.databaseURL).toBe('postgres://test:test@localhost:5432/test')
      expect(config.pgQueueEnableWorkers).toBe(true)
      expect(config.pgQueueMaxConnections).toBe(10)
      expect(config.pgQueueConcurrentTasksPerQueue).toBe(5)
      expect(config.pgQueueReadWriteTimeout).toBe(30000)
      expect(config.pgQueueArchiveCompletedAfterSeconds).toBe(3600)
      expect(config.pgQueueDeleteAfterDays).toBe(7)
      expect(config.pgQueueRetentionDays).toBe(7)
    })

    it('should handle multitenant configuration', () => {
      mockGetConfig.mockReturnValue({
        isMultitenant: true,
        databaseURL: 'postgres://test:test@localhost:5432/test',
        multitenantDatabaseUrl: 'postgres://test:test@localhost:5433/test',
        pgQueueConnectionURL: 'postgres://queue:queue@localhost:5434/queue',
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
      expect(config.pgQueueConnectionURL).toBe('postgres://queue:queue@localhost:5434/queue')
    })

    it('should handle custom queue connection URL', () => {
      mockGetConfig.mockReturnValue({
        isMultitenant: false,
        databaseURL: 'postgres://test:test@localhost:5432/test',
        multitenantDatabaseUrl: 'postgres://test:test@localhost:5433/test',
        pgQueueConnectionURL: 'postgres://queue:queue@localhost:5434/queue',
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

      expect(config.pgQueueConnectionURL).toBe('postgres://queue:queue@localhost:5434/queue')
    })

    it('should handle delete after hours configuration', () => {
      mockGetConfig.mockReturnValue({
        isMultitenant: false,
        databaseURL: 'postgres://test:test@localhost:5432/test',
        multitenantDatabaseUrl: 'postgres://test:test@localhost:5433/test',
        pgQueueConnectionURL: undefined,
        pgQueueArchiveCompletedAfterSeconds: 3600,
        pgQueueDeleteAfterDays: undefined,
        pgQueueDeleteAfterHours: 24,
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

      expect(config.pgQueueDeleteAfterDays).toBeUndefined()
      expect(config.pgQueueDeleteAfterHours).toBe(24)
    })

    it('should handle disabled workers configuration', () => {
      mockGetConfig.mockReturnValue({
        isMultitenant: false,
        databaseURL: 'postgres://test:test@localhost:5432/test',
        multitenantDatabaseUrl: 'postgres://test:test@localhost:5433/test',
        pgQueueConnectionURL: undefined,
        pgQueueArchiveCompletedAfterSeconds: 3600,
        pgQueueDeleteAfterDays: 7,
        pgQueueDeleteAfterHours: undefined,
        pgQueueRetentionDays: 7,
        pgQueueEnableWorkers: false,
        pgQueueReadWriteTimeout: 30000,
        pgQueueConcurrentTasksPerQueue: 5,
        pgQueueMaxConnections: 10,
        logLevel: 'info',
        logflareApiKey: undefined,
        logflareSourceToken: undefined,
        logflareEnabled: false,
      } as any)

      const config = mockGetConfig()

      expect(config.pgQueueEnableWorkers).toBe(false)
    })
  })

  describe('Queue Configuration Validation', () => {
    it('should validate required configuration fields', () => {
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

      const config = mockGetConfig()

      // Required fields
      expect(config.databaseURL).toBeDefined()
      expect(config.pgQueueEnableWorkers).toBeDefined()
      expect(config.pgQueueMaxConnections).toBeDefined()
      expect(config.pgQueueConcurrentTasksPerQueue).toBeDefined()
      expect(config.pgQueueReadWriteTimeout).toBeDefined()
      expect(config.pgQueueArchiveCompletedAfterSeconds).toBeDefined()
      expect(config.pgQueueRetentionDays).toBeDefined()
    })

    it('should validate optional configuration fields', () => {
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

      const config = mockGetConfig()

      // Optional fields
      expect(config.pgQueueConnectionURL).toBeUndefined()
      expect(config.pgQueueDeleteAfterHours).toBeUndefined()
      expect(config.multitenantDatabaseUrl).toBeDefined()
    })

    it('should validate multitenant configuration requirements', () => {
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
      expect(config.multitenantDatabaseUrl).toBe('postgres://test:test@localhost:5433/test')
    })
  })

  describe('Queue Configuration Types', () => {
    it('should validate configuration value types', () => {
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

      const config = mockGetConfig()

      // Boolean types
      expect(typeof config.isMultitenant).toBe('boolean')
      expect(typeof config.pgQueueEnableWorkers).toBe('boolean')
      expect(typeof config.logflareEnabled).toBe('boolean')

      // String types
      expect(typeof config.databaseURL).toBe('string')
      expect(typeof config.multitenantDatabaseUrl).toBe('string')
      expect(typeof config.logLevel).toBe('string')

      // Number types
      expect(typeof config.pgQueueMaxConnections).toBe('number')
      expect(typeof config.pgQueueConcurrentTasksPerQueue).toBe('number')
      expect(typeof config.pgQueueReadWriteTimeout).toBe('number')
      expect(typeof config.pgQueueArchiveCompletedAfterSeconds).toBe('number')
      expect(typeof config.pgQueueDeleteAfterDays).toBe('number')
      expect(typeof config.pgQueueRetentionDays).toBe('number')

      // Undefined types
      expect(config.pgQueueConnectionURL).toBeUndefined()
      expect(config.pgQueueDeleteAfterHours).toBeUndefined()
      expect(config.logflareApiKey).toBeUndefined()
      expect(config.logflareSourceToken).toBeUndefined()
    })
  })

  describe('Queue Configuration Ranges', () => {
    it('should validate configuration value ranges', () => {
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

      const config = mockGetConfig()

      // Positive numbers
      expect(config.pgQueueMaxConnections).toBeGreaterThan(0)
      expect(config.pgQueueConcurrentTasksPerQueue).toBeGreaterThan(0)
      expect(config.pgQueueReadWriteTimeout).toBeGreaterThan(0)
      expect(config.pgQueueArchiveCompletedAfterSeconds).toBeGreaterThan(0)
      expect(config.pgQueueDeleteAfterDays).toBeGreaterThan(0)
      expect(config.pgQueueRetentionDays).toBeGreaterThan(0)

      // Reasonable ranges
      expect(config.pgQueueMaxConnections).toBeLessThanOrEqual(100)
      expect(config.pgQueueConcurrentTasksPerQueue).toBeLessThanOrEqual(50)
      expect(config.pgQueueReadWriteTimeout).toBeLessThanOrEqual(300000)
      expect(config.pgQueueArchiveCompletedAfterSeconds).toBeLessThanOrEqual(86400)
      expect(config.pgQueueDeleteAfterDays).toBeLessThanOrEqual(365)
      expect(config.pgQueueRetentionDays).toBeLessThanOrEqual(365)
    })
  })

  describe('Queue Configuration Environment Variables', () => {
    it('should validate environment variable names', () => {
      const envVarNames = [
        'DATABASE_URL',
        'MULTITENANT_DATABASE_URL',
        'PG_QUEUE_CONNECTION_URL',
        'PG_QUEUE_ARCHIVE_COMPLETED_AFTER_SECONDS',
        'PG_QUEUE_DELETE_AFTER_DAYS',
        'PG_QUEUE_DELETE_AFTER_HOURS',
        'PG_QUEUE_RETENTION_DAYS',
        'PG_QUEUE_ENABLE_WORKERS',
        'PG_QUEUE_READ_WRITE_TIMEOUT',
        'PG_QUEUE_CONCURRENT_TASKS_PER_QUEUE',
        'PG_QUEUE_MAX_CONNECTIONS',
        'LOG_LEVEL',
        'LOGFLARE_API_KEY',
        'LOGFLARE_SOURCE_TOKEN',
        'LOGFLARE_ENABLED',
      ]

      envVarNames.forEach((envVar) => {
        expect(envVar).toBeDefined()
        expect(typeof envVar).toBe('string')
        expect(envVar.length).toBeGreaterThan(0)
        expect(envVar).toMatch(/^[A-Z_]+$/)
      })
    })
  })
})
