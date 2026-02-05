import { QueueDB } from '@internal/queue/database'
import { EventEmitter } from 'events'

describe('Queue Health Monitoring', () => {
  let queueDB: QueueDB
  let mockPool: any

  beforeEach(() => {
    queueDB = new QueueDB({
      connectionString: 'postgresql://test:test@localhost:5432/test',
      max: 10,
      min: 0,
    })
    mockPool = new EventEmitter()
    mockPool.query = jest.fn()
  })

  afterEach(() => {
    queueDB.removeAllListeners()
  })

  describe('QueueDB success event emission', () => {
    it('should emit success event after successful query execution', async () => {
      const successListener = jest.fn()
      queueDB.on('success', successListener)

      mockPool.query.mockResolvedValue({ rows: [{ id: 1 }] })

      await queueDB.open()
      ;(queueDB as any).pool = mockPool

      await queueDB.executeSql('SELECT 1', [])

      expect(successListener).toHaveBeenCalledTimes(1)
    })

    it('should not emit success event when query fails', async () => {
      const successListener = jest.fn()
      queueDB.on('success', successListener)

      mockPool.query.mockRejectedValue(new Error('Connection refused'))

      await queueDB.open()
      ;(queueDB as any).pool = mockPool

      await expect(queueDB.executeSql('SELECT 1', [])).rejects.toThrow('Connection refused')
      expect(successListener).not.toHaveBeenCalled()
    })

    it('should emit success event for each successful query', async () => {
      const successListener = jest.fn()
      queueDB.on('success', successListener)

      mockPool.query.mockResolvedValue({ rows: [] })

      await queueDB.open()
      ;(queueDB as any).pool = mockPool

      await queueDB.executeSql('SELECT 1', [])
      await queueDB.executeSql('SELECT 2', [])
      await queueDB.executeSql('SELECT 3', [])

      expect(successListener).toHaveBeenCalledTimes(3)
    })
  })

  describe('QueueDB error event emission', () => {
    it('should emit error events from pool errors', async () => {
      const errorListener = jest.fn()
      queueDB.on('error', errorListener)

      await queueDB.open()
      mockPool.on('error', (error: Error) => queueDB.emit('error', error))
      ;(queueDB as any).pool = mockPool

      const testError = new Error('Pool connection error')
      mockPool.emit('error', testError)

      expect(errorListener).toHaveBeenCalledTimes(1)
      expect(errorListener).toHaveBeenCalledWith(testError)
    })

    it('should handle ETIMEDOUT errors', async () => {
      const errorListener = jest.fn()
      queueDB.on('error', errorListener)

      await queueDB.open()
      mockPool.on('error', (error: Error) => queueDB.emit('error', error))
      ;(queueDB as any).pool = mockPool

      const timeoutError: any = new Error('connect ETIMEDOUT')
      timeoutError.code = 'ETIMEDOUT'

      mockPool.emit('error', timeoutError)

      expect(errorListener).toHaveBeenCalledTimes(1)
      expect(errorListener).toHaveBeenCalledWith(timeoutError)
    })

    it('should handle ECONNREFUSED errors', async () => {
      const errorListener = jest.fn()
      queueDB.on('error', errorListener)

      await queueDB.open()
      mockPool.on('error', (error: Error) => queueDB.emit('error', error))
      ;(queueDB as any).pool = mockPool

      const connRefusedError: any = new Error('connect ECONNREFUSED 127.0.0.1:5432')
      connRefusedError.code = 'ECONNREFUSED'

      mockPool.emit('error', connRefusedError)

      expect(errorListener).toHaveBeenCalledTimes(1)
      expect(errorListener).toHaveBeenCalledWith(connRefusedError)
    })
  })

  describe('QueueDB lifecycle', () => {
    it('should throw error when executing SQL on unopened database', async () => {
      await expect(queueDB.executeSql('SELECT 1', [])).rejects.toThrow('QueueDB not opened')
    })

    it('should allow queries after opening', async () => {
      mockPool.query.mockResolvedValue({ rows: [{ n: 1 }] })

      await queueDB.open()
      ;(queueDB as any).pool = mockPool

      const result = await queueDB.executeSql('SELECT 1 as n', [])
      expect(result.rows[0].n).toBe(1)
    })

    it('should set opened flag correctly', async () => {
      expect(queueDB.opened).toBe(false)

      await queueDB.open()
      expect(queueDB.opened).toBe(true)

      await queueDB.close()
      expect(queueDB.opened).toBe(false)
    })
  })

  describe('Connection health simulation', () => {
    it('should emit multiple errors followed by success on recovery', async () => {
      const errorListener = jest.fn()
      const successListener = jest.fn()

      queueDB.on('error', errorListener)
      queueDB.on('success', successListener)

      await queueDB.open()
      mockPool.on('error', (error: Error) => queueDB.emit('error', error))
      ;(queueDB as any).pool = mockPool

      // Simulate 3 ECONNREFUSED errors
      const error: any = new Error('connect ECONNREFUSED 127.0.0.1:5432')
      error.code = 'ECONNREFUSED'

      mockPool.emit('error', error)
      mockPool.emit('error', error)
      mockPool.emit('error', error)

      expect(errorListener).toHaveBeenCalledTimes(3)
      expect(successListener).not.toHaveBeenCalled()

      // Simulate recovery with successful query
      mockPool.query.mockResolvedValue({ rows: [] })
      await queueDB.executeSql('SELECT 1', [])

      expect(successListener).toHaveBeenCalledTimes(1)
    })

    it('should handle alternating errors and successes', async () => {
      const errorListener = jest.fn()
      const successListener = jest.fn()

      queueDB.on('error', errorListener)
      queueDB.on('success', successListener)

      await queueDB.open()
      mockPool.on('error', (error: Error) => queueDB.emit('error', error))
      ;(queueDB as any).pool = mockPool

      const error: any = new Error('connection error')
      error.code = 'ECONNREFUSED'

      // Error -> Success -> Error -> Success pattern
      mockPool.emit('error', error)
      expect(errorListener).toHaveBeenCalledTimes(1)

      mockPool.query.mockResolvedValue({ rows: [] })
      await queueDB.executeSql('SELECT 1', [])
      expect(successListener).toHaveBeenCalledTimes(1)

      mockPool.emit('error', error)
      expect(errorListener).toHaveBeenCalledTimes(2)

      await queueDB.executeSql('SELECT 2', [])
      expect(successListener).toHaveBeenCalledTimes(2)
    })
  })
})
