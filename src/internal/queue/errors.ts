export class QueueBatchBusyError extends Error {
  constructor(queueName: string) {
    super(`Another producer is inserting into queue ${queueName}`)
    this.name = 'QueueBatchBusyError'
  }
}
