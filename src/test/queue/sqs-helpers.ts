import { randomUUID } from 'node:crypto'
import {
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type QueueAttributeName,
  SQSClient,
  type SQSClientConfig,
} from '@aws-sdk/client-sqs'

/**
 * The SQS endpoint the suites run against (ElasticMQ: the `elasticmq` service of the docker
 * infra). The SQS suites skip themselves entirely when it is unset.
 */
export const sqsEndpoint = process.env.QUEUE_TEST_SQS_ENDPOINT

export const sqsClientConfig = (): SQSClientConfig => ({
  endpoint: sqsEndpoint,
  region: 'us-east-1',
  credentials: { accessKeyId: 'x', secretAccessKey: 'x' },
})

export const rawSqsClient = (): SQSClient => new SQSClient(sqsClientConfig())

export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

export const uniqueName = (label: string): string => `${label}_${randomUUID().slice(0, 8)}`

/** Run `fn` until it stops throwing, for assertions on something that lands eventually. */
export const until = async <T>(fn: () => Promise<T>, timeoutMs = 8_000): Promise<T> => {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await fn()
    } catch (error) {
      if (Date.now() >= deadline) throw error
    }
    await sleep(50)
  }
}

export const queueUrlOf = async (client: SQSClient, name: string): Promise<string> => {
  const found = await client.send(new GetQueueUrlCommand({ QueueName: name }))
  return found.QueueUrl as string
}

export const queueAttributesOf = async (
  client: SQSClient,
  name: string,
  attributeNames: QueueAttributeName[]
): Promise<Partial<Record<string, string>>> => {
  const response = await client.send(
    new GetQueueAttributesCommand({
      QueueUrl: await queueUrlOf(client, name),
      AttributeNames: attributeNames,
    })
  )
  return response.Attributes ?? {}
}
