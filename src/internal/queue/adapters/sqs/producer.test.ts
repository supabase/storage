import type { SQSClient } from '@aws-sdk/client-sqs'
import { PartialSendError } from '../../manager/adapter'
import { createSend } from './producer'
import type { Queues } from './queues'

type Entry = { Id: string; MessageBody: string }
type Answer = (entries: Entry[]) => { Failed?: { Id: string; SenderFault: boolean }[] }

/** An SQS client whose SendMessageBatch calls are answered by `answer`, which may throw. */
function sender(answer: Answer) {
  const accepted: string[] = []
  const client = {
    send: async (command: { input: { Entries: Entry[] } }) => {
      const entries = command.input.Entries
      const response = answer(entries)
      const failed = new Set((response.Failed ?? []).map((failure) => failure.Id))
      for (const entry of entries) if (!failed.has(entry.Id)) accepted.push(entry.MessageBody)
      return response
    },
  } as unknown as SQSClient
  const queues = {
    resolve: async () => ({ queue: 'jobs', url: 'https://sqs.test/jobs', arn: 'arn', fifo: false }),
  } as unknown as Queues
  return { send: createSend({ sqs: client, queues }), accepted }
}

const messages = (count: number) =>
  Array.from({ length: count }, (_, n) => ({ data: `m${n}`, headers: {} }))

describe('sqs send', () => {
  it('names the messages SQS did not confirm when a later call fails', async () => {
    // 25 messages are three calls: the one carrying m10..m19 is refused outright.
    const { send, accepted } = sender((entries) => {
      if (entries.some((entry) => entry.MessageBody === 'm10')) throw new Error('throttled')
      return {}
    })

    const error = await send('jobs', messages(25), {}).catch((e: unknown) => e)

    expect(error).toBeInstanceOf(PartialSendError)
    const { unsent, cause } = error as PartialSendError
    expect(cause).toEqual(new Error('throttled'))
    // Everything SQS took is left out, and everything else is named.
    const taken = new Set(accepted.map((body) => Number(body.slice(1))))
    expect(taken.size).toBeGreaterThan(0)
    expect([...unsent].sort((a, b) => a - b)).toEqual(
      Array.from({ length: 25 }, (_, n) => n).filter((n) => !taken.has(n))
    )
    expect([...unsent]).toEqual(expect.arrayContaining([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]))
  })

  it('names the entries SQS rejected inside one call', async () => {
    const { send } = sender((entries) => ({
      Failed: entries
        .filter((entry) => entry.MessageBody === 'm2')
        .map(({ Id }) => ({ Id, SenderFault: true })),
    }))

    const error = await send('jobs', messages(4), {}).catch((e: unknown) => e)

    expect(error).toBeInstanceOf(PartialSendError)
    expect([...(error as PartialSendError).unsent]).toEqual([2])
  })

  it('rejects with the plain error when nothing was confirmed', async () => {
    const { send } = sender(() => {
      throw new Error('unreachable')
    })

    const error = await send('jobs', messages(3), {}).catch((e: unknown) => e)

    expect(error).not.toBeInstanceOf(PartialSendError)
    expect(error).toEqual(new Error('unreachable'))
  })
})
