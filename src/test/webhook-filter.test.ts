import { shouldDisableWebhookEvent } from '@storage/events/lifecycle/webhook-filter'

describe('webhook filter', () => {
  test('matches object-level disableEvents entries with Unicode and URL-reserved object names', () => {
    const objectName = '폴더/子目录/파일-🙂-q?foo=1&bar=%25+plus;semi:colon,#frag.png'
    const eventType = 'ObjectCreated:Post'

    const disabled = shouldDisableWebhookEvent(disabledEvents(eventType, objectName), eventType, {
      bucketId: 'bucket6',
      name: objectName,
    })

    expect(disabled).toBe(true)
  })

  test('does not match URL-encoded object-level disableEvents entries', () => {
    const objectName = '폴더/子目录/파일-🙂-q?foo=1&bar=%25+plus;semi:colon,#frag.png'
    const eventType = 'ObjectCreated:Post'

    const disabled = shouldDisableWebhookEvent(
      [`Webhook:${eventType}:bucket6/${encodeURIComponent(objectName)}`],
      eventType,
      {
        bucketId: 'bucket6',
        name: objectName,
      }
    )

    expect(disabled).toBe(false)
  })
})

function disabledEvents(eventType: string, objectName: string) {
  return [`Webhook:${eventType}:bucket6/${objectName}`]
}
