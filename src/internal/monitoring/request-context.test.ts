import { getSbReqId, getSbReqIdFromPayload, SUPABASE_REQUEST_ID_HEADER } from './request-context'

describe('request log context helpers', () => {
  it('extracts sbReqId values from request headers', () => {
    expect(
      getSbReqId({
        [SUPABASE_REQUEST_ID_HEADER]: ['sb-req-123', 'sb-req-456'],
      })
    ).toBe('sb-req-123')

    expect(
      getSbReqId({
        [SUPABASE_REQUEST_ID_HEADER]: [],
      })
    ).toBeUndefined()
  })

  it('extracts sbReqId values from queue payloads', () => {
    expect(
      getSbReqIdFromPayload({
        sbReqId: 'sb-req-123',
        reqId: 'trace-123',
      })
    ).toBe('sb-req-123')

    expect(
      getSbReqIdFromPayload({
        event: {
          payload: {
            sbReqId: 'sb-req-456',
            reqId: 'trace-456',
          },
        },
      })
    ).toBe('sb-req-456')

    expect(
      getSbReqIdFromPayload({
        reqId: 'trace-only',
      })
    ).toBeUndefined()
    expect(getSbReqIdFromPayload({ sbReqId: '' })).toBeUndefined()
    expect(getSbReqIdFromPayload(undefined)).toBeUndefined()
  })
})
