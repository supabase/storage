import { describe, expect, it } from 'vitest'
import { getSslSettings } from '../ssl.js'

describe('database SSL settings', () => {
  it('disables SSL when sslmode=disable', () => {
    expect(getSslSettings('postgres://user:pass@localhost/db?sslmode=disable')).toBe(false)
  })

  it('uses certificate verification for sslmode=require without a root cert', () => {
    expect(getSslSettings('postgres://user:pass@localhost/db?sslmode=require')).toBe(true)
  })

  it('uses the configured root certificate when present', () => {
    expect(getSslSettings('postgres://user:pass@localhost/db?sslmode=require', '<cert>')).toEqual({
      ca: '<cert>',
    })
  })

  it('keeps sslmode=prefer compatible with poolers that use self-signed certificates', () => {
    expect(getSslSettings('postgres://user:pass@localhost/db?sslmode=prefer')).toEqual({
      rejectUnauthorized: false,
    })
  })
})
