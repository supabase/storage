'use strict'

import fs from 'node:fs'
import path from 'node:path'

describe('unicode object name migration', () => {
  test('keeps both SQL_ASCII and non-SQL_ASCII constraint branches', () => {
    const migrationPath = path.resolve(
      __dirname,
      '../../migrations/tenant/57-unicode-object-names.sql'
    )
    const sql = fs.readFileSync(migrationPath, 'utf8')

    expect(sql).toContain(`server_encoding = 'SQL_ASCII'`)
    expect(sql).toContain(String.raw`name !~ E'\\xEF\\xBF\\xBE|\\xEF\\xBF\\xBF'`)
    expect(sql).toContain(String.raw`name !~ E'\\xED[\\xA0-\\xBF][\\x80-\\xBF]'`)
    expect(sql).toContain(String.raw`POSITION(U&'\FFFE' IN name) = 0`)
    expect(sql).toContain(String.raw`POSITION(U&'\FFFF' IN name) = 0`)
    expect(sql).toContain('ADD CONSTRAINT objects_name_check')
    expect(sql).toContain('ADD CONSTRAINT s3_multipart_uploads_key_check')
    expect(sql).toContain('ADD CONSTRAINT s3_multipart_uploads_parts_key_check')
    expect(sql).toContain(String.raw`key !~ E'[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]'`)
    expect(sql).toContain(String.raw`key !~ E'\\xED[\\xA0-\\xBF][\\x80-\\xBF]'`)
    expect(sql).toContain(String.raw`POSITION(U&'\FFFE' IN key) = 0`)
    expect(sql).toContain(String.raw`POSITION(U&'\FFFF' IN key) = 0`)
    expect(sql).toContain('NOT VALID')
    expect(sql).toContain('VALIDATE CONSTRAINT objects_name_check')
    expect(sql).toContain('VALIDATE CONSTRAINT s3_multipart_uploads_key_check')
    expect(sql).toContain('VALIDATE CONSTRAINT s3_multipart_uploads_parts_key_check')
  })
})
