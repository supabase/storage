import * as glob from 'glob'
import fs from 'fs'
import path from 'path'

function main() {
  const migrationsPath = path.join(__dirname, '..', '..', 'migrations', 'tenant', '*.sql')
  const files = glob.sync(migrationsPath).sort()

  const migrations = files.map((file, index) => {
    return {
      file: file
        .split(path.sep)
        .pop()
        ?.replace(/[0-9]+-/, '')
        .replace('.sql', ''),
      index,
    }
  })

  const migrationsEnum = migrations.map((migration) => {
    return `    '${migration.file}': ${migration.index},`
  })

  const template = `
    export const DBMigration = {
       ${migrationsEnum.join('\n')}
    }
  `

  const destinationPath = path.resolve(
    __dirname,
    '..',
    'internal',
    'database',
    'migrations',
    'types.ts'
  )
  fs.writeFileSync(destinationPath, template, 'utf-8')
}

main()
