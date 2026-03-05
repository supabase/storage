import fs from 'fs'
import path from 'path'

function collectTestFiles(dir: string): string[] {
  const entries = fs.readdirSync(dir, { withFileTypes: true })
  const files: string[] = []

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name)

    if (entry.isDirectory()) {
      files.push(...collectTestFiles(fullPath))
      continue
    }

    if (entry.isFile() && fullPath.endsWith('.test.ts')) {
      files.push(fullPath)
    }
  }

  return files
}

describe('test hygiene', () => {
  it('does not leak ad-hoc app instances in test files', () => {
    const testFiles = collectTestFiles(__dirname)
    const violations: string[] = []
    const forbiddenPattern = `app().${'inject('}`
    const appFactoryPattern = /\b(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*app\(/g

    for (const file of testFiles) {
      const content = fs.readFileSync(file, 'utf8')
      const lines = content.split('\n')

      lines.forEach((line, index) => {
        if (line.includes(forbiddenPattern)) {
          violations.push(`${path.relative(process.cwd(), file)}:${index + 1}`)
        }
      })

      let match = appFactoryPattern.exec(content)
      while (match) {
        const appVar = match[1]
        if (!content.includes(`${appVar}.close(`)) {
          const declarationLine = content.slice(0, match.index).split('\n').length
          violations.push(`${path.relative(process.cwd(), file)}:${declarationLine}`)
        }
        match = appFactoryPattern.exec(content)
      }

      appFactoryPattern.lastIndex = 0
    }

    expect(violations).toEqual([])
  })
})
