// biome-ignore lint/style/noCommonJs: build script runs as CommonJS
const { build } = require('esbuild')
// biome-ignore lint/style/noCommonJs: build script runs as CommonJS
const { readdirSync, rmSync } = require('node:fs')
// biome-ignore lint/style/noCommonJs: build script runs as CommonJS
const { join, sep } = require('node:path')

function collectEntryPoints(dir) {
  const entryPoints = []

  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const fullPath = join(dir, entry.name)

    if (entry.isDirectory()) {
      if (fullPath === join('src', 'typecheck')) {
        continue
      }

      entryPoints.push(...collectEntryPoints(fullPath))
      continue
    }

    if (entry.isFile() && entry.name.endsWith('.ts')) {
      entryPoints.push(`./${fullPath.split(sep).join('/')}`)
    }
  }

  return entryPoints
}

// Typecheck sentinels are enforced by `tsc -noEmit`
// keep them out of emitted build output.
rmSync('dist/typecheck', { recursive: true, force: true })

build({
  entryPoints: collectEntryPoints('src').sort(),
  bundle: false,
  outdir: 'dist',
  platform: 'node',
  format: 'cjs',
  target: 'node24',
  sourcemap: true,
  tsconfig: 'tsconfig.json',
  loader: { '.ts': 'ts' },
}).catch((e) => {
  console.error(e)
  process.exit(1)
})
