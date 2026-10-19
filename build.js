// biome-ignore lint/style/noCommonJs: build script runs as CommonJS
const fs = require('node:fs')
// biome-ignore lint/style/noCommonJs: build script runs as CommonJS
const path = require('node:path')
// biome-ignore lint/style/noCommonJs: build script runs as CommonJS
const { build } = require('esbuild')

const srcDir = path.resolve('src')
const outDir = path.resolve('dist')
const { paths } = JSON.parse(fs.readFileSync('tsconfig.json', 'utf8')).compilerOptions
const aliases = Object.entries(paths).map(([from, [to]]) => [
  from.replace('*', ''),
  path.resolve(to.replace('*', '')),
])

function resolveSource(specifier, fromDir) {
  const alias = aliases.find(([prefix]) => specifier.startsWith(prefix))
  if (!alias && !specifier.startsWith('.')) return
  const base = alias
    ? path.join(alias[1], specifier.slice(alias[0].length))
    : path.resolve(fromDir, specifier)
  for (const file of [`${base}.ts`, path.join(base, 'index.ts')]) {
    if (fs.existsSync(file)) return file
  }
}

// Rewrite tsconfig path aliases and extensionless local imports to relative .js
// paths; native import() in CommonJS output does not resolve either.
function rewriteImports(code, sourceFile) {
  const fromDir = path.dirname(sourceFile)
  return code.replace(/\b(require|import)\("([^"]+)"\)/g, (match, call, specifier) => {
    const target = resolveSource(specifier, fromDir)
    if (!target) return match
    const relative = path
      .relative(fromDir, target)
      .split(path.sep)
      .join('/')
      .replace(/\.ts$/, '.js')
    return `${call}("${relative.startsWith('.') ? relative : `./${relative}`}")`
  })
}

build({
  entryPoints: ['./src/**/*.ts'],
  bundle: false,
  outdir: 'dist',
  platform: 'node',
  format: 'cjs',
  target: 'node24',
  sourcemap: true,
  tsconfig: 'tsconfig.json',
  loader: { '.ts': 'ts' },
  write: false,
})
  .then(({ outputFiles }) => {
    for (const file of outputFiles) {
      const code = file.path.endsWith('.js')
        ? rewriteImports(
            file.text,
            path.join(srcDir, path.relative(outDir, file.path)).replace(/\.js$/, '.ts')
          )
        : file.text
      fs.mkdirSync(path.dirname(file.path), { recursive: true })
      fs.writeFileSync(file.path, code)
    }
  })
  .catch((e) => {
    console.error(e)
    process.exit(1)
  })
