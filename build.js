// eslint-disable-next-line @typescript-eslint/no-var-requires
const { build } = require('esbuild')

build({
  entryPoints: ['./src/**/*.ts'],
  bundle: false,
  outdir: 'dist',
  platform: 'node',
  format: 'cjs',
  target: 'node20',
  sourcemap: true,
  tsconfig: 'tsconfig.json',
  loader: { '.ts': 'ts' },
}).catch((e) => {
  console.error(e)
  process.exit(1)
})
