import { type ChildProcess, spawn } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import dotenv from 'dotenv'

// Snapshot before loading .env.acceptance so server config only comes from .env.test/.env.
const inheritedEnv = { ...process.env }
loadAcceptanceEnvFile()

const args = process.argv.slice(2)
const profile = readArg('profile') ?? acceptanceEnv('ACCEPTANCE_PROFILE') ?? 'smoke'
const serverEnv = loadServerEnvFiles(inheritedEnv)
const serverPort = serverEnv.SERVER_PORT || serverEnv.PORT || '5000'
const baseUrl = acceptanceEnv('ACCEPTANCE_BASE_URL') ?? `http://127.0.0.1:${serverPort}`
const acceptanceRunEnv = {
  ...process.env,
  ACCEPTANCE_BASE_URL: baseUrl,
  ACCEPTANCE_PROFILE: profile,
  ACCEPTANCE_S3_ENDPOINT: acceptanceEnv('ACCEPTANCE_S3_ENDPOINT') ?? `${baseUrl}/s3`,
  ACCEPTANCE_TUS_ENDPOINT:
    acceptanceEnv('ACCEPTANCE_TUS_ENDPOINT') ?? `${baseUrl}/upload/resumable`,
}

let server: ChildProcess | undefined

main().catch((error) => {
  console.error(error)
  process.exit(1)
})

async function main() {
  try {
    if (process.env.ACCEPTANCE_SKIP_INFRA !== 'true') {
      await run('npm', ['run', 'infra:restart:ci'], serverEnv)
      await run('npm', ['run', 'test:dummy-data'], serverEnv)
    }

    server = spawn(localBin('tsx'), ['src/start/server.ts'], {
      detached: process.platform !== 'win32',
      env: serverEnv,
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    prefixOutput(server.stdout, '[storage] ')
    prefixOutput(server.stderr, '[storage] ')

    await waitForStatus(`${baseUrl}/status`, 60_000)
    await run('npm', ['run', 'acceptance:run', '--', ...args], acceptanceRunEnv)
  } finally {
    if (server) {
      await stopServer(server)
    }
  }
}

function run(cmd: string, runArgs: string[], runEnv: NodeJS.ProcessEnv): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn(command(cmd), runArgs, {
      env: runEnv,
      stdio: 'inherit',
    })

    child.on('exit', (code, signal) => {
      if (code === 0) {
        resolve()
      } else if (signal) {
        reject(new Error(`${cmd} ${runArgs.join(' ')} terminated with ${signal}`))
      } else {
        reject(new Error(`${cmd} ${runArgs.join(' ')} exited with ${code}`))
      }
    })
    child.on('error', reject)
  })
}

async function waitForStatus(url: string, timeoutMs: number) {
  const started = Date.now()
  let lastError: unknown

  while (Date.now() - started < timeoutMs) {
    let response: Response | undefined

    try {
      response = await fetch(url)
      if (response.status === 200) {
        return
      }
    } catch (error) {
      lastError = error
    } finally {
      await response?.body?.cancel()
    }

    await new Promise((resolve) => setTimeout(resolve, 500))
  }

  throw new Error(`Timed out waiting for ${url}: ${String(lastError)}`)
}

async function stopServer(child: ChildProcess) {
  if (hasExited(child)) {
    return
  }

  if (process.platform === 'win32') {
    const exitedAfterKill = waitForExit(child, 5_000)
    child.kill()

    if (!(await exitedAfterKill)) {
      process.stderr.write('[storage] server did not exit after kill\n')
    }
    return
  }

  const exitedAfterTerminate = waitForExit(child, 5_000)
  killProcessTree(child, 'SIGTERM')

  if (await exitedAfterTerminate) {
    return
  }

  process.stderr.write('[storage] server did not exit after SIGTERM; sending SIGKILL\n')

  const exitedAfterKill = waitForExit(child, 2_000)
  killProcessTree(child, 'SIGKILL')

  if (!(await exitedAfterKill)) {
    process.stderr.write('[storage] server did not exit after SIGKILL\n')
  }
}

function waitForExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (hasExited(child)) {
    return Promise.resolve(true)
  }

  return new Promise((resolve) => {
    let timer: NodeJS.Timeout

    function cleanup() {
      clearTimeout(timer)
      child.off('exit', onExit)
    }

    function onExit() {
      cleanup()
      resolve(true)
    }

    child.once('exit', onExit)
    timer = setTimeout(() => {
      cleanup()
      resolve(hasExited(child))
    }, timeoutMs)
  })
}

function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null
}

function killProcessTree(child: ChildProcess, signal: NodeJS.Signals) {
  if (process.platform === 'win32' || !child.pid) {
    child.kill(signal)
    return
  }

  try {
    process.kill(-child.pid, signal)
  } catch (error) {
    if (!isNoSuchProcessError(error)) {
      throw error
    }
  }
}

function isNoSuchProcessError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && error.code === 'ESRCH'
}

function readArg(name: string) {
  const flag = `--${name}`
  const equalsPrefix = `${flag}=`

  for (let index = 0; index < args.length; index++) {
    if (args[index] === flag) {
      return args[index + 1]
    }
    if (args[index].startsWith(equalsPrefix)) {
      return args[index].slice(equalsPrefix.length)
    }
  }

  return undefined
}

function prefixOutput(stream: NodeJS.ReadableStream | null, prefix: string) {
  stream?.setEncoding('utf8')
  stream?.on('data', (chunk: string) => {
    for (const line of chunk.split(/\r?\n/)) {
      if (line) {
        process.stderr.write(`${prefix}${line}\n`)
      }
    }
  })
}

function command(cmd: string) {
  return process.platform === 'win32' ? `${cmd}.cmd` : cmd
}

function localBin(cmd: string) {
  return path.resolve('node_modules', '.bin', command(cmd))
}

function acceptanceEnv(name: string): string | undefined {
  const value = process.env[name]
  return value === undefined || value === '' ? undefined : value
}

function loadAcceptanceEnvFile() {
  const acceptanceEnvPath = process.env.ACCEPTANCE_ENV_FILE ?? '.env.acceptance'

  dotenv.config({ path: path.resolve(acceptanceEnvPath), override: false })
}

function loadServerEnvFiles(baseEnv: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const env = { ...baseEnv }

  loadEnvFileInto(env, '.env.test')
  loadEnvFileInto(env, '.env')

  return env
}

function loadEnvFileInto(env: NodeJS.ProcessEnv, envPath: string) {
  const resolvedPath = path.resolve(envPath)
  if (!fs.existsSync(resolvedPath)) {
    return
  }

  const parsed = dotenv.parse(fs.readFileSync(resolvedPath))
  for (const [name, value] of Object.entries(parsed)) {
    if (env[name] === undefined) {
      env[name] = value
    }
  }
}
