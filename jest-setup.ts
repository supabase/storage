import { getConfig, setEnvPaths } from './src/config'

setEnvPaths(['.env.test', '.env'])

beforeEach(() => {
  getConfig({ reload: true })
})
