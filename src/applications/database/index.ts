import { Application } from './application'
import type { DatabaseWorkerIdentity } from './ownership'

export function create(options: { worker?: DatabaseWorkerIdentity } = {}) {
  const app = new Application(options)

  return {
    app,
    isBackgroundApplication: true,
    close: app.close.bind(app),
  }
}
