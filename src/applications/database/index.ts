import { Application } from './application'

export function create() {
  const app = new Application()

  return {
    app,
    isBackgroundApplication: true,
    close: app.close.bind(app),
  }
}
