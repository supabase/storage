import { Registry } from 'prom-client'

import app from '../admin-app'

export const adminApp = app({}, { register: new Registry() })
