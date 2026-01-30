import { Meter } from '@opentelemetry/api'
import { BaseCollector } from './base-collector'

export class HandlesCollector extends BaseCollector {
  updateMetricInstruments(meter: Meter): void {
    meter
      .createObservableGauge(`${this.namePrefix}nodejs.active_handles.total`, {
        description: 'Number of active libuv handles',
      })
      .addCallback((observable) => {
        if (!this._enabled) return
        // @ts-expect-error - _getActiveHandles is not in types but exists
        const handles = process._getActiveHandles?.() || []
        observable.observe(handles.length, this.labels)
      })

    meter
      .createObservableGauge(`${this.namePrefix}nodejs.active_requests.total`, {
        description: 'Number of active libuv requests',
      })
      .addCallback((observable) => {
        if (!this._enabled) return
        // @ts-expect-error - _getActiveRequests is not in types but exists
        const requests = process._getActiveRequests?.() || []
        observable.observe(requests.length, this.labels)
      })
  }

  protected internalEnable(): void {
    // no-op
  }

  protected internalDisable(): void {
    // no-op
  }
}
