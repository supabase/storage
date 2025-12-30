import { Meter } from '@opentelemetry/api'
import { BaseCollector } from './base-collector'

export class ExternalMemoryCollector extends BaseCollector {
  updateMetricInstruments(meter: Meter): void {
    meter
      .createObservableGauge(`${this.namePrefix}nodejs.memory.external`, {
        description: 'Node.js external memory size in bytes',
        unit: 'By',
      })
      .addCallback((observable) => {
        if (!this._enabled) return
        try {
          const mem = process.memoryUsage()
          if (mem.external !== undefined) {
            observable.observe(mem.external, this.labels)
          }
        } catch {
          // ignore errors
        }
      })

    meter
      .createObservableGauge(`${this.namePrefix}nodejs.memory.array_buffers`, {
        description: 'Node.js ArrayBuffers memory size in bytes',
        unit: 'By',
      })
      .addCallback((observable) => {
        if (!this._enabled) return
        try {
          const mem = process.memoryUsage()
          if (mem.arrayBuffers !== undefined) {
            observable.observe(mem.arrayBuffers, this.labels)
          }
        } catch {
          // ignore errors
        }
      })

    meter
      .createObservableGauge(`${this.namePrefix}nodejs.memory.rss`, {
        description: 'Resident Set Size - total memory allocated for the process',
        unit: 'By',
      })
      .addCallback((observable) => {
        if (!this._enabled) return
        try {
          const mem = process.memoryUsage()
          observable.observe(mem.rss, this.labels)
        } catch {
          // ignore errors
        }
      })
  }

  protected internalEnable(): void {
    // no-op
  }

  protected internalDisable(): void {
    // no-op
  }
}
