import { Instrumentation, InstrumentationConfig } from '@opentelemetry/instrumentation'
import { trace, Span, SpanStatusCode, TracerProvider, MeterProvider } from '@opentelemetry/api'

interface GenericInstrumentationConfig extends InstrumentationConfig {
  targetClass: new (...args: any[]) => any
  methodsToInstrument: string[]
  setName?: (name: string, attrs: Record<string, any>, targetClass: new () => any) => string
  setAttributes?: Record<
    GenericInstrumentationConfig['methodsToInstrument'][number],
    (...args: any[]) => Record<string, string>
  >
}

class ClassInstrumentation implements Instrumentation {
  readonly instrumentationName: string
  readonly instrumentationVersion = '1.0.0'

  private _config: GenericInstrumentationConfig
  private _tracerProvider?: TracerProvider
  private _meterProvider?: MeterProvider

  constructor(config: GenericInstrumentationConfig) {
    this._config = config
    this.instrumentationName = config.targetClass.name.toLowerCase()

    if (config.enabled) {
      this.enable()
    }
  }

  setTracerProvider(tracerProvider: TracerProvider): void {
    this._tracerProvider = tracerProvider
  }

  setMeterProvider(meterProvider: MeterProvider): void {
    this._meterProvider = meterProvider
  }

  getConfig(): GenericInstrumentationConfig {
    return this._config
  }

  setConfig(config: Partial<GenericInstrumentationConfig>): void {
    this._config = { ...this._config, ...config }
  }

  enable(): void {
    this.patchMethods()
  }

  disable(): void {
    this.unpatchMethods()
  }

  private patchMethods(): void {
    const { targetClass, methodsToInstrument } = this._config
    const proto = targetClass.prototype

    methodsToInstrument.forEach((methodName) => {
      if (methodName in proto && typeof proto[methodName] === 'function') {
        this.patchMethod(proto, methodName)
      }
    })
  }

  private unpatchMethods(): void {
    const { targetClass, methodsToInstrument } = this._config
    const proto = targetClass.prototype

    methodsToInstrument.forEach((methodName) => {
      if (methodName in proto && proto[methodName].__original) {
        proto[methodName] = proto[methodName].__original
      }
    })
  }

  private patchMethod(proto: any, methodName: string): void {
    const original = proto[methodName]
    const instrumentationName = this.instrumentationName
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const _this = this

    proto[methodName] = function (...args: any[]) {
      const tracer = trace.getTracer(instrumentationName)

      return tracer.startActiveSpan(
        `${instrumentationName}.${methodName}`,
        {
          attributes: {
            storageInternal: true,
          },
        },
        async (span: Span) => {
          try {
            const customAttrs = _this._config.setAttributes?.[methodName]?.apply(this, args) || {}
            span.setAttributes(customAttrs)

            const spanName = _this._config.setName?.(
              `${instrumentationName}.${methodName}`,
              customAttrs,
              this
            )

            if (spanName) {
              span.updateName(spanName)
            }
            const result = await original.apply(this, args)
            span.setStatus({ code: SpanStatusCode.OK })
            return result
          } catch (error) {
            if (error instanceof Error) {
              span.setAttributes({
                error: JSON.stringify({ message: error.message, stack: error.stack }),
                stack: error.stack,
              })
            }

            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: error instanceof Error ? error.message : String(error),
            })
            throw error
          } finally {
            span.end()
          }
        }
      )
    }

    proto[methodName].__original = original
  }
}

export { ClassInstrumentation, GenericInstrumentationConfig }
