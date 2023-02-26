import { DBError } from './knex'

type InnerValue<T> = T extends Promise<infer A> ? A : T

export class OptionalValue<T, K extends InnerValue<T> = InnerValue<T>> extends Promise<K> {
  protected shouldThrowWhenEmpty?: Error

  protected _value?: T
  protected entity?: string
  protected metadata?: Record<string, any>

  // constructor(
  //   executor: (resolve: (value: K | PromiseLike<K>) => void, reject: (reason?: any) => void) => void
  // ) {
  //   super(executor)
  // }

  init({ value, entity, metadata }: { value: T; entity?: string; metadata?: Record<string, any> }) {
    this._value = value
    this.entity = entity
    this.metadata = metadata
  }
  //
  // constructor(
  //
  // ) {
  //   super((resolve, reject) => {
  //     if (this._value instanceof Promise) {
  //       this._value.then(resolve).catch(reject)
  //     } else {
  //       resolve(this._value as K)
  //     }
  //   })
  // }
  //
  // constructor(
  //
  // ) {}

  finally(onfinally?: (() => void) | null): Promise<K> {
    if (this._value instanceof Promise) {
      return this._value.finally(() => {
        return onfinally ? onfinally() : null
      })
    }

    return Promise.resolve(this._value as K)
  }

  throwIfNotFound(metadata: Record<string, any> = {}) {
    const err = new DBError(`${this.entity || ''} not found`, 404, 'not_found', undefined, {
      ...(this.metadata ?? {}),
      ...(metadata ?? {}),
    })
    this.shouldThrowWhenEmpty = err
    return this as unknown as OptionalValue<NonNullable<T>>
  }

  protected resolveValue<T>(value: T) {
    if (value && this.shouldThrowWhenEmpty) {
      throw this.shouldThrowWhenEmpty
    }

    return value
  }

  then<TResult1 = K, TResult2 = never>(
    onfulfilled?: ((value: K) => PromiseLike<TResult1> | TResult1) | undefined | null,
    onrejected?: ((reason: any) => PromiseLike<TResult2> | TResult2) | undefined | null
  ): Promise<TResult1 | TResult2> {
    if (this._value instanceof Promise) {
      return (this._value as Promise<TResult1>).then((v) => {
        v = this.resolveValue(v)
        return onfulfilled ? onfulfilled(v as InnerValue<typeof this._value>) : v
      })
    }

    try {
      this.resolveValue(this._value)

      return Promise.resolve<TResult1>(
        onfulfilled ? onfulfilled(this._value as K) : (this._value as any)
      )
    } catch (e) {
      return Promise.reject(onrejected ? onrejected(e) : e)
    }
  }

  catch<TResult = never>(
    onrejected?: ((reason: any) => PromiseLike<TResult> | TResult) | undefined | null
  ): Promise<K | TResult> {
    const v = this._value
    if (v instanceof Promise) {
      return v.catch((e) => {
        return onrejected ? onrejected(e) : null
      })
    }

    return Promise.reject(null)
  }
}

export function Opt<T>(value: T, entity?: string, metadata?: Record<string, any>) {
  const promise = new OptionalValue<T>((resolve, reject) => {
    if (value instanceof Promise) {
      value.then(resolve).catch(reject)
    } else {
      resolve(value as InnerValue<T>)
    }
  })

  promise.init({
    value,
    entity,
    metadata,
  })

  return promise
}

export class Optional<T> {
  protected shouldThrowWhenEmpty?: Error
  
  constructor(
    public readonly value: T & Optional<T>,
    public readonly entity?: string,
    public readonly metadata?: Record<string, any>
  ) {
    new Proxy(this, {
      get(target, p: string) {
        if (p in target) {
          return (target as any)[p]
        }

        return (value as any)[p]
      },
    })
  }

  throwIfNotFound(metadata: Record<string, any> = {}) {
    let shouldThrow = false
    
    
    if (Array.isArray(this.value)) {
      shouldThrow = this.value.length === 0 
    }
    
    if (shouldThrow) {
      const err = new DBError(`${this.entity || ''} not found`, 404, 'not_found', undefined, {
        ...(this.metadata ?? {}),
        ...(metadata ?? {}),
      })
      this.shouldThrowWhenEmpty = err
    }
    
    return this as unknown as OptionalValue<NonNullable<T>>
  }
}
