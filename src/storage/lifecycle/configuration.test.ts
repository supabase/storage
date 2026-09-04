import type { BucketLifecycleConfiguration } from '../schemas/lifecycle'
import {
  LifecycleConfigurationValidationError,
  lifecycleConfigurationsEqual,
  lifecycleConfigurationToS3,
  normalizeLifecycleConfiguration,
} from './configuration'

describe('lifecycle configuration', () => {
  test('normalizes canonical and S3 rule shapes identically', () => {
    const canonical = normalizeLifecycleConfiguration({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: {
            noncurrentDays: 30,
            newerNoncurrentVersions: 2,
          },
        },
      ],
    })
    const s3 = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: {
          ID: 'expire-history',
          Status: 'Enabled',
          Filter: {},
          NoncurrentVersionExpiration: {
            NoncurrentDays: 30,
            NewerNoncurrentVersions: 2,
          },
        },
      },
    })

    expect(s3).toEqual(canonical)
  })

  test('rejects a mixed canonical and S3 wrapper', () => {
    expect(() =>
      normalizeLifecycleConfiguration({
        rules: [
          {
            id: 'expire-history',
            status: 'Enabled',
            filter: {},
            noncurrentVersionExpiration: { noncurrentDays: 30 },
          },
        ],
        LifecycleConfiguration: {
          Rule: {
            ID: 'other',
            Status: 'Disabled',
            Filter: {},
            NoncurrentVersionExpiration: { NoncurrentDays: 1 },
          },
        },
      })
    ).toThrow(
      expect.objectContaining({
        category: 'MALFORMED_XML',
        message: 'Lifecycle configuration must not mix rules with LifecycleConfiguration',
      })
    )
  })

  test('normalizes the S3 shape and round-trips the stored representation', () => {
    const canonical = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: [
          {
            ID: 'keep-two',
            Status: 'Enabled',
            Filter: '',
            NoncurrentVersionExpiration: {
              NoncurrentDays: ' 30 ',
              NewerNoncurrentVersions: '\n2\n',
            },
          },
          {
            Status: 'Disabled',
            Filter: {},
            NoncurrentVersionExpiration: { NoncurrentDays: 7 },
          },
        ],
      },
    })

    expect(canonical).toEqual({
      rules: [
        {
          id: 'keep-two',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: {
            noncurrentDays: 30,
            newerNoncurrentVersions: 2,
          },
        },
        {
          id: expect.stringMatching(/^rule-[0-9a-f]{64}$/),
          status: 'Disabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 7 },
        },
      ],
    })
    expect(normalizeLifecycleConfiguration(lifecycleConfigurationToS3(canonical))).toEqual(
      canonical
    )
  })

  test('keeps legacy Prefix spelling distinct and round-trippable', () => {
    const legacy = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: {
          Status: 'Enabled',
          Prefix: '',
          NoncurrentVersionExpiration: { NoncurrentDays: 30 },
        },
      },
    })
    const filtered = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: {
          Status: 'Enabled',
          Filter: '',
          NoncurrentVersionExpiration: { NoncurrentDays: 30 },
        },
      },
    })

    expect(legacy.rules[0]).toHaveProperty('legacyPrefix', '')
    expect(filtered.rules[0]).toHaveProperty('filter', {})
    expect(legacy.rules[0].id).not.toBe(filtered.rules[0].id)
    expect(lifecycleConfigurationToS3(legacy)).toEqual({
      LifecycleConfiguration: {
        Rule: [
          {
            ID: legacy.rules[0].id,
            Status: 'Enabled',
            Prefix: '',
            NoncurrentVersionExpiration: { NoncurrentDays: 30 },
          },
        ],
      },
    })
  })

  test('rejects a null filter instead of coercing it to a whole-bucket filter', () => {
    expect(() =>
      normalizeLifecycleConfiguration({
        rules: [
          {
            id: 'expire-history',
            status: 'Enabled',
            filter: null,
            noncurrentVersionExpiration: { noncurrentDays: 30 },
          },
        ],
      })
    ).toThrow('Rule 1 Filter must be an object')
  })

  test('canonicalizes an empty V2 Prefix filter as a whole-bucket filter', () => {
    const withEmptyPrefix = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: {
          Status: 'Enabled',
          Filter: { Prefix: '' },
          NoncurrentVersionExpiration: { NoncurrentDays: 30 },
        },
      },
    })
    const withEmptyFilter = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: {
          Status: 'Enabled',
          Filter: {},
          NoncurrentVersionExpiration: { NoncurrentDays: 30 },
        },
      },
    })

    expect(withEmptyPrefix).toEqual(withEmptyFilter)
    expect(withEmptyPrefix.rules[0]).toHaveProperty('filter', {})
  })

  test('treats an empty rule ID as omitted for deterministic generation', () => {
    const s3Rule = {
      Status: 'Enabled',
      Filter: {},
      NoncurrentVersionExpiration: { NoncurrentDays: 30 },
    }
    const omitted = normalizeLifecycleConfiguration({
      LifecycleConfiguration: { Rule: s3Rule },
    })
    const emptyS3Id = normalizeLifecycleConfiguration({
      LifecycleConfiguration: { Rule: { ...s3Rule, ID: '' } },
    })
    const emptyCanonicalId = normalizeLifecycleConfiguration({
      rules: [
        {
          id: '',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })

    expect(emptyS3Id).toEqual(omitted)
    expect(emptyCanonicalId).toEqual(omitted)
    expect(omitted.rules[0].id).toMatch(/^rule-[0-9a-f]{64}$/)
  })

  test('generates deterministic collision-safe IDs independent of rule order', () => {
    const rules = [
      {
        Status: 'Enabled',
        Filter: {},
        NoncurrentVersionExpiration: { NoncurrentDays: 30 },
      },
      {
        Status: 'Disabled',
        Filter: {},
        NoncurrentVersionExpiration: { NoncurrentDays: 7 },
      },
    ]
    const normalize = (Rule: unknown[]) =>
      normalizeLifecycleConfiguration({ LifecycleConfiguration: { Rule } })
    const idsByDays = (configuration: ReturnType<typeof normalize>) =>
      new Map(
        configuration.rules.map((rule) => [
          rule.noncurrentVersionExpiration.noncurrentDays,
          rule.id,
        ])
      )

    expect(idsByDays(normalize([...rules].reverse()))).toEqual(idsByDays(normalize(rules)))

    const duplicates = normalize([rules[0], rules[0]])
    expect(duplicates.rules[0].id).toMatch(/^rule-[0-9a-f]{64}$/)
    expect(duplicates.rules[1].id).toBe(`${duplicates.rules[0].id}-1`)

    const generatedId = normalize([rules[0]]).rules[0].id
    const explicitCollision = normalize([
      rules[0],
      {
        ...rules[1],
        ID: generatedId,
      },
    ])
    expect(explicitCollision.rules.map((rule) => rule.id)).toEqual([
      `${generatedId}-1`,
      generatedId,
    ])
  })

  test('treats rule order as irrelevant for generation equality', () => {
    const configuration = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: [
          {
            ID: 'first',
            Status: 'Enabled',
            Filter: '',
            NoncurrentVersionExpiration: { NoncurrentDays: 30 },
          },
          {
            ID: 'second',
            Status: 'Disabled',
            Filter: '',
            NoncurrentVersionExpiration: { NoncurrentDays: 7 },
          },
        ],
      },
    })

    expect(
      lifecycleConfigurationsEqual(configuration, {
        rules: [...configuration.rules].reverse(),
      })
    ).toBe(true)
  })

  test('compares canonical rules field-by-field instead of relying on object key order', () => {
    const left = {
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled' as const,
          filter: {},
          noncurrentVersionExpiration: {
            noncurrentDays: 30,
            newerNoncurrentVersions: 2,
          },
        },
      ],
    }
    const right = {
      rules: [
        {
          noncurrentVersionExpiration: {
            newerNoncurrentVersions: 2,
            noncurrentDays: 30,
          },
          filter: {},
          status: 'Enabled' as const,
          id: 'expire-history',
        },
      ],
    }

    expect(lifecycleConfigurationsEqual(left, right)).toBe(true)
  })

  test('treats a stored rule without an expiration as different', () => {
    const incoming = normalizeLifecycleConfiguration({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })
    const stored = structuredClone(incoming)
    Reflect.deleteProperty(stored.rules[0]!, 'noncurrentVersionExpiration')

    expect(lifecycleConfigurationsEqual(stored, incoming)).toBe(false)
  })

  test('treats a stored null rule as different instead of throwing', () => {
    const incoming = normalizeLifecycleConfiguration({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })

    expect(
      lifecycleConfigurationsEqual(
        { rules: [null] } as unknown as BucketLifecycleConfiguration,
        incoming
      )
    ).toBe(false)
  })

  test('treats a stored rule with a non-object filter as different', () => {
    const incoming = normalizeLifecycleConfiguration({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })
    const stored = structuredClone(incoming)
    const junkRule = stored.rules[0] as unknown as Record<string, unknown>
    junkRule.filter = 5

    expect(lifecycleConfigurationsEqual(stored, incoming)).toBe(false)
  })

  test('treats rules missing an expiration on both sides as equal', () => {
    const incoming = normalizeLifecycleConfiguration({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })
    const left = structuredClone(incoming)
    const right = structuredClone(incoming)
    Reflect.deleteProperty(left.rules[0]!, 'noncurrentVersionExpiration')
    Reflect.deleteProperty(right.rules[0]!, 'noncurrentVersionExpiration')

    expect(lifecycleConfigurationsEqual(left, right)).toBe(true)
  })

  test('omits the S3 expiration element for a rule without an expiration', () => {
    const canonical = normalizeLifecycleConfiguration({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })
    const stored = structuredClone(canonical)
    Reflect.deleteProperty(stored.rules[0]!, 'noncurrentVersionExpiration')

    const s3 = lifecycleConfigurationToS3(stored)

    expect(s3).toEqual({
      LifecycleConfiguration: {
        Rule: [
          {
            ID: 'expire-history',
            Status: 'Enabled',
            Filter: '',
          },
        ],
      },
    })
  })

  test('accepts only the S3 lifecycle namespace', () => {
    const input = {
      LifecycleConfiguration: {
        $: { xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/' },
        Rule: {
          Status: 'Enabled',
          Filter: '',
          NoncurrentVersionExpiration: { NoncurrentDays: '1' },
        },
      },
    }

    expect(normalizeLifecycleConfiguration(input)).toMatchObject({
      rules: [{ status: 'Enabled', noncurrentVersionExpiration: { noncurrentDays: 1 } }],
    })
    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: {
          ...input.LifecycleConfiguration,
          $: { xmlns: 'urn:not-s3' },
        },
      })
    ).toThrow('invalid XML namespace')
  })

  test('accepts one to 1000 rules', () => {
    const rule = {
      Status: 'Enabled',
      Filter: {},
      NoncurrentVersionExpiration: { NoncurrentDays: 1 },
    }

    expect(
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: { Rule: Array.from({ length: 1000 }, () => rule) },
      }).rules
    ).toHaveLength(1000)
    expect(() => normalizeLifecycleConfiguration({ LifecycleConfiguration: { Rule: [] } })).toThrow(
      'between 1 and 1000'
    )
    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: { Rule: Array.from({ length: 1001 }, () => rule) },
      })
    ).toThrow('between 1 and 1000')
  })

  test.each([
    [
      {
        Status: 'Enabled',
        Filter: {},
        NoncurrentVersionExpiration: { NoncurrentDays: 0 },
      },
      'INVALID_ARGUMENT',
      "'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer",
    ],
    [
      {
        Status: 'Enabled',
        Filter: {},
        NoncurrentVersionExpiration: {
          NoncurrentDays: 1,
          NewerNoncurrentVersions: 101,
        },
      },
      'INVALID_ARGUMENT',
      "'NewerNoncurrentVersions' for NoncurrentVersionExpiration action must be an integer between 1 and 100",
    ],
    [
      {
        ID: 'x'.repeat(256),
        Status: 'Enabled',
        Filter: {},
        NoncurrentVersionExpiration: { NoncurrentDays: 1 },
      },
      'INVALID_ARGUMENT',
      'Rule 1 ID must be 255 characters or fewer',
    ],
    [
      {
        Status: 'Enabled',
        Prefix: '',
        NoncurrentVersionExpiration: {
          NoncurrentDays: 1,
          NewerNoncurrentVersions: 2,
        },
      },
      'INVALID_REQUEST',
      'NewerNoncurrentVersions element can only be used in Lifecycle V2.',
    ],
    [
      {
        Status: 'Enabled',
        Filter: {},
      },
      'INVALID_REQUEST',
      'Rule 1 must contain NoncurrentVersionExpiration',
    ],
  ])('categorizes invalid rule %#', (rule, category, message) => {
    expect(() =>
      normalizeLifecycleConfiguration({ LifecycleConfiguration: { Rule: rule } })
    ).toThrow(
      expect.objectContaining({
        category,
        message,
      })
    )
  })

  describe.each([
    {
      label: 'canonical',
      input: (id: string) => ({
        rules: [
          {
            id,
            status: 'Enabled',
            filter: {},
            noncurrentVersionExpiration: { noncurrentDays: 1 },
          },
        ],
      }),
    },
    {
      label: 'S3',
      input: (id: string) => ({
        LifecycleConfiguration: {
          Rule: {
            ID: id,
            Status: 'Enabled',
            Filter: {},
            NoncurrentVersionExpiration: { NoncurrentDays: 1 },
          },
        },
      }),
    },
  ])('$label rule ID validation', ({ input }) => {
    test.each([
      ['255 ASCII code units', 'a'.repeat(255)],
      ['255 non-ASCII BMP code units', 'é'.repeat(255)],
      ['254 astral code units', '😀'.repeat(127)],
      ['255 mixed astral and ASCII code units', '😀'.repeat(127) + 'a'],
      ['XML whitespace and character boundaries', '\t\n\r &<>é\uD7FF\uE000\uFFFD😀'],
    ])('accepts %s', (_label, id) => {
      expect(normalizeLifecycleConfiguration(input(id)).rules[0]?.id).toBe(id)
    })

    test.each([
      ['256 ASCII code units', 'a'.repeat(256)],
      ['256 non-ASCII BMP code units', 'é'.repeat(256)],
      ['256 mixed astral and ASCII code units', '😀'.repeat(127) + 'aa'],
      ['256 astral code units', '😀'.repeat(128)],
    ])('rejects %s', (_label, id) => {
      expect(() => normalizeLifecycleConfiguration(input(id))).toThrow(
        expect.objectContaining({
          category: 'INVALID_ARGUMENT',
          message: 'Rule 1 ID must be 255 characters or fewer',
        })
      )
    })

    test.each([
      ['null', 'a\u0000b'],
      ['control character', 'a\u0001b'],
      ['vertical tab', 'a\u000Bb'],
      ['unit separator', 'a\u001Fb'],
      ['lone high surrogate', 'a\uD800b'],
      ['lone low surrogate', 'a\uDC00b'],
      ['U+FFFE', 'a\uFFFEb'],
      ['U+FFFF', 'a\uFFFFb'],
    ])('rejects an XML-incompatible ID containing %s', (_label, id) => {
      expect(() => normalizeLifecycleConfiguration(input(id))).toThrow(
        expect.objectContaining({
          category: 'INVALID_ARGUMENT',
          message: 'Rule 1 ID must contain only valid XML 1.0 characters',
        })
      )
    })
  })

  test.each([
    [{ Prefix: 'logs/' }, 'INVALID_REQUEST'],
    [{ ObjectSizeGreaterThan: '1' }, 'INVALID_REQUEST'],
    [{ ObjectSizeLessThan: '100' }, 'INVALID_REQUEST'],
    [{ And: { Prefix: 'logs/', Tag: { Key: 'retention', Value: 'short' } } }, 'INVALID_REQUEST'],
    [{ Tag: { Key: 'retention', Value: 'short' } }, 'INVALID_REQUEST'],
    [{ FuturePredicate: true }, 'MALFORMED_XML'],
  ])('rejects unsupported filters without stripping them: %o', (filter, category) => {
    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: {
          Rule: {
            Status: 'Enabled',
            Filter: filter,
            NoncurrentVersionExpiration: { NoncurrentDays: 1 },
          },
        },
      })
    ).toThrow(
      expect.objectContaining({
        category,
        message: 'Rule 1 uses a lifecycle filter that is not supported in v1',
      })
    )
  })

  test('rejects unsupported actions, ambiguous selectors, and duplicate IDs', () => {
    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: {
          Rule: {
            Status: 'Enabled',
            Filter: '',
            Expiration: { Days: 1 },
          },
        },
      })
    ).toThrow(
      expect.objectContaining({
        category: 'INVALID_REQUEST',
        message: 'Rule 1 contains unsupported element Expiration',
      })
    )

    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: {
          Rule: {
            Status: 'Enabled',
            Filter: '',
            FutureAction: { Days: 1 },
          },
        },
      })
    ).toThrow(expect.objectContaining({ category: 'MALFORMED_XML' }))

    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: {
          Rule: {
            Status: 'Enabled',
            Filter: '',
            Prefix: '',
            NoncurrentVersionExpiration: { NoncurrentDays: 1 },
          },
        },
      })
    ).toThrow('exactly one of Filter or Prefix')

    expect(() =>
      normalizeLifecycleConfiguration({
        LifecycleConfiguration: {
          Rule: [
            {
              ID: 'duplicate',
              Status: 'Enabled',
              Filter: {},
              NoncurrentVersionExpiration: { NoncurrentDays: 1 },
            },
            {
              ID: 'duplicate',
              Status: 'Disabled',
              Filter: {},
              NoncurrentVersionExpiration: { NoncurrentDays: 2 },
            },
          ],
        },
      })
    ).toThrow(
      expect.objectContaining({
        category: 'INVALID_ARGUMENT',
        message: 'Rule ID must be unique. Found same ID for more than one rule',
      })
    )
  })

  test('uses the dedicated validation error type', () => {
    expect(() => normalizeLifecycleConfiguration(null)).toThrow(
      LifecycleConfigurationValidationError
    )
  })
})
