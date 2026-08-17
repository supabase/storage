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

  test('normalizes the S3 shape and round-trips the stored representation', () => {
    const canonical = normalizeLifecycleConfiguration({
      LifecycleConfiguration: {
        Rule: [
          {
            ID: 'keep-two',
            Status: 'Enabled',
            Filter: '',
            NoncurrentVersionExpiration: {
              NoncurrentDays: '30',
              NewerNoncurrentVersions: '2',
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
