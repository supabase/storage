import { validateXRobotsTag } from '../storage/validators/x-robots-tag'

describe('validateXRobotsTag', () => {
  describe('invalid inputs', () => {
    it('should throw error for empty string', () => {
      expect(() => validateXRobotsTag('')).toThrow(
        'X-Robots-Tag header value must be a non-empty string'
      )
    })

    it('should throw error for whitespace-only string', () => {
      expect(() => validateXRobotsTag('   ')).toThrow(
        'X-Robots-Tag header value must be a non-empty string'
      )
    })

    it('should throw error for non-string value', () => {
      expect(() => validateXRobotsTag(null as unknown as string)).toThrow(
        'X-Robots-Tag header value must be a non-empty string'
      )
    })

    it('should throw error for undefined', () => {
      expect(() => validateXRobotsTag(undefined as unknown as string)).toThrow(
        'X-Robots-Tag header value must be a non-empty string'
      )
    })

    it('should throw error for empty rule in comma-separated list', () => {
      expect(() => validateXRobotsTag('noindex,  , nofollow')).toThrow(
        'X-Robots-Tag header contains empty rule'
      )
    })

    it('should throw error for invalid rule', () => {
      expect(() => validateXRobotsTag('invalidrule')).toThrow(
        'Invalid X-Robots-Tag rule: "invalidrule"'
      )
    })
  })

  describe('valid simple rules', () => {
    it('should accept "all"', () => {
      expect(() => validateXRobotsTag('all')).not.toThrow()
    })

    it('should accept "noindex"', () => {
      expect(() => validateXRobotsTag('noindex')).not.toThrow()
    })

    it('should accept "nofollow"', () => {
      expect(() => validateXRobotsTag('nofollow')).not.toThrow()
    })

    it('should accept "none"', () => {
      expect(() => validateXRobotsTag('none')).not.toThrow()
    })

    it('should accept "nosnippet"', () => {
      expect(() => validateXRobotsTag('nosnippet')).not.toThrow()
    })

    it('should accept "indexifembedded"', () => {
      expect(() => validateXRobotsTag('indexifembedded')).not.toThrow()
    })

    it('should accept "notranslate"', () => {
      expect(() => validateXRobotsTag('notranslate')).not.toThrow()
    })

    it('should accept "noimageindex"', () => {
      expect(() => validateXRobotsTag('noimageindex')).not.toThrow()
    })
  })

  describe('multiple rules', () => {
    it('should accept multiple valid rules separated by commas', () => {
      expect(() => validateXRobotsTag('noindex, nofollow')).not.toThrow()
    })

    it('should accept multiple rules with extra whitespace', () => {
      expect(() => validateXRobotsTag('noindex,   nofollow,  noimageindex')).not.toThrow()
    })

    it('should accept rules with trailing comma and whitespace', () => {
      expect(() => validateXRobotsTag('noindex, nofollow,   ')).not.toThrow()
    })

    it('should accept single rule with trailing comma', () => {
      expect(() => validateXRobotsTag('noindex,')).not.toThrow()
    })

    it('should throw for invalid rule in multiple rules', () => {
      expect(() => validateXRobotsTag('noindex, invalidrule, nofollow')).toThrow(
        'Invalid X-Robots-Tag rule: "invalidrule"'
      )
    })
  })

  describe('max-snippet parametric rule', () => {
    it('should accept valid max-snippet with number', () => {
      expect(() => validateXRobotsTag('max-snippet: 50')).not.toThrow()
    })

    it('should accept max-snippet with 0', () => {
      expect(() => validateXRobotsTag('max-snippet: 0')).not.toThrow()
    })

    it('should throw for max-snippet with negative number', () => {
      expect(() => validateXRobotsTag('max-snippet: -5')).toThrow(
        'X-Robots-Tag "max-snippet" value must be a non-negative number'
      )
    })

    it('should throw for max-snippet with non-numeric value', () => {
      expect(() => validateXRobotsTag('max-snippet: abc')).toThrow(
        'X-Robots-Tag "max-snippet" value must be a non-negative number'
      )
    })

    it('should throw for max-snippet without value', () => {
      expect(() => validateXRobotsTag('max-snippet:')).toThrow(
        'X-Robots-Tag rule "max-snippet" requires a value'
      )
    })

    it('should throw for max-snippet with whitespace-only value', () => {
      expect(() => validateXRobotsTag('max-snippet:   ')).toThrow(
        'X-Robots-Tag rule "max-snippet" requires a value'
      )
    })
  })

  describe('max-image-preview parametric rule', () => {
    it('should accept "none"', () => {
      expect(() => validateXRobotsTag('max-image-preview: none')).not.toThrow()
    })

    it('should accept "standard"', () => {
      expect(() => validateXRobotsTag('max-image-preview: standard')).not.toThrow()
    })

    it('should accept "large"', () => {
      expect(() => validateXRobotsTag('max-image-preview: large')).not.toThrow()
    })

    it('should throw for invalid value', () => {
      expect(() => validateXRobotsTag('max-image-preview: invalid')).toThrow(
        'X-Robots-Tag "max-image-preview" value must be one of: none, standard, large'
      )
    })

    it('should throw for missing value', () => {
      expect(() => validateXRobotsTag('max-image-preview:')).toThrow(
        'X-Robots-Tag rule "max-image-preview" requires a value'
      )
    })
  })

  describe('max-video-preview parametric rule', () => {
    it('should accept positive number', () => {
      expect(() => validateXRobotsTag('max-video-preview: 30')).not.toThrow()
    })

    it('should accept 0', () => {
      expect(() => validateXRobotsTag('max-video-preview: 0')).not.toThrow()
    })

    it('should accept -1 (no limit)', () => {
      expect(() => validateXRobotsTag('max-video-preview: -1')).not.toThrow()
    })

    it('should throw for number less than -1', () => {
      expect(() => validateXRobotsTag('max-video-preview: -2')).toThrow(
        'X-Robots-Tag "max-video-preview" value must be a number >= -1'
      )
    })

    it('should throw for non-numeric value', () => {
      expect(() => validateXRobotsTag('max-video-preview: abc')).toThrow(
        'X-Robots-Tag "max-video-preview" value must be a number >= -1'
      )
    })

    it('should throw for missing value', () => {
      expect(() => validateXRobotsTag('max-video-preview:')).toThrow(
        'X-Robots-Tag rule "max-video-preview" requires a value'
      )
    })
  })

  describe('unavailable_after parametric rule', () => {
    it('should accept valid RFC 822 date', () => {
      expect(() =>
        validateXRobotsTag('unavailable_after: Wed, 03 Dec 2025 13:09:53 GMT')
      ).not.toThrow()
    })

    it('should accept valid ISO 8601 date', () => {
      expect(() => validateXRobotsTag('unavailable_after: 2025-12-03T13:09:53Z')).not.toThrow()
    })

    it('should accept other valid date format', () => {
      expect(() => validateXRobotsTag('unavailable_after: 2025-12-03')).not.toThrow()
    })

    it('should accept RFC 822 date followed by another rule', () => {
      expect(() =>
        validateXRobotsTag('unavailable_after: Wed, 03 Dec 2025 13:09:53 GMT, noindex')
      ).not.toThrow()
    })

    it('should throw for invalid date', () => {
      expect(() => validateXRobotsTag('unavailable_after: not-a-date')).toThrow(
        'X-Robots-Tag "unavailable_after" value must be a valid date'
      )
    })

    it('should throw for missing value', () => {
      expect(() => validateXRobotsTag('unavailable_after:')).toThrow(
        'X-Robots-Tag rule "unavailable_after" requires a value'
      )
    })
  })

  describe('user agent specific rules', () => {
    it('should accept single rule for specific user agent', () => {
      expect(() => validateXRobotsTag('googlebot: noindex')).not.toThrow()
    })

    it('should accept multiple rules for specific user agent', () => {
      expect(() => validateXRobotsTag('googlebot: noindex, nofollow')).not.toThrow()
    })

    it('should accept multiple user agents with different rules', () => {
      expect(() =>
        validateXRobotsTag('BadBot: noindex, nofollow, googlebot: nofollow')
      ).not.toThrow()
    })

    it('should throw for user agent with no rules', () => {
      expect(() => validateXRobotsTag('googlebot:')).toThrow(
        'X-Robots-Tag user agent "googlebot" has no rules specified'
      )
    })

    it('should throw for user agent with whitespace-only rules', () => {
      expect(() => validateXRobotsTag('googlebot:   ')).toThrow(
        'X-Robots-Tag user agent "googlebot" has no rules specified'
      )
    })

    it('should throw for invalid rule in user agent rules', () => {
      expect(() => validateXRobotsTag('googlebot: invalidrule')).toThrow(
        'Invalid X-Robots-Tag rule: "invalidrule"'
      )
    })
  })

  describe('invalid parametric rule names', () => {
    it('should throw for unknown parametric rule', () => {
      // When an unknown parametric-looking rule is provided, it's treated as a user agent
      // and the value is validated as a rule, which should fail
      expect(() => validateXRobotsTag('unknown-rule: invalidvalue')).toThrow(
        'Invalid X-Robots-Tag rule: "invalidvalue"'
      )
    })
  })

  describe('complex mixed rules', () => {
    it('should accept mix of simple and parametric rules', () => {
      expect(() => validateXRobotsTag('noindex, max-snippet: 100')).not.toThrow()
    })

    it('should accept mix of user agent and parametric rules', () => {
      expect(() => validateXRobotsTag('googlebot: noindex, max-snippet: 50')).not.toThrow()
    })

    it('should accept complex real-world example', () => {
      expect(() =>
        validateXRobotsTag('noindex, nofollow, max-snippet: 100, max-image-preview: large')
      ).not.toThrow()
    })
  })

  describe('whitespace handling', () => {
    it('should accept rules with excessive whitespace between rules', () => {
      expect(() => validateXRobotsTag('noindex,     nofollow,   noimageindex')).not.toThrow()
    })

    it('should accept parametric rules with whitespace after colon', () => {
      expect(() => validateXRobotsTag('max-snippet:    50')).not.toThrow()
    })

    it('should reject rules with spaces in rule names', () => {
      expect(() => validateXRobotsTag('no index')).toThrow('Invalid X-Robots-Tag rule: "no index"')
    })

    it('should accept rules with leading and trailing whitespace', () => {
      expect(() => validateXRobotsTag('  noindex, nofollow  ')).not.toThrow()
    })
  })
})
