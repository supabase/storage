import crypto from 'k6/crypto';
import encoding from 'k6/encoding';

const JWT_SECRET = __ENV.AUTH_JWT_SECRET || 'f023d3db-39dc-4ac9-87b2-b2be72e9162b';

export const BASE_URL = __ENV.BASE_URL || 'http://storage:5000';
export const VARIANT = __ENV.BENCH_VARIANT || 'unknown';

// Generate a service_role JWT for authenticated requests
export function makeServiceRoleJwt() {
  const header = encoding.b64encode(
    JSON.stringify({ alg: 'HS256', typ: 'JWT' }),
    'rawurl'
  );
  const payload = encoding.b64encode(
    JSON.stringify({
      role: 'service_role',
      iss: 'supabase',
      iat: Math.floor(Date.now() / 1000),
      exp: Math.floor(Date.now() / 1000) + 3600,
    }),
    'rawurl'
  );
  const sig = encoding.b64encode(
    crypto.hmac('sha256', JWT_SECRET, `${header}.${payload}`, 'binary'),
    'rawurl'
  );
  return `${header}.${payload}.${sig}`;
}

export function authHeaders() {
  return {
    Authorization: `Bearer ${makeServiceRoleJwt()}`,
  };
}

export function taggedParams(extra = {}) {
  return { tags: { variant: VARIANT, ...extra } };
}
