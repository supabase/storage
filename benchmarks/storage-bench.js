import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { BASE_URL, authHeaders, taggedParams, VARIANT } from './helpers.js';

export const options = {
  scenarios: {
    health_check: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: 'healthCheck',
      tags: { scenario: 'health_check', variant: VARIANT },
    },
    object_crud: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '1m', target: 20 },
        { duration: '30s', target: 0 },
      ],
      exec: 'objectCrud',
      tags: { scenario: 'object_crud', variant: VARIANT },
    },
    bucket_operations: {
      executor: 'constant-vus',
      vus: 5,
      duration: '2m',
      exec: 'bucketOperations',
      tags: { scenario: 'bucket_operations', variant: VARIANT },
    },
  },
  thresholds: {
    'http_req_duration{scenario:health_check}': ['p(95)<100'],
    'http_req_duration{scenario:object_crud}': ['p(95)<2000'],
    'http_req_failed': ['rate<0.05'],
  },
};

export function setup() {
  const headers = { ...authHeaders(), 'Content-Type': 'application/json' };
  const res = http.post(
    `${BASE_URL}/bucket`,
    JSON.stringify({ name: 'k6-bench-bucket', public: true }),
    { headers }
  );
  check(res, {
    'bucket created or exists': (r) => r.status === 200 || r.status === 409,
  });
  return { bucketName: 'k6-bench-bucket' };
}

export function healthCheck() {
  const res = http.get(`${BASE_URL}/status`, taggedParams());
  check(res, { 'status 200': (r) => r.status === 200 });
}

export function objectCrud(data) {
  const headers = authHeaders();
  const objectName = `bench-${__VU}-${__ITER}-${Date.now()}.txt`;
  const payload = 'x'.repeat(1024); // 1KB payload

  group('upload', () => {
    const res = http.post(
      `${BASE_URL}/object/${data.bucketName}/${objectName}`,
      payload,
      {
        headers: { ...headers, 'Content-Type': 'text/plain' },
        ...taggedParams({ op: 'upload' }),
      }
    );
    check(res, { 'upload success': (r) => r.status === 200 });
  });

  group('download', () => {
    const res = http.get(
      `${BASE_URL}/object/authenticated/${data.bucketName}/${objectName}`,
      { headers, ...taggedParams({ op: 'download' }) }
    );
    check(res, { 'download success': (r) => r.status === 200 });
  });

  group('delete', () => {
    const res = http.del(
      `${BASE_URL}/object/${data.bucketName}`,
      JSON.stringify({ prefixes: [objectName] }),
      {
        headers: { ...headers, 'Content-Type': 'application/json' },
        ...taggedParams({ op: 'delete' }),
      }
    );
    check(res, { 'delete success': (r) => r.status === 200 });
  });

  sleep(0.5);
}

export function bucketOperations() {
  const headers = authHeaders();
  const res = http.get(`${BASE_URL}/bucket`, {
    headers,
    ...taggedParams({ op: 'list_buckets' }),
  });
  check(res, { 'list buckets success': (r) => r.status === 200 });
  sleep(1);
}

export function teardown() {
  // Leave bucket for inspection; clean up manually if needed
}
