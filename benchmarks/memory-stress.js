import http from 'k6/http';
import { check } from 'k6';
import { BASE_URL, authHeaders, taggedParams, VARIANT } from './helpers.js';

export const options = {
  scenarios: {
    sustained_upload: {
      executor: 'constant-arrival-rate',
      rate: 20,
      timeUnit: '1s',
      duration: '5m',
      preAllocatedVUs: 20,
      maxVUs: 100,
      tags: { variant: VARIANT },
    },
  },
};

export function setup() {
  const headers = { ...authHeaders(), 'Content-Type': 'application/json' };
  http.post(
    `${BASE_URL}/bucket`,
    JSON.stringify({ name: 'k6-memory-bucket', public: true }),
    { headers }
  );
  return { bucketName: 'k6-memory-bucket' };
}

export default function (data) {
  const headers = authHeaders();
  const objectName = `mem-${__VU}-${__ITER}-${Date.now()}.bin`;
  const payload = 'x'.repeat(10240); // 10KB payload

  const uploadRes = http.post(
    `${BASE_URL}/object/${data.bucketName}/${objectName}`,
    payload,
    {
      headers: { ...headers, 'Content-Type': 'application/octet-stream' },
      ...taggedParams(),
    }
  );
  check(uploadRes, { 'upload ok': (r) => r.status === 200 });

  const downloadRes = http.get(
    `${BASE_URL}/object/authenticated/${data.bucketName}/${objectName}`,
    { headers, ...taggedParams() }
  );
  check(downloadRes, { 'download ok': (r) => r.status === 200 });
}
