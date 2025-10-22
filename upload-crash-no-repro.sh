#!/bin/bash

## This script has good body and will upload successfully

curl -X POST http://localhost:5000/object/your-private-bucket/my-picture.jpg \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoic2VydmljZV9yb2xlIiwiaWF0IjoxNjEzNTMxOTg1LCJleHAiOjE5MjkxMDc5ODV9.th84OKK0Iz8QchDyXZRrojmKSEZ-OuitQm_5DvLiSIc" \
  -H "Content-Type: multipart/form-data; boundary=----MyBoundary" \
  --data-binary $'------MyBoundary\r\nContent-Disposition: form-data; name="file"; filename="my-picture.jpg"\r\nContent-Type: image/jpeg\r\n\r\nABC\r\n------MyBoundary--\r\n'
