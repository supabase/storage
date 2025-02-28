#!/bin/bash

VERSION=$1

docker build -t harbor.internal.millcrest.dev/supabase/storage-api:v$VERSION-tytonai --build-arg VERSION=v$VERSION .

if [ $? -eq 0 ]; then
    docker push harbor.internal.millcrest.dev/supabase/storage-api:v$VERSION-tytonai
    docker tag harbor.internal.millcrest.dev/supabase/storage-api:v$VERSION-tytonai asia-southeast1-docker.pkg.dev/tytonai/docker/supabase/storage-api:v$VERSION-tytonai
    docker push asia-southeast1-docker.pkg.dev/tytonai/docker/supabase/storage-api:v$VERSION-tytonai
fi