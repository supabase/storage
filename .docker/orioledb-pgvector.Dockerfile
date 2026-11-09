ARG ORIOLEDB_IMAGE=supabase-storage-orioledb:pg17-local
FROM ${ORIOLEDB_IMAGE}

ARG PGVECTOR_VERSION=0.8.2

USER root

RUN apk add --no-cache --virtual .pgvector-build-deps \
      build-base \
      clang \
      git \
      llvm \
    && git clone --depth 1 --branch "v${PGVECTOR_VERSION}" \
      https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector \
    && make clean \
    && make OPTFLAGS="" \
    && make install \
    && rm -rf /tmp/pgvector \
    && apk del .pgvector-build-deps

USER postgres
