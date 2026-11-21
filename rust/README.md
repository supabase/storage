# Rust workspace

`crates/queue` (`storage-queue`) provides transport primitives with SQS standard-queue semantics
(at-least-once delivery, visibility timeout, receive count, delay, redrive). It currently implements
the transport for AWS SQS. It does not provide event dispatch or job policies.

The SQS contract tests are ignored by default and require an endpoint. From `rust/`:

    STORAGE_QUEUE_TEST_SQS_ENDPOINT=http://127.0.0.1:9324 \
    cargo test --test contract -- --ignored

ElasticMQ (the SQS endpoint) is the `elasticmq` service in `.docker/docker-compose-infra.yml`.

Start it from the repo root:

    docker compose --project-directory . -f ./.docker/docker-compose-infra.yml up -d elasticmq
