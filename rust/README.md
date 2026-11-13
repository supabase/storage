# Rust workspace

`crates/queue-transport` provides queue transport primitives, currently backed by SQS standard queues.
`crates/queue` provides JSON job dispatch, bounded concurrency, visibility heartbeats, retries,
and graceful shutdown.

Jobs use the Node queue manager's bare JSON payload and message attributes. Each worker handles
one job type on one queue. Tenant keys are used as SQS message groups for fair queues; this does
not guarantee ordering or deduplication. All consumers of a queue must support its payloads.

Configure visibility, execution timeout, retry policy, and dead-letter settings consistently across
workers sharing a queue. Set retry backoff and attempt limits with `WorkerConfig::retry`. An attempt
limit requires a distinct dead-letter queue supplied through `set_dead_letter`. Provision queues
separately and align their redrive settings. A failed dead-letter copy leaves the original message
for redelivery; a crash after copying can produce duplicates.

Set `WorkerConfig::execution_timeout` to limit handler runtime. On timeout the worker signals
cancellation, then aborts a handler that exceeds `shutdown_grace` and retries it. Automatic
heartbeats are disabled by default; enable `auto_heartbeat` with an execution timeout to renew
visibility during longer jobs. Without heartbeats, visibility expiry cancels the job.

Delivery is at least once, and visibility is advisory. Jobs need their own correctness fences,
such as Postgres claims. Retry delays are capped at 11 hours and by the remaining SQS visibility
window, so a message can reappear earlier than requested.

The SQS tests require an endpoint and are ignored by default. Start ElasticMQ from the repo root:

    docker compose --project-directory . -f ./.docker/docker-compose-infra.yml up -d elasticmq

Then run from `rust/`:

    QUEUE_TEST_SQS_ENDPOINT=http://127.0.0.1:9324 \
    cargo test -- --ignored
