use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use aws_sdk_sqs::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
};
use serde::{Deserialize, Serialize};
use storage_queue::{
    BoxError, Context, Disposition, Job, RetryPolicy, Worker, WorkerConfig, encode,
};
use storage_queue_transport::{Transport, sqs::SqsQueue};
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

const SQS_ENDPOINT: &str = "STORAGE_QUEUE_TEST_SQS_ENDPOINT";
static NEXT_QUEUE: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize)]
struct Payload {
    tenant: Tenant,
    id: u32,
    retry_once: bool,
}

#[derive(Serialize, Deserialize)]
struct Tenant {
    #[serde(rename = "ref")]
    reference: String,
    host: String,
}

struct Echo(Arc<Mutex<Vec<(u32, u32)>>>);

impl Job for Echo {
    const NAME: &'static str = "echo";
    type Payload = Payload;

    async fn run(&self, ctx: Context, payload: Payload) -> Result<Disposition, BoxError> {
        if payload.retry_once && ctx.receive_count == 1 {
            return Ok(Disposition::RetryAfter(Duration::from_secs(1)));
        }
        self.0.lock().unwrap().push((payload.id, ctx.receive_count));
        Ok(Disposition::Done)
    }
}

async fn queue() -> SqsQueue {
    let endpoint =
        std::env::var(SQS_ENDPOINT).expect("STORAGE_QUEUE_TEST_SQS_ENDPOINT must be set");
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("x", "x", None, None, "test"))
        .build();
    let client = Client::from_conf(config);
    let name = format!(
        "jobs-worker-{}-{}",
        std::process::id(),
        NEXT_QUEUE.fetch_add(1, Ordering::Relaxed)
    );
    let url = client
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    SqsQueue::new(client, url)
}

#[tokio::test]
#[ignore = "requires an SQS-compatible endpoint"]
async fn exhausted_delivery_preserves_payload_and_attributes_in_dlq() {
    let source = Arc::new(queue().await);
    let dlq = Arc::new(queue().await);
    let payload = Payload {
        tenant: Tenant {
            reference: "tenant-1".into(),
            host: "localhost".into(),
        },
        id: 10,
        retry_once: true,
    };
    let original = encode::<Echo>(&payload).unwrap();
    source.send(std::slice::from_ref(&original)).await.unwrap();

    let mut worker = Worker::new(
        source,
        WorkerConfig {
            wait: Duration::from_secs(1),
            retry: RetryPolicy {
                max_attempts: Some(1),
                ..RetryPolicy::default()
            },
            ..WorkerConfig::default()
        },
    )
    .unwrap();
    worker.set_dead_letter(dlq.clone());
    worker.register(Echo(Arc::new(Mutex::new(Vec::new()))));
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));
    let deadline = Instant::now() + Duration::from_secs(15);
    let message = loop {
        let received = dlq
            .receive(1, Duration::from_secs(1), Duration::from_secs(2))
            .await
            .unwrap();
        if let Some(message) = received.into_iter().next() {
            break message;
        }
        assert!(
            Instant::now() < deadline,
            "message did not reach the dead-letter queue"
        );
    };
    assert_eq!(message.body, original.body);
    assert_eq!(message.attributes, original.attributes);
    assert_eq!(message.group_id, original.group_id);
    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires an SQS-compatible endpoint"]
async fn processes_jobs_and_drains_queue() {
    let queue = Arc::new(queue().await);
    let done = Arc::new(Mutex::new(Vec::new()));

    let jobs: Vec<_> = (0..4)
        .map(|id| {
            let payload = Payload {
                tenant: Tenant {
                    reference: format!("tenant-{id}"),
                    host: "localhost".into(),
                },
                id,
                retry_once: id == 3,
            };
            encode::<Echo>(&payload).unwrap()
        })
        .collect();
    queue.send(&jobs).await.unwrap();

    let mut worker = Worker::new(
        queue.clone(),
        WorkerConfig {
            wait: Duration::from_secs(1),
            visibility: Duration::from_secs(5),
            ..WorkerConfig::default()
        },
    )
    .unwrap();
    worker.register(Echo(done.clone()));
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    let deadline = Instant::now() + Duration::from_secs(20);
    while done.lock().unwrap().len() < 4 {
        assert!(Instant::now() < deadline, "jobs did not complete");
        sleep(Duration::from_millis(100)).await;
    }
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let mut done = done.lock().unwrap().clone();
    done.sort();
    assert_eq!(done, [(0, 1), (1, 1), (2, 1), (3, 2)]);
    assert!(
        queue
            .receive(10, Duration::ZERO, Duration::from_secs(1))
            .await
            .unwrap()
            .is_empty()
    );
}
