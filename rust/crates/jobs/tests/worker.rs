use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};
use storage_jobs::{
    Backoff, BoxError, Context, Disposition, EncodeOptions, Error, Job, RetryPolicy, Worker,
    WorkerConfig, encode, encode_with_options,
};
use storage_queue::{AttributeValue, MessageAttribute, Outgoing, Received, Transport};
use tokio::{
    sync::{Notify, Semaphore},
    task::JoinHandle,
    time::{Instant, sleep},
};
use tokio_util::sync::CancellationToken;

const S1: Duration = Duration::from_secs(1);

#[derive(Serialize, Deserialize)]
enum Mode {
    Done,
    Retry(u64),
    Fail,
    Panic,
    Sleep(u64),
    SleepRetry(u64, u64),
    UntilCancel,
    DoneOnCancel,
    Ignore,
}

#[derive(Default)]
struct Stats {
    running: AtomicUsize,
    max_running: AtomicUsize,
    started: AtomicUsize,
    cancelled: AtomicUsize,
    exited: AtomicUsize,
    exit: Notify,
}

struct Running(Arc<Stats>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, SeqCst);
        self.0.exited.fetch_add(1, SeqCst);
        self.0.exit.notify_one();
    }
}

struct Probe(Arc<Stats>);

impl Job for Probe {
    const NAME: &'static str = "probe";
    type Payload = Mode;

    async fn run(&self, ctx: Context, mode: Mode) -> Result<Disposition, BoxError> {
        self.0.started.fetch_add(1, SeqCst);
        let now = self.0.running.fetch_add(1, SeqCst) + 1;
        self.0.max_running.fetch_max(now, SeqCst);
        let _running = Running(self.0.clone());
        match mode {
            Mode::Done => Ok(Disposition::Done),
            Mode::Retry(ms) => Ok(Disposition::RetryAfter(Duration::from_millis(ms))),
            Mode::Fail => Err("boom".into()),
            Mode::Panic => panic!("boom"),
            Mode::Sleep(ms) => {
                sleep(Duration::from_millis(ms)).await;
                Ok(Disposition::Done)
            }
            Mode::SleepRetry(sleep_ms, retry_ms) => {
                sleep(Duration::from_millis(sleep_ms)).await;
                Ok(Disposition::RetryAfter(Duration::from_millis(retry_ms)))
            }
            Mode::UntilCancel => {
                ctx.cancel.cancelled().await;
                self.0.cancelled.fetch_add(1, SeqCst);
                Ok(Disposition::RetryAfter(Duration::ZERO))
            }
            Mode::DoneOnCancel => {
                ctx.cancel.cancelled().await;
                self.0.cancelled.fetch_add(1, SeqCst);
                Ok(Disposition::Done)
            }
            Mode::Ignore => std::future::pending().await,
        }
    }
}

type RecordedPayloads = Arc<Mutex<Vec<(Option<String>, serde_json::Value)>>>;
struct NodeEvent(RecordedPayloads);

impl Job for NodeEvent {
    const NAME: &'static str = "test-event";
    const TYPE: Option<&'static str> = Some("TestEvent");
    type Payload = serde_json::Value;

    async fn run(&self, ctx: Context, payload: Self::Payload) -> Result<Disposition, BoxError> {
        self.0.lock().unwrap().push((ctx.tenant, payload));
        Ok(Disposition::Done)
    }
}

#[derive(Clone, Debug)]
enum Call {
    Send,
    Receive { max: usize, running: usize },
    Extend(String, Duration, Instant),
    Delete(String),
}

struct Msg {
    receipt: String,
    body: String,
    count: u32,
    visible_at: Instant,
    group_id: Option<String>,
    attributes: std::collections::HashMap<String, MessageAttribute>,
}

struct Fake {
    stats: Arc<Stats>,
    msgs: Mutex<Vec<Msg>>,
    calls: Mutex<Vec<Call>>,
    stale: Mutex<bool>,
    fail_receives: AtomicUsize,
    fail_sends: AtomicBool,
    block_extends: AtomicBool,
    fail_extends: AtomicUsize,
    extend_delay_ms: AtomicU64,
    extend_until_exit: AtomicBool,
}

impl Fake {
    fn new(stats: &Arc<Stats>) -> Arc<Self> {
        Arc::new(Self {
            stats: stats.clone(),
            msgs: Mutex::default(),
            calls: Mutex::default(),
            stale: Mutex::default(),
            fail_receives: AtomicUsize::new(0),
            fail_sends: AtomicBool::new(false),
            block_extends: AtomicBool::new(false),
            fail_extends: AtomicUsize::new(0),
            extend_delay_ms: AtomicU64::new(0),
            extend_until_exit: AtomicBool::new(false),
        })
    }

    fn push(&self, body: String, count: u32) -> String {
        let mut msgs = self.msgs.lock().unwrap();
        let receipt = format!("r{}", msgs.len());
        msgs.push(Msg {
            receipt: receipt.clone(),
            body,
            count,
            visible_at: Instant::now(),
            group_id: None,
            attributes: Default::default(),
        });
        receipt
    }

    fn push_mode(&self, mode: &Mode, count: u32) -> String {
        self.push(job_body(mode), count)
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn extends(&self) -> Vec<(String, Duration, Instant)> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                Call::Extend(r, d, at) => Some((r, d, at)),
                _ => None,
            })
            .collect()
    }

    fn deletes(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|c| match c {
                Call::Delete(r) => Some(r),
                _ => None,
            })
            .collect()
    }

    fn receives(&self) -> usize {
        self.calls()
            .iter()
            .filter(|c| matches!(c, Call::Receive { .. }))
            .count()
    }

    fn counts(&self) -> Vec<u32> {
        self.msgs.lock().unwrap().iter().map(|m| m.count).collect()
    }
}

impl Transport for Fake {
    async fn send(&self, messages: &[Outgoing]) -> Result<(), storage_queue::Error> {
        self.calls.lock().unwrap().push(Call::Send);
        if self.fail_sends.load(SeqCst) {
            return Err(storage_queue::Error::InvalidResponse(
                "injected send failure",
            ));
        }
        for m in messages {
            let receipt = self.push(m.body.clone(), 0);
            let mut msgs = self.msgs.lock().unwrap();
            let msg = msgs.iter_mut().find(|msg| msg.receipt == receipt).unwrap();
            msg.group_id = m.group_id.clone();
            msg.attributes = m.attributes.clone();
        }
        Ok(())
    }

    async fn receive(
        &self,
        max: usize,
        wait: Duration,
        visibility: Duration,
    ) -> Result<Vec<Received>, storage_queue::Error> {
        self.calls.lock().unwrap().push(Call::Receive {
            max,
            running: self.stats.running.load(SeqCst),
        });
        if self
            .fail_receives
            .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(storage_queue::Error::InvalidResponse("injected"));
        }
        let deadline = Instant::now() + wait;
        loop {
            let now = Instant::now();
            let got: Vec<Received> = self
                .msgs
                .lock()
                .unwrap()
                .iter_mut()
                .filter(|m| m.visible_at <= now)
                .take(max)
                .map(|m| {
                    m.count += 1;
                    m.visible_at = now + visibility;
                    Received {
                        receipt: m.receipt.clone(),
                        body: m.body.clone(),
                        receive_count: m.count,
                        group_id: m.group_id.clone(),
                        attributes: m.attributes.clone(),
                    }
                })
                .collect();
            if !got.is_empty() || now >= deadline {
                return Ok(got);
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn extend(
        &self,
        receipt: &str,
        visibility: Duration,
    ) -> Result<(), storage_queue::Error> {
        assert!(
            visibility.subsec_nanos() == 0 && visibility <= Duration::from_secs(12 * 3600),
            "invalid visibility {visibility:?}"
        );
        let now = Instant::now();
        self.calls
            .lock()
            .unwrap()
            .push(Call::Extend(receipt.into(), visibility, now));
        if self.block_extends.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.extend_until_exit.load(SeqCst) {
            self.stats.exit.notified().await;
        }
        let delay = self.extend_delay_ms.load(SeqCst);
        if delay > 0 {
            sleep(Duration::from_millis(delay)).await;
        }
        if self
            .fail_extends
            .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(storage_queue::Error::InvalidResponse(
                "injected heartbeat failure",
            ));
        }
        if *self.stale.lock().unwrap() {
            return Err(storage_queue::Error::StaleReceipt);
        }
        if let Some(m) = self
            .msgs
            .lock()
            .unwrap()
            .iter_mut()
            .find(|m| m.receipt == receipt)
        {
            m.visible_at = now + visibility;
        }
        Ok(())
    }

    async fn delete(&self, receipts: &[&str]) -> Result<(), storage_queue::Error> {
        for r in receipts {
            self.calls.lock().unwrap().push(Call::Delete((*r).into()));
            self.msgs.lock().unwrap().retain(|m| m.receipt != *r);
        }
        Ok(())
    }
}

fn job_body(mode: &Mode) -> String {
    serde_json::to_string(mode).unwrap()
}

fn setup(config: WorkerConfig) -> (Arc<Stats>, Arc<Fake>, Worker<Fake>) {
    let stats = Arc::new(Stats::default());
    let fake = Fake::new(&stats);
    let mut worker = Worker::new(fake.clone(), config).unwrap();
    worker.register(Probe(stats.clone()));
    (stats, fake, worker)
}

fn start(worker: Worker<Fake>) -> (CancellationToken, JoinHandle<Result<(), Error>>) {
    let token = CancellationToken::new();
    (token.clone(), tokio::spawn(worker.run(token)))
}

async fn stop(token: CancellationToken, handle: JoinHandle<Result<(), Error>>) {
    token.cancel();
    handle.await.unwrap().unwrap();
}

async fn until(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

#[test]
fn encode_node_payload() {
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/node-payload.json")).unwrap();
    let out = encode::<NodeEvent>(&expected).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&out.body).unwrap(),
        expected
    );
    assert_eq!(out.group_id.as_deref(), Some("test-tenant"));
    assert_eq!(
        out.attributes["queue.key"].value,
        AttributeValue::Text("test-tenant".into())
    );
    assert_eq!(
        out.attributes["queue.headers"].value,
        AttributeValue::Text(r#"{"message-type":"TestEvent"}"#.into())
    );
}

#[test]
fn encode_accepts_non_storage_payloads_without_grouping() {
    for payload in [
        serde_json::json!({}),
        serde_json::json!({"tenant": {"ref": "test-tenant"}}),
        serde_json::json!({"tenant": {"ref": 42, "host": "localhost"}}),
    ] {
        if payload["tenant"]["ref"] == "test-tenant" {
            assert_eq!(
                encode::<NodeEvent>(&payload).unwrap().group_id.as_deref(),
                Some("test-tenant")
            );
        } else {
            assert!(encode::<NodeEvent>(&payload).unwrap().group_id.is_none());
        }
    }
}

#[test]
fn fair_group_matches_manager_for_system_and_non_ascii_keys() {
    let system = serde_json::json!({"tenant": {"ref": "SYSTEM_TENANT", "host": ""}});
    let encoded = encode::<NodeEvent>(&system).unwrap();
    assert!(encoded.group_id.is_none());
    assert!(!encoded.attributes.contains_key("queue.key"));
    let fallback = encode_with_options::<NodeEvent>(
        &system,
        EncodeOptions {
            idempotency_key: Some("dedup-id"),
            ..EncodeOptions::default()
        },
    )
    .unwrap();
    assert_eq!(fallback.group_id.as_deref(), Some("dedup-id"));
    assert!(!fallback.attributes.contains_key("queue.key"));

    let spaced = serde_json::json!({"tenant": {"ref": "tenant with spaces", "host": "localhost"}});
    let encoded = encode::<NodeEvent>(&spaced).unwrap();
    assert_eq!(
        encoded.group_id.as_deref(),
        Some("sha256:60d9d80137733115ce9492acf315e5ff47020c01e5e094384ec65c58b7a5bbcc")
    );
    assert_eq!(
        encoded.attributes["queue.key"].value,
        AttributeValue::Text("tenant with spaces".into())
    );
    let override_ = encode_with_options::<NodeEvent>(
        &spaced,
        EncodeOptions {
            key: Some("custom-key"),
            headers: std::collections::HashMap::from([(
                "message-type".into(),
                "custom-type".into(),
            )]),
            ..EncodeOptions::default()
        },
    )
    .unwrap();
    assert_eq!(override_.group_id.as_deref(), Some("custom-key"));
    assert_eq!(
        override_.attributes["queue.headers"].value,
        AttributeValue::Text(r#"{"message-type":"custom-type"}"#.into())
    );
    let empty_key = encode_with_options::<NodeEvent>(
        &spaced,
        EncodeOptions {
            key: Some(""),
            idempotency_key: Some("dedup-id"),
            ..EncodeOptions::default()
        },
    )
    .unwrap();
    assert_eq!(empty_key.group_id.as_deref(), Some("dedup-id"));
    assert!(!empty_key.attributes.contains_key("queue.key"));
}

type RecordedMetadata =
    Arc<Mutex<Option<(Option<String>, std::collections::HashMap<String, String>)>>>;
struct MetadataProbe(RecordedMetadata);

impl Job for MetadataProbe {
    const NAME: &'static str = "metadata";
    type Payload = serde_json::Value;

    async fn run(&self, ctx: Context, _payload: Self::Payload) -> Result<Disposition, BoxError> {
        *self.0.lock().unwrap() = Some((ctx.key, ctx.headers));
        Ok(Disposition::Done)
    }
}

#[tokio::test(start_paused = true)]
async fn worker_reads_manager_key_and_headers() {
    let stats = Arc::new(Stats::default());
    let fake = Fake::new(&stats);
    let output = Arc::new(Mutex::new(None));
    let mut worker = Worker::new(fake.clone(), WorkerConfig::default()).unwrap();
    worker.register(MetadataProbe(output.clone()));
    let message = Outgoing {
        body: "{}".into(),
        group_id: Some("tenant-a".into()),
        attributes: std::collections::HashMap::from([
            (
                "queue.key".into(),
                MessageAttribute {
                    data_type: "String".into(),
                    value: AttributeValue::Text("tenant-a".into()),
                },
            ),
            (
                "queue.headers".into(),
                MessageAttribute {
                    data_type: "String".into(),
                    value: AttributeValue::Text(
                        r#"{"message-type":"Thing","extra":"value","ignored":42}"#.into(),
                    ),
                },
            ),
        ]),
    };
    fake.send(&[message]).await.unwrap();
    let (token, handle) = start(worker);
    until(|| output.lock().unwrap().is_some()).await;
    let result = output.lock().unwrap().clone().unwrap();
    assert_eq!(result.0.as_deref(), Some("tenant-a"));
    assert_eq!(
        result.1,
        std::collections::HashMap::from([
            ("message-type".into(), "Thing".into()),
            ("extra".into(), "value".into())
        ])
    );
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn worker_accepts_node_payload_and_derives_context_from_it() {
    let stats = Arc::new(Stats::default());
    let fake = Fake::new(&stats);
    let handled = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Worker::new(fake.clone(), WorkerConfig::default()).unwrap();
    worker.register(NodeEvent(handled.clone()));
    let body = include_str!("fixtures/node-payload.json");
    let expected: serde_json::Value = serde_json::from_str(body).unwrap();
    let receipt = fake.push(body.into(), 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [receipt]);
    assert_eq!(
        *handled.lock().unwrap(),
        [(Some("test-tenant".into()), expected)]
    );
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn manager_payload_resembling_an_envelope_is_not_unwrapped() {
    let stats = Arc::new(Stats::default());
    let fake = Fake::new(&stats);
    let handled = Arc::new(Mutex::new(Vec::new()));
    let mut worker = Worker::new(fake.clone(), WorkerConfig::default()).unwrap();
    worker.register(NodeEvent(handled.clone()));
    let payload = serde_json::json!({"v":2,"name":"test-event","payload":{"value":1}});
    fake.push(payload.to_string(), 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(*handled.lock().unwrap(), [(None, payload)]);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn done_deletes() {
    let (_, fake, worker) = setup(WorkerConfig::default());
    let r = fake.push_mode(&Mode::Done, 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    sleep(S1).await;
    assert_eq!(fake.deletes(), [r]);
    assert!(fake.extends().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn retry_after_rounds_up() {
    let (_, fake, worker) = setup(WorkerConfig::default());
    let r = fake.push_mode(&Mode::Retry(1200), 0);
    let (token, handle) = start(worker);
    until(|| !fake.extends().is_empty()).await;
    sleep(S1).await;
    let extends = fake.extends();
    assert_eq!(extends.len(), 1);
    assert_eq!(
        (extends[0].0.as_str(), extends[0].1),
        (r.as_str(), Duration::from_secs(2))
    );
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn error_backs_off_exponentially_with_cap() {
    let (_, fake, worker) = setup(WorkerConfig::default());
    let first = fake.push_mode(&Mode::Fail, 0);
    let third = fake.push_mode(&Mode::Fail, 2);
    let capped = fake.push_mode(&Mode::Fail, 9);
    let (token, handle) = start(worker);
    until(|| fake.extends().len() == 3).await;
    let by_receipt = |r: &str| fake.extends().into_iter().find(|e| e.0 == r).unwrap().1;
    assert_eq!(by_receipt(&first), Duration::from_secs(5));
    assert_eq!(by_receipt(&third), Duration::from_secs(20));
    assert_eq!(by_receipt(&capped), Duration::from_secs(300));
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn fixed_backoff_is_per_queue() {
    let (_, fake, worker) = setup(WorkerConfig {
        retry: RetryPolicy {
            backoff: Backoff::Fixed,
            base: Duration::from_secs(7),
            max: None,
            max_attempts: None,
        },
        ..WorkerConfig::default()
    });
    let first = fake.push_mode(&Mode::Fail, 0);
    let later = fake.push_mode(&Mode::Fail, 3);
    let (token, handle) = start(worker);
    until(|| fake.extends().len() == 2).await;
    for receipt in [first, later] {
        let delay = fake
            .extends()
            .into_iter()
            .find(|e| e.0 == receipt)
            .unwrap()
            .1;
        assert_eq!(delay, Duration::from_secs(7));
    }
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff_keeps_growing_past_u32_factor() {
    let (_, fake, worker) = setup(WorkerConfig {
        retry: RetryPolicy {
            backoff: Backoff::Exponential,
            base: Duration::from_nanos(1),
            max: None,
            max_attempts: None,
        },
        ..WorkerConfig::default()
    });
    fake.push_mode(&Mode::Fail, 39);
    let (token, handle) = start(worker);
    until(|| !fake.extends().is_empty()).await;
    assert_eq!(fake.extends()[0].1, Duration::from_secs(550));
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn exhausted_delivery_is_copied_before_ack_with_metadata() {
    let (stats, fake, mut worker) = setup(WorkerConfig {
        retry: RetryPolicy {
            max_attempts: Some(2),
            ..RetryPolicy::default()
        },
        ..WorkerConfig::default()
    });
    let dlq = Fake::new(&stats);
    worker.set_dead_letter(dlq.clone());
    let receipt = fake.push_mode(&Mode::Fail, 1);
    {
        let mut msgs = fake.msgs.lock().unwrap();
        let message = &mut msgs[0];
        message.group_id = Some("tenant-a".into());
        message.attributes.insert(
            "queue.key".into(),
            MessageAttribute {
                data_type: "String".into(),
                value: AttributeValue::Text("tenant-a".into()),
            },
        );
    }
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [receipt]);
    {
        let copied = dlq.msgs.lock().unwrap();
        assert_eq!(copied.len(), 1);
        assert_eq!(copied[0].body, job_body(&Mode::Fail));
        assert_eq!(copied[0].group_id.as_deref(), Some("tenant-a"));
        assert_eq!(
            copied[0].attributes["queue.key"].value,
            AttributeValue::Text("tenant-a".into())
        );
    }
    assert!(fake.extends().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn failed_dead_letter_send_keeps_original_for_retry() {
    let (stats, fake, mut worker) = setup(WorkerConfig {
        visibility: Duration::from_secs(2),
        retry: RetryPolicy {
            max_attempts: Some(1),
            ..RetryPolicy::default()
        },
        ..WorkerConfig::default()
    });
    let dlq = Fake::new(&stats);
    dlq.fail_sends.store(true, SeqCst);
    worker.set_dead_letter(dlq.clone());
    fake.push_mode(&Mode::Fail, 0);
    let (token, handle) = start(worker);
    until(|| dlq.calls().iter().any(|c| matches!(c, Call::Send))).await;
    assert!(fake.deletes().is_empty());
    dlq.fail_sends.store(false, SeqCst);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(
        stats.started.load(SeqCst),
        1,
        "exhausted redelivery must not run again"
    );
    assert_eq!(dlq.msgs.lock().unwrap().len(), 1);
    stop(token, handle).await;
}

#[tokio::test]
async fn retry_limit_requires_distinct_dead_letter_queue() {
    let (_, fake, mut worker) = setup(WorkerConfig {
        retry: RetryPolicy {
            max_attempts: Some(1),
            ..RetryPolicy::default()
        },
        ..WorkerConfig::default()
    });
    worker.set_dead_letter(fake.clone());
    assert!(matches!(
        worker.run(CancellationToken::new()).await,
        Err(Error::InvalidConfig(_))
    ));
    assert_eq!(fake.receives(), 0);
    let (_, fake, worker) = setup(WorkerConfig {
        retry: RetryPolicy {
            max_attempts: Some(1),
            ..RetryPolicy::default()
        },
        ..WorkerConfig::default()
    });
    assert!(matches!(
        worker.run(CancellationToken::new()).await,
        Err(Error::InvalidConfig(_))
    ));
    assert_eq!(fake.receives(), 0);
}

#[tokio::test(start_paused = true)]
async fn panic_is_treated_as_error() {
    let (_, fake, worker) = setup(WorkerConfig::default());
    let panicked = fake.push_mode(&Mode::Panic, 0);
    let (token, handle) = start(worker);
    until(|| !fake.extends().is_empty()).await;
    assert_eq!(fake.extends()[0].0, panicked);
    assert_eq!(fake.extends()[0].1, Duration::from_secs(5));

    let ok = fake.push_mode(&Mode::Done, 0);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [ok]);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn undecodable_messages_are_retried() {
    let (stats, fake, worker) = setup(WorkerConfig::default());
    fake.push("not json".into(), 0);
    fake.push(r#"{"unexpected":true}"#.into(), 0);
    let (token, handle) = start(worker);
    until(|| fake.counts().iter().all(|c| *c == 1)).await;
    until(|| fake.extends().len() == 2).await;
    assert!(fake.deletes().is_empty());
    assert_eq!(stats.started.load(SeqCst), 0);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn without_heartbeat_visibility_expiry_cancels_job() {
    let (stats, fake, worker) = setup(WorkerConfig {
        concurrency: 1,
        visibility: Duration::from_secs(2),
        ..WorkerConfig::default()
    });
    fake.push_mode(&Mode::UntilCancel, 0);
    let (token, handle) = start(worker);
    until(|| stats.cancelled.load(SeqCst) > 0).await;
    assert!(fake.extends().is_empty());
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_extends_at_half_visibility() {
    let (_, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    let r = fake.push_mode(&Mode::Sleep(7000), 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    let extends = fake.extends();
    assert_eq!(extends.len(), 3);
    for (receipt, d, _) in &extends {
        assert_eq!((receipt, *d), (&r, Duration::from_secs(4)));
    }
    for pair in extends.windows(2) {
        assert_eq!(pair[1].2 - pair[0].2, Duration::from_secs(2));
    }
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_signals_job_and_honors_completion() {
    let (stats, fake, worker) = setup(WorkerConfig {
        execution_timeout: Some(Duration::from_secs(3)),
        ..WorkerConfig::default()
    });
    let receipt = fake.push_mode(&Mode::DoneOnCancel, 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(stats.cancelled.load(SeqCst), 1);
    assert_eq!(fake.deletes(), [receipt]);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_aborts_stuck_job_and_retries_after_heartbeat() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        concurrency: 1,
        visibility: Duration::from_secs(4),
        execution_timeout: Some(Duration::from_secs(1)),
        shutdown_grace: Duration::from_secs(2),
        ..WorkerConfig::default()
    });
    fake.extend_delay_ms.store(1500, SeqCst);
    let receipt = fake.push_mode(&Mode::Ignore, 0);
    let (token, handle) = start(worker);
    until(|| fake.extends().len() == 2).await;
    let extends = fake.extends();
    assert_eq!(
        (extends[0].0.as_str(), extends[0].1),
        (receipt.as_str(), Duration::from_secs(4))
    );
    assert_eq!(
        (extends[1].0.as_str(), extends[1].1),
        (receipt.as_str(), Duration::from_secs(5))
    );
    assert_eq!(stats.exited.load(SeqCst), 1);
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_on_last_attempt_dead_letters() {
    let (stats, fake, mut worker) = setup(WorkerConfig {
        execution_timeout: Some(S1),
        shutdown_grace: S1,
        retry: RetryPolicy {
            max_attempts: Some(1),
            ..RetryPolicy::default()
        },
        ..WorkerConfig::default()
    });
    let dlq = Fake::new(&stats);
    worker.set_dead_letter(dlq.clone());
    let receipt = fake.push_mode(&Mode::Ignore, 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [receipt]);
    assert_eq!(dlq.msgs.lock().unwrap().len(), 1);
    assert_eq!(stats.exited.load(SeqCst), 1);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_does_not_settle_after_lease_loss() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        concurrency: 1,
        visibility: Duration::from_secs(2),
        execution_timeout: Some(Duration::from_millis(500)),
        shutdown_grace: Duration::from_secs(3),
        ..WorkerConfig::default()
    });
    fake.block_extends.store(true, SeqCst);
    fake.push_mode(&Mode::Ignore, 0);
    let (token, handle) = start(worker);
    until(|| stats.exited.load(SeqCst) == 1).await;
    assert_eq!(fake.extends().len(), 1);
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn stale_heartbeat_cancels_job() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    *fake.stale.lock().unwrap() = true;
    fake.push_mode(&Mode::UntilCancel, 0);
    let (token, handle) = start(worker);
    until(|| stats.cancelled.load(SeqCst) == 1).await;
    stop(token, handle).await;
    let durations: Vec<_> = fake.extends().into_iter().map(|e| e.1).collect();
    assert_eq!(durations, [Duration::from_secs(4)]);
    assert!(fake.deletes().is_empty());
}

#[tokio::test(start_paused = true)]
async fn stale_heartbeat_ready_with_completion_is_not_settled() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    *fake.stale.lock().unwrap() = true;
    fake.extend_until_exit.store(true, SeqCst);
    fake.push_mode(&Mode::Sleep(3000), 0);
    let (token, handle) = start(worker);
    until(|| stats.exited.load(SeqCst) == 1).await;
    stop(token, handle).await;
    assert_eq!(fake.extends().len(), 1);
    assert!(fake.deletes().is_empty());
}

#[tokio::test(start_paused = true)]
async fn stale_in_flight_heartbeat_prevents_retry() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    *fake.stale.lock().unwrap() = true;
    fake.extend_delay_ms.store(1000, SeqCst);
    fake.push_mode(&Mode::SleepRetry(2500, 10_000), 0);
    let (token, handle) = start(worker);
    until(|| stats.exited.load(SeqCst) == 1).await;
    sleep(S1).await;
    stop(token, handle).await;
    assert_eq!(fake.extends().len(), 1);
    assert!(fake.deletes().is_empty());
}

#[tokio::test(start_paused = true)]
async fn completed_job_is_settled_while_heartbeat_is_blocked() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(2),
        ..WorkerConfig::default()
    });
    fake.block_extends.store(true, SeqCst);
    let receipt = fake.push_mode(&Mode::Sleep(1500), 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [receipt]);
    assert_eq!(stats.exited.load(SeqCst), 1);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn failed_heartbeats_cancel_at_visibility_deadline() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    fake.fail_extends.store(usize::MAX, SeqCst);
    fake.push_mode(&Mode::UntilCancel, 0);
    let (token, handle) = start(worker);
    until(|| stats.cancelled.load(SeqCst) > 0).await;
    let extends = fake.extends();
    assert_eq!(extends.len(), 2);
    assert_eq!(extends[1].2 - extends[0].2, S1);
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn transient_heartbeat_failure_is_retried() {
    let (_, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    fake.fail_extends.store(1, SeqCst);
    let r = fake.push_mode(&Mode::Sleep(10_000), 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [r]);
    let extends = fake.extends();
    assert!(extends.len() >= 3);
    assert_eq!(extends[1].2 - extends[0].2, S1);
    for pair in extends[1..].windows(2) {
        assert_eq!(pair[1].2 - pair[0].2, Duration::from_secs(2));
    }
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn slow_heartbeats_keep_the_lease() {
    let (_, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    fake.extend_delay_ms.store(1500, SeqCst);
    let r = fake.push_mode(&Mode::Sleep(9000), 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.deletes(), [r]);
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn retry_waits_for_in_flight_heartbeat() {
    let (_, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(4),
        ..WorkerConfig::default()
    });
    fake.extend_delay_ms.store(1000, SeqCst);
    fake.push_mode(&Mode::SleepRetry(2500, 10_000), 0);
    let (token, handle) = start(worker);
    until(|| fake.extends().len() == 2).await;
    let extends = fake.extends();
    assert_eq!(extends[0].1, Duration::from_secs(4));
    assert_eq!(extends[1].1, Duration::from_secs(10));
    assert_eq!(extends[1].2 - extends[0].2, S1);
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn blocked_heartbeat_cancels_at_visibility_deadline() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(2),
        ..WorkerConfig::default()
    });
    fake.block_extends.store(true, SeqCst);
    fake.push_mode(&Mode::UntilCancel, 0);
    let (token, handle) = start(worker);
    until(|| stats.cancelled.load(SeqCst) > 0).await;
    assert_eq!(fake.extends().len(), 1);
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn uncooperative_job_is_aborted_after_lease_loss() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(2),
        shutdown_grace: Duration::from_secs(3),
        ..WorkerConfig::default()
    });
    fake.block_extends.store(true, SeqCst);
    fake.push_mode(&Mode::Ignore, 0);
    let began = Instant::now();
    let (token, handle) = start(worker);
    until(|| stats.started.load(SeqCst) == 2).await;
    let elapsed = began.elapsed();
    assert!(elapsed >= Duration::from_secs(5) && elapsed < Duration::from_secs(6));
    assert_eq!(stats.exited.load(SeqCst), 1);
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn completion_after_visibility_deadline_is_not_acknowledged() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(60)),
        concurrency: 1,
        visibility: Duration::from_secs(2),
        ..WorkerConfig::default()
    });
    fake.block_extends.store(true, SeqCst);
    fake.push_mode(&Mode::Sleep(2000), 0);
    let (token, handle) = start(worker);
    until(|| stats.exited.load(SeqCst) > 0).await;
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_respects_remaining_receive_window() {
    let (stats, fake, worker) = setup(WorkerConfig {
        auto_heartbeat: true,
        execution_timeout: Some(Duration::from_secs(12 * 3600)),
        concurrency: 1,
        visibility: Duration::from_secs(12 * 3600),
        ..WorkerConfig::default()
    });
    fake.push_mode(&Mode::Sleep(6 * 3600 * 1000 + 1000), 0);
    let (token, handle) = start(worker);
    until(|| stats.started.load(SeqCst) == 1).await;
    tokio::time::advance(Duration::from_secs(6 * 3600)).await;
    until(|| !fake.extends().is_empty()).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    until(|| !fake.deletes().is_empty()).await;
    assert_eq!(fake.extends().len(), 1);
    let duration = fake.extends()[0].1;
    assert!(duration >= Duration::from_secs(6 * 3600 - 6));
    assert!(duration <= Duration::from_secs(6 * 3600 - 5));
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn excessive_retry_is_clamped_to_receive_window() {
    let (stats, fake, worker) = setup(WorkerConfig::default());
    let receipt = fake.push_mode(&Mode::Retry(12 * 3600 * 1000), 0);
    let (token, handle) = start(worker);
    until(|| stats.exited.load(SeqCst) == 1).await;
    sleep(Duration::from_millis(100)).await;
    let extends = fake.extends();
    assert_eq!(extends.len(), 1);
    assert_eq!(extends[0].0, receipt);
    assert_eq!(extends[0].1, Duration::from_secs(11 * 3600));
    assert!(fake.deletes().is_empty());
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_jobs_and_stops_receiving() {
    let (stats, fake, worker) = setup(WorkerConfig::default());
    let r = fake.push_mode(&Mode::UntilCancel, 0);
    let (token, handle) = start(worker);
    until(|| stats.started.load(SeqCst) == 1).await;
    let receives = fake.receives();
    token.cancel();
    handle.await.unwrap().unwrap();
    assert_eq!(fake.receives(), receives);
    assert_eq!(stats.cancelled.load(SeqCst), 1);
    let extends = fake.extends();
    assert_eq!(
        (extends[0].0.as_str(), extends[0].1),
        (r.as_str(), Duration::ZERO)
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_grace_aborts_stuck_jobs() {
    let (stats, fake, worker) = setup(WorkerConfig {
        shutdown_grace: Duration::from_secs(3),
        ..WorkerConfig::default()
    });
    fake.push_mode(&Mode::Ignore, 0);
    let (token, handle) = start(worker);
    until(|| stats.started.load(SeqCst) == 1).await;
    let began = Instant::now();
    token.cancel();
    handle.await.unwrap().unwrap();
    let elapsed = began.elapsed();
    assert!(elapsed >= Duration::from_secs(3) && elapsed < Duration::from_secs(4));
    sleep(Duration::from_millis(1)).await;
    assert_eq!(stats.exited.load(SeqCst), 1);
    assert!(fake.deletes().is_empty() && fake.extends().is_empty());
}

#[tokio::test(start_paused = true)]
async fn concurrency_is_bounded() {
    let (stats, fake, worker) = setup(WorkerConfig {
        concurrency: 3,
        ..WorkerConfig::default()
    });
    for _ in 0..12 {
        fake.push_mode(&Mode::Sleep(1000), 0);
    }
    let (token, handle) = start(worker);
    until(|| fake.deletes().len() == 12).await;
    assert_eq!(stats.max_running.load(SeqCst), 3);
    let mut first = true;
    for call in fake.calls() {
        if let Call::Receive { max, running } = call {
            assert!(max + running <= 3, "max {max} with {running} running");
            if std::mem::take(&mut first) {
                assert_eq!(max, 3);
            }
        }
    }
    stop(token, handle).await;
}

#[tokio::test(start_paused = true)]
async fn receive_errors_do_not_end_run() {
    let (_, fake, worker) = setup(WorkerConfig::default());
    fake.fail_receives.store(2, SeqCst);
    fake.push_mode(&Mode::Done, 0);
    let (token, handle) = start(worker);
    until(|| !fake.deletes().is_empty()).await;
    assert!(!handle.is_finished());
    stop(token, handle).await;
}

#[test]
#[should_panic(expected = "already registered")]
fn duplicate_register_panics() {
    let (stats, _, mut worker) = setup(WorkerConfig::default());
    worker.register(Probe(stats));
}

#[test]
fn invalid_config_is_rejected() {
    let d = WorkerConfig::default;
    let secs = Duration::from_secs;
    let bad = [
        WorkerConfig {
            retry: RetryPolicy {
                max_attempts: Some(0),
                ..RetryPolicy::default()
            },
            ..d()
        },
        WorkerConfig {
            retry: RetryPolicy {
                base: secs(10),
                max: Some(secs(5)),
                ..RetryPolicy::default()
            },
            ..d()
        },
        WorkerConfig {
            concurrency: 0,
            ..d()
        },
        WorkerConfig {
            concurrency: Semaphore::MAX_PERMITS + 1,
            ..d()
        },
        WorkerConfig { batch: 0, ..d() },
        WorkerConfig { batch: 11, ..d() },
        WorkerConfig {
            wait: Duration::ZERO,
            ..d()
        },
        WorkerConfig {
            wait: secs(21),
            ..d()
        },
        WorkerConfig {
            wait: Duration::from_millis(1500),
            ..d()
        },
        WorkerConfig {
            visibility: secs(1),
            ..d()
        },
        WorkerConfig {
            visibility: Duration::from_millis(2500),
            ..d()
        },
        WorkerConfig {
            visibility: secs(12 * 3600 + 1),
            ..d()
        },
        WorkerConfig {
            execution_timeout: Some(Duration::ZERO),
            ..d()
        },
        WorkerConfig {
            execution_timeout: Some(Duration::MAX),
            ..d()
        },
        WorkerConfig {
            auto_heartbeat: true,
            ..d()
        },
    ];
    let stats = Arc::new(Stats::default());
    for config in bad {
        let result = Worker::new(Fake::new(&stats), config);
        assert!(matches!(result, Err(Error::InvalidConfig(_))));
    }
}
