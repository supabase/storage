use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use storage_queue::{AttributeValue, MessageAttribute, Outgoing, Received, Transport};
use tokio::{
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, error, info_span, warn};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
type Heartbeat<'a> = (
    Instant,
    Duration,
    Pin<Box<dyn Future<Output = Result<(), storage_queue::Error>> + Send + 'a>>,
);

const SYSTEM_TENANT_REF: &str = "SYSTEM_TENANT";
const HEADERS_ATTRIBUTE: &str = "queue.headers";
const KEY_ATTRIBUTE: &str = "queue.key";
const MAX_BATCH: usize = 10;
const MAX_WAIT: Duration = Duration::from_secs(20);
const MAX_VISIBILITY: Duration = Duration::from_secs(12 * 60 * 60);
// Matches the Node SQS adapter's cap on parked retries.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(11 * 60 * 60);
// Leave time for the visibility request to reach SQS before its 12-hour deadline.
const VISIBILITY_MARGIN: Duration = Duration::from_secs(5);
const RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(1);
const HEARTBEAT_RETRY: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid config: {0}")]
    InvalidConfig(&'static str),
    #[error("encode failed: {0}")]
    Encode(#[from] serde_json::Error),
}

pub trait Job: Send + Sync + 'static {
    /// Queue name used by the Node manager's registry.
    const NAME: &'static str;
    /// Optional manager message type, carried in `queue.headers`.
    const TYPE: Option<&'static str> = None;
    type Payload: Serialize + DeserializeOwned + Send + 'static;
    fn run(
        &self,
        ctx: Context,
        payload: Self::Payload,
    ) -> impl Future<Output = Result<Disposition, BoxError>> + Send;
}

pub struct Context {
    pub tenant: Option<String>,
    pub receive_count: u32,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
    /// Fires on timeout, shutdown, or when the message lease is lost.
    pub cancel: CancellationToken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    Done,
    /// Retry using the current receipt's visibility timeout. SQS cannot guarantee
    /// delays beyond the remaining 12-hour receive window.
    RetryAfter(Duration),
}

fn payload_tenant(payload: &RawValue) -> Option<String> {
    let value = serde_json::from_str::<serde_json::Value>(payload.get()).ok()?;
    value.get("tenant")?.get("ref")?.as_str().map(str::to_owned)
}

fn string_attribute(value: String) -> MessageAttribute {
    MessageAttribute {
        data_type: "String".into(),
        value: AttributeValue::Text(value),
    }
}

fn group_id(key: &str) -> String {
    if !key.is_empty() && key.len() <= 128 && key.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        key.to_owned()
    } else {
        use std::fmt::Write;
        let mut result = String::from("sha256:");
        for byte in Sha256::digest(key.as_bytes()) {
            write!(&mut result, "{byte:02x}").expect("writing to a string cannot fail");
        }
        result
    }
}

#[derive(Debug, Default)]
pub struct EncodeOptions<'a> {
    /// Override the key derived from the payload's tenant reference.
    pub key: Option<&'a str>,
    /// Used as a fair-queue group when there is no key. Standard SQS does not deduplicate it.
    pub idempotency_key: Option<&'a str>,
    pub headers: HashMap<String, String>,
}

/// Encode the bare JSON body and manager attributes for this job's queue.
pub fn encode<J: Job>(payload: &J::Payload) -> Result<Outgoing, Error> {
    encode_with_options::<J>(payload, EncodeOptions::default())
}

pub fn encode_with_options<J: Job>(
    payload: &J::Payload,
    options: EncodeOptions<'_>,
) -> Result<Outgoing, Error> {
    let payload = serde_json::value::to_raw_value(payload)?;
    let tenant = payload_tenant(&payload).filter(|t| t != SYSTEM_TENANT_REF);
    let key = options
        .key
        .or(tenant.as_deref())
        .filter(|key| !key.is_empty());
    let fair_group = key.or(options.idempotency_key.filter(|key| !key.is_empty()));
    let mut attributes = HashMap::new();
    if let Some(key) = key {
        attributes.insert(KEY_ATTRIBUTE.into(), string_attribute(key.to_owned()));
    }
    let mut headers = options.headers;
    if let Some(kind) = J::TYPE {
        headers
            .entry("message-type".into())
            .or_insert_with(|| kind.to_owned());
    }
    if !headers.is_empty() {
        attributes.insert(
            HEADERS_ATTRIBUTE.into(),
            string_attribute(serde_json::to_string(&headers)?),
        );
    }
    Ok(Outgoing {
        body: payload.get().to_owned(),
        group_id: fair_group.map(group_id),
        attributes,
    })
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub concurrency: usize,
    pub batch: usize,
    pub wait: Duration,
    pub visibility: Duration,
    /// Renew visibility while a job runs. Requires an execution timeout.
    pub auto_heartbeat: bool,
    /// Optional maximum handler runtime before cancellation.
    pub execution_timeout: Option<Duration>,
    /// Failure retry policy for this queue. A limit requires a dead-letter transport.
    pub retry: RetryPolicy,
    /// How long a cancelled job may run before it is aborted.
    pub shutdown_grace: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    Fixed,
    Exponential,
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum deliveries, including crash redeliveries. `None` means no local limit.
    pub max_attempts: Option<u32>,
    pub backoff: Backoff,
    pub base: Duration,
    pub max: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: None,
            backoff: Backoff::Exponential,
            base: Duration::from_secs(5),
            max: Some(Duration::from_secs(300)),
        }
    }
}

impl RetryPolicy {
    fn delay(&self, receive_count: u32) -> Duration {
        let delay = match self.backoff {
            Backoff::Fixed => self.base,
            Backoff::Exponential => {
                let mut delay = self.base;
                if !delay.is_zero() {
                    for _ in 0..receive_count.saturating_sub(1) {
                        delay = delay.saturating_mul(2);
                        // The SQS settle path cannot park longer than this.
                        if delay >= MAX_RETRY_DELAY {
                            break;
                        }
                    }
                }
                delay
            }
        };
        self.max.map_or(delay, |max| delay.min(max))
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 16,
            batch: MAX_BATCH,
            wait: MAX_WAIT,
            visibility: Duration::from_secs(60),
            auto_heartbeat: false,
            execution_timeout: None,
            retry: RetryPolicy::default(),
            shutdown_grace: Duration::from_secs(30),
        }
    }
}

impl WorkerConfig {
    fn validate(&self) -> Result<(), Error> {
        if !(1..=Semaphore::MAX_PERMITS).contains(&self.concurrency) {
            return Err(Error::InvalidConfig(
                "concurrency must be 1..=Semaphore::MAX_PERMITS",
            ));
        }
        if !(1..=MAX_BATCH).contains(&self.batch) {
            return Err(Error::InvalidConfig("batch must be 1..=10"));
        }
        if self.wait.subsec_nanos() != 0 || self.wait.is_zero() || self.wait > MAX_WAIT {
            return Err(Error::InvalidConfig(
                "wait must be whole seconds in 1..=20 s",
            ));
        }
        if self.visibility.subsec_nanos() != 0
            || self.visibility < Duration::from_secs(2)
            || self.visibility > MAX_VISIBILITY
        {
            return Err(Error::InvalidConfig(
                "visibility must be whole seconds in 2 s..=12 h",
            ));
        }
        if self.execution_timeout.is_some_and(|timeout| {
            timeout.is_zero() || Instant::now().checked_add(timeout).is_none()
        }) {
            return Err(Error::InvalidConfig(
                "execution_timeout must be greater than zero and representable",
            ));
        }
        if self.auto_heartbeat && self.execution_timeout.is_none() {
            return Err(Error::InvalidConfig(
                "auto_heartbeat requires execution_timeout",
            ));
        }
        if Instant::now().checked_add(self.shutdown_grace).is_none() {
            return Err(Error::InvalidConfig("shutdown_grace must be representable"));
        }
        if self.retry.max_attempts == Some(0) {
            return Err(Error::InvalidConfig("max_attempts must be at least 1"));
        }
        if self.retry.max.is_some_and(|max| max < self.retry.base) {
            return Err(Error::InvalidConfig(
                "maximum backoff must be at least the base",
            ));
        }
        Ok(())
    }
}

trait Handler: Send + Sync {
    fn start(
        &self,
        ctx: Context,
        payload: &RawValue,
    ) -> Result<BoxFuture<Result<Disposition, BoxError>>, serde_json::Error>;
}

struct Typed<J>(Arc<J>);

impl<J: Job> Handler for Typed<J> {
    fn start(
        &self,
        ctx: Context,
        payload: &RawValue,
    ) -> Result<BoxFuture<Result<Disposition, BoxError>>, serde_json::Error> {
        let payload = serde_json::from_str(payload.get())?;
        let job = self.0.clone();
        Ok(Box::pin(async move { job.run(ctx, payload).await }))
    }
}

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Shared<T> {
    transport: Arc<T>,
    config: WorkerConfig,
    handler: Box<dyn Handler>,
    queue: &'static str,
    dead_letter: Option<Arc<T>>,
    jobs: CancellationToken,
}

pub struct Worker<T> {
    transport: Arc<T>,
    config: WorkerConfig,
    handler: Option<(&'static str, Box<dyn Handler>)>,
    dead_letter: Option<Arc<T>>,
}

impl<T: Transport + Send + Sync + 'static> Worker<T> {
    pub fn new(transport: Arc<T>, config: WorkerConfig) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            transport,
            config,
            handler: None,
            dead_letter: None,
        })
    }

    /// Bind the single job handled by this queue. Panics if already bound.
    pub fn register<J: Job>(&mut self, job: J) {
        assert!(self.handler.is_none(), "a job is already registered");
        self.handler = Some((J::NAME, Box::new(Typed(Arc::new(job)))));
    }

    /// Supply a distinct queue for exhausted messages. Delivery to it is at least once.
    pub fn set_dead_letter(&mut self, transport: Arc<T>) {
        self.dead_letter = Some(transport);
    }

    pub async fn run(self, shutdown: CancellationToken) -> Result<(), Error> {
        let (queue, handler) = self
            .handler
            .ok_or(Error::InvalidConfig("no job registered"))?;
        if self.config.retry.max_attempts.is_some() && self.dead_letter.is_none() {
            return Err(Error::InvalidConfig(
                "max_attempts requires a dead-letter queue",
            ));
        }
        if self
            .dead_letter
            .as_ref()
            .is_some_and(|dlq| Arc::ptr_eq(dlq, &self.transport))
        {
            return Err(Error::InvalidConfig(
                "dead-letter queue must be distinct from the source",
            ));
        }
        let grace = self.config.shutdown_grace;
        let shared = Arc::new(Shared {
            transport: self.transport,
            config: self.config,
            handler,
            queue,
            dead_letter: self.dead_letter,
            jobs: CancellationToken::new(),
        });
        let permits = Arc::new(Semaphore::new(shared.config.concurrency));
        let mut tasks = JoinSet::new();

        while let Some(messages) = receive(&shared, &permits, &shutdown).await {
            for (message, permit, received_at, receive_started) in messages {
                let shared = shared.clone();
                tasks.spawn(async move {
                    handle(&shared, message, received_at, receive_started).await;
                    drop(permit);
                });
            }
            while tasks.try_join_next().is_some() {}
        }

        shared.jobs.cancel();
        let drain = async { while tasks.join_next().await.is_some() {} };
        if time::timeout(grace, drain).await.is_err() {
            warn!("shutdown grace elapsed, aborting in-flight jobs");
        }
        tasks.shutdown().await;
        Ok(())
    }
}

async fn receive<T: Transport>(
    shared: &Shared<T>,
    permits: &Arc<Semaphore>,
    shutdown: &CancellationToken,
) -> Option<
    Vec<(
        Received,
        tokio::sync::OwnedSemaphorePermit,
        Instant,
        Instant,
    )>,
> {
    let config = &shared.config;
    let first = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return None,
        permit = permits.clone().acquire_owned() => permit.expect("semaphore is never closed"),
    };
    let mut held = vec![first];
    while held.len() < config.batch {
        match permits.clone().try_acquire_owned() {
            Ok(permit) => held.push(permit),
            Err(_) => break,
        }
    }
    let receive_started = Instant::now();
    let result = tokio::select! {
        biased;
        _ = shutdown.cancelled() => return None,
        result = shared.transport.receive(held.len(), config.wait, config.visibility) => result,
    };
    match result {
        Ok(messages) => {
            let received_at = Instant::now();
            Some(
                messages
                    .into_iter()
                    .zip(held)
                    .map(|(message, permit)| (message, permit, received_at, receive_started))
                    .collect(),
            )
        }
        Err(e) => {
            warn!(error = ?e, "receive failed");
            tokio::select! {
                _ = shutdown.cancelled() => None,
                _ = time::sleep(RECEIVE_RETRY_DELAY) => Some(Vec::new()),
            }
        }
    }
}

async fn handle<T: Transport + Send + Sync + 'static>(
    shared: &Shared<T>,
    message: Received,
    received_at: Instant,
    receive_started: Instant,
) {
    let deadline = received_at + shared.config.visibility;
    if shared
        .config
        .retry
        .max_attempts
        .is_some_and(|max| message.receive_count > max)
    {
        if let Err(e) = dead_letter(shared, &message, deadline).await {
            warn!(error = ?e, "dead-lettering failed; original message remains in the queue");
        }
        return;
    }
    let payload = match serde_json::from_str::<Box<RawValue>>(&message.body) {
        Ok(payload) => payload,
        Err(e) => {
            warn!(error = %e, receive_count = message.receive_count, "invalid JSON payload");
            if let Err(e) = retry_later(shared, &message, receive_started, deadline).await {
                warn!(error = ?e, "disposition failed");
            }
            return;
        }
    };
    let tenant = payload_tenant(&payload);
    let span = info_span!(
        "job",
        name = %shared.queue,
        tenant = ?tenant,
        receive_count = message.receive_count
    );
    process(
        shared,
        message,
        payload,
        tenant,
        received_at,
        receive_started,
    )
    .instrument(span)
    .await;
}

fn text_attribute<'a>(message: &'a Received, name: &str) -> Option<&'a str> {
    match &message.attributes.get(name)?.value {
        AttributeValue::Text(text) => Some(text),
        AttributeValue::Binary(_) => None,
    }
}

fn message_headers(message: &Received) -> HashMap<String, String> {
    if let Some(packed) = text_attribute(message, HEADERS_ATTRIBUTE) {
        return match serde_json::from_str::<serde_json::Value>(packed) {
            Ok(serde_json::Value::Object(headers)) => headers
                .into_iter()
                .filter_map(|(name, value)| value.as_str().map(|text| (name, text.to_owned())))
                .collect(),
            _ => HashMap::new(),
        };
    }
    message
        .attributes
        .iter()
        .filter_map(|(name, attribute)| {
            if name.starts_with("queue.") || attribute.data_type.starts_with("Binary") {
                return None;
            }
            match &attribute.value {
                AttributeValue::Text(value) => Some((name.clone(), value.clone())),
                AttributeValue::Binary(_) => None,
            }
        })
        .collect()
}

async fn process<T: Transport + Send + Sync + 'static>(
    shared: &Shared<T>,
    message: Received,
    payload: Box<RawValue>,
    tenant: Option<String>,
    received_at: Instant,
    receive_started: Instant,
) {
    let cancel = shared.jobs.child_token();
    let ctx = Context {
        tenant,
        receive_count: message.receive_count,
        key: text_attribute(&message, KEY_ATTRIBUTE).map(str::to_owned),
        headers: message_headers(&message),
        cancel: cancel.clone(),
    };
    let future = match shared.handler.start(ctx, &payload) {
        Ok(future) => future,
        Err(e) => {
            warn!(error = %e, "invalid payload");
            if let Err(e) = retry_later(
                shared,
                &message,
                receive_started,
                received_at + shared.config.visibility,
            )
            .await
            {
                warn!(error = ?e, "disposition failed");
            }
            return;
        }
    };
    let execution_deadline = shared
        .config
        .execution_timeout
        .map(|timeout| Instant::now() + timeout);
    let mut job = AbortOnDrop(tokio::spawn(future.in_current_span()));

    let visibility = shared.config.visibility;
    let half = visibility / 2;
    let mut next_beat = received_at + half;
    let mut heartbeat: Option<Heartbeat<'_>> = None;
    let mut lease_deadline = received_at + visibility;
    let mut cancel_deadline = None;
    let mut timed_out = false;
    // Lease-loss branches precede the job so that a ready loss is never ignored.
    let joined = loop {
        tokio::select! {
            biased;
            _ = time::sleep_until(lease_deadline) => {
                warn!("visibility expired, cancelling job");
                break None;
            }
            result = async { heartbeat.as_mut().expect("guarded").2.as_mut().await }, if heartbeat.is_some() => {
                let (started, duration, _) = heartbeat.take().expect("guarded");
                match result {
                    Ok(()) => {
                        lease_deadline = started + duration;
                        next_beat = started + half;
                    }
                    Err(storage_queue::Error::StaleReceipt) => {
                        warn!("lease lost, cancelling job");
                        break None;
                    }
                    Err(e) => {
                        warn!(error = ?e, "heartbeat failed");
                        next_beat = Instant::now() + HEARTBEAT_RETRY.min(half);
                    }
                }
            }
            joined = &mut job.0 => break Some(joined),
            _ = async { time::sleep_until(execution_deadline.expect("guarded")).await }, if execution_deadline.is_some() && cancel_deadline.is_none() => {
                warn!("job execution timed out, cancelling job");
                cancel.cancel();
                cancel_deadline = Some(Instant::now() + shared.config.shutdown_grace);
            }
            _ = async { time::sleep_until(cancel_deadline.expect("guarded")).await }, if cancel_deadline.is_some() => {
                warn!("job ignored cancellation, aborting");
                job.0.abort();
                let joined = (&mut job.0).await;
                timed_out = matches!(&joined, Err(e) if e.is_cancelled());
                break Some(joined);
            }
            _ = time::sleep_until(next_beat), if shared.config.auto_heartbeat && heartbeat.is_none() => {
                let started = Instant::now();
                let duration = visibility.min(remaining_window(receive_started, started));
                if duration.is_zero() {
                    warn!("visibility window exhausted, cancelling job");
                    break None;
                }
                heartbeat = Some((
                    started,
                    duration,
                    Box::pin(shared.transport.extend(&message.receipt, duration)),
                ));
            }
        }
    };
    let Some(joined) = joined else {
        // A lost lease must not settle a delivery that may now be owned elsewhere.
        cancel.cancel();
        if time::timeout(shared.config.shutdown_grace, &mut job.0)
            .await
            .is_err()
        {
            warn!("job ignored cancellation, aborting");
        }
        return;
    };

    if !matches!(joined, Ok(Ok(Disposition::Done))) {
        // A late heartbeat would override the retry delay.
        if let Some((started, duration, extend)) = heartbeat {
            match time::timeout_at(lease_deadline, extend).await {
                Ok(Ok(())) => lease_deadline = started + duration,
                Ok(Err(storage_queue::Error::StaleReceipt)) => return,
                Ok(Err(_)) | Err(_) => {}
            }
        }
    }
    if Instant::now() >= lease_deadline {
        return;
    }
    let result = if timed_out {
        if shared.jobs.is_cancelled() {
            extend(shared, &message, Duration::ZERO, receive_started).await
        } else {
            retry_later(shared, &message, receive_started, lease_deadline).await
        }
    } else {
        match joined {
            Ok(Ok(Disposition::Done)) => shared.transport.delete(&[&message.receipt]).await,
            Ok(Ok(Disposition::RetryAfter(delay))) => {
                if shared.jobs.is_cancelled() {
                    extend(shared, &message, Duration::ZERO, receive_started).await
                } else if exhausted(shared, &message) {
                    dead_letter(shared, &message, lease_deadline).await
                } else {
                    extend(shared, &message, delay, receive_started).await
                }
            }
            Ok(Err(e)) => {
                error!(error = %e, "job failed");
                if shared.jobs.is_cancelled() {
                    extend(shared, &message, Duration::ZERO, receive_started).await
                } else {
                    retry_later(shared, &message, receive_started, lease_deadline).await
                }
            }
            Err(e) => {
                error!(error = %e, "job panicked");
                retry_later(shared, &message, receive_started, lease_deadline).await
            }
        }
    };
    if let Err(e) = result {
        warn!(error = ?e, "disposition failed, message will reappear after visibility timeout");
    }
}

async fn retry_later<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    receive_started: Instant,
    lease_deadline: Instant,
) -> Result<(), storage_queue::Error> {
    if exhausted(shared, message) {
        dead_letter(shared, message, lease_deadline).await
    } else {
        extend(
            shared,
            message,
            shared.config.retry.delay(message.receive_count),
            receive_started,
        )
        .await
    }
}

fn exhausted<T>(shared: &Shared<T>, message: &Received) -> bool {
    shared
        .config
        .retry
        .max_attempts
        .is_some_and(|max| message.receive_count >= max)
}

async fn dead_letter<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    lease_deadline: Instant,
) -> Result<(), storage_queue::Error> {
    let Some(destination) = &shared.dead_letter else {
        return Err(storage_queue::Error::InvalidArgument(
            "dead-letter queue is not configured",
        ));
    };
    let copy = Outgoing {
        body: message.body.clone(),
        group_id: message.group_id.clone(),
        attributes: message.attributes.clone(),
    };
    time::timeout_at(lease_deadline, destination.send(&[copy]))
        .await
        .map_err(|_| storage_queue::Error::StaleReceipt)??;
    if Instant::now() >= lease_deadline {
        return Err(storage_queue::Error::StaleReceipt);
    }
    shared.transport.delete(&[&message.receipt]).await
}

async fn extend<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    delay: Duration,
    receive_started: Instant,
) -> Result<(), storage_queue::Error> {
    let secs = delay
        .as_secs()
        .saturating_add(u64::from(delay.subsec_nanos() > 0));
    let remaining = remaining_window(receive_started, Instant::now()).min(MAX_RETRY_DELAY);
    let delay = if secs > remaining.as_secs() {
        warn!(
            requested_seconds = secs,
            remaining_seconds = remaining.as_secs(),
            "retry delay shortened to the supported visibility window"
        );
        remaining
    } else {
        Duration::from_secs(secs)
    };
    shared.transport.extend(&message.receipt, delay).await
}

fn remaining_window(receive_started: Instant, now: Instant) -> Duration {
    let remaining =
        (receive_started + MAX_VISIBILITY - VISIBILITY_MARGIN).saturating_duration_since(now);
    Duration::from_secs(remaining.as_secs())
}
