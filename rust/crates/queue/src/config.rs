use std::time::Duration;

use tokio::{sync::Semaphore, time::Instant};

use crate::Error;

const MAX_WAIT: Duration = Duration::from_secs(20);
pub(crate) const MAX_VISIBILITY: Duration = Duration::from_secs(12 * 60 * 60);
// Matches the Node SQS adapter's cap on parked retries.
pub(crate) const MAX_RETRY_DELAY: Duration = Duration::from_secs(11 * 60 * 60);

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub concurrency: usize,
    /// Maximum deliveries handed to the worker but not yet settled. Late fill results may be carried separately.
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
    pub(crate) fn delay(&self, receive_count: u32) -> Duration {
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
            batch: 16,
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
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !(1..=Semaphore::MAX_PERMITS).contains(&self.concurrency) {
            return Err(Error::InvalidConfig(
                "concurrency must be 1..=Semaphore::MAX_PERMITS",
            ));
        }
        if !(1..=Semaphore::MAX_PERMITS).contains(&self.batch) {
            return Err(Error::InvalidConfig(
                "batch must be 1..=Semaphore::MAX_PERMITS",
            ));
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
