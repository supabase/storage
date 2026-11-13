mod process;
mod pull;
mod settlement;
mod wait;

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use serde_json::value::RawValue;
use storage_queue_transport::Transport;
use tokio::{
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{BoxError, Context, Disposition, Error, Job, WorkerConfig};
use process::handle;
use pull::Puller;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

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
        let permits = Arc::new(Semaphore::new(shared.config.batch));
        let slots = Arc::new(Semaphore::new(shared.config.concurrency));
        let mut tasks = JoinSet::new();
        let mut puller = Puller::new(shared.clone(), permits);

        while let Some(messages) = puller.receive(&shutdown).await {
            for (message, permit, received_at, receive_started) in messages {
                let shared = shared.clone();
                let slots = slots.clone();
                tasks.spawn(async move {
                    let release =
                        match wait::acquire(&shared, &message, received_at, receive_started, slots)
                            .await
                        {
                            wait::Outcome::Acquired(_slot, received_at) => {
                                handle(&shared, message, received_at, receive_started).await;
                                None
                            }
                            wait::Outcome::Release(deadline) => Some((message.receipt, deadline)),
                            wait::Outcome::Lost => None,
                        };
                    drop(permit);
                    release
                });
            }
            while tasks.try_join_next().is_some() {}
        }

        shared.jobs.cancel();
        let drain = async {
            puller.close().await;
            while let Some(result) = tasks.join_next().await {
                let mut releases = Vec::new();
                if let Ok(Some(release)) = result {
                    releases.push(release);
                }
                while let Some(result) = tasks.try_join_next() {
                    if let Ok(Some(release)) = result {
                        releases.push(release);
                    }
                }
                let now = Instant::now();
                let safety = Duration::from_secs(1).min(shared.config.visibility / 10);
                let receipts: Vec<&str> = releases
                    .iter()
                    .filter(|(_, deadline)| now < *deadline - safety)
                    .map(|(receipt, _)| receipt.as_str())
                    .collect();
                if !receipts.is_empty() {
                    let _ = shared
                        .transport
                        .extend_batch(&receipts, Duration::ZERO)
                        .await;
                }
            }
        };
        if time::timeout(grace, drain).await.is_err() {
            warn!("shutdown grace elapsed, aborting in-flight jobs");
        }
        tasks.shutdown().await;
        Ok(())
    }
}
