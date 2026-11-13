use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use storage_queue_transport::{Received, Transport};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{self, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::Shared;

const RECEIVE_BATCH: usize = 10;
const FILL_TIMEOUT: Duration = Duration::from_secs(1);
const RECEIVE_RETRY_DELAY: Duration = Duration::from_secs(1);

type Buffered = (Received, Instant, Instant);
type Delivery = (Received, OwnedSemaphorePermit, Instant, Instant);
type Fill = (u64, Vec<Buffered>);

pub(super) struct Puller<T> {
    shared: Arc<Shared<T>>,
    permits: Arc<Semaphore>,
    pending: JoinSet<Fill>,
    carried: VecDeque<Buffered>,
    generation: u64,
}

impl<T: Transport + Send + Sync + 'static> Puller<T> {
    pub(super) fn new(shared: Arc<Shared<T>>, permits: Arc<Semaphore>) -> Self {
        Self {
            shared,
            permits,
            pending: JoinSet::new(),
            carried: VecDeque::new(),
            generation: 0,
        }
    }

    fn collect_ready(&mut self) {
        while let Some(result) = self.pending.try_join_next() {
            if let Ok((_, messages)) = result {
                self.carried.extend(messages);
            }
        }
    }

    async fn once(
        shared: &Shared<T>,
        max: usize,
        wait: Duration,
    ) -> Result<Vec<Buffered>, storage_queue_transport::Error> {
        let started = Instant::now();
        let messages = shared
            .transport
            .receive(max, wait, shared.config.visibility)
            .await?;
        let received = Instant::now();
        Ok(messages
            .into_iter()
            .map(|message| (message, received, started))
            .collect())
    }

    pub(super) async fn receive(&mut self, shutdown: &CancellationToken) -> Option<Vec<Delivery>> {
        self.collect_ready();
        let first = loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return None,
                result = self.pending.join_next(), if !self.pending.is_empty() && self.carried.is_empty() => {
                    if let Some(Ok((_, messages))) = result { self.carried.extend(messages); }
                }
                permit = self.permits.clone().acquire_owned() => break permit.expect("semaphore is never closed"),
            }
        };
        let mut permits = vec![first];
        while permits.len() < self.shared.config.batch {
            match self.permits.clone().try_acquire_owned() {
                Ok(permit) => permits.push(permit),
                Err(_) => break,
            }
        }
        self.collect_ready();
        let mut taken = Vec::new();
        while !permits.is_empty() {
            let Some((message, received, started)) = self.carried.pop_front() else {
                break;
            };
            taken.push((message, permits.pop().expect("checked"), received, started));
        }
        let visibility = self.shared.config.visibility;
        let safety = Duration::from_secs(1).min(visibility / 10);
        let now = Instant::now();
        taken.retain(|(_, _, received, _)| now < *received + visibility - safety);
        deduplicate(&mut taken);
        let mut renewed = Vec::new();
        if !taken.is_empty() {
            let requested = Instant::now();
            let receipts: Vec<&str> = taken
                .iter()
                .map(|(message, _, _, _)| message.receipt.as_str())
                .collect();
            let result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return None,
                result = self.shared.transport.extend_batch(&receipts, visibility) => result,
            };
            if let Ok(accepted) = result
                && accepted.len() == taken.len()
                && Instant::now() < requested + visibility
            {
                renewed.extend(taken.into_iter().zip(accepted).filter_map(
                    |((message, permit, _, started), accepted)| {
                        accepted.then_some((message, permit, requested, started))
                    },
                ));
            }
        }
        self.pull(renewed, permits, shutdown).await
    }

    async fn pull(
        &mut self,
        mut received: Vec<Delivery>,
        mut permits: Vec<OwnedSemaphorePermit>,
        shutdown: &CancellationToken,
    ) -> Option<Vec<Delivery>> {
        let config = &self.shared.config;
        while received.len() + permits.len() < config.batch {
            match self.permits.clone().try_acquire_owned() {
                Ok(permit) => permits.push(permit),
                Err(_) => break,
            }
        }
        if permits.is_empty() {
            deduplicate(&mut received);
            return Some(received);
        }
        let initial: Vec<_> = permits.drain(..permits.len().min(RECEIVE_BATCH)).collect();
        let wait = if received.is_empty() {
            config.wait
        } else {
            Duration::ZERO
        };
        let first = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                self.park_deliveries(received);
                return None;
            },
            result = Self::once(&self.shared, initial.len(), wait) => result,
        };
        let first = match first {
            Ok(first) => first,
            Err(error) => {
                warn!(?error, "receive failed");
                drop(received);
                tokio::select! {
                    _ = shutdown.cancelled() => return None,
                    _ = time::sleep(RECEIVE_RETRY_DELAY) => return Some(Vec::new()),
                }
            }
        };
        let full = first.len() == RECEIVE_BATCH;
        received.extend(
            first
                .into_iter()
                .zip(initial)
                .map(|((message, received, started), permit)| (message, permit, received, started)),
        );
        if !full || permits.is_empty() {
            deduplicate(&mut received);
            return Some(received);
        }

        self.generation = self.generation.wrapping_add(1);
        let generation = self.generation;
        let mut remaining = permits.len().div_ceil(RECEIVE_BATCH);
        let mut to_request = permits.len();
        while to_request > 0 {
            let max = to_request.min(RECEIVE_BATCH);
            to_request -= max;
            let shared = self.shared.clone();
            self.pending.spawn(async move {
                let messages = Self::once(&shared, max, Duration::ZERO)
                    .await
                    .unwrap_or_default();
                (generation, messages)
            });
        }
        let deadline = Instant::now() + FILL_TIMEOUT;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    self.park_deliveries(received);
                    return None;
                },
                _ = time::sleep_until(deadline) => break,
                result = self.pending.join_next(), if !self.pending.is_empty() => {
                    match result {
                        Some(Ok((id, messages))) if id == generation => {
                            remaining -= 1;
                            let count = messages.len().min(permits.len());
                            received.extend(messages.into_iter().zip(permits.drain(..count)).map(|((message, received, started), permit)| (message, permit, received, started)));
                        }
                        Some(Ok((_, messages))) => self.carried.extend(messages),
                        _ => {}
                    }
                    if remaining == 0 { break; }
                }
                else => break,
            }
        }
        deduplicate(&mut received);
        Some(received)
    }

    fn park_deliveries(&mut self, deliveries: Vec<Delivery>) {
        self.carried.extend(
            deliveries
                .into_iter()
                .map(|(message, _, received, started)| (message, received, started)),
        );
    }

    pub(super) async fn close(&mut self) {
        self.collect_ready();
        self.pending.abort_all();
        let visibility = self.shared.config.visibility;
        let safety = Duration::from_secs(1).min(visibility / 10);
        let receipts: Vec<&str> = self
            .carried
            .iter()
            .filter(|(_, received, _)| Instant::now() < *received + visibility - safety)
            .map(|(message, _, _)| message.receipt.as_str())
            .collect();
        if !receipts.is_empty() {
            let _ = self
                .shared
                .transport
                .extend_batch(&receipts, Duration::ZERO)
                .await;
        }
        self.carried.clear();
    }
}

fn deduplicate(messages: &mut Vec<Delivery>) {
    let mut latest = HashMap::new();
    for (index, (message, _, _, _)) in messages.iter().enumerate() {
        let current = latest
            .entry(message.id.clone())
            .or_insert((message.receive_count, index));
        if message.receive_count > current.0 {
            *current = (message.receive_count, index);
        }
    }
    let mut index = 0;
    messages.retain(|(message, _, _, _)| {
        let keep = latest
            .get(&message.id)
            .is_some_and(|(_, retained)| *retained == index);
        index += 1;
        keep
    });
}
