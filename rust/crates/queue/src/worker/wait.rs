use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use storage_queue_transport::{Received, Transport};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{self, Instant},
};
use tracing::warn;

use super::{Shared, settlement::remaining_window};

type Heartbeat<'a> = (
    Instant,
    Pin<Box<dyn Future<Output = Result<(), storage_queue_transport::Error>> + Send + 'a>>,
);

pub(super) enum Outcome {
    Acquired(OwnedSemaphorePermit, Instant),
    Release(Instant),
    Lost,
}

pub(super) async fn acquire<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    mut received_at: Instant,
    receive_started: Instant,
    slots: Arc<Semaphore>,
) -> Outcome {
    let visibility = shared.config.visibility;
    let half = visibility / 2;
    let mut deadline = received_at + visibility;
    let mut next_beat = received_at + half;
    let mut heartbeat: Option<Heartbeat<'_>> = None;
    loop {
        tokio::select! {
            biased;
            _ = time::sleep_until(deadline) => return Outcome::Lost,
            result = async { heartbeat.as_mut().expect("guarded").1.as_mut().await }, if heartbeat.is_some() => {
                let (started, _) = heartbeat.take().expect("guarded");
                match result {
                    Ok(()) => {
                        deadline = started + visibility;
                        received_at = started;
                        next_beat = started + half;
                    }
                    Err(storage_queue_transport::Error::StaleReceipt) => return Outcome::Lost,
                    Err(error) => {
                        if shared.jobs.is_cancelled() { return Outcome::Lost; }
                        warn!(?error, "heartbeat for waiting job failed");
                        next_beat = Instant::now() + Duration::from_secs(1).min(half);
                    }
                }
            }
            _ = shared.jobs.cancelled(), if heartbeat.is_none() => {
                return Outcome::Release(deadline);
            }
            permit = slots.clone().acquire_owned(), if heartbeat.is_none() => {
                return Outcome::Acquired(permit.expect("semaphore is never closed"), received_at);
            }
            _ = time::sleep_until(next_beat), if shared.config.auto_heartbeat && heartbeat.is_none() => {
                let started = Instant::now();
                if remaining_window(receive_started, started) < visibility { return Outcome::Lost; }
                heartbeat = Some((started, Box::pin(shared.transport.extend(&message.receipt, visibility))));
            }
        }
    }
}
