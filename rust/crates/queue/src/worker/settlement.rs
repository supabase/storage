use std::time::Duration;

use storage_queue_transport::{Outgoing, Received, Transport};
use tokio::time::{self, Instant};
use tracing::warn;

use super::Shared;
use crate::config::{MAX_RETRY_DELAY, MAX_VISIBILITY};

// Leave time for the visibility request to reach SQS before its 12-hour deadline.
const VISIBILITY_MARGIN: Duration = Duration::from_secs(5);

pub(super) async fn retry_later<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    receive_started: Instant,
    lease_deadline: Instant,
) -> Result<(), storage_queue_transport::Error> {
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

pub(super) fn exhausted<T>(shared: &Shared<T>, message: &Received) -> bool {
    shared
        .config
        .retry
        .max_attempts
        .is_some_and(|max| message.receive_count >= max)
}

pub(super) async fn dead_letter<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    lease_deadline: Instant,
) -> Result<(), storage_queue_transport::Error> {
    let Some(destination) = &shared.dead_letter else {
        return Err(storage_queue_transport::Error::InvalidArgument(
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
        .map_err(|_| storage_queue_transport::Error::StaleReceipt)??;
    if Instant::now() >= lease_deadline {
        return Err(storage_queue_transport::Error::StaleReceipt);
    }
    shared.transport.delete(&[&message.receipt]).await
}

pub(super) async fn extend<T: Transport>(
    shared: &Shared<T>,
    message: &Received,
    delay: Duration,
    receive_started: Instant,
) -> Result<(), storage_queue_transport::Error> {
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

pub(super) fn remaining_window(receive_started: Instant, now: Instant) -> Duration {
    let remaining =
        (receive_started + MAX_VISIBILITY - VISIBILITY_MARGIN).saturating_duration_since(now);
    Duration::from_secs(remaining.as_secs())
}
