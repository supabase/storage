use std::{future::Future, pin::Pin, time::Duration};

use serde_json::value::RawValue;
use storage_queue_transport::{Received, Transport};
use tokio::time::{self, Instant};
use tracing::{Instrument, error, info_span, warn};

use super::{
    AbortOnDrop, Shared,
    settlement::{dead_letter, exhausted, extend, remaining_window, retry_later},
};
use crate::{
    Context, Disposition,
    message::{KEY_ATTRIBUTE, message_headers, payload_tenant, text_attribute},
};

type Heartbeat<'a> = (
    Instant,
    Duration,
    Pin<Box<dyn Future<Output = Result<(), storage_queue_transport::Error>> + Send + 'a>>,
);
const HEARTBEAT_RETRY: Duration = Duration::from_secs(1);

pub(super) async fn handle<T: Transport + Send + Sync + 'static>(
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
                    Err(storage_queue_transport::Error::StaleReceipt) => {
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
                Ok(Err(storage_queue_transport::Error::StaleReceipt)) => return,
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
