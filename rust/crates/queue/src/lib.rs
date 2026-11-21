use std::time::Duration;

pub mod sqs;

const MAX_RECEIVE: usize = 10;
const MAX_WAIT_SECS: u64 = 20;
const MAX_DELAY_SECS: u64 = 15 * 60;
const MAX_VISIBILITY_SECS: u64 = 12 * 60 * 60;

#[derive(Debug, Clone)]
pub struct Outgoing {
    pub body: String,
    pub delay: Duration,
    pub group_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Received {
    pub receipt: String,
    pub body: String,
    pub receive_count: u32,
    pub group_id: Option<String>,
}

pub trait Transport {
    /// Enqueue, chunking as needed. Batch errors report an outcome for each input.
    /// Unknown outcomes may have succeeded; retrying them can produce duplicates.
    /// The underlying client may also retry requests, so a failed entry in the final
    /// response does not rule out success in an earlier attempt.
    /// Durations must be whole seconds.
    fn send(&self, messages: &[Outgoing]) -> impl Future<Output = Result<(), Error>> + Send;
    /// Claim up to `max` messages (1..=10), long-polling up to `wait` (<= 20 s). Claimed messages are
    /// invisible for `visibility` (0 s ..= 12 h) and have receive_count incremented.
    /// Durations must be whole seconds. If any returned message is malformed, the
    /// whole receive fails; claimed messages remain invisible until their timeout.
    /// SQS limits visibility to 12 hours from receiving the original receive request.
    fn receive(
        &self,
        max: usize,
        wait: Duration,
        visibility: Duration,
    ) -> impl Future<Output = Result<Vec<Received>, Error>> + Send;
    /// Set the message's visibility timeout to `visibility` measured from now (0 = make visible now, i.e. nack).
    /// Returns Error::StaleReceipt when the backend identifies the message as no longer in flight.
    /// Invalid handles and other provider errors are returned separately; a provider may not
    /// distinguish every stale handle from an invalid one.
    /// The duration must be whole seconds and at most 12 hours. SQS also rejects a duration
    /// beyond the time remaining in the 12-hour window from the original receive request;
    /// the remaining window is determined by SQS, not this client. Such errors are returned
    /// as Error::VisibilityChange with the provider details; the timeout is not clamped.
    fn extend(
        &self,
        receipt: &str,
        visibility: Duration,
    ) -> impl Future<Output = Result<(), Error>> + Send;
    /// Delete (ack). SQS may report success for an old receipt without deleting the message;
    /// rejected receipts are reported as failures.
    /// Batch errors report an outcome for each input.
    fn delete(&self, receipts: &[&str]) -> impl Future<Output = Result<(), Error>> + Send;
}

/// The outcome of one input in a failed batch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOutcome {
    Succeeded,
    /// The final response rejected this entry. Earlier request attempts may have succeeded.
    Failed {
        code: String,
        message: Option<String>,
        sender_fault: bool,
    },
    /// The request may have been sent, but its result could not be determined.
    Unknown,
    /// No request containing this input was made.
    NotAttempted,
}

#[derive(Debug, thiserror::Error)]
#[error("{operation} batch failed")]
pub struct BatchError {
    pub operation: &'static str,
    /// Outcomes are ordered to match the original input slice.
    pub outcomes: Vec<BatchOutcome>,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("stale receipt")]
    StaleReceipt,
    #[error("invalid SQS response: {0}")]
    InvalidResponse(&'static str),
    #[error("visibility change failed (code: {code:?}, message: {message:?})")]
    VisibilityChange {
        code: Option<String>,
        message: Option<String>,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error(transparent)]
    Sqs(Box<dyn std::error::Error + Send + Sync>),
}

pub(crate) fn check_send(messages: &[Outgoing]) -> Result<(), Error> {
    if messages.iter().any(|m| m.delay.subsec_nanos() != 0) {
        return Err(Error::InvalidArgument("delay must be whole seconds"));
    }
    if messages.iter().any(|m| m.delay.as_secs() > MAX_DELAY_SECS) {
        return Err(Error::InvalidArgument("delay exceeds 15 minutes"));
    }
    Ok(())
}

pub(crate) fn check_receive(max: usize, wait: Duration, visibility: Duration) -> Result<(), Error> {
    if !(1..=MAX_RECEIVE).contains(&max) {
        return Err(Error::InvalidArgument("max must be 1..=10"));
    }
    if wait.subsec_nanos() != 0 {
        return Err(Error::InvalidArgument("wait must be whole seconds"));
    }
    if wait.as_secs() > MAX_WAIT_SECS {
        return Err(Error::InvalidArgument("wait exceeds 20 seconds"));
    }
    check_visibility(visibility)
}

pub(crate) fn check_visibility(visibility: Duration) -> Result<(), Error> {
    if visibility.subsec_nanos() != 0 {
        return Err(Error::InvalidArgument("visibility must be whole seconds"));
    }
    if visibility.as_secs() > MAX_VISIBILITY_SECS {
        return Err(Error::InvalidArgument("visibility exceeds 12 hours"));
    }
    Ok(())
}
