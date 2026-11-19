use std::{collections::HashMap, future::Future, time::Duration};

use serde::{Serialize, de::DeserializeOwned};
use tokio_util::sync::CancellationToken;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

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
