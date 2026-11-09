use std::time::Duration;

use aws_sdk_sqs::{
    Client,
    error::ProvideErrorMetadata,
    types::{
        DeleteMessageBatchRequestEntry, Message, MessageSystemAttributeName,
        SendMessageBatchRequestEntry,
    },
};

use crate::{
    BatchError, BatchOutcome, Error, Outgoing, Received, Transport, check_receive, check_send,
    check_visibility,
};

const BATCH: usize = 10;
const MAX_BATCH_BYTES: usize = 1024 * 1024;

pub struct SqsQueue {
    client: Client,
    queue_url: String,
}

impl SqsQueue {
    pub fn new(client: Client, queue_url: impl Into<String>) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
        }
    }
}

fn sqs<E: std::error::Error + Send + Sync + 'static>(e: E) -> Error {
    Error::Sqs(Box::new(e))
}

fn secs(d: Duration) -> i32 {
    d.as_secs() as i32
}

fn batch_error(
    operation: &'static str,
    outcomes: Vec<BatchOutcome>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
) -> Error {
    BatchError {
        operation,
        outcomes,
        source,
    }
    .into()
}

fn record_outcome(
    outcomes: &mut [BatchOutcome],
    start: usize,
    end: usize,
    id: &str,
    outcome: BatchOutcome,
) -> Result<(), Error> {
    let index = id
        .parse::<usize>()
        .map_err(|_| Error::InvalidResponse("invalid batch entry id"))?;
    let slot = outcomes
        .get_mut(index)
        .ok_or(Error::InvalidResponse("batch entry id out of range"))?;
    if index < start || index >= end || !matches!(slot, BatchOutcome::Unknown) {
        return Err(Error::InvalidResponse(
            "unexpected or duplicate batch entry id",
        ));
    }
    *slot = outcome;
    Ok(())
}

fn received(m: &Message) -> Result<Received, Error> {
    let attrs = m.attributes();
    let receive_count = attrs
        .and_then(|a| a.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .ok_or(Error::InvalidResponse("missing receive count"))?
        .parse::<u32>()
        .ok()
        .filter(|count| *count > 0)
        .ok_or(Error::InvalidResponse("invalid receive count"))?;
    Ok(Received {
        receipt: m
            .receipt_handle()
            .ok_or(Error::InvalidResponse("missing receipt handle"))?
            .to_owned(),
        body: m
            .body()
            .ok_or(Error::InvalidResponse("missing message body"))?
            .to_owned(),
        receive_count,
        group_id: attrs
            .and_then(|a| a.get(&MessageSystemAttributeName::MessageGroupId))
            .cloned(),
    })
}

impl Transport for SqsQueue {
    async fn send(&self, messages: &[Outgoing]) -> Result<(), Error> {
        check_send(messages)?;
        if messages.iter().any(|m| m.body.len() > MAX_BATCH_BYTES) {
            return Err(Error::InvalidArgument("message body exceeds 1 MiB"));
        }
        let mut outcomes = vec![BatchOutcome::NotAttempted; messages.len()];
        let mut start = 0;
        while start < messages.len() {
            let mut end = start;
            let mut bytes = 0;
            while end < messages.len() && end - start < BATCH {
                let size = messages[end].body.len();
                if size > MAX_BATCH_BYTES - bytes {
                    break;
                }
                bytes += size;
                end += 1;
            }
            let chunk = &messages[start..end];
            let entries = chunk
                .iter()
                .enumerate()
                .map(|(i, m)| {
                    let mut e = SendMessageBatchRequestEntry::builder()
                        .id((start + i).to_string())
                        .message_body(&m.body)
                        .delay_seconds(secs(m.delay));
                    if let Some(g) = &m.group_id {
                        e = e.message_group_id(g);
                    }
                    e.build()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| batch_error("send", outcomes.clone(), Some(Box::new(e))))?;
            outcomes[start..end].fill(BatchOutcome::Unknown);
            let out = self
                .client
                .send_message_batch()
                .queue_url(&self.queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| batch_error("send", outcomes.clone(), Some(Box::new(e))))?;
            let result = (|| {
                for entry in out.successful() {
                    record_outcome(
                        &mut outcomes,
                        start,
                        end,
                        entry.id(),
                        BatchOutcome::Succeeded,
                    )?;
                }
                for entry in out.failed() {
                    record_outcome(
                        &mut outcomes,
                        start,
                        end,
                        entry.id(),
                        BatchOutcome::Failed {
                            code: entry.code().to_owned(),
                            message: entry.message().map(str::to_owned),
                            sender_fault: entry.sender_fault(),
                        },
                    )?;
                }
                Ok::<_, Error>(())
            })();
            if let Err(e) = result {
                outcomes[start..end].fill(BatchOutcome::Unknown);
                return Err(batch_error("send", outcomes, Some(Box::new(e))));
            }
            if outcomes[start..end]
                .iter()
                .any(|o| !matches!(o, BatchOutcome::Succeeded))
            {
                return Err(batch_error("send", outcomes, None));
            }
            start = end;
        }
        Ok(())
    }

    async fn receive(
        &self,
        max: usize,
        wait: Duration,
        visibility: Duration,
    ) -> Result<Vec<Received>, Error> {
        check_receive(max, wait, visibility)?;
        let out = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(max as i32)
            .wait_time_seconds(secs(wait))
            .visibility_timeout(secs(visibility))
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .message_system_attribute_names(MessageSystemAttributeName::MessageGroupId)
            .send()
            .await
            .map_err(sqs)?;
        out.messages().iter().map(received).collect()
    }

    async fn extend(&self, receipt: &str, visibility: Duration) -> Result<(), Error> {
        check_visibility(visibility)?;
        self.client
            .change_message_visibility()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt)
            .visibility_timeout(secs(visibility))
            .send()
            .await
            .map_err(|e| {
                let not_inflight = e.as_service_error().is_some_and(|e| e.is_message_not_inflight())
                    || e.code() == Some("MessageNotInflight");
                // AWS has also returned this unmodeled error for a deleted or unavailable message.
                let unavailable = e.code() == Some("InvalidParameterValue")
                    && e.message().is_some_and(|message| message.contains("for parameter ReceiptHandle is invalid")
                        && message.contains("Message does not exist or is not available for visibility timeout change"));
                if not_inflight || unavailable {
                    Error::StaleReceipt
                } else {
                    Error::VisibilityChange {
                        code: e.code().map(str::to_owned),
                        message: e.message().map(str::to_owned),
                        source: Box::new(e),
                    }
                }
            })?;
        Ok(())
    }

    async fn delete(&self, receipts: &[&str]) -> Result<(), Error> {
        let mut outcomes = vec![BatchOutcome::NotAttempted; receipts.len()];
        for (chunk_index, chunk) in receipts.chunks(BATCH).enumerate() {
            let start = chunk_index * BATCH;
            let end = start + chunk.len();
            let entries = chunk
                .iter()
                .enumerate()
                .map(|(i, r)| {
                    DeleteMessageBatchRequestEntry::builder()
                        .id((start + i).to_string())
                        .receipt_handle(*r)
                        .build()
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| batch_error("delete", outcomes.clone(), Some(Box::new(e))))?;
            outcomes[start..end].fill(BatchOutcome::Unknown);
            let out = self
                .client
                .delete_message_batch()
                .queue_url(&self.queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| batch_error("delete", outcomes.clone(), Some(Box::new(e))))?;
            let result = (|| {
                for entry in out.successful() {
                    record_outcome(
                        &mut outcomes,
                        start,
                        end,
                        entry.id(),
                        BatchOutcome::Succeeded,
                    )?;
                }
                for entry in out.failed() {
                    let outcome = BatchOutcome::Failed {
                        code: entry.code().to_owned(),
                        message: entry.message().map(str::to_owned),
                        sender_fault: entry.sender_fault(),
                    };
                    record_outcome(&mut outcomes, start, end, entry.id(), outcome)?;
                }
                Ok::<_, Error>(())
            })();
            if let Err(e) = result {
                outcomes[start..end].fill(BatchOutcome::Unknown);
                return Err(batch_error("delete", outcomes, Some(Box::new(e))));
            }
            if outcomes[start..end]
                .iter()
                .any(|o| !matches!(o, BatchOutcome::Succeeded))
            {
                return Err(batch_error("delete", outcomes, None));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn received_requires_message_fields_and_a_valid_count() {
        let make = |receipt: Option<&str>, body: Option<&str>, count: Option<&str>| {
            let mut builder = Message::builder()
                .set_receipt_handle(receipt.map(str::to_owned))
                .set_body(body.map(str::to_owned));
            if let Some(count) = count {
                builder =
                    builder.attributes(MessageSystemAttributeName::ApproximateReceiveCount, count);
            }
            builder.build()
        };
        assert!(matches!(
            received(&make(None, Some("body"), Some("1"))),
            Err(Error::InvalidResponse("missing receipt handle"))
        ));
        assert!(matches!(
            received(&make(Some("receipt"), None, Some("1"))),
            Err(Error::InvalidResponse("missing message body"))
        ));
        for count in [None, Some("invalid"), Some("0")] {
            assert!(matches!(
                received(&make(Some("receipt"), Some("body"), count)),
                Err(Error::InvalidResponse(_))
            ));
        }
        let result = received(&make(Some("receipt"), Some("body"), Some("3"))).unwrap();
        assert_eq!(result.receive_count, 3);
    }
}
