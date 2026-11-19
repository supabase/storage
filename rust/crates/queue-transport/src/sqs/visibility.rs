use std::time::Duration;

use aws_sdk_sqs::{error::ProvideErrorMetadata, types::ChangeMessageVisibilityBatchRequestEntry};

use super::{SqsQueue, batch_error, record_outcome, secs, sqs};
use crate::{BatchOutcome, Error, Transport};

fn stale_code(code: &str) -> bool {
    let code = code.strip_prefix("AWS.SimpleQueueService.").unwrap_or(code);
    let code = code.strip_suffix("Exception").unwrap_or(code);
    matches!(
        code,
        "ReceiptHandleIsInvalid" | "MessageNotInflight" | "InvalidParameterValue"
    )
}

impl SqsQueue {
    pub(super) async fn extend_chunk(
        &self,
        receipts: &[String],
        visibility: Duration,
    ) -> Result<Vec<bool>, Error> {
        let entries = receipts
            .iter()
            .enumerate()
            .map(|(index, receipt)| {
                ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(index.to_string())
                    .receipt_handle(receipt)
                    .visibility_timeout(secs(visibility))
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(sqs)?;
        let response = self
            .client
            .change_message_visibility_batch()
            .queue_url(&self.queue_url)
            .set_entries(Some(entries))
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(error) if error.code().is_some_and(stale_code) => {
                let mut accepted = Vec::with_capacity(receipts.len());
                for receipt in receipts {
                    match self.extend(receipt, visibility).await {
                        Ok(()) => accepted.push(true),
                        Err(Error::StaleReceipt) => accepted.push(false),
                        Err(Error::VisibilityChange { ref code, .. })
                            if code.as_deref().is_some_and(stale_code) =>
                        {
                            accepted.push(false)
                        }
                        Err(error) => return Err(error),
                    }
                }
                return Ok(accepted);
            }
            Err(error) => return Err(sqs(error)),
        };
        let mut outcomes = vec![BatchOutcome::Unknown; receipts.len()];
        for item in response.successful() {
            record_outcome(
                &mut outcomes,
                0,
                receipts.len(),
                item.id(),
                BatchOutcome::Succeeded,
            )?;
        }
        for item in response.failed() {
            record_outcome(
                &mut outcomes,
                0,
                receipts.len(),
                item.id(),
                BatchOutcome::Failed {
                    code: item.code().to_owned(),
                    message: item.message().map(str::to_owned),
                    sender_fault: item.sender_fault(),
                },
            )?;
        }
        if outcomes.iter().any(|outcome| match outcome {
            BatchOutcome::Succeeded => false,
            BatchOutcome::Failed { code, .. } => !stale_code(code),
            _ => true,
        }) {
            return Err(batch_error("extend", outcomes, None));
        }
        Ok(outcomes
            .iter()
            .map(|outcome| matches!(outcome, BatchOutcome::Succeeded))
            .collect())
    }
}
