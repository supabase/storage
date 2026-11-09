use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};

use aws_sdk_sqs::{
    Client,
    config::{BehaviorVersion, Credentials, Region, retry::RetryConfig},
};
use storage_queue::{BatchOutcome, Error, Outgoing, Transport, sqs::SqsQueue};

// A small HTTP endpoint exercises the SDK and transport without depending on emulator failure injection.
fn endpoint(responses: Vec<(u16, String)>) -> (SqsQueue, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0; 4096];
            loop {
                let n = stream.read(&mut buffer).unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buffer[..n]);
                if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&bytes[..pos]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    if bytes.len() >= pos + 4 + length {
                        requests.push(
                            String::from_utf8(bytes[pos + 4..pos + 4 + length].to_vec()).unwrap(),
                        );
                        break;
                    }
                }
            }
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        }
        requests
    });
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&url)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("x", "x", None, None, "test"))
        .retry_config(RetryConfig::standard().with_max_attempts(1))
        .build();
    (SqsQueue::new(Client::from_conf(config), &url), handle)
}

fn successful(start: usize, end: usize) -> String {
    (start..end)
        .map(|i| format!(r#"{{"Id":"{i}","MessageId":"m{i}","MD5OfMessageBody":"x"}}"#))
        .collect::<Vec<_>>()
        .join(",")
}

fn messages(n: usize) -> Vec<Outgoing> {
    (0..n)
        .map(|_| Outgoing {
            body: "body".into(),
            delay: Duration::ZERO,
            group_id: None,
        })
        .collect()
}

#[tokio::test]
async fn send_reports_successes_failures_and_unsent_entries_across_chunks() {
    let first = format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful(0, 10));
    let second = format!(
        r#"{{"Successful":[{}],"Failed":[{{"Id":"10","Code":"bad","Message":"rejected","SenderFault":true}}]}}"#,
        successful(11, 20)
    );
    let (queue, server) = endpoint(vec![(200, first), (200, second)]);
    let Error::Batch(error) = queue.send(&messages(21)).await.unwrap_err() else {
        panic!("expected batch error")
    };
    server.join().unwrap();
    assert_eq!(error.operation, "send");
    assert!(
        error.outcomes[..10]
            .iter()
            .all(|o| *o == BatchOutcome::Succeeded)
    );
    assert_eq!(
        error.outcomes[10],
        BatchOutcome::Failed {
            code: "bad".into(),
            message: Some("rejected".into()),
            sender_fault: true
        }
    );
    assert!(
        error.outcomes[11..20]
            .iter()
            .all(|o| *o == BatchOutcome::Succeeded)
    );
    assert_eq!(error.outcomes[20], BatchOutcome::NotAttempted);
}

#[tokio::test]
async fn request_error_keeps_current_chunk_unknown_and_later_chunks_unsent() {
    let first = format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful(0, 10));
    let (queue, server) = endpoint(vec![
        (200, first),
        (
            500,
            r#"{"__type":"InternalError","message":"failed"}"#.into(),
        ),
    ]);
    let Error::Batch(error) = queue.send(&messages(21)).await.unwrap_err() else {
        panic!("expected batch error")
    };
    server.join().unwrap();
    assert!(error.source.is_some());
    assert!(
        error.outcomes[..10]
            .iter()
            .all(|o| *o == BatchOutcome::Succeeded)
    );
    assert!(
        error.outcomes[10..20]
            .iter()
            .all(|o| *o == BatchOutcome::Unknown)
    );
    assert_eq!(error.outcomes[20], BatchOutcome::NotAttempted);
}

#[tokio::test]
async fn delete_reports_invalid_receipts_and_other_rejections() {
    let body = r#"{"Successful":[{"Id":"0"}],"Failed":[{"Id":"1","Code":"ReceiptHandleIsInvalid","SenderFault":true},{"Id":"2","Code":"Throttled","SenderFault":false}]}"#;
    let (queue, server) = endpoint(vec![(200, body.into())]);
    let Error::Batch(error) = queue.delete(&["a", "b", "c"]).await.unwrap_err() else {
        panic!("expected batch error")
    };
    server.join().unwrap();
    assert_eq!(error.operation, "delete");
    assert_eq!(
        error.outcomes,
        vec![
            BatchOutcome::Succeeded,
            BatchOutcome::Failed {
                code: "ReceiptHandleIsInvalid".into(),
                message: None,
                sender_fault: true
            },
            BatchOutcome::Failed {
                code: "Throttled".into(),
                message: None,
                sender_fault: false
            }
        ]
    );
}

#[tokio::test]
async fn send_splits_on_utf8_bytes_and_explicitly_sends_zero_delay() {
    let responses = vec![
        (
            200,
            format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful(0, 2)),
        ),
        (
            200,
            format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful(2, 3)),
        ),
    ];
    let (queue, server) = endpoint(responses);
    let input = vec![
        Outgoing {
            body: "é".repeat(262_144),
            delay: Duration::ZERO,
            group_id: None,
        },
        Outgoing {
            body: "x".repeat(524_288),
            delay: Duration::from_secs(3),
            group_id: None,
        },
        Outgoing {
            body: "z".into(),
            delay: Duration::ZERO,
            group_id: None,
        },
    ];
    queue.send(&input).await.unwrap();
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[0].contains(r#""Id":"0""#));
    assert!(requests[0].contains(r#""Id":"1""#));
    assert!(!requests[0].contains(r#""Id":"2""#));
    assert!(requests[0].contains(r#""DelaySeconds":0"#));
    assert!(requests[0].contains(r#""DelaySeconds":3"#));
    assert!(requests[1].contains(r#""Id":"2""#));
    assert!(requests[1].contains(r#""DelaySeconds":0"#));
}

#[tokio::test]
async fn oversized_message_is_rejected_before_any_send() {
    let (queue, server) = endpoint(vec![]);
    let mut input = messages(2);
    input[1].body = "x".repeat(1_048_577);
    assert!(matches!(
        queue.send(&input).await,
        Err(Error::InvalidArgument("message body exceeds 1 MiB"))
    ));
    assert!(server.join().unwrap().is_empty());
}

#[tokio::test]
async fn byte_sized_chunks_preserve_original_indices_on_partial_failure() {
    let first = format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful(0, 2));
    let second = r#"{"Successful":[],"Failed":[{"Id":"2","Code":"InvalidMessageContents","SenderFault":true}]}"#;
    let (queue, server) = endpoint(vec![(200, first), (200, second.into())]);
    let mut input = messages(4);
    input[0].body = "x".repeat(524_288);
    input[1].body = "x".repeat(524_288);
    input[2].body = "x".repeat(1_048_576);
    let Error::Batch(error) = queue.send(&input).await.unwrap_err() else {
        panic!("expected batch error")
    };
    server.join().unwrap();
    assert_eq!(
        error.outcomes,
        vec![
            BatchOutcome::Succeeded,
            BatchOutcome::Succeeded,
            BatchOutcome::Failed {
                code: "InvalidMessageContents".into(),
                message: None,
                sender_fault: true
            },
            BatchOutcome::NotAttempted,
        ]
    );
}

#[tokio::test]
async fn extend_distinguishes_unavailable_messages_from_invalid_handles() {
    let cases = [
        (
            "AWS.SimpleQueueService.MessageNotInflight",
            "not in flight",
            true,
        ),
        ("MessageNotInflight", "not in flight", true),
        (
            "InvalidParameterValue",
            "Value example for parameter ReceiptHandle is invalid. Reason: Message does not exist or is not available for visibility timeout change.",
            true,
        ),
        ("ReceiptHandleIsInvalid", "invalid receipt", false),
        ("InvalidParameterValue", "invalid visibility timeout", false),
    ];
    for (code, message, stale) in cases {
        let response = format!(r#"{{"__type":"{code}","message":"{message}"}}"#);
        let (queue, server) = endpoint(vec![(400, response)]);
        let result = queue.extend("receipt", Duration::from_secs(1)).await;
        server.join().unwrap();
        if stale {
            assert!(
                matches!(result, Err(Error::StaleReceipt)),
                "{code}: {result:?}"
            );
        } else {
            assert!(matches!(result, Err(Error::Sqs(_))), "{code}: {result:?}");
        }
    }
}

#[tokio::test]
async fn incomplete_batch_response_reports_unknown_outcome() {
    let (queue, server) = endpoint(vec![(
        200,
        format!(r#"{{"Successful":[{}],"Failed":[]}}"#, successful(0, 1)),
    )]);
    let Error::Batch(error) = queue.send(&messages(2)).await.unwrap_err() else {
        panic!("expected batch error")
    };
    server.join().unwrap();
    assert_eq!(
        error.outcomes,
        vec![BatchOutcome::Succeeded, BatchOutcome::Unknown]
    );
}

#[tokio::test]
async fn duplicate_batch_ids_leave_the_chunk_unknown() {
    let response = format!(
        r#"{{"Successful":[{},{}],"Failed":[]}}"#,
        successful(0, 1),
        successful(0, 1)
    );
    let (queue, server) = endpoint(vec![(200, response)]);
    let Error::Batch(error) = queue.send(&messages(2)).await.unwrap_err() else {
        panic!("expected batch error")
    };
    server.join().unwrap();
    assert!(error.source.is_some());
    assert_eq!(
        error.outcomes,
        vec![BatchOutcome::Unknown, BatchOutcome::Unknown]
    );
}
