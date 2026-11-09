use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use aws_sdk_sqs::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
    types::QueueAttributeName,
};
use storage_queue::{BatchOutcome, Error, Outgoing, Received, Transport, sqs::SqsQueue};
use tokio::time::sleep;

const SQS_ENDPOINT: &str = "STORAGE_QUEUE_TEST_SQS_ENDPOINT";

static COUNTER: AtomicUsize = AtomicUsize::new(0);

fn unique(test: &str) -> String {
    format!(
        "{test}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

fn sqs_client(endpoint: &str) -> Client {
    let config = aws_sdk_sqs::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(endpoint)
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("x", "x", None, None, "test"))
        .build();
    Client::from_conf(config)
}

async fn create_queue(client: &Client, name: &str, redrive: Option<(&str, u32)>) -> String {
    let mut req = client.create_queue().queue_name(name);
    if let Some((dlq_url, max)) = redrive {
        let arn = client
            .get_queue_attributes()
            .queue_url(dlq_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .unwrap()
            .attributes()
            .unwrap()
            .get(&QueueAttributeName::QueueArn)
            .unwrap()
            .clone();
        req = req.attributes(
            QueueAttributeName::RedrivePolicy,
            format!(r#"{{"deadLetterTargetArn":"{arn}","maxReceiveCount":"{max}"}}"#),
        );
    }
    req.send().await.unwrap().queue_url.unwrap()
}

async fn sqs_queue(test: &str) -> SqsQueue {
    let client = sqs_client(
        &std::env::var(SQS_ENDPOINT).expect("STORAGE_QUEUE_TEST_SQS_ENDPOINT must be set"),
    );
    let url = create_queue(&client, &unique(test), None).await;
    SqsQueue::new(client, url)
}

async fn sqs_redrive(test: &str, max_receive_count: u32) -> (SqsQueue, SqsQueue) {
    let client = sqs_client(
        &std::env::var(SQS_ENDPOINT).expect("STORAGE_QUEUE_TEST_SQS_ENDPOINT must be set"),
    );
    let dlq_url = create_queue(&client, &unique(&format!("{test}-dlq")), None).await;
    let url = create_queue(&client, &unique(test), Some((&dlq_url, max_receive_count))).await;
    (
        SqsQueue::new(client.clone(), url),
        SqsQueue::new(client, dlq_url),
    )
}

macro_rules! sqs_contract {
    ($f:ident) => {
        mod $f {
            #[tokio::test]
            #[ignore = "requires an SQS-compatible endpoint"]
            async fn sqs() {
                let q = super::sqs_queue(stringify!($f)).await;
                super::$f(&q).await;
            }
        }
    };
}

fn msg(body: &str) -> Outgoing {
    Outgoing {
        body: body.into(),
        delay: Duration::ZERO,
        group_id: None,
    }
}

const S1: Duration = Duration::from_secs(1);
const S2: Duration = Duration::from_secs(2);
const NOW: Duration = Duration::ZERO;

async fn receive_one<T: Transport>(q: &T, visibility: Duration) -> Received {
    let mut got = q.receive(1, NOW, visibility).await.unwrap();
    assert_eq!(got.len(), 1);
    got.remove(0)
}

async fn assert_empty<T: Transport>(q: &T) {
    assert!(q.receive(10, NOW, S1).await.unwrap().is_empty());
}

async fn round_trip<T: Transport>(q: &T) {
    q.send(&[Outgoing {
        body: "hello".into(),
        delay: NOW,
        group_id: Some("g1".into()),
    }])
    .await
    .unwrap();
    let got = receive_one(q, S2).await;
    assert_eq!(got.body, "hello");
    assert_eq!(got.receive_count, 1);
    assert_eq!(got.group_id.as_deref(), Some("g1"));
}
sqs_contract!(round_trip);

async fn redelivery<T: Transport>(q: &T) {
    q.send(&[msg("a")]).await.unwrap();
    receive_one(q, S2).await;
    assert_empty(q).await;
    sleep(Duration::from_millis(2500)).await;
    let got = receive_one(q, S2).await;
    assert_eq!((got.body.as_str(), got.receive_count), ("a", 2));
}
sqs_contract!(redelivery);

async fn delete_acks<T: Transport>(q: &T) {
    q.send(&[msg("a")]).await.unwrap();
    let got = receive_one(q, S1).await;
    q.delete(&[&got.receipt]).await.unwrap();
    sleep(Duration::from_millis(1500)).await;
    assert_empty(q).await;
}
sqs_contract!(delete_acks);

async fn extend_and_nack<T: Transport>(q: &T) {
    q.send(&[msg("a")]).await.unwrap();
    let got = receive_one(q, S1).await;
    q.extend(&got.receipt, Duration::from_secs(4))
        .await
        .unwrap();
    sleep(Duration::from_millis(1500)).await;
    assert_empty(q).await;
    q.extend(&got.receipt, NOW).await.unwrap();
    let again = receive_one(q, S2).await;
    assert_eq!((again.body.as_str(), again.receive_count), ("a", 2));
}
sqs_contract!(extend_and_nack);

async fn stale_receipt<T: Transport>(q: &T) {
    q.send(&[msg("a")]).await.unwrap();
    let first = receive_one(q, S1).await;
    sleep(Duration::from_millis(1500)).await;
    let second = receive_one(q, Duration::from_secs(10)).await;
    assert_eq!(second.receive_count, 2);
    // Providers can reject a stale handle as invalid rather than as not-in-flight.
    assert!(matches!(
        q.extend(&first.receipt, S2).await,
        Err(Error::StaleReceipt | Error::Sqs(_))
    ));
    match q.delete(&[&first.receipt]).await {
        Ok(()) => (),
        Err(Error::Batch(e)) => assert!(
            matches!(&e.outcomes[..], [BatchOutcome::Failed { code, .. }] if code == "ReceiptHandleIsInvalid")
        ),
        Err(e) => panic!("unexpected error: {e}"),
    }
    q.extend(&second.receipt, NOW).await.unwrap();
    let third = receive_one(q, S2).await;
    assert_eq!(third.receive_count, 3);
}
sqs_contract!(stale_receipt);

async fn delay<T: Transport>(q: &T) {
    q.send(&[Outgoing {
        delay: S2,
        ..msg("a")
    }])
    .await
    .unwrap();
    assert_empty(q).await;
    sleep(Duration::from_millis(2500)).await;
    assert_eq!(receive_one(q, S2).await.body, "a");
}
sqs_contract!(delay);

#[tokio::test]
#[ignore = "requires an SQS-compatible endpoint"]
async fn zero_delay_overrides_queue_default() {
    let endpoint =
        std::env::var(SQS_ENDPOINT).expect("STORAGE_QUEUE_TEST_SQS_ENDPOINT must be set");
    let client = sqs_client(&endpoint);
    let url = client
        .create_queue()
        .queue_name(unique("default-delay"))
        .attributes(QueueAttributeName::DelaySeconds, "10")
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap();
    let q = SqsQueue::new(client, url);
    q.send(&[msg("immediate")]).await.unwrap();
    let got = q.receive(1, S1, S2).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].body, "immediate");
}

async fn chunked_send<T: Transport>(q: &T) {
    let all: Vec<_> = (0..25).map(|i| msg(&format!("m{i}"))).collect();
    q.send(&all).await.unwrap();
    let mut seen = HashSet::new();
    for _ in 0..50 {
        if seen.len() == 25 {
            break;
        }
        for m in q.receive(10, S1, Duration::from_secs(30)).await.unwrap() {
            assert!(seen.insert(m.body));
        }
    }
    assert_eq!(seen.len(), 25);
}
sqs_contract!(chunked_send);

async fn concurrent_receivers<T: Transport + Send + Sync + 'static>(q: T) {
    let q = Arc::new(q);
    let all: Vec<_> = (0..20).map(|i| msg(&format!("m{i}"))).collect();
    q.send(&all).await.unwrap();
    let drain = |q: Arc<T>| {
        tokio::spawn(async move {
            let mut got = Vec::new();
            loop {
                let batch = q.receive(3, S1, Duration::from_secs(30)).await.unwrap();
                if batch.is_empty() {
                    return got;
                }
                got.extend(batch);
            }
        })
    };
    let (a, b) = tokio::join!(drain(q.clone()), drain(q.clone()));
    let all: Vec<Received> = [a.unwrap(), b.unwrap()].concat();
    assert_eq!(all.len(), 20);
    assert_eq!(
        all.iter().map(|m| &m.body).collect::<HashSet<_>>().len(),
        20
    );
    assert_eq!(
        all.iter().map(|m| &m.receipt).collect::<HashSet<_>>().len(),
        20
    );
}

#[tokio::test]
#[ignore = "requires an SQS-compatible endpoint"]
async fn concurrent_receivers_sqs() {
    concurrent_receivers(sqs_queue("concurrent").await).await;
}

async fn redrive<T: Transport>(main: &T, dlq: &T) {
    main.send(&[msg("a")]).await.unwrap();
    for n in 1..=2 {
        assert_eq!(receive_one(main, S1).await.receive_count, n);
        sleep(Duration::from_millis(1500)).await;
    }
    assert_empty(main).await;
    let dead = receive_one(dlq, S2).await;
    assert_eq!(dead.body, "a");
}

#[tokio::test]
#[ignore = "requires an SQS-compatible endpoint"]
async fn redrive_sqs() {
    let (main, dlq) = sqs_redrive("redrive", 2).await;
    redrive(&main, &dlq).await;
}

async fn validation<T: Transport>(q: &T) {
    let bad = |r: Result<_, Error>| assert!(matches!(r, Err(Error::InvalidArgument(_))));
    bad(q.receive(0, NOW, S1).await.map(drop));
    bad(q.receive(11, NOW, S1).await.map(drop));
    bad(q.receive(1, Duration::from_secs(21), S1).await.map(drop));
    bad(q.receive(1, Duration::from_millis(500), S1).await.map(drop));
    bad(q
        .receive(1, NOW, Duration::from_millis(500))
        .await
        .map(drop));
    bad(q
        .receive(1, NOW, Duration::from_secs(13 * 3600))
        .await
        .map(drop));
    bad(q.extend("r", Duration::from_secs(13 * 3600)).await);
    bad(q.extend("r", Duration::from_millis(500)).await);
    bad(q
        .send(&[Outgoing {
            delay: Duration::from_millis(500),
            ..msg("a")
        }])
        .await);
    bad(q
        .send(&[Outgoing {
            delay: Duration::from_millis(900_001),
            ..msg("a")
        }])
        .await);
    bad(q
        .send(&[Outgoing {
            delay: Duration::from_secs(16 * 60),
            ..msg("a")
        }])
        .await);
}

#[tokio::test]
async fn validation_sqs() {
    let q = SqsQueue::new(sqs_client("http://127.0.0.1:1"), "http://127.0.0.1:1/q");
    validation(&q).await;
}
