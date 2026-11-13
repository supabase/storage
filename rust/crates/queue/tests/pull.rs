use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::json;
use storage_queue::{BoxError, Context, Disposition, Job, Worker, WorkerConfig};
use storage_queue_transport::{Error, Outgoing, Received, Transport};
use tokio::{
    sync::{Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;

struct Request {
    max: usize,
    wait: Duration,
    reply: oneshot::Sender<Result<Vec<Received>, Error>>,
}

struct Scripted {
    requests: mpsc::UnboundedSender<Request>,
    changes: Mutex<Vec<(Vec<String>, Duration)>>,
    accept_renewal: bool,
}

impl Transport for Scripted {
    async fn send(&self, _: &[Outgoing]) -> Result<(), Error> {
        unreachable!()
    }

    async fn receive(
        &self,
        max: usize,
        wait: Duration,
        _: Duration,
    ) -> Result<Vec<Received>, Error> {
        let (reply, result) = oneshot::channel();
        self.requests.send(Request { max, wait, reply }).unwrap();
        result
            .await
            .unwrap_or_else(|_| Err(Error::InvalidResponse("receive failed")))
    }

    async fn extend(&self, receipt: &str, visibility: Duration) -> Result<(), Error> {
        self.changes
            .lock()
            .unwrap()
            .push((vec![receipt.to_owned()], visibility));
        Ok(())
    }

    async fn extend_batch(
        &self,
        receipts: &[&str],
        visibility: Duration,
    ) -> Result<Vec<bool>, Error> {
        self.changes.lock().unwrap().push((
            receipts.iter().map(|r| (*r).to_owned()).collect(),
            visibility,
        ));
        Ok(vec![self.accept_renewal; receipts.len()])
    }

    async fn delete(&self, _: &[&str]) -> Result<(), Error> {
        Ok(())
    }
}

struct Handler {
    started: Arc<Mutex<Vec<(String, u32)>>>,
    finish: Arc<Semaphore>,
}

impl Job for Handler {
    const NAME: &'static str = "pull-test";
    type Payload = String;
    async fn run(&self, ctx: Context, payload: String) -> Result<Disposition, BoxError> {
        self.started
            .lock()
            .unwrap()
            .push((payload, ctx.receive_count));
        tokio::select! {
            permit = self.finish.acquire() => permit.unwrap().forget(),
            _ = ctx.cancel.cancelled() => {}
        }
        Ok(Disposition::Done)
    }
}

fn messages(start: usize, count: usize) -> Vec<Received> {
    (start..start + count)
        .map(|n| Received {
            id: format!("id-{n}"),
            receipt: format!("receipt-{n}"),
            body: json!(format!("id-{n}")).to_string(),
            receive_count: 1,
            group_id: None,
            attributes: Default::default(),
        })
        .collect()
}

struct Harness {
    requests: mpsc::UnboundedReceiver<Request>,
    transport: Arc<Scripted>,
    started: Arc<Mutex<Vec<(String, u32)>>>,
    finish: Arc<Semaphore>,
    shutdown: CancellationToken,
    worker: JoinHandle<Result<(), storage_queue::Error>>,
}

fn start(batch: usize, concurrency: usize, accept_renewal: bool) -> Harness {
    start_with(
        WorkerConfig {
            batch,
            concurrency,
            ..WorkerConfig::default()
        },
        accept_renewal,
    )
}

fn start_with(config: WorkerConfig, accept_renewal: bool) -> Harness {
    let (tx, requests) = mpsc::unbounded_channel();
    let transport = Arc::new(Scripted {
        requests: tx,
        changes: Mutex::new(Vec::new()),
        accept_renewal,
    });
    let started = Arc::new(Mutex::new(Vec::new()));
    let finish = Arc::new(Semaphore::new(0));
    let mut worker = Worker::new(transport.clone(), config).unwrap();
    worker.register(Handler {
        started: started.clone(),
        finish: finish.clone(),
    });
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(worker.run(shutdown.clone()));
    Harness {
        requests,
        transport,
        started,
        finish,
        shutdown,
        worker: task,
    }
}

async fn spin_until(mut condition: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not reached");
}

async fn stop(harness: Harness) {
    harness.shutdown.cancel();
    harness.worker.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn fills_in_parallel_then_renews_late_results_without_exceeding_concurrency() {
    let mut h = start(40, 20, true);
    let first = h.requests.recv().await.unwrap();
    assert_eq!((first.max, first.wait), (10, Duration::from_secs(20)));
    first.reply.send(Ok(messages(0, 10))).unwrap();
    let mut fills = Vec::new();
    for _ in 0..3 {
        let request = h.requests.recv().await.unwrap();
        assert_eq!((request.max, request.wait), (10, Duration::ZERO));
        fills.push(request);
    }
    fills.remove(0).reply.send(Ok(messages(10, 10))).unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    time::advance(Duration::from_millis(999)).await;
    assert!(h.started.lock().unwrap().is_empty());
    time::advance(Duration::from_millis(1)).await;
    spin_until(|| h.started.lock().unwrap().len() == 20).await;
    let next = h.requests.recv().await.unwrap();
    assert_eq!(next.wait, Duration::from_secs(20));
    fills.remove(0).reply.send(Ok(messages(20, 10))).unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    next.reply.send(Ok(Vec::new())).unwrap();
    spin_until(|| {
        h.transport
            .changes
            .lock()
            .unwrap()
            .iter()
            .any(|(r, v)| r.len() == 10 && *v == Duration::from_secs(60))
    })
    .await;
    assert_eq!(h.started.lock().unwrap().len(), 20);
    let fill_carried = h.requests.recv().await.unwrap();
    assert_eq!(fill_carried.wait, Duration::ZERO);
    fill_carried.reply.send(Ok(Vec::new())).unwrap();
    h.finish.add_permits(10);
    spin_until(|| h.started.lock().unwrap().len() == 30).await;
    stop(h).await;
}

#[tokio::test(start_paused = true)]
async fn duplicate_ids_in_a_pull_use_the_latest_delivery() {
    let mut h = start(20, 20, true);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let fill = h.requests.recv().await.unwrap();
    let mut more = messages(10, 9);
    let mut duplicate = messages(0, 1).pop().unwrap();
    duplicate.receive_count = 2;
    duplicate.receipt = "new-receipt".into();
    more.push(duplicate);
    fill.reply.send(Ok(more)).unwrap();
    spin_until(|| h.started.lock().unwrap().len() == 19).await;
    let started = h.started.lock().unwrap().clone();
    assert_eq!(started.iter().filter(|(id, _)| id == "id-0").count(), 1);
    assert!(started.contains(&("id-0".to_owned(), 2)));
    stop(h).await;
}

#[tokio::test(start_paused = true)]
async fn partial_first_receive_does_not_fan_out() {
    let mut h = start(40, 40, true);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 5)))
        .unwrap();
    let next = h.requests.recv().await.unwrap();
    assert_eq!((next.max, next.wait), (10, Duration::from_secs(20)));
    spin_until(|| h.started.lock().unwrap().len() == 5).await;
    stop(h).await;
    assert!(next.reply.is_closed());
}

#[tokio::test(start_paused = true)]
async fn failed_renewal_drops_late_messages() {
    let mut h = start(20, 10, false);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let fill = h.requests.recv().await.unwrap();
    time::advance(Duration::from_secs(1)).await;
    spin_until(|| h.started.lock().unwrap().len() == 10).await;
    let next = h.requests.recv().await.unwrap();
    fill.reply.send(Ok(messages(10, 10))).unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    next.reply.send(Ok(Vec::new())).unwrap();
    spin_until(|| !h.transport.changes.lock().unwrap().is_empty()).await;
    assert_eq!(h.started.lock().unwrap().len(), 10);
    stop(h).await;
}

#[tokio::test(start_paused = true)]
async fn queued_jobs_heartbeat_until_a_handler_slot_is_available() {
    let (tx, mut requests) = mpsc::unbounded_channel();
    let transport = Arc::new(Scripted {
        requests: tx,
        changes: Mutex::new(Vec::new()),
        accept_renewal: true,
    });
    let started = Arc::new(Mutex::new(Vec::new()));
    let finish = Arc::new(Semaphore::new(0));
    let mut worker = Worker::new(
        transport.clone(),
        WorkerConfig {
            batch: 2,
            concurrency: 1,
            visibility: Duration::from_secs(4),
            auto_heartbeat: true,
            execution_timeout: Some(Duration::from_secs(30)),
            ..WorkerConfig::default()
        },
    )
    .unwrap();
    worker.register(Handler {
        started: started.clone(),
        finish: finish.clone(),
    });
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(worker.run(shutdown.clone()));
    requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 2)))
        .unwrap();
    spin_until(|| started.lock().unwrap().len() == 1).await;
    time::advance(Duration::from_secs(2)).await;
    spin_until(|| {
        transport
            .changes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, d)| *d == Duration::from_secs(4))
            .count()
            >= 2
    })
    .await;
    finish.add_permits(1);
    spin_until(|| started.lock().unwrap().len() == 2).await;
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_queued_job_does_not_start() {
    let (tx, mut requests) = mpsc::unbounded_channel();
    let transport = Arc::new(Scripted {
        requests: tx,
        changes: Mutex::new(Vec::new()),
        accept_renewal: true,
    });
    let started = Arc::new(Mutex::new(Vec::new()));
    let finish = Arc::new(Semaphore::new(0));
    let mut worker = Worker::new(
        transport,
        WorkerConfig {
            batch: 2,
            concurrency: 1,
            visibility: Duration::from_secs(2),
            ..WorkerConfig::default()
        },
    )
    .unwrap();
    worker.register(Handler {
        started: started.clone(),
        finish,
    });
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(worker.run(shutdown.clone()));
    requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 2)))
        .unwrap();
    spin_until(|| started.lock().unwrap().len() == 1).await;
    time::advance(Duration::from_secs(2)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(started.lock().unwrap().len(), 1);
    shutdown.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_late_receives_and_releases_waiting_jobs() {
    let mut h = start(20, 1, true);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let fill = h.requests.recv().await.unwrap();
    time::advance(Duration::from_secs(1)).await;
    spin_until(|| h.started.lock().unwrap().len() == 1).await;
    h.shutdown.cancel();
    h.worker.await.unwrap().unwrap();
    assert!(fill.reply.is_closed());
    assert_eq!(
        h.transport
            .changes
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, d)| d.is_zero())
            .map(|(receipts, _)| receipts.len())
            .sum::<usize>(),
        9
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_during_fill_releases_the_received_batch() {
    let mut h = start(20, 20, true);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let fill = h.requests.recv().await.unwrap();
    let transport = h.transport.clone();
    h.shutdown.cancel();
    h.worker.await.unwrap().unwrap();
    assert!(fill.reply.is_closed());
    let changes = transport.changes.lock().unwrap();
    assert!(
        changes
            .iter()
            .any(|(receipts, duration)| receipts.len() == 10 && duration.is_zero())
    );
}

#[tokio::test(start_paused = true)]
async fn five_hundred_message_prefetch_fans_out_in_requests_of_ten() {
    let mut h = start(500, 1, true);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    for i in 0..49 {
        let fill = h.requests.recv().await.unwrap();
        assert_eq!((fill.max, fill.wait), (10, Duration::ZERO));
        fill.reply.send(Ok(messages(10 + i * 10, 10))).unwrap();
    }
    spin_until(|| h.started.lock().unwrap().len() == 1).await;
    assert!(h.requests.try_recv().is_err());
    stop(h).await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_releases_late_messages_parked_during_a_new_long_poll() {
    let mut h = start(20, 10, true);
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let late = h.requests.recv().await.unwrap();
    time::advance(Duration::from_secs(1)).await;
    spin_until(|| h.started.lock().unwrap().len() == 10).await;
    let next = h.requests.recv().await.unwrap();
    late.reply.send(Ok(messages(10, 10))).unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    h.shutdown.cancel();
    h.worker.await.unwrap().unwrap();
    assert!(next.reply.is_closed());
    assert!(
        h.transport
            .changes
            .lock()
            .unwrap()
            .iter()
            .any(|(receipts, delay)| receipts.len() == 10 && delay.is_zero())
    );
}

#[tokio::test(start_paused = true)]
async fn expired_carried_message_is_not_renewed_or_dispatched() {
    let mut h = start_with(
        WorkerConfig {
            batch: 20,
            concurrency: 10,
            visibility: Duration::from_secs(2),
            ..WorkerConfig::default()
        },
        true,
    );
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let fill = h.requests.recv().await.unwrap();
    time::advance(Duration::from_secs(1)).await;
    spin_until(|| h.started.lock().unwrap().len() == 10).await;
    let next = h.requests.recv().await.unwrap();
    fill.reply.send(Ok(messages(10, 10))).unwrap();
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    time::advance(Duration::from_secs(2)).await;
    next.reply.send(Ok(Vec::new())).unwrap();
    let following = h.requests.recv().await.unwrap();
    assert_eq!(following.wait, Duration::from_secs(20));
    assert!(h.transport.changes.lock().unwrap().is_empty());
    assert_eq!(h.started.lock().unwrap().len(), 10);
    stop(h).await;
}

#[tokio::test(start_paused = true)]
async fn failed_fill_does_not_delay_other_results() {
    let mut h = start(30, 30, true);
    let started = time::Instant::now();
    h.requests
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(messages(0, 10)))
        .unwrap();
    let first_fill = h.requests.recv().await.unwrap();
    let second_fill = h.requests.recv().await.unwrap();
    first_fill
        .reply
        .send(Err(Error::InvalidResponse("failed fill")))
        .unwrap();
    second_fill.reply.send(Ok(messages(10, 10))).unwrap();
    spin_until(|| h.started.lock().unwrap().len() == 20).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    stop(h).await;
}
