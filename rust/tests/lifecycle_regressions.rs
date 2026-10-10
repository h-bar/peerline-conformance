//! Process-bounded regression probes for lifecycle paths which historically
//! deadlocked or left public handles parked after the UDS driver had gone.

use std::{
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, RawWaker, RawWakerVTable, Wake, Waker},
    time::{Duration, Instant},
};

use futures::{Future, StreamExt, future::BoxFuture};
use peerline::runtime::{Error, Peer, Publisher};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

const CHILD_ENV: &str = "PEERLINE_LIFECYCLE_CHILD";

fn run_child(mode: &str) {
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "lifecycle_child", "--nocapture"])
        .env(CHILD_ENV, mode)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn lifecycle probe");
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = child.try_wait().expect("poll lifecycle probe") {
            let output = child.wait_with_output().expect("collect lifecycle probe");
            assert!(
                status.success(),
                "lifecycle probe {mode:?} failed\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill wedged lifecycle probe");
            let output = child.wait_with_output().expect("collect wedged probe");
            panic!(
                "lifecycle probe {mode:?} deadlocked\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn response_and_stream_teardown_callbacks_may_reenter_metrics() {
    run_child("response-reentry");
    run_child("unary-to-stream-reentry");
    run_child("stream-to-unary-reentry");
    run_child("stream-close-reentry");
    run_child("subscriber-close-reentry");
    run_child("subscriber-supersede-reentry");
    run_child("subscriber-drop-reentry");
    run_child("stream-drop-reentry");
    run_child("call-drop-reentry");
}

#[test]
fn replay_enqueue_callbacks_may_clone_drop_and_wake_reentrantly() {
    run_child("writer-callbacks");
}

#[test]
fn clean_eof_still_drains_accepted_handlers() {
    run_child("clean-handler-drain");
}

#[test]
fn concurrent_publish_and_resume_preserve_wire_sequence_order() {
    run_child("concurrent-replay");
}

#[test]
fn dropping_driver_settles_registered_and_not_yet_polled_work() {
    run_child("driver-drop");
    run_child("driver-drop-before-poll");
}

#[test]
fn cancelling_driver_discards_escaped_stream_senders_depth_without_underflow() {
    run_child("concurrent-stream-cancellation");
}

#[test]
fn subscriptions_after_eof_are_abandoned_once_then_end() {
    run_child("subscriptions-after-eof");
}

#[test]
fn eof_releases_handlers_that_capture_peer_owned_handles() {
    run_child("handler-cycles");
}

#[test]
fn replay_serialization_may_query_the_same_publisher() {
    run_child("replay-serialize-reentry");
}

struct MetricsWake {
    peer: Peer,
    woke: tokio::sync::Notify,
}

impl MetricsWake {
    fn new(peer: &Peer) -> Arc<Self> {
        Arc::new(Self {
            peer: peer.clone(),
            woke: tokio::sync::Notify::new(),
        })
    }
}

impl Wake for MetricsWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.peer.metrics();
        self.woke.notify_one();
    }
}

struct CallbackState {
    peer: Peer,
    clones: AtomicUsize,
    drops: AtomicUsize,
    wakes: AtomicUsize,
}

impl CallbackState {
    fn waker(self: &Arc<Self>) -> Waker {
        // Each raw waker owns exactly one Arc strong reference.
        unsafe {
            Waker::from_raw(RawWaker::new(
                Arc::into_raw(self.clone()).cast(),
                &CALLBACK_VTABLE,
            ))
        }
    }
}

unsafe fn clone_callback(data: *const ()) -> RawWaker {
    // The vtable receives the Arc pointer created by CallbackState::waker.
    let state = unsafe { &*data.cast::<CallbackState>() };
    let _ = state.peer.metrics();
    state.clones.fetch_add(1, Ordering::Relaxed);
    unsafe { Arc::increment_strong_count(data.cast::<CallbackState>()) };
    RawWaker::new(data, &CALLBACK_VTABLE)
}

unsafe fn wake_callback(data: *const ()) {
    // A consuming wake takes back and releases the raw waker's Arc.
    let state = unsafe { Arc::from_raw(data.cast::<CallbackState>()) };
    let _ = state.peer.metrics();
    state.wakes.fetch_add(1, Ordering::Relaxed);
}

unsafe fn wake_ref_callback(data: *const ()) {
    // A borrowed wake leaves the raw waker's Arc ownership unchanged.
    let state = unsafe { &*data.cast::<CallbackState>() };
    let _ = state.peer.metrics();
    state.wakes.fetch_add(1, Ordering::Relaxed);
}

unsafe fn drop_callback(data: *const ()) {
    // Drop takes back exactly the reference owned by this raw waker.
    let state = unsafe { Arc::from_raw(data.cast::<CallbackState>()) };
    let _ = state.peer.metrics();
    state.drops.fetch_add(1, Ordering::Relaxed);
}

static CALLBACK_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_callback,
    wake_callback,
    wake_ref_callback,
    drop_callback,
);

struct LiveDuringSerialize(Publisher);

impl Serialize for LiveDuringSerialize {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0.live() as u64)
    }
}

struct RefusesSerialization;

impl Serialize for RefusesSerialization {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "no listener should need this payload",
        ))
    }
}

async fn raw_uds() -> (
    Peer,
    BoxFuture<'static, ()>,
    BufReader<tokio::net::UnixStream>,
) {
    // Keep this deliberately short: Darwin's sockaddr_un path is only 104 bytes.
    let path = std::path::PathBuf::from(format!("/tmp/plr-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path).expect("bind raw UDS peer");
    let (peer, driver) = peerline_transport_uds::connect(&path)
        .await
        .expect("connect raw UDS peer");
    let (remote, _) = listener.accept().await.expect("accept raw UDS peer");
    drop(listener);
    std::fs::remove_file(path).expect("unlink raw UDS path");
    (peer, Box::pin(driver), BufReader::new(remote))
}

async fn wire_id(remote: &mut BufReader<tokio::net::UnixStream>) -> u64 {
    let mut request = String::new();
    remote.read_line(&mut request).await.expect("read request");
    serde_json::from_str::<Value>(&request).expect("request JSON")["id"]
        .as_u64()
        .expect("numeric request id")
}

async fn probe(mode: &str) {
    if mode == "concurrent-stream-cancellation" {
        for round in 0..128 {
            let (peer, driver, mut remote) = raw_uds().await;
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
            peer.on_stream_request("send", move |_: Value, sender| {
                tx.send(Arc::new(sender))
                    .expect("escape accepted stream sender");
                futures::future::pending::<Result<(), peerline::wire::RpcError>>()
            });
            let driver = tokio::spawn(driver);
            remote
                .get_mut()
                .write_all(
                    b"{\"ver\":\"1\",\"kind\":\"req\",\"id\":1,\"op\":\"send\",\"args\":{}}\n",
                )
                .await
                .expect("request actual outbound stream");
            let sender = rx.recv().await.expect("handler accepted wire request");
            let stopped = Arc::new(AtomicBool::new(false));
            let attempts = Arc::new(AtomicUsize::new(0));
            let mut threads = Vec::new();
            for _ in 0..8 {
                let sender = sender.clone();
                let stopped = stopped.clone();
                let attempts = attempts.clone();
                threads.push(std::thread::spawn(move || {
                    while !stopped.load(Ordering::Acquire) {
                        let _ = sender.send_item(&1u64);
                        attempts.fetch_add(1, Ordering::Relaxed);
                    }
                }));
            }
            while attempts.load(Ordering::Relaxed) < 256 {
                std::thread::yield_now();
            }
            driver.abort();
            assert!(driver.await.expect_err("aborted driver").is_cancelled());
            stopped.store(true, Ordering::Release);
            for thread in threads {
                thread.join().expect("sending thread joins");
            }
            assert_eq!(sender.queued_items(), 0, "stream depth after round {round}");
            assert_eq!(
                peer.metrics().outbound_depth,
                0,
                "aggregate depth after round {round}"
            );
            assert!(matches!(sender.send_item(&2u64), Err(Error::Closed)));
            assert_eq!(sender.queued_items(), 0);
            assert_eq!(peer.metrics().outbound_depth, 0);
        }
        return;
    }
    let (peer, driver, mut remote) = raw_uds().await;
    if mode == "driver-drop-before-poll" {
        let args = json!({});
        let mut call = Box::pin(peer.call::<_, Value>("queued-call", &args));
        let mut stream = peer
            .call_stream::<_, Value>("queued-stream", &args)
            .expect("register stream before driver poll");
        let mut sub = peer.replaying_subscription::<Value>("queued-sub");
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(call.as_mut().poll(&mut cx).is_pending());
        assert!(stream.poll_next_unpin(&mut cx).is_pending());
        assert!(sub.poll_next_unpin(&mut cx).is_pending());
        drop(driver);
        assert!(matches!(call.await, Err(Error::Abandoned)));
        assert!(matches!(stream.next().await, Some(Err(Error::Abandoned))));
        assert!(stream.next().await.is_none());
        assert!(matches!(sub.next().await, Some(Err(Error::Abandoned))));
        assert!(sub.next().await.is_none());
        assert_eq!(peer.metrics().pending_responses, 0);
        assert_eq!(peer.metrics().active_streams, 0);
        assert_eq!(peer.metrics().live_subscriptions, 0);
        assert!(matches!(
            peer.notify("late", &json!({})),
            Err(Error::Closed)
        ));
        return;
    }
    if mode == "writer-callbacks" {
        let mut driver = driver;
        let callbacks = || {
            Arc::new(CallbackState {
                peer: peer.clone(),
                clones: AtomicUsize::new(0),
                drops: AtomicUsize::new(0),
                wakes: AtomicUsize::new(0),
            })
        };
        let first_callbacks = callbacks();
        let first = first_callbacks.waker();
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(&first))
                .is_pending()
        );
        assert!(first_callbacks.clones.load(Ordering::Relaxed) > 0);
        let second_callbacks = callbacks();
        let second = second_callbacks.waker();
        assert!(!first.will_wake(&second));
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(&second))
                .is_pending()
        );
        assert!(second_callbacks.clones.load(Ordering::Relaxed) > 0);
        assert!(first_callbacks.drops.load(Ordering::Relaxed) > 0);
        let publisher = peer.publisher();
        publisher.enable_replay("events", 8);
        let live = publisher.subscribe("events").expect("live subscription");
        let wakes = second_callbacks.wakes.load(Ordering::Relaxed);
        assert_eq!(publisher.publish("events", &json!(7)).expect("publish"), 1);
        assert!(second_callbacks.wakes.load(Ordering::Relaxed) > wakes);
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(&second))
                .is_pending()
        );
        let mut line = String::new();
        remote.read_line(&mut line).await.expect("live event frame");
        let frame: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(frame["args"]["subscription_id"], live.subscription_id);
        assert_eq!(frame["args"]["seq"], 0);
        assert_eq!(frame["args"]["event"], 7);
        let replay = publisher
            .subscribe_from("events", None)
            .expect("replay subscription");
        let wakes = second_callbacks.wakes.load(Ordering::Relaxed);
        assert!(publisher.start_replay(&replay.subscription_id));
        assert!(second_callbacks.wakes.load(Ordering::Relaxed) > wakes);
        assert!(
            driver
                .as_mut()
                .poll(&mut Context::from_waker(&second))
                .is_pending()
        );
        line.clear();
        remote.read_line(&mut line).await.expect("replayed frame");
        let frame: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(frame["args"]["subscription_id"], replay.subscription_id);
        assert_eq!(frame["args"]["seq"], 0);
        assert_eq!(frame["args"]["event"], 7);
        drop(driver);
        assert!(second_callbacks.drops.load(Ordering::Relaxed) > 0);
        drop(first);
        drop(second);
        return;
    }
    let driver = tokio::spawn(driver);
    match mode {
        "response-reentry" | "stream-to-unary-reentry" => {
            let args = json!({});
            let mut call = Box::pin(peer.call::<_, Value>("hold", &args));
            let wake = MetricsWake::new(&peer);
            let waker = Waker::from(wake.clone());
            assert!(
                call.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let id = wire_id(&mut remote).await;
            let response = if mode == "response-reentry" {
                json!({"ver":"1","kind":"resp","id":id,"data":1})
            } else {
                json!({"ver":"1","kind":"stream","id":id,"seq":0,"data":1})
            };
            remote
                .get_mut()
                .write_all(format!("{response}\n").as_bytes())
                .await
                .expect("write response");
            // Await the custom callback before repolling can replace its waker.
            wake.woke.notified().await;
            if mode == "response-reentry" {
                assert_eq!(call.await.expect("call response"), json!(1));
            } else {
                assert!(matches!(call.await, Err(Error::Rpc(_))));
            }
        }
        "unary-to-stream-reentry" => {
            let mut stream = peer.call_stream::<_, Value>("hold", &json!({})).unwrap();
            let wake = MetricsWake::new(&peer);
            let waker = Waker::from(wake.clone());
            assert!(
                stream
                    .poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            let id = wire_id(&mut remote).await;
            let response = json!({"ver":"1","kind":"resp","id":id,"data":1});
            remote
                .get_mut()
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
            wake.woke.notified().await;
            assert!(matches!(stream.next().await, Some(Err(Error::Rpc(_)))));
            assert!(stream.next().await.is_none());
        }
        "stream-close-reentry" => {
            let mut stream = peer
                .call_stream::<_, Value>("hold", &json!({}))
                .expect("stream");
            let waker = Waker::from(MetricsWake::new(&peer));
            assert!(
                stream
                    .poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            wire_id(&mut remote).await;
            drop(remote);
            driver.await.expect("driver joins");
            assert!(matches!(stream.next().await, Some(Err(Error::Abandoned))));
            assert!(stream.next().await.is_none());
            assert_eq!(peer.metrics().active_streams, 0);
            return;
        }
        "subscriber-close-reentry" => {
            let mut sub = peer.subscription::<Value>("open");
            let waker = Waker::from(MetricsWake::new(&peer));
            assert!(
                sub.poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(remote);
            driver.await.expect("driver joins");
            assert!(matches!(sub.next().await, Some(Err(Error::Abandoned))));
            assert!(sub.next().await.is_none());
            assert_eq!(peer.metrics().live_subscriptions, 0);
            return;
        }
        "subscriber-supersede-reentry" | "subscriber-drop-reentry" => {
            let mut sub = peer.subscription::<Value>("open");
            let waker = Waker::from(MetricsWake::new(&peer));
            assert!(
                sub.poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            if mode == "subscriber-drop-reentry" {
                sub.unsubscribe();
                assert_eq!(peer.metrics().live_subscriptions, 0);
            } else {
                let successor = peer.subscription::<Value>("open");
                assert!(matches!(sub.next().await, Some(Err(Error::Abandoned))));
                assert!(sub.next().await.is_none());
                assert_eq!(peer.metrics().live_subscriptions, 1);
                drop(successor);
            }
        }
        "stream-drop-reentry" => {
            let mut stream = peer.call_stream::<_, Value>("hold", &json!({})).unwrap();
            let waker = Waker::from(MetricsWake::new(&peer));
            assert!(
                stream
                    .poll_next_unpin(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(stream);
            assert_eq!(peer.metrics().active_streams, 0);
        }
        "call-drop-reentry" => {
            let args = json!({});
            let mut call = Box::pin(peer.call::<_, Value>("hold", &args));
            let waker = Waker::from(MetricsWake::new(&peer));
            assert!(
                call.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(call);
            assert_eq!(peer.metrics().pending_responses, 0);
        }
        "driver-drop" => {
            let p = peer.clone();
            let call = tokio::spawn(async move { p.call::<_, Value>("hold", &json!({})).await });
            let mut stream = peer
                .call_stream::<_, Value>("hold-stream", &json!({}))
                .expect("register stream");
            let mut sub = peer.subscription::<Value>("hold-sub");
            // `call_stream` enqueues synchronously, while the spawned unary
            // call must first be scheduled. Seeing both requests proves all
            // three observer kinds are registered before the abort.
            wire_id(&mut remote).await;
            wire_id(&mut remote).await;
            driver.abort();
            assert!(driver.await.expect_err("aborted driver").is_cancelled());
            assert!(matches!(
                call.await.expect("call task"),
                Err(Error::Abandoned)
            ));
            assert!(matches!(stream.next().await, Some(Err(Error::Abandoned))));
            assert!(stream.next().await.is_none());
            assert!(matches!(sub.next().await, Some(Err(Error::Abandoned))));
            assert!(sub.next().await.is_none());
            assert_eq!(peer.metrics().pending_responses, 0);
            assert_eq!(peer.metrics().active_streams, 0);
            assert_eq!(peer.metrics().live_subscriptions, 0);
            assert!(matches!(
                peer.notify("late", &json!({})),
                Err(Error::Closed)
            ));
            return;
        }
        "subscriptions-after-eof" => {
            let mut existing = peer.subscription::<Value>("existing");
            drop(remote);
            driver.await.expect("driver joins");
            assert!(matches!(existing.next().await, Some(Err(Error::Abandoned))));
            assert!(existing.next().await.is_none());
            for replaying in [false, true] {
                let mut late = if replaying {
                    peer.replaying_subscription::<Value>("late")
                } else {
                    peer.subscription::<Value>("late")
                };
                assert!(matches!(late.next().await, Some(Err(Error::Abandoned))));
                assert!(late.next().await.is_none());
            }
            assert_eq!(peer.metrics().live_subscriptions, 0);
            return;
        }
        "clean-handler-drain" => {
            peer.on_request("echo", |args: Value| async move {
                tokio::task::yield_now().await;
                Ok::<_, peerline::wire::RpcError>(args["n"].clone())
            });
            let requests: String = (0..24)
                .map(|n| {
                    format!(
                        "{}\n",
                        json!({"ver":"1","kind":"req","id":n,"op":"echo","args":{"n":n}})
                    )
                })
                .collect();
            remote
                .get_mut()
                .write_all(requests.as_bytes())
                .await
                .unwrap();
            remote.get_mut().shutdown().await.unwrap();
            let mut replies = Vec::new();
            let mut line = String::new();
            while remote.read_line(&mut line).await.unwrap() != 0 {
                let frame: Value = serde_json::from_str(&line).unwrap();
                assert!(
                    frame.get("err").is_none(),
                    "accepted handler was lost: {frame}"
                );
                replies.push(frame["data"].as_u64().unwrap());
                line.clear();
            }
            driver.await.unwrap();
            replies.sort_unstable();
            assert_eq!(replies, (0..24).collect::<Vec<_>>());
            return;
        }
        "handler-cycles" => {
            let sentinel = Arc::new(());
            let weak = Arc::downgrade(&sentinel);
            let publisher = peer.publisher();
            peer.on_request("request", move |_: Value| {
                let keep = sentinel.clone();
                let publisher = publisher.clone();
                async move {
                    let _ = (&keep, publisher.live());
                    Ok::<_, peerline::wire::RpcError>(())
                }
            });
            let sentinel = weak.upgrade().expect("sentinel still live");
            peer.on_notification("notification", move |_: Value| {
                let keep = sentinel.clone();
                async move { drop(keep) }
            });
            let sentinel = weak.upgrade().expect("sentinel still live");
            peer.on_stream_request("stream", move |_: Value, _| {
                let keep = sentinel.clone();
                async move {
                    drop(keep);
                    Ok::<_, peerline::wire::RpcError>(())
                }
            });
            let sentinel = weak.upgrade().expect("sentinel still live");
            peer.on_protocol_error(move |_| drop(sentinel.clone()));
            drop(remote);
            driver.await.expect("driver joins");
            drop(peer);
            assert!(
                weak.upgrade().is_none(),
                "handler registry retained an application Arc"
            );
            return;
        }
        "replay-serialize-reentry" => {
            let publisher = peer.publisher();
            assert_eq!(
                publisher.publish("unused", &RefusesSerialization).unwrap(),
                0
            );
            publisher
                .subscribe("ringless")
                .expect("ringless subscriber");
            assert_eq!(
                publisher
                    .publish("ringless", &LiveDuringSerialize(publisher.clone()))
                    .expect("ringless publish"),
                1
            );
            publisher.enable_replay("replay", 4);
            publisher.subscribe("replay").expect("replay subscriber");
            assert_eq!(
                publisher
                    .publish("replay", &LiveDuringSerialize(publisher.clone()))
                    .expect("replay publish"),
                1
            );
            assert_eq!(publisher.replay_len("replay"), Some(1));
        }
        "concurrent-replay" => {
            const EVENTS: usize = 128;
            let publisher = peer.publisher();
            publisher.enable_replay("events", EVENTS);
            let live = publisher.subscribe("events").unwrap();
            let replay = publisher.subscribe_from("events", None).unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(5));
            let mut workers = Vec::new();
            for producer in 0..4 {
                let publisher = publisher.clone();
                let barrier = barrier.clone();
                workers.push(std::thread::spawn(move || {
                    barrier.wait();
                    for n in 0..32 {
                        publisher.publish("events", &json!([producer, n])).unwrap();
                    }
                }));
            }
            let replay_id = replay.subscription_id.clone();
            let resume_publisher = publisher.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                assert!(resume_publisher.start_replay(&replay_id));
            }));
            for worker in workers {
                worker.join().unwrap();
            }
            let mut live_frames = Vec::new();
            let mut replay_frames = Vec::new();
            for _ in 0..EVENTS * 2 {
                let mut line = String::new();
                remote.read_line(&mut line).await.unwrap();
                let frame: Value = serde_json::from_str(&line).unwrap();
                let args = &frame["args"];
                let event = (args["seq"].as_u64().unwrap(), args["event"].clone());
                if args["subscription_id"] == live.subscription_id {
                    live_frames.push(event);
                } else {
                    assert_eq!(args["subscription_id"], replay.subscription_id);
                    replay_frames.push(event);
                }
            }
            assert_eq!(live_frames.len(), EVENTS);
            assert_eq!(
                replay_frames, live_frames,
                "replay/live splice changed order or duplicated an event"
            );
            assert_eq!(
                live_frames.iter().map(|(seq, _)| *seq).collect::<Vec<_>>(),
                (0..EVENTS as u64).collect::<Vec<_>>()
            );
            assert_eq!(
                live_frames
                    .iter()
                    .map(|(_, event)| event.to_string())
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
                EVENTS
            );
        }
        other => panic!("unknown lifecycle probe {other}"),
    }
    drop(remote);
    driver.await.expect("driver joins");
}

#[test]
fn lifecycle_child() {
    let Ok(mode) = std::env::var(CHILD_ENV) else {
        return;
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime")
        .block_on(probe(&mode));
}
