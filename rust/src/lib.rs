//! The shared conformance handler-set and battery.
//!
//! One [`register`] (the server side) and one [`battery`] (the client side)
//! that between them exercise the whole peerline surface: unary calls, typed
//! args, error propagation (with and without `data`), notifications,
//! server-streaming with a clean end and with a mid-stream error, concurrent
//! multiplexing, peer symmetry (server → client call inside a client →
//! server call) and a 1 MiB frame.
//!
//! Then the surface each implementation grew its own tests for but which had
//! never met its counterpart over a wire: the **pubsub runtime layer**
//! ([`Publisher`] / [`Subscription`], not hand-rolled frames) including
//! **replay from a cursor** and the **gap** a rolled window admits, **kind
//! mismatch** (a `call` against a stream handler and a `call_stream` against
//! a unary one), **stream cancel** observed from the producing end,
//! **malformed args** answered `-32602` by both servers, and the two error
//! paths that are easy to get subtly wrong — an unknown op and a handler that
//! fails in a way that is not already an [`RpcError`].
//!
//! [`truncation_battery`] is the tail that cannot live in [`battery`]: it
//! kills the connection on purpose, to pin what a consumer sees when the
//! peer dies with a stream open and a call in flight. It runs last, only in
//! the cross-process interop runners, and needs the server to have installed
//! [`register_crash`].
//!
//! They live in a library rather than in some crate's `tests/` because Rust
//! cannot share test code across crates — an integration-test or binary
//! target is not linkable, so anything reachable from *another* crate's tests
//! has to be a lib. That is also why this is its own crate instead of a
//! feature on `peerline`: every consumer is a dev target, so keeping it here
//! leaves the protocol crate with no test code in its public API or its
//! published tarball, and no runtime dependency.
//!
//! The runners:
//!
//! - `tests/matrix.rs` runs both halves in-process, once per transport, so
//!   drift *between transports* fails a test;
//! - the `conformance_server` binary serves [`register`] over WebSocket, so a
//!   **TypeScript** client can run the ported battery against it;
//! - the `conformance_client` binary runs [`battery`] against any peerline
//!   server — in particular the TypeScript one — which is the other half of
//!   the cross-language interop harness (`just interop`);
//! - downstream crates that build on peerline (a service host, say) can
//!   dev-depend on this crate and check themselves against the same battery.
//!
//! This is the *behavioural* half of the conformance story; the static half
//! is `vectors.json` at the repository root, which pins frame-level accept/reject
//! across implementations.
//!
//! The `ts/battery.ts` port mirrors this file case for case;
//! keep the two in step, and keep every op name here stable — they are the
//! interop wire contract.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use futures::stream::StreamExt;
use peerline::pubsub::SubscriptionAck;
use peerline::runtime::{Error, Peer, StreamReceiver, Subscription};
use peerline::wire::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Concurrent in-flight requests used by the multiplexing check. Both sides
/// must agree: the `barrier` op only releases once this many calls are
/// parked on it.
pub const CONCURRENCY: usize = 8;

#[derive(Serialize, Deserialize)]
struct Echo {
    s: String,
}
#[derive(Serialize, Deserialize)]
struct Add {
    a: i64,
    b: i64,
}
#[derive(Serialize, Deserialize)]
struct N {
    n: u32,
}
#[derive(Serialize, Deserialize)]
struct Idx {
    i: u32,
}
/// Names one application-chosen pubsub fan-out group. The key never
/// reaches the wire as such — it is an argument to the *application's*
/// subscribe op, which is the whole point of the convention reserving only
/// the push ops.
#[derive(Serialize, Deserialize)]
struct Key {
    key: String,
}
#[derive(Serialize, Deserialize)]
struct KeyN {
    key: String,
    n: u32,
}
/// Args of `sub.replay_on`: give a key a replay window this deep.
#[derive(Serialize, Deserialize)]
struct KeyCap {
    key: String,
    capacity: usize,
}
/// Args of `sub.open_from`: subscribe, resuming after this seq. `after` is
/// absent for "from the beginning of the key's stream" — and being an
/// ordinary optional field of an *application* op is the point: the cursor
/// rides the app's own subscribe, which is what the convention reserves the
/// push ops in order to leave room for.
#[derive(Serialize, Deserialize)]
struct KeyAfter {
    key: String,
    #[serde(default)]
    after: Option<u64>,
}
/// One published event. Typed on both ends, so a payload that arrived
/// mis-shaped fails the decode rather than the assertion.
#[derive(Debug, Serialize, Deserialize)]
struct Ev {
    i: u32,
}

fn rpc_err(e: impl std::fmt::Display) -> RpcError {
    RpcError {
        code: -32000,
        message: e.to_string(),
        data: None,
    }
}

/// The server-side handler set. Register it on a freshly-connected peer;
/// the state it needs (notification counter, barrier) is per-connection.
pub fn register(peer: &Peer) {
    peer.on_request("echo", |e: Echo| async move { Ok::<_, RpcError>(e.s) });
    peer.on_request("add", |a: Add| async move { Ok::<_, RpcError>(a.a + a.b) });
    peer.on_request("fail", |_: serde_json::Value| async move {
        Err::<(), _>(RpcError {
            code: -32050,
            message: "boom".into(),
            data: None,
        })
    });
    // Same, but carrying `data` — the field is part of the protocol and both
    // runtimes must forward it verbatim (it used to be dropped in Rust).
    peer.on_request("fail_data", |_: serde_json::Value| async move {
        Err::<(), _>(RpcError {
            code: -32052,
            message: "boom".into(),
            data: Some(json!({ "hint": "data" })),
        })
    });

    // Notification sink + a request that reads the accumulated count.
    let counter = Arc::new(AtomicI64::new(0));
    let c = counter.clone();
    peer.on_notification("note", move |n: N| {
        let c = c.clone();
        async move {
            c.fetch_add(n.n as i64, Ordering::SeqCst);
        }
    });
    let c = counter.clone();
    peer.on_request("count", move |_: serde_json::Value| {
        let c = c.clone();
        async move { Ok::<_, RpcError>(c.load(Ordering::SeqCst)) }
    });

    // Streaming: N items, then a stream that errors mid-flight.
    peer.on_stream_request("count_stream", |q: N, sender| async move {
        for i in 0..q.n {
            sender.send_item(&i).map_err(rpc_err)?;
        }
        Ok(())
    });
    peer.on_stream_request("boom_stream", |_: serde_json::Value, sender| async move {
        sender.send_item(&"first").map_err(rpc_err)?;
        Err(RpcError {
            code: -32051,
            message: "mid-stream".into(),
            data: None,
        })
    });

    // Concurrency: every in-flight call must reach the barrier before any
    // returns — proves the transport multiplexes concurrent requests rather
    // than serialising them (no wall-clock assertion involved).
    let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
    let b = barrier.clone();
    peer.on_request("barrier", move |idx: Idx| {
        let b = b.clone();
        async move {
            b.wait().await;
            Ok::<_, RpcError>(idx.i)
        }
    });

    // Peer symmetry: the server calls back into the client mid-request.
    let p = peer.clone();
    peer.on_request("reverse", move |e: Echo| {
        let p = p.clone();
        async move {
            let r: String = p.call("client_echo", &e).await.map_err(rpc_err)?;
            Ok::<_, RpcError>(r)
        }
    });

    peer.on_request("big", |e: Echo| async move { Ok::<_, RpcError>(e.s) });

    // -----------------------------------------------------------------------
    // Pubsub, through the runtime layer
    // -----------------------------------------------------------------------
    //
    // The subscribe op is deliberately application-named — the convention
    // reserves only the `$peerline/pubsub.*` push ops, so "what it takes to
    // open a subscription" stays the application's contract. Everything else
    // here goes through `Publisher`, never through hand-built frames: that is
    // what makes this a check of the two *runtime layers* against each other
    // rather than of two test helpers.
    //
    // `sub.count` is what lets the client assert pruning. A consumer that
    // unsubscribes (or simply drops the handle) sends one fire-and-forget
    // request and can never otherwise see whether the producer acted on it;
    // asking the producer is the only honest way to pin "stops costing the
    // producer anything".
    let publisher = peer.publisher();
    let p = publisher.clone();
    peer.on_request("sub.open", move |k: Key| {
        let p = p.clone();
        // `subscribe` returns `Result<_, RpcError>` because a Rust peer may
        // be at `max_subscriptions`; returning it as-is is the documented
        // shape for an application subscribe handler.
        async move { p.subscribe(k.key) }
    });
    let p = publisher.clone();
    peer.on_request("sub.emit", move |q: KeyN| {
        let p = p.clone();
        async move {
            let mut sent = 0usize;
            for i in 0..q.n {
                sent += p.publish(&q.key, &Ev { i }).map_err(rpc_err)?;
            }
            Ok::<_, RpcError>(sent)
        }
    });
    let p = publisher.clone();
    peer.on_request("sub.end_key", move |k: Key| {
        let p = p.clone();
        async move { Ok::<_, RpcError>(p.end_key(&k.key)) }
    });
    let p = publisher.clone();
    peer.on_request("sub.count", move |k: Key| {
        let p = p.clone();
        async move { Ok::<_, RpcError>(p.subscribers(&k.key)) }
    });

    // Replay. `sub.open_from` is the second subscribe op the convention's
    // shape demands: the cursor is an argument to the *application's* op, so
    // an implementation that reserved a field for it would already be wrong.
    // `sub.replay_on` exists because the window is a producer-side decision a
    // client cannot otherwise make on a server it did not configure — and the
    // forced-roll case needs a window of exactly two.
    let p = publisher.clone();
    peer.on_request("sub.replay_on", move |q: KeyCap| {
        let p = p.clone();
        async move { Ok::<_, RpcError>(p.enable_replay(&q.key, q.capacity)) }
    });
    let p = publisher.clone();
    peer.on_request("sub.open_from", move |q: KeyAfter| {
        let p = p.clone();
        async move { p.subscribe_from(&q.key, q.after) }
    });

    // -----------------------------------------------------------------------
    // Kind mismatch
    // -----------------------------------------------------------------------
    //
    // The wire carries no "I want a stream" marker: whether a request is
    // answered with one `resp` or a run of `stream` frames is decided
    // entirely by which handler the *responder* registered. Each runtime
    // resolves the mismatch locally, in the caller — so pinning it across the
    // wire is really pinning that the *counterpart* emits the frames that
    // resolution expects.
    //
    // `kind_stream` parks on the cancel rather than returning, because a
    // caller that used `call` is by then gone: its runtime sends the stream
    // cancel, and waiting for it keeps a terminal from racing the mismatch.
    peer.on_stream_request("kind_stream", |_: serde_json::Value, sender| async move {
        sender.send_item(&"item").map_err(rpc_err)?;
        sender.cancelled().await;
        Ok(())
    });
    peer.on_request("kind_unary", |_: serde_json::Value| async move {
        Ok::<_, RpcError>("unary")
    });

    // -----------------------------------------------------------------------
    // Stream cancel, observed from the producing end
    // -----------------------------------------------------------------------
    //
    // The consumer half of a cancel is invisible to the consumer: it drops a
    // handle and, by construction, hears nothing back. So the producer
    // records what it saw and `cancel_report` hands it over on the next call.
    // `refused` is the half that matters most — after the cancel the
    // producer's outbound stream is closed, which is why *no terminal is
    // delivered*: the same gate that refuses this late item drops the
    // handler's own end-of-stream on the floor.
    let cancel_seen = Arc::new(AtomicBool::new(false));
    let late_refused = Arc::new(AtomicBool::new(false));
    let (seen, refused) = (cancel_seen.clone(), late_refused.clone());
    peer.on_stream_request("slow_stream", move |_: serde_json::Value, sender| {
        let (seen, refused) = (seen.clone(), refused.clone());
        async move {
            sender.send_item(&"tick").map_err(rpc_err)?;
            sender.cancelled().await;
            // Store `refused` first: `seen` is what the client polls on, so
            // ordering it last means a report that says "observed" always
            // carries a settled `refused`.
            refused.store(sender.send_item(&"late").is_err(), Ordering::SeqCst);
            seen.store(true, Ordering::SeqCst);
            Ok(())
        }
    });
    let (seen, refused) = (cancel_seen.clone(), late_refused.clone());
    peer.on_request("cancel_report", move |_: serde_json::Value| {
        let (seen, refused) = (seen.clone(), refused.clone());
        async move {
            Ok::<_, RpcError>(json!({
                "observed": seen.load(Ordering::SeqCst),
                "refused": refused.load(Ordering::SeqCst),
            }))
        }
    });

    // -----------------------------------------------------------------------
    // Malformed args
    // -----------------------------------------------------------------------
    //
    // A typed Rust handler rejects args of the wrong shape with `-32602`
    // before it is ever entered — `serde_json::from_value` does it. TypeScript
    // erases its types, so the TS half of this op registers the equivalent
    // `opts.parse` validator explicitly. That asymmetry *is* the case: both
    // servers must answer the same code to the same bad frame.
    peer.on_request("strict_add", |a: Add| async move {
        Ok::<_, RpcError>(a.a + a.b)
    });

    // -----------------------------------------------------------------------
    // Error paths
    // -----------------------------------------------------------------------
    //
    // A handler that fails in a way that is not already an `RpcError`. Rust
    // handlers return `Result<_, RpcError>`, so there is no non-RPC value to
    // throw and the spelling is an explicit `internal`; TypeScript throws a
    // plain `Error` and its runtime maps it. Same code, same message, so the
    // battery can assert both.
    peer.on_request("throw_plain", |_: serde_json::Value| async move {
        Err::<(), _>(RpcError::internal("plain failure"))
    });
    // `fail_data` above pins `data` on a *response*; this pins it on a stream
    // error terminal, which is a different frame and a different code path.
    peer.on_stream_request(
        "boom_stream_data",
        |_: serde_json::Value, sender| async move {
            sender.send_item(&"first").map_err(rpc_err)?;
            Err(RpcError {
                code: -32053,
                message: "mid-stream".into(),
                data: Some(json!({ "hint": "stream" })),
            })
        },
    );

    // The truncation pair (see `truncation_battery`): work that is still
    // outstanding when the connection dies. Neither ever settles on its own,
    // so the only way a consumer sees either end is the connection going
    // away — which is exactly the case being pinned.
    peer.on_request("hang", |_: serde_json::Value| async move {
        futures::future::pending::<()>().await;
        Ok::<_, RpcError>(())
    });
    peer.on_stream_request("hang_stream", |_: serde_json::Value, sender| async move {
        sender.send_item(&"open").map_err(rpc_err)?;
        futures::future::pending::<()>().await;
        Ok(())
    });
}

/// The `crash` sink: on notification, **exit this process**, dropping the
/// connection with the `hang` / `hang_stream` work above still outstanding.
///
/// Registered only by the interop *server runners* — deliberately not by
/// [`register`], which the in-process transport matrix installs on a peer
/// living in the test process. Killing a whole process is the bluntest
/// available "the producer died", and it is honest: peerline has no
/// per-connection close on the server side, and the field failure this pins
/// (a producer dying mid-stream) is precisely a process going away.
///
/// Mirrored by `registerCrash` in `ts/battery.ts`; `crash` is part of the
/// interop wire contract.
pub fn register_crash(peer: &Peer) {
    peer.on_notification("crash", |_: serde_json::Value| async move {
        std::process::exit(0);
    });
}

/// The full client-side battery. `client`'s peer is connected to a server
/// that ran [`register`] (in any language). Panics on the first mismatch.
pub async fn battery(client: &Peer) {
    // The reverse test needs a client-side handler for the server to call.
    client.on_request("client_echo", |e: Echo| async move {
        Ok::<_, RpcError>(format!("echoed:{}", e.s))
    });

    // Unary + typed args.
    let r: String = client
        .call("echo", &Echo { s: "hi".into() })
        .await
        .expect("echo");
    assert_eq!(r, "hi");
    let sum: i64 = client.call("add", &Add { a: 2, b: 40 }).await.expect("add");
    assert_eq!(sum, 42);

    // Error propagation.
    let err = client
        .call::<_, ()>("fail", &json!({}))
        .await
        .expect_err("fail");
    match err {
        Error::Rpc(e) => {
            assert_eq!(e.code, -32050);
            assert_eq!(e.message, "boom");
            assert_eq!(e.data, None);
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }

    // …including an error that carries `data`, which must survive intact.
    let err = client
        .call::<_, ()>("fail_data", &json!({}))
        .await
        .expect_err("fail_data");
    match err {
        Error::Rpc(e) => {
            assert_eq!(e.code, -32052);
            assert_eq!(e.message, "boom");
            assert_eq!(e.data, Some(json!({ "hint": "data" })));
        }
        other => panic!("expected Rpc error, got {other:?}"),
    }

    // Notification delivery (bounded poll — no timing assertion).
    client.notify("note", &N { n: 3 }).expect("notify");
    let mut delivered = false;
    for _ in 0..200 {
        let c: i64 = client.call("count", &json!({})).await.expect("count");
        if c >= 3 {
            delivered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(delivered, "notification was not delivered");

    // Server-streaming: ordered items then clean end.
    let mut stream: StreamReceiver<u32> = client
        .call_stream("count_stream", &N { n: 5 })
        .expect("count_stream");
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item.expect("stream item").data);
    }
    assert_eq!(items, vec![0, 1, 2, 3, 4]);

    // Mid-stream error surfaces after the first item, code and message intact.
    let mut stream: StreamReceiver<String> = client
        .call_stream("boom_stream", &json!({}))
        .expect("boom_stream");
    assert_eq!(
        stream.next().await.expect("first").expect("ok item").data,
        "first"
    );
    match stream.next().await.expect("second") {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.code, -32051);
            assert_eq!(e.message, "mid-stream");
        }
        other => panic!("expected mid-stream Rpc error, got {other:?}"),
    }
    assert!(
        stream.next().await.is_none(),
        "stream ended after the error"
    );

    // Concurrent multiplexing — the barrier deadlocks if serialised, so the
    // timeout is a safety net, not a performance assertion. Args live in a
    // Vec so each in-flight call's borrow outlives the join.
    let idxs: Vec<Idx> = (0..CONCURRENCY as u32).map(|i| Idx { i }).collect();
    let calls = idxs.iter().map(|a| client.call::<_, u32>("barrier", a));
    let mut got = tokio::time::timeout(
        Duration::from_secs(15),
        futures::future::try_join_all(calls),
    )
    .await
    .expect("concurrent calls deadlocked (transport not multiplexing?)")
    .expect("barrier calls");
    got.sort_unstable();
    assert_eq!(got, (0..CONCURRENCY as u32).collect::<Vec<_>>());

    // Peer symmetry: server → client call inside a client → server call.
    let rr: String = client
        .call("reverse", &Echo { s: "x".into() })
        .await
        .expect("reverse");
    assert_eq!(rr, "echoed:x");

    // Large frame near the shared ceiling (1 MiB) round-trips intact.
    let big = "a".repeat(1 << 20);
    let back: String = client
        .call("big", &Echo { s: big.clone() })
        .await
        .expect("big");
    assert_eq!(back.len(), big.len());
    assert_eq!(back, big);

    // -----------------------------------------------------------------------
    // Pubsub: three endings, one subscription each
    // -----------------------------------------------------------------------

    // (a) The producer ends it: a clean completion, which is the control for
    //     (b) and (c) — without it "every subscription just stops" would pass.
    let ack: SubscriptionAck = client
        .call("sub.open", &Key { key: "a".into() })
        .await
        .expect("sub.open a");
    // Registered immediately after the ack, which is the only ordering the
    // wire lets either side rely on.
    let mut sub: Subscription<Ev> = client.subscription(ack.subscription_id);
    let sent: usize = client
        .call(
            "sub.emit",
            &KeyN {
                key: "a".into(),
                n: 3,
            },
        )
        .await
        .expect("sub.emit a");
    assert_eq!(sent, 3, "one subscriber, three events");
    // No polling: the events were queued before the `sub.emit` response, and
    // the wire is ordered, so they are routed by the time that call resolves.
    for i in 0..3u32 {
        let ev = sub.next().await.expect("event").expect("event decoded");
        assert_eq!(ev.i, i, "events arrive in order and typed");
    }
    let ended: usize = client
        .call("sub.end_key", &Key { key: "a".into() })
        .await
        .expect("sub.end_key a");
    assert_eq!(ended, 1);
    assert!(
        sub.next().await.is_none(),
        "the producer's end marker completes the subscription cleanly"
    );

    // (b) The consumer unsubscribes: the producer must prune, which only
    //     the producer can report.
    let ack: SubscriptionAck = client
        .call("sub.open", &Key { key: "b".into() })
        .await
        .expect("sub.open b");
    let mut sub: Subscription<Ev> = client.subscription(ack.subscription_id);
    assert_eq!(subscribers(client, "b").await, 1);
    let sent: usize = client
        .call(
            "sub.emit",
            &KeyN {
                key: "b".into(),
                n: 1,
            },
        )
        .await
        .expect("sub.emit b");
    assert_eq!(sent, 1);
    assert_eq!(
        sub.next().await.expect("event").expect("decoded").i,
        0,
        "a live subscription receives before it is retired"
    );
    sub.unsubscribe();
    await_subscribers(client, "b", 0).await;

    // (c) The consumer just drops the handle — same wire effect, no explicit
    //     call. (TypeScript has no destructor; its port uses the iterator's
    //     `return()`, which is what a `break` out of a `for await` calls.)
    let ack: SubscriptionAck = client
        .call("sub.open", &Key { key: "c".into() })
        .await
        .expect("sub.open c");
    let sub: Subscription<Ev> = client.subscription(ack.subscription_id);
    assert_eq!(subscribers(client, "c").await, 1);
    drop(sub);
    await_subscribers(client, "c", 0).await;

    // -----------------------------------------------------------------------
    // Pubsub: replay from a cursor
    // -----------------------------------------------------------------------
    //
    // The case the seq / gap / resume frames exist for, and the one nothing
    // else here can catch: a consumer that goes away and comes back must be
    // served **exactly** what it missed and then the live stream. Off-by-one
    // at that splice duplicates or holes, and both read as success to a test
    // that only counts events.
    //
    // Every seq is asserted through `last_seq`, which is the consumer's
    // cursor — so this also pins that the two runtimes agree about *which*
    // events a given cursor names, not merely about how many there are.
    assert_eq!(
        client
            .call::<_, usize>(
                "sub.replay_on",
                &json!({ "key": "r", "capacity": 16 })
            )
            .await
            .expect("sub.replay_on r"),
        16
    );

    let ack: SubscriptionAck = client
        .call("sub.open", &Key { key: "r".into() })
        .await
        .expect("sub.open r");
    assert_eq!(
        ack.from_seq, None,
        "an ordinary subscribe replays nothing and says so"
    );
    let mut sub: Subscription<Ev> = client.subscription(ack.subscription_id);
    let sent: usize = client
        .call("sub.emit", &KeyN { key: "r".into(), n: 3 })
        .await
        .expect("sub.emit r");
    assert_eq!(sent, 3);
    for i in 0..3u32 {
        let ev = sub.next().await.expect("event").expect("decoded");
        assert_eq!(ev.i, i);
        assert_eq!(
            sub.last_seq(),
            Some(u64::from(i)),
            "a stamped event advances the cursor"
        );
    }
    let cursor = sub.last_seq();

    // Away. What the producer publishes now is what the resume must return —
    // and the count of 0 proves nobody received it live.
    sub.unsubscribe();
    await_subscribers(client, "r", 0).await;
    let sent: usize = client
        .call("sub.emit", &KeyN { key: "r".into(), n: 2 })
        .await
        .expect("sub.emit r while away");
    assert_eq!(sent, 0, "nobody is subscribed — but the window keeps them");

    // Back, with the cursor riding the application's own subscribe op.
    let ack: SubscriptionAck = client
        .call(
            "sub.open_from",
            &json!({ "key": "r", "after": cursor }),
        )
        .await
        .expect("sub.open_from r");
    assert_eq!(
        ack.from_seq,
        Some(3),
        "the ack says where the stream will pick up, before any event arrives"
    );
    let mut sub: Subscription<Ev> = client.replaying_subscription(ack.subscription_id);
    // Exactly the two that were missed, carrying the seqs they were first
    // published under — `i` restarts per `sub.emit` call, `seq` does not.
    for (i, seq) in [(0u32, 3u64), (1, 4)] {
        let ev = sub.next().await.expect("replayed event").expect("decoded");
        assert_eq!(ev.i, i);
        assert_eq!(sub.last_seq(), Some(seq), "replayed with its original seq");
    }
    // …and then live on the same handle, with no seam.
    let sent: usize = client
        .call("sub.emit", &KeyN { key: "r".into(), n: 1 })
        .await
        .expect("sub.emit r live");
    assert_eq!(sent, 1, "the resumed subscription is a live one now");
    let ev = sub.next().await.expect("live event").expect("decoded");
    assert_eq!(ev.i, 0);
    assert_eq!(sub.last_seq(), Some(5), "one dense sequence across the splice");
    sub.unsubscribe();
    await_subscribers(client, "r", 0).await;

    // -----------------------------------------------------------------------
    // Pubsub: a forced roll is reported as a gap
    // -----------------------------------------------------------------------
    //
    // A window of two under five events, resumed from a cursor three of them
    // behind. What cannot be served must be *said* — in position, before the
    // events that survived, with the range and the reason identical in both
    // languages — and the subscription must continue afterwards. A consumer
    // that could not tell this from a complete history is the failure the
    // whole gap frame exists to prevent, and only a cross-language run can
    // show the two runtimes agreeing about which events are gone.
    assert_eq!(
        client
            .call::<_, usize>("sub.replay_on", &json!({ "key": "g", "capacity": 2 }))
            .await
            .expect("sub.replay_on g"),
        2
    );
    let sent: usize = client
        .call("sub.emit", &KeyN { key: "g".into(), n: 5 })
        .await
        .expect("sub.emit g");
    assert_eq!(sent, 0, "no subscribers yet — this is all window");

    let ack: SubscriptionAck = client
        .call("sub.open_from", &json!({ "key": "g", "after": 0 }))
        .await
        .expect("sub.open_from g");
    assert_eq!(ack.from_seq, Some(3), "seqs 1 and 2 have rolled off");
    let mut sub: Subscription<Ev> = client.replaying_subscription(ack.subscription_id.clone());

    match sub.next().await.expect("the gap, before anything else") {
        Err(Error::Gap(gap)) => {
            assert_eq!(gap.subscription_id, ack.subscription_id);
            assert_eq!(gap.missing_from, 1, "the first seq the cursor asked for");
            assert_eq!(gap.missing_to, 2, "through the last one evicted, inclusive");
            assert_eq!(gap.lost(), 2);
            assert_eq!(gap.reason, "rolled");
        }
        other => panic!("expected a gap first, got {other:?}"),
    }
    // Not an ending: the far side of the hole follows immediately. Here `i`
    // and `seq` coincide, because all five came from one `sub.emit`.
    for seq in [3u64, 4] {
        let ev = sub.next().await.expect("surviving event").expect("decoded");
        assert_eq!(u64::from(ev.i), seq);
        assert_eq!(sub.last_seq(), Some(seq));
    }
    sub.unsubscribe();
    await_subscribers(client, "g", 0).await;

    // -----------------------------------------------------------------------
    // Kind mismatch, in both directions
    // -----------------------------------------------------------------------
    //
    // Both codes *and* both messages are asserted: the two runtimes phrase
    // this identically on purpose, and a consumer telling the two mismatches
    // apart has nothing else to go on.
    let err = client
        .call::<_, serde_json::Value>("kind_stream", &json!({}))
        .await
        .expect_err("a call against a stream handler cannot succeed");
    match err {
        Error::Rpc(e) => {
            assert_eq!(e.code, -32603);
            assert_eq!(
                e.message,
                "responder answered with a stream; use call_stream"
            );
        }
        other => panic!("expected the stream-shape violation, got {other:?}"),
    }

    let mut rx: StreamReceiver<serde_json::Value> = client
        .call_stream("kind_unary", &json!({}))
        .expect("kind_unary");
    match rx.next().await.expect("a terminal, not a silent end") {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.code, -32603);
            assert_eq!(
                e.message,
                "responder answered with a unary response; use call"
            );
        }
        other => panic!("expected the unary-shape violation, got {other:?}"),
    }
    assert!(
        rx.next().await.is_none(),
        "the stream ends after the violation"
    );

    // -----------------------------------------------------------------------
    // Stream cancel
    // -----------------------------------------------------------------------
    let mut rx: StreamReceiver<String> = client
        .call_stream("slow_stream", &json!({}))
        .expect("slow_stream");
    assert_eq!(
        rx.next().await.expect("opened").expect("first item").data,
        "tick",
        "the producer is provably live before the cancel"
    );
    drop(rx);
    // Bounded poll: the cancel is fire-and-forget, so there is no moment the
    // consumer can name at which the producer must already have seen it.
    let mut report = json!(null);
    for _ in 0..200 {
        report = client
            .call("cancel_report", &json!({}))
            .await
            .expect("cancel_report");
        if report["observed"] == json!(true) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        report["observed"],
        json!(true),
        "the producer never observed the cancel"
    );
    assert_eq!(
        report["refused"],
        json!(true),
        "a send after the cancel must be refused — which is also why no \
         terminal is delivered: the same closed stream drops both"
    );

    // -----------------------------------------------------------------------
    // Malformed args
    // -----------------------------------------------------------------------
    let ok: i64 = client
        .call("strict_add", &Add { a: 2, b: 3 })
        .await
        .expect("strict_add");
    assert_eq!(ok, 5, "the validator must not reject good args");
    let err = client
        .call::<_, ()>("strict_add", &json!({ "a": "x", "b": 3 }))
        .await
        .expect_err("strict_add with the wrong arg shape");
    match err {
        // Code only: Rust carries serde's phrasing and TypeScript the
        // validator's, and the cross-language contract is the code.
        Error::Rpc(e) => assert_eq!(e.code, -32602, "malformed args are InvalidParams"),
        other => panic!("expected InvalidParams, got {other:?}"),
    }

    // -----------------------------------------------------------------------
    // Error paths
    // -----------------------------------------------------------------------
    let err = client
        .call::<_, ()>("no_such_op", &json!({}))
        .await
        .expect_err("an unregistered op cannot succeed");
    match err {
        // Message not asserted: the two runtimes name the missing op
        // differently, and only the code is the contract.
        Error::Rpc(e) => assert_eq!(e.code, -32601, "unknown op is MethodNotFound"),
        other => panic!("expected MethodNotFound, got {other:?}"),
    }

    let err = client
        .call::<_, ()>("throw_plain", &json!({}))
        .await
        .expect_err("throw_plain");
    match err {
        Error::Rpc(e) => {
            assert_eq!(e.code, -32603, "a non-RPC handler failure is Internal");
            assert_eq!(e.message, "plain failure");
            assert_eq!(e.data, None, "a synthesized Internal carries no data");
        }
        other => panic!("expected Internal, got {other:?}"),
    }

    // `data` on a stream error terminal, which `fail_data` does not reach.
    let mut rx: StreamReceiver<String> = client
        .call_stream("boom_stream_data", &json!({}))
        .expect("boom_stream_data");
    assert_eq!(
        rx.next().await.expect("first").expect("ok item").data,
        "first"
    );
    match rx.next().await.expect("terminal") {
        Err(Error::Rpc(e)) => {
            assert_eq!(e.code, -32053);
            assert_eq!(e.message, "mid-stream");
            assert_eq!(e.data, Some(json!({ "hint": "stream" })));
        }
        other => panic!("expected the mid-stream error with data, got {other:?}"),
    }
    assert!(rx.next().await.is_none(), "the stream ends after the error");
}

/// Live subscriptions the *producer* is holding on `key`.
async fn subscribers(client: &Peer, key: &str) -> usize {
    client
        .call(
            "sub.count",
            &Key {
                key: key.to_string(),
            },
        )
        .await
        .expect("sub.count")
}

/// Poll [`subscribers`] until it reaches `want`, or fail.
///
/// Bounded rather than instant because unsubscribe is fire-and-forget on the
/// consumer side: the reply exists, but nothing on the consumer holds it, so
/// the only observable is the producer's own count settling.
async fn await_subscribers(client: &Peer, key: &str, want: usize) {
    let mut last = usize::MAX;
    for _ in 0..200 {
        last = subscribers(client, key).await;
        if last == want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("subscribers on {key} never reached {want} (last saw {last})");
}

/// The truncation half of the battery: what a consumer sees when the
/// connection dies with work outstanding.
///
/// Kept out of [`battery`] because it *ends the connection* — it must run
/// last, and only against a server that also ran [`register_crash`] (the
/// interop runners; the in-process matrix runs [`battery`] alone).
///
/// Three things must be true at once, and the first is what keeps the other
/// two honest:
///
/// 1. a stream that reaches its terminal still ends **cleanly**;
/// 2. a stream the connection cuts short ends with an **error**, not with
///    the same clean end — otherwise a truncated stream reads as a complete
///    one, and half a history looks like all of it;
/// 3. a unary call in flight when the connection dies fails as
///    `Error::Abandoned`, not as an `Error::Rpc` a consumer would have to
///    tell from a real remote error by its message text.
///
/// The TypeScript port is `truncationBattery` in `ts/battery.ts`, asserting
/// the same three things in that runtime's spelling (a thrown
/// `AbandonedError` where Rust yields `Err(Error::Abandoned)`).
pub async fn truncation_battery(client: &Peer) {
    // 1. A stream that reaches its terminal ends cleanly — the control for
    //    everything below, without which "every stream errors" would pass.
    let mut complete: StreamReceiver<u32> = client
        .call_stream("count_stream", &N { n: 2 })
        .expect("count_stream");
    let mut items = Vec::new();
    while let Some(item) = complete.next().await {
        items.push(item.expect("no error on a clean stream").data);
    }
    assert_eq!(items, vec![0, 1]);

    // Open the stream that never terminates, and wait for its first item so
    // the producer is provably live before the connection is cut.
    let mut cut: StreamReceiver<String> = client
        .call_stream("hang_stream", &json!({}))
        .expect("hang_stream");
    assert_eq!(
        cut.next()
            .await
            .expect("hang_stream opened")
            .expect("first item")
            .data,
        "open"
    );

    // `join` polls the call first, so it is registered and sent before the
    // `crash` notification below — in flight, exactly as required.
    let (call, ()) = tokio::time::timeout(
        Duration::from_secs(30),
        futures::future::join(
            client.call::<_, serde_json::Value>("hang", &json!({})),
            async {
                client.notify("crash", &json!({})).expect("crash");

                // 2. The truncated stream: an error, and specifically not the
                //    clean end a complete stream gives.
                match cut.next().await {
                    Some(Err(Error::Abandoned)) => {}
                    other => panic!("expected Err(Abandoned) on truncation, got {other:?}"),
                }
                assert!(
                    cut.next().await.is_none(),
                    "the stream should be fused after the truncation error"
                );
            },
        ),
    )
    .await
    .expect("teardown never reached the consumer");

    // 3. The in-flight unary call keeps its own type.
    match call.expect_err("the call cannot succeed") {
        Error::Abandoned => {}
        other => panic!("expected Abandoned for the in-flight call, got {other:?}"),
    }
}
