//! The shared conformance handler-set and battery.
//!
//! One [`register`] (the server side) and one [`battery`] (the client side)
//! that between them exercise the whole peerline surface: unary calls, typed
//! args, error propagation (with and without `data`), notifications,
//! server-streaming with a clean end and with a mid-stream error, concurrent
//! multiplexing, peer symmetry (server → client call inside a client →
//! server call) and a 1 MiB frame.
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
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use futures::stream::StreamExt;
use peerline::runtime::{Error, Peer, StreamReceiver};
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
