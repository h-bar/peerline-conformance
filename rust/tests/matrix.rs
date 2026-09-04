//! One conformance battery, run against every transport.
//!
//! Each transport is wrapped in a small [`Harness`] that stands up a server
//! registering the same [`register`] handler-set and hands back an
//! [`Endpoint`] you can dial. The identical [`battery`] then exercises the
//! full peerline surface over that connection — so any behavioural drift
//! between transports fails a test.
//!
//! Both halves live in this crate's library root rather than here, so the
//! interop binaries — and through them the TypeScript harness — run this
//! exact code. Downstream crates that build on peerline check themselves
//! against the same battery the same way.
//!
//! The `perf` test (ignored by default) reuses the same harnesses to run
//! identical workloads across transports and prints a comparison table:
//!
//! ```text
//! cargo test -p peerline-conformance --release -- --ignored --nocapture perf
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use peerline::runtime::Peer;
use peerline_conformance::{battery, register};
use serde_json::json;

type PeerHandler = Arc<dyn Fn(&Peer) + Send + Sync + 'static>;
type Connected = (Peer, BoxFuture<'static, ()>);

// ---------------------------------------------------------------------------
// Transport harnesses
// ---------------------------------------------------------------------------

/// A running server that can be dialled repeatedly.
trait Endpoint: Send + Sync {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>>;
}

/// A transport under test: stands up a server, hands back an [`Endpoint`].
trait Harness: Send + Sync {
    fn name(&self) -> &'static str;
    fn start(&self, register: PeerHandler)
    -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>>;
}

static SEQ: AtomicU64 = AtomicU64::new(0);

fn tmp(ext: &str) -> PathBuf {
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "peerline-conf-{}-{}.{}",
        std::process::id(),
        n,
        ext
    ))
}

async fn dial_retry<F, Fut>(mut dial: F) -> Result<Connected, String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Connected, String>>,
{
    for _ in 0..300 {
        if let Ok(c) = dial().await {
            return Ok(c);
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Err("dial timed out".into())
}

// --- in-process loopback (baseline: the same battery with no transport) ---
struct Loopback;
impl Harness for Loopback {
    fn name(&self) -> &'static str {
        "loopback"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            // Reuse peerline's own in-process pair (as `tests/loopback.rs`
            // does): register the handler set on the server peer, spawn the
            // shared driver, hand back the client peer.
            let (client, server, driver) = peerline::runtime::loopback();
            reg(&server);
            tokio::spawn(driver);
            Ok(Box::new(LoopbackEp {
                client,
                _server: server,
            }) as Box<dyn Endpoint>)
        })
    }
}
struct LoopbackEp {
    client: Peer,
    // Held so the in-process pair (and its handlers) stay alive.
    _server: Peer,
}
impl Endpoint for LoopbackEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        let client = self.client.clone();
        Box::pin(async move { Ok((client, Box::pin(async {}) as BoxFuture<'static, ()>)) })
    }
}

// --- UDS ---
struct Uds;
impl Harness for Uds {
    fn name(&self) -> &'static str {
        "uds"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let path = tmp("sock");
            let p = path.clone();
            tokio::spawn(async move {
                let _ = peerline_transport_uds::serve(&p, move |peer| reg(peer)).await;
            });
            Ok(Box::new(UdsEp { path }) as Box<dyn Endpoint>)
        })
    }
}
struct UdsEp {
    path: PathBuf,
}
impl Endpoint for UdsEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        Box::pin(dial_retry(move || {
            peerline_transport_uds::connect(&self.path)
        }))
    }
}

// --- WS ---
struct Ws;
impl Harness for Ws {
    fn name(&self) -> &'static str {
        "ws"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let addr = free_addr().await?;
            tokio::spawn(async move {
                let _ = peerline_transport_ws::serve(addr, move |peer| reg(peer)).await;
            });
            Ok(Box::new(WsEp {
                url: format!("ws://{addr}"),
            }) as Box<dyn Endpoint>)
        })
    }
}
struct WsEp {
    url: String,
}
impl Endpoint for WsEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        Box::pin(dial_retry(move || {
            peerline_transport_ws::connect(&self.url)
        }))
    }
}

async fn free_addr() -> Result<std::net::SocketAddr, String> {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let addr = probe.local_addr().map_err(|e| e.to_string())?;
    drop(probe);
    Ok(addr)
}

// --- iroh ---
struct Iroh;
impl Harness for Iroh {
    fn name(&self) -> &'static str {
        "iroh"
    }
    fn start(&self, reg: PeerHandler) -> BoxFuture<'static, Result<Box<dyn Endpoint>, String>> {
        Box::pin(async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let alpn: Vec<u8> = b"peerline/conformance/1".to_vec();
            let a = alpn.clone();
            let key = peerline_transport_iroh::SecretKey::generate();
            tokio::spawn(async move {
                let _ = peerline_transport_iroh::serve(
                    &a,
                    key,
                    peerline_transport_iroh::IrohConfig::default(),
                    move |t| {
                        let _ = tx.send(t.to_string());
                    },
                    move |peer| reg(peer),
                )
                .await;
            });
            let ticket = tokio::time::timeout(Duration::from_secs(20), rx)
                .await
                .map_err(|_| "iroh ticket timeout".to_string())?
                .map_err(|_| "iroh serve exited before binding".to_string())?;
            Ok(Box::new(IrohEp { ticket, alpn }) as Box<dyn Endpoint>)
        })
    }
}
struct IrohEp {
    ticket: String,
    alpn: Vec<u8>,
}
impl Endpoint for IrohEp {
    fn connect(&self) -> BoxFuture<'_, Result<Connected, String>> {
        Box::pin(dial_retry(move || {
            peerline_transport_iroh::connect(
                &self.ticket,
                &self.alpn,
                peerline_transport_iroh::DialMode::Auto,
            )
        }))
    }
}

/// All harnesses compiled into this build.
#[allow(clippy::vec_init_then_push)] // pushes are cfg-gated per transport
fn harnesses() -> Vec<Box<dyn Harness>> {
    let mut v: Vec<Box<dyn Harness>> = Vec::new();
    v.push(Box::new(Loopback)); // baseline
    v.push(Box::new(Uds));
    v.push(Box::new(Ws));
    v.push(Box::new(Iroh));
    v
}

async fn run(h: &dyn Harness) {
    let ep = h.start(Arc::new(register)).await.expect("start server");
    let (client, driver) = ep.connect().await.expect("connect");
    tokio::spawn(driver);
    battery(&client).await;
}

#[tokio::test]
async fn conformance_loopback() {
    run(&Loopback).await;
}

#[tokio::test]
async fn conformance_uds() {
    run(&Uds).await;
}

#[tokio::test]
async fn conformance_ws() {
    run(&Ws).await;
}

#[tokio::test]
async fn conformance_iroh() {
    run(&Iroh).await;
}

// ---------------------------------------------------------------------------
// Performance comparison (ignored; run explicitly)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "perf comparison; run with --release --ignored --nocapture"]
async fn perf() {
    const SEQ_CALLS: usize = 2_000; // latency
    const CONC_CALLS: usize = 5_000; // throughput
    const BLOB_CALLS: usize = 300; // bandwidth
    let blob = "x".repeat(256 * 1024); // 256 KiB

    println!(
        "\n{:<10} {:>12} {:>14} {:>14}",
        "transport", "latency(µs)", "throughput/s", "bandwidth(MiB/s)"
    );
    println!("{}", "-".repeat(52));

    for h in harnesses() {
        let ep = h.start(Arc::new(register)).await.expect("start");
        let (client, driver) = ep.connect().await.expect("connect");
        tokio::spawn(driver);

        // Warm up.
        let _: String = client.call("echo", &json!({ "s": "warm" })).await.unwrap();

        // Sequential latency.
        let t = Instant::now();
        for _ in 0..SEQ_CALLS {
            let _: String = client.call("echo", &json!({ "s": "p" })).await.unwrap();
        }
        let latency_us = t.elapsed().as_secs_f64() * 1e6 / SEQ_CALLS as f64;

        // Concurrent throughput. One shared arg so each in-flight call's
        // borrow outlives the join.
        let arg = json!({ "s": "p" });
        let t = Instant::now();
        let calls = (0..CONC_CALLS).map(|_| client.call::<_, String>("echo", &arg));
        futures::future::try_join_all(calls).await.unwrap();
        let throughput = CONC_CALLS as f64 / t.elapsed().as_secs_f64();

        // Large-frame bandwidth.
        let t = Instant::now();
        for _ in 0..BLOB_CALLS {
            let _: String = client.call("big", &json!({ "s": blob })).await.unwrap();
        }
        let mib = (BLOB_CALLS * blob.len()) as f64 / (1024.0 * 1024.0);
        let bandwidth = mib / t.elapsed().as_secs_f64();

        println!(
            "{:<10} {:>12.1} {:>14.0} {:>14.1}",
            h.name(),
            latency_us,
            throughput,
            bandwidth
        );
    }
    println!();
}
