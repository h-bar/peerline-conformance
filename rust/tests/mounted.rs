//! Multi-service mounting, per transport.
//!
//! The conformance matrix stands up one handler-set per transport, so it only
//! ever exercises the single-service `serve(...)` path. `serve_mounted(...)`
//! is a different code path — several services behind one listener, addressed
//! by WebSocket path or by socket file — and nothing else covers it.
//!
//! The check is the same in both cases: two services whose ops collide by
//! name (`echo`) stay isolated, each mount answering with its own tag.

use std::sync::Arc;
use std::time::Duration;

use peerline::runtime::Peer;
use peerline::wire::RpcError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct EchoArgs {
    s: String,
}

/// A mount whose `echo` prefixes its own tag, so the reply names the service
/// that answered.
macro_rules! tagged_mount {
    ($handler_ty:ty, $tag:expr) => {{
        let tag: &'static str = $tag;
        let h: $handler_ty = Arc::new(move |peer: &Peer| {
            peer.on_request("echo", move |a: EchoArgs| async move {
                Ok::<_, RpcError>(format!("{tag}:{}", a.s))
            });
        });
        h
    }};
}

#[tokio::test]
async fn ws_mounted_paths_route_to_distinct_services() {
    use peerline_transport_ws::{PeerHandler, connect, serve_mounted};

    // Take a port from the OS, then hand the same address to the server. The
    // drop-to-rebind window is covered by the dial retry below.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("probe bind");
    let addr = probe.local_addr().expect("local_addr");
    drop(probe);

    let server = tokio::spawn(async move {
        serve_mounted(
            addr,
            vec![
                ("/a".into(), tagged_mount!(PeerHandler, "A")),
                ("/b".into(), tagged_mount!(PeerHandler, "B")),
            ],
        )
        .await
        .expect("serve_mounted");
    });

    let dial = |path: &str| {
        let url = format!("ws://{addr}{path}");
        async move {
            for _ in 0..100 {
                if let Ok(pair) = connect(&url).await {
                    return pair;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("connect {url} timed out");
        }
    };

    let (peer_a, driver_a) = dial("/a").await;
    tokio::spawn(driver_a);
    let (peer_b, driver_b) = dial("/b").await;
    tokio::spawn(driver_b);

    let a: String = peer_a
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call a");
    let b: String = peer_b
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call b");
    assert_eq!(a, "A:hi");
    assert_eq!(b, "B:hi");

    server.abort();
}

#[tokio::test]
async fn uds_mounted_sockets_route_to_distinct_services() {
    use peerline_transport_uds::{PeerHandler, connect, serve_mounted};

    let dir = std::env::temp_dir();
    let pid = std::process::id();
    let path_a = dir.join(format!("peerline-conf-uds-{pid}-a.sock"));
    let path_b = dir.join(format!("peerline-conf-uds-{pid}-b.sock"));

    let (sa, sb) = (path_a.clone(), path_b.clone());
    let server = tokio::spawn(async move {
        serve_mounted(vec![
            (sa, tagged_mount!(PeerHandler, "A")),
            (sb, tagged_mount!(PeerHandler, "B")),
        ])
        .await
        .expect("serve_mounted");
    });

    // `serve` unlinks and binds asynchronously, so the socket may not exist
    // for the first attempt.
    let dial = |path: std::path::PathBuf| async move {
        for _ in 0..100 {
            if let Ok(pair) = connect(&path).await {
                return pair;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("connect {} timed out", path.display());
    };

    let (peer_a, driver_a) = dial(path_a.clone()).await;
    tokio::spawn(driver_a);
    let (peer_b, driver_b) = dial(path_b.clone()).await;
    tokio::spawn(driver_b);

    let a: String = peer_a
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call a");
    let b: String = peer_b
        .call("echo", &EchoArgs { s: "hi".into() })
        .await
        .expect("call b");
    assert_eq!(a, "A:hi");
    assert_eq!(b, "B:hi");

    server.abort();
    let _ = tokio::fs::remove_file(&path_a).await;
    let _ = tokio::fs::remove_file(&path_b).await;
}
