//! The conformance handler-set, served over WebSocket or a Unix socket.
//!
//! Half of the cross-language interop harness: this is the Rust *server*
//! the TypeScript client (`ts/client.mjs`) runs
//! against. It prints exactly one machine-readable line once it is accepting
//! connections
//!
//! ```text
//! LISTENING ws://127.0.0.1:<port>/
//! LISTENING /tmp/plc.XXXXXX/s          # with a path argument
//! ```
//!
//! and then serves until it is killed — by the harness, or by the `crash`
//! notification the client sends to end the run (see
//! `peerline_conformance::register_crash`).
//!
//! With no argument it binds an OS-assigned port on 127.0.0.1 and serves
//! WebSocket. Given a filesystem path it serves
//! `peerline-transport-uds` there instead, which is the leg that puts this
//! transport's newline framing in front of the TypeScript `node:net` adapter:
//!
//! ```sh
//! cargo run -p peerline-conformance --features ws,uds --bin conformance_server
//! cargo run -p peerline-conformance --features ws,uds --bin conformance_server -- /tmp/plc.abc/s
//! ```
//!
//! One binary rather than one per transport so that `_interop` spawns the
//! same program for every leg: only the address on the LISTENING line, and
//! the argument the client is handed, differ.

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use peerline_conformance::{register, register_crash};

/// Install the handler set on each accepted peer. Shared by both transports
/// so a leg cannot accidentally serve a different surface.
fn accept(peer: &peerline::runtime::Peer) {
    register(peer);
    // The interop client's last act is a `crash` notification, which kills
    // this process mid-stream. Only the runners install it; see
    // `register_crash`.
    register_crash(peer);
}

/// Print the readiness line exactly once, flushed — the harness greps for it.
fn announce(address: &str) -> Result<(), String> {
    println!("LISTENING {address}");
    std::io::stdout().flush().map_err(|e| e.to_string())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    match std::env::args().nth(1) {
        Some(path) => serve_uds(PathBuf::from(path)).await,
        None => serve_ws().await,
    }
}

async fn serve_ws() -> Result<(), String> {
    // Take a port from the OS, then hand the same address to the server.
    // The window between the probe closing and the server binding is
    // covered by the readiness poll below, which is what the LISTENING
    // line actually promises.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("probe bind: {e}"))?;
    let addr: SocketAddr = probe.local_addr().map_err(|e| e.to_string())?;
    drop(probe);

    let serving = tokio::spawn(async move {
        peerline_transport_ws::serve(
            addr,
            peerline_transport_ws::WsPolicy::allow_any().acceptor(accept),
        )
        .await
    });

    // Announce only once a TCP connection actually succeeds, so a client
    // that dials the moment it reads the line can't lose the race.
    for _ in 0..600 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            announce(&format!("ws://{addr}/"))?;
            return serving.await.map_err(|e| format!("serve task: {e}"))?;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("ws server never came up on {addr}"))
}

async fn serve_uds(path: PathBuf) -> Result<(), String> {
    let listening = path.clone();
    let serving = tokio::spawn(async move {
        // `allow_any` rather than the default `same_user`: the readiness poll
        // below dials the socket itself, and the leg runs both peers as the
        // same user anyway, so peer-credential filtering would only add a way
        // for this harness to fail for reasons the battery is not about. The
        // socket is still published `0600` by the transport.
        peerline_transport_uds::serve(
            &path,
            peerline_transport_uds::UdsPolicy::allow_any().acceptor(accept),
        )
        .await
    });

    // `serve` binds to a staging name and renames, so the path existing is
    // not enough — connecting is. Same promise the WS branch makes.
    for _ in 0..600 {
        if connectable(&listening).await {
            announce(&listening.to_string_lossy())?;
            return serving.await.map_err(|e| format!("serve task: {e}"))?;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!(
        "uds server never came up on {}",
        listening.display()
    ))
}

async fn connectable(path: &Path) -> bool {
    tokio::net::UnixStream::connect(path).await.is_ok()
}
