//! The conformance handler-set, served over WebSocket on a loopback port.
//!
//! Half of the cross-language interop harness: this is the Rust *server*
//! the TypeScript client (`conformance/ts/client.mjs`) runs
//! against. It binds an OS-assigned port on 127.0.0.1, prints exactly one
//! machine-readable line once the port is accepting connections
//!
//! ```text
//! LISTENING ws://127.0.0.1:<port>/
//! ```
//!
//! and then serves until it is killed.
//!
//! ```sh
//! cargo run -p peerline-transport-ws --features conformance --example conformance_server
//! ```

use std::io::Write;
use std::net::SocketAddr;
use std::time::Duration;

use peerline_conformance::register;

#[tokio::main]
async fn main() -> Result<(), String> {
    // Take a port from the OS, then hand the same address to the server.
    // The window between the probe closing and the server binding is
    // covered by the readiness poll below, which is what the LISTENING
    // line actually promises.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("probe bind: {e}"))?;
    let addr: SocketAddr = probe.local_addr().map_err(|e| e.to_string())?;
    drop(probe);

    let serving = tokio::spawn(async move { peerline_transport_ws::serve(addr, register).await });

    // Announce only once a TCP connection actually succeeds, so a client
    // that dials the moment it reads the line can't lose the race.
    for _ in 0..600 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            println!("LISTENING ws://{addr}/");
            std::io::stdout().flush().map_err(|e| e.to_string())?;
            return serving.await.map_err(|e| format!("serve task: {e}"))?;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Err(format!("ws server never came up on {addr}"))
}
