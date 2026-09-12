//! The conformance battery, run against any peerline server.
//!
//! The other half of the cross-language interop harness: point it at the
//! TypeScript server (`ts/server.mjs`) and the same
//! battery that pins every Rust transport now pins the TS runtime too.
//!
//! The single argument is the address the server announced. A `ws://` or
//! `wss://` url dials WebSocket; anything else is taken as a Unix socket
//! path, which is how this reaches a TypeScript server built on `node:net`.
//!
//! ```sh
//! cargo run -p peerline-conformance --features ws,uds \
//!     --bin conformance_client -- ws://127.0.0.1:1234/
//! cargo run -p peerline-conformance --features ws,uds \
//!     --bin conformance_client -- /tmp/plc.abc/s
//! ```
//!
//! Prints `BATTERY OK` and exits 0 on success. On failure the battery's
//! assertion panics, which prints the mismatch and exits non-zero — the
//! caller only has to check the status (and, if it likes, the line).

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use futures::future::BoxFuture;
use peerline::runtime::Peer;
use peerline_conformance::{battery, truncation_battery};

/// Dial until the server is up, whichever transport the address names.
///
/// Both transports hand back the same `(Peer, driver)` pair, so the only
/// thing the address decides is which `connect` runs — the battery below
/// cannot tell the difference, which is the point.
async fn dial(address: &str) -> Result<(Peer, BoxFuture<'static, ()>), String> {
    let is_ws = address.starts_with("ws://") || address.starts_with("wss://");
    let mut last = String::new();
    for _ in 0..300 {
        let attempt = if is_ws {
            peerline_transport_ws::connect(address)
                .await
                .map_err(|e| e.to_string())
        } else {
            peerline_transport_uds::connect(Path::new(address))
                .await
                .map_err(|e| e.to_string())
        };
        match attempt {
            Ok(c) => return Ok(c),
            Err(e) => {
                last = e;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    Err(format!("dial {address} timed out: {last}"))
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let address = std::env::args()
        .nth(1)
        .ok_or("usage: conformance_client <ws-url|socket-path>")?;

    let (client, driver) = dial(&address).await?;
    let driving = tokio::spawn(driver);

    // `battery` registers the handlers it needs on the client (the reverse
    // echo) and panics on the first mismatch.
    battery(&client).await;

    // Last, because it ends the connection: what a consumer sees when the
    // producing peer dies with a stream open and a call in flight.
    truncation_battery(&client).await;

    println!("BATTERY OK");
    std::io::stdout().flush().map_err(|e| e.to_string())?;

    // Drop the peer, then let the driver observe it and finish. The server
    // is already gone (the truncation battery told it to crash), so this is
    // just tidy shutdown of a connection that has nothing left to say.
    drop(client);
    let _ = tokio::time::timeout(Duration::from_secs(5), driving).await;
    Ok(())
}
