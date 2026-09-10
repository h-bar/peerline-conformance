//! The conformance battery, run against any peerline WebSocket server.
//!
//! The other half of the cross-language interop harness: point it at the
//! TypeScript server (`ts/server.mjs`) and the same
//! battery that pins every Rust transport now pins the TS runtime too.
//!
//! ```sh
//! cargo run -p peerline-conformance --features ws \
//!     --bin conformance_client -- ws://127.0.0.1:1234/
//! ```
//!
//! Prints `BATTERY OK` and exits 0 on success. On failure the battery's
//! assertion panics, which prints the mismatch and exits non-zero — the
//! caller only has to check the status (and, if it likes, the line).

use std::io::Write;
use std::time::Duration;

use peerline_conformance::{battery, truncation_battery};

#[tokio::main]
async fn main() -> Result<(), String> {
    let url = std::env::args()
        .nth(1)
        .ok_or("usage: conformance_client <ws-url>")?;

    // Dial retry: the server may still be binding when we are started.
    let mut last = String::new();
    let mut connected = None;
    for _ in 0..300 {
        match peerline_transport_ws::connect(&url).await {
            Ok(c) => {
                connected = Some(c);
                break;
            }
            Err(e) => {
                last = e.to_string();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let (client, driver) = connected.ok_or(format!("dial {url} timed out: {last}"))?;
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
