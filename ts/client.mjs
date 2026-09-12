/**
 * The TypeScript half of the interop harness: the battery, run as a client.
 *
 * Mirrors `rust/src/bin/conformance_client.rs` — dials the address
 * given as `argv[2]`, runs the shared {@link battery} against whatever is
 * serving there (in practice the Rust `conformance_server`), and prints
 *
 * ```text
 * BATTERY OK
 * ```
 *
 * on success. On failure the battery's assertion throws, which prints the
 * mismatch and exits non-zero — so a caller only has to check the status.
 *
 * A `ws://` or `wss://` address dials the WebSocket adapter; anything else is
 * a Unix socket path and dials {@link connectNodeSocket}, which is the leg
 * that puts this package's newline framing in front of the Rust
 * `peerline-transport-uds`. The battery cannot tell the two apart, which is
 * the point of running it over both.
 *
 * The two directions are symmetric: each is one long-lived server and one
 * short-lived client, one per language, and `just interop` pairs them both
 * ways. Deliberately a plain script rather than a vitest spec so that both
 * directions are driven identically.
 *
 * Plain `.mjs`, run by `node` with no loader: the runtime comes from the
 * built `dist/` — Node can't load `src/` directly, since the sources import
 * each other with the `.js` specifiers TypeScript wants — and the battery
 * comes from the shared `battery.ts`, which Node type-strips on import. The
 * node-socket adapter lives on the `dist/node.js` entry point rather than
 * `dist/index.js`, deliberately: it imports `node:net`, which is kept out of
 * anything a browser bundle can reach.
 */

import { WebSocket } from "ws";

import { connectWebSocket } from "../../peerline-ts/dist/index.js";
import { connectNodeSocket } from "../../peerline-ts/dist/node.js";
import { battery, truncationBattery } from "./battery.ts";

const address = process.argv[2];
if (!address) {
  console.error("usage: node client.mjs <ws-url|socket-path>");
  process.exit(2);
}

const isWebSocket = address.startsWith("ws://") || address.startsWith("wss://");

// Dial retry: the server may still be binding when we start. Each attempt
// needs a fresh socket — neither a `ws` WebSocket nor a `net.Socket` that
// failed to open can be reused.
let connection;
let last;
for (let attempt = 0; attempt < 300; attempt += 1) {
  try {
    connection = isWebSocket
      ? await connectWebSocket(new WebSocket(address))
      : await connectNodeSocket(address);
    break;
  } catch (thrown) {
    last = thrown;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}
if (!connection) {
  console.error(`dial ${address} timed out: ${String(last)}`);
  process.exit(1);
}

const { peer, closed } = connection;

// `battery` registers the handlers it needs on this peer (the reverse echo)
// and throws on the first mismatch.
await battery(peer);

// Last, because it ends the connection: what a consumer sees when the
// producing peer dies with a stream open and a call in flight.
await truncationBattery(peer);

console.log("BATTERY OK");

// Close down. The server is already gone (the truncation battery told it to
// crash), so this is just tidy shutdown of a connection with nothing left to
// say — `closed` has resolved already.
peer.close();
await closed;
process.exit(0);
