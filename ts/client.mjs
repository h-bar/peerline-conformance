/**
 * The TypeScript half of the interop harness: the battery, run as a client.
 *
 * Mirrors `rust/src/bin/conformance_client.rs` — dials the URL
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
 * The two directions are symmetric: each is one long-lived server and one
 * short-lived client, one per language, and `just interop` pairs them both
 * ways. Deliberately a plain script rather than a vitest spec so that both
 * directions are driven identically.
 *
 * Plain `.mjs`, run by `node` with no loader: the runtime comes from the
 * built `dist/` — Node can't load `src/` directly, since the sources import
 * each other with the `.js` specifiers TypeScript wants — and the battery
 * comes from the shared `battery.ts`, which Node type-strips on import.
 */

import { WebSocket } from "ws";

import { connectWebSocket } from "../../peerline-ts/dist/index.js";
import { battery } from "./battery.ts";

const url = process.argv[2];
if (!url) {
  console.error("usage: node client.mjs <ws-url>");
  process.exit(2);
}

// Dial retry: the server may still be binding when we start. Each attempt
// needs a fresh socket — a `ws` WebSocket that failed to open cannot be
// reused.
let connection;
let last;
for (let attempt = 0; attempt < 300; attempt += 1) {
  try {
    connection = await connectWebSocket(new WebSocket(url));
    break;
  } catch (thrown) {
    last = thrown;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}
if (!connection) {
  console.error(`dial ${url} timed out: ${String(last)}`);
  process.exit(1);
}

const { peer, closed } = connection;

// `battery` registers the handlers it needs on this peer (the reverse echo)
// and throws on the first mismatch.
await battery(peer);

console.log("BATTERY OK");

// Close cleanly, then let the driver observe it: the server sees the socket
// go rather than the process vanishing mid-frame.
peer.close();
await closed;
process.exit(0);
