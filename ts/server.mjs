/**
 * The TypeScript half of the interop harness: a peerline server on Node.
 *
 * Mirrors `conformance/rust/src/bin/conformance_server.rs` — a `ws`
 * server on an OS-assigned loopback port, running
 * {@link registerHandlers} on one {@link Peer} per connection, announcing
 *
 * ```text
 * LISTENING ws://127.0.0.1:<port>/
 * ```
 *
 * on stdout and then serving until it is killed. `just interop` starts it,
 * reads that line, and points the Rust `conformance_client` binary at it.
 *
 * Plain `.mjs`, run by `node` with no loader: the accepted socket is handed
 * straight to `connectWebSocket`, whose duck type covers the *server* side
 * of a `ws` connection as happily as the client side (already OPEN, so it
 * resolves immediately). The runtime comes from the built `dist/` — Node
 * can't load `src/` directly, since the sources import each other with the
 * `.js` specifiers TypeScript wants — and the handler set comes from the
 * shared `battery.ts`, which Node type-strips on import.
 */

import { WebSocketServer } from "ws";

import { connectWebSocket } from "../../peerline-ts/dist/index.js";
import { registerHandlers } from "./battery.ts";

const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });

server.on("connection", (socket) => {
  connectWebSocket(socket).then(
    ({ peer }) => registerHandlers(peer),
    (error) => console.error("interop server: adopt failed:", error),
  );
});

server.on("error", (error) => {
  console.error("interop server:", error);
  process.exit(1);
});

server.on("listening", () => {
  const { port } = server.address();
  console.log(`LISTENING ws://127.0.0.1:${port}/`);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    for (const client of server.clients) client.terminate();
    server.close(() => process.exit(0));
  });
}
