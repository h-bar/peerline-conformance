/**
 * The TypeScript half of the interop harness: a peerline server on Node.
 *
 * Mirrors `rust/src/bin/conformance_server.rs` — runs
 * {@link registerHandlers} on one {@link Peer} per connection, announcing
 *
 * ```text
 * LISTENING ws://127.0.0.1:<port>/
 * LISTENING /tmp/plc.XXXXXX/s          # with a path argument
 * ```
 *
 * on stdout and then serving until it is killed — by the harness, or by the
 * `crash` notification the client sends to end the run (see
 * {@link registerCrash}). `just interop` starts it, reads that line, and
 * points the Rust `conformance_client` binary at it.
 *
 * With no argument it takes an OS-assigned loopback port and serves `ws`.
 * Given a filesystem path it listens there with `node:net` and adopts each
 * accepted socket with {@link attachNodeSocket} — the leg that puts this
 * package's newline framing in front of the Rust `peerline-transport-uds`.
 * There is no `serveNodeSocket`: a listener is `net.createServer` plus one
 * `attachNodeSocket` per connection, which is exactly what this does.
 *
 * Plain `.mjs`, run by `node` with no loader: the accepted socket is handed
 * straight to `connectWebSocket`, whose duck type covers the *server* side
 * of a `ws` connection as happily as the client side (already OPEN, so it
 * resolves immediately). The runtime comes from the built `dist/` — Node
 * can't load `src/` directly, since the sources import each other with the
 * `.js` specifiers TypeScript wants — and the handler set comes from the
 * shared `battery.ts`, which Node type-strips on import. The node-socket
 * adapter comes from `dist/node.js` rather than `dist/index.js`,
 * deliberately: it imports `node:net`, which is kept out of anything a
 * browser bundle can reach.
 */

import { createServer } from "node:net";

import { WebSocketServer } from "ws";

import { connectWebSocket } from "../../peerline-ts/dist/index.js";
import { attachNodeSocket } from "../../peerline-ts/dist/node.js";
import { registerCrash, registerHandlers } from "./battery.ts";

/**
 * Install the handler set on one accepted peer. Shared by both transports so
 * a leg cannot accidentally serve a different surface.
 */
function accept(peer) {
  registerHandlers(peer);
  // The interop client's last act is a `crash` notification, which kills this
  // process mid-stream. Only the runners install it; see `registerCrash`.
  registerCrash(peer);
}

/** Print the readiness line exactly once — the harness greps for it. */
function announce(address) {
  console.log(`LISTENING ${address}`);
}

const socketPath = process.argv[2];
if (socketPath) {
  serveUnixSocket(socketPath);
} else {
  serveWebSocket();
}

function serveWebSocket() {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });

  server.on("connection", (socket) => {
    connectWebSocket(socket).then(
      ({ peer }) => accept(peer),
      (error) => console.error("interop server: adopt failed:", error),
    );
  });

  server.on("error", (error) => {
    console.error("interop server:", error);
    process.exit(1);
  });

  server.on("listening", () => {
    const { port } = server.address();
    announce(`ws://127.0.0.1:${port}/`);
  });

  onSignal(() => {
    for (const client of server.clients) client.terminate();
    server.close(() => process.exit(0));
  });
}

function serveUnixSocket(path) {
  const accepted = [];
  const server = createServer((socket) => {
    accepted.push(socket);
    // The Rust client's last act is a `crash` notification and this process
    // dies mid-frame; the peer on the other end of that resets the socket.
    // Swallow it, or an unhandled 'error' takes the process down with a
    // status the harness would read as a battery failure.
    socket.on("error", () => {});
    // Synchronous, unlike the WebSocket adapter: the socket is already
    // connected, so there is nothing to await.
    accept(attachNodeSocket(socket).peer);
  });

  server.on("error", (error) => {
    console.error("interop server:", error);
    process.exit(1);
  });

  // The listen callback fires once the socket file exists and is accepting,
  // which is the same promise the WebSocket branch's `listening` makes.
  server.listen(path, () => announce(path));

  onSignal(() => {
    for (const socket of accepted) socket.destroy();
    server.close(() => process.exit(0));
  });
}

function onSignal(shutdown) {
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, shutdown);
}
