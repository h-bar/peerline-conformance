/**
 * The conformance battery and handler set, in TypeScript.
 *
 * A case-for-case port of `crates/peerline/src/conformance.rs`: the two
 * exports here answer to the two exports there, over the same op names, and
 * the two are the cross-language wire contract. Change one, change both.
 *
 * Both interop directions reuse this file:
 *
 * - `client.mjs` runs {@link battery} against the Rust
 *   `conformance_server` binary;
 * - `server.mjs` runs {@link registerHandlers} per connection, and the
 *   Rust `conformance_client` binary runs the Rust battery against it.
 *
 * Two constraints shape the style. It must run **unbuilt under plain Node**
 * (`server.mjs` imports it and Node strips the types), so every import of
 * the package is `import type` — erased before Node resolves anything — and
 * assertions come from `node:assert`, not from vitest. And the errors
 * handlers raise are plain objects rather than `rpcError()` calls, because
 * the runtime's `isRpcErrorLike` test is structural: `{ code, msg, data }` is
 * all a thrown value needs to travel as itself.
 */

import assert from "node:assert/strict";

import type { Peer } from "../../peerline-ts/src/runtime/peer.js";
import type { StreamItem, StreamSender } from "../../peerline-ts/src/runtime/stream.js";

/**
 * Concurrent in-flight requests used by the multiplexing check. Must match
 * `conformance::CONCURRENCY` on the Rust side: the `barrier` op only
 * releases once this many calls are parked on it, so a mismatch hangs.
 */
export const CONCURRENCY = 8;

// ---------------------------------------------------------------------------
// The server side
// ---------------------------------------------------------------------------

/**
 * The server-side handler set — `conformance::register`. Register it on a
 * freshly-connected peer; the state it needs (notification counter, barrier)
 * is per-connection, exactly as in Rust.
 */
export function registerHandlers(peer: Peer): void {
  peer.onRequest<{ s: string }, string>("echo", (args) => args.s);
  peer.onRequest<{ a: number; b: number }, number>("add", (args) => args.a + args.b);
  peer.onRequest("fail", () => {
    throw { code: -32050, msg: "boom" };
  });
  // Same, but carrying `data` — the field is part of the protocol and both
  // runtimes must forward it verbatim.
  peer.onRequest("fail_data", () => {
    throw { code: -32052, msg: "boom", data: { hint: "data" } };
  });

  // Notification sink + a request that reads the accumulated count.
  let counter = 0;
  peer.onNotification<{ n: number }>("note", (args) => {
    counter += args.n;
  });
  peer.onRequest("count", () => counter);

  // Streaming: N items, then a stream that errors mid-flight.
  peer.onStreamRequest<{ n: number }>("count_stream", (args, sender: StreamSender) => {
    for (let i = 0; i < args.n; i += 1) sender.sendItem(i);
  });
  peer.onStreamRequest("boom_stream", (_args, sender: StreamSender) => {
    sender.sendItem("first");
    throw { code: -32051, msg: "mid-stream" };
  });

  // Concurrency: every in-flight call must reach the barrier before any
  // returns — proves the connection multiplexes concurrent requests rather
  // than serialising them (no wall-clock assertion involved).
  const enterBarrier = barrier(CONCURRENCY);
  peer.onRequest<{ i: number }, number>("barrier", async (args) => {
    await enterBarrier();
    return args.i;
  });

  // Peer symmetry: the server calls back into the client mid-request.
  peer.onRequest<{ s: string }, string>("reverse", (args) =>
    peer.call<string>("client_echo", { s: args.s }),
  );

  peer.onRequest<{ s: string }, string>("big", (args) => args.s);
}

// ---------------------------------------------------------------------------
// The client side
// ---------------------------------------------------------------------------

/**
 * The full client-side battery — `conformance::battery`. `client` is
 * connected to a server that ran the handler set above (in either language).
 * Throws on the first mismatch.
 */
export async function battery(client: Peer): Promise<void> {
  // The reverse test needs a client-side handler for the server to call.
  client.onRequest<{ s: string }, string>("client_echo", (args) => `echoed:${args.s}`);

  // Unary + typed args.
  assert.equal(await client.call<string>("echo", { s: "hi" }), "hi");
  assert.equal(await client.call<number>("add", { a: 2, b: 40 }), 42);

  // Error propagation.
  const failed = await rejection(client.call("fail", {}), "fail");
  assert.equal(failed.code, -32050);
  assert.equal(failed.msg, "boom");
  assert.equal(failed.data, undefined);

  // …including an error that carries `data`, which must survive intact.
  const withData = await rejection(client.call("fail_data", {}), "fail_data");
  assert.equal(withData.code, -32052);
  assert.equal(withData.msg, "boom");
  assert.deepEqual(withData.data, { hint: "data" });

  // Notification delivery (bounded poll — no timing assertion).
  client.notify("note", { n: 3 });
  let delivered = false;
  for (let i = 0; i < 200; i += 1) {
    if ((await client.call<number>("count", {})) >= 3) {
      delivered = true;
      break;
    }
    await sleep(5);
  }
  assert.ok(delivered, "notification was not delivered");

  // Server-streaming: ordered items then clean end.
  const items: number[] = [];
  for await (const item of client.callStream<number>("count_stream", { n: 5 })) {
    items.push(item.data);
  }
  assert.deepEqual(items, [0, 1, 2, 3, 4]);

  // Mid-stream error surfaces after the first item, code and message intact.
  const stream = client.callStream<string>("boom_stream", {});
  const first = await stream.next();
  assert.equal(first.done, false);
  assert.equal((first.value as StreamItem<string>).data, "first");
  const midStream = await rejection(stream.next(), "boom_stream");
  assert.equal(midStream.code, -32051);
  assert.equal(midStream.msg, "mid-stream");
  assert.equal((await stream.next()).done, true, "stream ended after the error");

  // Concurrent multiplexing — the barrier never releases if the calls are
  // serialised, so this deadlocks (and the runner's timeout fires) rather
  // than measuring anything.
  const got = await Promise.all(
    Array.from({ length: CONCURRENCY }, (_unused, i) => client.call<number>("barrier", { i })),
  );
  got.sort((a, b) => a - b);
  assert.deepEqual(
    got,
    Array.from({ length: CONCURRENCY }, (_unused, i) => i),
  );

  // Peer symmetry: server → client call inside a client → server call.
  assert.equal(await client.call<string>("reverse", { s: "x" }), "echoed:x");

  // Large frame near the shared ceiling (1 MiB) round-trips intact.
  const big = "a".repeat(1 << 20);
  const back = await client.call<string>("big", { s: big });
  assert.equal(back.length, big.length);
  assert.ok(back === big, "1 MiB payload came back changed");
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/** The fields of an `RpcCallError` this file cares about. */
interface RejectedWith {
  readonly code: number;
  readonly msg: string;
  readonly data?: unknown;
}

/**
 * Await a promise that must reject with an RPC error, and hand back the
 * wire fields. Structural on purpose: this file never imports the runtime.
 */
async function rejection(promise: Promise<unknown>, what: string): Promise<RejectedWith> {
  try {
    await promise;
  } catch (thrown) {
    const error = thrown as Partial<RejectedWith> & { name?: string; message?: string };
    assert.equal(
      error.name,
      "RpcCallError",
      `${what}: expected an RpcCallError, got ${String(error.message ?? thrown)}`,
    );
    return error as RejectedWith;
  }
  throw new Error(`${what}: expected a rejection, got a value`);
}

/**
 * A promise-based rendezvous: the first `n - 1` callers park, the `n`-th
 * releases them all. The `tokio::sync::Barrier` in the Rust handler set.
 */
function barrier(n: number): () => Promise<void> {
  let waiting: Array<() => void> = [];
  return () =>
    new Promise<void>((resolve) => {
      waiting.push(resolve);
      if (waiting.length === n) {
        const release = waiting;
        waiting = [];
        for (const wake of release) wake();
      }
    });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
