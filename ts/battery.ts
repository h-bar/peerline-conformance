/**
 * The conformance battery and handler set, in TypeScript.
 *
 * A case-for-case port of `rust/src/lib.rs`: the two exports here answer to
 * the two exports there, over the same op names, and the two are the
 * cross-language wire contract. Change one, change both.
 *
 * Both interop directions reuse this file:
 *
 * - `client.mjs` runs {@link battery} against the Rust
 *   `conformance_server` binary;
 * - `server.mjs` runs {@link registerHandlers} per connection, and the
 *   Rust `conformance_client` binary runs the Rust battery against it.
 *
 * {@link truncationBattery} is the tail that cannot live in {@link battery}:
 * it kills the connection on purpose, to pin what a consumer sees when the
 * peer dies with a stream open and a call in flight. It runs last, and needs
 * the server to have installed {@link registerCrash}.
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

  // -------------------------------------------------------------------------
  // Pubsub, through the runtime layer
  // -------------------------------------------------------------------------
  //
  // The subscribe op is deliberately application-named — the convention
  // reserves only the `$peerline/pubsub.*` push ops, so "what it takes to open
  // a subscription" stays the application's contract. Everything else goes
  // through `Publisher`, never through hand-built frames: that is what makes
  // this a check of the two *runtime layers* against each other rather than of
  // two test helpers.
  //
  // `sub.count` is what lets the client assert pruning. A consumer that
  // unsubscribes (or drops its handle) sends one fire-and-forget request and
  // can never otherwise see whether the producer acted on it.
  const publisher = peer.publisher();
  peer.onRequest<{ key: string }>("sub.open", (args) => publisher.subscribe(args.key));
  peer.onRequest<{ key: string; n: number }, number>("sub.emit", (args) => {
    let sent = 0;
    for (let i = 0; i < args.n; i += 1) sent += publisher.publish(args.key, { i });
    return sent;
  });
  peer.onRequest<{ key: string }, number>("sub.end_key", (args) => publisher.endKey(args.key));
  peer.onRequest<{ key: string }, number>("sub.count", (args) =>
    publisher.subscribers(args.key),
  );

  // -------------------------------------------------------------------------
  // Kind mismatch
  // -------------------------------------------------------------------------
  //
  // The wire carries no "I want a stream" marker: whether a request is
  // answered with one `resp` or a run of `stream` frames is decided entirely
  // by which handler the *responder* registered. Each runtime resolves the
  // mismatch locally, in the caller — so pinning it across the wire is really
  // pinning that the counterpart emits the frames that resolution expects.
  //
  // `kind_stream` parks on the cancel rather than returning, because a caller
  // that used `call` is by then gone: its runtime sends the stream cancel, and
  // waiting for it keeps a terminal from racing the mismatch.
  peer.onStreamRequest("kind_stream", async (_args, sender: StreamSender) => {
    sender.sendItem("item");
    await sender.cancelled;
  });
  peer.onRequest("kind_unary", () => "unary");

  // -------------------------------------------------------------------------
  // Stream cancel, observed from the producing end
  // -------------------------------------------------------------------------
  //
  // The consumer half of a cancel is invisible to the consumer: it drops a
  // handle and, by construction, hears nothing back. So the producer records
  // what it saw and `cancel_report` hands it over on the next call. `refused`
  // is the half that matters most — after the cancel the producer's outbound
  // stream is closed, which is also why *no terminal is delivered*: the same
  // gate that refuses this late item drops the handler's own end of stream.
  let cancelSeen = false;
  let lateRefused = false;
  peer.onStreamRequest("slow_stream", async (_args, sender: StreamSender) => {
    sender.sendItem("tick");
    await sender.cancelled;
    try {
      sender.sendItem("late");
      lateRefused = false;
    } catch {
      lateRefused = true;
    }
    // Set last: `observed` is what the client polls on, so ordering it after
    // `refused` means a report that says "observed" carries a settled answer.
    cancelSeen = true;
  });
  peer.onRequest("cancel_report", () => ({ observed: cancelSeen, refused: lateRefused }));

  // -------------------------------------------------------------------------
  // Malformed args
  // -------------------------------------------------------------------------
  //
  // The point of the case. A typed Rust handler rejects args of the wrong
  // shape with `-32602` before it is entered, because `serde_json::from_value`
  // does it. TypeScript erases its types, so `onRequest<{a,b}>` alone is a
  // convenience cast and `{ a: "x", b: 3 }` would happily answer `"x3"`. The
  // `parse` validator is what closes that gap, and registering it here is what
  // makes the two servers answer the same bad frame identically.
  peer.onRequest<{ a: number; b: number }, number>(
    "strict_add",
    (args) => args.a + args.b,
    {
      parse: (raw) => {
        const args = raw as { a?: unknown; b?: unknown } | null;
        if (typeof args?.a !== "number" || typeof args.b !== "number") {
          // A plain throw becomes `-32602`; an `rpcError`-shaped throw would
          // be relayed verbatim instead, which is not what Rust does here.
          throw new Error("strict_add expects { a: number, b: number }");
        }
        return { a: args.a, b: args.b };
      },
    },
  );

  // -------------------------------------------------------------------------
  // Error paths
  // -------------------------------------------------------------------------
  //
  // A handler that fails in a way that is not already an RPC error. Here that
  // is a thrown `Error`, which the runtime maps to Internal carrying its
  // message; Rust handlers return `Result<_, RpcError>` and so spell the same
  // thing as an explicit `RpcError::internal`. Same code, same message.
  peer.onRequest("throw_plain", () => {
    throw new Error("plain failure");
  });
  // `fail_data` above pins `data` on a *response*; this pins it on a stream
  // error terminal, which is a different frame and a different code path.
  peer.onStreamRequest("boom_stream_data", (_args, sender: StreamSender) => {
    sender.sendItem("first");
    throw { code: -32053, msg: "mid-stream", data: { hint: "stream" } };
  });

  // The truncation pair (see `truncationBattery`): work that is still
  // outstanding when the connection dies. Neither ever settles on its own,
  // so the only way a consumer sees either end is the connection going away
  // — which is exactly the case being pinned.
  peer.onRequest("hang", () => new Promise<never>(() => {}));
  peer.onStreamRequest("hang_stream", (_args, sender: StreamSender) => {
    sender.sendItem("open");
    return new Promise<never>(() => {});
  });
}

/**
 * The `crash` sink: on notification, **exit this process**, dropping the
 * connection with the `hang` / `hang_stream` work above still outstanding.
 *
 * Registered only by `server.mjs` — deliberately not by
 * {@link registerHandlers}, which is also installed on peers living inside a
 * test process. `conformance::register_crash` is the Rust half; `crash` is
 * part of the interop wire contract.
 */
export function registerCrash(peer: Peer): void {
  peer.onNotification("crash", () => {
    process.exit(0);
  });
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

  // -------------------------------------------------------------------------
  // Pubsub: three endings, one subscription each
  // -------------------------------------------------------------------------

  // (a) The producer ends it: a clean completion, which is the control for
  //     (b) and (c) — without it "every subscription just stops" would pass.
  const ackA = await client.call<SubscriptionAckLike>("sub.open", { key: "a" });
  // Registered immediately after the ack, which is the only ordering the wire
  // lets either side rely on.
  const subA = client.subscription<{ i: number }>(ackA.subscription_id);
  assert.equal(
    await client.call<number>("sub.emit", { key: "a", n: 3 }),
    3,
    "one subscriber, three events",
  );
  // No polling: the events were queued before the `sub.emit` response, and the
  // wire is ordered, so they are routed by the time that call resolves.
  for (let i = 0; i < 3; i += 1) {
    const next = await subA.next();
    assert.equal(next.done, false, "expected an event");
    assert.deepEqual(next.value, { i }, "events arrive in order and typed");
  }
  assert.equal(await client.call<number>("sub.end_key", { key: "a" }), 1);
  assert.equal(
    (await subA.next()).done,
    true,
    "the producer's end marker completes the subscription cleanly",
  );

  // (b) The consumer unsubscribes: the producer must prune, which only the
  //     producer can report.
  const ackB = await client.call<SubscriptionAckLike>("sub.open", { key: "b" });
  const subB = client.subscription<{ i: number }>(ackB.subscription_id);
  assert.equal(await subscribers(client, "b"), 1);
  assert.equal(await client.call<number>("sub.emit", { key: "b", n: 1 }), 1);
  const onB = await subB.next();
  assert.deepEqual(onB.value, { i: 0 }, "a live subscription receives before it is retired");
  subB.unsubscribe();
  await awaitSubscribers(client, "b", 0);

  // (c) The consumer walks away from the iterator instead — `return()`, which
  //     is what a `break` out of a `for await` calls, and this runtime's
  //     stand-in for the Rust port simply dropping the handle.
  const ackC = await client.call<SubscriptionAckLike>("sub.open", { key: "c" });
  const subC = client.subscription<{ i: number }>(ackC.subscription_id);
  assert.equal(await subscribers(client, "c"), 1);
  await subC.return();
  await awaitSubscribers(client, "c", 0);

  // -------------------------------------------------------------------------
  // Kind mismatch, in both directions
  // -------------------------------------------------------------------------
  //
  // Both codes *and* both messages are asserted: the two runtimes phrase this
  // identically on purpose, and a consumer telling the two mismatches apart
  // has nothing else to go on.
  const streamAsCall = await rejection(client.call("kind_stream", {}), "kind_stream");
  assert.equal(streamAsCall.code, -32603);
  assert.equal(streamAsCall.msg, "responder answered with a stream; use call_stream");

  const unaryAsStream = client.callStream("kind_unary", {});
  const unaryShape = await rejection(unaryAsStream.next(), "kind_unary");
  assert.equal(unaryShape.code, -32603);
  assert.equal(unaryShape.msg, "responder answered with a unary response; use call");
  assert.equal((await unaryAsStream.next()).done, true, "the stream ends after the violation");

  // -------------------------------------------------------------------------
  // Stream cancel
  // -------------------------------------------------------------------------
  const slow = client.callStream<string>("slow_stream", {});
  const tick = await slow.next();
  assert.equal(tick.done, false, "slow_stream should open");
  assert.equal(
    (tick.value as StreamItem<string>).data,
    "tick",
    "the producer is provably live before the cancel",
  );
  await slow.return();
  // Bounded poll: the cancel is fire-and-forget, so there is no moment the
  // consumer can name at which the producer must already have seen it.
  let report: { observed?: boolean; refused?: boolean } = {};
  for (let i = 0; i < 200; i += 1) {
    report = await client.call<{ observed: boolean; refused: boolean }>("cancel_report", {});
    if (report.observed === true) break;
    await sleep(5);
  }
  assert.equal(report.observed, true, "the producer never observed the cancel");
  assert.equal(
    report.refused,
    true,
    "a send after the cancel must be refused — which is also why no terminal is delivered: the same closed stream drops both",
  );

  // -------------------------------------------------------------------------
  // Malformed args
  // -------------------------------------------------------------------------
  assert.equal(
    await client.call<number>("strict_add", { a: 2, b: 3 }),
    5,
    "the validator must not reject good args",
  );
  // Code only: Rust carries serde's phrasing and TypeScript the validator's,
  // and the cross-language contract is the code.
  const badArgs = await rejection(client.call("strict_add", { a: "x", b: 3 }), "strict_add");
  assert.equal(badArgs.code, -32602, "malformed args are InvalidParams");

  // -------------------------------------------------------------------------
  // Error paths
  // -------------------------------------------------------------------------
  // Message not asserted: the two runtimes name the missing op differently,
  // and only the code is the contract.
  const unknown = await rejection(client.call("no_such_op", {}), "no_such_op");
  assert.equal(unknown.code, -32601, "unknown op is MethodNotFound");

  const plain = await rejection(client.call("throw_plain", {}), "throw_plain");
  assert.equal(plain.code, -32603, "a non-RPC handler failure is Internal");
  assert.equal(plain.msg, "plain failure");
  assert.equal(plain.data, undefined, "a synthesized Internal carries no data");

  // `data` on a stream error terminal, which `fail_data` does not reach.
  const dataStream = client.callStream<string>("boom_stream_data", {});
  const firstOfData = await dataStream.next();
  assert.equal(firstOfData.done, false);
  assert.equal((firstOfData.value as StreamItem<string>).data, "first");
  const terminal = await rejection(dataStream.next(), "boom_stream_data");
  assert.equal(terminal.code, -32053);
  assert.equal(terminal.msg, "mid-stream");
  assert.deepEqual(terminal.data, { hint: "stream" });
  assert.equal((await dataStream.next()).done, true, "the stream ends after the error");
}

/** The one field of a `SubscriptionAck` this file needs. */
interface SubscriptionAckLike {
  readonly subscription_id: string;
}

/** Live subscriptions the *producer* is holding on `key`. */
function subscribers(client: Peer, key: string): Promise<number> {
  return client.call<number>("sub.count", { key });
}

/**
 * Poll {@link subscribers} until it reaches `want`, or throw.
 *
 * Bounded rather than instant because unsubscribe is fire-and-forget on the
 * consumer side: the reply exists, but nothing on the consumer holds it, so
 * the only observable is the producer's own count settling.
 */
async function awaitSubscribers(client: Peer, key: string, want: number): Promise<void> {
  let last = -1;
  for (let i = 0; i < 200; i += 1) {
    last = await subscribers(client, key);
    if (last === want) return;
    await sleep(5);
  }
  throw new Error(`subscribers on ${key} never reached ${want} (last saw ${last})`);
}

/**
 * The truncation half of the battery — `conformance::truncation_battery`.
 *
 * Kept out of {@link battery} because it *ends the connection*: it must run
 * last, and only against a server that also ran {@link registerCrash}.
 *
 * Three things must be true at once, and the first is what keeps the other
 * two honest:
 *
 * 1. a stream that reaches its terminal still ends **cleanly**;
 * 2. a stream the connection cuts short **throws**, rather than ending the
 *    same way a finished one does — otherwise a truncated stream reads as a
 *    complete one, and half a history looks like all of it;
 * 3. a unary call in flight when the connection dies rejects with
 *    `AbandonedError`, not with the `RpcCallError` a consumer would have to
 *    tell from a real remote error by its message text.
 *
 * Where Rust yields `Err(Error::Abandoned)` from `poll_next`, an async
 * iterator raises: the same distinction, spelled the way each language
 * spells "this did not finish".
 */
export async function truncationBattery(client: Peer): Promise<void> {
  // 1. A stream that reaches its terminal ends cleanly — the control for
  //    everything below, without which "every stream throws" would pass.
  const items: number[] = [];
  for await (const item of client.callStream<number>("count_stream", { n: 2 })) {
    items.push(item.data);
  }
  assert.deepEqual(items, [0, 1]);

  // Open the stream that never terminates, and wait for its first item so
  // the producer is provably live before the connection is cut.
  const cut = client.callStream<string>("hang_stream", {});
  const opened = await cut.next();
  assert.equal(opened.done, false, "hang_stream should open");
  assert.equal((opened.value as StreamItem<string>).data, "open");

  // In flight when the crash lands. Registration is synchronous in `call`,
  // and the frame goes out before the notification below, so the server
  // has it in hand when it dies.
  const hang = client.call("hang", {});
  const inflight = hang.then(
    () => new Error("the in-flight call cannot succeed"),
    (thrown: unknown) => thrown,
  );

  client.notify("crash", {});

  // 2. The truncated stream raises, and specifically not with the clean end
  //    a complete stream gives.
  const truncated = await failure(
    withTimeout(cut.next(), 30_000, "truncated stream"),
    "hang_stream",
  );
  assert.equal(
    truncated.name,
    "AbandonedError",
    `hang_stream: expected AbandonedError, got ${truncated.name}: ${truncated.message}`,
  );
  assert.equal(
    (await cut.next()).done,
    true,
    "the stream should be fused after the truncation error",
  );

  // 3. The in-flight unary call keeps its own type.
  const abandoned = (await withTimeout(inflight, 30_000, "in-flight call")) as Error;
  assert.equal(
    abandoned.name,
    "AbandonedError",
    `hang: expected AbandonedError, got ${abandoned.name}: ${abandoned.message}`,
  );
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

/**
 * Await a promise that must reject, and hand back what it threw. Unlike
 * {@link rejection} this asserts nothing about the shape: the truncation
 * checks are about *which* error class arrives.
 */
async function failure(promise: Promise<unknown>, what: string): Promise<Error> {
  try {
    await promise;
  } catch (thrown) {
    return thrown as Error;
  }
  throw new Error(`${what}: expected a rejection, got a value`);
}

/** Fail loudly instead of hanging if teardown never reaches the consumer. */
function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`${what}: nothing arrived within ${ms}ms`));
    }, ms);
    timer.unref();
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (thrown: unknown) => {
        clearTimeout(timer);
        reject(thrown as Error);
      },
    );
  });
}
