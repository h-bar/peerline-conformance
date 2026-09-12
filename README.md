# peerline-conformance

[![CI](https://github.com/h-bar/peerline-conformance/actions/workflows/ci.yml/badge.svg)](https://github.com/h-bar/peerline-conformance/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

The conformance suite for peerline — what makes "two implementations of one
protocol" a checked claim rather than an aspiration.

The implementations live in their own repositories: [`peerline`] (Rust) and
[`peerline-ts`] (TypeScript). This one depends on both; neither depends on
it.

Two implementations of a protocol drift unless something stops them. Two
things stop them here: a set of golden frame vectors that pins the *parsers*,
and a live battery that pins the *runtimes*.

Everything here is test scaffolding, and nothing is published.

```
vectors.json     the contract: golden frames as raw text, plus the disposition
rust/            one battery, one server, one client — the Rust half
ts/              one battery, one server, one client — the TypeScript half
```

## The static half: golden vectors

`vectors.json` holds 112 entries. Each is a frame **as text** plus the
disposition both implementations must agree on:

```jsonc
{
  "name": "resp-ok-null-data",
  "frame": "{\"ver\":\"1\",\"kind\":\"resp\",\"id\":7,\"data\":null}",
  "expect": { "valid": true, "kind": "resp-ok", "id": 7, "data": null }
}
```

**42 accept, 70 reject.** The reject majority is the point — compatibility is
mostly about refusing the same things. The set covers `bad-ver-2`,
`unknown-key-on-req`, `notif-with-id`, `resp-both-data-and-err`,
`req-id-fractional`, `stream-seq-string`, `resp-err-code-above-i32` and so on.

The accept side also pins the exact spelling of the reserved conventions,
which is the one thing the live battery cannot: it asserts that a
subscription completed cleanly, not that the frame saying so was
`$peerline/pubsub.end` carrying `subscription_id` and nothing else. Hence
`pubsub-event-notif`, `pubsub-end-notif`, `pubsub-unsubscribe-req`,
`pubsub-unsubscribe-ack` and `stream-cancel-notif`.

Storing frames as text rather than parsed objects keeps JSON-level divergence
in scope: duplicate keys, number formats, lone surrogates.

Two details worth knowing:

- Invalid entries carry a `category`, but the Rust side deliberately does not
  assert it. serde and TypeScript phrase rejections differently, so the
  cross-language contract is the *disposition*, not the message.
- `tsOnly: true` marks genuine, documented divergence — ids past 2^53 are
  legal `u64`s in Rust but beyond JavaScript's safe-integer range. Two entries
  carry it, and the Rust reader skips them.

`rust/tests/vectors.rs` and `ts/vectors.test.ts` read the same file, so the
two implementations are pinned to each other rather than each to its own
expectations.

## The live half: the battery

One handler-set and one battery, defined once per language and mirrored case
for case (`rust/src/lib.rs`, `ts/battery.ts`).

`register` installs twenty-four handlers and `battery` calls them and asserts
the answers. The original thirteen — `echo`, `add`, `fail`, `fail_data`,
`note`, `count`, `count_stream`, `boom_stream`, `barrier`, `reverse`, `big`,
`hang`, `hang_stream` — cover typed args, error propagation with and without
`data`, notification delivery, streaming to a clean end and to a mid-stream
error, concurrent multiplexing, peer symmetry, and a 1 MiB frame.

The multiplexing check parks `CONCURRENCY` calls on a shared barrier that only
releases once all of them arrive, so an implementation that serialises
requests deadlocks rather than quietly passing. It cannot succeed by accident.

The rest cover the surface each implementation grew its own tests for but
which had never met its counterpart over a wire:

| Handlers | Pins |
| --- | --- |
| `sub.open`, `sub.emit`, `sub.end_key`, `sub.count` | the pubsub **runtime layer** — `Publisher` / `Subscription`, not hand-built frames — through all three endings: the producer's `end` completes cleanly, and an unsubscribe *or* a dropped handle prunes the producer, which only the producer can report |
| `kind_stream`, `kind_unary` | **kind mismatch** both ways: `call` against a stream handler and `call_stream` against a unary one. Each runtime resolves this locally, in the caller, so running it across the wire is what checks the *counterpart* emits the frames that resolution expects — same code and same message text in both languages |
| `slow_stream`, `cancel_report` | **stream cancel** seen from the producing end: the cancel arrives, and a send after it is refused — which is also why no terminal is delivered, since the same closed stream drops both |
| `strict_add` | **malformed args** answered `-32602` by both servers. Rust gets that free from `serde_json::from_value`; the TypeScript half registers the equivalent `opts.parse` validator, and that asymmetry is the case |
| `throw_plain`, `boom_stream_data` | an unknown op is `MethodNotFound` both ways; a handler failing in a way that is not already an `RpcError` is `-32603` both ways; and `data` survives on a stream **error terminal**, which `fail_data` (a response) does not reach |

### The tail: what a dying peer looks like

`truncation_battery` / `truncationBattery` runs last in the interop runners
only, because it **ends the connection**. The client opens `hang_stream` (one
item, no terminal ever) and leaves a `hang` call in flight, then sends the
`crash` notification, which the server runners answer by exiting the process —
peerline has no server-side per-connection close, and "the producing process
died" is the failure this pins.

Three assertions, and the first is what keeps the other two honest:

1. a stream that reaches its terminal still ends **cleanly**;
2. a stream the connection cut short ends with an **error** — `Err(Abandoned)`
   in Rust, a thrown `AbandonedError` in TypeScript — never with the clean end
   a complete stream gives, or half a stream would read as all of it;
3. the in-flight call fails as `Abandoned` too, not as an RPC error a consumer
   would have to tell from a real remote failure by its message text.

This is a *runtime* contract rather than a wire one, which is exactly why it
belongs here: nothing in `vectors.json` can catch two runtimes disagreeing
about what a dropped connection means.

### Runners

Each language ships one **server** and one **client** built on its battery, so
a cross-language run is one language's server paired with the other's client —
the same recipe twice with the halves swapped.

| Runner | Proves |
| --- | --- |
| `just matrix` | no drift between loopback / uds / ws / iroh; multi-service mounting; the iroh seam below `Peer` |
| `just interop-rust-serves` | Rust serves over WebSocket, the TypeScript battery dials (and kills the Rust server on its way out) |
| `just interop-ts-serves` | TypeScript serves over WebSocket, the Rust battery dials (and kills the TypeScript server on its way out) |
| `just interop-uds-rust-serves` | Rust serves over a Unix socket, the TypeScript battery dials it with `connectNodeSocket` |
| `just interop-uds-ts-serves` | TypeScript serves over a Unix socket (`net.createServer` + `attachNodeSocket`), the Rust UDS transport dials it |
| `just bindings-check` | `peerline-ts`'s generated wire types match the Rust source |

`just interop` runs all four. The two pairings are there because the two
languages have two framings that have to agree: WebSocket carries frames the
protocol already delimits, while the Unix-socket leg puts Rust's
`peerline-transport-uds` (a `LinesCodec` with a shared `MAX_FRAME_LEN`) in
front of the TypeScript `node:net` adapter (its own newline splitter with a
FIFO write queue and the same ceiling) — two independent implementations of
one framing.

One server binary and one client binary per language serve every leg: each
dispatches on the shape of its single address argument, a `ws://` url or a
filesystem path, so `_interop` spawns the same two programs either way and
only what it greps off the `LISTENING` line differs. Sockets go under a short
`/tmp` template, because `sun_path` is 104 bytes on Darwin and the UDS
transport appends a staging suffix before renaming.

The transport matrix is why this repository holds all transport testing: each
transport's own crate has no `tests/` directory, because running the identical
battery over every transport is what catches behavioural drift between them.

## Generated wire types

`peerline-ts`'s `src/generated/` is [ts-rs] output produced from the Rust
source and committed, so the npm package builds without a Rust toolchain.
Keeping that honest needs both languages at once, which is why `just bindings`
and `just bindings-check` live here rather than in either implementation.

## Running it

Needs both implementation repositories checked out alongside this one:

```
peerline/                  (any parent folder)
├── peerline/              the Rust implementation
├── peerline-ts/           the TypeScript implementation
└── peerline-conformance/  this repo
```

```sh
just              # vectors + bindings-check + interop
just vectors      # the golden vectors, both languages
just bindings-check  # the generated wire types match the Rust source
just bindings     # regenerate the bindings in ../peerline-ts
just matrix       # the battery against every transport
just interop      # the cross-language battery, both directions
```

The Rust side is a normal cargo workspace. The TypeScript side is a private
package with one runtime dependency (`ws` — Node has no built-in WebSocket
*server*); `pnpm install --dir ts` before the first run. `just vectors-ts` and
`just interop` also import and build the sibling `peerline-ts` sources, so run
`pnpm install` in `../peerline-ts` first as well.

This repository also carries `family.justfile` — the local CI for the whole
family, spanning every sibling's suite plus everything above. It runs from
the parent folder via a symlink, which a fresh checkout creates once:

```sh
cd .. && ln -s peerline-conformance/family.justfile justfile
just quick        # the fast cross-language gate — seconds
just verify       # the release gate — every suite, matrix and battery
```

## License

MIT.

[`peerline`]: https://github.com/h-bar/peerline
[`peerline-ts`]: https://github.com/h-bar/peerline-ts
[ts-rs]: https://github.com/Aleph-Alpha/ts-rs
