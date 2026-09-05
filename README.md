# peerline-conformance

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

`vectors.json` holds 108 entries. Each is a frame **as text** plus the
disposition both implementations must agree on:

```jsonc
{
  "name": "resp-ok-null-data",
  "frame": "{\"ver\":\"1\",\"kind\":\"resp\",\"id\":7,\"data\":null}",
  "expect": { "valid": true, "kind": "resp-ok", "id": 7, "data": null }
}
```

**38 accept, 70 reject.** The reject majority is the point — compatibility is
mostly about refusing the same things. The set covers `bad-ver-2`,
`unknown-key-on-req`, `notif-with-id`, `resp-both-data-and-err`,
`req-id-fractional`, `stream-seq-string`, `resp-err-code-above-i32` and so on.

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

`register` installs eleven handlers — `echo`, `add`, `fail`, `fail_data`,
`note`, `count`, `count_stream`, `boom_stream`, `barrier`, `reverse`, `big` —
and `battery` calls them and asserts the answers: typed args, error
propagation with and without `data`, notification delivery, streaming to a
clean end and to a mid-stream error, concurrent multiplexing, peer symmetry,
and a 1 MiB frame.

The multiplexing check parks `CONCURRENCY` calls on a shared barrier that only
releases once all of them arrive, so an implementation that serialises
requests deadlocks rather than quietly passing. It cannot succeed by accident.

### Runners

Each language ships one **server** and one **client** built on its battery, so
a cross-language run is one language's server paired with the other's client —
the same recipe twice with the halves swapped.

| Runner | Proves |
| --- | --- |
| `just matrix` | no drift between loopback / uds / ws / iroh; multi-service mounting; the iroh seam below `Peer` |
| `just interop-rust-serves` | Rust serves, the TypeScript battery dials |
| `just interop-ts-serves` | TypeScript serves, the Rust battery dials |
| `just bindings-check` | `peerline-ts`'s generated wire types match the Rust source |

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

## License

MIT.

[`peerline`]: https://github.com/h-bar/peerline
[`peerline-ts`]: https://github.com/h-bar/peerline-ts
[ts-rs]: https://github.com/Aleph-Alpha/ts-rs
