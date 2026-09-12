# The peerline conformance suite. List with `just`.
#
# Everything here checks the two implementations, which live in their own
# repositories, so a fresh clone needs both checked out alongside this one:
#
#     peerline/                  (any parent folder)
#     ├── peerline/              the Rust implementation
#     ├── peerline-ts/           the TypeScript implementation
#     └── peerline-conformance/  this repo
#
# The TypeScript recipes (`vectors-ts`, `interop`) import and build the
# sibling sources, so run `pnpm install` in ../peerline-ts (and
# `pnpm install --dir ts` here) before the first run.

# Check-only by default: `bindings-check` exports into a scratch directory and
# diffs, so checking never writes into ../peerline-ts. Run `just bindings`
# explicitly when regeneration is the point.
default: vectors bindings-check interop

# --- the static half ---
# `vectors.json` is raw frame text plus the disposition every implementation
# must agree on. Both readers consume the same file, so the two languages are
# pinned to each other rather than each to its own expectations.

vectors: vectors-rust vectors-ts

vectors-rust:
    @echo "vectors: Rust parser"
    cargo test -p peerline-conformance --test vectors

vectors-ts:
    @echo "vectors: TypeScript parser"
    pnpm --dir ts test

# --- generated wire types ---
# `peerline-ts`'s `src/generated/` is ts-rs output produced from the Rust
# source and committed, so the npm package builds without a Rust toolchain.
# Keeping it honest needs both languages at once, which is why the check lives
# here rather than in either implementation.

# Regenerate the bindings in the sibling TypeScript repo.
bindings:
    cargo test --quiet --manifest-path ../peerline/Cargo.toml \
        -p peerline --features ts-export export_bindings
    @echo "bindings: regenerated into ../peerline-ts/src/generated"

# Fail if the committed bindings have drifted from the Rust source.
#
# Deliberately *not* `bindings` + `git diff`: that would rewrite the sibling
# repo's working tree as a side effect of checking it, and its verdict would
# be confused by any drift already sitting uncommitted there. Instead the
# export goes to a scratch directory — a plain environment variable wins over
# the `.cargo/config.toml` `[env]` value, which is not `force`d — and the
# committed directory is compared against it, byte for byte, sibling untouched.
bindings-check:
    #!/usr/bin/env bash
    set -euo pipefail
    scratch=$(mktemp -d -t peerline-bindings-check)
    trap 'rm -rf "$scratch"' EXIT
    TS_RS_EXPORT_DIR="$scratch" cargo test --quiet \
        --manifest-path ../peerline/Cargo.toml \
        -p peerline --features ts-export export_bindings
    if ! diff -ru ../peerline-ts/src/generated "$scratch"; then
        echo "bindings-check: src/generated has drifted from the Rust source." >&2
        echo "bindings-check: run \`just bindings\` to regenerate it." >&2
        exit 1
    fi
    echo "bindings-check: ../peerline-ts/src/generated matches the Rust source"

# --- the live half ---

# The battery against every transport, so drift *between* transports fails.
matrix:
    cargo test -p peerline-conformance --test matrix --test mounted --test iroh_seam

# One battery, run live over a real WebSocket in *both* directions, so any
# wire-level drift between the two runtimes fails here rather than in a
# consumer. Each language ships one server and one client, so a direction is
# one language's server paired with the other's client — the same recipe
# twice with the halves swapped. Loopback only; no network.
#
# The client's last act is the truncation battery, which tells the server to
# crash so it can check what a dying peer looks like to a consumer. Each
# direction therefore starts its own server and leaves it dead — which is
# what `_interop` already does anyway.
interop: interop-rust-serves interop-ts-serves

# Rust serves, TypeScript runs the battery.
interop-rust-serves:
    @just _interop "Rust serves → TypeScript battery" \
        "{{justfile_directory()}}/target/debug/conformance_server" \
        "node {{justfile_directory()}}/ts/client.mjs"

# TypeScript serves, Rust runs the battery.
interop-ts-serves:
    @just _interop "TypeScript serves → Rust battery" \
        "node {{justfile_directory()}}/ts/server.mjs" \
        "{{justfile_directory()}}/target/debug/conformance_client"

# One direction: start SERVER, wait for its `LISTENING <url>` line, point
# CLIENT at that url, require `BATTERY OK`.
#
# Both binaries are *built* and then spawned rather than run via `cargo run`:
# killing `cargo run` leaves the child alive as an orphan holding the port.
_interop label server client:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "interop: {{label}}"
    cargo build --quiet -p peerline-conformance --features ws \
        --bin conformance_server --bin conformance_client
    pnpm --dir ../peerline-ts build >/dev/null
    log=$(mktemp -t peerline-interop.XXXXXX)
    {{server}} >"$log" 2>&1 &
    server_pid=$!
    # Trap, not a plain kill at the end: the server must die even when the
    # battery below fails or the recipe is interrupted.
    trap 'kill "$server_pid" 2>/dev/null || true; rm -f "$log"' EXIT
    url=""
    for _ in $(seq 1 300); do
        url=$(sed -n 's/^LISTENING //p' "$log" | head -1 || true)
        if [ -n "$url" ]; then break; fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            echo "interop: the server exited before listening:" >&2
            cat "$log" >&2
            exit 1
        fi
        sleep 0.1
    done
    if [ -z "$url" ]; then
        echo "interop: the server never printed LISTENING:" >&2
        cat "$log" >&2
        exit 1
    fi
    echo "interop: server at $url"
    out=$({{client}} "$url")
    echo "$out"
    if ! printf '%s\n' "$out" | grep -qx 'BATTERY OK'; then
        echo "interop: the client did not report BATTERY OK" >&2
        cat "$log" >&2
        exit 1
    fi

clippy:
    cargo clippy --workspace --all-targets

fmt:
    cargo fmt --all

clean:
    cargo clean
