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

# One battery, run live in *both* directions over two transport pairings, so
# any wire-level drift between the two runtimes fails here rather than in a
# consumer. Each language ships one server and one client, so a direction is
# one language's server paired with the other's client — the same recipe with
# the halves swapped. Loopback and Unix sockets only; no network.
#
# Two pairings, because the two languages have two framings that have to
# agree. WebSocket carries frames the protocol already delimits. The Unix
# socket leg instead puts the Rust `peerline-transport-uds` (a `LinesCodec`)
# in front of the TypeScript `node:net` adapter (its own newline splitter with
# a FIFO write queue) — two independent implementations of the same framing,
# which until now had never been pointed at each other.
#
# The client's last act is the truncation battery, which tells the server to
# crash so it can check what a dying peer looks like to a consumer. Each
# direction therefore starts its own server and leaves it dead — which is
# what `_interop` already does anyway.
interop: interop-rust-serves interop-ts-serves interop-uds-rust-serves interop-uds-ts-serves

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

# Rust serves over a Unix socket, TypeScript dials it with `connectNodeSocket`.
interop-uds-rust-serves:
    @just _uds-leg "Rust serves over UDS → TypeScript battery" \
        "{{justfile_directory()}}/target/debug/conformance_server" \
        "node {{justfile_directory()}}/ts/client.mjs"

# TypeScript serves over a Unix socket (`net.createServer` + `attachNodeSocket`),
# the Rust UDS transport dials it.
interop-uds-ts-serves:
    @just _uds-leg "TypeScript serves over UDS → Rust battery" \
        "node {{justfile_directory()}}/ts/server.mjs" \
        "{{justfile_directory()}}/target/debug/conformance_client"

# One Unix-socket direction. Both runners take the socket path as their single
# argument, exactly where the WebSocket legs take a url, so this only has to
# *choose* the path and hand it to `_interop` — which greps the same
# `LISTENING` line and requires the same `BATTERY OK`.
#
# The path matters: `sun_path` is 104 bytes on Darwin (108 on Linux) and the
# UDS transport appends a `.<pid>.<seq>.tmp` staging suffix before renaming,
# so a socket under `$TMPDIR` — `/var/folders/…` on macOS, ~50 characters
# before anything of ours — is uncomfortably close. An explicit short template
# under `/tmp` keeps the whole path around 20 bytes.
_uds-leg label server client:
    #!/usr/bin/env bash
    set -euo pipefail
    dir=$(mktemp -d /tmp/plc.XXXXXX)
    trap 'rm -rf "$dir"' EXIT
    just _interop "{{label}}" "{{server}} $dir/s" "{{client}}"

# One direction: start SERVER, wait for its `LISTENING <address>` line, point
# CLIENT at that address, require `BATTERY OK`. The address is a url or a
# socket path — both runners on both sides dispatch on its shape, so this
# recipe never has to know which transport it is driving.
#
# Both binaries are *built* and then spawned rather than run via `cargo run`:
# killing `cargo run` leaves the child alive as an orphan holding the port.
_interop label server client:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "interop: {{label}}"
    cargo build --quiet -p peerline-conformance --features ws,uds \
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
