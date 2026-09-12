# Local CI for the peerline family. List with `just`.
#
# This file is versioned here as `family.justfile` but *runs* from the parent
# folder, above every repository, via a symlink:
#
#     cd .. && ln -s peerline-conformance/family.justfile justfile
#
# Every path below is relative to that parent folder — running it from inside
# this repository will not work. It lives up there because its job is the one
# no single repository can do: run the checks that span them.
# CI here is executed locally, by hand — there is no upstream automation, so
# these recipes *are* the gate. Two tiers:
#
#     just quick     after any edit that could move the cross-language
#                    contract — seconds, no battery
#     just verify    before a release, or after a batch of changes —
#                    every repository's full suite plus the conformance
#                    matrix and the live interop battery
#
# Layout expected (the same one peerline-conformance documents):
#
#     peerline/                  this folder
#     ├── peerline/              the Rust implementation
#     ├── peerline-ts/           the TypeScript implementation
#     ├── peerline-conformance/  the suite, which depends on both
#     ├── peerline-host/         the service layer
#     └── peerline-cluster/      the supervisor and coordinator
#
# pnpm note: both TypeScript packages pin `packageManager` to pnpm 10, and
# pnpm self-switches to the pinned version, so a newer pnpm on PATH is fine.

default:
    @just --list --unsorted

# --- the fast gate ---
# Everything here is cheap enough to run on every cross-cutting edit: the
# golden vectors through both parsers, the generated-bindings drift check
# (side-effect free — it exports to scratch and diffs), and the TypeScript
# typecheck. No battery, no full test suites.

quick: vectors bindings-check ts-typecheck
    @echo "quick: green"

vectors:
    just -f peerline-conformance/justfile vectors

bindings-check:
    just -f peerline-conformance/justfile bindings-check

ts-typecheck:
    pnpm --dir peerline-ts typecheck

# --- the full gate ---
# Every repository's own suite and lints, then the cross-transport matrix and
# the live cross-language battery. This is the release gate: nothing ships
# from any of these repositories without a green `verify`.

verify: quick test-rust test-ts clippy matrix interop
    @echo "verify: green"

# The three Rust workspaces' full test suites.
test-rust:
    just -f peerline/justfile test
    just -f peerline-host/justfile test
    just -f peerline-cluster/justfile test

test-ts:
    pnpm --dir peerline-ts test

clippy:
    just -f peerline/justfile clippy
    just -f peerline-host/justfile clippy
    just -f peerline-cluster/justfile clippy

# The battery over every Rust transport, from the conformance repo.
matrix:
    just -f peerline-conformance/justfile matrix

# The live cross-language battery, both directions.
interop:
    just -f peerline-conformance/justfile interop
