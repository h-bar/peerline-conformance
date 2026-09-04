//! Shared cross-language conformance vectors.
//!
//! `vectors.json` at this repo's root is the golden frame set: raw
//! JSON frame *text* plus the disposition both implementations must agree on.
//! This test and `ts/vectors.test.ts` consume the same
//! file, so Rust and TS are pinned to each other rather than each to its own
//! test suite.
//!
//! ### Entry schema
//!
//! ```jsonc
//! {
//!   "name": "resp-ok-null-data",
//!   "frame": "{\"ver\":\"1\",\"kind\":\"resp\",\"id\":7,\"data\":null}",
//!   "expect": { "valid": true, "kind": "resp-ok", "id": 7, "data": null },
//!   "tsOnly": false,   // optional
//!   "note": "…"        // optional, documentation only
//! }
//! ```
//!
//! `kind` is one of `req` / `resp-ok` / `resp-err` / `notif` / `stream`.
//! Field expectations are optional and only asserted when present; the
//! `argsAbsent` / `dataAbsent` / `errAbsent` flags assert the opposite.
//! Invalid entries carry a coarse `category` instead — **not** asserted here:
//! serde and TypeScript phrase and group their rejections differently, so the
//! cross-language contract is the *disposition*, and only the TS side pins
//! the category.
//!
//! `tsOnly` entries are skipped: they encode a TypeScript-side restriction
//! Rust doesn't share (ids past 2^53, which are legal `u64`s).

use peerline::peer::{self, InboundKind};
use peerline::wire::{ErrorType, Frame, RawJson, RpcError};
use serde_json::{Map, Value};

const VECTORS: &str = include_str!("../../vectors.json");

#[test]
fn conformance_vectors_match_the_wire_contract() {
    let entries: Vec<Value> = serde_json::from_str(VECTORS).expect("vectors.json is valid JSON");
    assert!(
        entries.len() >= 100,
        "the vector set shrank unexpectedly: {} entries",
        entries.len()
    );

    let mut checked = 0usize;
    for entry in &entries {
        let name = str_field(entry, "name");
        if entry
            .get("tsOnly")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            continue;
        }
        let text = str_field(entry, "frame");
        let expect = entry
            .get("expect")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("{name}: expect must be an object"));
        let valid = expect
            .get("valid")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| panic!("{name}: expect.valid must be a bool"));

        match peer::parse_frame(text) {
            Ok(frame) => {
                assert!(valid, "{name}: expected a rejection, but the frame parsed");
                check_valid(name, expect, frame);
            }
            Err(reply) => {
                let error = reply.error().expect("parse_frame replies with an error");
                assert!(
                    !valid,
                    "{name}: expected a valid frame, got rejection {:?}",
                    error.message
                );
                // The reply is the ready-to-send parse-error response: the id
                // could not be trusted, so it is null.
                assert_eq!(
                    reply.id(),
                    None,
                    "{name}: rejection reply must carry id null"
                );
                assert_eq!(
                    error.error_type(),
                    ErrorType::InvalidRequest,
                    "{name}: rejection reply must be InvalidRequest"
                );
            }
        }
        checked += 1;
    }

    assert!(checked >= 100, "only {checked} vectors ran");
}

/// Assert a parsed frame against the vector's field expectations.
fn check_valid(name: &str, expect: &Map<String, Value>, frame: Frame) {
    let kind = expect
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("{name}: valid vectors need a kind"));

    match (kind, peer::classify(frame)) {
        ("req", InboundKind::IncomingRequest(r)) => {
            check_u64(name, "id", expect, r.id);
            check_str(name, "op", expect, &r.op);
            check_args(name, expect, r.args);
        }
        ("notif", InboundKind::IncomingNotification(n)) => {
            check_str(name, "op", expect, &n.op);
            check_args(name, expect, n.args);
        }
        ("resp-ok", InboundKind::Response { id, outcome }) => {
            check_opt_id(name, expect, id);
            let data = outcome
                .unwrap_or_else(|e| panic!("{name}: expected an Ok response, got error {e:?}"));
            check_payload(name, expect, Some(&data));
        }
        ("resp-err", InboundKind::Response { id, outcome }) => {
            check_opt_id(name, expect, id);
            match outcome {
                Err(e) => check_error(name, expect, Some(&e)),
                Ok(v) => panic!("{name}: expected an error response, got Ok({v:?})"),
            }
        }
        ("stream", InboundKind::Stream(s)) => {
            check_u64(name, "id", expect, s.id);
            if let Some(seq) = expect.get("seq").and_then(Value::as_i64) {
                assert_eq!(s.seq, seq, "{name}: seq mismatch");
            }
            check_payload(name, expect, s.data.as_ref());
            check_error(name, expect, s.error.as_ref());
        }
        (other, got) => panic!("{name}: expected kind {other}, classified as {got:?}"),
    }
}

fn check_args(name: &str, expect: &Map<String, Value>, args: Option<Map<String, Value>>) {
    if let Some(want) = expect.get("args") {
        let got = args.unwrap_or_else(|| panic!("{name}: expected args, frame carried none"));
        assert_eq!(&Value::Object(got), want, "{name}: args mismatch");
    } else if expect.contains_key("argsAbsent") {
        assert!(args.is_none(), "{name}: expected no args");
    }
}

fn check_payload(name: &str, expect: &Map<String, Value>, data: Option<&RawJson>) {
    if let Some(want) = expect.get("data") {
        let got = data.unwrap_or_else(|| panic!("{name}: expected data, frame carried none"));
        assert_eq!(
            &got.to_value().expect("payload is valid JSON"),
            want,
            "{name}: data mismatch"
        );
    } else if expect.contains_key("dataAbsent") {
        assert!(data.is_none(), "{name}: expected no data");
    }
}

fn check_error(name: &str, expect: &Map<String, Value>, error: Option<&RpcError>) {
    if let Some(want) = expect.get("err").and_then(Value::as_object) {
        let got = error.unwrap_or_else(|| panic!("{name}: expected err, frame carried none"));
        assert_eq!(
            i64::from(got.code),
            want.get("code").and_then(Value::as_i64).expect("err.code"),
            "{name}: err.code mismatch"
        );
        assert_eq!(
            got.message.as_str(),
            want.get("msg").and_then(Value::as_str).expect("err.msg"),
            "{name}: err.msg mismatch"
        );
        // An `err` expectation is exhaustive about `data`: stated means
        // present and equal, unstated means absent (an explicit `data: null`
        // collapses to absent on both sides).
        match want.get("data") {
            Some(data) => assert_eq!(got.data.as_ref(), Some(data), "{name}: err.data mismatch"),
            None => assert!(got.data.is_none(), "{name}: expected no err.data"),
        }
    } else if expect.contains_key("errAbsent") {
        assert!(error.is_none(), "{name}: expected no err");
    }
}

fn check_u64(name: &str, field: &str, expect: &Map<String, Value>, got: u64) {
    if let Some(want) = expect.get(field) {
        let want = want
            .as_u64()
            .unwrap_or_else(|| panic!("{name}: expect.{field} must be a non-negative integer"));
        assert_eq!(got, want, "{name}: {field} mismatch");
    }
}

/// Response ids are `Option<Id>` — a vector states `null` for the
/// id-could-not-be-recovered case.
fn check_opt_id(name: &str, expect: &Map<String, Value>, got: Option<u64>) {
    match expect.get("id") {
        None => {}
        Some(Value::Null) => assert_eq!(got, None, "{name}: expected id null"),
        Some(want) => {
            let want = want
                .as_u64()
                .unwrap_or_else(|| panic!("{name}: bad expect.id"));
            assert_eq!(got, Some(want), "{name}: id mismatch");
        }
    }
}

fn check_str(name: &str, field: &str, expect: &Map<String, Value>, got: &str) {
    if let Some(want) = expect.get(field).and_then(Value::as_str) {
        assert_eq!(got, want, "{name}: {field} mismatch");
    }
}

fn str_field<'a>(entry: &'a Value, field: &str) -> &'a str {
    entry
        .get(field)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("every vector needs a string {field}"))
}
