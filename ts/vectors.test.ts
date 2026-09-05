/**
 * The shared cross-language conformance vectors.
 *
 * `vectors.json` lives at the repo root and is consumed by *both*
 * implementations: `rust/tests/vectors.rs` on the Rust side and this
 * spec on the TypeScript side. That's the point — the two are pinned to each
 * other, not each to its own suite.
 *
 * The Rust runner only asserts the disposition (accepted / rejected), since
 * serde phrases and groups its rejections differently. This side goes
 * further and pins the `category` too, because the categories are ours.
 *
 * Entries flagged `tsOnly` encode a TypeScript-side restriction Rust doesn't
 * share (ids past 2^53, which are perfectly good `u64`s); the Rust runner
 * skips them, this one doesn't.
 */

import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { classify, parseFrame, serializeFrame, type Inbound } from "../../peerline-ts/src/peer.js";
import { parseFrameText, type RejectCategory, type WireFrame } from "../../peerline-ts/src/wire.js";

interface ValidExpect {
  valid: true;
  kind: "req" | "resp-ok" | "resp-err" | "notif" | "stream";
  id?: number | null;
  op?: string;
  seq?: number;
  args?: Record<string, unknown>;
  argsAbsent?: true;
  data?: unknown;
  dataAbsent?: true;
  err?: { code: number; msg: string; data?: unknown };
  errAbsent?: true;
}

interface InvalidExpect {
  valid: false;
  category: RejectCategory;
}

interface Vector {
  name: string;
  frame: string;
  expect: ValidExpect | InvalidExpect;
  tsOnly?: boolean;
  note?: string;
}

const vectors = JSON.parse(
  readFileSync(new URL("../vectors.json", import.meta.url), "utf8"),
) as Vector[];

describe("conformance vectors", () => {
  it("loads the shared vector file", () => {
    expect(vectors.length).toBeGreaterThanOrEqual(100);
  });

  for (const vector of vectors) {
    const { name, frame: text, expect: want } = vector;

    if (!want.valid) {
      it(`rejects ${name} (${want.category})`, () => {
        const result = parseFrameText(text);
        if (result.ok) {
          throw new Error(`expected a rejection, but the frame parsed: ${text}`);
        }
        expect(result.rejection.category).toBe(want.category);
        expect(result.rejection.message).toBeTruthy();

        // …and the peer-level entry point turns it into the ready-to-send
        // parse-error reply.
        const parsed = parseFrame(text);
        expect(parsed.ok).toBeUndefined();
        expect(parsed.err).toEqual({
          ver: "1",
          kind: "resp",
          id: null,
          err: { code: -32600, msg: expect.any(String) },
        });
      });
      continue;
    }

    it(`accepts ${name}`, () => {
      const parsed = parseFrame(text);
      if (parsed.err !== undefined) {
        throw new Error(`expected a valid frame, got: ${parsed.err.err.msg}`);
      }
      const frame = parsed.ok;
      checkFields(frame, want);

      // Re-encoding a validated frame and parsing it back is a fixed point:
      // normalization dropped everything the wire doesn't carry.
      const round = parseFrame(serializeFrame(frame));
      expect(round.ok).toEqual(frame);
    });
  }
});

function checkFields(frame: WireFrame, want: ValidExpect): void {
  const inbound: Inbound = classify(frame);

  switch (want.kind) {
    case "req": {
      if (inbound.kind !== "request") throw new Error(`classified as ${inbound.kind}`);
      const { request } = inbound;
      if (want.id !== undefined) expect(request.id).toBe(want.id);
      if (want.op !== undefined) expect(request.op).toBe(want.op);
      checkArgs(request.args, want);
      return;
    }
    case "notif": {
      if (inbound.kind !== "notification") throw new Error(`classified as ${inbound.kind}`);
      const { notification } = inbound;
      if (want.op !== undefined) expect(notification.op).toBe(want.op);
      checkArgs(notification.args, want);
      return;
    }
    case "resp-ok": {
      if (inbound.kind !== "response") throw new Error(`classified as ${inbound.kind}`);
      expect(inbound.outcome.ok).toBe(true);
      if (want.id !== undefined) expect(inbound.id).toBe(want.id);
      if (!inbound.outcome.ok) return;
      if ("data" in want) expect(inbound.outcome.data).toEqual(want.data);
      return;
    }
    case "resp-err": {
      if (inbound.kind !== "response") throw new Error(`classified as ${inbound.kind}`);
      expect(inbound.outcome.ok).toBe(false);
      if (want.id !== undefined) expect(inbound.id).toBe(want.id);
      if (inbound.outcome.ok) return;
      checkErr(inbound.outcome.error, want);
      return;
    }
    case "stream": {
      if (inbound.kind !== "stream") throw new Error(`classified as ${inbound.kind}`);
      const stream = inbound.frame;
      if (want.id !== undefined) expect(stream.id).toBe(want.id);
      if (want.seq !== undefined) expect(stream.seq).toBe(want.seq);
      if ("data" in want) {
        expect("data" in stream).toBe(true);
        expect(stream.data).toEqual(want.data);
      }
      if (want.dataAbsent) expect("data" in stream).toBe(false);
      checkErr(stream.err, want);
      return;
    }
  }
}

function checkArgs(args: Record<string, unknown> | undefined, want: ValidExpect): void {
  if (want.args !== undefined) {
    expect(args).toEqual(want.args);
  }
  if (want.argsAbsent) {
    expect(args).toBeUndefined();
    // Absent, not present-and-undefined: the key must not exist at all.
    expect(args === undefined).toBe(true);
  }
}

function checkErr(
  err: { code: number; msg: string; data?: unknown } | undefined,
  want: ValidExpect,
): void {
  if (want.err !== undefined) {
    if (err === undefined) throw new Error("expected err, frame carried none");
    expect(err.code).toBe(want.err.code);
    expect(err.msg).toBe(want.err.msg);
    // An `err` expectation is exhaustive about `data`: stated means present
    // and equal, unstated means absent (an explicit null collapses to absent
    // on both sides).
    if ("data" in want.err) {
      expect(err.data).toEqual(want.err.data);
    } else {
      expect("data" in err).toBe(false);
    }
  }
  if (want.errAbsent) expect(err).toBeUndefined();
}
