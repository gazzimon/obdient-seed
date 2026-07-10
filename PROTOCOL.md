# OBDIENT-HARVEST/1 — wire & data contract

**This file is the source of truth** for the contract between the OBDient app
(device side: `gazzimon/OBDient`, `src/data/knowledge/distributed-chunk.ts` +
the Bare harvest worklet) and this hub. Breaking changes bump the protocol
version (`OBDIENT-HARVEST/2`) and/or the `CaseChunk.v` schema version.

## Transport (phase C — P2P seed)

- **DHT topic:** `obdient-harvest-v1`, UTF-8, padded with `\0` to exactly
  32 bytes (same convention as the knowledge topic `obdient-rag-v1`).
- **Roles:** devices join as *client* only; the seed joins as *server* only.
- **Connection layout** over the Hyperswarm secret stream:

```
[32 bytes]  contributor's Hypercore public key   (fixed-size preamble, no framing)
[rest…]     standard Hypercore replication stream for that core
```

- The contributor writes the preamble, then replicates as **initiator**; the
  seed reads exactly 32 bytes (pushing any over-read remainder back with
  `unshift()` — pause the stream first), then replicates as non-initiator.
- Topic separation is deliberate (data minimization): cases flow **up** only to
  the seed; curated knowledge flows **down** via the signed bundle (ADR-0002).
  Peers never replicate each other's cases.

## Data: `CaseChunk` (schema v1)

One validated diagnostic case, appended as a JSON block to the contributor's
append-only feed (the feed IS the outbox — store-and-forward is Hypercore's).

```jsonc
{
  "type": "case",
  "v": 1,                      // schema version (absent ⇒ treated as 1)
  "id": "<sha256(briefJson + seniorAnswer)>",  // content-addressed dedup key
  "brief": { /* redacted DiagnosticBrief — no VIN by construction */ },
  "seniorAnswer": "…",
  "gate": {                    // deterministic gate verdict at capture time
    "passed": true,
    "violations": [ { "rule": "G1", "weight": "hard", "detail": "…" } ]
  },
  "outcome": "yes" | "no" | "pending" | null,   // UX4, may arrive LATER
  "appVersion": "0.9.x",       // optional
  "createdAt": "<ISO-8601>"
}
```

**Privacy contract (ADR-0002/0003):** never VIN, Bluetooth address, user
identity, or raw conversations. Contribution is opt-in on-device; only
gate-passed cases should be appended — and the hub **re-checks** (`gate.passed`)
on ingest (defense in depth).

## Ingest semantics (transport-agnostic — phases C *and* D)

The store (`src/ingest/store.mjs`) applies the same policy regardless of how a
case arrives (P2P replication today, HTTP proxy capture in phase D):

1. **Shape check:** `type === 'case'`, string `id`, `v` missing or `1`.
2. **Gate re-check:** `gate.passed === true` or the case is rejected.
3. **Dedup + MERGE by `id`:** `outcome` is NOT part of the content hash, so an
   enriched re-append (outcome captured days later, offline) keeps the same id.
   Merge policy: a record with non-null `outcome` **wins** over null; if both
   have (or both lack) an outcome, the newer `createdAt` wins.
4. **Export:** `corrections.jsonl` (ADR-0002 Phase 1 — distillation input):

```jsonl
{ "case_id": "…", "brief": {…}, "senior_answer": "…",
  "gate": {…}, "outcome": "yes"|null, "app": "…"|null, "observed_at": "…" }
```

## Phase D (senior proxy — future)

The proxy forwards `brief → Claude` and calls `store.addCase()` inline with a
null outcome; the enriched outcome still arrives via the P2P path (or a later
sync) and merges by id. Same store, second transport — nothing else changes.