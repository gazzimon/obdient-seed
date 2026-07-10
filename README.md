# obdient-seed

Central hub for [OBDient](https://github.com/gazzimon/OBDient) — **build-time
infrastructure, never a runtime dependency of the app**. If this hub is down,
devices lose nothing: they keep appending cases to their local feeds
(store-and-forward is built into Hypercore) and sync when it reappears.

| Phase | Component | Status |
|-------|-----------|--------|
| C | **Harvest seed peer** (`src/seed/`) — P2P collection of validated case pairs | ✅ working |
| C | **Ingest store** (`src/ingest/`) — id + gate re-check, dedup+merge, `corrections.jsonl` export | ✅ working |
| D | **Senior proxy** (`src/proxy/`) — gateway in front of Claude; same store, second transport | 🔲 phase 2 |
| — | Curation/publisher pipeline (ADR-0002: signed RAG bundle + batch distillation) | 🔲 later |

The wire & data contract with the app lives in **[PROTOCOL.md](PROTOCOL.md)**
(source of truth). Architecture rationale: PLAN-002 v2 §5 and ADR-0002/0003 in
the OBDient repo.

## Run

```bash
npm install

npm run seed        # seed daemon — joins the harvest DHT topic, replicates feeds
npm run harvest     # offline: replicated feeds → out/corrections.jsonl
npm test            # local self-test (no network needed)
```

End-to-end demo over the real DHT (two terminals):

```bash
npm run seed                      # terminal 1
node test/sim-client.mjs          # terminal 2 — simulates an OBDient device
npm run harvest                   # then: inspect out/corrections.jsonl
```

## Privacy

Feeds carry `CaseChunk`s only: redacted brief (no VIN by construction),
gate-checked senior answer, optional outcome. Never: VIN, Bluetooth address,
user identity, raw conversations. Contribution is opt-in on-device; the hub
**re-checks the gate verdict on ingest** (defense in depth). See PROTOCOL.md.

## Layout

- `src/seed/wire.mjs` — OBDIENT-HARVEST/1 preamble + topic helpers
- `src/seed/index.mjs` — seed daemon (feeds persisted under `data/`)
- `src/ingest/store.mjs` — transport-agnostic case store (C **and** D feed it)
- `src/ingest/harvest.mjs` — CLI: feeds → store → `out/corrections.jsonl`
- `src/proxy/` — phase D slot (see its README)
- `test/replication.test.mjs` — self-test incl. outcome-merge policy
- `test/sim-client.mjs` — device simulator; reference for the app's Bare worklet
