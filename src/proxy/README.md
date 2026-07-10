# src/proxy — senior proxy (phase D, not yet implemented)

Decision (PLAN-002 v2 §5, 2026-07-10): build **after** the P2P track (C) works
end-to-end. Only makes sense with a subsidized/B2B product model — BYOK already
solved API-key security on-device (OBDient audit C1).

## Design constraints (already fixed)

- The proxy is a **second transport into the same ingest store**
  (`../ingest/store.mjs`) — it must NOT grow its own case storage or export.
- Flow: device sends the redacted brief → proxy forwards to the Claude API
  (server-side key) → returns the senior answer → calls `store.addCase()`
  inline with `outcome: null`.
- The enriched outcome (UX4, captured days later, offline) arrives via the P2P
  seed path and **merges by content-addressed id** — see PROTOCOL.md §Ingest.
- Offline-first is not violated: the senior call already requires network by
  definition; the local diagnostic path never touches this proxy.
- Never receives: VIN, Bluetooth address, user identity (same contract as the
  direct BYOK path in `claude-api.datasource.ts`).
