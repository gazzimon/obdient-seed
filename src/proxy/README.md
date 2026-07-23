# src/proxy — senior proxy (phase D)

Gateway in front of the senior model. The OBDient app's cloud assistant
("Assistente Sr") calls this instead of holding a provider key on-device — it
**replaces the BYOK path** (`claude-api.datasource.ts`) for the Beta V2 launch.

> **Provider note:** originally specified in front of the Claude API. As of the
> Beta V2 plan the senior model is **NVIDIA Nemotron** (`senior.mjs`). The
> provider/model is server-side only and **must never reach the client** — the
> app UI knows exactly one name, "Assistente Sr".

## Run

```bash
export NVIDIA_API_KEY=...      # senior credential — NEVER commit it
npm run proxy                  # listens on :8787 (PORT overridable)
```

Health check: `GET /health` → `{ "ok": true }`.

### HTTPS (required for the mobile app)

Android/iOS reject cleartext HTTP, so the app must reach the proxy over HTTPS.
Put **Caddy** in front (repo-root `Caddyfile`): it terminates TLS on :443 and
reverse-proxies to `localhost:8787`. A bare IP can't get a public cert, so the
`Caddyfile` uses an **sslip.io** hostname that resolves to the VM IP, letting
Caddy auto-provision a Let's Encrypt cert.

```bash
# VM: open ports 80 + 443, keep the proxy on :8787, then:
caddy run --config ./Caddyfile
```

The app sets `EXPO_PUBLIC_SENIOR_PROXY_URL=https://<ip>.sslip.io`.

## API

`POST /v1/senior`

```jsonc
// request  — already redacted upstream (no VIN/plate/BT/identity)
{
  "messages": [ { "role": "user", "content": "<brief + turns>" }, ... ],
  "language": "pt" | "es" | "en"        // optional hint; senior mirrors it anyway
}
// response
{ "answer": "…" }                        // reasoning_content stripped
```

Errors are generic to the client (`400` bad payload, `429` rate limited,
`502`/`504` senior unavailable); provider detail stays in server logs.

## Env

| Var | Default | Notes |
|-----|---------|-------|
| `NVIDIA_API_KEY` | — (**required**) | senior credential; refuses to start without it |
| `PORT` | `8787` | HTTP port |
| `ALLOWED_ORIGIN` | `*` | CORS origin for the app |
| `RATE_PER_MIN` | `20` | requests per IP per minute |

## Design constraints (fixed — PLAN-002 v2 §5)

- The proxy is a **second transport into the same ingest store**
  (`../ingest/store.mjs`) — it must NOT grow its own case storage or export.
- Flow: device sends the redacted brief → proxy forwards to the senior model
  (server-side key) → returns the senior answer.
- Never receives: VIN, Bluetooth address, user identity (same contract as the
  BYOK path it replaces).
- Offline-first is not violated: the senior call already requires network by
  definition; the local diagnostic path never touches this proxy.

## Not yet wired (phase D.2)

- **Inline `store.addCase()`.** The store rejects any case with
  `gate.passed !== true`, and the deterministic gate lives on-device
  (`diagnostic-gate.ts`). To store proxy-generated cases as a second transport we
  must **port the gate to Node** first. Until then the launch relies on the
  existing on-device P2P harvest (opt-in `contributeCases`) to capture cases — the
  proxy is a **pure forwarder**.
- **Streaming.** v1 awaits the full reply. If the app renders tokens
  incrementally later, stream `content` only and keep discarding `reasoning_content`.
