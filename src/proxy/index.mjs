// Senior proxy HTTP server (phase D) — gateway in front of the senior model.
//
// Runtime component (unlike the harvest/seed track, which is build-time infra):
// the app's cloud "Assistente Sr" depends on this being up. The offline path
// (on-device model) never touches this proxy, so offline-first is preserved.
//
// One route: POST /v1/senior  { messages, language } -> { answer }
//            GET  /health      -> { ok: true }
//
// Deliberately dependency-light — plain node:http, no framework — matching the
// rest of obdient-seed (only hypercore/hyperswarm/b4a). Node >=20 gives us fetch.
//
// Env:
//   NVIDIA_API_KEY   (required) senior credential — NEVER commit it
//   PORT             (default 8787)
//   ALLOWED_ORIGIN   (default '*') CORS origin for the app
//   RATE_PER_MIN     (default 20) requests per IP per minute

import http from 'node:http';
import { askSenior } from './senior.mjs';

const PORT = Number(process.env.PORT ?? 8787);
const API_KEY = process.env.NVIDIA_API_KEY ?? '';
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN ?? '*';
const RATE_PER_MIN = Number(process.env.RATE_PER_MIN ?? 20);

const MAX_BODY_BYTES = 128 * 1024;   // a redacted case is a few KB; cap abuse
const MAX_MESSAGES = 60;             // matches the app's MAX_SENIOR_TURNS budget
const REQUEST_TIMEOUT_MS = 60_000;

if (!API_KEY) {
  console.error('[proxy] NVIDIA_API_KEY is not set — refusing to start.');
  process.exit(1);
}

// ── Minimal in-memory rate limit (per IP, sliding 60s window). Good enough for a
//    single-instance beta; move to a shared store if the proxy is ever scaled out.
const hits = new Map();
function rateLimited(ip) {
  const now = Date.now();
  const windowStart = now - 60_000;
  const arr = (hits.get(ip) ?? []).filter((t) => t > windowStart);
  arr.push(now);
  hits.set(ip, arr);
  return arr.length > RATE_PER_MIN;
}
// Periodic GC so the map doesn't grow unbounded.
setInterval(() => {
  const cutoff = Date.now() - 60_000;
  for (const [ip, arr] of hits) {
    const kept = arr.filter((t) => t > cutoff);
    if (kept.length === 0) hits.delete(ip);
    else hits.set(ip, kept);
  }
}, 60_000).unref();

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(body);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > MAX_BODY_BYTES) {
        reject(new Error('payload too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

// Validates the app payload. Returns { messages, language } or throws.
function parsePayload(raw) {
  let json;
  try {
    json = JSON.parse(raw);
  } catch {
    throw new Error('invalid JSON');
  }
  const { messages, language } = json ?? {};
  if (!Array.isArray(messages) || messages.length === 0 || messages.length > MAX_MESSAGES) {
    throw new Error('messages must be a non-empty array within budget');
  }
  for (const m of messages) {
    if (
      m == null ||
      (m.role !== 'user' && m.role !== 'assistant') ||
      typeof m.content !== 'string' ||
      m.content.length === 0 ||
      m.content.length > 8000
    ) {
      throw new Error('each message needs role user|assistant and non-empty content');
    }
  }
  const lang = language === 'pt' || language === 'es' || language === 'en' ? language : undefined;
  return { messages, language: lang };
}

const server = http.createServer(async (req, res) => {
  cors(res);

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === 'GET' && req.url === '/health') {
    sendJson(res, 200, { ok: true });
    return;
  }

  if (req.method !== 'POST' || req.url !== '/v1/senior') {
    sendJson(res, 404, { error: 'not found' });
    return;
  }

  // Trust the platform's proxy header when present (Render/Railway/Fly set it),
  // else the socket address.
  const ip =
    (req.headers['x-forwarded-for']?.toString().split(',')[0].trim()) ||
    req.socket.remoteAddress ||
    'unknown';

  if (rateLimited(ip)) {
    sendJson(res, 429, { error: 'rate limited' });
    return;
  }

  let payload;
  try {
    payload = parsePayload(await readBody(req));
  } catch (err) {
    sendJson(res, 400, { error: err instanceof Error ? err.message : 'bad request' });
    return;
  }

  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), REQUEST_TIMEOUT_MS);
  try {
    const answer = await askSenior(payload, { apiKey: API_KEY, signal: ac.signal });
    sendJson(res, 200, { answer });
  } catch (err) {
    // Generic to the client — provider details stay in the server logs.
    const aborted = err instanceof Error && err.name === 'AbortError';
    sendJson(res, aborted ? 504 : 502, { error: 'senior unavailable' });
  } finally {
    clearTimeout(timer);
  }
});

server.listen(PORT, () => {
  console.log(`[proxy] senior proxy listening on :${PORT} (rate ${RATE_PER_MIN}/min, origin ${ALLOWED_ORIGIN})`);
});
