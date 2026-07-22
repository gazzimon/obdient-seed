// Combined launcher — runs BOTH long-lived services in one deployment:
//   - the harvest seed daemon (src/seed/index.mjs): P2P DHT, persistent feeds
//   - the senior proxy      (src/proxy/index.mjs):  HTTP gateway to the senior model
//
// Why one host: the seed is a stateful P2P daemon that needs an always-on box
// with a persistent disk (Hyperswarm + data/feeds) — it does NOT fit scale-to-zero
// serverless. Since that box exists anyway, the proxy rides along and they share
// the same `data/` dir, which is what phase D.2 wants (proxy as a second transport
// into the same ingest store). See src/proxy/README.md.
//
// Each service is spawned as its own child process (isolation: a proxy crash must
// not take the P2P daemon down, and vice-versa). If either exits, we log it and
// exit non-zero so the host's supervisor (systemd / Docker restart policy /
// Compute Engine) restarts the whole unit cleanly.
//
// Usage:  node src/serve.mjs [--data <dir>]
//   env:  NVIDIA_API_KEY (required by the proxy), PORT, ALLOWED_ORIGIN, RATE_PER_MIN

import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));

// Pass a shared --data dir through to the seed so both services agree on it.
const dataIdx = process.argv.indexOf('--data');
const dataArgs = dataIdx >= 0 && process.argv[dataIdx + 1]
  ? ['--data', process.argv[dataIdx + 1]]
  : [];

const services = [
  { name: 'seed', entry: path.join(HERE, 'seed', 'index.mjs'), args: dataArgs },
  { name: 'proxy', entry: path.join(HERE, 'proxy', 'index.mjs'), args: [] },
];

const children = [];
let shuttingDown = false;

function prefixLines(name, buf) {
  const text = buf.toString();
  for (const line of text.split(/\r?\n/)) {
    if (line.length > 0) console.log(`[${name}] ${line}`);
  }
}

function start({ name, entry, args }) {
  const child = spawn(process.execPath, [entry, ...args], {
    env: process.env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout.on('data', (b) => prefixLines(name, b));
  child.stderr.on('data', (b) => prefixLines(name, b));
  child.on('exit', (code, signal) => {
    if (shuttingDown) return;
    console.error(`[serve] "${name}" exited (code ${code}, signal ${signal}) — stopping the unit for a clean restart.`);
    shutdown(code ?? 1);
  });
  children.push({ name, child });
  console.log(`[serve] started "${name}" (pid ${child.pid})`);
}

function shutdown(code) {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const { child } of children) {
    if (!child.killed) child.kill('SIGTERM');
  }
  // Give children a moment to close sockets/feeds, then exit.
  setTimeout(() => process.exit(code), 2000).unref();
}

process.on('SIGINT', () => { console.log('\n[serve] SIGINT'); shutdown(0); });
process.on('SIGTERM', () => { console.log('\n[serve] SIGTERM'); shutdown(0); });

for (const svc of services) start(svc);
