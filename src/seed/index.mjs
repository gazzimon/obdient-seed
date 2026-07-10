// OBDient harvest seed peer — phase-C transport daemon (PROTOCOL.md).
//
// Runs on a PC/VPS. Joins the harvest DHT topic as SERVER only, replicates
// contributor feeds READ-ONLY, and persists them under ./data so harvest.mjs
// can export corrections.jsonl offline.
//
// The seed is a SINK, never a runtime dependency: if it's down, devices keep
// their local feeds (store-and-forward is built into Hypercore) and sync
// whenever the seed reappears. Privacy: feeds carry CaseChunks only — redacted
// briefs, no VIN / BT address / identity (contract: PROTOCOL.md).
//
// Usage:  node src/seed/index.mjs [--data <dir>]

import Hypercore from 'hypercore';
import Hyperswarm from 'hyperswarm';
import b4a from 'b4a';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { harvestTopic, readKeyPreamble } from './wire.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(HERE, '..', '..');

function parseDataDir() {
  const idx = process.argv.indexOf('--data');
  return idx >= 0 && process.argv[idx + 1]
    ? path.resolve(process.argv[idx + 1])
    : path.join(ROOT, 'data');
}

const DATA_DIR = parseDataDir();
const FEEDS_DIR = path.join(DATA_DIR, 'feeds');
const KEYS_FILE = path.join(DATA_DIR, 'keys.json');

// Abuse caps — the harvest topic is announced on a PUBLIC DHT, so every
// resource a stranger can grow must be bounded: feeds on disk, blocks per
// feed (plus the preamble timeout in wire.mjs). Generous vs. real devices,
// which append a handful of cases per day; bump deliberately if ever hit.
const MAX_FEEDS = 512;
const MAX_FEED_BLOCKS = 10_000;

// keys.json — registry of every contributor feed ever seen, so harvest.mjs
// can reopen them offline and re-replication resumes across restarts.
function loadKeys() {
  try {
    return JSON.parse(fs.readFileSync(KEYS_FILE, 'utf8'));
  } catch {
    return {};
  }
}

function saveKeys(keys) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  // Write-then-rename: a crash mid-write must not corrupt the registry
  // (loadKeys would silently return {} and the seed would forget every feed).
  const tmp = KEYS_FILE + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(keys, null, 2));
  fs.renameSync(tmp, KEYS_FILE);
}

const keys = loadKeys();
const cores = new Map(); // keyHex → Hypercore

function openCore(keyHex) {
  let core = cores.get(keyHex);
  if (!core) {
    // Passing the key makes this a read-only replica of the contributor feed.
    core = new Hypercore(path.join(FEEDS_DIR, keyHex), b4a.from(keyHex, 'hex'));
    // Replication is sparse by default — without a live download range the
    // seed only learns the feed LENGTH; block DATA is never fetched and
    // harvest.mjs finds nothing. end: -1 keeps fetching as the feed grows.
    core.download({ start: 0, end: -1 });
    // Growth log lives here (once per core), NOT per connection — reconnects
    // from the same contributor must not pile listeners onto the core.
    core.on('append', () => {
      console.log(
        `[seed] feed ${keyHex.slice(0, 16)}… grew to ${core.length} blocks`,
      );
    });
    cores.set(keyHex, core);
  }
  return core;
}

async function main() {
  fs.mkdirSync(FEEDS_DIR, { recursive: true });

  // Reopen known feeds so their replication resumes on reconnect.
  for (const keyHex of Object.keys(keys)) {
    await openCore(keyHex).ready();
  }
  console.log(`[seed] data dir: ${DATA_DIR}`);
  console.log(`[seed] known contributor feeds: ${Object.keys(keys).length}`);

  const swarm = new Hyperswarm();

  swarm.on('connection', (socket) => {
    const peer = socket.remotePublicKey
      ? b4a.toString(socket.remotePublicKey, 'hex').slice(0, 8)
      : 'unknown';
    console.log(`[seed] peer connected: ${peer}… (${swarm.connections.size} open)`);

    readKeyPreamble(socket)
      .then(async (key) => {
        const keyHex = b4a.toString(key, 'hex');
        if (!keys[keyHex]) {
          if (Object.keys(keys).length >= MAX_FEEDS) {
            console.warn(
              `[seed] feed cap (${MAX_FEEDS}) reached — refusing new contributor ${keyHex.slice(0, 16)}…`,
            );
            socket.destroy();
            return;
          }
          keys[keyHex] = { firstSeen: new Date().toISOString() };
          saveKeys(keys);
          console.log(`[seed] NEW contributor feed: ${keyHex.slice(0, 16)}…`);
        }
        const core = openCore(keyHex);
        await core.ready();

        if (core.length >= MAX_FEED_BLOCKS) {
          console.warn(
            `[seed] feed ${keyHex.slice(0, 16)}… is at the ${MAX_FEED_BLOCKS}-block cap — refusing replication`,
          );
          socket.destroy();
          return;
        }

        // Per-connection cap guard; removed on close so it doesn't accumulate.
        const onAppend = () => {
          if (core.length >= MAX_FEED_BLOCKS) {
            console.warn(
              `[seed] feed ${keyHex.slice(0, 16)}… hit the ${MAX_FEED_BLOCKS}-block cap — dropping connection`,
            );
            socket.destroy();
          }
        };
        core.on('append', onAppend);
        socket.on('close', () => core.off('append', onAppend));

        // Non-initiator replication over the remaining stream.
        core.replicate(socket);

        const before = core.length;
        await core.update({ wait: true }).catch(() => {});
        if (core.length > before) {
          console.log(
            `[seed] feed ${keyHex.slice(0, 16)}…: +${core.length - before} blocks (total ${core.length})`,
          );
        }
      })
      .catch((err) => {
        console.warn(`[seed] preamble failed from ${peer}…: ${err.message}`);
        socket.destroy();
      });

    socket.on('error', () => {}); // peer went away — normal churn
    socket.on('close', () =>
      console.log(`[seed] peer closed: ${peer}… (${swarm.connections.size} open)`),
    );
  });

  const discovery = swarm.join(harvestTopic(), { server: true, client: false });
  await discovery.flushed();
  console.log('[seed] joined harvest topic — announcing on the DHT. Ctrl-C to stop.');

  process.on('SIGINT', async () => {
    console.log('\n[seed] shutting down…');
    await swarm.destroy();
    for (const core of cores.values()) await core.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('[seed] fatal:', err);
  process.exit(1);
});
