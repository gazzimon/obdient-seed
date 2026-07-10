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
  fs.writeFileSync(KEYS_FILE, JSON.stringify(keys, null, 2));
}

const keys = loadKeys();
const cores = new Map(); // keyHex → Hypercore

function openCore(keyHex) {
  let core = cores.get(keyHex);
  if (!core) {
    // Passing the key makes this a read-only replica of the contributor feed.
    core = new Hypercore(path.join(FEEDS_DIR, keyHex), b4a.from(keyHex, 'hex'));
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
    console.log(`[seed] peer connected: ${peer}…`);

    readKeyPreamble(socket)
      .then(async (key) => {
        const keyHex = b4a.toString(key, 'hex');
        if (!keys[keyHex]) {
          keys[keyHex] = { firstSeen: new Date().toISOString() };
          saveKeys(keys);
          console.log(`[seed] NEW contributor feed: ${keyHex.slice(0, 16)}…`);
        }
        const core = openCore(keyHex);
        await core.ready();

        // Non-initiator replication over the remaining stream.
        core.replicate(socket);

        const before = core.length;
        core.on('append', () => {
          console.log(
            `[seed] feed ${keyHex.slice(0, 16)}… grew to ${core.length} blocks`,
          );
        });
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
    socket.on('close', () => console.log(`[seed] peer closed: ${peer}…`));
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
