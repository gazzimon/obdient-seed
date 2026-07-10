// Phase-C harvest CLI: replicated feeds → out/corrections.jsonl.
//
// Thin transport adapter over the shared case store (store.mjs): this file
// only knows how to READ Hypercore feeds; every ingest decision (shape check,
// gate re-check, dedup+merge) lives in the store — the same store the phase-D
// proxy will feed inline.
//
// Usage:  node src/ingest/harvest.mjs [--data <dir>] [--out <file>]

import Hypercore from 'hypercore';
import b4a from 'b4a';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createCaseStore, toJsonl } from './store.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(HERE, '..', '..');

/** A CaseChunk is a few KB of redacted brief + answer; anything bigger is
 *  garbage or abuse — skip it before handing it to JSON.parse. */
const MAX_BLOCK_BYTES = 256 * 1024;

function argValue(flag, fallback) {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 && process.argv[idx + 1] ? path.resolve(process.argv[idx + 1]) : fallback;
}

/** Read the given feed dirs through the shared store. Exported for the test. */
export async function harvestFeeds(feedDirs) {
  const store = createCaseStore();
  const feedStats = { blocks: 0, malformed: 0, oversize: 0 };

  for (const dir of feedDirs) {
    // Opening a missing dir would CREATE a fresh writable core there — skip
    // registry entries whose storage was deleted instead of polluting data/.
    if (!fs.existsSync(dir)) {
      console.warn(`[harvest] feed dir missing, skipped: ${dir}`);
      continue;
    }
    const core = new Hypercore(dir);
    await core.ready();

    for (let i = 0; i < core.length; i++) {
      feedStats.blocks++;
      let chunk;
      try {
        const buf = await core.get(i, { wait: false });
        if (!buf) continue; // block not locally available (sparse replica)
        if (buf.length > MAX_BLOCK_BYTES) {
          feedStats.oversize++;
          continue;
        }
        chunk = JSON.parse(b4a.toString(buf, 'utf8'));
      } catch {
        feedStats.malformed++;
        continue;
      }
      store.addCase(chunk);
    }
    await core.close();
  }

  return { records: store.records(), stats: { ...feedStats, ...store.stats } };
}

async function main() {
  const dataDir = argValue('--data', path.join(ROOT, 'data'));
  const outFile = argValue('--out', path.join(ROOT, 'out', 'corrections.jsonl'));

  let keys = {};
  try {
    keys = JSON.parse(fs.readFileSync(path.join(dataDir, 'keys.json'), 'utf8'));
  } catch {
    console.error(`[harvest] no keys.json under ${dataDir} — run the seed first.`);
    process.exit(1);
  }

  const feedDirs = Object.keys(keys).map((k) => path.join(dataDir, 'feeds', k));
  const { records, stats } = await harvestFeeds(feedDirs);

  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, toJsonl(records));

  console.log(`[harvest] feeds: ${feedDirs.length} · blocks: ${stats.blocks} · case chunks: ${stats.cases}`);
  console.log(`[harvest] gate-rejected: ${stats.gateRejected} · id-mismatch: ${stats.idMismatch} · merged: ${stats.merged} · malformed: ${stats.malformed} · oversize: ${stats.oversize} · ignored: ${stats.ignored}`);
  console.log(`[harvest] → ${records.length} records written to ${outFile}`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((err) => {
    console.error('[harvest] fatal:', err);
    process.exit(1);
  });
}
