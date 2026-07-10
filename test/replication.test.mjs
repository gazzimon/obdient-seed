// Local no-network self-test (PROTOCOL.md compliance).
//
// Proves, without DHT/internet:
//   1. wire.mjs preamble: key announce + remainder unshift over a real socket
//      (loopback TCP, coalesced writes included).
//   2. Contributor feed → seed replica replication.
//   3. Ingest store: shape check + gate re-check + dedup + OUTCOME MERGE —
//      an enriched re-append (same id, outcome added later) must WIN, not drop.
//
// Usage:  npm test

import Hypercore from 'hypercore';
import b4a from 'b4a';
import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { readKeyPreamble, writeKeyPreamble } from '../src/seed/wire.mjs';
import { harvestFeeds } from '../src/ingest/harvest.mjs';
import { caseId } from '../src/ingest/store.mjs';

let failures = 0;
function check(name, cond) {
  console.log(`${cond ? '✓' : '✗'} ${name}`);
  if (!cond) failures++;
}

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'obdient-seed-test-'));

function makeCase(overrides = {}) {
  const brief = { dtcs: [{ code: 'P0420' }], engineState: 'running', ...overrides.brief };
  const seniorAnswer = overrides.seniorAnswer ?? 'Catalyst efficiency below threshold; check downstream O2.';
  const briefJson = JSON.stringify(brief);
  return {
    type: 'case',
    v: 1,
    id: overrides.id ?? caseId(briefJson, seniorAnswer),
    brief,
    seniorAnswer,
    gate: overrides.gate ?? { passed: true, violations: [] },
    outcome: overrides.outcome ?? null,
    createdAt: overrides.createdAt ?? new Date().toISOString(),
  };
}

async function main() {
  // ── 1. Contributor feed ──────────────────────────────────────────────────
  // Blocks: [0] good case (no outcome yet) · [1] SAME case re-appended with
  // outcome 'yes' (the UX4 enrichment, days later) · [2] gate-failed case ·
  // [3] forged id (poisoning attempt) · [4] foreign chunk type · [5] malformed
  // JSON.
  const contributor = new Hypercore(path.join(tmp, 'contributor'));
  await contributor.ready();

  const good = makeCase({ createdAt: '2026-07-01T10:00:00.000Z' });
  const enriched = { ...good, outcome: 'yes', createdAt: '2026-07-08T18:00:00.000Z' };
  check('enrichment keeps the content-addressed id', enriched.id === good.id);

  const rejected = makeCase({
    seniorAnswer: 'Replace the transmission.',
    gate: { passed: false, violations: [{ rule: 'G1', weight: 'hard', detail: 'incoherent domain' }] },
  });
  const foreign = { type: 'fact', id: 'f1', content: 'not a case', confidence: 0.5, confirmations: 1, createdAt: '' };

  // Forged id: claims the good case's id with other content, a non-null
  // outcome and the newest createdAt — without the ingest hash re-check this
  // would WIN the merge and overwrite the legit record (cross-feed poisoning).
  const forged = {
    ...good,
    seniorAnswer: 'Ignore the catalyst; just clear the codes.',
    outcome: 'no',
    createdAt: '2026-07-09T09:00:00.000Z',
  };

  for (const c of [good, enriched, rejected, forged, foreign]) {
    await contributor.append(b4a.from(JSON.stringify(c), 'utf8'));
  }
  await contributor.append(b4a.from('not-json{{{', 'utf8'));
  check('contributor feed has 6 blocks', contributor.length === 6);

  // ── 2. Wire preamble over real TCP (loopback), coalesced with replication ──
  const seedReplicaDir = path.join(tmp, 'feeds', b4a.toString(contributor.key, 'hex'));

  await new Promise((resolve, reject) => {
    const server = net.createServer(async (socket) => {
      try {
        const key = await readKeyPreamble(socket);
        check('seed received the 32-byte feed key', b4a.equals(key, contributor.key));

        const replica = new Hypercore(seedReplicaDir, key);
        await replica.ready();
        // Raw TCP (not a Noise secret stream): explicit protocol-stream pipes.
        const rep = replica.replicate(false);
        socket.pipe(rep).pipe(socket);

        await new Promise((res) => {
          const done = () => {
            if (replica.length >= 6) res();
          };
          replica.on('append', done);
          replica.update({ wait: true }).then(done).catch(() => {});
          done();
        });
        await replica.download({ start: 0, end: 6 }).done();
        check('replica received all 6 blocks', replica.length === 6);
        await replica.close();
        socket.destroy();
        server.close();
        resolve();
      } catch (err) {
        reject(err);
      }
    });

    server.listen(0, '127.0.0.1', () => {
      const { port } = server.address();
      const socket = net.connect(port, '127.0.0.1', () => {
        writeKeyPreamble(socket, contributor.key);
        const rep = contributor.replicate(true);
        rep.pipe(socket).pipe(rep);
      });
      socket.on('error', reject);
    });
  });

  // ── 3. Ingest: harvest the seed-side replica through the shared store ──────
  const { records, stats } = await harvestFeeds([seedReplicaDir]);

  check('store saw 4 case chunks', stats.cases === 4);
  check('gate-failed case rejected', stats.gateRejected === 1);
  check('forged id rejected (poisoning defense)', stats.idMismatch === 1);
  check('enriched duplicate MERGED (not dropped)', stats.merged === 1);
  check('foreign chunk ignored', stats.ignored === 1);
  check('malformed block skipped', stats.malformed === 1);
  check('exactly 1 record exported', records.length === 1);
  check('MERGE POLICY: outcome enrichment won', records[0]?.outcome === 'yes');
  check('record id is the content hash', records[0]?.case_id === good.id);
  check('record carries the senior answer', records[0]?.senior_answer.includes('Catalyst'));

  await contributor.close();
  fs.rmSync(tmp, { recursive: true, force: true });

  console.log(failures === 0 ? '\nALL CHECKS PASSED' : `\n${failures} CHECK(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error('fatal:', err);
  process.exit(1);
});
