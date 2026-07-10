// Device simulator — reference implementation for the OBDient Bare worklet
// (phases C0/C1 in the app repo). Creates a local writer feed, appends a
// sample CaseChunk (schema v1, PROTOCOL.md), joins the harvest topic as
// CLIENT, announces its key (wire preamble) and replicates as initiator.
//
// End-to-end demo over the real DHT:
//   terminal 1:  npm run seed
//   terminal 2:  node test/sim-client.mjs
//   then:        npm run harvest
//
// Usage:  node test/sim-client.mjs [--data <dir>]

import Hypercore from 'hypercore';
import Hyperswarm from 'hyperswarm';
import b4a from 'b4a';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { harvestTopic, writeKeyPreamble } from '../src/seed/wire.mjs';
import { caseId } from '../src/ingest/store.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(HERE, '..');

function argValue(flag, fallback) {
  const idx = process.argv.indexOf(flag);
  return idx >= 0 && process.argv[idx + 1] ? path.resolve(process.argv[idx + 1]) : fallback;
}

function sampleCase() {
  const brief = {
    vehicle: { make: 'Chevrolet', model: 'Tracker', year: 2014 },
    dtcs: [{ code: 'P0420', severity: 'warning' }],
    symptoms: ['loss_of_power'],
    engineState: 'running',
  };
  const briefJson = JSON.stringify(brief);
  const seniorAnswer =
    'P0420 with high mileage and no misfires points to catalyst efficiency decay. ' +
    'Check the downstream O2 sensor response first; if it mirrors upstream, the ' +
    'converter is degraded.';
  return {
    type: 'case',
    v: 1,
    id: caseId(briefJson, seniorAnswer),
    brief,
    seniorAnswer,
    gate: { passed: true, violations: [] },
    outcome: null,
    appVersion: 'sim',
    createdAt: new Date().toISOString(),
  };
}

async function main() {
  const dataDir = argValue('--data', path.join(ROOT, 'sim-device'));

  const feed = new Hypercore(dataDir);
  await feed.ready();
  console.log(`[sim] local feed: ${b4a.toString(feed.key, 'hex').slice(0, 16)}… (${feed.length} blocks)`);

  const chunk = sampleCase();
  await feed.append(b4a.from(JSON.stringify(chunk), 'utf8'));
  console.log(`[sim] appended case ${chunk.id.slice(0, 12)}… (feed now ${feed.length} blocks)`);

  const swarm = new Hyperswarm();
  swarm.on('connection', (socket) => {
    console.log('[sim] seed connected — announcing key + replicating');
    writeKeyPreamble(socket, feed.key);
    feed.replicate(socket);
    socket.on('error', () => {});
  });

  swarm.join(harvestTopic(), { server: false, client: true });
  await swarm.flush();
  console.log('[sim] flushed to DHT. Leave running until the seed logs the blocks; Ctrl-C to stop.');

  process.on('SIGINT', async () => {
    await swarm.destroy();
    await feed.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error('[sim] fatal:', err);
  process.exit(1);
});
