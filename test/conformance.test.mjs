// Cross-repo contract conformance (quality loop, 2026-07-10).
//
// The device (gazzimon/OBDient, p2p/harvest-worklet.mjs) computes the case id
// from `JSON.stringify(brief)`, then transmits the whole chunk with `brief`
// nested inside. The seed re-checks the id from `JSON.stringify(chunk.brief)`
// AFTER a JSON round-trip. This test proves the id survives that round-trip for
// a realistic, deeply-nested DiagnosticBrief — the silent-failure point of the
// whole harvest pipeline (a drift here makes the seed reject every case as
// idMismatch). Neither repo's own unit tests cover this seam.
//
// It reproduces the device's EXACT chunk construction (worklet appendCase) and
// pushes the parsed result through the real seed store.
//
// Usage:  node tools/../test/conformance.test.mjs   (run via `npm test`)

import { createCaseStore, caseId, toJsonl } from '../src/ingest/store.mjs';

let failures = 0;
function check(name, cond) {
  console.log(`${cond ? '✓' : '✗'} ${name}`);
  if (!cond) failures++;
}

// A realistic brief matching OBDient's DiagnosticBrief shape (nested objects,
// arrays, nulls, mixed key insertion order) — no VIN by construction.
function sampleBrief() {
  return {
    identity: { make: 'Chevrolet', model: 'Corsa', year: 2008, engine: '1.6', fuelType: 'petrol', source: 'vin' },
    mileageKm: 150000,
    dtcs: [
      { code: 'P0335', description: 'Crankshaft position sensor', severity: 'critical', faultClassId: 'sensor_crank', faultClassLabel: 'Crankshaft Position Sensor' },
    ],
    vehicleState: { engineState: 'no_start', aggregateSeverity: 'critical', activeAlerts: [], presentPids: ['RPM'], readinessComplete: false, hasFreezeFrame: false },
    liveReadings: [{ pid: 'RPM', name: 'Engine RPM', value: 0, unit: 'rpm', alert: null }],
    symptoms: [{ id: 'sym_no_start', label: 'Engine does not start' }],
    describedSymptoms: ['no arranca en frío'],
    deniedSymptoms: [],
    userNotes: null,
    createdAt: 1783200000000,
  };
}

// EXACT reproduction of the device worklet's appendCase construction.
function deviceChunk(brief, seniorAnswer, extra = {}) {
  const briefJson = JSON.stringify(brief);
  const id = caseId(briefJson, seniorAnswer);
  return {
    type: 'case',
    v: 1,
    id,
    brief,
    seniorAnswer,
    gate: extra.gate ?? { passed: true, violations: [] },
    outcome: extra.outcome ?? null,
    ...(extra.appVersion ? { appVersion: extra.appVersion } : {}),
    createdAt: new Date().toISOString(),
  };
}

// Simulate the wire: append serializes, replication delivers bytes, the seed
// parses. This is where key order must survive.
function overTheWire(chunk) {
  return JSON.parse(JSON.stringify(chunk));
}

function main() {
  const brief = sampleBrief();
  const answer = 'P0335 with no-start points to the crankshaft position sensor; test its wiring and reluctor gap.';

  // ── 1. Round-trip id stability ─────────────────────────────────────────────
  const chunk = deviceChunk(brief, answer);
  const received = overTheWire(chunk);
  const recomputed = caseId(JSON.stringify(received.brief), received.seniorAnswer);
  check('id survives JSON round-trip (nested brief)', recomputed === chunk.id);

  // ── 2. The seed store accepts the device chunk (not idMismatch) ────────────
  const store = createCaseStore();
  const accepted = store.addCase(received);
  check('seed store accepts the device chunk', accepted === true);
  check('no id mismatch', store.stats.idMismatch === 0);
  check('gate re-check passed', store.stats.gateRejected === 0);

  // ── 3. Export produces one clean corrections.jsonl record ──────────────────
  const records = store.records();
  check('exactly one record exported', records.length === 1);
  check('exported case_id equals the device id', records[0]?.case_id === chunk.id);
  const line = toJsonl(records).trim();
  check('jsonl line is valid JSON', (() => { try { JSON.parse(line); return true; } catch { return false; } })());

  // ── 4. Outcome enrichment keeps the id (UX4 re-append merges) ──────────────
  const enriched = overTheWire(deviceChunk(brief, answer, { outcome: 'yes' }));
  check('enriched re-append keeps the same id', enriched.id === chunk.id);
  store.addCase(enriched);
  check('merge, not duplicate', store.records().length === 1);
  check('outcome enrichment won', store.records()[0]?.outcome === 'yes');

  // ── 5. A forged id (poisoning) is still rejected ───────────────────────────
  const forged = overTheWire({ ...deviceChunk(brief, answer), seniorAnswer: 'totally different', id: chunk.id });
  const store2 = createCaseStore();
  store2.addCase(forged);
  check('forged id rejected (content hash re-check)', store2.stats.idMismatch === 1 && store2.records().length === 0);

  console.log(failures === 0 ? '\nCONFORMANCE OK' : `\n${failures} CHECK(S) FAILED`);
  process.exit(failures === 0 ? 0 : 1);
}

main();
