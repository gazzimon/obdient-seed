// Transport-agnostic case store (PROTOCOL.md §Ingest semantics).
//
// BOTH transports feed this store with parsed CaseChunk objects:
//   - phase C: P2P replication (harvest.mjs reads the replicated feeds)
//   - phase D: the senior proxy calls addCase() inline at capture time
//
// Policy, in order:
//   1. shape check    — type 'case', string id, schema v missing or 1
//   2. gate re-check  — gate.passed === true or rejected (defense in depth)
//   3. dedup + MERGE  — outcome is NOT part of the content hash, so an
//      enriched re-append (outcome captured days later, offline) keeps the
//      same id. Non-null outcome wins over null; ties → newest createdAt.

import { createHash } from 'node:crypto';

/** Content-addressed case id — the exact recipe devices use (PROTOCOL.md). */
export function caseId(briefJson, seniorAnswer) {
  return createHash('sha256').update(briefJson).update(seniorAnswer).digest('hex');
}

function betterOf(a, b) {
  const aHas = a.outcome != null;
  const bHas = b.outcome != null;
  if (aHas !== bHas) return bHas ? b : a;
  return (b.createdAt ?? '') > (a.createdAt ?? '') ? b : a;
}

export function createCaseStore() {
  const byId = new Map();
  const stats = { cases: 0, gateRejected: 0, merged: 0, ignored: 0 };

  return {
    stats,

    /** Returns true when the case was stored (new or merged). */
    addCase(chunk) {
      if (
        chunk == null ||
        chunk.type !== 'case' ||
        typeof chunk.id !== 'string' ||
        (chunk.v != null && chunk.v !== 1)
      ) {
        stats.ignored++;
        return false;
      }
      stats.cases++;

      if (chunk.gate?.passed !== true) {
        stats.gateRejected++;
        return false;
      }

      const existing = byId.get(chunk.id);
      if (existing) {
        byId.set(chunk.id, betterOf(existing, chunk));
        stats.merged++;
      } else {
        byId.set(chunk.id, chunk);
      }
      return true;
    },

    /** corrections.jsonl records (ADR-0002 Phase 1 — distillation input). */
    records() {
      return [...byId.values()].map((c) => ({
        case_id: c.id,
        brief: c.brief,
        senior_answer: c.seniorAnswer,
        gate: c.gate,
        outcome: c.outcome ?? null,
        app: c.appVersion ?? null,
        observed_at: c.createdAt,
      }));
    },
  };
}

export function toJsonl(records) {
  return records.map((r) => JSON.stringify(r)).join('\n') + (records.length ? '\n' : '');
}
