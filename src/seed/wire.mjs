// OBDIENT-HARVEST/1 wire protocol helpers (PLAN-002 v2, C3/C4).
//
// Connection layout over the Hyperswarm secret stream:
//   [32 bytes]  contributor's Hypercore public key (fixed-size preamble — no framing)
//   [rest…]     standard Hypercore replication stream for that core
//
// The contributor (swarm client) writes the preamble then replicates as
// initiator; the seed (swarm server) reads exactly 32 bytes, pushes any
// over-read remainder back with `unshift()` (both net.Socket and streamx
// secret streams support it), then replicates as non-initiator.

import b4a from 'b4a';

/** Shared DHT topic for case harvest — MUST match HARVEST_TOPIC in
 *  src/data/knowledge/distributed-chunk.ts. Padded to the 32-byte topic
 *  buffer, same convention as obdient-rag-v1. */
export const HARVEST_TOPIC_NAME = 'obdient-harvest-v1';

export function harvestTopic() {
  return b4a.from(HARVEST_TOPIC_NAME.padEnd(32, '\0').slice(0, 32), 'utf8');
}

export const KEY_BYTES = 32;

/** Contributor side: announce the local feed key, then hand the socket back. */
export function writeKeyPreamble(socket, publicKey) {
  if (publicKey.length !== KEY_BYTES) {
    throw new Error(`feed key must be ${KEY_BYTES} bytes, got ${publicKey.length}`);
  }
  socket.write(publicKey);
}

/** Seed side: read exactly 32 bytes off the socket, unshift any remainder so
 *  the replication protocol sees an untouched stream. Resolves with the key. */
export function readKeyPreamble(socket) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;

    const onError = (err) => {
      cleanup();
      reject(err);
    };
    const onClose = () => {
      cleanup();
      reject(new Error('connection closed before key preamble'));
    };
    const onData = (data) => {
      chunks.push(data);
      total += data.length;
      if (total < KEY_BYTES) return;

      // CRITICAL: stop the flow BEFORE detaching, or bytes arriving while the
      // caller is still setting up replication (async) are silently discarded
      // in flowing mode — dropped handshake, both sides hang forever.
      socket.pause();
      cleanup();
      const all = b4a.concat(chunks);
      const key = all.subarray(0, KEY_BYTES);
      const rest = all.subarray(KEY_BYTES);
      // Push back what belongs to the replication protocol; the consumer's
      // pipe()/replicate() resumes the flow and reads this first.
      if (rest.length > 0) socket.unshift(rest);
      resolve(key);
    };

    function cleanup() {
      socket.off('data', onData);
      socket.off('error', onError);
      socket.off('close', onClose);
    }

    socket.on('data', onData);
    socket.on('error', onError);
    socket.on('close', onClose);
  });
}
